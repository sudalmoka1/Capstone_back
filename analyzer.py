import json
import logging
import re
import time
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

# 같은 종목의 시장 데이터는 일정 시간 재사용해 야후파이낸스 요청 수를 줄인다
STOCK_CACHE_TTL = 60 * 60        # 성공한 데이터: 1시간
STOCK_FAIL_CACHE_TTL = 10 * 60   # 실패(데이터 없음): 10분 뒤 재시도

SYSTEM_PROMPT = (
    "당신은 한국 증권 뉴스의 신뢰성을 평가하는 금융 분석 전문가입니다. "
    "모든 설명은 반드시 한국어로만 작성합니다. 중국어나 영어 문장을 절대 사용하지 마세요. "
    "응답은 요청한 JSON 형식만 출력합니다."
)

# 한자가 2글자 이상 연속되면 중국어가 섞인 것으로 본다.
# (한국 기사의 '美', '中', '銀' 같은 한 글자 한자는 허용)
HAN_RUN_RE = re.compile(r"[一-鿿]{2,}")

# 과장 문구 검증 시 비교에서 무시할 문자 (공백, 각종 따옴표, 말줄임표 등 표기 차이)
PHRASE_NOISE_RE = re.compile(r"[\s'\"‘’“”`·….]+")
MAX_PHRASE_LEN = 40  # 이보다 긴 '문구'는 문장 통째 인용이라 과장 표현으로 보지 않음

# 증권 기사에서 기대감·공포감을 부추기는 자극적 표현 사전.
# 7B 모델은 프롬프트에 예시를 넣어도 과장을 대부분 놓쳐서(사람 대비 κ 0.29), 사전으로 직접 찾아 보완한다.
# 일반 시세 표현(상승, 하락, 급등, 급락)은 넣지 않는다.
HYPE_PATTERNS = (
    # 기대감
    r"폭등", r"불기둥", r"잭팟", r"역대급", r"터진다", r"터졌다", r"터질", r"들썩", r"싹쓸이", r"초?대박", r"훨훨",
    r"껑충", r"치솟", r"수직\s?상승", r"폭발(?!\s?사고)", r"광풍", r"열풍", r"텐배거", r"떡상", r"고공행진", r"축포",
    r"불장", r"쓸어\s?담", r"줍줍", r"슈퍼\s?사이클", r"승부수", r"통\s?큰", r"꿈의",
    # 근거 없는 단정
    r"반드시", r"무조건", r"(?<!불)확실시",
    # 공포감
    r"폭락", r"증발", r"휘청", r"풀썩", r"털썩", r"와르르", r"패닉", r"공포(?!\s?(지수|탐욕))", r"쇼크", r"곤두박질",
    r"추풍낙엽", r"날벼락", r"직격탄", r"쑥대밭", r"초토화", r"피눈물", r"비명", r"악몽", r"아비규환",
    r"검은\s?[월화수목금토일]요일", r"블랙\s?(먼데이|스완)",
)
HYPE_RE = re.compile("|".join(f"(?:{p})" for p in HYPE_PATTERNS))

# 숫자가 들어간 문구는 대부분 수치 전달("3% 올랐다")이라 과장이 아님.
# 7B 모델이 프롬프트 규칙을 자주 어기므로, 숫자 문구는 위 자극적 표현이 함께 있을 때만 과장으로 인정한다.
HAS_DIGIT_RE = re.compile(r"\d")


def find_hype_phrases(title: str, text: str = "", scope: str = "title") -> list[str]:
    """
    사전에 있는 자극적 표현을 찾아 '앞 단어 + 걸린 단어'로 반환 (scope: "title" = 제목만, "all" = 제목 + 본문)
    예: "증시 '휘청'에 시총 389조원 '증발'" → ["증시 휘청에", "389조원 증발"]
    """
    sources = [title or ""] + ([text or ""] if scope == "all" else [])
    found: list[str] = []
    for src in sources:
        # 문장 전체에서 찾아야 "폭발 사고"처럼 뒤 단어를 보는 예외가 동작함
        clean = " ".join(re.sub(r"[…·]", " ", re.sub(r"['\"‘’“”]", "", src)).split())
        for m in HYPE_RE.finditer(clean):
            end = clean.find(" ", m.end())
            end = len(clean) if end == -1 else end
            word_start = clean.rfind(" ", 0, m.start()) + 1
            prev_start = clean.rfind(" ", 0, max(0, word_start - 1)) + 1 if word_start > 0 else word_start
            phrase = " ".join(clean[prev_start:end].split())
            if phrase and phrase not in found:
                found.append(phrase)
    return found


class LLMAnalyzer:
    def __init__(self, model_name="qwen2.5:7b"):
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
        self._stock_cache: dict[str, tuple[float, dict | None]] = {}  # ticker -> (저장 시각, 데이터)

    def get_stock_data_cached(self, ticker: str):
        """get_stock_data 결과를 ticker 단위로 캐시해서 반환"""
        if not ticker or not isinstance(ticker, str):
            return None

        key = ticker.strip().upper()
        cached = self._stock_cache.get(key)
        if cached:
            saved_at, data = cached
            ttl = STOCK_CACHE_TTL if data is not None else STOCK_FAIL_CACHE_TTL
            if time.time() - saved_at < ttl:
                return data

        data = self.get_stock_data(key)
        self._stock_cache[key] = (time.time(), data)
        return data

    def preprocess(self, text: str) -> str:
        if not text:
            return ""
        return text.strip()[:1500]

    def get_stock_data(self, ticker: str):
        """yfinance를 통해 실시간 주식 지표 수집"""
        # 💡 [핵심 방어 코드] ticker가 None이거나 빈 문자열이면 즉시 None 반환
        if not ticker or not isinstance(ticker, str):
            return None

        try:
            ticker_clean = ticker.strip().upper()
            stock = yf.Ticker(ticker_clean)
            info = stock.info

            # 데이터가 비어 있는 경우 예외 처리
            if not info:
                return None

            data_summary = {
                "종목명": info.get("longName", "정보 없음"),
                "현재가": info.get("currentPrice", "정보 없음"),
                "목표주가": info.get("targetMeanPrice", "정보 없음"),
                "52주최고가": info.get("fiftyTwoWeekHigh", "정보 없음"),
                "PER(주가수익비율)": info.get("forwardPE", "정보 없음"),
                "시가총액": info.get("marketCap", "정보 없음"),
                "추천의견": info.get("recommendationKey", "정보 없음"),
            }
            return data_summary
        except Exception as e:
            logger.warning(f"데이터 수집 오류 ({ticker}): {e}")
            return None

    def build_prompt(self, text: str, stock_data: dict = None, title: str = None) -> str:
        if stock_data:
            market_context = (
                "[실제 시장 데이터]\n"
                f"{json.dumps(stock_data, ensure_ascii=False, indent=2)}"
            )
            data_rule = "기사 속 주장과 수치가 위 시장 데이터와 부합하는지"
        else:
            market_context = "[실제 시장 데이터]\n제공되지 않음"
            data_rule = "시장 데이터가 없으므로, 기사 안의 수치와 주장이 서로 모순 없이 일관적인지"

        return f"""다음 증권 뉴스 기사의 신뢰성을 평가하세요.

{market_context}

[분석 대상 기사]
제목: {title or "(제목 없음)"}
\"\"\"
{text}
\"\"\"

[과장 표현 찾기] 투자자의 기대감을 과도하게 부추기는 표현을 제목과 본문에서 찾아 그대로 인용합니다.
- 자극적 수식어: 폭등, 불기둥, 잭팟, 역대급, 터진다, 들썩, 싹쓸이, 대박 등
- 근거 없는 단정적 전망: "반드시 오른다", "확실시", "더 크게 터진다"
- 일부 수치나 일회성 사건을 과대 해석해 장밋빛 전망만 강조하는 표현
- 제목이 본문 내용보다 훨씬 자극적인 경우의 제목 속 표현
다음은 과장이 아닙니다.
- 실적·주가 등락률·목표가 같은 수치를 그대로 전한 문장 (예: "3% 올랐다", "영업이익 42% 상향")
- 공시, 사실 전달, 출처가 분명한 전문가·증권사 인용
인용은 짧은 표현 단위(몇 단어)로 하고, 기사에 없는 말을 지어내지 않습니다. 없으면 빈 배열입니다.

[평가 기준] 각 항목을 true 또는 false로 판단합니다.
1. exaggeration: 위에서 찾은 과장 표현이 하나라도 있으면 true
2. investment_basis: 구체적인 수치나 논리적 근거가 있으면 true
3. data_based: {data_rule} 판단해 부합하면 true
4. risk_explanation: 손실 가능성이나 하락 요인을 구체적으로 언급하면 true

[summary 작성 규칙]
- 반드시 한국어(한글)로만 2~4문장 작성합니다.
- 중국어(한자 문장)와 영어 문장은 절대 쓰지 않습니다. 종목명, 숫자, PER 같은 약어는 그대로 써도 됩니다.
- exaggeration 같은 JSON 키 이름을 summary 안에 쓰지 않습니다.

아래 JSON 형식으로만 답하세요.
{{
  "exaggeration_phrases": ["기사에서 그대로 인용한 과장 표현", ...],
  "exaggeration": true 또는 false,
  "investment_basis": true 또는 false,
  "data_based": true 또는 false,
  "risk_explanation": true 또는 false,
  "summary": "한국어 요약"
}}
"""

    def call_llm(self, prompt: str):
        payload = {
            "model": self.model_name,
            # 시스템 지시로 응답 언어를 한국어로 고정 (qwen 계열은 중국어가 섞이는 경향이 있음)
            "system": SYSTEM_PROMPT,
            "prompt": prompt,
            "format": "json",  # Ollama에 JSON 포맷 강제
            "stream": False,
            "options": {"temperature": 0.2},  # 낮을수록 언어/형식이 흔들리지 않음
        }
        try:
            response = requests.post(self.ollama_url, json=payload, timeout=120)
            response.raise_for_status()
            return response.json().get("response")
        except Exception as e:
            logger.error(f"LLM 호출 오류: {e}")
            return None

    def parse_result(self, response_text: str):
        if not response_text:
            return None
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            try:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                return json.loads(response_text[start:end])
            except Exception as e:
                logger.error(f"파싱 오류: {e}")
                return None

    def verify_exaggeration(self, result: dict, source: str) -> dict:
        """
        LLM이 뽑은 과장 문구가 실제 기사(제목+본문)에 있는지 확인해 지어낸 문구를 버리고,
        남은 문구가 있을 때만 exaggeration=true로 확정한다.
        """
        phrases = result.get("exaggeration_phrases")
        if not isinstance(phrases, list):
            phrases = []

        normalized_source = PHRASE_NOISE_RE.sub("", source or "")
        verified, dropped = [], []
        for p in phrases:
            if not isinstance(p, str):
                continue
            p = re.sub(r"['\"‘’“”]", "", p).strip()  # 저장·표시용으로 따옴표 제거 (예: '100조' 역대급 → 100조 역대급)
            key = PHRASE_NOISE_RE.sub("", p)
            numeric_fact = HAS_DIGIT_RE.search(p) and not HYPE_RE.search(p)
            if key and len(p) <= MAX_PHRASE_LEN and key in normalized_source and not numeric_fact and p not in verified:
                verified.append(p)
            elif p:
                dropped.append(p)

        if dropped:
            logger.info("과장 문구에서 제외 (기사에 없음 / 너무 긺 / 단순 수치): %s", dropped)
        result["exaggeration_phrases"] = verified
        result["exaggeration"] = bool(verified)
        return result

    def apply_hype_rule(self, result: dict, title: str) -> dict:
        """
        LLM이 놓친 과장을 사전으로 보완: 제목에 자극적 표현이 있으면 과장으로 판정.
        (검증: 사람 라벨 대비 과장 κ가 개발용 100건 0.29→0.58, 새 기사 50건 0.00→0.25. 본문까지 보면 인용문 오탐이 생겨 제목만 봄)
        """
        hits = find_hype_phrases(title, scope="title")
        phrases = result.get("exaggeration_phrases") or []
        # LLM이 이미 뽑은 문구와 겹치면("역대급" / "매수 역대급") 한 번만 표시
        result["exaggeration_phrases"] = phrases + [h for h in hits if not any(p in h or h in p for p in phrases)]
        result["exaggeration_rule"] = hits  # 어떤 표현이 사전으로 잡혔는지 (LLM 판정과 구분용)
        result["exaggeration"] = bool(result["exaggeration_phrases"])
        return result

    def calculate_score(self, result: dict) -> int:
        score = 100
        if result.get("exaggeration"):
            score -= 25
        if not result.get("investment_basis"):
            score -= 25
        if not result.get("data_based"):
            score -= 25
        if not result.get("risk_explanation"):
            score -= 25
        return max(score, 0)

    def analyze(self, ticker: str, text: str, stock_name: str = None, title: str = None):
        stock_data = self.get_stock_data_cached(ticker)
        # 야후 종목명은 영어(예: Samsung Electronics)라 요약에 영어가 섞이므로 한국어 종목명으로 교체
        if stock_data and stock_name:
            stock_data = {**stock_data, "종목명": stock_name}
        clean_text = self.preprocess(text)
        prompt = self.build_prompt(clean_text, stock_data, title)

        raw_response = self.call_llm(prompt)
        if not raw_response:
            return {"error": "LLM 응답이 없습니다."}

        result = self.parse_result(raw_response)

        # 요약에 중국어가 섞였으면 한국어로만 다시 쓰라고 한 번 더 요청
        if result and HAN_RUN_RE.search(str(result.get("summary", ""))):
            logger.info("요약에 중국어가 섞여 한 번 재요청합니다.")
            retry_prompt = prompt + "\n주의: 이전 답변에 중국어가 섞였습니다. summary를 한국어(한글)로만 다시 작성하세요.\n"
            retry_raw = self.call_llm(retry_prompt)
            retry_result = self.parse_result(retry_raw) if retry_raw else None
            if retry_result and not HAN_RUN_RE.search(str(retry_result.get("summary", ""))):
                result = retry_result

        if result:
            # LLM이 보고 판단한 범위(제목 + 잘린 본문) 안에서 과장 문구를 검증
            result = self.verify_exaggeration(result, f"{title or ''}\n{clean_text}")
            result = self.apply_hype_rule(result, title)
            result["ticker"] = ticker
            result["score"] = self.calculate_score(result)
            return result
        return {"error": "분석에 실패했습니다."}