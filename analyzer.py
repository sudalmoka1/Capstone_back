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

    def build_prompt(self, text: str, stock_data: dict = None) -> str:
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
\"\"\"
{text}
\"\"\"

[평가 기준] 각 항목을 true 또는 false로 판단합니다.
1. exaggeration: 실제 지표(목표가, PER 등) 대비 주장이 지나치게 낙관적이면 true
2. investment_basis: 구체적인 수치나 논리적 근거가 있으면 true
3. data_based: {data_rule} 판단해 부합하면 true
4. risk_explanation: 손실 가능성이나 하락 요인을 구체적으로 언급하면 true

[summary 작성 규칙]
- 반드시 한국어(한글)로만 2~4문장 작성합니다.
- 중국어(한자 문장)와 영어 문장은 절대 쓰지 않습니다. 종목명, 숫자, PER 같은 약어는 그대로 써도 됩니다.
- exaggeration 같은 JSON 키 이름을 summary 안에 쓰지 않습니다.

아래 JSON 형식으로만 답하세요.
{{
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

    def analyze(self, ticker: str, text: str, stock_name: str = None):
        stock_data = self.get_stock_data_cached(ticker)
        # 야후 종목명은 영어(예: Samsung Electronics)라 요약에 영어가 섞이므로 한국어 종목명으로 교체
        if stock_data and stock_name:
            stock_data = {**stock_data, "종목명": stock_name}
        clean_text = self.preprocess(text)
        prompt = self.build_prompt(clean_text, stock_data)

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
            result["ticker"] = ticker
            result["score"] = self.calculate_score(result)
            return result
        return {"error": "분석에 실패했습니다."}