import json
import logging
import requests
import yfinance as yf

logger = logging.getLogger(__name__)


class LLMAnalyzer:
    def __init__(self, model_name="llama3"):
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"

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
        market_context = ""
        if stock_data:
            market_context = f"\n[실제 시장 데이터 참고]\n{json.dumps(stock_data, ensure_ascii=False, indent=2)}\n"

        return f"""
당신은 금융 투자 정보 분석 전문가입니다. 제공된 실제 시장 데이터와 분석 대상 텍스트를 비교하여 신뢰성을 평가하세요.
{market_context}
[분석 대상 텍스트]
\"\"\"
{text}
\"\"\"

[분석 기준]
1. exaggeration: 실제 지표(목표가, PER 등) 대비 주장이 지나치게 낙관적인지 여부
2. investment_basis: 구체적인 수치나 논리적 근거가 포함되었는지 여부
3. data_based: 실제 시장 데이터와 텍스트의 내용이 부합하는지 여부
4. risk_explanation: 손실 가능성이나 하락 요인이 구체적으로 언급되었는지 여부

반드시 JSON 형식으로만 답하세요. (한국어로 답변)
{{
  "exaggeration": true or false,
  "investment_basis": true or false,
  "data_based": true or false,
  "risk_explanation": true or false,
  "summary": "시장 데이터와 대조한 상세 요약"
}}
"""

    def call_llm(self, prompt: str):
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "format": "json",  # Ollama에 JSON 포맷 강제
            "stream": False,
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

    def analyze(self, ticker: str, text: str):
        stock_data = self.get_stock_data(ticker)
        clean_text = self.preprocess(text)
        prompt = self.build_prompt(clean_text, stock_data)

        raw_response = self.call_llm(prompt)
        if not raw_response:
            return {"error": "LLM 응답이 없습니다."}

        result = self.parse_result(raw_response)

        if result:
            result["ticker"] = ticker
            result["score"] = self.calculate_score(result)
            return result
        return {"error": "분석에 실패했습니다."}