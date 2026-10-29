"""
evaluation/run_api.py - 라벨링한 표본 기사를 외부 대형 모델(Gemini, GPT)로 분석 (우리 모델과 비교용)

- 우리 모델(analyzer.py)과 같은 프롬프트·시장 데이터·과장 문구 검증을 쓰고, LLM 호출만 외부 API로 바꿉니다.
- 비교 기준(preds_db_before_*.csv)이 제목 사전 규칙 적용 전 판정이라 기본값은 사전 규칙 없이 실행합니다.
- 기사 본문은 DB가 아니라 라벨 CSV에서 읽습니다. (DB를 덤프로 교체하면서 기사 번호가 바뀌었기 때문)
- API 키는 환경 변수 GEMINI_API_KEY / OPENAI_API_KEY에서 읽습니다. 코드나 파일에 키를 적지 마세요.
  (학과 공용 PC에서는 터미널에서만 $env:로 넣고, 다 쓰면 키를 폐기)

[실행]
  $env:GEMINI_API_KEY = "<키>"
  python -m evaluation.run_api evaluation/data/labels_조성목_100.csv --model gemini-3.5-flash
  $env:OPENAI_API_KEY = "<키>"
  python -m evaluation.run_api evaluation/data/labels_조성목_100.csv --model gpt-5-mini
  python -m evaluation.run_api evaluation/data/labels_조성목_100.csv --model gpt-5-mini --resume   (실패한 기사만 다시)
  python -m evaluation.agreement evaluation/data/labels_조성목_100.csv ... --preds evaluation/data/preds_gpt-5-mini.csv
"""

import argparse
import asyncio
import csv
import logging
import os
import time
from pathlib import Path

import requests
from sqlalchemy import select

from analyzer import SYSTEM_PROMPT, LLMAnalyzer
from api.storage import AsyncSessionLocal, StockModel, engine
from evaluation.common import CRITERIA, read_csv_rows
from scheduler import to_ticker

logger = logging.getLogger(__name__)

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
RETRY_STATUS = (429, 500, 502, 503)  # 분당 요청 제한·일시 오류는 기다렸다 재시도
MAX_RETRY = 5


class APIAnalyzer(LLMAnalyzer):
    def __init__(self, model_name: str, api_key: str, use_rule: bool = False):
        super().__init__(model_name=model_name)
        self.api_key = api_key
        self.use_rule = use_rule

    def request(self, prompt: str) -> requests.Response:
        raise NotImplementedError

    def extract_text(self, data: dict) -> str:
        raise NotImplementedError

    def call_llm(self, prompt: str):
        for attempt in range(1, MAX_RETRY + 1):
            try:
                response = self.request(prompt)
                if response.status_code in RETRY_STATUS:
                    wait = 10 * attempt
                    logger.warning("%s %s, %d초 뒤 재시도 (%d/%d)", self.model_name, response.status_code, wait, attempt, MAX_RETRY)
                    time.sleep(wait)
                    continue
                if not response.ok:
                    logger.error("%s 호출 오류 %s: %s", self.model_name, response.status_code, response.text[:300])
                    return None
                return self.extract_text(response.json())
            except Exception as e:
                logger.error(f"{self.model_name} 호출 오류: {e}")
                return None
        return None

    def apply_hype_rule(self, result: dict, title: str) -> dict:
        return super().apply_hype_rule(result, title) if self.use_rule else result


class GeminiAnalyzer(APIAnalyzer):
    def request(self, prompt: str) -> requests.Response:
        payload = {
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
        }
        return requests.post(
            GEMINI_URL.format(model=self.model_name), headers={"x-goog-api-key": self.api_key}, json=payload, timeout=120
        )

    def extract_text(self, data: dict) -> str:
        parts = data["candidates"][0]["content"]["parts"]
        return "".join(p.get("text", "") for p in parts if not p.get("thought"))


class OpenAIAnalyzer(APIAnalyzer):
    def request(self, prompt: str) -> requests.Response:
        payload = {
            "model": self.model_name,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
        }
        if not self.model_name.startswith(("gpt-5", "o")):  # 추론 모델은 temperature 기본값(1)만 허용
            payload["temperature"] = 0.2
        return requests.post(OPENAI_URL, headers={"Authorization": f"Bearer {self.api_key}"}, json=payload, timeout=180)

    def extract_text(self, data: dict) -> str:
        return data["choices"][0]["message"]["content"]


def make_analyzer(model: str, use_rule: bool) -> APIAnalyzer:
    cls, env = (GeminiAnalyzer, "GEMINI_API_KEY") if model.startswith("gemini") else (OpenAIAnalyzer, "OPENAI_API_KEY")
    api_key = os.getenv(env)
    if not api_key:
        raise SystemExit(f"환경 변수 {env}가 없습니다.")
    return cls(model, api_key, use_rule)


async def fetch_stocks(names: set[str]) -> dict[str, tuple[str, str]]:
    """종목명 → (종목코드, 시장)"""
    stmt = select(StockModel.stock_name, StockModel.stock_code, StockModel.market).where(StockModel.stock_name.in_(names))
    async with AsyncSessionLocal() as session:
        return {name: (code, market) for name, code, market in (await session.execute(stmt)).all()}


def read_done(out: Path) -> dict[str, list]:
    """이전 실행에서 성공한 행 (--resume용): article_id → CSV 행"""
    if not out.exists():
        return {}
    with open(out, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return {r[0]: r for r in reader if r and not r[-1]}


async def _main(samples: Path, model: str, out: Path, limit: int | None, use_rule: bool, resume: bool):
    analyzer = make_analyzer(model, use_rule)
    rows = read_csv_rows(samples)[:limit]
    done = read_done(out) if resume else {}
    stocks = await fetch_stocks({r["stock"] for r in rows if r.get("stock")})
    await engine.dispose()

    out.parent.mkdir(parents=True, exist_ok=True)
    ok = 0
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["article_id"] + [key for key, _ in CRITERIA] + ["score", "exaggeration_phrases", "error"])
        for idx, row in enumerate(rows, 1):
            art_id = row["article_id"]
            if art_id in done:
                writer.writerow(done[art_id])
                ok += 1
                continue
            code, market = stocks.get(row.get("stock"), (None, None))
            report = await asyncio.to_thread(
                analyzer.analyze, to_ticker(code, market), row["content"], row.get("stock") or None, row["title"]
            )
            if not report or "error" in report:
                err = (report or {}).get("error", "응답 파싱 실패")
                writer.writerow([art_id] + [""] * len(CRITERIA) + ["", "", err])
                print(f"[{idx}/{len(rows)}] 기사 {art_id}: 실패 ({err})")
            else:
                writer.writerow(
                    [art_id]
                    + [int(bool(report.get(key))) for key, _ in CRITERIA]
                    + [report["score"], " | ".join(report.get("exaggeration_phrases", [])), ""]
                )
                ok += 1
                print(f"[{idx}/{len(rows)}] 기사 {art_id}: {report['score']}점")
            f.flush()  # 중간에 멈춰도 그때까지의 결과는 남김

    print(f"{model}: {len(rows)}건 중 {ok}건 성공 → {out}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description="표본 기사를 외부 대형 모델(Gemini, GPT)로 분석")
    parser.add_argument("samples", type=Path, help="라벨 CSV (article_id, stock, title, content 열 사용)")
    parser.add_argument("--model", required=True, help="모델 이름 (gemini-로 시작하면 Gemini, 그 외는 OpenAI. 예: gemini-3.5-flash, gpt-5-mini)")
    parser.add_argument("--out", type=Path, default=None, help="결과 CSV (기본: evaluation/data/preds_<모델>.csv)")
    parser.add_argument("--limit", type=int, default=None, help="앞에서부터 N건만 (동작 확인용)")
    parser.add_argument("--rule", action="store_true", help="제목 과장 사전 규칙도 적용 (기본: LLM 판정만)")
    parser.add_argument("--resume", action="store_true", help="결과 CSV에서 성공한 기사는 두고 실패·미실행 기사만 다시 분석")
    args = parser.parse_args()
    out = args.out or Path(__file__).parent / "data" / f"preds_{args.model}{'_rule' if args.rule else ''}.csv"
    asyncio.run(_main(args.samples, args.model, out, args.limit, args.rule, args.resume))
