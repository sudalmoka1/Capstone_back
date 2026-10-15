"""
evaluation/run_model.py - 라벨링한 표본 기사를 다른 LLM 모델로 다시 분석 (모델 비교용)

DB에는 저장하지 않고 결과를 CSV로만 남깁니다. 만든 CSV는 agreement.py의 --preds로 넘겨 사람 라벨과 비교합니다.

[실행] python -m evaluation.run_model evaluation/data/labels_template.csv --model llama3
"""

import argparse
import asyncio
import csv
from pathlib import Path

from sqlalchemy import select

from analyzer import LLMAnalyzer
from api.storage import ArticleModel, ArticleStockModel, AsyncSessionLocal, StockModel, engine
from evaluation.common import CRITERIA, read_csv_rows
from scheduler import to_ticker


def read_ids(path: Path) -> list[int]:
    return [int(row["article_id"]) for row in read_csv_rows(path)]


async def fetch_articles(ids: list[int]) -> dict[int, tuple]:
    """기사 + 대표 종목 (scheduler.py의 분석 입력과 동일)"""
    stmt = (
        select(ArticleModel, StockModel.stock_code, StockModel.market, StockModel.stock_name)
        .outerjoin(ArticleStockModel, (ArticleStockModel.article_id == ArticleModel.id) & (ArticleStockModel.rank == 0))
        .outerjoin(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
        .where(ArticleModel.id.in_(ids))
    )
    async with AsyncSessionLocal() as session:
        return {a.id: (a, code, market, name) for a, code, market, name in (await session.execute(stmt)).all()}


async def _main(samples: Path, model: str, out: Path, limit: int | None):
    ids = read_ids(samples)[:limit]
    articles = await fetch_articles(ids)
    await engine.dispose()

    analyzer = LLMAnalyzer(model_name=model)
    out.parent.mkdir(parents=True, exist_ok=True)
    ok = 0
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["article_id"] + [key for key, _ in CRITERIA] + ["score", "exaggeration_phrases", "error"])
        for idx, art_id in enumerate(ids, 1):
            if art_id not in articles:
                print(f"[{idx}/{len(ids)}] 기사 {art_id}: DB에 없음, 건너뜀")
                continue
            article, code, market, name = articles[art_id]
            report = await asyncio.to_thread(
                analyzer.analyze, to_ticker(code, market), article.content, name, article.title
            )
            if "error" in report:
                writer.writerow([art_id] + [""] * len(CRITERIA) + ["", "", report["error"]])
                print(f"[{idx}/{len(ids)}] 기사 {art_id}: 실패 ({report['error']})")
            else:
                writer.writerow(
                    [art_id]
                    + [int(bool(report.get(key))) for key, _ in CRITERIA]
                    + [report["score"], " | ".join(report.get("exaggeration_phrases", [])), ""]
                )
                ok += 1
                print(f"[{idx}/{len(ids)}] 기사 {art_id}: {report['score']}점")
            f.flush()  # 중간에 멈춰도 그때까지의 결과는 남김

    print(f"{model}: {len(ids)}건 중 {ok}건 성공 → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="표본 기사를 다른 모델로 다시 분석")
    parser.add_argument("samples", type=Path, help="export_samples.py로 만든 CSV (article_id 열만 사용)")
    parser.add_argument("--model", required=True, help="Ollama 모델 이름 (예: llama3, qwen2.5:7b)")
    parser.add_argument("--out", type=Path, default=None, help="결과 CSV (기본: evaluation/data/preds_<모델>.csv)")
    parser.add_argument("--limit", type=int, default=None, help="앞에서부터 N건만 (동작 확인용)")
    args = parser.parse_args()
    out = args.out or Path(__file__).parent / "data" / f"preds_{args.model.replace(':', '_')}.csv"
    asyncio.run(_main(args.samples, args.model, out, args.limit))
