"""
evaluation/export_samples.py - 사람 라벨링용 표본 기사 추출

분석 완료(COMPLETED) 기사 중 AI 점수별 비율을 맞춰 무작위로 뽑아 CSV로 저장합니다.
- 본문은 LLM이 실제로 본 범위(앞 1500자)만 넣어 사람과 AI가 같은 정보로 판단하게 합니다.
- LLM 판정은 넣지 않습니다. 라벨링하는 사람이 AI 답을 보고 따라가지 않도록 하기 위해서입니다.
- 행 순서를 섞어 점수 순서가 드러나지 않게 합니다.
- AI가 과장으로 판정한 기사는 전체의 5% 정도로 적어서, 무작위로만 뽑으면 과장 항목의 일치도를 계산할 수 없습니다.
  그래서 기본값으로 과장 판정 기사를 모두 넣고 나머지를 점수별 비율로 채웁니다. (--no-include-exaggeration 으로 끔)

[실행] python -m evaluation.export_samples --n 60
[검증용 새 표본] python -m evaluation.export_samples --n 40 --exclude evaluation/data/labels_template.csv --out evaluation/data/labels_template_2.csv
          (프롬프트를 고칠 때 참고한 기사를 빼고 뽑아야 개선 효과를 공정하게 잴 수 있음)
"""

import argparse
import asyncio
import csv
import json
import random
from collections import defaultdict
from pathlib import Path

from sqlalchemy import select

from analyzer import LLMAnalyzer
from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticleStockModel,
    AsyncSessionLocal,
    PublisherModel,
    StockModel,
    engine,
)
from evaluation.common import CRITERIA, read_csv_rows

DEFAULT_OUT = Path(__file__).parent / "data" / "labels_template.csv"


async def fetch_completed() -> list:
    stmt = (
        select(
            ArticleModel,
            ArticleEvaluationModel.score,
            ArticleEvaluationModel.report,
            PublisherModel.name.label("publisher"),
            StockModel.stock_name,
        )
        .join(
            ArticleEvaluationModel,
            (ArticleEvaluationModel.article_id == ArticleModel.id) & (ArticleEvaluationModel.status == "COMPLETED"),
        )
        .outerjoin(PublisherModel, PublisherModel.id == ArticleModel.publisher_id)
        # 대표 종목이 연결된 기사만 (부고·포토 등 주식 무관으로 제외된 기사는 뽑지 않음)
        .join(ArticleStockModel, (ArticleStockModel.article_id == ArticleModel.id) & (ArticleStockModel.rank == 0))
        .outerjoin(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
        .order_by(ArticleModel.id)
    )
    async with AsyncSessionLocal() as session:
        return (await session.execute(stmt)).all()


def is_exaggeration(row) -> bool:
    try:
        return bool(json.loads(row.report or "{}").get("exaggeration"))
    except json.JSONDecodeError:
        return False


def stratified_sample(rows: list, n: int, seed: int, include_exaggeration: bool) -> list:
    """(옵션) AI 과장 판정 기사는 모두 넣고, 나머지는 AI 점수별 비율대로 뽑음 (점수 구간마다 최소 1건)"""
    rng = random.Random(seed)
    forced = [r for r in rows if include_exaggeration and is_exaggeration(r)][:n]
    rest = [r for r in rows if r not in forced]
    need = n - len(forced)
    if need >= len(rest):
        picked = forced + rest
    else:
        groups = defaultdict(list)
        for r in rest:
            groups[r.score].append(r)
        picked = list(forced)
        for _, items in sorted(groups.items()):
            k = max(1, round(need * len(items) / len(rest)))
            picked += rng.sample(items, min(k, len(items)))
    rng.shuffle(picked)
    return picked


def write_csv(rows: list, out: Path) -> None:
    preprocess = LLMAnalyzer().preprocess
    out.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig: 엑셀에서 열어도 한글이 깨지지 않음
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["article_id", "publisher", "published_at", "stock", "title", "content", "url"]
            + [key for key, _ in CRITERIA]
            + ["memo"]
        )
        for r in rows:
            a = r.ArticleModel
            writer.writerow(
                [
                    a.id,
                    r.publisher or "",
                    a.published_at.strftime("%Y-%m-%d %H:%M") if a.published_at else "",
                    r.stock_name or "",
                    a.title,
                    preprocess(a.content),
                    a.url,
                ]
                + [""] * len(CRITERIA)
                + [""]
            )


async def _main(n: int, seed: int, out: Path, include_exaggeration: bool, exclude: list[Path]):
    rows = await fetch_completed()
    await engine.dispose()
    excluded = {int(r["article_id"]) for p in exclude for r in read_csv_rows(p)}
    if excluded:
        rows = [r for r in rows if r.ArticleModel.id not in excluded]
        print(f"이전 표본 {len(excluded)}건 제외")
    if not rows:
        print("뽑을 수 있는 분석 완료(COMPLETED) 기사가 없습니다. 스케줄러로 먼저 분석하세요.")
        return
    picked = stratified_sample(rows, n, seed, include_exaggeration)
    write_csv(picked, out)
    n_ex = sum(1 for r in picked if is_exaggeration(r))
    print(f"분석 완료 기사 {len(rows)}건 중 {len(picked)}건 추출 (AI 과장 판정 {n_ex}건 포함) → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="사람 라벨링용 표본 기사 추출")
    parser.add_argument("--n", type=int, default=60, help="추출할 기사 수 (기본 60)")
    parser.add_argument("--seed", type=int, default=42, help="무작위 추출 시드 (같은 값이면 같은 표본)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="저장할 CSV 경로")
    parser.add_argument(
        "--include-exaggeration", action=argparse.BooleanOptionalAction, default=True,
        help="AI가 과장으로 판정한 기사를 모두 포함 (기본 켜짐)",
    )
    parser.add_argument("--exclude", type=Path, nargs="*", default=[], help="이 CSV들에 있는 기사는 빼고 추출")
    args = parser.parse_args()
    asyncio.run(_main(args.n, args.seed, args.out, args.include_exaggeration, args.exclude))
