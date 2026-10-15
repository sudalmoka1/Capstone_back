"""
evaluation/apply_rule.py - DB에 저장된 LLM 판정에 과장 사전 규칙을 더한 결과를 CSV로 저장

LLM을 다시 돌리지 않고, 저장된 판정의 exaggeration에 사전(analyzer.find_hype_phrases) 결과를 OR로 합칩니다.
analyzer.py의 분석 순서(LLM 판정 → 규칙 보완)와 같은 결과가 나옵니다. DB는 바꾸지 않습니다.

[실행] python -m evaluation.apply_rule --scope title
       python -m evaluation.agreement evaluation/data/labels_A.csv --preds evaluation/data/preds_rule_title.csv
[DB 반영] python -m evaluation.apply_rule --apply-db
       (반영 전 판정을 evaluation/data/backup_evaluations_<시각>.json 과 preds_db_before_<시각>.csv 로 저장한 뒤,
        규칙이 적용되지 않은 판정에만 제목 사전 규칙을 더해 report와 score를 갱신. 여러 번 실행해도 결과가 같음)
"""

import argparse
import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from analyzer import LLMAnalyzer, find_hype_phrases
from api.storage import ArticleEvaluationModel, ArticleModel, AsyncSessionLocal, engine
from evaluation.common import CRITERIA


async def _main(scope: str, out: Path):
    stmt = (
        select(ArticleModel.id, ArticleModel.title, ArticleModel.content, ArticleEvaluationModel.report)
        .join(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticleModel.id)
        .where(ArticleEvaluationModel.status == "COMPLETED")
    )
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(stmt)).all()
    await engine.dispose()

    analyzer = LLMAnalyzer()
    changed = 0
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["article_id"] + [key for key, _ in CRITERIA] + ["score", "exaggeration_phrases", "error"])
        for art_id, title, content, report in rows:
            r = json.loads(report) if isinstance(report, str) else dict(report or {})
            before = bool(r.get("exaggeration"))
            if scope == "title":
                r = analyzer.apply_hype_rule(r, title)  # 서비스(analyzer.analyze)와 같은 함수
            else:
                # 실험용: LLM이 본 범위(제목 + 본문 앞 1500자) 전체에서 찾음
                hits = find_hype_phrases(title, analyzer.preprocess(content), scope="all")
                r["exaggeration_phrases"] = list(r.get("exaggeration_phrases") or []) + hits
                r["exaggeration"] = before or bool(hits)
            changed += int(r["exaggeration"] and not before)
            writer.writerow(
                [art_id]
                + [int(bool(r.get(key))) for key, _ in CRITERIA]
                + [analyzer.calculate_score(r), " | ".join(r.get("exaggeration_phrases") or []), ""]
            )
    print(f"{len(rows)}건 중 {changed}건이 규칙으로 과장 판정 추가 (scope={scope}) → {out}")


async def apply_to_db() -> None:
    """저장된 판정에 제목 사전 규칙을 반영 (백업 후 갱신)"""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(ArticleModel.id, ArticleModel.title, ArticleEvaluationModel)
            .join(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticleModel.id)
        )).all()

        # 1) 백업: 원본 행 그대로(JSON) + agreement.py --preds 로 바로 쓸 수 있는 CSV
        backup = [{"article_id": e.article_id, "status": e.status, "score": e.score, "report": e.report} for _, _, e in rows]
        (data_dir / f"backup_evaluations_{stamp}.json").write_text(json.dumps(backup, ensure_ascii=False, indent=1), encoding="utf-8")
        with open(data_dir / f"preds_db_before_{stamp}.csv", "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["article_id"] + [key for key, _ in CRITERIA] + ["score", "exaggeration_phrases", "error"])
            for _, _, e in rows:
                if e.status != "COMPLETED":
                    continue
                r = json.loads(e.report) if isinstance(e.report, str) else (e.report or {})
                writer.writerow([e.article_id] + [int(bool(r.get(key))) for key, _ in CRITERIA]
                                + [e.score, " | ".join(r.get("exaggeration_phrases") or []), ""])

        # 2) 규칙 반영 (이미 반영된 판정은 건너뜀)
        analyzer = LLMAnalyzer()
        changed = skipped = 0
        for _, title, e in rows:
            if e.status != "COMPLETED":
                continue
            r = json.loads(e.report) if isinstance(e.report, str) else dict(e.report or {})
            if "exaggeration_rule" in r:
                skipped += 1
                continue
            before = bool(r.get("exaggeration"))
            r = analyzer.apply_hype_rule(r, title)
            r["score"] = analyzer.calculate_score(r)
            e.report = json.dumps(r, ensure_ascii=False)
            e.score = r["score"]
            changed += int(r["exaggeration"] and not before)
        await session.commit()
    await engine.dispose()
    print(f"백업 → backup_evaluations_{stamp}.json, preds_db_before_{stamp}.csv")
    print(f"규칙 반영 완료: 과장으로 바뀐 판정 {changed}건 (이미 반영돼 건너뜀 {skipped}건)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="저장된 LLM 판정 + 과장 사전 규칙")
    parser.add_argument("--scope", choices=["title", "all"], default="title", help="title = 제목만, all = 제목 + 본문")
    parser.add_argument("--out", type=Path, default=None, help="결과 CSV (기본: evaluation/data/preds_rule_<scope>.csv)")
    parser.add_argument("--apply-db", action="store_true", help="CSV 대신 DB의 판정을 갱신 (백업 후, 제목 규칙)")
    args = parser.parse_args()
    if args.apply_db:
        asyncio.run(apply_to_db())
    else:
        out = args.out or Path(__file__).parent / "data" / f"preds_rule_{args.scope}.csv"
        asyncio.run(_main(args.scope, out))
