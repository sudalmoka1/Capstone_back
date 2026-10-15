"""
evaluation/agreement.py - 사람 라벨 vs AI 판정 일치도 계산

- 항목별 일치율과 Cohen's kappa (우연히 맞을 확률을 뺀 일치도)
- 점수 비교: 사람 라벨에도 같은 공식을 적용해 점수 차이(MAE)와 상관계수 계산
- 라벨 파일이 2개 이상이면 사람끼리의 일치도도 계산 (AI 성능을 비교할 기준선)

[실행]
  python -m evaluation.agreement evaluation/data/labels_조성목.csv
  python -m evaluation.agreement evaluation/data/labels_A.csv evaluation/data/labels_B.csv --out evaluation/data/result.md
  python -m evaluation.agreement evaluation/data/labels_A.csv --preds evaluation/data/preds_llama3.csv   (다른 모델 결과와 비교)
"""

import argparse
import asyncio
import csv
import itertools
import json
from pathlib import Path

from sqlalchemy import select

from api.storage import ArticleEvaluationModel, ArticleModel, AsyncSessionLocal, engine
from evaluation.common import CRITERIA, cohen_kappa, is_complete, kappa_label, read_csv_rows, read_labels, score_of


async def load_db_preds() -> tuple[dict[int, dict], dict[int, dict]]:
    """DB에 저장된 AI 판정 → ({article_id: {항목: bool}}, {article_id: {title, phrases}})"""
    stmt = (
        select(ArticleModel.id, ArticleModel.title, ArticleEvaluationModel.report)
        .join(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticleModel.id)
        .where(ArticleEvaluationModel.status == "COMPLETED")
    )
    preds, info = {}, {}
    async with AsyncSessionLocal() as session:
        for art_id, title, report in (await session.execute(stmt)).all():
            try:
                r = json.loads(report) if isinstance(report, str) else (report or {})
            except json.JSONDecodeError:
                continue
            preds[art_id] = {key: bool(r.get(key)) for key, _ in CRITERIA}
            info[art_id] = {"title": title, "phrases": " | ".join(r.get("exaggeration_phrases") or [])}
    return preds, info


def load_csv_preds(path: Path) -> tuple[dict[int, dict], dict[int, dict]]:
    preds, _ = read_labels(path)
    info = {
        int(row["article_id"]): {"title": "", "phrases": row.get("exaggeration_phrases", "")}
        for row in read_csv_rows(path)
    }
    return preds, info


def pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (vx * vy) ** 0.5


def fmt(v: float | None, pct: bool = False) -> str:
    if v is None:
        return "-"
    return f"{v * 100:.1f}%" if pct else f"{v:.2f}"


def compare(a: dict[int, dict], b: dict[int, dict]) -> dict:
    """두 판정 묶음의 항목별 일치율·κ와 점수 비교. 한쪽이라도 빈칸인 기사는 그 항목에서만 뺌"""
    common = sorted(set(a) & set(b))
    items = {}
    for key, _ in CRITERIA:
        pairs = [(a[i][key], b[i][key]) for i in common if a[i][key] is not None and b[i][key] is not None]
        xs, ys = [x for x, _ in pairs], [y for _, y in pairs]
        n = len(pairs)
        items[key] = {
            "n": n,
            "acc": sum(x == y for x, y in pairs) / n if n else None,
            "kappa": cohen_kappa(xs, ys),
            "rate_a": sum(xs) / n if n else None,
            "rate_b": sum(ys) / n if n else None,
        }
    # 점수는 네 항목이 다 있어야 계산되므로 양쪽 모두 빈칸 없는 기사만
    full = [i for i in common if is_complete(a[i]) and is_complete(b[i])]
    sa = [score_of(a[i]) for i in full]
    sb = [score_of(b[i]) for i in full]
    return {"common": len(common), "items": items, "n_score": len(full), "sa": sa, "sb": sb, "corr": pearson(sa, sb)}


def compare_table(a: dict[int, dict], b: dict[int, dict], name_a: str, name_b: str) -> tuple[list[str], dict]:
    """두 판정 묶음의 항목별 일치도 표 (마크다운 줄 목록, compare() 결과)"""
    c = compare(a, b)
    lines = [
        f"| 항목 | 기사 수 | 일치율 | Cohen's κ | 해석 | {name_a} O 비율 | {name_b} O 비율 |",
        "|---|---|---|---|---|---|---|",
    ]
    for key, label in CRITERIA:
        it = c["items"][key]
        lines.append(
            f"| {label} | {it['n']} | {fmt(it['acc'], pct=True)} | {fmt(it['kappa'])} | {kappa_label(it['kappa'])} "
            f"| {fmt(it['rate_a'], pct=True)} | {fmt(it['rate_b'], pct=True)} |"
        )

    sa, sb, n = c["sa"], c["sb"], c["n_score"]
    if n:
        exact = sum(x == y for x, y in zip(sa, sb)) / n
        mae = sum(abs(x - y) for x, y in zip(sa, sb)) / n
        lines += [
            "",
            f"- 점수 비교 기사 수: {n}건 (네 항목이 양쪽 모두 채워진 기사)",
            f"- 점수 완전 일치: {fmt(exact, pct=True)}",
            f"- 점수 평균 차이(MAE): {mae:.1f}점 (평균 {name_a} {sum(sa) / n:.1f}점 / {name_b} {sum(sb) / n:.1f}점)",
            f"- 점수 상관계수: {fmt(c['corr'])}",
        ]
    else:
        lines += ["", "- 점수: 네 항목이 양쪽 모두 채워진 기사가 없어 계산 불가"]
    return lines, c


def summary_table(labels: dict[str, dict], preds: dict, ai_name: str) -> list[str]:
    """사람끼리 평균 κ vs AI-사람 평균 κ (항목별 + 점수 상관계수)"""
    def mean(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    human = [compare(la, lb) for (_, la), (_, lb) in itertools.combinations(labels.items(), 2)]
    ai = [compare(lab, preds) for lab in labels.values()]
    lines = [
        f"| 항목 | 사람끼리 평균 κ | {ai_name}-사람 평균 κ | 차이 |",
        "|---|---|---|---|",
    ]
    rows = [(label, [c["items"][key]["kappa"] for c in human], [c["items"][key]["kappa"] for c in ai]) for key, label in CRITERIA]
    rows.append(("점수 상관계수", [c["corr"] for c in human], [c["corr"] for c in ai]))
    for label, hs, as_ in rows:
        h, a = mean(hs), mean(as_)
        diff = f"{a - h:+.2f}" if h is not None and a is not None else "-"
        lines.append(f"| {label} | {fmt(h)} | {fmt(a)} | {diff} |")
    lines += ["", "계산할 수 없는 쌍(한쪽이 전부 빈칸이거나 한 값만 쓴 경우)은 평균에서 뺐습니다."]
    return lines


def write_diff(path: Path, labels: dict[str, dict], preds: dict, info: dict) -> int:
    """사람과 AI 판정이 다른 경우 목록 (발표용 오판 사례 찾기)"""
    rows = []
    for name, lab in labels.items():
        for art_id in sorted(set(lab) & set(preds)):
            for key, label in CRITERIA:
                if None in (lab[art_id][key], preds[art_id][key]):
                    continue
                if lab[art_id][key] != preds[art_id][key]:
                    rows.append([
                        name, art_id, info.get(art_id, {}).get("title", ""), label,
                        "O" if lab[art_id][key] else "X", "O" if preds[art_id][key] else "X",
                        info.get(art_id, {}).get("phrases", "") if key == "exaggeration" else "",
                    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["labeler", "article_id", "title", "항목", "사람", "AI", "AI가 뽑은 과장 문구"])
        writer.writerows(rows)
    return len(rows)


async def _main(label_paths: list[Path], preds_path: Path | None, out: Path | None, diff: Path | None):
    labels = {}
    for p in label_paths:
        lab, incomplete = read_labels(p)
        labels[p.stem] = lab
        note = f" (빈칸 있는 행 {incomplete}건: 빈 항목만 비교에서 제외)" if incomplete else ""
        print(f"{p.name}: 라벨 {len(lab)}건{note}")

    if preds_path:
        preds, info = load_csv_preds(preds_path)
        ai_name = preds_path.stem
    else:
        preds, info = await load_db_preds()
        await engine.dispose()
        ai_name = "AI(DB)"

    report = [f"# AI 판정 검증 결과 ({ai_name})", ""]
    if len(labels) >= 2:
        report += [
            "## 종합: 사람끼리 vs AI-사람",
            "",
            "AI-사람 κ가 사람끼리 κ에 가까울수록 AI가 사람 한 명만큼 판단한다는 뜻입니다.",
            "",
        ] + summary_table(labels, preds, ai_name) + [""]

    for name, lab in labels.items():
        lines, c = compare_table(lab, preds, name, ai_name)
        report += [f"## {name} vs {ai_name} (기사 {c['common']}건)", ""] + lines + [""]

    if len(labels) >= 2:
        report += ["## 사람끼리 일치도 (기준선)", "", "AI와 사람의 일치도가 이 값에 가까울수록 AI가 사람 수준으로 판단한다는 뜻입니다.", ""]
        for (na, la), (nb, lb) in itertools.combinations(labels.items(), 2):
            lines, c = compare_table(la, lb, na, nb)
            report += [f"### {na} vs {nb} (기사 {c['common']}건)", ""] + lines + [""]

    text = "\n".join(report)
    print("\n" + text)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        print(f"결과 저장 → {out}")
    if diff:
        n = write_diff(diff, labels, preds, info)
        print(f"불일치 {n}건 저장 → {diff}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="사람 라벨 vs AI 판정 일치도")
    parser.add_argument("labels", type=Path, nargs="+", help="라벨링한 CSV (여러 개 가능)")
    parser.add_argument("--preds", type=Path, default=None, help="비교할 AI 판정 CSV (기본: DB에 저장된 판정)")
    parser.add_argument("--out", type=Path, default=None, help="결과를 마크다운으로 저장할 경로")
    parser.add_argument("--diff", type=Path, default=None, help="사람과 AI가 다르게 판정한 목록 CSV 경로")
    args = parser.parse_args()
    asyncio.run(_main(args.labels, args.preds, args.out, args.diff))
