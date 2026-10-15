"""
evaluation/common.py - 라벨링 평가 공통 함수 (CSV 읽기, 참/거짓 해석, Cohen's kappa)
"""

import csv
from pathlib import Path

from analyzer import LLMAnalyzer

# (컬럼명, 표시 이름) — analyzer.py의 평가 항목과 같은 키
CRITERIA = [
    ("exaggeration", "과장 표현"),
    ("investment_basis", "투자 근거"),
    ("data_based", "데이터 부합"),
    ("risk_explanation", "리스크 설명"),
]

# 엑셀에서 편하게 입력하도록 여러 표기를 허용
TRUE_WORDS = {"1", "o", "y", "yes", "true", "t", "참", "예", "ㅇ"}
FALSE_WORDS = {"0", "x", "n", "no", "false", "f", "거짓", "아니오", "ㄴ"}

_scorer = LLMAnalyzer()


def parse_bool(value) -> bool | None:
    """'1', 'O', 'X', 'true' 등을 True/False로. 비어 있으면 None"""
    v = str(value or "").strip().lower()
    if not v:
        return None
    if v in TRUE_WORDS:
        return True
    if v in FALSE_WORDS:
        return False
    raise ValueError(f"참/거짓으로 해석할 수 없는 값: {value!r}")


def score_of(labels: dict) -> int:
    """analyzer.py와 같은 공식으로 점수 계산 (사람 라벨에도 똑같이 적용)"""
    return _scorer.calculate_score(labels)


def read_csv_rows(path: str | Path) -> list[dict]:
    """CSV를 행 목록으로 읽음. 엑셀에서 저장하면 CP949(ANSI)로 바뀌는 경우가 있어 둘 다 시도"""
    with open(path, "rb") as f:
        if f.read(2) == b"PK":
            raise ValueError(
                f"{path}: 확장자는 .csv인데 실제로는 엑셀 통합 문서(.xlsx)로 저장된 파일입니다. "
                "엑셀에서 [다른 이름으로 저장] → 'CSV UTF-8(쉼표로 분리)'로 다시 저장하세요."
            )
    for encoding in ("utf-8-sig", "cp949"):
        try:
            with open(path, encoding=encoding, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    raise ValueError(f"{path}: UTF-8, CP949 어느 쪽으로도 읽을 수 없는 파일입니다.")


def read_labels(path: str | Path) -> tuple[dict[int, dict[str, bool | None]], int]:
    """
    라벨 CSV → ({article_id: {항목: bool 또는 None(빈칸)}}, 빈칸이 있는 행 수)
    빈칸은 그 항목 비교에서만 빠지고, 네 항목이 모두 빈 행은 아예 제외합니다.
    """
    labels: dict[int, dict[str, bool | None]] = {}
    incomplete = 0
    for line_no, row in enumerate(read_csv_rows(path), start=2):
        try:
            values = {key: parse_bool(row.get(key)) for key, _ in CRITERIA}
        except ValueError as e:
            raise ValueError(f"{path} {line_no}행: {e}") from None
        if any(v is None for v in values.values()):
            incomplete += 1
        if all(v is None for v in values.values()):
            continue
        labels[int(row["article_id"])] = values
    return labels, incomplete


def is_complete(values: dict) -> bool:
    return all(values.get(key) is not None for key, _ in CRITERIA)


def cohen_kappa(a: list[bool], b: list[bool]) -> float | None:
    """두 평가자의 참/거짓 판정 일치도 (우연히 맞을 확률을 뺀 일치율)"""
    n = len(a)
    if n == 0:
        return None
    po = sum(x == y for x, y in zip(a, b)) / n
    pa, pb = sum(a) / n, sum(b) / n
    pe = pa * pb + (1 - pa) * (1 - pb)
    if pe == 1:
        # 둘 다 한쪽 값만 썼으면 kappa를 정의할 수 없음
        return None
    return (po - pe) / (1 - pe)


def kappa_label(k: float | None) -> str:
    """Landis & Koch (1977) 해석 기준"""
    if k is None:
        return "계산 불가"
    if k < 0:
        return "우연보다 낮음"
    if k <= 0.2:
        return "약간"
    if k <= 0.4:
        return "보통 이하"
    if k <= 0.6:
        return "보통"
    if k <= 0.8:
        return "상당"
    return "거의 완벽"
