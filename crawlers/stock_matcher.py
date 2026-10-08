"""
기사 ↔ 종목 매칭기 (crawlers/stock_matcher.py)

stocks 테이블의 종목명을 사전으로 삼아 기사 제목/본문에서 언급된 종목을 찾습니다.

[규칙]
- 긴 종목명을 먼저 매칭 ("삼성전자우" 가 "삼성전자" 로, "SK하이닉스" 가 "SK" 로 잘못 잡히지 않게)
- 앞 글자가 한글/영문/숫자면 단어 일부로 보고 제외, 영문 종목명은 뒤에 영문/숫자가 붙어도 제외
- 제목에 나온 종목은 바로 인정, 본문에만 나온 종목은 2회 이상 언급돼야 인정
- 이름이 2글자 이하인 종목(LG, SK, KT, 두산 ...)은 제목에 있을 때만 인정
- 결과는 제목 언급 > 언급 횟수 순으로 정렬 (첫 번째 = 대표 종목)

[백필]  python -m crawlers.stock_matcher --backfill   (이미 저장된 기사에 종목 연결)
"""

import argparse
import asyncio
import logging
import re
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)

MAX_STOCKS_PER_ARTICLE = 5
BODY_LIMIT = 3000          # 본문은 앞부분만 검사
BODY_MIN_COUNT = 2         # 본문에만 나온 종목이 인정되려면 필요한 언급 횟수
SHORT_NAME_LEN = 2         # 이 길이 이하의 종목명은 제목에서만 인정

# 일반 명사와 겹쳐 오탐이 많은 종목명 (매칭 제외)
AMBIGUOUS_NAMES = {"대상"}

# 약칭 → 정식 종목명 (네이버/카카오 등 기사 출처와 겹치는 이름은 넣지 않음)
ALIASES = {
    "삼전": "삼성전자",
    "하닉": "SK하이닉스",
}


def _is_ascii_end(name: str) -> bool:
    return name[-1].isascii() and name[-1].isalnum()


class StockMatcher:
    def __init__(self):
        self._pattern: Optional[re.Pattern] = None
        self._name_to_code: dict[str, str] = {}

    @property
    def ready(self) -> bool:
        return self._pattern is not None

    def build(self, stocks: list[tuple[str, str]]) -> None:
        """stocks: [(stock_code, stock_name), ...]"""
        name_to_code = {name: code for code, name in stocks if name and name not in AMBIGUOUS_NAMES}
        for alias, real in ALIASES.items():
            if real in name_to_code:
                name_to_code[alias] = name_to_code[real]

        self._name_to_code = name_to_code
        if not name_to_code:
            self._pattern = None
            return

        alts = []
        for name in sorted(name_to_code, key=len, reverse=True):
            alt = re.escape(name)
            if _is_ascii_end(name):
                alt += r"(?![A-Za-z0-9])"
            alts.append(alt)
        self._pattern = re.compile(r"(?<![가-힣A-Za-z0-9])(?:" + "|".join(alts) + ")")

    async def load(self) -> int:
        """DB의 stocks 테이블로 사전을 만듭니다. 비어 있으면 종목 마스터를 먼저 채웁니다."""
        from api.storage import AsyncSessionLocal, StockModel

        async def _fetch():
            async with AsyncSessionLocal() as session:
                rows = await session.execute(select(StockModel.stock_code, StockModel.stock_name))
                return [(c, n) for c, n in rows.all()]

        stocks = await _fetch()
        if not stocks:
            from crawlers.stock_master import build_stock_master
            logger.info("stocks 테이블이 비어 있어 종목 마스터를 먼저 수집합니다...")
            await build_stock_master()
            stocks = await _fetch()

        self.build(stocks)
        return len(stocks)

    def match(self, title: str, content: str = "") -> list[str]:
        """기사에서 언급된 종목코드 리스트 (대표 종목 먼저)"""
        if not self._pattern:
            return []

        scores: dict[str, int] = {}

        for m in self._pattern.finditer(title or ""):
            code = self._name_to_code[m.group(0)]
            scores[code] = scores.get(code, 0) + 10

        body_counts: dict[str, int] = {}
        for m in self._pattern.finditer((content or "")[:BODY_LIMIT]):
            name = m.group(0)
            if len(name) <= SHORT_NAME_LEN:
                continue
            code = self._name_to_code[name]
            body_counts[code] = body_counts.get(code, 0) + 1

        for code, cnt in body_counts.items():
            if code in scores or cnt >= BODY_MIN_COUNT:
                scores[code] = scores.get(code, 0) + cnt

        ranked = sorted(scores, key=lambda c: scores[c], reverse=True)
        return ranked[:MAX_STOCKS_PER_ARTICLE]


async def backfill() -> tuple[int, int]:
    """DB에 이미 저장된 기사에 종목을 연결합니다. (처리한 기사 수, 연결 수)"""
    from api.storage import ArticleModel, ArticleStockModel, AsyncSessionLocal

    matcher = StockMatcher()
    await matcher.load()

    links = 0
    async with AsyncSessionLocal() as session:
        async with session.begin():
            rows = (await session.execute(
                select(ArticleModel.id, ArticleModel.title, ArticleModel.content)
            )).all()
            for art_id, title, content in rows:
                for rank, code in enumerate(matcher.match(title, content)):
                    stmt = pg_insert(ArticleStockModel).values(article_id=art_id, stock_code=code, rank=rank)
                    await session.execute(stmt.on_conflict_do_update(
                        index_elements=["article_id", "stock_code"],
                        set_={"rank": stmt.excluded.rank},
                    ))
                    links += 1
    return len(rows), links


async def _main():
    from api.storage import engine, init_db
    await init_db()
    articles, links = await backfill()
    print(f"기사 {articles}건 처리, 종목 연결 {links}건")
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="기사-종목 매칭")
    parser.add_argument("--backfill", action="store_true", help="DB에 저장된 기사에 종목 연결")
    args = parser.parse_args()
    if args.backfill:
        asyncio.run(_main())
    else:
        parser.print_help()
