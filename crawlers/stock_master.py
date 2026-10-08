"""
종목 마스터 수집기 (crawlers/stock_master.py)

네이버 모바일 증권 API에서 종목코드/종목명/시장(KOSPI·KOSDAQ)을 받아 stocks 테이블에 저장합니다.

[실행]  python -m crawlers.stock_master
        python -m crawlers.stock_master --kosdaq-limit 300   (코스닥은 시가총액 상위 N개만)
"""

import argparse
import asyncio
import logging

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.storage import AsyncSessionLocal, StockModel, engine, init_db

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Referer": "https://m.stock.naver.com/",
}
API_URL = "https://m.stock.naver.com/api/stocks/marketValue/{market}"
PAGE_SIZE = 100  # 서버가 허용하는 최대값(200 이상은 오류)


async def fetch_market(client: httpx.AsyncClient, market: str, limit: int | None = None) -> list[dict]:
    """한 시장의 종목을 시가총액 순으로 수집합니다. ETF/ETN은 제외하고 일반 주식만 담습니다."""
    stocks: list[dict] = []
    page = 1
    while True:
        res = await client.get(API_URL.format(market=market), params={"page": page, "pageSize": PAGE_SIZE})
        res.raise_for_status()
        items = res.json().get("stocks", [])
        if not items:
            break

        for item in items:
            if item.get("stockEndType") != "stock":
                continue
            stocks.append({
                "stock_code": item["itemCode"],
                "stock_name": item["stockName"].strip(),
                "market": market,
            })
            if limit and len(stocks) >= limit:
                return stocks

        page += 1
        await asyncio.sleep(0.2)

    return stocks


async def save_stocks(stocks: list[dict]) -> None:
    """종목코드 기준 upsert. 이미 있으면 이름과 시장을 최신 값으로 갱신합니다."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = pg_insert(StockModel)
            stmt = stmt.on_conflict_do_update(
                index_elements=["stock_code"],
                set_={"stock_name": stmt.excluded.stock_name, "market": stmt.excluded.market},
            )
            await session.execute(stmt, stocks)


async def build_stock_master(kosdaq_limit: int | None = 500) -> int:
    async with httpx.AsyncClient(headers=HEADERS, timeout=15.0) as client:
        kospi = await fetch_market(client, "KOSPI")
        kosdaq = await fetch_market(client, "KOSDAQ", limit=kosdaq_limit)

    stocks = kospi + kosdaq
    logger.info("종목 수집 완료 | KOSPI %d개, KOSDAQ %d개", len(kospi), len(kosdaq))
    await save_stocks(stocks)
    return len(stocks)


async def _main(kosdaq_limit: int | None):
    await init_db()
    total = await build_stock_master(kosdaq_limit)
    print(f"stocks 테이블에 {total}개 종목 저장 완료")
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="종목 마스터 수집")
    parser.add_argument("--kosdaq-limit", type=int, default=500, help="코스닥 시가총액 상위 N개 (0이면 전체, 기본 500)")
    args = parser.parse_args()
    asyncio.run(_main(args.kosdaq_limit or None))
