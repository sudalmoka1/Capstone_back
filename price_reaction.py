"""
price_reaction.py - 기사 발행 후 실제 주가 반응 계산 (AI 점수 검증용)

- 기준가:   기사 발행 직전의 마지막 종가
            (장 마감 15:30 이후 발행이면 당일 종가, 그 전이면 전 거래일 종가)
- 1일/5일:  기준일 다음 1번째 / 5번째 거래일 종가의 수익률 (%)
- 초과 수익률: 같은 기간 시장지수(코스피 ^KS11 / 코스닥 ^KQ11) 수익률을 뺀 값 (%p)
- 대상:     LLM 분석이 끝난(COMPLETED) 기사의 대표 종목. 5일 수익률이 아직 없으면 다음 실행 때 다시 계산

[단독 실행] python price_reaction.py   (밀린 기사 전부 계산)
[자동 실행] scheduler.py가 주기적으로 호출
"""

import asyncio
import logging
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticlePriceReactionModel,
    ArticleStockModel,
    AsyncSessionLocal,
    StockModel,
)

logger = logging.getLogger(__name__)

MARKET_CLOSE = dtime(15, 30)    # 이 시각 이후 발행된 기사는 당일 종가를 기준가로 사용
CLOSE_SETTLED = dtime(16, 0)    # 이 시각 전에는 오늘 일봉(장중 미확정 가격)을 계산에서 제외
LOOKBACK_DAYS = 60              # 이보다 오래된 기사는 계산 대상에서 제외
MAX_BASE_GAP_DAYS = 7           # 기준가가 발행일보다 이만큼 이상 앞서면(거래정지 등) 계산하지 않음
DOWNLOAD_CHUNK = 40             # yfinance 한 번에 받을 종목 수
INDEX_KOSPI, INDEX_KOSDAQ = "^KS11", "^KQ11"


def _stock_ticker(code: str, market: str | None) -> str:
    return f"{code}.KQ" if market == "KOSDAQ" else f"{code}.KS"


def _index_ticker(market: str | None) -> str:
    return INDEX_KOSDAQ if market == "KOSDAQ" else INDEX_KOSPI


def _download_closes(tickers: list[str], start: date, end: date) -> dict[str, pd.Series]:
    """여러 종목의 일별 종가를 묶어서 받아 {ticker: 종가 Series(index=date)}로 반환"""
    today = date.today()
    drop_today = datetime.now().time() < CLOSE_SETTLED
    result: dict[str, pd.Series] = {}

    for i in range(0, len(tickers), DOWNLOAD_CHUNK):
        chunk = tickers[i:i + DOWNLOAD_CHUNK]
        try:
            df = yf.download(
                chunk, start=start, end=end, auto_adjust=False,
                group_by="ticker", progress=False, threads=True,
            )
        except Exception as e:
            logger.warning("주가 일괄 조회 실패 (%d종목): %s", len(chunk), e)
            continue

        for t in chunk:
            try:
                closes = df[t]["Close"] if isinstance(df.columns, pd.MultiIndex) else df["Close"]
            except KeyError:
                continue
            closes = closes.dropna()
            closes.index = [ts.date() for ts in closes.index]
            if drop_today:
                closes = closes[closes.index != today]
            if not closes.empty:
                result[t] = closes

    return result


def _pct(after: float, before: float) -> float:
    return round((float(after) / float(before) - 1) * 100, 2)


def calc_reaction(published_at: datetime, closes: pd.Series, index_closes: pd.Series | None) -> dict | None:
    """기사 1건의 기준일과 1일/5일 (초과)수익률 계산. 기준가를 정할 수 없으면 None"""
    pub_day = published_at.date()
    after_close = published_at.time() >= MARKET_CLOSE
    dates = list(closes.index)

    before = [d for d in dates if d < pub_day or (after_close and d == pub_day)]
    if not before:
        return None
    base = before[-1]
    if (pub_day - base).days > MAX_BASE_GAP_DAYS:
        return None
    later = [d for d in dates if d > base]

    def index_close_on(d: date):
        if index_closes is None:
            return None
        upto = index_closes[index_closes.index <= d]  # 지수에 그날 데이터가 없으면 직전 값 사용
        return None if upto.empty else upto.iloc[-1]

    out = {"base_date": base}
    for n in (1, 5):
        ret = excess = None
        if len(later) >= n:
            target = later[n - 1]
            ret = _pct(closes[target], closes[base])
            idx_base, idx_target = index_close_on(base), index_close_on(target)
            if idx_base is not None and idx_target is not None:
                excess = round(ret - _pct(idx_target, idx_base), 2)
        out[f"ret_{n}d"] = ret
        out[f"excess_{n}d"] = excess
    return out


async def update_price_reactions() -> int:
    """5일 수익률이 아직 없는 분석 완료 기사들의 주가 반응을 계산해 저장. 저장한 기사 수를 반환"""
    since = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(ArticleModel.id, ArticleModel.published_at, StockModel.stock_code, StockModel.market)
            .join(ArticleStockModel, (ArticleStockModel.article_id == ArticleModel.id) & (ArticleStockModel.rank == 0))
            .join(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
            .join(
                ArticleEvaluationModel,
                (ArticleEvaluationModel.article_id == ArticleModel.id) & (ArticleEvaluationModel.status == "COMPLETED"),
            )
            .outerjoin(ArticlePriceReactionModel, ArticlePriceReactionModel.article_id == ArticleModel.id)
            .where(ArticleModel.published_at.is_not(None), ArticleModel.published_at >= since)
            .where(or_(ArticlePriceReactionModel.article_id.is_(None), ArticlePriceReactionModel.ret_5d.is_(None)))
        )
        targets = (await session.execute(stmt)).all()

    if not targets:
        logger.info("주가 반응 계산 대상 기사가 없습니다.")
        return 0

    tickers = {_stock_ticker(code, market) for _, _, code, market in targets} | {INDEX_KOSPI, INDEX_KOSDAQ}
    start = min(pub for _, pub, _, _ in targets).date() - timedelta(days=MAX_BASE_GAP_DAYS + 7)
    end = date.today() + timedelta(days=1)
    logger.info("주가 반응 계산 시작: 기사 %d건, 종목 %d개", len(targets), len(tickers) - 2)

    closes = await asyncio.to_thread(_download_closes, sorted(tickers), start, end)

    values = []
    for art_id, pub, code, market in targets:
        stock_closes = closes.get(_stock_ticker(code, market))
        if stock_closes is None:
            continue
        reaction = calc_reaction(pub, stock_closes, closes.get(_index_ticker(market)))
        if reaction:
            values.append({"article_id": art_id, "stock_code": code, **reaction})

    if values:
        async with AsyncSessionLocal() as session:
            stmt = pg_insert(ArticlePriceReactionModel).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[ArticlePriceReactionModel.article_id],
                set_={
                    col: stmt.excluded[col]
                    for col in ("stock_code", "base_date", "ret_1d", "ret_5d", "excess_1d", "excess_5d")
                },
            )
            await session.execute(stmt)
            await session.commit()

    done_5d = sum(1 for v in values if v["ret_5d"] is not None)
    logger.info("주가 반응 계산 완료: %d건 저장 (5일 수익률까지 확정 %d건)", len(values), done_5d)
    return len(values)


if __name__ == "__main__":
    from api.storage import engine, init_db

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    async def _main():
        await init_db()
        await update_price_reactions()
        await engine.dispose()

    asyncio.run(_main())
