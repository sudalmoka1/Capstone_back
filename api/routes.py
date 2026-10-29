"""
FastAPI 라우터 - 증권 뉴스 크롤링 및 DB 조회 API
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select

from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticlePriceReactionModel,
    ArticleStockModel,
    AsyncSessionLocal,
    PublisherModel,
    SchedulerStatusModel,
    StockModel,
)
from crawlers.manager import SecuritiesNewsCrawlerManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["뉴스 크롤링 및 조회"])

manager = SecuritiesNewsCrawlerManager(max_pages=1)


# ── 응답 스키마 ───────────────────────────────────────────────
class ArticleOut(BaseModel):
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[str]
    related_stocks: list[str]


class CrawlResult(BaseModel):
    total: int
    sources_used: list[str]
    articles: list[ArticleOut]


class EvaluationOut(BaseModel):
    status: str
    score: int
    report: Optional[Dict[str, Any]] = None


class StockOut(BaseModel):
    code: str
    name: str
    market: Optional[str] = None
    rank: int = 0  # 0 = 대표 종목


class ReactionOut(BaseModel):
    """기사 발행 후 대표 종목의 주가 반응 (%). 아직 해당 거래일이 지나지 않았으면 None"""
    stock_code: str
    base_date: str
    ret_1d: Optional[float] = None
    ret_5d: Optional[float] = None
    excess_1d: Optional[float] = None
    excess_5d: Optional[float] = None


class ArticleDBOut(BaseModel):
    id: int
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[str]
    # 기사 본문은 저작권/약관 문제로 응답에 넣지 않음 (분석에만 내부적으로 사용)
    evaluation: Optional[EvaluationOut] = None
    stocks: list[StockOut] = []
    reaction: Optional[ReactionOut] = None

    class Config:
        from_attributes = True


class StockRankingOut(BaseModel):
    rank: int                       # 순위 (1부터)
    stock_code: str
    stock_name: str
    market: Optional[str]
    mentions: int                   # 기사에서 언급된 횟수(기사 수)
    headline_mentions: int          # 그중 대표 종목으로 잡힌 기사 수
    analyzed: int                   # LLM 분석이 끝난 기사 수
    avg_score: Optional[float]      # 분석 완료 기사의 평균 신뢰도 (없으면 None)


class CandleOut(BaseModel):
    time: str       # YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: int


class StockChartOut(BaseModel):
    stock_code: str
    stock_name: Optional[str]
    market: Optional[str]
    ticker: str
    candles: list[CandleOut]


# ── 엔드포인트 ────────────────────────────────────────────────

@router.get("/news/securities", response_model=CrawlResult)
async def get_securities_news(
    sources: Optional[list[str]] = Query(default=None, description="언론사 코드(oid) 또는 이름. 비우면 전체"),
    fetch_content: bool = Query(default=True, description="본문 크롤링 여부"),
    limit: int = Query(default=50, le=200),
):
    """실시간 증권 뉴스 수집 엔드포인트"""
    articles = await manager.run(sources=sources, fetch_content=fetch_content)
    articles = articles[:limit]

    return CrawlResult(
        total=len(articles),
        sources_used=sources or [p.name for p in manager.presses],
        articles=[
            ArticleOut(
                title=a.title,
                url=a.url,
                source=a.source,
                publisher=a.publisher,
                published_at=a.published_at.isoformat() if a.published_at else None,
                related_stocks=a.related_stocks,
            )
            for a in articles
        ],
    )


@router.get("/articles", response_model=list[ArticleDBOut])
async def get_db_articles(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0, description="건너뛸 기사 수 (페이지네이션)"),
    source: Optional[str] = Query(default=None, description="네이버금융 / 한국경제 필터"),
    stock_code: Optional[str] = Query(default=None, description="특정 종목코드가 연결된 기사만 조회"),
    publisher: Optional[str] = Query(default=None, description="언론사 이름 필터 (예: 이데일리)"),
    stock_only: bool = Query(default=True, description="종목이 언급된 기사(주식 뉴스)만 조회"),
):
    """
    DB에 적재된 기사, 언론사 정보(Publisher), LLM 평가 결과를 함께 조회
    """
    async with AsyncSessionLocal() as session:
        # 💡 [핵심] ArticleModel + ArticleEvaluationModel + PublisherModel 3개 테이블 조인
        stmt = (
            select(ArticleModel, ArticleEvaluationModel, PublisherModel.name)
            .outerjoin(ArticleEvaluationModel, ArticleModel.id == ArticleEvaluationModel.article_id)
            .outerjoin(PublisherModel, ArticleModel.publisher_id == PublisherModel.id)
            # 발행 시각 최신순 (id는 저장 순서라 한 번에 수집된 기사끼리는 순서가 뒤집힘)
            # 시각이 같으면 id로 순서를 고정해 페이지네이션 결과가 흔들리지 않게 함
            .order_by(ArticleModel.published_at.desc().nulls_last(), ArticleModel.id.desc())
        )

        if source and source != "전체":
            stmt = stmt.where(ArticleModel.source == source)

        if publisher and publisher != "전체":
            stmt = stmt.where(PublisherModel.name == publisher)

        if stock_code:
            stmt = stmt.where(
                ArticleModel.id.in_(
                    select(ArticleStockModel.article_id).where(ArticleStockModel.stock_code == stock_code)
                )
            )
        elif stock_only:
            # 종목 연결이 하나도 없는 기사(주식과 무관한 경제 뉴스)는 제외
            stmt = stmt.where(ArticleModel.id.in_(select(ArticleStockModel.article_id)))

        stmt = stmt.limit(limit).offset(offset)

        result = await session.execute(stmt)
        rows = result.all()  # (ArticleModel, ArticleEvaluationModel, publisher_name) 튜플 반환

        # 기사별 연결 종목 (대표 종목이 앞에 오도록 rank 순)
        stocks_by_article: dict[int, list[StockOut]] = {}
        article_ids = [article.id for article, _, _ in rows]
        if article_ids:
            stock_rows = await session.execute(
                select(
                    ArticleStockModel.article_id,
                    StockModel.stock_code,
                    StockModel.stock_name,
                    StockModel.market,
                    ArticleStockModel.rank,
                )
                .join(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
                .where(ArticleStockModel.article_id.in_(article_ids))
                .order_by(ArticleStockModel.article_id, ArticleStockModel.rank)
            )
            for art_id, code, name, market, rank in stock_rows.all():
                stocks_by_article.setdefault(art_id, []).append(
                    StockOut(code=code, name=name, market=market, rank=rank)
                )

        # 기사별 주가 반응 (price_reaction.py가 계산해 둔 값)
        reaction_by_article: dict[int, ReactionOut] = {}
        if article_ids:
            reaction_rows = await session.execute(
                select(ArticlePriceReactionModel).where(ArticlePriceReactionModel.article_id.in_(article_ids))
            )
            for r in reaction_rows.scalars():
                reaction_by_article[r.article_id] = ReactionOut(
                    stock_code=r.stock_code,
                    base_date=r.base_date.isoformat(),
                    ret_1d=r.ret_1d, ret_5d=r.ret_5d,
                    excess_1d=r.excess_1d, excess_5d=r.excess_5d,
                )

        output = []
        for article, evaluation, pub_name in rows:
            eval_data = None
            if evaluation:
                parsed_report = None
                if evaluation.report:
                    try:
                        parsed_report = (
                            json.loads(evaluation.report)
                            if isinstance(evaluation.report, str)
                            else evaluation.report
                        )
                    except Exception:
                        parsed_report = {"summary": str(evaluation.report)}

                eval_data = EvaluationOut(
                    status=evaluation.status,
                    score=evaluation.score or 0,
                    report=parsed_report
                )

            # 💡 조인으로 추출한 언론사 이름 매핑 (없으면 source 이름 적용)
            publisher_display = pub_name or article.source or "언론사 미지정"

            output.append(
                ArticleDBOut(
                    id=article.id,
                    title=article.title,
                    url=article.url,
                    source=article.source,
                    publisher=publisher_display,
                    published_at=article.published_at.isoformat() if article.published_at else None,
                    evaluation=eval_data,
                    stocks=stocks_by_article.get(article.id, []),
                    reaction=reaction_by_article.get(article.id),
                )
            )

        return output


class PublisherOut(BaseModel):
    name: str
    count: int   # DB에 저장된 해당 언론사 기사 수


@router.get("/publishers", response_model=list[PublisherOut])
async def get_publishers():
    """수집 대상 언론사 목록과 저장된 기사 수 (프론트 필터 탭용). 설정 순서를 유지합니다."""
    names = [p.name for p in manager.presses]
    async with AsyncSessionLocal() as session:
        rows = await session.execute(
            select(PublisherModel.name, func.count(ArticleModel.id))
            .join(ArticleModel, ArticleModel.publisher_id == PublisherModel.id)
            .where(PublisherModel.name.in_(names))
            .where(ArticleModel.id.in_(select(ArticleStockModel.article_id)))  # 주식 뉴스만 집계
            .group_by(PublisherModel.name)
        )
        counts = dict(rows.all())
    return [PublisherOut(name=n, count=counts.get(n, 0)) for n in names]


# ── 언론사별 기사 신뢰도 ──────────────────────────────────────
# 기사 수가 적은 언론사는 평균이 우연에 크게 흔들리므로 순위에서 제외
PUBLISHER_MIN_ARTICLES = 30


class PublisherStatOut(BaseModel):
    rank: Optional[int]                 # 순위 (기사 수 부족이면 None)
    name: str
    analyzed: int                       # 분석 완료 기사 수
    avg_score: float                    # 평균 신뢰도
    exaggeration_pct: float             # 과장 표현 감지 비율 (%)
    basis_pct: float                    # 구체적 투자 근거 제시 비율 (%)
    data_pct: float                     # 시장 데이터 부합 비율 (%)
    risk_pct: float                     # 리스크 언급 비율 (%)


class PublisherRankingOut(BaseModel):
    days: int
    min_articles: int
    ranked: list[PublisherStatOut]      # 기사 수 충분, 평균 신뢰도 순
    insufficient: list[PublisherStatOut]  # 기사 수 부족 (순위 없음, 기사 수 순)


@router.get("/publishers/ranking", response_model=PublisherRankingOut)
async def get_publisher_ranking(
    days: int = Query(default=90, ge=1, le=365, description="최근 N일 발행 기사 기준"),
):
    """
    언론사별 AI 판정 집계 (분석 완료된 주식 기사 기준)
    - 순위는 참고용이며 기사 점수에는 반영하지 않음
    """
    since = datetime.now() - timedelta(days=days)
    article_time = func.coalesce(ArticleModel.published_at, ArticleModel.created_at)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PublisherModel.name, ArticleEvaluationModel.score, ArticleEvaluationModel.report)
            .join(ArticleModel, ArticleModel.publisher_id == PublisherModel.id)
            .join(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticleModel.id)
            .where(ArticleEvaluationModel.status == "COMPLETED")
            .where(article_time >= since)
            .where(ArticleModel.id.in_(select(ArticleStockModel.article_id)))  # 주식 뉴스만 집계
        )
        by_publisher: dict[str, list[tuple[int, dict]]] = {}
        for name, score, report in result.all():
            try:
                parsed = json.loads(report) if isinstance(report, str) else (report or {})
            except Exception:
                parsed = {}
            by_publisher.setdefault(name, []).append((score or 0, parsed))

    def pct(items: list[tuple[int, dict]], key: str) -> float:
        return round(sum(1 for _, r in items if r.get(key)) / len(items) * 100, 1)

    stats = [
        PublisherStatOut(
            rank=None,
            name=name,
            analyzed=len(items),
            avg_score=round(sum(s for s, _ in items) / len(items), 1),
            exaggeration_pct=pct(items, "exaggeration"),
            basis_pct=pct(items, "investment_basis"),
            data_pct=pct(items, "data_based"),
            risk_pct=pct(items, "risk_explanation"),
        )
        for name, items in by_publisher.items()
    ]

    ranked = sorted(
        (s for s in stats if s.analyzed >= PUBLISHER_MIN_ARTICLES),
        key=lambda s: (-s.avg_score, -s.analyzed, s.name),
    )
    for i, s in enumerate(ranked, 1):
        s.rank = i
    insufficient = sorted(
        (s for s in stats if s.analyzed < PUBLISHER_MIN_ARTICLES),
        key=lambda s: (-s.analyzed, s.name),
    )
    return PublisherRankingOut(days=days, min_articles=PUBLISHER_MIN_ARTICLES, ranked=ranked, insufficient=insufficient)


@router.get("/stocks/ranking", response_model=list[StockRankingOut])
async def get_stock_ranking(
    days: int = Query(default=7, ge=1, le=90, description="최근 N일 기사 기준"),
    limit: int = Query(default=10, ge=1, le=50),
):
    """
    종목별 언급 순위 (스코어보드/그래프용)
    - 언급 횟수 = 해당 종목이 연결된 기사 수
    - 평균 신뢰도 = LLM 분석이 완료된(COMPLETED) 기사들의 평균 점수
    """
    since = datetime.now() - timedelta(days=days)
    article_time = func.coalesce(ArticleModel.published_at, ArticleModel.created_at)
    completed = ArticleEvaluationModel.status == "COMPLETED"

    mentions = func.count(ArticleStockModel.article_id)
    headline = func.count(ArticleStockModel.article_id).filter(ArticleStockModel.rank == 0)

    stmt = (
        select(
            StockModel.stock_code,
            StockModel.stock_name,
            StockModel.market,
            mentions.label("mentions"),
            headline.label("headline_mentions"),
            func.count(ArticleEvaluationModel.article_id).filter(completed).label("analyzed"),
            func.avg(ArticleEvaluationModel.score).filter(completed).label("avg_score"),
        )
        .select_from(ArticleStockModel)
        .join(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
        .join(ArticleModel, ArticleModel.id == ArticleStockModel.article_id)
        .outerjoin(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticleModel.id)
        .where(article_time >= since)
        .group_by(StockModel.stock_code, StockModel.stock_name, StockModel.market)
        .order_by(mentions.desc(), headline.desc(), StockModel.stock_name)
        .limit(limit)
    )

    async with AsyncSessionLocal() as session:
        result = await session.execute(stmt)
        return [
            StockRankingOut(
                rank=i,
                stock_code=code,
                stock_name=name,
                market=market,
                mentions=m,
                headline_mentions=h,
                analyzed=a,
                avg_score=round(float(avg), 1) if avg is not None else None,
            )
            for i, (code, name, market, m, h, a, avg) in enumerate(result.all(), 1)
        ]


# ── 주가 차트 ─────────────────────────────────────────────────
CHART_PERIODS = {"1mo", "3mo", "6mo", "1y"}
CHART_CACHE_TTL = 600  # 같은 종목/기간은 10분 동안 야후 재요청 안 함
_chart_cache: dict[tuple[str, str], tuple[float, str, list[CandleOut]]] = {}


def _fetch_candles(ticker: str, period: str) -> list[CandleOut]:
    """야후파이낸스 일봉 조회 (동기 함수 → to_thread로 호출)"""
    df = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=False)
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return [
        CandleOut(
            time=idx.strftime("%Y-%m-%d"),
            open=float(row["Open"]),
            high=float(row["High"]),
            low=float(row["Low"]),
            close=float(row["Close"]),
            volume=int(row["Volume"] or 0),
        )
        for idx, row in df.iterrows()
    ]


@router.get("/stocks/{stock_code}/chart", response_model=StockChartOut)
async def get_stock_chart(
    stock_code: str,
    period: str = Query(default="3mo", description="1mo / 3mo / 6mo / 1y"),
):
    """종목 일봉(OHLCV) 데이터 - 기사 클릭 시 주가 차트용"""
    if period not in CHART_PERIODS:
        raise HTTPException(400, f"period는 {sorted(CHART_PERIODS)} 중 하나여야 합니다.")

    async with AsyncSessionLocal() as session:
        stock = await session.get(StockModel, stock_code)
    name = stock.stock_name if stock else None
    market = stock.market if stock else None

    key = (stock_code, period)
    cached = _chart_cache.get(key)
    if cached and time.time() - cached[0] < CHART_CACHE_TTL:
        _, ticker, candles = cached
    else:
        # 시장 정보가 없거나 틀린 경우를 대비해 다른 시장 접미사도 시도 (코스피 .KS / 코스닥 .KQ)
        suffixes = [".KQ", ".KS"] if market == "KOSDAQ" else [".KS", ".KQ"]
        ticker, candles = f"{stock_code}{suffixes[0]}", []
        for suffix in suffixes:
            try:
                candles = await asyncio.to_thread(_fetch_candles, f"{stock_code}{suffix}", period)
            except Exception as e:
                logger.warning("주가 조회 실패 (%s%s): %s", stock_code, suffix, e)
                candles = []
            if candles:
                ticker = f"{stock_code}{suffix}"
                break
        if candles:
            _chart_cache[key] = (time.time(), ticker, candles)

    if not candles:
        raise HTTPException(404, "주가 데이터를 찾을 수 없습니다.")

    return StockChartOut(stock_code=stock_code, stock_name=name, market=market, ticker=ticker, candles=candles)



# ── 스케줄러 상태 ─────────────────────────────────────────────
HEARTBEAT_TIMEOUT_SECONDS = 180  # scheduler.py가 60초마다 기록하므로 3분 넘게 끊기면 중지로 판단


class SchedulerStatusOut(BaseModel):
    running: bool
    mode: Optional[str]                 # full / analyze-only
    last_crawl_at: Optional[str]
    last_analyze_at: Optional[str]
    pending: int                        # 분석 대기 기사 수 (종목 연결 + 평가 없음/ERROR)


@router.get("/status", response_model=SchedulerStatusOut)
async def get_scheduler_status():
    """헤더의 '스케줄러 가동 중' 표시용 — 마지막 생존 신호 기준으로 실제 가동 여부를 판단"""
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(
                SchedulerStatusModel,
                func.extract("epoch", func.now() - SchedulerStatusModel.heartbeat_at),
            ).where(SchedulerStatusModel.id == 1)
        )).first()

        pending = await session.scalar(
            select(func.count(ArticleModel.id))
            .outerjoin(ArticleEvaluationModel, ArticleModel.id == ArticleEvaluationModel.article_id)
            .where(ArticleModel.id.in_(select(ArticleStockModel.article_id)))
            .where((ArticleEvaluationModel.article_id.is_(None)) | (ArticleEvaluationModel.status == "ERROR"))
        )

    status, age = row if row else (None, None)
    return SchedulerStatusOut(
        running=age is not None and float(age) < HEARTBEAT_TIMEOUT_SECONDS,
        mode=status.mode if status else None,
        last_crawl_at=status.last_crawl_at.isoformat() if status and status.last_crawl_at else None,
        last_analyze_at=status.last_analyze_at.isoformat() if status and status.last_analyze_at else None,
        pending=pending or 0,
    )



# ── AI 점수 vs 실제 주가 반응 ─────────────────────────────────
SCORE_BANDS = [  # (key, 라벨, 최소 점수)
    ("high", "75점 이상", 75),
    ("mid", "50~74점", 50),
    ("low", "50점 미만", 0),
]


class ReactionGroupOut(BaseModel):
    key: str
    label: str
    n_1d: int                           # 1일 수익률이 있는 기사 수
    n_5d: int                           # 5일 수익률이 있는 기사 수
    avg_ret_1d: Optional[float]
    avg_ret_5d: Optional[float]
    avg_excess_1d: Optional[float]      # 시장지수 대비 초과 수익률 평균 (%p)
    avg_excess_5d: Optional[float]
    up_ratio_5d: Optional[float]        # 5일 뒤 시장보다 많이 오른(초과 수익률 > 0) 기사 비율 (%)


class ReactionSummaryOut(BaseModel):
    total: int
    by_score: list[ReactionGroupOut]
    by_exaggeration: list[ReactionGroupOut]
    correlation_5d: Optional[float]     # AI 점수와 5일 초과 수익률의 피어슨 상관계수 (표본 10건 미만이면 None)


def _avg(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 2) if values else None


def _group(key: str, label: str, rows: list[dict]) -> ReactionGroupOut:
    r1 = [r for r in rows if r["ret_1d"] is not None]
    r5 = [r for r in rows if r["ret_5d"] is not None]
    ex1 = [r["excess_1d"] for r in r1 if r["excess_1d"] is not None]
    ex5 = [r["excess_5d"] for r in r5 if r["excess_5d"] is not None]
    return ReactionGroupOut(
        key=key, label=label, n_1d=len(r1), n_5d=len(r5),
        avg_ret_1d=_avg([r["ret_1d"] for r in r1]),
        avg_ret_5d=_avg([r["ret_5d"] for r in r5]),
        avg_excess_1d=_avg(ex1),
        avg_excess_5d=_avg(ex5),
        up_ratio_5d=round(sum(1 for v in ex5 if v > 0) / len(ex5) * 100, 1) if ex5 else None,
    )


def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    n = len(xs)
    if n < 10:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return round(cov / (vx * vy) ** 0.5, 3)


@router.get("/reactions/summary", response_model=ReactionSummaryOut)
async def get_reaction_summary(
    days: int = Query(default=90, ge=1, le=365, description="최근 N일 발행 기사 기준"),
):
    """
    AI 신뢰도 점수 구간별 / 과장 판정 여부별로 기사 발행 후 실제 주가가 어떻게 움직였는지 집계
    (AI 점수가 실제로 의미가 있는지 검증하는 용도)
    """
    since = datetime.now() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ArticlePriceReactionModel, ArticleEvaluationModel.score, ArticleEvaluationModel.report)
            .join(ArticleEvaluationModel, ArticleEvaluationModel.article_id == ArticlePriceReactionModel.article_id)
            .join(ArticleModel, ArticleModel.id == ArticlePriceReactionModel.article_id)
            .where(ArticleEvaluationModel.status == "COMPLETED")
            .where(ArticleModel.published_at >= since)
            # 종목 연결이 끊긴 기사(주식 무관으로 제외된 기사)는 집계에서 뺌
            .where(ArticleModel.id.in_(select(ArticleStockModel.article_id)))
        )
        rows = []
        for reaction, score, report in result.all():
            try:
                parsed = json.loads(report) if isinstance(report, str) else (report or {})
            except Exception:
                parsed = {}
            rows.append({
                "score": score or 0,
                "exaggeration": bool(parsed.get("exaggeration")),
                "ret_1d": reaction.ret_1d, "ret_5d": reaction.ret_5d,
                "excess_1d": reaction.excess_1d, "excess_5d": reaction.excess_5d,
            })

    by_score, assigned = [], set()
    for key, label, min_score in SCORE_BANDS:
        band = [r for i, r in enumerate(rows) if i not in assigned and r["score"] >= min_score]
        assigned |= {i for i, r in enumerate(rows) if i not in assigned and r["score"] >= min_score}
        by_score.append(_group(key, label, band))

    with_5d = [r for r in rows if r["excess_5d"] is not None]
    return ReactionSummaryOut(
        total=len(rows),
        by_score=by_score,
        by_exaggeration=[
            _group("yes", "과장 표현 감지", [r for r in rows if r["exaggeration"]]),
            _group("no", "과장 표현 없음", [r for r in rows if not r["exaggeration"]]),
        ],
        correlation_5d=_pearson([r["score"] for r in with_5d], [r["excess_5d"] for r in with_5d]),
    )
