"""
FastAPI 라우터 - 증권 뉴스 크롤링 및 DB 조회 API
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import func, select

from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticleStockModel,
    AsyncSessionLocal,
    PublisherModel,
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
            .order_by(ArticleModel.id.desc())
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