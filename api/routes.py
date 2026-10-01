"""
FastAPI 라우터 - 증권 뉴스 크롤링 및 DB 조회 API
"""

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import select

from api.storage import ArticleEvaluationModel, ArticleModel, PublisherModel, AsyncSessionLocal
from crawlers.manager import SecuritiesNewsCrawlerManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["뉴스 크롤링 및 조회"])

manager = SecuritiesNewsCrawlerManager(max_pages=3)


# ── 응답 스키마 ───────────────────────────────────────────────
class ArticleOut(BaseModel):
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[str]
    content_preview: str
    related_stocks: list[str]


class CrawlResult(BaseModel):
    total: int
    sources_used: list[str]
    articles: list[ArticleOut]


class EvaluationOut(BaseModel):
    status: str
    score: int
    report: Optional[Dict[str, Any]] = None


class ArticleDBOut(BaseModel):
    id: int
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[str]
    content: Optional[str]
    evaluation: Optional[EvaluationOut] = None

    class Config:
        from_attributes = True


# ── 엔드포인트 ────────────────────────────────────────────────

@router.get("/news/securities", response_model=CrawlResult)
async def get_securities_news(
    sources: list[str] = Query(default=["naver", "hankyung"]),
    fetch_content: bool = Query(default=True, description="본문 크롤링 여부"),
    limit: int = Query(default=50, le=200),
):
    """실시간 증권 뉴스 수집 엔드포인트"""
    articles = await manager.run(sources=sources, fetch_content=fetch_content)
    articles = articles[:limit]

    return CrawlResult(
        total=len(articles),
        sources_used=sources,
        articles=[
            ArticleOut(
                title=a.title,
                url=a.url,
                source=a.source,
                publisher=a.publisher,
                published_at=a.published_at.isoformat() if a.published_at else None,
                content_preview=a.content[:200] if a.content else "",
                related_stocks=a.related_stocks,
            )
            for a in articles
        ],
    )


@router.get("/articles", response_model=list[ArticleDBOut])
async def get_db_articles(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0, description="건너뛸 기사 수 (페이지네이션)"),
    source: Optional[str] = Query(default=None, description="네이버금융 / 한국경제 필터")
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

        stmt = stmt.limit(limit).offset(offset)

        result = await session.execute(stmt)
        rows = result.all()  # (ArticleModel, ArticleEvaluationModel, publisher_name) 튜플 반환

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
                    content=article.content,
                    evaluation=eval_data
                )
            )

        return output