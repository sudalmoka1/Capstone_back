"""
FastAPI 라우터 - 증권 뉴스 크롤링 API
"""

import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from crawlers.manager import SecuritiesNewsCrawlerManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/news", tags=["뉴스 크롤링"])

manager = SecuritiesNewsCrawlerManager(max_pages=3)


# ── 응답 스키마 ───────────────────────────────────────────────
class ArticleOut(BaseModel):
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[str]
    content_preview: str          # 본문 앞 200자
    related_stocks: list[str]


class CrawlResult(BaseModel):
    total: int
    sources_used: list[str]
    articles: list[ArticleOut]


# ── 엔드포인트 ────────────────────────────────────────────────
@router.get("/securities", response_model=CrawlResult)
async def get_securities_news(
    sources: list[str] = Query(default=["naver", "hankyung"]),
    fetch_content: bool = Query(default=True, description="본문 크롤링 여부"),
    limit: int = Query(default=50, le=200),
):
    """
    전체 증권 뉴스 수집 엔드포인트
    
    - sources: naver / hankyung (복수 선택 가능)
    - fetch_content: false면 제목+메타만 빠르게 수집
    - limit: 반환할 최대 기사 수
    """
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
