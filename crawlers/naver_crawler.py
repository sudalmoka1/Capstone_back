"""
네이버 금융 - 증권 뉴스 크롤러
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Referer": "https://m.stock.naver.com/",
}


@dataclass
class NewsArticle:
    """크롤링된 뉴스 기사 데이터 클래스"""
    title: str
    url: str
    source: str
    publisher: str
    published_at: Optional[datetime]
    content: str = ""
    related_stocks: list[str] = field(default_factory=list)


class NaverSecuritiesNewsCrawler:
    """
    네이버 증권 메인 뉴스 수집기
    """
    API_URL = "https://m.stock.naver.com/api/news/list?category=mainnews&page={page}&pageSize=20"

    def __init__(self, max_pages: int = 1, delay: float = 0.4):
        self.max_pages = max_pages
        self.delay = delay

    async def crawl(self, fetch_content: bool = True) -> list[NewsArticle]:
        articles: list[NewsArticle] = []

        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=10.0) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    url = self.API_URL.format(page=page)
                    res = await client.get(url)

                    if res.status_code != 200:
                        logger.warning(f"[Naver] API GET 실패 [{res.status_code}]")
                        break

                    items = res.json()
                    if not isinstance(items, list) or not items:
                        logger.info(f"[Naver] 페이지 {page} 항목 없음, 종료")
                        break

                    for item in items:
                        title = item.get("tit", "").strip()
                        oid = item.get("oid", "")
                        aid = item.get("aid", "")
                        publisher = item.get("ohnm", "네이버뉴스").strip()
                        subcontent = item.get("subcontent", "").strip()
                        dt_str = item.get("dt", "")

                        if not oid or not aid or not title:
                            continue

                        article_url = f"https://n.news.naver.com/mnews/article/{oid}/{aid}"

                        published_at = None
                        if len(dt_str) >= 14:
                            try:
                                published_at = datetime.strptime(dt_str[:14], "%Y%m%d%H%M%S")
                            except Exception:
                                pass

                        articles.append(NewsArticle(
                            title=title,
                            url=article_url,
                            source="네이버금융",
                            publisher=publisher,
                            published_at=published_at,
                            content=subcontent
                        ))

                except Exception as e:
                    logger.warning(f"[Naver] 수집 중 예외 발생: {e}")

                await asyncio.sleep(self.delay)

        logger.info(f"[Naver] 최종 수집: {len(articles)}건")
        return articles