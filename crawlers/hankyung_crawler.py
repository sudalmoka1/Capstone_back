"""
한국경제(한경) - 증권 뉴스 크롤러
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional
import httpx
from crawlers.naver_crawler import NewsArticle

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Referer": "https://m.stock.naver.com/",
}


class HankyungSecuritiesNewsCrawler:
    """
    네이버 증권 OpenAPI 경로 중 한국경제(OID: 015) 증권 뉴스 직접 수집
    """

    # 한국경제 언론사 코드(015) 뉴스 전용 API Endpoint
    API_URL = "https://m.stock.naver.com/api/news/list?category=mainnews&officeId=015&page={page}&pageSize=20"

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
                        logger.warning(f"[Hankyung] API GET 실패 [{res.status_code}]")
                        break

                    items = res.json()
                    if not isinstance(items, list) or not items:
                        logger.info(f"[Hankyung] 페이지 {page} 항목 없음, 종료")
                        break

                    for item in items:
                        title = item.get("tit", "").strip()
                        oid = item.get("oid", "")
                        aid = item.get("aid", "")
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

                        # 💡 source를 '한국경제'로 명확히 명시
                        articles.append(NewsArticle(
                            title=title,
                            url=article_url,
                            source="한국경제",
                            publisher="한국경제",
                            published_at=published_at,
                            content=subcontent
                        ))

                except Exception as e:
                    logger.warning(f"[Hankyung] 수집 중 예외 발생: {e}")

                await asyncio.sleep(self.delay)

        logger.info(f"[Hankyung] 최종 수집: {len(articles)}건")
        return articles