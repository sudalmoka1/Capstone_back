"""
통합 크롤러 매니저 (crawlers/manager.py)
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional
from api.storage import save_articles_to_db
from crawlers.naver_crawler import NaverSecuritiesNewsCrawler, NewsArticle
from crawlers.hankyung_crawler import HankyungSecuritiesNewsCrawler

logger = logging.getLogger(__name__)


class SecuritiesNewsCrawlerManager:
    def __init__(self, max_pages: int = 1, delay: float = 0.4):
        self.crawlers = {
            "naver": NaverSecuritiesNewsCrawler(max_pages=max_pages, delay=delay),
            "hankyung": HankyungSecuritiesNewsCrawler(max_pages=max_pages, delay=delay),
        }

    async def run(
        self,
        sources: Optional[list[str]] = None,
        fetch_content: bool = True,
    ) -> list[NewsArticle]:

        active = sources or list(self.crawlers.keys())
        tasks = [
            self.crawlers[src].crawl(fetch_content=fetch_content)
            for src in active
            if src in self.crawlers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: list[NewsArticle] = []
        for src, res in zip(active, results):
            if isinstance(res, Exception):
                logger.error("[%s] 크롤링 예외: %s", src, res)
                continue
            merged.extend(res)

        # DB는 url이 unique라 같은 URL을 두 출처로 저장하면 서로 덮어쓴다.
        # URL 기준으로 한 건만 남기되, 한국경제(015) 기사는 한국경제 출처를 우선한다.
        merged.sort(key=lambda a: a.source != "한국경제")
        seen: set[str] = set()
        unique: list[NewsArticle] = []
        for a in merged:
            if a.url not in seen:
                seen.add(a.url)
                unique.append(a)

        unique.sort(key=lambda a: a.published_at or datetime.min, reverse=True)
        logger.info("통합 수집 완료 | 총 %d건 → 중복 제거 후 %d건", len(merged), len(unique))

        if unique:
            try:
                dict_list = self.to_dict_list(unique)
                with open("crawl_result.json", "w", encoding="utf-8") as f:
                    json.dump(dict_list, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error("JSON 저장 실패: %s", e)

            try:
                await save_articles_to_db(unique)
            except Exception as e:
                logger.error("PostgreSQL 저장 실패: %s", e)

        return unique

    def to_dict_list(self, articles: list[NewsArticle]) -> list[dict]:
        return [
            {
                "title": a.title,
                "url": a.url,
                "source": a.source,
                "publisher": a.publisher,
                "published_at": a.published_at.isoformat() if a.published_at else None,
                "content": a.content,
                "related_stocks": getattr(a, 'related_stocks', [])
            }
            for a in articles
        ]