"""
통합 크롤러 매니저
- 네이버 금융 + 한국경제 병렬 수집
- 중복 제거 (URL 기준)
- DB 저장 연동
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional
from api.storage import save_articles_to_db, init_db
from crawlers.naver_crawler import NaverSecuritiesNewsCrawler, NewsArticle
from crawlers.hankyung_crawler import HankyungSecuritiesNewsCrawler

logger = logging.getLogger(__name__)


class SecuritiesNewsCrawlerManager:
    """
    증권 뉴스 통합 수집 매니저

    사용 예시:
        manager = SecuritiesNewsCrawlerManager(max_pages=3)
        articles = await manager.run(fetch_content=True)
    """

    def __init__(self, max_pages: int = 3, delay: float = 0.4):
        self.crawlers = {
            "naver":    NaverSecuritiesNewsCrawler(max_pages=max_pages, delay=delay),
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

        # 결과 통합
        merged: list[NewsArticle] = []
        for src, res in zip(active, results):
            if isinstance(res, Exception):
                logger.error("[%s] 크롤링 예외: %s", src, res)
                continue
            merged.extend(res)

        # URL 기준 중복 제거
        seen: set[str] = set()
        unique: list[NewsArticle] = []
        for a in merged:
            if a.url and a.url not in seen:
                seen.add(a.url)
                unique.append(a)

        unique.sort(key=lambda a: a.published_at or datetime.min, reverse=True)
        logger.info("통합 수집 완료 | 총 %d건 → 중복 제거 후 %d건", len(merged), len(unique))

        # ── 💡 [자동화 로직 추가 영역] ──────────────────────────────
        if unique:
            # 1. crawl_result.json 파일 생성 자동화
            try:
                dict_list = self.to_dict_list(unique)
                with open("crawl_result.json", "w", encoding="utf-8") as f:
                    json.dump(dict_list, f, ensure_ascii=False, indent=2)
                logger.info("파일 저장 성공: crawl_result.json")
            except Exception as e:
                logger.error("JSON 파일 저장 실패: %s", e)

            # 2. PostgreSQL 데이터베이스 저장 자동화
            try:
                await save_articles_to_db(unique)
                logger.info("PostgreSQL 데이터베이스(DB) 적재 완료")
            except Exception as e:
                logger.error("PostgreSQL 저장 실패: %s", e)
        # ──────────────────────────────────────────────────────────

        return unique

    def to_dict_list(self, articles: list[NewsArticle]) -> list[dict]:
        return [
            {
                "title":          a.title,
                "url":            a.url,
                "source":         a.source,
                "publisher":      a.publisher,
                "published_at":   a.published_at.isoformat() if a.published_at else None,
                "content":        a.content,
                "related_stocks": a.related_stocks if hasattr(a, 'related_stocks') else []
            }
            for a in articles
        ]
