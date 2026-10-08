"""
통합 크롤러 매니저 (crawlers/manager.py)

언론사별 목록 수집 → URL 중복 제거 → (이미 저장된 기사 제외) → 본문 수집 → 종목 매칭 → 저장
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

import httpx

from api.storage import get_existing_urls, save_articles_to_db
from crawlers.news_crawler import (
    DEFAULT_PRESSES,
    HEADERS,
    NaverPressNewsCrawler,
    NewsArticle,
    Press,
    fetch_article_detail,
)
from crawlers.stock_matcher import StockMatcher

logger = logging.getLogger(__name__)


class SecuritiesNewsCrawlerManager:
    def __init__(
        self,
        max_pages: int = 1,
        delay: float = 1.0,
        presses: Optional[list[Press]] = None,
        concurrency: int = 1,
    ):
        self.presses = presses or DEFAULT_PRESSES
        self.crawlers = {
            p.oid: NaverPressNewsCrawler(p, max_pages=max_pages, delay=delay) for p in self.presses
        }
        self.delay = delay
        self.concurrency = concurrency   # 동시에 보내는 요청 수 (서버 부담을 줄이기 위해 낮게 유지)
        self.matcher = StockMatcher()

    def _select(self, sources: Optional[list[str]]) -> list[NaverPressNewsCrawler]:
        """sources: 언론사 코드(oid) 또는 언론사 이름. 없으면 전체."""
        if not sources:
            return list(self.crawlers.values())
        wanted = set(sources)
        return [c for c in self.crawlers.values() if c.press.oid in wanted or c.press.name in wanted]

    async def run(
        self,
        sources: Optional[list[str]] = None,
        fetch_content: bool = True,
    ) -> list[NewsArticle]:

        crawlers = self._select(sources)
        sem = asyncio.Semaphore(self.concurrency)

        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=10.0) as client:

            async def _list(crawler: NaverPressNewsCrawler):
                async with sem:
                    return await crawler.crawl_list(client)

            results = await asyncio.gather(*[_list(c) for c in crawlers], return_exceptions=True)

            merged: list[NewsArticle] = []
            for crawler, res in zip(crawlers, results):
                if isinstance(res, Exception):
                    logger.error("[%s] 크롤링 예외: %s", crawler.press.name, res)
                    continue
                merged.extend(res)

            # 같은 URL은 한 건만
            seen: set[str] = set()
            unique: list[NewsArticle] = []
            for a in merged:
                if a.url not in seen:
                    seen.add(a.url)
                    unique.append(a)
            logger.info("목록 수집 완료 | %d개 언론사, 총 %d건 → 중복 제거 후 %d건", len(crawlers), len(merged), len(unique))

            if fetch_content:
                unique = await self._attach_bodies(client, unique, sem)

        unique.sort(key=lambda a: a.published_at or datetime.min, reverse=True)

        if unique:
            # 기사별 언급 종목 연결 (stocks 테이블 기준)
            try:
                await self.matcher.load()
                for a in unique:
                    a.related_stocks = self.matcher.match(a.title, a.content)
                matched = sum(1 for a in unique if a.related_stocks)
                logger.info("종목 매칭 | %d건 중 %d건에서 종목 발견", len(unique), matched)
            except Exception as e:
                logger.error("종목 매칭 실패(종목 없이 진행): %s", e)

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

    async def _attach_bodies(
        self, client: httpx.AsyncClient, articles: list[NewsArticle], sem: asyncio.Semaphore
    ) -> list[NewsArticle]:
        """이미 저장된 기사는 건너뛰고, 새 기사만 상세 페이지에서 본문/정확한 시각을 받아온다."""
        try:
            known = await get_existing_urls([a.url for a in articles])
        except Exception as e:
            logger.warning("기존 기사 조회 실패, 전부 새 기사로 처리: %s", e)
            known = set()

        fresh = [a for a in articles if a.url not in known]
        logger.info("새 기사 %d건 (이미 저장된 %d건 제외) 본문 수집 시작", len(fresh), len(articles) - len(fresh))

        async def _body(a: NewsArticle):
            async with sem:
                content, published_at = await fetch_article_detail(client, a.url)
                await asyncio.sleep(self.delay)
            a.content = content
            if published_at:
                a.published_at = published_at

        await asyncio.gather(*[_body(a) for a in fresh])

        # 본문이 없는 기사는 저장하지 않는다 (다음 수집 때 다시 시도됨)
        with_body = [a for a in fresh if a.content]
        if len(with_body) < len(fresh):
            logger.warning("본문 수집 실패 %d건은 저장하지 않고 건너뜀", len(fresh) - len(with_body))
        return with_body

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
