"""
한국경제(한경) - 전체 증권 뉴스 크롤러
URL: https://www.hankyung.com/securities
RSS: https://www.hankyung.com/feed/securities
"""

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from crawlers.naver_crawler import NewsArticle, HEADERS, safe_get

logger = logging.getLogger(__name__)


class HankyungSecuritiesNewsCrawler:
    """
    한국경제 증권 뉴스 크롤러
    
    전략 1 (우선): RSS 피드 파싱 → 안정적이고 빠름
    전략 2 (보완): 증권 섹션 HTML 스크래핑 → RSS에 없는 기사 보완
    """

    # 한경 RSS (정확한 현재 경로)
    RSS_URLS = [
        "https://rss.hankyung.com/feed/finance.xml",   # 금융/증권 통합
        "https://rss.hankyung.com/feed/stock.xml",     # 주식
    ]
    # 한경 증권 뉴스 목록
    LIST_URL = "https://www.hankyung.com/finance?page={page}"

    def __init__(self, max_pages: int = 3, delay: float = 0.5):
        self.max_pages = max_pages
        self.delay = delay

    # ── RSS 파싱 ──────────────────────────────────────────────
    async def _fetch_rss(self, client: httpx.AsyncClient) -> list[NewsArticle]:
        """RSS 피드에서 최신 뉴스 수집 (복수 URL 시도)"""
        articles = []

        for rss_url in self.RSS_URLS:
            xml_text = await safe_get(client, rss_url)
            if not xml_text:
                logger.warning("[Hankyung RSS] 404/실패: %s", rss_url)
                continue

            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as e:
                logger.error("[Hankyung RSS] XML 파싱 실패 (%s): %s", rss_url, e)
                continue

            ns = {"dc": "http://purl.org/dc/elements/1.1/"}
            count = 0
            for item in root.findall(".//item"):
                title     = (item.findtext("title") or "").strip()
                url       = (item.findtext("link")  or "").strip()
                pub_str   = (item.findtext("pubDate") or "").strip()
                publisher = (item.findtext("dc:creator", namespaces=ns) or "한국경제").strip()

                if not title or not url:
                    continue

                published_at = None
                if pub_str:
                    try:
                        published_at = parsedate_to_datetime(pub_str).replace(tzinfo=None)
                    except Exception:
                        pass

                articles.append(NewsArticle(
                    title=title,
                    url=url,
                    source="한국경제",
                    publisher=publisher,
                    published_at=published_at,
                ))
                count += 1
            logger.info("[Hankyung RSS] %s → %d건", rss_url, count)

        return articles

    # ── HTML 목록 파싱 ────────────────────────────────────────
    def _parse_list_html(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # 한경 뉴스 카드 선택자 (여러 레이아웃 대응)
        selectors = [
            "ul.news-list li",
            "div.article-list article",
            "div.news_list li",
            "ul.list_news li",
        ]
        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        # fallback: href에 hankyung.com/article 포함된 링크 직접 탐색
        if not cards:
            for a in soup.select("a[href*='hankyung.com/article']"):
                title = a.get_text(strip=True)
                if len(title) < 5:
                    continue
                items.append({"title": title, "url": a["href"], "date_str": ""})
            return items

        for card in cards:
            a = card.select_one("h3 a, h2 a, a.article-title, a[href*='article']")
            if not a:
                continue
            date_tag = card.select_one("span.date, time, span.time, span.datetime")
            url = a.get("href", "")
            if url.startswith("/"):
                url = "https://www.hankyung.com" + url
            items.append({
                "title":    a.get_text(strip=True),
                "url":      url,
                "date_str": date_tag.get_text(strip=True) if date_tag else "",
            })
        return items

    # ── 본문 파싱 ─────────────────────────────────────────────
    def _parse_content(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        body = (
            soup.select_one("div.article-body")
            or soup.select_one("div#articleBody")
            or soup.select_one("div.content-body")
            or soup.select_one("div#newsView")
        )
        if not body:
            return ""

        for tag in body.select("script,style,figure,aside,.ad-wrap"):
            tag.decompose()

        return body.get_text(separator="\n", strip=True)

    @staticmethod
    def _parse_date(s: str) -> Optional[datetime]:
        s = s.strip()
        for fmt in (
            "%Y.%m.%d %H:%M",
            "%Y.%m.%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    # ── HTML 보완 크롤 ────────────────────────────────────────
    async def _fetch_html_supplement(
        self,
        client: httpx.AsyncClient,
        existing_urls: set[str],
    ) -> list[NewsArticle]:
        """RSS에서 누락된 기사를 HTML 파싱으로 보완"""
        extra = []
        for page in range(1, self.max_pages + 1):
            html = await safe_get(client, self.LIST_URL.format(page=page))
            if not html:
                break

            for item in self._parse_list_html(html):
                url = item["url"]
                if not url or url in existing_urls:
                    continue
                # 상대경로 처리
                if url.startswith("/"):
                    url = "https://www.hankyung.com" + url

                extra.append(NewsArticle(
                    title=item["title"],
                    url=url,
                    source="한국경제",
                    publisher="한국경제",
                    published_at=self._parse_date(item["date_str"]),
                ))
                existing_urls.add(url)

            await asyncio.sleep(self.delay)

        logger.info("[Hankyung HTML] 보완 수집: %d건", len(extra))
        return extra

    # ── 본문 일괄 수집 ────────────────────────────────────────
    async def _fill_contents(
        self,
        client: httpx.AsyncClient,
        articles: list[NewsArticle],
    ) -> None:
        """기사 리스트의 content 필드를 본문으로 채움 (in-place)"""
        for article in articles:
            html = await safe_get(client, article.url)
            if html:
                article.content = self._parse_content(html)
            await asyncio.sleep(self.delay)

    # ── 메인 크롤 ─────────────────────────────────────────────
    async def crawl(self, fetch_content: bool = True) -> list[NewsArticle]:
        async with httpx.AsyncClient(headers=HEADERS) as client:
            # 1. RSS 우선 수집
            articles = await self._fetch_rss(client)
            seen = {a.url for a in articles}

            # 2. HTML로 보완
            extra = await self._fetch_html_supplement(client, seen)
            articles.extend(extra)

            # 3. 본문 크롤링
            if fetch_content:
                logger.info("[Hankyung] 본문 수집 시작 (%d건)...", len(articles))
                await self._fill_contents(client, articles)

        logger.info("[Hankyung] 최종 수집: %d건", len(articles))
        return articles