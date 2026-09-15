"""
네이버 금융 - 전체 증권 뉴스 크롤러
URL: https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://finance.naver.com/",
}


@dataclass
class NewsArticle:
    """크롤링된 뉴스 기사"""
    title: str
    url: str
    source: str           # "네이버금융" | "한국경제"
    publisher: str        # 언론사명
    published_at: Optional[datetime]
    content: str = ""
    related_stocks: list[str] = field(default_factory=list)  # 관련 종목코드


async def safe_get(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url, headers=HEADERS, timeout=12.0, follow_redirects=True)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.warning("GET 실패 [%s]: %s", url, e)
        return None


class NaverSecuritiesNewsCrawler:
    """
    네이버 금융 증권 전체 뉴스 수집기
    - URL: finance.naver.com/news/mainnews.nhn (증권 메인 뉴스)
    - 페이지네이션으로 최신 뉴스 일괄 수집
    - 각 기사 본문 파싱 포함
    """

    # 네이버 금융 증권 메인 뉴스 (안정적인 URL)
    LIST_URL = "https://finance.naver.com/news/mainnews.nhn?page={page}"
    # 네이버 뉴스 본문
    ARTICLE_BASE = "https://n.news.naver.com/mnews/article/{office_id}/{article_id}"

    def __init__(self, max_pages: int = 5, delay: float = 0.4):
        self.max_pages = max_pages
        self.delay = delay

    # ── 목록 파싱 ─────────────────────────────────────────────
    def _parse_list(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # 네이버 금융 mainnews 구조: div.newsTitle > a, span.press, span.date
        for row in soup.select("li.newsList, div.newsTitle"):
            a_tag = row.select_one("a")
            if not a_tag:
                continue

            href = a_tag.get("href", "")
            # office_id / article_id 추출 (두 가지 URL 형식 대응)
            m_oid = re.search(r"office_id=(\d+)", href) or re.search(r"/article/(\d+)/", href)
            m_aid = re.search(r"article_id=(\d+)", href) or re.search(r"/article/\d+/(\d+)", href)

            press = row.select_one("span.press, span.info")
            date  = row.select_one("span.date, span.wdate")

            items.append({
                "title":      a_tag.get_text(strip=True),
                "href":       href,
                "office_id":  m_oid.group(1) if m_oid else "",
                "article_id": m_aid.group(1) if m_aid else "",
                "publisher":  press.get_text(strip=True) if press else "",
                "date_str":   date.get_text(strip=True)  if date  else "",
            })

        # 위 선택자로 못 잡으면 href 패턴으로 직접 탐색 (fallback)
        if not items:
            for a_tag in soup.select("a[href*='article_id'], a[href*='/mnews/article/']"):
                href = a_tag.get("href", "")
                m_oid = re.search(r"office_id=(\d+)", href) or re.search(r"/article/(\d+)/", href)
                m_aid = re.search(r"article_id=(\d+)", href) or re.search(r"/article/\d+/(\d+)", href)
                if not m_oid or not m_aid:
                    continue
                title = a_tag.get_text(strip=True)
                if len(title) < 5:
                    continue
                items.append({
                    "title":      title,
                    "href":       href,
                    "office_id":  m_oid.group(1),
                    "article_id": m_aid.group(1),
                    "publisher":  "",
                    "date_str":   "",
                })

        return items

    # ── 본문 파싱 ─────────────────────────────────────────────
    def _parse_content(self, html: str) -> tuple[str, list[str]]:
        """(본문 텍스트, 관련 종목코드 리스트) 반환"""
        soup = BeautifulSoup(html, "html.parser")

        # 본문 선택자 (네이버 뉴스 여러 레이아웃 대응)
        body = (
            soup.select_one("div#dic_area")
            or soup.select_one("div.newsct_article")
            or soup.select_one("div#articeBody")
        )
        content = ""
        if body:
            for tag in body.select("script,style,figure,iframe"):
                tag.decompose()
            content = body.get_text(separator="\n", strip=True)

        # 관련 종목코드 추출 (네이버 금융 종목 링크)
        stock_codes = []
        for a in soup.select("a[href*='item/main.naver?code=']"):
            m = re.search(r"code=(\d{6})", a["href"])
            if m:
                stock_codes.append(m.group(1))

        return content, list(set(stock_codes))

    @staticmethod
    def _parse_date(s: str) -> Optional[datetime]:
        s = s.strip()
        for fmt in ("%Y.%m.%d %H:%M", "%Y.%m.%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    # ── 메인 크롤 ─────────────────────────────────────────────
    async def crawl(self, fetch_content: bool = True) -> list[NewsArticle]:
        articles: list[NewsArticle] = []
        seen_urls: set[str] = set()

        async with httpx.AsyncClient(headers=HEADERS) as client:
            for page in range(1, self.max_pages + 1):
                html = await safe_get(client, self.LIST_URL.format(page=page))
                if not html:
                    logger.warning("[Naver] 페이지 %d 수집 실패, 종료", page)
                    break

                raw_items = self._parse_list(html)
                if not raw_items:
                    logger.info("[Naver] 페이지 %d 항목 없음, 종료", page)
                    break

                logger.info("[Naver] 페이지 %d: %d건 파싱", page, len(raw_items))

                for item in raw_items:
                    # 본문 URL 조립
                    if item["office_id"] and item["article_id"]:
                        article_url = self.ARTICLE_BASE.format(
                            office_id=item["office_id"],
                            article_id=item["article_id"],
                        )
                    else:
                        article_url = urljoin("https://finance.naver.com", item["href"])

                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)

                    content, stocks = "", []
                    if fetch_content:
                        article_html = await safe_get(client, article_url)
                        if article_html:
                            content, stocks = self._parse_content(article_html)
                        await asyncio.sleep(self.delay)

                    articles.append(NewsArticle(
                        title=item["title"],
                        url=article_url,
                        source="네이버금융",
                        publisher=item["publisher"],
                        published_at=self._parse_date(item["date_str"]),
                        content=content,
                        related_stocks=stocks,
                    ))

                await asyncio.sleep(self.delay)

        logger.info("[Naver] 최종 수집: %d건", len(articles))
        return articles