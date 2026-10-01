"""
한국경제(한경) - 증권 뉴스 크롤러
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from crawlers.naver_crawler import NewsArticle

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
}

DATE_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})\.\s*(오전|오후)\s*(\d{1,2}):(\d{2})")


def _parse_date(text: str) -> Optional[datetime]:
    """'2026.09.30. 오전 6:03' 형태를 datetime으로 변환"""
    m = DATE_RE.search(text)
    if not m:
        return None
    year, month, day, ampm, hour, minute = m.groups()
    hour = int(hour) % 12 + (12 if ampm == "오후" else 0)
    return datetime(int(year), int(month), int(day), hour, int(minute))


class HankyungSecuritiesNewsCrawler:
    """
    네이버 뉴스의 한국경제(OID: 015) 언론사별 기사 목록 수집
    (네이버 증권 API는 officeId 파라미터를 무시하므로 언론사별 목록 페이지를 사용)
    """

    # sid1=101(경제) sid2=258(증권) 섹션으로 한정
    LIST_URL = "https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=015&sid1=101&sid2=258&listType=title&page={page}"
    ARTICLE_URL = "https://n.news.naver.com/mnews/article/015/{aid}"

    def __init__(self, max_pages: int = 1, delay: float = 0.4):
        self.max_pages = max_pages
        self.delay = delay

    async def crawl(self, fetch_content: bool = True) -> list[NewsArticle]:
        articles: list[NewsArticle] = []
        seen: set[str] = set()

        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=10.0) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    res = await client.get(self.LIST_URL.format(page=page))
                    if res.status_code != 200:
                        logger.warning(f"[Hankyung] 목록 GET 실패 [{res.status_code}]")
                        break

                    # 목록 페이지는 EUC-KR 인코딩
                    soup = BeautifulSoup(res.content.decode("cp949", "ignore"), "html.parser")
                    new_in_page = 0

                    # 기사 한 건 = li 하나 (1면 목록 ul.type13 / 일반 목록 ul.type02)
                    for li in soup.select("ul.type13 li, ul.type02 li"):
                        link = li.find("a", href=re.compile(r"article/015/(\d+)"))
                        if not link:
                            continue
                        title = link.get_text(strip=True)
                        aid = re.search(r"article/015/(\d+)", link["href"]).group(1)
                        if not title or aid in seen:
                            continue
                        seen.add(aid)
                        new_in_page += 1

                        date_tag = li.select_one("span.date")
                        published_at = _parse_date(date_tag.get_text()) if date_tag else None

                        articles.append(NewsArticle(
                            title=title,
                            url=self.ARTICLE_URL.format(aid=aid),
                            source="한국경제",
                            publisher="한국경제",
                            published_at=published_at,
                        ))

                    # 새 기사가 없으면(마지막 페이지 초과 시 같은 목록 반복) 종료
                    if new_in_page == 0:
                        logger.info(f"[Hankyung] 페이지 {page} 신규 항목 없음, 종료")
                        break

                except Exception as e:
                    logger.warning(f"[Hankyung] 목록 수집 중 예외 발생: {e}")

                await asyncio.sleep(self.delay)

            if fetch_content:
                for a in articles:
                    a.content = await self._fetch_body(client, a.url)
                    await asyncio.sleep(self.delay)

        logger.info(f"[Hankyung] 최종 수집: {len(articles)}건")
        return articles

    async def _fetch_body(self, client: httpx.AsyncClient, url: str) -> str:
        """기사 상세 페이지에서 본문 추출 (실패 시 빈 문자열)"""
        try:
            res = await client.get(url)
            if res.status_code != 200:
                return ""
            soup = BeautifulSoup(res.text, "html.parser")
            body = soup.select_one("#dic_area")
            return body.get_text(" ", strip=True) if body else ""
        except Exception as e:
            logger.warning(f"[Hankyung] 본문 수집 실패 {url}: {e}")
            return ""
