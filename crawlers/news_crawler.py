"""
네이버 뉴스 언론사별 증권 뉴스 크롤러 (crawlers/news_crawler.py)

네이버 뉴스의 '언론사별 · 경제 > 증권' 목록 페이지를 언론사 코드(oid)만 바꿔 가며 수집합니다.
언론사마다 크롤러를 따로 두지 않고, 클래스 하나에 언론사 설정(Press)을 넘기는 구조입니다.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
}

SOURCE_NAME = "네이버뉴스"

DATE_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})\.\s*(오전|오후)\s*(\d{1,2}):(\d{2})")
RELATIVE_RE = re.compile(r"(\d+)\s*(분|시간|일)\s*전")


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


@dataclass(frozen=True)
class Press:
    """수집 대상 언론사 (oid = 네이버 언론사 코드)"""
    oid: str
    name: str


# 증권 기사가 많은 언론사. 목록 페이지 응답을 확인한 코드만 넣었고, 추가/삭제는 여기서만 하면 된다.
DEFAULT_PRESSES: list[Press] = [
    Press("015", "한국경제"),
    Press("009", "매일경제"),
    Press("011", "서울경제"),
    Press("008", "머니투데이"),
    Press("018", "이데일리"),
    Press("277", "아시아경제"),
    Press("366", "조선비즈"),
    Press("001", "연합뉴스"),
    Press("215", "한국경제TV"),
    Press("421", "뉴스1"),
]


def _parse_date(text: str) -> Optional[datetime]:
    """'2026.09.30. 오전 6:03' 형태를 datetime으로 변환"""
    m = DATE_RE.search(text)
    if not m:
        return None
    year, month, day, ampm, hour, minute = m.groups()
    hour = int(hour) % 12 + (12 if ampm == "오후" else 0)
    return datetime(int(year), int(month), int(day), hour, int(minute))


def _parse_relative(text: str) -> Optional[datetime]:
    """'4분전', '2시간전', '1일전' 같은 상대 시각을 현재 기준 datetime으로 변환 (근사값)"""
    m = RELATIVE_RE.search(text)
    if not m:
        return None
    amount, unit = int(m.group(1)), m.group(2)
    delta = {"분": timedelta(minutes=amount), "시간": timedelta(hours=amount), "일": timedelta(days=amount)}[unit]
    return (datetime.now() - delta).replace(second=0, microsecond=0)


async def fetch_article_detail(client: httpx.AsyncClient, url: str) -> tuple[str, Optional[datetime]]:
    """
    기사 상세 페이지(n.news.naver.com)에서 (본문, 발행 시각)을 추출합니다.
    언론사와 관계없이 같은 구조입니다. 실패 시 ("", None).
    """
    try:
        res = await client.get(url)
        if res.status_code != 200:
            return "", None
        soup = BeautifulSoup(res.text, "html.parser")

        body = soup.select_one("#dic_area")
        content = body.get_text(" ", strip=True) if body else ""

        published_at = None
        date_tag = soup.select_one("span._ARTICLE_DATE_TIME")
        if date_tag and date_tag.get("data-date-time"):
            try:
                published_at = datetime.strptime(date_tag["data-date-time"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        return content, published_at
    except Exception as e:
        logger.warning(f"[News] 본문 수집 실패 {url}: {e}")
        return "", None


class NaverPressNewsCrawler:
    """한 언론사의 증권 섹션(sid1=101 경제 / sid2=258 증권) 기사 목록 수집기"""

    # 목록 페이지는 EUC-KR 인코딩
    LIST_URL = (
        "https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}"
        "&sid1=101&sid2=258&listType=title&page={page}"
    )
    ARTICLE_URL = "https://n.news.naver.com/mnews/article/{oid}/{aid}"

    def __init__(self, press: Press, max_pages: int = 1, delay: float = 1.0):
        self.press = press
        self.max_pages = max_pages
        self.delay = delay

    async def crawl_list(self, client: httpx.AsyncClient) -> list[NewsArticle]:
        """목록만 수집합니다. 본문(content)은 비어 있고, 발행 시각은 목록 기준(상대 시각은 근사값)입니다."""
        oid = self.press.oid
        link_re = re.compile(rf"article/{oid}/(\d+)")
        articles: list[NewsArticle] = []
        seen: set[str] = set()

        for page in range(1, self.max_pages + 1):
            try:
                res = await client.get(self.LIST_URL.format(oid=oid, page=page))
                if res.status_code != 200:
                    logger.warning(f"[{self.press.name}] 목록 GET 실패 [{res.status_code}]")
                    break

                soup = BeautifulSoup(res.content.decode("cp949", "ignore"), "html.parser")
                new_in_page = 0

                # 기사 한 건 = li 하나 (1면 목록 ul.type13 / 일반 목록 ul.type02)
                for li in soup.select("ul.type13 li, ul.type02 li"):
                    link = li.find("a", href=link_re)
                    if not link:
                        continue
                    title = link.get_text(strip=True)
                    aid = link_re.search(link["href"]).group(1)
                    if not title or aid in seen:
                        continue
                    seen.add(aid)
                    new_in_page += 1

                    # 목록에는 절대 시각('2026.10.07. 오후 5:38') 또는 상대 시각('4분전')이 섞여 있다.
                    date_tag = li.select_one("span.date")
                    date_text = date_tag.get_text() if date_tag else ""

                    articles.append(NewsArticle(
                        title=title,
                        url=self.ARTICLE_URL.format(oid=oid, aid=aid),
                        source=SOURCE_NAME,
                        publisher=self.press.name,
                        published_at=_parse_date(date_text) or _parse_relative(date_text),
                    ))

                # 새 기사가 없으면(마지막 페이지 초과 시 같은 목록 반복) 종료
                if new_in_page == 0:
                    break

            except Exception as e:
                logger.warning(f"[{self.press.name}] 목록 수집 중 예외 발생: {e}")

            await asyncio.sleep(self.delay)

        logger.info(f"[{self.press.name}] 목록 수집: {len(articles)}건")
        return articles
