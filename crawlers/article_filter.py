"""
주식과 무관한 기사 판별기 (crawlers/article_filter.py)

부고·인사·포토처럼 투자 판단에 쓸 내용이 없는 기사는 종목을 연결하지 않습니다.
종목이 연결되지 않은 기사는 기사 목록, 종목 순위, LLM 분석 대상에서 자동으로 빠집니다.

[규칙]
- 제목 앞 태그가 [부고], [인사], [포토] 등이면 제외
- 본문이 짧고(SHORT_BODY 미만) 사진 기사 표시("재판매 및 DB 금지", photo@ 등)가 있으면 사진 설명 기사로 보고 제외
  (일반 기사 본문에도 사진 캡션이 섞이므로, 길이 조건 없이 표시만으로는 판단하지 않음)
- 본문이 아주 짧으면(MIN_BODY 미만) 분석할 내용이 없으므로 제외

[기존 기사 정리]  python -m crawlers.article_filter            (제외 대상 목록만 출력)
                  python -m crawlers.article_filter --apply    (제외 대상의 종목 연결 삭제)
"""

import argparse
import asyncio
import logging
import re

from sqlalchemy import delete, select

logger = logging.getLogger(__name__)

# 제목 맨 앞 [태그] 중 주식 기사가 아닌 것
EXCLUDED_TAGS = {"부고", "부음", "인사", "동정", "포토", "사진", "화보", "게시판", "알림", "운세", "날씨"}
TITLE_TAG_RE = re.compile(r"^\s*[\[【<]\s*([^\]】>]{1,10}?)\s*[\]】>]")

# 통신사 사진 기사에 붙는 표시
PHOTO_MARKERS = ("재판매 및 DB 금지", "photo@", "(서울=뉴스1) =")
SHORT_BODY = 400   # 이보다 짧은 본문에 사진 표시가 있으면 사진 설명 기사
MIN_BODY = 60      # 이보다 짧으면 분석할 내용이 없음 (한두 문장짜리 [속보]는 남도록 낮게 잡음)


def exclusion_reason(title: str, content: str = "") -> str | None:
    """주식과 무관한 기사면 그 이유를, 아니면 None을 반환"""
    m = TITLE_TAG_RE.match(title or "")
    if m and m.group(1) in EXCLUDED_TAGS:
        return f"[{m.group(1)}] 기사"

    body = (content or "").strip()
    if len(body) < SHORT_BODY and any(marker in body for marker in PHOTO_MARKERS):
        return "사진 설명 기사"
    if len(body) < MIN_BODY:
        return f"본문 {len(body)}자"
    return None


async def find_linked_excluded() -> list[tuple[int, str, str]]:
    """종목이 연결돼 있지만 제외 대상인 기존 기사 (id, 제목, 이유)"""
    from api.storage import ArticleModel, ArticleStockModel, AsyncSessionLocal

    stmt = select(ArticleModel.id, ArticleModel.title, ArticleModel.content).where(
        ArticleModel.id.in_(select(ArticleStockModel.article_id))
    )
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(stmt)).all()
    return [(i, t, r) for i, t, c in rows if (r := exclusion_reason(t, c))]


async def unlink(article_ids: list[int]) -> int:
    """기사-종목 연결만 삭제 (기사와 기존 LLM 평가는 남겨 검증 결과를 다시 계산할 수 있게 함)"""
    from api.storage import ArticleStockModel, AsyncSessionLocal

    if not article_ids:
        return 0
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(delete(ArticleStockModel).where(ArticleStockModel.article_id.in_(article_ids)))
    return result.rowcount


async def _main(apply: bool):
    from api.storage import engine, init_db

    await init_db()
    targets = await find_linked_excluded()
    for art_id, title, reason in targets:
        print(f"  [{art_id}] {reason:<12} {title}")
    if not targets:
        print("제외할 기사가 없습니다.")
    elif apply:
        n = await unlink([i for i, _, _ in targets])
        print(f"기사 {len(targets)}건의 종목 연결 {n}개를 삭제했습니다.")
    else:
        print(f"제외 대상 {len(targets)}건. 종목 연결을 지우려면 --apply 를 붙여 다시 실행하세요.")
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="주식과 무관한 기사의 종목 연결 정리")
    parser.add_argument("--apply", action="store_true", help="목록만 보지 않고 실제로 종목 연결을 삭제")
    args = parser.parse_args()
    asyncio.run(_main(args.apply))
