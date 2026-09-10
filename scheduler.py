"""
scheduler.py — 자동 크롤링 스케줄러

실행 방법:
    python scheduler.py               # 기본 (30분 간격)
    python scheduler.py --interval 10 # 10분 간격
    python scheduler.py --once        # 1회만 실행
"""

import argparse
import asyncio
import logging
from datetime import datetime

from crawlers.manager import SecuritiesNewsCrawlerManager
import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ── 핵심 크롤링 작업 ──────────────────────────────────────────
async def crawl_job(manager: SecuritiesNewsCrawlerManager) -> int:
    """
    1회 크롤링 작업:
      1) 기존 JSON 로드
      2) 새 기사 수집
      3) 중복 제거
      4) 신규 기사만 append 저장
    Returns: 저장된 신규 기사 수
    """
    logger.info("===== 크롤링 시작: %s =====", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 1. 기존 데이터 로드
    existing = storage.load_existing()

    # 2. 새 기사 수집 (본문 제외 → 빠름. True로 바꾸면 본문까지 수집)
    articles = await manager.run(fetch_content=False)
    new_dicts = manager.to_dict_list(articles)

    # 3. 중복 제거
    new_only = storage.filter_new(new_dicts, existing)

    # 4. 누적 저장
    saved = storage.save(existing, new_only)

    logger.info("===== 크롤링 완료: 신규 %d건 저장 =====\n", saved)
    return saved


# ── asyncio while-loop 스케줄러 (의존성 없음) ─────────────────
async def run_scheduler(interval_minutes: int):
    """
    asyncio 기반 주기 실행.
    외부 라이브러리 없이 동작하므로 가장 간단합니다.
    """
    manager = SecuritiesNewsCrawlerManager(max_pages=3)

    logger.info("스케줄러 시작 — %d분 간격으로 실행됩니다. (Ctrl+C로 종료)", interval_minutes)

    while True:
        try:
            await crawl_job(manager)
        except Exception as e:
            logger.error("크롤링 중 오류 발생: %s", e)

        logger.info("다음 실행까지 %d분 대기...", interval_minutes)
        await asyncio.sleep(interval_minutes * 60)


# ── 1회 실행 ─────────────────────────────────────────────────
async def run_once():
    manager = SecuritiesNewsCrawlerManager(max_pages=3)
    await crawl_job(manager)


# ── CLI 진입점 ────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="증권 뉴스 자동 크롤러")
    parser.add_argument(
        "--interval", type=int, default=30,
        help="크롤링 간격 (분 단위, 기본값: 30)"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="1회만 실행하고 종료"
    )
    args = parser.parse_args()

    if args.once:
        asyncio.run(run_once())
    else:
        asyncio.run(run_scheduler(args.interval))
