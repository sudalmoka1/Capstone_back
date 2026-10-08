"""
scheduler.py — 자동 크롤링 및 LLM 분석 스케줄러

크롤링과 분석을 서로 다른 루프로 동시에 돌립니다.
- 크롤링 루프: 외부 사이트 요청이므로 --interval 분마다 한 번 (기본 60분)
- 분석 루프:   로컬 LLM이므로 대기 기사가 있는 동안 계속 분석, 없으면 --idle 분 쉬고 다시 확인 (기본 5분)

[실행]      python scheduler.py
[간격 지정]  python scheduler.py --interval 60 --idle 5
[1회 실행]  python scheduler.py --once   (크롤링 1번 + 분석 1배치 후 종료)
[분석만]    python scheduler.py --analyze-only          (크롤링 없이 분석 루프만 계속)
[분석만 1회] python scheduler.py --analyze-only --once   (밀린 기사 전부 분석 후 종료)
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from analyzer import LLMAnalyzer
from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticleStockModel,
    AsyncSessionLocal,
    StockModel,
    init_db,
)
from crawlers.manager import SecuritiesNewsCrawlerManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# LLMAnalyzer 인스턴스 전역 생성
analyzer = LLMAnalyzer(model_name="qwen2.5:7b")

BATCH_SIZE = 50  # 분석 1배치에서 처리할 최대 기사 수


def to_ticker(stock_code, market) -> str | None:
    """종목코드를 야후파이낸스 티커로 변환 (코스피 .KS / 코스닥 .KQ)"""
    if not stock_code:
        return None
    return f"{stock_code}.KQ" if market == "KOSDAQ" else f"{stock_code}.KS"


# ── 핵심 크롤링 작업 ──────────────────────────────────────────
async def crawl_job(manager: SecuritiesNewsCrawlerManager) -> int:
    """
    manager.run() 내부에서 DB 적재까지 완료되므로 호출만 수행합니다.
    """
    logger.info("===== 크롤링 시작: %s =====", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 본문까지 수집해야 LLM이 분석 가능하므로 fetch_content=True로 설정
    articles = await manager.run(fetch_content=True)

    logger.info("===== 크롤링 완료 =====")
    return len(articles)


# ── 비동기 DB 조회를 포함한 LLM 분석 작업 ──────────────────────────────
async def analyze_job(batch_size: int = BATCH_SIZE) -> tuple[int, int]:
    """
    분석 대기 기사를 최대 batch_size건 분석합니다. (처리한 기사 수, 성공한 기사 수)를 반환합니다.
    - 대상: 종목이 연결된 기사 중 평가가 없거나 ERROR인 기사
    - 아직 한 번도 분석 안 된 기사를 먼저, 그다음 ERROR 재시도 (각각 최신순)
    - 기사 하나 끝날 때마다 커밋 → 중간에 종료해도 그때까지의 결과는 남는다
    """
    async with AsyncSessionLocal() as session:
        # 1) 이번 배치에서 분석할 기사 id 선정
        has_stock = ArticleModel.id.in_(select(ArticleStockModel.article_id))
        id_stmt = (
            select(ArticleModel.id)
            .outerjoin(ArticleEvaluationModel, ArticleModel.id == ArticleEvaluationModel.article_id)
            .where(has_stock)
            .where(
                or_(
                    ArticleEvaluationModel.article_id.is_(None),
                    ArticleEvaluationModel.status == 'ERROR',
                )
            )
            .order_by(ArticleEvaluationModel.article_id.is_not(None), ArticleModel.id.desc())
            .limit(batch_size)
        )
        target_ids = [i for (i,) in (await session.execute(id_stmt)).all()]
        if not target_ids:
            return 0, 0

        # 2) 기사 + 대표 종목(rank 가장 작은 것) 조회
        # DISTINCT ON(id) + rank 오름차순 → 기사마다 대표 종목 한 행만 남는다
        stmt = (
            select(ArticleModel, ArticleStockModel.stock_code, StockModel.market, StockModel.stock_name)
            .distinct(ArticleModel.id)
            .join(ArticleStockModel, ArticleModel.id == ArticleStockModel.article_id)
            .outerjoin(StockModel, StockModel.stock_code == ArticleStockModel.stock_code)
            .where(ArticleModel.id.in_(target_ids))
            .order_by(ArticleModel.id.desc(), ArticleStockModel.rank.asc())
        )
        rows = (await session.execute(stmt)).all()

        total_count = len(rows)
        succeeded = 0
        logger.info("===== LLM 분석 배치 시작: %d건 =====", total_count)

        for idx, (article, stock_code, market, stock_name) in enumerate(rows, 1):
            ticker = to_ticker(stock_code, market)
            stock_disp = stock_code if stock_code else "미지정(None)"

            logger.info(f"[{idx}/{total_count}] 기사 ID {article.id} (종목: {stock_disp}) 분석 시작...")

            try:
                report = await asyncio.to_thread(analyzer.analyze, ticker, article.content, stock_name)

                if "error" not in report:
                    values = dict(
                        article_id=article.id,
                        status='COMPLETED',
                        score=report.get('score', 0),
                        report=json.dumps(report, ensure_ascii=False)
                    )
                    succeeded += 1
                    logger.info(f"[{idx}/{total_count}] 기사 ID {article.id} 분석 완료 (점수: {report.get('score', 0)}점)")
                else:
                    values = dict(
                        article_id=article.id,
                        status='ERROR',
                        score=None,
                        report=report["error"]
                    )
                    logger.warning(f"[{idx}/{total_count}] 기사 ID {article.id} 분석 실패: {report['error']}")

                # 기존 ERROR 행이 있으면 덮어쓰기(재분석 결과 반영)
                eval_stmt = pg_insert(ArticleEvaluationModel).values(**values)
                eval_stmt = eval_stmt.on_conflict_do_update(
                    index_elements=[ArticleEvaluationModel.article_id],
                    set_={k: v for k, v in values.items() if k != 'article_id'},
                )
                await session.execute(eval_stmt)
                await session.commit()

            except Exception as e:
                await session.rollback()
                logger.error(f"기사 ID {article.id} 저장 중 오류: {e}")

        logger.info("===== LLM 분석 배치 완료: %d건 중 %d건 성공 =====", total_count, succeeded)
        return total_count, succeeded


# ── 루프 ──────────────────────────────────────────────────────
async def crawl_loop(manager: SecuritiesNewsCrawlerManager, interval_minutes: int):
    """외부 요청이므로 interval_minutes마다 한 번만 크롤링"""
    while True:
        try:
            await crawl_job(manager)
        except Exception as e:
            logger.error("크롤링 작업 중 오류 발생: %s", e)

        logger.info("다음 크롤링까지 %d분 대기...", interval_minutes)
        await asyncio.sleep(interval_minutes * 60)


async def analyze_loop(idle_minutes: int):
    """로컬 LLM 분석: 대기 기사가 있으면 쉬지 않고 이어서, 없거나 전부 실패하면 idle_minutes 쉬고 재확인"""
    while True:
        try:
            processed, succeeded = await analyze_job()
        except Exception as e:
            logger.error("분석 작업 중 오류 발생: %s", e)
            processed, succeeded = 0, 0

        if processed == 0:
            logger.info("분석할 기사가 없습니다. %d분 후 다시 확인합니다.", idle_minutes)
            await asyncio.sleep(idle_minutes * 60)
        elif succeeded == 0:
            # Ollama가 꺼져 있는 등 전부 실패한 경우 같은 기사를 계속 두드리지 않도록 쉬어 간다
            logger.warning("이번 배치가 모두 실패했습니다(Ollama 실행 여부 확인). %d분 후 재시도합니다.", idle_minutes)
            await asyncio.sleep(idle_minutes * 60)
        else:
            await asyncio.sleep(1)


async def run_scheduler(interval_minutes: int, idle_minutes: int):
    await init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=1)
    logger.info(
        "스케줄러 시작 — 크롤링 %d분 간격, 분석은 대기 기사가 있으면 계속 (없으면 %d분 대기). Ctrl+C로 종료",
        interval_minutes, idle_minutes,
    )

    await asyncio.gather(
        crawl_loop(manager, interval_minutes),
        analyze_loop(idle_minutes),
    )


async def run_once():
    await init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=1)
    await crawl_job(manager)
    await analyze_job()


async def run_analyze_only(idle_minutes: int, once: bool):
    """크롤링 없이 LLM 분석만 실행 (이미 DB에 쌓인 기사 대상)"""
    await init_db()

    if not once:
        logger.info("분석 전용 모드 시작 — 크롤링 없이 대기 기사를 계속 분석합니다. (Ctrl+C로 종료)")
        await analyze_loop(idle_minutes)
        return

    # --once: 밀린 기사를 모두 분석하고 종료
    logger.info("분석 전용 1회 모드 — 대기 기사를 모두 분석한 뒤 종료합니다.")
    total_processed = total_succeeded = 0
    while True:
        processed, succeeded = await analyze_job()
        total_processed += processed
        total_succeeded += succeeded
        if processed == 0:
            break
        if succeeded == 0:
            logger.warning("이번 배치가 모두 실패해 중단합니다(Ollama 실행 여부 확인).")
            break
    logger.info("분석 전용 모드 종료 — 총 %d건 처리, %d건 성공", total_processed, total_succeeded)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="증권 뉴스 자동 크롤러")
    parser.add_argument("--interval", type=int, default=60, help="크롤링 간격 (분 단위, 기본값: 60)")
    parser.add_argument("--idle", type=int, default=5, help="분석할 기사가 없을 때 쉬는 시간 (분 단위, 기본값: 5)")
    parser.add_argument("--once", action="store_true", help="1회만 실행하고 종료")
    parser.add_argument("--analyze-only", action="store_true", help="크롤링 없이 LLM 분석만 실행")
    args = parser.parse_args()

    if args.analyze_only:
        asyncio.run(run_analyze_only(args.idle, args.once))
    elif args.once:
        asyncio.run(run_once())
    else:
        asyncio.run(run_scheduler(args.interval, args.idle))
