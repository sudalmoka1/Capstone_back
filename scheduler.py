"""
scheduler.py — 자동 크롤링 및 LLM 분석 스케줄러
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from analyzer import LLMAnalyzer
from api.storage import (
    ArticleEvaluationModel,
    ArticleModel,
    ArticleStockModel,
    AsyncSessionLocal,
    init_db,
)
from crawlers.manager import SecuritiesNewsCrawlerManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# LLMAnalyzer 인스턴스 전역 생성
analyzer = LLMAnalyzer(model_name="llama3")


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
async def analyze_job():
    logger.info("===== LLM 텍스트 분석 시작 =====")
    
    async with AsyncSessionLocal() as session:
        # 💡 distinct(ArticleModel.id)를 추가하여 기사 중복 조회 방지
        stmt = (
            select(ArticleModel, ArticleStockModel.stock_code)
            .distinct(ArticleModel.id)
            .outerjoin(ArticleEvaluationModel, ArticleModel.id == ArticleEvaluationModel.article_id)
            .outerjoin(ArticleStockModel, ArticleModel.id == ArticleStockModel.article_id)
            .where(ArticleEvaluationModel.article_id.is_(None))
            .order_by(ArticleModel.id.desc())
            .limit(50)
        )
        result = await session.execute(stmt)
        rows = result.all()

        total_count = len(rows)
        if total_count == 0:
            logger.info("분석할 새로운 기사가 없습니다.")
            return

        logger.info(f"총 {total_count}건의 고유 기사 분석을 시작합니다.")

        for idx, (article, stock_code) in enumerate(rows, 1):
            ticker = f"{stock_code}.KS" if stock_code else None
            stock_disp = stock_code if stock_code else "미지정(None)"

            logger.info(f"[{idx}/{total_count}] 기사 ID {article.id} (종목: {stock_disp}) 분석 시작...")
            
            try:
                async with session.begin_nested():
                    report = await asyncio.to_thread(analyzer.analyze, ticker, article.content)

                    if "error" not in report:
                        eval_stmt = pg_insert(ArticleEvaluationModel).values(
                            article_id=article.id,
                            status='COMPLETED',
                            score=report.get('score', 0),
                            report=json.dumps(report, ensure_ascii=False)
                        )
                        logger.info(f"[{idx}/{total_count}] 기사 ID {article.id} 분석 완료 (점수: {report.get('score', 0)}점)")
                    else:
                        eval_stmt = pg_insert(ArticleEvaluationModel).values(
                            article_id=article.id,
                            status='ERROR',
                            report=report["error"]
                        )
                        logger.warning(f"[{idx}/{total_count}] 기사 ID {article.id} 분석 실패: {report['error']}")

                    await session.execute(eval_stmt)

            except Exception as e:
                logger.error(f"기사 ID {article.id} 저장 중 오류: {e}")

        await session.commit()
        logger.info("===== LLM 텍스트 분석 완료 =====")


# ── asyncio while-loop 스케줄러 ───────────────────────────────
async def run_scheduler(interval_minutes: int):
    await init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=3)
    logger.info("스케줄러 시작 — %d분 간격으로 실행됩니다. (Ctrl+C로 종료)", interval_minutes)

    while True:
        try:
            await crawl_job(manager)
            await analyze_job()
        except Exception as e:
            logger.error("스케줄러 작업 중 오류 발생: %s", e)

        logger.info("다음 실행까지 %d분 대기...", interval_minutes)
        await asyncio.sleep(interval_minutes * 60)


async def run_once():
    await init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=3)
    await crawl_job(manager)
    await analyze_job()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="증권 뉴스 자동 크롤러")
    parser.add_argument("--interval", type=int, default=30, help="크롤링 간격 (분 단위, 기본값: 30)")
    parser.add_argument("--once", action="store_true", help="1회만 실행하고 종료")
    args = parser.parse_args()

    if args.once:
        asyncio.run(run_once())
    else:
        asyncio.run(run_scheduler(args.interval))