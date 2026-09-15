"""
main.py

[서버 실행]    uvicorn main:app --reload
[단독 테스트]  python main.py
[자동 스케줄]  python scheduler.py --interval 30
"""

import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from api.routes import router
from api.storage import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# 애플리케이션 시작 시 DB 테이블 자동 생성
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("서버 시작: PostgreSQL 데이터베이스 초기화 진행 중...")
    await init_db()
    yield
    logging.info("서버 종료 중...")

app = FastAPI(
    title="증권 뉴스 크롤러 API",
    description="네이버 금융 + 한국경제 증권 뉴스 수집 및 신뢰도 평가",
    version="0.3.0",
    lifespan=lifespan
)
app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


# ── 단독 테스트 (1회 실행) ────────────────────────────────────
async def _test():
    from collections import Counter
    from crawlers.manager import SecuritiesNewsCrawlerManager
    from api import storage  # 패키지 경로에 맞게 수정

    print("\n" + "=" * 55)
    print("  증권 뉴스 크롤러 단독 테스트 시작 (본문 수집 활성화)")
    print("=" * 55)
    print("[테스트] PostgreSQL 테이블 연결 및 초기화 중...")
    await storage.init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=1)

    print("[테스트] 크롤러 매니저 가동...")
    articles = await manager.run(fetch_content=True)

    stats = Counter(a.source for a in articles)
    print(f"\n소스별 수집 현황:")
    for src, cnt in stats.items():
        print(f"   {src}: {cnt}건")
    print(f"   합계: {len(articles)}건\n")


# ── 실행 제어 스위치 ──────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # 💡 터미널에 `python main.py test` 라고 치면 크롤러만 1번 딱 실행하고 종료됩니다.
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(_test())

    # 💡 그냥 `python main.py` 나 `uvicorn`으로 켜면 얌전하게 웹 서버 모드로 대기합니다.
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)