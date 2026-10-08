"""
main.py

[서버 실행]    uvicorn main:app --reload  (또는 python main.py)
[단독 테스트]  python main.py test
[자동 스케줄]  python scheduler.py --interval 60  (크롤링 + LLM 분석, 별도 터미널)
"""

import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/health")
async def health():
    return {"status": "ok"}

# ── 단독 테스트 (1회 실행) ────────────────────────────────────
async def _test():
    from collections import Counter
    from crawlers.manager import SecuritiesNewsCrawlerManager
    from api import storage

    print("\n" + "=" * 55)
    print("  증권 뉴스 크롤러 단독 테스트 시작 (본문 수집 활성화)")
    print("=" * 55)
    print("[테스트] PostgreSQL 테이블 연결 및 초기화 중...")
    await storage.init_db()

    manager = SecuritiesNewsCrawlerManager(max_pages=1)

    print("[테스트] 크롤러 매니저 가동...")
    articles = await manager.run(fetch_content=True)

    # 언론사(publisher) 기준으로 집계
    stats = Counter(a.publisher for a in articles)

    print(f"\n언론사별 수집 현황:")
    for src, cnt in stats.items():
        print(f"   {src}: {cnt}건")
    print(f"   합계: {len(articles)}건\n")

    await storage.engine.dispose()


# ── 실행 제어 스위치 ──────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # 💡 test 명령어가 들어오면 uvicorn을 켜지 않고 단독 테스트만 바로 실행
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(_test())

    # 💡 그냥 python main.py 로 실행할 때만 uvicorn 가동
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)