import os
import logging
from datetime import date, datetime
from typing import Optional
from sqlalchemy import Date, DateTime, Float, ForeignKey, Integer, String, Text, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)

# [연결 설정] 환경 변수가 없으면 localhost의 securities_news 데이터베이스에 접속합니다.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:1234@localhost:5432/securities_news"
)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=10,          # 기본 커넥션 풀 크기
    max_overflow=20,       # 초과 허용 풀 크기
    pool_recycle=1800,     # 연결 재사용 주기 (초)
    pool_pre_ping=True,    # 연결이 살아있는지 미리 확인하는 옵션
    connect_args={
        "timeout": 30      # 연결 타임아웃 시간을 30초로 늘림
    }
)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


# 1. 언론사(출처) 모델
class PublisherModel(Base):
    __tablename__ = "publishers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)


# 2. 뉴스 기사 메인 모델
class ArticleModel(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    publisher_id: Mapped[Optional[int]] = mapped_column(ForeignKey("publishers.id", ondelete="SET NULL"), nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# 2-1. 기사 평가(LLM 분석 결과) 모델
class ArticleEvaluationModel(Base):
    __tablename__ = "article_evaluations"

    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default="PENDING")
    score: Mapped[Optional[int]] = mapped_column(nullable=True)
    report: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


# 3. 주식 종목 마스터 모델
class StockModel(Base):
    __tablename__ = "stocks"

    stock_code: Mapped[str] = mapped_column(String(20), primary_key=True)
    stock_name: Mapped[str] = mapped_column(String(100), nullable=False)
    market: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # KOSPI / KOSDAQ


# 4. 기사-주식 다대다 매핑 모델
class ArticleStockModel(Base):
    __tablename__ = "article_stocks"

    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    stock_code: Mapped[str] = mapped_column(ForeignKey("stocks.stock_code", ondelete="CASCADE"), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, default=0, server_default="0")  # 0 = 대표 종목, 1, 2 ... 순


# 4-1. 기사 발행 후 실제 주가 반응 (AI 점수 검증용, price_reaction.py가 계산)
class ArticlePriceReactionModel(Base):
    __tablename__ = "article_price_reactions"

    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)   # 계산에 쓴 대표 종목
    base_date: Mapped[date] = mapped_column(Date, nullable=False)         # 기준가(기사 직전 종가) 날짜
    # 수익률은 % 단위. 아직 해당 거래일이 지나지 않았으면 NULL → 다음 계산 때 채움
    ret_1d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ret_5d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    excess_1d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # 시장지수 대비 초과 수익률 (%p)
    excess_5d: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# 5. 스케줄러 상태 (한 행만 사용, id=1) — 별도 프로세스인 scheduler.py가 주기적으로 갱신
class SchedulerStatusModel(Base):
    __tablename__ = "scheduler_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    mode: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # full / analyze-only
    # 시각은 DB의 now()로 기록 → API에서도 DB 기준으로 경과 시간을 계산해 시계/시간대 차이 문제를 피함
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_crawl_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_analyze_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


async def update_scheduler_status(
    *, mode: Optional[str] = None, heartbeat: bool = False, crawled: bool = False,
    analyzed: bool = False, stopped: bool = False,
) -> None:
    """스케줄러 상태 행을 갱신합니다. 상태 기록 실패가 스케줄러를 멈추지 않도록 예외는 로그만 남깁니다."""
    values = {}
    if mode is not None:
        values["mode"] = mode
    if heartbeat:
        values["heartbeat_at"] = func.now()
    if stopped:
        values["heartbeat_at"] = None
    if crawled:
        values["last_crawl_at"] = func.now()
    if analyzed:
        values["last_analyze_at"] = func.now()
    if not values:
        return
    try:
        async with AsyncSessionLocal() as session:
            stmt = pg_insert(SchedulerStatusModel).values(id=1, **values)
            stmt = stmt.on_conflict_do_update(index_elements=[SchedulerStatusModel.id], set_=values)
            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        logger.warning("스케줄러 상태 기록 실패: %s", e)


# 데이터베이스 초기화 함수 (main.py의 lifespan에서 호출됨)
async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # create_all은 기존 테이블에 컬럼을 추가하지 않으므로 market 컬럼을 직접 보강
            await conn.execute(text("ALTER TABLE stocks ADD COLUMN IF NOT EXISTS market VARCHAR(10)"))
            await conn.execute(text("ALTER TABLE article_stocks ADD COLUMN IF NOT EXISTS rank INTEGER NOT NULL DEFAULT 0"))
        logger.info("PostgreSQL 테이블 모델 연결 성공")
    except Exception as e:
        logger.error("PostgreSQL 초기화 실패: %s", e)


async def get_existing_urls(urls: list[str]) -> set[str]:
    """주어진 URL 중 이미 articles 테이블에 저장된 것만 반환 (본문 재수집 방지용)"""
    if not urls:
        return set()
    async with AsyncSessionLocal() as session:
        rows = await session.execute(select(ArticleModel.url).where(ArticleModel.url.in_(urls)))
        return {u for (u,) in rows.all()}


# ── 💡 핵심 복합 테이블 자동 저장 로직 (Upsert Pipeline) ──
async def save_articles_to_db(articles) -> None:
    if not articles:
        return

    async with AsyncSessionLocal() as session:
        async with session.begin():
            for a in articles:
                # [단계 1] 언론사(Publisher) 저장 및 ID 가져오기
                pub_name = a.publisher if a.publisher else (a.source if a.source else "기타")

                pub_stmt = pg_insert(PublisherModel).values(name=pub_name)
                pub_upsert = pub_stmt.on_conflict_do_nothing(index_elements=['name'])
                await session.execute(pub_upsert)

                pub_result = await session.execute(select(PublisherModel.id).where(PublisherModel.name == pub_name))
                current_pub_id = pub_result.scalar_one()

                # [단계 2] 뉴스 기사(Article) Upsert 실행
                art_stmt = pg_insert(ArticleModel).values(
                    title=a.title,
                    url=a.url,
                    content=a.content,
                    source=a.source,
                    publisher_id=current_pub_id,
                    published_at=a.published_at
                )

                art_upsert = art_stmt.on_conflict_do_update(
                    index_elements=['url'],
                    set_={
                        'title': art_stmt.excluded.title,
                        'content': art_stmt.excluded.content,
                        'published_at': art_stmt.excluded.published_at,
                        'publisher_id': art_stmt.excluded.publisher_id,
                        'source': art_stmt.excluded.source  # 💡 [핵심] 수집 출처(source)도 한국경제로 덮어쓰도록 추가
                    }
                )
                await session.execute(art_upsert)

                art_id_result = await session.execute(select(ArticleModel.id).where(ArticleModel.url == a.url))
                current_art_id = art_id_result.scalar_one()

                # [단계 3] 주식 종목(Stocks) 및 매핑(Article_Stocks) 저장
                if hasattr(a, 'related_stocks') and a.related_stocks:
                    # related_stocks는 대표 종목이 앞에 오는 순서 → rank로 저장
                    for rank, stock in enumerate(a.related_stocks):
                        stock_stmt = pg_insert(StockModel).values(
                            stock_code=stock,
                            stock_name=f"종목_{stock}"
                        ).on_conflict_do_nothing(index_elements=['stock_code'])
                        await session.execute(stock_stmt)

                        link_stmt = pg_insert(ArticleStockModel).values(
                            article_id=current_art_id,
                            stock_code=stock,
                            rank=rank
                        )
                        link_stmt = link_stmt.on_conflict_do_update(
                            index_elements=['article_id', 'stock_code'],
                            set_={'rank': link_stmt.excluded.rank}
                        )
                        await session.execute(link_stmt)

            # 루프가 정상 종료되면 한 번에 커밋
            await session.commit()