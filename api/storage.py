import os
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Text, DateTime, ForeignKey, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
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
    pool_pre_ping=True,    # 💡 중요: 연결이 살아있는지 미리 확인하는 옵션
    connect_args={
        "timeout": 30      # 💡 연결 타임아웃 시간을 30초로 늘림
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
    source: Mapped[str] = mapped_column(String(50), nullable=False)  # 네이버금융 / 한국경제 구별용 컬럼 유지
    publisher_id: Mapped[Optional[int]] = mapped_column(ForeignKey("publishers.id", ondelete="SET NULL"), nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# 3. 주식 종목 마스터 모델
class StockModel(Base):
    __tablename__ = "stocks"

    stock_code: Mapped[str] = mapped_column(String(20), primary_key=True)
    stock_name: Mapped[str] = mapped_column(String(100), nullable=False)


# 4. 기사-주식 다대다 매핑 모델
class ArticleStockModel(Base):
    __tablename__ = "article_stocks"

    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    stock_code: Mapped[str] = mapped_column(ForeignKey("stocks.stock_code", ondelete="CASCADE"), primary_key=True)


# 데이터베이스 초기화 함수 (main.py의 lifespan에서 호출됨)
async def init_db():
    try:
        async with engine.begin() as conn:
            # 주석 처리: 이미 pgAdmin에서 테이블을 생성하셨다면 아래 라인은 생략해도 무방하나,
            # 모델 검증 및 자동 생성을 위해 유지하는 것이 안전합니다.
            await conn.run_sync(Base.metadata.create_all)
        logger.info("PostgreSQL 테이블 모델 연결 성공")
    except Exception as e:
        logger.error("PostgreSQL 초기화 실패: %s", e)


# ── 💡 핵심 복합 테이블 자동 저장 로직 (Upsert Pipeline) ──
async def save_articles_to_db(articles) -> None:
    if not articles:
        return

    async with AsyncSessionLocal() as session:
        async with session.begin():
            for a in articles:
                # [단계 1] 언론사(Publisher) 저장 및 ID 가져오기
                # 언론사 명이 없는 경우 기본값 처리
                pub_name = a.publisher if a.publisher else (a.source if a.source else "기타")

                pub_stmt = pg_insert(PublisherModel).values(name=pub_name)
                # 이미 존재하면 아무것도 안 함 (DO NOTHING), 대신 기존 id를 조회하기 위함
                pub_upsert = pub_stmt.on_conflict_do_nothing(index_elements=['name'])
                await session.execute(pub_upsert)

                # 실제 생성되거나 기존에 있던 publisher의 ID 조회
                pub_result = await session.execute(select(PublisherModel.id).where(PublisherModel.name == pub_name))
                current_pub_id = pub_result.scalar_one()

                # [단계 2] 뉴스 기사(Article) Upsert 실행
                # 중복 기준인 'url'이 부딪히면 제목, 본문, 발행일, 언론사 정보를 최신으로 업데이트합니다.
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
                        'publisher_id': art_stmt.excluded.publisher_id
                    }
                )
                await session.execute(art_upsert)

                # 매핑 테이블에 넣기 위해 방금 넣은(혹은 업데이트된) 기사의 id 조회
                art_id_result = await session.execute(select(ArticleModel.id).where(ArticleModel.url == a.url))
                current_art_id = art_id_result.scalar_one()

                # [단계 3] 주식 종목(Stocks) 및 매핑(Article_Stocks) 저장
                # 크롤러 결과 데이터 내부에 관련 주식이 파싱되어 들어있을 경우에만 작동합니다.
                if hasattr(a, 'related_stocks') and a.related_stocks:
                    for stock in a.related_stocks:
                        # stock 변수가 문자열 종목코드('005930')라면 마스터 테이블에 먼저 등록되어야 함
                        # 종목명 파싱이 어려울 경우 기본값으로 코드를 임시 배치하거나, 사전에 마스터를 채워두는 것이 정석입니다.
                        # 여기선 무결성 에러 방지를 위해 가상의 이름을 넣어 stocks 마스터를 채웁니다.
                        stock_stmt = pg_insert(StockModel).values(
                            stock_code=stock,
                            stock_name=f"종목_{stock}"  # 대시보드 고도화 시 실제 종목명 매핑 테이블과 연동 권장
                        ).on_conflict_do_nothing(index_elements=['stock_code'])
                        await session.execute(stock_stmt)

                        # 마지막으로 기사-주식 연결 쌍을 매핑 테이블에 저장 (중복 쌍은 무시)
                        link_stmt = pg_insert(ArticleStockModel).values(
                            article_id=current_art_id,
                            stock_code=stock
                        ).on_conflict_do_nothing(index_elements=['article_id', 'stock_code'])
                        await session.execute(link_stmt)

            # 루프가 끝나면 한 번에 커밋(Transaction 완료)
            await session.commit()