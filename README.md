# 증권 뉴스 해체 분석기 — 백엔드

서일대학교 캡스톤 프로젝트. 네이버 뉴스의 언론사별 증권 기사를 수집하고, 로컬 LLM으로 기사의 신뢰도(과장 표현, 투자 근거, 데이터 부합, 리스크 언급)를 평가합니다. 기사가 나온 뒤 실제 주가가 어떻게 움직였는지 계산해 AI 점수가 의미 있는지도 검증합니다.

- 백엔드 (이 저장소): https://github.com/sudalmoka1/Capstone_back
- 프론트엔드: https://github.com/sudalmoka1/Capstone_front

```
[네이버 뉴스] ──크롤링──▶ [PostgreSQL] ◀──분석 결과── [Ollama LLM]
                              │    ▲
                              │    └──주가 반응── [Yahoo Finance]
                              ▼
                       [FastAPI :8000] ──▶ [Vue 프론트 :5173]
```

## 구조

```
├── main.py               # API 서버 진입점
├── scheduler.py          # 크롤링 / LLM 분석 / 주가 반응 계산 루프
├── analyzer.py           # LLM 신뢰도 평가 (qwen2.5:7b)
├── price_reaction.py     # 기사 발행 후 1일·5일 (초과)수익률 계산
├── api/
│   ├── routes.py         # REST API
│   └── storage.py        # DB 모델 및 저장 로직
└── crawlers/
    ├── news_crawler.py   # 네이버 뉴스 언론사별 증권 기사 수집
    ├── manager.py        # 수집 → 본문 → 종목 매칭 → 저장 파이프라인
    ├── stock_master.py   # 종목 마스터(KOSPI/KOSDAQ) 수집
    └── stock_matcher.py  # 기사 ↔ 종목 매칭
```

## 사전 준비

| 항목 | 버전 / 비고 |
|---|---|
| Python | 3.12 |
| PostgreSQL | `securities_news` 데이터베이스 생성 필요 |
| Ollama | `ollama pull qwen2.5:7b` 로 모델 설치 |

DB 접속 정보는 환경 변수 `DATABASE_URL`로 지정합니다. 지정하지 않으면 `postgresql+asyncpg://postgres:1234@localhost:5432/securities_news`를 사용합니다.

```powershell
$env:DATABASE_URL = "postgresql+asyncpg://사용자:비밀번호@localhost:5432/securities_news"
```

## 설치

```powershell
git clone https://github.com/sudalmoka1/Capstone_back.git
cd Capstone_back
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 실행 순서

테이블은 처음 실행할 때 자동으로 생성됩니다.

1. **종목 마스터 수집** (최초 1회. 비어 있으면 크롤링 시 자동으로 수집됨)
   ```powershell
   python -m crawlers.stock_master
   ```
2. **Ollama 실행** (`ollama serve`, 또는 Ollama 앱 실행)
3. **스케줄러 실행** (별도 터미널): 크롤링 60분 간격, 분석·주가 반응 계산은 자동
   ```powershell
   python scheduler.py
   ```
4. **API 서버 실행** (별도 터미널): http://localhost:8000/docs 에서 API 문서 확인
   ```powershell
   python main.py
   ```
5. **프론트엔드 실행**: [Capstone_front](https://github.com/sudalmoka1/Capstone_front) README 참고

### 스케줄러 옵션

| 명령 | 설명 |
|---|---|
| `python scheduler.py --interval 60 --idle 5` | 크롤링 간격 / 분석 대기 없을 때 쉬는 시간(분) |
| `python scheduler.py --once` | 크롤링 1번 + 분석 1배치 후 종료 |
| `python scheduler.py --analyze-only` | 크롤링 없이 분석만 계속 |
| `python scheduler.py --analyze-only --once` | 밀린 기사 전부 분석 후 종료 |

### 기타 유틸리티

| 명령 | 설명 |
|---|---|
| `python main.py test` | 크롤러 단독 테스트 (1회 수집) |
| `python price_reaction.py` | 주가 반응 수동 계산 |
| `python -m crawlers.stock_matcher --backfill` | 저장된 기사에 종목 다시 연결 |

## 주요 API

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/articles` | 기사 + AI 평가 + 연결 종목 + 주가 반응 |
| GET | `/api/publishers` | 언론사별 기사 수 |
| GET | `/api/stocks/ranking` | 종목별 언급 순위 / 평균 신뢰도 |
| GET | `/api/stocks/{code}/chart` | 종목 일봉 데이터 |
| GET | `/api/reactions/summary` | AI 점수별 기사 후 주가 반응 집계 |
| GET | `/api/status` | 스케줄러 가동 상태 |

## 신뢰도 점수

LLM이 아래 4개 항목을 판정하고, 항목마다 25점씩 감점합니다 (100점 만점).

- 과장 표현이 있음 (LLM이 뽑은 문구가 실제 기사에 있는지 코드로 재검증)
- 구체적인 투자 근거가 없음
- 시장 데이터와 맞지 않음
- 리스크 설명이 없음

## 면책

학업 목적의 비영리 프로젝트입니다. 분석 결과는 투자 권유가 아니며, 기사 저작권은 각 언론사에 있습니다. 기사 본문은 분석에만 사용하고 API로 제공하지 않습니다.
