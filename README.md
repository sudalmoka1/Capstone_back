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
├── crawlers/
│   ├── news_crawler.py   # 네이버 뉴스 언론사별 증권 기사 수집
│   ├── manager.py        # 수집 → 본문 → 종목 매칭 → 저장 파이프라인
│   ├── stock_master.py   # 종목 마스터(KOSPI/KOSDAQ) 수집
│   └── stock_matcher.py  # 기사 ↔ 종목 매칭
└── evaluation/           # AI 판정 검증 (사람 라벨링 vs AI 일치도)
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

## AI 판정 검증 (사람 라벨링)

AI 판정이 사람의 판단과 얼마나 맞는지 측정합니다. 결과 파일은 `evaluation/data/`에 저장되며, 기사 본문이 들어 있어 git에 올리지 않습니다.

### 1. 표본 추출

```powershell
python -m evaluation.export_samples --n 60
```

`evaluation/data/labels_template.csv`가 생성됩니다. 이 파일을 라벨링할 사람 수만큼 `labels_이름.csv`로 복사해 나눠 줍니다. **모두 같은 표본을 라벨링해야** 사람끼리의 일치도도 계산할 수 있습니다.

- AI 점수별 비율에 맞춰 뽑고, AI가 과장으로 판정한 기사는 모두 포함합니다. 과장 판정은 전체의 5% 정도라 무작위로만 뽑으면 너무 적기 때문입니다.
- 파일에는 AI 판정을 넣지 않고, 행 순서도 섞습니다.

### 2. 라벨링 (엑셀)

각 행의 `exaggeration`, `investment_basis`, `data_based`, `risk_explanation` 열에 `O` 또는 `X`를 입력합니다 (`1`/`0`도 가능). `memo` 열은 자유롭게 씁니다.
저장할 때는 **[다른 이름으로 저장] → "CSV UTF-8(쉼표로 분리)"**를 고르세요. "Excel 통합 문서"로 저장하면 이름은 .csv여도 계산 스크립트가 읽지 못합니다.

**규칙**
- `title`과 `content`만 보고 판단합니다. `content`는 AI가 실제로 본 범위(본문 앞 1500자)입니다.
- 다른 사람과 상의하지 않고 혼자 판단합니다.
- 주식과 무관한 기사(부고, 인사 등)도 기준대로 판정하고 `memo`에 "주식 무관"이라고 적습니다.

| 열 | O로 판정하는 경우 |
|---|---|
| `exaggeration` (과장 표현) | 투자 기대감이나 공포감을 과도하게 부추기는 표현이 **하나라도** 있음. 예: 폭등, 불기둥, 잭팟, 역대급, 싹쓸이, 폭락, 증발, 휘청, "반드시 오른다", 일회성 사건을 과대 해석한 전망, 본문보다 훨씬 자극적인 제목. **수치를 그대로 전한 문장("3% 올랐다")이나 공시, 출처가 분명한 인용은 과장이 아님** |
| `investment_basis` (투자 근거) | 실적, 수치, 논리적 근거가 구체적으로 제시됨 |
| `data_based` (데이터 부합) | 기사 속 수치와 주장이 서로 모순되지 않고, 근거가 분명함 |
| `risk_explanation` (리스크 설명) | 손실 가능성이나 하락 요인을 구체적으로 언급함 |

> `data_based`의 경우, AI는 분석 시점의 야후파이낸스 지표(현재가, 목표주가 등)도 함께 봤습니다. 사람은 기사 안의 일관성 위주로 판단하므로 이 항목은 기준이 조금 다릅니다. 결과를 해석할 때 감안하세요.

### 3. 일치도 계산

```powershell
# 한 명
python -m evaluation.agreement evaluation/data/labels_조성목.csv

# 여러 명 + 결과 저장 + 불일치 사례 목록
python -m evaluation.agreement evaluation/data/labels_A.csv evaluation/data/labels_B.csv `
    --out evaluation/data/result.md --diff evaluation/data/diff.csv
```

- 항목별 **일치율**과 **Cohen's κ**(우연히 맞을 확률을 뺀 일치도)를 계산합니다. 해석 기준은 0.4 이하 낮음, 0.4~0.6 보통, 0.6~0.8 상당, 0.8 이상 거의 완벽입니다 (Landis & Koch).
- 사람 라벨에도 같은 공식을 적용해 점수 차이(MAE)와 상관계수를 계산합니다.
- 라벨 파일이 2개 이상이면 **사람끼리의 일치도**도 계산합니다. AI와 사람의 일치도가 이 값에 가까울수록 AI가 사람 수준으로 판단한다는 뜻입니다.
- `--diff`로 만든 파일에는 사람과 AI의 판정이 다른 기사가 정리됩니다. 발표용 오판 사례를 찾을 때 쓰면 됩니다.

### 4. (선택) 다른 모델과 비교

```powershell
ollama pull llama3
python -m evaluation.run_model evaluation/data/labels_template.csv --model llama3
python -m evaluation.agreement evaluation/data/labels_조성목.csv --preds evaluation/data/preds_llama3.csv
```

같은 표본을 다른 모델로 다시 분석해 CSV로 저장합니다 (DB에는 저장하지 않음). 모델별 일치도를 비교하면 모델 선정 근거가 됩니다.

### 5. 과장 사전 규칙

7B 모델이 과장 표현을 대부분 놓쳐서, `analyzer.py`의 자극적 표현 사전(`HYPE_PATTERNS`)으로 **제목**을 검사해 보완합니다. (LLM 판정 → 사전 검사 순서로 적용)

| 과장 표현 κ (사람 대비) | 개발용 100건 | 새 기사 50건 |
|---|---|---|
| LLM만 | 0.29 | 0.00 |
| LLM + 사전 | 0.58 | 0.25 |

```powershell
# 저장된 판정에 규칙을 적용한 결과를 CSV로 (DB는 그대로) → agreement.py --preds 로 비교
python -m evaluation.apply_rule --scope title

# DB의 기존 판정에 규칙 반영 (반영 전 판정은 evaluation/data/backup_evaluations_*.json, preds_db_before_*.csv 로 백업)
python -m evaluation.apply_rule --apply-db
```

사전을 고칠 때는 라벨링한 기사를 보며 단어를 추가하지 말고(과적합), 일반적인 표현으로 고친 뒤 새 기사로 확인하세요.

## 면책

학업 목적의 비영리 프로젝트입니다. 분석 결과는 투자 권유가 아니며, 기사 저작권은 각 언론사에 있습니다. 기사 본문은 분석에만 사용하고 API로 제공하지 않습니다.
