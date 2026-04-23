# Daily Financial Market Morning Brief

매일 아침 발행하는 금융시장 모닝 브리프 자동 생성기. 시장 데이터 수집 → Claude로 Executive Summary 작성 → HTML 리포트 출력.

## 빠른 시작

```bash
cd ~/Desktop/db

# 1. 가상환경 + 의존성
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. API 키 설정
cp .env.example .env
# .env 파일을 열어 ANTHROPIC_API_KEY 입력

# 3. 실행
python generate.py
```

출력은 `reports/YYYY/MM/DD/daily_report_*.html`에 저장됩니다. 브라우저로 직접 열거나 간단한 HTTP 서버로 서빙하세요:

```bash
python -m http.server 9999 --directory reports
# http://localhost:9999/2026/04/23/daily_report_*.html
```

## 구조

| 파일 | 역할 |
|---|---|
| `collectors.py` | Yahoo Finance, Google News RSS 데이터 수집 |
| `analyzer.py` | Claude API로 Executive Summary 생성 (JSON 출력) |
| `renderer.py` | Jinja2로 HTML 렌더링 + 파일 저장 |
| `generate.py` | 메인 진입점 (수집 → 분석 → 렌더 파이프라인) |
| `templates/report.html.j2` | 다크 테마 리포트 템플릿 |

## 데이터 소스

**MVP에 포함됨 (무료):**
- 주요 지수 (KOSPI, S&P500, NASDAQ, VIX 등) — Yahoo Finance
- 원자재 (금, 은, 구리, WTI, 브렌트) — Yahoo Finance
- 암호화폐 (BTC, ETH) — Yahoo Finance
- 환율 (USD/KRW, EUR/USD 등) — Yahoo Finance
- 미국 국채 수익률 (5Y, 10Y, 30Y) — Yahoo Finance
- S&P500 섹터 (XLK, XLE 등) — Yahoo Finance
- VIX 기간구조 — Yahoo Finance
- 국내/글로벌 뉴스 헤드라인 — Google News RSS

**원본에는 있지만 MVP에서 제외:**
- 한국 국고채/회사채 — KOFIA (스크래핑 필요)
- KRX 투자자별 수급 — Naver Finance (스크래핑 필요)
- 일본 국채 — 별도 소스 필요
- CME FedWatch FOMC 확률 — 스크래핑 복잡
- 경제지표 캘린더 — FRED API 키 필요
- CNN F&G, NAAIM, AAII 심리지표 — 스크래핑 필요
- SPY 옵션 P/C, Max Pain — 별도 OI 데이터 필요

추가하려면 `collectors.py`에 함수를 더하고 `collect_all()`에 등록 + 템플릿에 섹션 추가.

## 자동화 (선택)

매일 아침 자동 실행하려면 `cron` 또는 `launchd` 사용:

```bash
crontab -e
# 매일 08:30 KST에 실행
30 8 * * * cd ~/Desktop/db && .venv/bin/python generate.py >> reports/cron.log 2>&1
```

## 주의

- Yahoo Finance는 비공식 무료 API라 가끔 rate limit/장애가 있음
- Claude API 비용: 1회 생성당 약 $0.05~0.20 (Sonnet 4.6 기준)
- 시장 휴장일은 데이터가 빈 값으로 나올 수 있음
