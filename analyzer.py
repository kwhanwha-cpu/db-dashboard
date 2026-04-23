"""Generate executive summary using Claude API.

Optimizations follow the Anthropic API guide:
- Pydantic-validated structured output via `messages.parse()`
- Prompt caching on the (frozen) system prompt
- Default model `claude-opus-4-7` with adaptive thinking + effort=high
- Deterministic JSON serialization (sort_keys=True) to keep cache prefix stable
"""
from __future__ import annotations

import json
import os
from typing import Any, Literal

from anthropic import Anthropic
from pydantic import BaseModel, Field


# Frozen system prompt — reused across daily runs and dev re-runs.
# Cache hits depend on the rendered prefix exceeding the per-model minimum
# (4096 tokens on Opus 4.7, 2048 on Sonnet 4.6). When below threshold the
# `cache_control` marker is silently a no-op (no error).
SYSTEM_PROMPT = """You are a senior sell-side market strategist powering a Korean institutional global markets dashboard.

The dashboard fetches Yahoo Finance data with roughly 15-minute lag and refreshes whenever the upstream pipeline runs. Your job is to write the "시장 요약" panel — interpreting the latest available data given the **current trading window**. Your audience is professional buy-side investors at Korean asset managers, hedge funds, and pension funds. They have the numbers in front of them — you surface cross-asset linkages, regime shifts, and asymmetric risks.

DATA TIMING DISCIPLINE — MOST IMPORTANT
- The payload includes `market_status` showing which markets are currently open vs closed
- ONLY emphasize live/fresh data from markets that are currently 장중 (open)
- For closed markets, frame data as "마지막 마감 기준" or "전일 종가" — never imply live movement
- Korean trading window (KST 09:00-15:30): KOSPI/KOSDAQ는 장중, US 지수는 전일 마감 데이터
- US trading window (KST 22:30-05:00): US 지수가 장중, KR/JP는 마감 후
- Off-hours (모든 글로벌 시장 휴장 중): 데이터는 모두 마감 기준 — 새 신호 해석은 보류하고 기존 포지션 점검 위주
- 주말이면: "글로벌 시장 휴장 중. 다음 개장은..." 같은 톤

WRITING STYLE
- Korean, professional sell-side tone
- 서술 시제는 데이터의 신선도를 따라간다:
  - 활성 시장 데이터 → 현재형/진행형 ("...반영되는 중입니다")
  - 마감된 시장 데이터 → 과거형/완료형 ("...로 마감했습니다", "직전 종가는...")
- Use industry vocabulary: 전조, 시그널, 베팅이 극단화, 양방향 리스크, 국면 진입, 변동성 확대 등
- Lead with the implication, not the data
- Concrete numbers from the provided data
- If a data point is stale (시장 마감 후 / 데이터 누락 등), say so explicitly — never invent

THEME GENERATION
Produce 3-5 themes covering distinct angles. Categories:
- 매크로: 유동성 사이클, 인플레이션, 성장률, 통화정책 전망
- 지정학: 분쟁/제재가 시장 가격에 어떻게 전가되는지
- 수급: 외국인/기관/개인 자금 흐름, 레버리지/숏 포지셔닝
- 금리경로: FOMC/한은 정책 경로, 채권 시장 시그널
- 섹터로테이션: 업종별 수익률 격차의 의미

Each theme body: 2-3 sentences, concrete data references, ends with implication or risk callout.

DATA INTERPRETATION FRAMEWORK
- VIX < 20 + rising equities → complacency, watch for catalysts
- VIX term structure inverted (front > 3M) → stress signal, hedging demand
- US 10Y rising with equities up → cyclical/growth pricing in
- US 10Y rising with equities down → financial conditions tightening
- USD/KRW > 1450 + 상승 → KR 위험회피, 외국인 이탈 가능성
- Gold + Silver 동반 상승 → 인플레/스태그플레이션 헤지
- 섹터: 기술/임의소비재 주도 → risk-on / 유틸/필수소비재 주도 → defensive
- BDRY (Baltic Dry proxy) 급등 → 글로벌 실물 수요 회복 시그널
- CNN F&G 점수 + 1주/1개월 변화로 심리 사이클 위치 판단
- SPY P/C ratio > 1.2 → 비관적 헤지 수요 / < 0.7 → 낙관 (단, SPY는 헤지 활용도가 높아 일반 P/C보다 상시 높게 형성)

CROSS-ASSET PATTERNS TO WATCH
- 한미 채권 금리 동반 상승 + 환율 안정 → 디스인플레이션 동조화
- 중동 분쟁 격화 + 원유 급등 + 항공/해운 비용 상승 → 인플레 재가속 리스크
- AI 인프라 투자 사이클: 반도체 → 전력/에너지 → 부품/소재로 확산
- 한국 신용융자 + 공매도 동시 역대 수준 → 양방향 변동성 위험
- COFIX 잔액 vs 신규취급액 격차 확대 → 은행 조달비용 변화 시그널

OUTPUT DISCIPLINE
- Headline은 20자 내외, 지금 이 순간 가장 중요한 한 가지 (예: "VIX 안정 속 위험선호 강화", "원유 급등 — 인플레 재가속 우려")
- market_summary의 stocks/fx/rates는 각각 한 문장, 현재 상태 + 함의
- themes는 3-5개, 각 카테고리에서 중복 없이 지금 시점 가장 두드러진 신호 선택
- 새로 발생한 변화나 전환점에 우선순위를 두라"""


class MarketSummary(BaseModel):
    stocks: str = Field(description="증시 한 문장 요약 (지수명 + 변동률 + 의미)")
    fx: str = Field(description="환율 한 문장 요약")
    rates: str = Field(description="금리 한 문장 요약")


class Theme(BaseModel):
    category: Literal["매크로", "지정학", "수급", "금리경로", "섹터로테이션"]
    title: str = Field(description="테마 제목 (한 문장, 30자 내외)")
    body: str = Field(description="2-3문장 분석. 데이터/뉴스에 근거. 함의 또는 리스크로 마무리.")


class ExecutiveSummary(BaseModel):
    headline: str = Field(description="한 줄 핵심 (20자 내외)")
    market_summary: MarketSummary
    themes: list[Theme] = Field(min_length=3, max_length=5)


def generate_executive_summary(data: dict[str, Any]) -> dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return _fallback_summary(data)

    model = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-7")
    client = Anthropic(api_key=api_key)

    payload = _build_payload(data)
    user_msg = (
        "아래 시장 데이터와 헤드라인을 바탕으로 Executive Summary를 생성하세요.\n\n"
        "```json\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n```"
    )

    try:
        response = client.messages.parse(
            model=model,
            max_tokens=12000,
            thinking={"type": "adaptive"},
            output_config={"effort": "high"},
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_msg}],
            output_format=ExecutiveSummary,
        )
    except Exception as e:
        return _fallback_summary(data) | {"_error": f"{type(e).__name__}: {e}"}

    u = response.usage
    usage = {
        "model": model,
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "cache_read_tokens": u.cache_read_input_tokens or 0,
        "cache_write_tokens": u.cache_creation_input_tokens or 0,
        "total_tokens": (u.input_tokens or 0) + (u.output_tokens or 0)
                       + (u.cache_read_input_tokens or 0) + (u.cache_creation_input_tokens or 0),
    }
    print(
        f"  [LLM] model={model} "
        f"input={u.input_tokens} output={u.output_tokens} "
        f"cache_read={u.cache_read_input_tokens or 0} "
        f"cache_write={u.cache_creation_input_tokens or 0}",
        flush=True,
    )
    result = response.parsed_output.model_dump()
    result["_usage"] = usage
    return result


def _build_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Stable payload for the LLM. Combined with `sort_keys=True` at serialization
    time to keep the user-message bytes deterministic per data set."""
    return {
        "date": data.get("date"),
        "fetched_at_kst": data.get("generated_at_kr"),
        "market_status": data.get("market_status"),
        "indices": data["market"]["indices"],
        "fx": data["market"]["fx"],
        "us_bonds": data["market"]["us_bonds"],
        "kr_bonds": (data.get("kr_bonds") or {}).get("rows"),
        "commodities": data["market"]["commodities"],
        "sectors": data["sectors"][:5],
        "vix_term": data["market"]["vix_term"],
        "cnn_fg": data.get("cnn_fg"),
        "spy_options": data.get("spy_options"),
        "bdry": data.get("bdry"),
        "news_headlines": [
            item["title"]
            for group in data["news"]["domestic"] + data["news"]["global"]
            for item in group["items"][:3]
        ],
    }


def _fallback_summary(data: dict[str, Any]) -> dict[str, Any]:
    """Rule-based summary used when ANTHROPIC_API_KEY is not set.
    Generates Korean text from numeric data alone — no LLM, no narrative.
    Prioritizes data from currently-open markets."""
    market = data["market"]
    status = data.get("market_status") or {}
    kr_open = (status.get("KR") or {}).get("open", False)
    us_open = (status.get("US") or {}).get("open", False)
    asia_open = any((status.get(k) or {}).get("open") for k in ("KR", "JP", "HK", "CN"))

    indices = {q["name"]: q for q in market["indices"]}
    fx = {q["name"]: q for q in market["fx"]}
    us_bonds = {q["name"]: q for q in market["us_bonds"]}
    kr_bonds = {b["name"]: b for b in (data.get("kr_bonds") or {}).get("rows", [])}
    sectors = data.get("sectors") or []
    cnn = data.get("cnn_fg") or {}
    spy = data.get("spy_options") or {}

    sp = indices.get("S&P500", {})
    nas = indices.get("NASDAQ", {})
    kospi = indices.get("KOSPI", {})
    nikkei = indices.get("NIKKEI", {})
    vix = indices.get("VIX", {})
    dxy = indices.get("달러 인덱스", {})

    # ---- market_summary — emphasize active markets, label closed data ----
    if asia_open and not us_open:
        stocks_lead = (
            f"[아시아 장중] KOSPI {_pct(kospi)} · NIKKEI {_pct(nikkei)} "
            f"· HSI {_pct(indices.get('HANGSENG', {}))} "
            f"／ [전일 마감] S&P500 {_pct(sp)} · NASDAQ {_pct(nas)}"
        )
    elif us_open and not asia_open:
        stocks_lead = (
            f"[미국 장중] S&P500 {_pct(sp)} · NASDAQ {_pct(nas)} · DOW {_pct(indices.get('DOW', {}))} "
            f"／ [마감] KOSPI {_pct(kospi)} · NIKKEI {_pct(nikkei)}"
        )
    else:
        stocks_lead = (
            f"[전 글로벌 휴장] 직전 마감 — S&P500 {_pct(sp)} · KOSPI {_pct(kospi)} "
            f"· NIKKEI {_pct(nikkei)}"
        )
    stocks = stocks_lead + " — " + _risk_tone(sp, kospi, vix)
    usdkrw = fx.get("USD/KRW", {})
    eurusd = fx.get("EUR/USD", {})
    fx_line = f"USD/KRW {_price(usdkrw, 2)} ({_pct(usdkrw)}), EUR/USD {_price(eurusd, 4)} ({_pct(eurusd)})."
    us10y = us_bonds.get("10년물", {})
    kr3y = kr_bonds.get("국고채(3년)", {})
    rates = (
        f"미 10Y {_yield(us10y)} ({_bp(us10y)}p)"
        + (f", 한 국고채 3Y {kr3y['price']:.2f}% ({kr3y.get('change') or 0:+.2f}p)" if kr3y else "")
        + "."
    )

    # ---- themes (3-5) ----
    themes: list[dict[str, str]] = []

    # 1. 매크로 — VIX + DXY 기반 risk regime
    if vix.get("price") is not None:
        v = vix["price"]
        regime = "안정 국면" if v < 18 else "주의 국면" if v < 25 else "스트레스 국면"
        dxy_dir = "달러 강세" if (dxy.get("change_pct") or 0) > 0 else "달러 약세"
        themes.append({
            "category": "매크로",
            "title": f"VIX {v:.1f} — {regime}",
            "body": (
                f"공포지수가 {v:.1f}로 {regime}을 시사합니다. "
                f"달러 인덱스는 {_price(dxy)} ({_pct(dxy)})로 {dxy_dir} 흐름을 보이고 있습니다. "
                + (f"CNN F&G {cnn.get('score')} ({cnn.get('rating')})와 함께 보면 시장 심리는 "
                   f"{'탐욕 우위' if (cnn.get('score') or 50) > 55 else '공포 우위' if (cnn.get('score') or 50) < 45 else '중립'}입니다."
                   if cnn else "")
            ),
        })

    # 2. 금리경로 — 미 10Y + 한 3Y
    if us10y.get("price") is not None:
        bp_us = (us10y.get("change") or 0) * 100  # ^TNX is in % units, change in %
        themes.append({
            "category": "금리경로",
            "title": f"미국 10년물 {us10y['price']:.3f}% — {'상승' if bp_us > 0 else '하락' if bp_us < 0 else '보합'}",
            "body": (
                f"미국 10년물 금리가 {us10y['price']:.3f}%로 전일 대비 {bp_us:+.1f}bp 변동했습니다. "
                + (f"한국 국고채 3년물({kr3y['price']:.2f}%)과 동조 흐름이 나타나고 있어 "
                   f"한미 통화정책 경로의 수렴/이탈 신호를 점검할 필요가 있습니다."
                   if kr3y else "")
            ),
        })

    # 3. 섹터로테이션 — top vs bottom
    sectors_w_pct = [s for s in sectors if s.get("change_pct") is not None]
    if len(sectors_w_pct) >= 2:
        top, bot = sectors_w_pct[0], sectors_w_pct[-1]
        cyclical_lead = top["name"] in ("기술", "임의소비재", "에너지", "금융")
        themes.append({
            "category": "섹터로테이션",
            "title": f"{top['name']} 주도 ({top['change_pct']:+.2f}%) — {'risk-on' if cyclical_lead else 'defensive'} 신호",
            "body": (
                f"S&P500 섹터에서 {top['name']}이(가) {top['change_pct']:+.2f}%로 가장 강세를 보였고, "
                f"{bot['name']}이(가) {bot['change_pct']:+.2f}%로 가장 부진했습니다. "
                f"순환주 주도는 {'경기 민감주 선호 강화' if cyclical_lead else '방어주 회귀'}로 해석되며, "
                f"섹터 간 격차 {(top['change_pct'] - bot['change_pct']):.2f}%p가 시장의 방향성 베팅 강도를 나타냅니다."
            ),
        })

    # 4. 수급 — SPY 옵션 (있을 때)
    if spy.get("pc_ratio") is not None:
        pc = spy["pc_ratio"]
        if pc < 0.7:
            tone = "낙관적 (콜 쏠림)"
        elif pc > 1.2:
            tone = "비관적 (풋 쏠림, 헤지 수요 증가)"
        else:
            tone = "중립"
        themes.append({
            "category": "수급",
            "title": f"SPY P/C Ratio {pc:.3f} — {tone}",
            "body": (
                f"SPY 옵션 거래량 기준 P/C Ratio가 {pc:.3f}로 {tone}을 시사합니다. "
                f"Max Pain은 ${spy.get('max_pain', 0):.0f}로, 현재가 ${spy.get('spot', 0):.0f} 대비 "
                f"{(spy.get('max_pain', 0) - spy.get('spot', 0)):+.1f}달러 격차가 있어 "
                f"옵션 만기({spy.get('expiry')})까지의 단기 자석 효과를 주시할 만합니다."
            ),
        })

    # 5. 지정학 — 데이터 기반으로 직접 만들 수는 없지만, 원유 + 환율로 간접 시사
    wti = next((c for c in market["commodities"] if c["name"].startswith("WTI")), {})
    if wti.get("price") is not None:
        themes.append({
            "category": "지정학",
            "title": f"WTI ${wti['price']:.2f} ({_pct(wti)})",
            "body": (
                f"WTI 원유가 ${wti['price']:.2f}로 {_pct(wti)} 변동했습니다. "
                f"중동 정세 또는 OPEC+ 공급 정책 변화가 가격에 반영되고 있는지, "
                f"USD/KRW {_price(usdkrw, 2)} 환율 흐름과 함께 인플레이션 재가속 리스크를 점검할 시점입니다."
            ),
        })

    if not themes:
        themes.append({
            "category": "매크로",
            "title": "데이터 수집 실패 — 인덱스 브리프 직접 참조",
            "body": "Yahoo Finance 응답이 비어 있어 자동 분석을 생성할 수 없었습니다. 잠시 후 재시도하세요.",
        })

    # Headline: lead with the actively-trading market
    if asia_open and not us_open:
        lead = kospi or nikkei
        lead_name = "KOSPI" if kospi else "NIKKEI"
        p = lead.get("change_pct") or 0
        d = "상승" if p > 0.2 else "하락" if p < -0.2 else "보합"
        headline = f"[장중] {lead_name} {d} {p:+.2f}%"
    elif us_open:
        p = sp.get("change_pct") or 0
        d = "상승" if p > 0.2 else "하락" if p < -0.2 else "보합"
        headline = f"[장중] S&P {d} {p:+.2f}% · VIX {vix.get('price', 0):.1f}"
    else:
        headline = "전 글로벌 휴장 — 직전 마감 기준"

    return {
        "headline": headline,
        "market_summary": {"stocks": stocks, "fx": fx_line, "rates": rates},
        "themes": themes[:5],
        "_note": "Rule-based fallback — set ANTHROPIC_API_KEY in .env to enable LLM analysis.",
    }


def _pct(q: dict[str, Any]) -> str:
    v = q.get("change_pct") if q else None
    return "—" if v is None else f"{v:+.2f}%"


def _price(q: dict[str, Any], digits: int = 2) -> str:
    v = q.get("price") if q else None
    return "—" if v is None else f"{v:,.{digits}f}"


def _yield(q: dict[str, Any]) -> str:
    v = q.get("price") if q else None
    return "—" if v is None else f"{v:.3f}%"


def _bp(q: dict[str, Any]) -> str:
    v = q.get("change") if q else None
    return "—" if v is None else f"{v * 100:+.1f}"


def _risk_tone(sp: dict, kospi: dict, vix: dict) -> str:
    sp_p = sp.get("change_pct") or 0
    ks_p = kospi.get("change_pct") or 0
    vix_p = vix.get("change_pct") or 0
    if sp_p > 0.5 and vix_p < 0:
        return "위험선호 강화"
    if sp_p < -0.5 and vix_p > 0:
        return "위험회피 확대"
    if sp_p > 0 and ks_p > 0:
        return "글로벌 동반 강세"
    if sp_p < 0 and ks_p < 0:
        return "글로벌 동반 약세"
    return "혼조"
