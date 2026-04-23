"""Data collectors for the daily financial brief."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any
import re
import urllib.parse

import feedparser
import requests
import yfinance as yf
from bs4 import BeautifulSoup

import kis


KST = timezone(timedelta(hours=9))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1"


@dataclass
class Quote:
    name: str
    symbol: str
    price: float | None
    change: float | None
    change_pct: float | None
    as_of_kst: str | None = None  # last data timestamp in KST (YYYY-MM-DD HH:MM)

    @property
    def direction(self) -> str:
        if self.change_pct is None:
            return "·"
        if self.change_pct > 0:
            return "▲"
        if self.change_pct < 0:
            return "▼"
        return "·"


def _to_kst(ts: Any) -> str | None:
    if ts is None:
        return None
    py = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
    if py.tzinfo is None:
        py = py.replace(tzinfo=timezone.utc)
    return py.astimezone(KST).strftime("%m-%d %H:%M")


def _quote(name: str, symbol: str) -> Quote:
    """Use daily bar for change (vs prior close) and 5-minute intraday bar for
    the actual last-trade timestamp + freshest price.

    Yahoo Finance free tier still has ~15-min delay underneath, but a 5-min
    bar interval ensures the last bar's timestamp is at most ~5 min coarse
    rather than the previous 1-hour grid."""
    try:
        t = yf.Ticker(symbol)
        daily = t.history(period="5d", auto_adjust=False)
        if daily.empty or len(daily) < 2:
            return Quote(name, symbol, None, None, None)
        prev = float(daily["Close"].iloc[-2])

        last = float(daily["Close"].iloc[-1])
        last_ts = daily.index[-1]

        # Try 5-min bars first (freshest), fall back to 1-hour, then daily.
        for interval, period in (("5m", "1d"), ("15m", "5d"), ("1h", "5d")):
            try:
                intra = t.history(period=period, interval=interval, auto_adjust=False)
                if not intra.empty:
                    last = float(intra["Close"].iloc[-1])
                    last_ts = intra.index[-1]
                    break
            except Exception:
                continue

        change = last - prev
        pct = (change / prev) * 100 if prev else None
        return Quote(name, symbol, last, change, pct, _to_kst(last_ts))
    except Exception:
        return Quote(name, symbol, None, None, None)


def _q_dict(name: str, symbol: str) -> dict[str, Any]:
    q = _quote(name, symbol)
    d = asdict(q)
    d["direction"] = q.direction
    return d


# ---------- Yahoo Finance market data ----------

INDEX_TICKERS = [
    ("KOSPI", "^KS11"), ("KOSDAQ", "^KQ11"),
    ("S&P500", "^GSPC"), ("NASDAQ", "^IXIC"), ("DOW", "^DJI"),
    ("NIKKEI", "^N225"), ("HANGSENG", "^HSI"), ("SHANGHAI", "000001.SS"),
    ("VIX", "^VIX"), ("달러 인덱스", "DX-Y.NYB"),
]

COMMODITY_TICKERS = [
    ("금 ($/oz)", "GC=F"), ("은 ($/oz)", "SI=F"), ("구리 ($/lb)", "HG=F"),
    ("WTI ($/bbl)", "CL=F"), ("브렌트유 ($/bbl)", "BZ=F"),
    ("천연가스 ($/MMBtu)", "NG=F"),
]

CRYPTO_TICKERS = [("비트코인 (BTC)", "BTC-USD"), ("이더리움 (ETH)", "ETH-USD")]

FX_TICKERS = [
    ("USD/KRW", "KRW=X"), ("EUR/USD", "EURUSD=X"), ("USD/JPY", "JPY=X"),
    ("GBP/USD", "GBPUSD=X"), ("USD/CNY", "CNY=X"),
]

US_BOND_TICKERS = [("5년물", "^FVX"), ("10년물", "^TNX"), ("30년물", "^TYX")]

VIX_TERM_TICKERS = [
    ("VIX 9D", "^VIX9D"), ("VIX", "^VIX"),
    ("VIX 3M", "^VIX3M"), ("VIX 6M", "^VIX6M"),
]

SECTOR_TICKERS = [
    ("기술", "XLK"), ("에너지", "XLE"), ("커뮤니케이션", "XLC"),
    ("필수소비재", "XLP"), ("헬스케어", "XLV"), ("소재", "XLB"),
    ("임의소비재", "XLY"), ("금융", "XLF"), ("유틸리티", "XLU"),
    ("산업재", "XLI"), ("부동산", "XLRE"),
]


def collect_market_data() -> dict[str, Any]:
    indices = [_q_dict(n, s) for n, s in INDEX_TICKERS]

    # If KIS OpenAPI credentials are set, overlay realtime KR indices.
    kis_data = kis.collect_kr_indices_realtime()
    if kis_data:
        by_name = {q["name"]: q for q in indices}
        for kr_name in ("KOSPI", "KOSDAQ"):
            if kr_name in kis_data and kr_name in by_name:
                q = by_name[kr_name]
                fresh = kis_data[kr_name]
                q.update({
                    "price": fresh["price"],
                    "change": fresh["change"],
                    "change_pct": fresh["change_pct"],
                    "direction": fresh["direction"],
                    "as_of_kst": fresh["as_of_kst"],
                    "source": "KIS",
                })

    return {
        "indices": indices,
        "commodities": [_q_dict(n, s) for n, s in COMMODITY_TICKERS],
        "crypto": [_q_dict(n, s) for n, s in CRYPTO_TICKERS],
        "fx": [_q_dict(n, s) for n, s in FX_TICKERS],
        "us_bonds": [_q_dict(n, s) for n, s in US_BOND_TICKERS],
        "vix_term": [_q_dict(n, s) for n, s in VIX_TERM_TICKERS],
        "kis_active": bool(kis_data),
    }


def collect_sectors() -> list[dict[str, Any]]:
    sectors = [_q_dict(n, s) for n, s in SECTOR_TICKERS]
    sectors.sort(key=lambda x: (x["change_pct"] is None, -(x["change_pct"] or 0)))
    return sectors


# ---------- Korean bonds (Naver Finance, EUC-KR) ----------

NAVER_BOND_KEYS = {
    "IRR_CD91": "CD금리(91일)",
    "IRR_CALL": "콜 금리",
    "IRR_GOVT03Y": "국고채(3년)",
    "IRR_CORP03Y": "회사채(3년)",
    "IRR_COFIXBAL": "COFIX 잔액",
    "IRR_COFIXNEW": "COFIX 신규",
}


def collect_kr_bonds() -> dict[str, Any]:
    """Scrape Korean bond yields from Naver Finance market index page."""
    try:
        resp = requests.get(
            "https://finance.naver.com/marketindex/",
            headers={"User-Agent": UA}, timeout=10,
        )
        resp.encoding = "euc-kr"
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return {"as_of_kst": None, "rows": []}

    # Try to extract "기준일" / 마지막 업데이트 from the page
    as_of = None
    date_node = soup.find(string=re.compile(r"\d{4}\.\d{2}\.\d{2}"))
    if date_node:
        m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})\s*(\d{2}:\d{2})?", str(date_node))
        if m:
            ymd = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            as_of = f"{ymd} {m.group(4)}" if m.group(4) else ymd
    if not as_of:
        as_of = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    rows: list[dict[str, Any]] = []
    for code, name in NAVER_BOND_KEYS.items():
        link = soup.find("a", href=re.compile(rf"marketindexCd={code}\b"))
        if not link:
            continue
        # Walk up to <th>, then find the <td> following it within the same parent <tr>
        th = link.find_parent("th")
        if not th:
            continue
        tr = th.find_parent("tr")
        if not tr:
            continue
        tds = tr.find_all("td")
        if not tds:
            continue
        try:
            price = float(tds[0].get_text(strip=True).replace(",", ""))
        except (ValueError, IndexError):
            price = None
        change = None
        direction = "·"
        if len(tds) >= 2:
            img = tds[1].find("img")
            if img:
                alt = img.get("alt", "")
                if "상승" in alt:
                    direction = "▲"
                elif "하락" in alt:
                    direction = "▼"
            txt = tds[1].get_text(" ", strip=True)
            m = re.search(r"[\d.]+", txt)
            if m:
                try:
                    change = float(m.group(0))
                    if direction == "▼":
                        change = -change
                except ValueError:
                    pass
        rows.append({
            "name": name, "price": price, "change": change,
            "direction": direction, "unit": "%",
        })
    return {"as_of_kst": as_of, "rows": rows}


# ---------- CNN Fear & Greed ----------

def collect_cnn_fear_greed() -> dict[str, Any] | None:
    try:
        r = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={
                "User-Agent": UA,
                "Origin": "https://edition.cnn.com",
                "Referer": "https://edition.cnn.com/",
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        fg = data.get("fear_and_greed", {})
        ts = fg.get("timestamp")
        as_of = None
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                as_of = dt.astimezone(KST).strftime("%Y-%m-%d %H:%M")
            except (ValueError, AttributeError):
                pass
        return {
            "score": round(fg.get("score", 0), 1),
            "rating": fg.get("rating", ""),
            "previous_close": round(fg.get("previous_close", 0), 1),
            "previous_1_week": round(fg.get("previous_1_week", 0), 1),
            "previous_1_month": round(fg.get("previous_1_month", 0), 1),
            "as_of_kst": as_of,
        }
    except Exception:
        return None


# ---------- SPY Put/Call and Max Pain ----------

def collect_spy_options() -> dict[str, Any] | None:
    try:
        spy = yf.Ticker("SPY")
        if not spy.options:
            return None
        expiry = spy.options[0]
        chain = spy.option_chain(expiry)
        calls, puts = chain.calls, chain.puts
        call_vol = float(calls["volume"].fillna(0).sum())
        put_vol = float(puts["volume"].fillna(0).sum())
        pc_ratio = put_vol / call_vol if call_vol > 0 else None

        spot_hist = spy.history(period="2d")
        spot = float(spot_hist["Close"].iloc[-1]) if not spot_hist.empty else None

        # Max pain: strike that minimizes total writer payout.
        # Restrict candidates to ±20% of spot to avoid skew from far-OTM OI.
        all_strikes = sorted(set(calls["strike"]) | set(puts["strike"]))
        if spot:
            lo, hi = spot * 0.8, spot * 1.2
            strikes = [k for k in all_strikes if lo <= k <= hi] or all_strikes
        else:
            strikes = all_strikes
        if not strikes:
            return None
        pain: dict[float, float] = {}
        for K in strikes:
            total = 0.0
            for _, r in calls.iterrows():
                oi = r.get("openInterest") or 0
                total += oi * max(0.0, K - r["strike"])
            for _, r in puts.iterrows():
                oi = r.get("openInterest") or 0
                total += oi * max(0.0, r["strike"] - K)
            pain[K] = total
        max_pain = min(pain, key=pain.get)
        return {
            "expiry": expiry,
            "call_volume": int(call_vol),
            "put_volume": int(put_vol),
            "pc_ratio": round(pc_ratio, 3) if pc_ratio is not None else None,
            "spot": round(spot, 2) if spot is not None else None,
            "max_pain": float(max_pain),
            "as_of_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        }
    except Exception:
        return None


# ---------- Baltic Dry Index proxy ----------

def collect_bdry() -> dict[str, Any] | None:
    """BDRY ETF as a public proxy for the Baltic Dry Index."""
    q = _quote("Baltic Dry (BDRY ETF)", "BDRY")
    if q.price is None:
        return None
    d = asdict(q)
    d["direction"] = q.direction
    return d


# ---------- News (Google News RSS) ----------

NEWS_QUERIES_KR = [
    "코스피 OR 코스닥",
    "한국은행 금리",
    "원달러 환율",
    "삼성전자 OR SK하이닉스",
]

NEWS_QUERIES_GLOBAL = [
    "Federal Reserve interest rate",
    "S&P 500 stock market",
    "oil price OPEC",
    "China economy",
]


def _google_news(query: str, hl: str, gl: str, ceid: str, limit: int = 5) -> list[dict[str, str]]:
    """Google News RSS — request last 3 days only and sort by published desc."""
    from email.utils import parsedate_to_datetime

    # Append `when:3d` so Google returns only the last 3 days
    q = urllib.parse.quote(f"{query} when:3d")
    url = f"https://news.google.com/rss/search?q={q}&hl={hl}&gl={gl}&ceid={ceid}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    def _pub_dt(s: str) -> datetime:
        try:
            return parsedate_to_datetime(s)
        except (TypeError, ValueError):
            return datetime.min.replace(tzinfo=timezone.utc)

    items: list[dict[str, str]] = []
    for entry in feed.entries:
        pub_str = entry.get("published", "")
        pub_dt = _pub_dt(pub_str)
        if pub_dt < cutoff:
            continue  # skip stale items even if Google ignored when:3d
        items.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "source": entry.get("source", {}).get("title", "") if hasattr(entry, "source") else "",
            "published": pub_str,
            "_pub_sort": pub_dt.isoformat(),
        })

    items.sort(key=lambda x: x["_pub_sort"], reverse=True)
    for item in items:
        item.pop("_pub_sort", None)
    return items[:limit]


def collect_news() -> dict[str, list[dict[str, Any]]]:
    domestic = [{"query": q, "items": _google_news(q, "ko", "KR", "KR:ko")} for q in NEWS_QUERIES_KR]
    global_news = [{"query": q, "items": _google_news(q, "en", "US", "US:en")} for q in NEWS_QUERIES_GLOBAL]
    return {"domestic": domestic, "global": global_news}


# ---------- Economic calendar (hardcoded, US-focused) ----------

# 2026 FOMC meetings (publicly announced schedule)
FOMC_DATES_2026 = [
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-11-04", "2026-12-16",
]


def collect_econ_calendar(now: datetime, days_ahead: int = 14) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    end = now + timedelta(days=days_ahead)

    # FOMC meetings (rate decision at 14:00 ET, ~03:00 KST next day)
    for d in FOMC_DATES_2026:
        dt = datetime.fromisoformat(d).replace(tzinfo=KST)
        if now.date() <= dt.date() <= end.date():
            events.append({
                "date": dt.strftime("%Y-%m-%d"),
                "weekday": "월화수목금토일"[dt.weekday()],
                "time_kst": "03:00 (다음날)",
                "indicator": "FOMC 금리결정",
                "freq": "연 8회",
            })

    # Weekly initial jobless claims — every Thursday at 21:30 KST
    cur = now.date()
    for _ in range(days_ahead + 1):
        if cur.weekday() == 3:  # Thursday
            events.append({
                "date": cur.strftime("%Y-%m-%d"),
                "weekday": "목",
                "time_kst": "21:30",
                "indicator": "미국 신규실업수당청구",
                "freq": "주간",
            })
        cur += timedelta(days=1)

    events.sort(key=lambda e: (e["date"], e["time_kst"]))
    return events


# ---------- Top-level orchestrator ----------

def market_status(now_kst: datetime) -> dict[str, dict[str, str]]:
    """Open/closed status of major markets in KST.
    US uses EDT-default (23:30→06:00 KST in winter, 22:30→05:00 KST in summer).
    Approximated to a single window — may be off by 1h half the year."""
    weekday = now_kst.weekday()
    hm = now_kst.hour * 60 + now_kst.minute

    def _open(start_h: int, start_m: int, end_h: int, end_m: int) -> bool:
        if weekday >= 5:
            return False
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        if start <= end:
            return start <= hm < end
        return hm >= start or hm < end  # overnight (US)

    def _label(market: str, is_open: bool, hours: str) -> dict[str, str]:
        if weekday >= 5:
            return {"market": market, "status": "주말 휴장", "open": False, "hours": hours}
        return {
            "market": market,
            "status": "장중" if is_open else "마감",
            "open": is_open,
            "hours": hours,
        }

    return {
        "KR": _label("KOSPI", _open(9, 0, 15, 30), "09:00-15:30 KST"),
        "JP": _label("NIKKEI", _open(9, 0, 15, 0), "09:00-15:00 KST"),
        "HK": _label("HSI", _open(10, 0, 16, 0), "10:00-16:00 KST"),
        "CN": _label("SHANGHAI", _open(10, 30, 16, 0), "10:30-16:00 KST"),
        "US": _label("S&P500", _open(22, 30, 5, 0), "22:30-05:00 KST (EDT 기준)"),
    }


def collect_all() -> dict[str, Any]:
    now = datetime.now(KST)
    return {
        "generated_at": now.isoformat(),
        "generated_at_kr": now.strftime("%Y-%m-%d %H:%M:%S"),
        "date": now.strftime("%Y-%m-%d"),
        "date_kr": now.strftime("%Y년 %m월 %d일") + f" ({'월화수목금토일'[now.weekday()]})",
        "market_status": market_status(now),
        "market": collect_market_data(),
        "sectors": collect_sectors(),
        "kr_bonds": collect_kr_bonds(),
        "cnn_fg": collect_cnn_fear_greed(),
        "spy_options": collect_spy_options(),
        "bdry": collect_bdry(),
        "econ_calendar": collect_econ_calendar(now),
        "news": collect_news(),
    }
