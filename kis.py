"""Korea Investment Securities (KIS) OpenAPI client for realtime KR market data.

Free for KIS account holders. ~3-5s latency (near-realtime).

Setup:
  1. https://apiportal.koreainvestment.com/ → OpenAPI 신청
  2. APP Key + APP Secret 발급
  3. .env:
     KIS_APP_KEY=PSxxxxxxxx
     KIS_APP_SECRET=xxxxxxxx
     KIS_ENV=real      # 'real' (실전) or 'vts' (모의투자)
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


KST = timezone(timedelta(hours=9))
TOKEN_CACHE = Path(__file__).parent / "reports" / ".kis_token.json"

# KIS index codes (FID_INPUT_ISCD, market=U)
KR_INDEX_CODES = {
    "KOSPI": "0001",
    "KOSDAQ": "1001",
    "KOSPI200": "2001",
}


@dataclass
class KISConfig:
    app_key: str
    app_secret: str
    base_url: str

    @classmethod
    def from_env(cls) -> "KISConfig | None":
        key = os.environ.get("KIS_APP_KEY")
        secret = os.environ.get("KIS_APP_SECRET")
        if not key or not secret:
            return None
        env = os.environ.get("KIS_ENV", "real").lower()
        base = (
            "https://openapi.koreainvestment.com:9443" if env == "real"
            else "https://openapivts.koreainvestment.com:29443"
        )
        return cls(app_key=key, app_secret=secret, base_url=base)


def _load_cached_token(app_key: str) -> str | None:
    """Return a cached token if < 23h old and matching the current key."""
    if not TOKEN_CACHE.exists():
        return None
    try:
        data = json.loads(TOKEN_CACHE.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("app_key_hash") != hash(app_key):
        return None
    issued = datetime.fromisoformat(data.get("issued_at", ""))
    if datetime.now(timezone.utc) - issued > timedelta(hours=23):
        return None
    return data.get("access_token")


def _save_token(app_key: str, token: str) -> None:
    TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE.write_text(json.dumps({
        "app_key_hash": hash(app_key),
        "access_token": token,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }))


def get_access_token(cfg: KISConfig) -> str | None:
    """Fetch or reuse a 24-hour OAuth token. KIS rate-limits this heavily
    (~1 token per minute per app), so cache aggressively."""
    cached = _load_cached_token(cfg.app_key)
    if cached:
        return cached
    try:
        r = requests.post(
            f"{cfg.base_url}/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": cfg.app_key,
                "appsecret": cfg.app_secret,
            },
            timeout=10,
        )
        r.raise_for_status()
        token = r.json().get("access_token")
        if token:
            _save_token(cfg.app_key, token)
        return token
    except Exception:
        return None


def fetch_index_quote(cfg: KISConfig, token: str, iscd: str) -> dict[str, Any] | None:
    """Fetch realtime KR index quote. tr_id=FHPUP02100000 (업종 현재가)."""
    try:
        r = requests.get(
            f"{cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-index-price",
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": cfg.app_key,
                "appsecret": cfg.app_secret,
                "tr_id": "FHPUP02100000",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "U",
                "FID_INPUT_ISCD": iscd,
            },
            timeout=10,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("rt_cd") != "0":
            return None
        o = body.get("output") or {}
        price = float(o.get("bstp_nmix_prpr") or 0)
        change = float(o.get("bstp_nmix_prdy_vrss") or 0)
        pct = float(o.get("bstp_nmix_prdy_ctrt") or 0)
        sign = o.get("prdy_vrss_sign", "3")
        if sign in ("4", "5"):  # 하한 / 하락
            change = -abs(change)
            pct = -abs(pct)
        direction = "▲" if pct > 0 else "▼" if pct < 0 else "·"
        return {
            "price": price,
            "change": change,
            "change_pct": pct,
            "direction": direction,
        }
    except Exception:
        return None


def collect_kr_indices_realtime() -> dict[str, dict[str, Any]] | None:
    """Return realtime KOSPI/KOSDAQ/KOSPI200 quotes via KIS, or None if unavailable."""
    cfg = KISConfig.from_env()
    if cfg is None:
        return None
    token = get_access_token(cfg)
    if not token:
        return None

    result: dict[str, dict[str, Any]] = {}
    as_of = datetime.now(KST).strftime("%m-%d %H:%M")
    for name, code in KR_INDEX_CODES.items():
        quote = fetch_index_quote(cfg, token, code)
        if quote:
            quote["as_of_kst"] = as_of
            quote["source"] = "KIS"
            result[name] = quote
        time.sleep(0.1)  # gentle rate limiting
    return result if result else None
