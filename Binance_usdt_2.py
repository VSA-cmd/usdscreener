import os
import sys
import time
import json
import math
from typing import List, Dict, Any, Optional, Set

import requests
import pandas as pd


# -------------------------------
# Config por entorno
# -------------------------------
FORCE_GLOBAL = os.environ.get("FORCE_GLOBAL", "").strip() == "1"
BINANCE_REGION = (os.environ.get("BINANCE_REGION", "") or "").upper().strip()

if FORCE_GLOBAL or BINANCE_REGION not in {"US"}:
    SPOT_BASE = "https://api.binance.com"
    FUTU_BASE = "https://fapi.binance.com"  # futures (USDT-m)
else:
    SPOT_BASE = "https://api.binance.us"
    FUTU_BASE = None  # deshabilitado en modo US


# -------------------------------
# Helpers HTTP
# -------------------------------
def _get(url: str, params: Dict[str, Any] = None, timeout: int = 12) -> Optional[Any]:
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        # Mandamos a stderr; app redirige stderr->stdout para el log
        print(f"[warn] GET failed {url} -> {repr(e)}", file=sys.stderr, flush=True)
        return None


# -------------------------------
# Descubrir qué símbolos tienen futuros
# -------------------------------
def fetch_futures_symbols() -> Set[str]:
    if not FUTU_BASE:
        return set()
    info = _get(f"{FUTU_BASE}/fapi/v1/exchangeInfo")
    symbols = set()
    if info and isinstance(info, dict):
        for s in info.get("symbols", []):
            try:
                if s.get("status") == "TRADING":
                    symbols.add(str(s.get("symbol")))
            except Exception:
                pass
    return symbols


FUT_SYMBOLS = fetch_futures_symbols()


# -------------------------------
# Data builders
# -------------------------------
def fetch_24h_tickers_usdt(limit_symbols: int = 20) -> List[Dict[str, Any]]:
    """Trae tickers 24h y filtra los que terminan en USDT; devuelve top por quoteVolume."""
    data = _get(f"{SPOT_BASE}/api/v3/ticker/24hr") or []
    rows = []
    for d in data:
        sym = d.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            rows.append({
                "symbol": sym,
                "price": float(d.get("lastPrice") or d.get("weightedAvgPrice") or 0.0),
                "pct_change_24h": float(d.get("priceChangePercent") or 0.0) / 100.0,
                "quote_volume_24h": float(d.get("quoteVolume") or 0.0),
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["quote_volume_24h"], reverse=True)
    return rows[:limit_symbols] if rows else []


def fetch_klines_close(symbol: str, interval: str = "4h", limit: int = 30) -> List[float]:
    data = _get(f"{SPOT_BASE}/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    closes = []
    if isinstance(data, list):
        for item in data:
            try:
                closes.append(float(item[4]))  # close
            except Exception:
                pass
    return closes


def compute_trend_4h(closes: List[float]) -> Dict[str, Any]:
    if not closes or len(closes) < 2:
        return {"Trend_4H": None, "ScoreTrend": None}
    c0 = closes[0]
    c1 = closes[-1]
    chg = (c1 - c0) / c0 if c0 else 0.0
    label = "Alcista" if chg > 0.01 else ("Bajista" if chg < -0.01 else "Lateral")
    score = round(float(chg), 4)
    return {"Trend_4H": label, "ScoreTrend": score}


def fetch_funding_rate(symbol: str) -> Optional[float]:
    if not FUTU_BASE or symbol not in FUT_SYMBOLS:
        return None
    data = _get(f"{FUTU_BASE}/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
    try:
        if isinstance(data, list) and data:
            return float(data[0].get("fundingRate") or 0.0)
    except Exception:
        pass
    return None


def fetch_open_interest(symbol: str) -> Optional[float]:
    if not FUTU_BASE or symbol not in FUT_SYMBOLS:
        return None
    data = _get(f"{FUTU_BASE}/fapi/v1/openInterest", {"symbol": symbol})
    try:
        if isinstance(data, dict) and "openInterest" in data:
            return float(data["openInterest"])
    except Exception:
        pass
    return None


def build_df(limit_symbols: int = 20) -> pd.DataFrame:
    tickers = fetch_24h_tickers_usdt(limit_symbols=limit_symbols)
    if not tickers:
        tickers = [
            {"symbol": "BTCUSDT", "price": None, "pct_change_24h": None, "quote_volume_24h": None},
            {"symbol": "ETHUSDT", "price": None, "pct_change_24h": None, "quote_volume_24h": None},
        ]

    out_rows = []
    for t in tickers:
        sym = t["symbol"]
        closes = fetch_klines_close(sym, "4h", 30)
        trend = compute_trend_4h(closes)
        fr = fetch_funding_rate(sym)
        oi = fetch_open_interest(sym)
        out_rows.append({
            "symbol": sym,
            "price": t.get("price"),
            "pct_change_24h": t.get("pct_change_24h"),
            "quote_volume_24h": t.get("quote_volume_24h"),
            "Trend_4H": trend["Trend_4H"],
            "ScoreTrend": trend["ScoreTrend"],
            "fundingRate": fr,
            "openInterest": oi,
        })
        time.sleep(0.05)  # rate-limit suave

    df = pd.DataFrame(out_rows)
    if "quote_volume_24h" in df.columns:
        df = df.sort_values("quote_volume_24h", ascending=False, na_position="last").reset_index(drop=True)
    return df


def _to_float_or_none(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


# -------------------------------
# Main: imprime JSON para app.py
# -------------------------------
if __name__ == "__main__":
    try:
        limit = int(os.environ.get("LIMIT_SYMBOLS", "20"))
    except Exception:
        limit = 20

    df = build_df(limit_symbols=limit)

    safe = []
    for _, r in df.iterrows():
        safe.append({
            "symbol": r.get("symbol"),
            "price": _to_float_or_none(r.get("price")),
            "pct_change_24h": _to_float_or_none(r.get("pct_change_24h")),
            "quote_volume_24h": _to_float_or_none(r.get("quote_volume_24h")),
            "Trend_4H": r.get("Trend_4H"),
            "ScoreTrend": _to_float_or_none(r.get("ScoreTrend")),
            "fundingRate": _to_float_or_none(r.get("fundingRate")),
            "openInterest": _to_float_or_none(r.get("openInterest")),
        })

    print("__DFJSON__=" + json.dumps(safe, ensure_ascii=False), flush=True)
