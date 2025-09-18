import os
import sys
import time
import json
from typing import List, Dict, Any, Optional

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
    # Región USA (spot). Futures no expuestos igual que global; omitir métricas futures si no hay FORCE_GLOBAL
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
        print(f"[warn] GET failed {url} -> {repr(e)}", file=sys.stderr, flush=True)
        return None


# -------------------------------
# Data builders
# -------------------------------
def fetch_24h_tickers_usdt(limit_symbols: int = 20) -> List[Dict[str, Any]]:
    """Trae todos los tickers 24h y filtra los que terminan en USDT; devuelve top por quoteVolume."""
    url = f"{SPOT_BASE}/api/v3/ticker/24hr"
    data = _get(url) or []
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
            # si hay campos corruptos, ignora esa fila
            continue
    # ordenar por volumen desc y limitar
    rows.sort(key=lambda x: x["quote_volume_24h"], reverse=True)
    return rows[:limit_symbols] if rows else []


def fetch_klines_close(symbol: str, interval: str = "4h", limit: int = 30) -> List[float]:
    url = f"{SPOT_BASE}/api/v3/klines"
    data = _get(url, {"symbol": symbol, "interval": interval, "limit": limit})
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
    if not FUTU_BASE:
        return None
    url = f"{FUTU_BASE}/fapi/v1/fundingRate"
    data = _get(url, {"symbol": symbol, "limit": 1})
    try:
        if isinstance(data, list) and data:
            return float(data[0].get("fundingRate") or 0.0)
    except Exception:
        pass
    return None


def fetch_open_interest(symbol: str) -> Optional[float]:
    if not FUTU_BASE:
        return None
    url = f"{FUTU_BASE}/fapi/v1/openInterest"
    data = _get(url, {"symbol": symbol})
    try:
        if isinstance(data, dict) and "openInterest" in data:
            return float(data["openInterest"])
    except Exception:
        pass
    return None


def build_df(limit_symbols: int = 20) -> pd.DataFrame:
    # 1) Top por volumen USDT (spot)
    tickers = fetch_24h_tickers_usdt(limit_symbols=limit_symbols)
    if not tickers:
        # fallback muy pequeño para que nunca explote: BTC y ETH si todo falla
        tickers = [
            {"symbol": "BTCUSDT", "price": None, "pct_change_24h": None, "quote_volume_24h": None},
            {"symbol": "ETHUSDT", "price": None, "pct_change_24h": None, "quote_volume_24h": None},
        ]

    out_rows = []
    for t in tickers:
        sym = t["symbol"]
        # 2) Trend 4H
        closes = fetch_klines_close(sym, "4h", 30)
        trend = compute_trend_4h(closes)

        # 3) Futures metrics (si aplica)
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

        # pequeña pausa para no golpear límites si hay muchas llamadas
        time.sleep(0.05)

    df = pd.DataFrame(out_rows)
    # Orden por volumen si existe
    if "quote_volume_24h" in df.columns:
        df = df.sort_values("quote_volume_24h", ascending=False, na_position="last").reset_index(drop=True)
    return df


# -------------------------------
# Main: imprime JSON para app.py
# -------------------------------
if __name__ == "__main__":
    try:
        # puedes ajustar el límite por env si quieres: LIMIT_SYMBOLS=20
        limit = int(os.environ.get("LIMIT_SYMBOLS", "20"))
    except Exception:
        limit = 20

    df = build_df(limit_symbols=limit)

    # Asegurar tipos serializables
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    safe = []
    for _, r in df.iterrows():
        safe.append({
            "symbol": r.get("symbol"),
            "price": _to_float(r.get("price")),
            "pct_change_24h": _to_float(r.get("pct_change_24h")),
            "quote_volume_24h": _to_float(r.get("quote_volume_24h")),
            "Trend_4H": r.get("Trend_4H"),
            "ScoreTrend": _to_float(r.get("ScoreTrend")),
            "fundingRate": _to_float(r.get("fundingRate")),
            "openInterest": _to_float(r.get("openInterest")),
        })

    # IMPORTANTE: línea que detecta app.py
    print("__DFJSON__=" + json.dumps(safe, ensure_ascii=False), flush=True)
