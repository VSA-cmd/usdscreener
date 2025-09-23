# -*- coding: utf-8 -*-
"""
Motor de screener USDT para Render (ligero):
- Universo: todos los pares SPOT con quote=USDT & status=TRADING (vía exchangeInfo)
- Cruce con /ticker/24hr para precio, %24h y quoteVolume
- Tendencia 4H: Close vs EMA50 + slope de EMA50 (Alcista/Bajista/Lateral) + ScoreTrend
- Derivados (USDT-M): fundingRate (último) + Open Interest 1h (delta)
- NetScore = ScoreTrend + puntos por funding y OI
- GHI-lite: z-scores de NetScore, ScoreTrend, %24h, fundingRate y delta de OI
- HTTP robusto con rotación de hosts y reintentos
- Concurrencia limitada + presupuesto de tiempo

ENV (Render):
  FORCE_GLOBAL=1         # usar binance global para tener Futuros USDT-M
  LIMIT_SYMBOLS=150      # top por quoteVolume a procesar
  MAX_WORKERS=12
  TIME_BUDGET_SECONDS=110
"""

import os
import time
import json
import math
import random
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

# ---------------- Config HTTP / Hosts ----------------
SPOT_HOSTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]
FUTU_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]

FORCE_GLOBAL = os.environ.get("FORCE_GLOBAL", "1") == "1"
SPOT_BASES = SPOT_HOSTS if FORCE_GLOBAL else [SPOT_HOSTS[0]]
FUTU_BASES = FUTU_HOSTS if FORCE_GLOBAL else []  # si es .us no hay USDT-M

HTTP_CONNECT_TIMEOUT = 5
HTTP_READ_TIMEOUT    = 15
HTTP_MAX_RETRIES     = 5
HTTP_BACKOFF         = 0.8
HTTP_JITTER          = (0.0, 0.6)

_session = requests.Session()

def http_get(urls, params=None, timeout=None, retries=None, backoff=None):
    """
    GET robusto: acepta str o list[str], rota hosts, reintenta con backoff + jitter,
    salta 429/5xx cambiando de host.
    """
    if isinstance(urls, str):
        urls = [urls]
    timeout = timeout or (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)
    retries = retries if retries is not None else HTTP_MAX_RETRIES
    backoff = backoff if backoff is not None else HTTP_BACKOFF

    last = None
    for attempt in range(1, retries + 1):
        try_hosts = urls[:] if attempt == 1 else random.sample(urls, len(urls))
        for u in try_hosts:
            try:
                r = _session.get(u, params=params, timeout=timeout)
                if r.status_code in (429, 418) or 500 <= r.status_code < 600:
                    last = Exception(f"HTTP {r.status_code}")
                    continue
                r.raise_for_status()
                return r.json()
            except (requests.ReadTimeout, requests.ConnectTimeout) as e:
                last = e
                continue
            except requests.RequestException as e:
                last = e
                continue
        time.sleep(backoff * attempt + random.uniform(*HTTP_JITTER))
    raise last or RuntimeError("http_get failed")

def log_default(msg: str):
    print(msg)

# ---------------- Datos base ----------------
def fetch_usdt_symbols_trading(logger=log_default):
    data = http_get([f"{b}/api/v3/exchangeInfo" for b in SPOT_BASES])
    syms = []
    for s in data.get("symbols", []):
        if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
            syms.append(s["symbol"])
    logger(f"TRADING USDT symbols: {len(syms)}")
    return syms

def fetch_24h_tickers(logger=log_default):
    arr = http_get([f"{b}/api/v3/ticker/24hr" for b in SPOT_BASES]) or []
    out = {}
    for d in arr:
        sym = d.get("symbol")
        if not sym:
            continue
        out[sym] = {
            "price": float(d.get("lastPrice") or 0.0),
            "pct_change_24h": float(d.get("priceChangePercent") or 0.0) / 100.0,
            "quote_volume_24h": float(d.get("quoteVolume") or 0.0),
        }
    logger(f"Ticker 24h objetos: {len(out)}")
    return out

def fetch_klines_4h(symbol: str, limit=120):
    data = http_get([f"{b}/api/v3/klines" for b in SPOT_BASES],
                    params={"symbol": symbol, "interval": "4h", "limit": limit})
    if not data:
        return None
    df = pd.DataFrame(data, columns=["t","o","h","l","c","v","t2","q","n","tb","tq","i"])
    df["c"] = pd.to_numeric(df["c"], errors="coerce")
    return df

# --------------- Derivados (USDT-M) ----------------
_fut_sym_cache = {}

def futures_symbol(symbol_spot: str) -> str | None:
    """Mapea 'BTCUSDT' spot a 'BTCUSDT' futuros (mismo). Verifica que exista en FAPI."""
    if not FUTU_BASES:
        return None
    base = symbol_spot[:-4]  # quita USDT
    cand = f"{base}USDT"
    if cand in _fut_sym_cache:
        return _fut_sym_cache[cand]
    try:
        data = http_get([f"{b}/fapi/v1/exchangeInfo" for b in FUTU_BASES], params={"symbol": cand})
        if data and data.get("symbols"):
            _fut_sym_cache[cand] = cand
            return cand
    except Exception:
        pass
    _fut_sym_cache[cand] = None
    return None

def fetch_funding_rate_last(symbol_fut: str) -> float | None:
    if not FUTU_BASES:
        return None
    for b in FUTU_BASES:
        try:
            arr = http_get(f"{b}/fapi/v1/fundingRate", params={"symbol": symbol_fut, "limit": 3})
            if arr:
                return float(arr[-1]["fundingRate"])
        except Exception:
            continue
    return None

def fetch_open_interest_hist(symbol_fut: str, period="1h", limit=10) -> pd.Series | None:
    if not FUTU_BASES:
        return None
    for b in FUTU_BASES:
        try:
            arr = http_get(f"{b}/futures/data/openInterestHist", params={"symbol": symbol_fut, "period": period, "limit": limit})
            if arr:
                df = pd.DataFrame(arr)
                df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
                return df["sumOpenInterest"]
        except Exception:
            continue
    return None

# --------------- Señales / Scoring ----------------
FUND_BULL_T = -0.0005  # < -0.05% sesgo alcista extremo (posible squeeze)
FUND_BEAR_T =  0.0010  # > +0.10% sesgo alcista excesivo (riesgo long squeeze)

def compute_trend_4h(df4: pd.DataFrame):
    ema = df4["c"].ewm(span=50, adjust=False).mean()
    ema50 = float(ema.iloc[-1])
    close = float(df4["c"].iloc[-1])
    slope = float(ema.diff().tail(5).mean()) if len(ema) >= 5 else 0.0
    if close > ema50 and slope > 0:
        label = "Alcista"
    elif close < ema50 and slope < 0:
        label = "Bajista"
    else:
        label = "Lateral"
    score_trend = (close / ema50 - 1.0) if ema50 else 0.0
    return label, float(score_trend)

def build_one(symbol: str, tick: dict, logger=log_default):
    # 4H trend
    df4 = fetch_klines_4h(symbol, limit=120)
    if df4 is None or df4.empty:
        return None
    trend_label, score_trend = compute_trend_4h(df4)

    # Derivados
    fut = futures_symbol(symbol)
    fr = None; oi_last = None; oi_prev = None; oi_delta = None
    if fut:
        fr = fetch_funding_rate_last(fut)
        oi_hist = fetch_open_interest_hist(fut, period="1h", limit=10)
        if isinstance(oi_hist, pd.Series) and len(oi_hist) >= 2:
            oi_last = float(oi_hist.iloc[-1])
            oi_prev = float(oi_hist.iloc[-2])
            oi_delta = oi_last - oi_prev

    # Puntos por derivados
    points = 0.0
    if fr is not None:
        if fr < FUND_BULL_T:
            points += 3.0
        elif fr > FUND_BEAR_T:
            points -= 3.0

    if oi_delta is not None:
        if oi_delta > 0 and score_trend > 0:    # OI sube con sesgo alcista
            points += 2.0
        elif oi_delta > 0 and score_trend <= 0: # OI sube contra sesgo
            points -= 2.0
        else:
            points -= 1.0

    net = float(score_trend + points)

    return {
        "symbol": symbol,
        "price": tick["price"],
        "pct_change_24h": tick["pct_change_24h"],
        "quote_volume_24h": tick["quote_volume_24h"],
        "Trend_4H": trend_label,
        "ScoreTrend": score_trend,
        "fundingRate": fr,
        "openInterest": oi_last,
        "oi_delta": oi_delta,
        "NetScore": round(net, 2),
    }

def _z(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    mu = x.mean()
    sd = x.std(ddof=0)
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(np.zeros(len(x)), index=x.index)
    z = (x - mu) / sd
    z[~np.isfinite(z)] = 0.0
    return z

# --------------- Motor público ----------------
def build_df(limit_symbols: int | None = None, logger=log_default):
    t0 = time.time()
    TIME_BUDGET = int(os.environ.get("TIME_BUDGET_SECONDS", "110"))
    LIMIT = int(os.environ.get("LIMIT_SYMBOLS", str(limit_symbols or 150)))
    MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "12"))

    logger(f"Iniciando job · LIMIT_SYMBOLS={LIMIT} MAX_WORKERS={MAX_WORKERS} BUDGET={TIME_BUDGET}s")

    # Universo
    trading = set(fetch_usdt_symbols_trading(logger))
    tickers = fetch_24h_tickers(logger)

    rows = []
    for sym in trading:
        t = tickers.get(sym)
        if not t:
            continue
        rows.append({"symbol": sym, **t})

    if not rows:
        return pd.DataFrame(), {"count": 0}

    base = pd.DataFrame(rows)
    base = base.sort_values("quote_volume_24h", ascending=False).head(LIMIT).reset_index(drop=True)
    logger(f"Candidatos tras cruce & rank por liquidez: {base.shape[0]}")

    # Procesamiento concurrente por símbolo
    out = []
    futures = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for _, r in base.iterrows():
            if time.time() - t0 > TIME_BUDGET * 0.9:
                logger("Presupuesto casi agotado: deteniendo envío de tareas nuevas.")
                break
            futures.append(ex.submit(build_one, r["symbol"], r.to_dict(), logger))

        for f in as_completed(futures, timeout=max(5, TIME_BUDGET - (time.time() - t0))):
            if time.time() - t0 > TIME_BUDGET:
                logger("Presupuesto agotado durante recolección.")
                break
            try:
                res = f.result(timeout=2)
                if res:
                    out.append(res)
            except Exception as e:
                logger(f"Tarea falló: {e}")

    df = pd.DataFrame(out)
    if df.empty:
        return df, {"count": 0}

    # GHI-lite (similar criterio a laptop, pero más barato)
    df["GHI"] = (
        4.0 * _z(df["NetScore"]) +
        3.0 * _z(df["ScoreTrend"]) +
        0.8 * _z(df["pct_change_24h"]) +
        0.5 * _z(df["fundingRate"]) +
        0.5 * _z(df["oi_delta"])
    )

    df = df.sort_values(["GHI", "quote_volume_24h"], ascending=[False, False], na_position="last").reset_index(drop=True)

    # CSV temporal (útil para depurar en Render)
    try:
        tmp = Path("/tmp/usdt_screener.csv")
        df.to_csv(tmp, index=False)
        logger(f"CSV guardado en {tmp}")
    except Exception as e:
        logger(f"No pude escribir CSV en /tmp: {e}")

    info = {
        "count": int(df.shape[0]),
        "elapsed_sec": round(time.time() - t0, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "params": {"LIMIT_SYMBOLS": LIMIT, "MAX_WORKERS": MAX_WORKERS, "BUDGET": TIME_BUDGET}
    }
    logger(json.dumps(info))
    return df, info

if __name__ == "__main__":
    # Prueba local rápida:
    df, info = build_df(limit_symbols=int(os.environ.get("LIMIT_SYMBOLS", "120")))
    print(df.head(20).to_string(index=False))
    print(info)
