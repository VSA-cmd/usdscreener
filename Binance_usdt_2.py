# -*- coding: utf-8 -*-
"""
Motor de screener USDT para Render (ligero):
- Universo: todos los pares SPOT con quote=USDT & status=TRADING
- Cruce con /ticker/24hr para precio, %24h y quoteVolume
- Tendencia 4H: Close vs EMA50 + slope
- Derivados (USDT-M): fundingRate (último) + OI 1h (delta)
- NetScore + GHI-lite
- HTTP robusto con rotación de hosts y reintentos
- Concurrencia limitada + presupuesto de tiempo
ENV:
  FORCE_GLOBAL=1
  LIMIT_SYMBOLS=150
  MAX_WORKERS=12
  TIME_BUDGET_SECONDS=110
"""
import os, time, json, random
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import numpy as np

# ---------------- HTTP ----------------
SPOT_HOSTS = ["https://api.binance.com","https://api1.binance.com","https://api2.binance.com","https://api3.binance.com"]
FUTU_HOSTS = ["https://fapi.binance.com","https://fapi1.binance.com","https://fapi2.binance.com"]

FORCE_GLOBAL = os.environ.get("FORCE_GLOBAL","1") == "1"
SPOT_BASES = SPOT_HOSTS if FORCE_GLOBAL else [SPOT_HOSTS[0]]
FUTU_BASES = FUTU_HOSTS if FORCE_GLOBAL else []

HTTP_CONNECT_TIMEOUT = 5
HTTP_READ_TIMEOUT    = 15
HTTP_MAX_RETRIES     = 5
HTTP_BACKOFF         = 0.8
HTTP_JITTER          = (0.0, 0.6)
_session = requests.Session()

def http_get(urls, params=None, timeout=None, retries=None, backoff=None):
    if isinstance(urls, str): urls = [urls]
    timeout = timeout or (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)
    retries = HTTP_MAX_RETRIES if retries is None else retries
    backoff = HTTP_BACKOFF if backoff is None else backoff
    last = None
    for a in range(1, retries+1):
        hosts = urls if a==1 else random.sample(urls, len(urls))
        for u in hosts:
            try:
                r = _session.get(u, params=params, timeout=timeout)
                if r.status_code in (429,418) or 500 <= r.status_code < 600:
                    last = Exception(f"HTTP {r.status_code}")
                    continue
                r.raise_for_status()
                return r.json()
            except (requests.ConnectTimeout, requests.ReadTimeout) as e:
                last = e; continue
            except requests.RequestException as e:
                last = e; continue
        time.sleep(backoff*a + random.uniform(*HTTP_JITTER))
    raise last or RuntimeError("http_get failed")

def log_default(m): print(m)

# ---------------- Datos base ----------------
def fetch_usdt_symbols_trading(logger=log_default):
    data = http_get([f"{b}/api/v3/exchangeInfo" for b in SPOT_BASES])
    out = []
    for s in data.get("symbols", []):
        if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
            out.append(s["symbol"])
    logger(f"TRADING USDT symbols: {len(out)}")
    return out

def fetch_24h_tickers(logger=log_default):
    arr = http_get([f"{b}/api/v3/ticker/24hr" for b in SPOT_BASES]) or []
    d = {}
    for t in arr:
        sym = t.get("symbol"); if not sym: continue
        d[sym] = {
            "price": float(t.get("lastPrice") or 0.0),
            "pct_change_24h": float(t.get("priceChangePercent") or 0.0)/100.0,
            "quote_volume_24h": float(t.get("quoteVolume") or 0.0),
        }
    logger(f"Ticker 24h objetos: {len(d)}")
    return d

def fetch_klines_4h(symbol, limit=120):
    data = http_get([f"{b}/api/v3/klines" for b in SPOT_BASES], {"symbol":symbol,"interval":"4h","limit":limit})
    if not data: return None
    df = pd.DataFrame(data, columns=["t","o","h","l","c","v","t2","q","n","tb","tq","i"])
    df["c"] = pd.to_numeric(df["c"], errors="coerce")
    return df

# ---------------- Futuros ----------------
_fut_sym_cache = {}
def futures_symbol(symbol_spot: str) -> str|None:
    if not FUTU_BASES: return None
    cand = symbol_spot  # mismo nombre en USDT-M
    if cand in _fut_sym_cache: return _fut_sym_cache[cand]
    try:
        data = http_get([f"{b}/fapi/v1/exchangeInfo" for b in FUTU_BASES], {"symbol": cand})
        if data and data.get("symbols"): _fut_sym_cache[cand] = cand; return cand
    except Exception: pass
    _fut_sym_cache[cand] = None
    return None

def fetch_funding_rate_last(symbol_fut: str):
    if not FUTU_BASES: return None
    for b in FUTU_BASES:
        try:
            arr = http_get(f"{b}/fapi/v1/fundingRate", {"symbol":symbol_fut,"limit":3})
            if arr: return float(arr[-1]["fundingRate"])
        except Exception: continue
    return None

def fetch_open_interest_hist(symbol_fut: str, period="1h", limit=10):
    if not FUTU_BASES: return None
    for b in FUTU_BASES:
        try:
            arr = http_get(f"{b}/futures/data/openInterestHist", {"symbol":symbol_fut,"period":period,"limit":limit})
            if arr:
                df = pd.DataFrame(arr)
                df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
                return df["sumOpenInterest"]
        except Exception: continue
    return None

# ---------------- Señales ----------------
FUND_BULL_T = -0.0005
FUND_BEAR_T =  0.0010

def compute_trend_4h(df4: pd.DataFrame):
    ema = df4["c"].ewm(span=50, adjust=False).mean()
    ema50 = float(ema.iloc[-1])
    close = float(df4["c"].iloc[-1])
    slope = float(ema.diff().tail(5).mean()) if len(ema)>=5 else 0.0
    if close > ema50 and slope > 0: label = "Alcista"
    elif close < ema50 and slope < 0: label = "Bajista"
    else: label = "Lateral"
    score_trend = (close/ema50 - 1.0) if ema50 else 0.0
    return label, float(score_trend)

def build_one(symbol: str, tick: dict, logger=log_default):
    df4 = fetch_klines_4h(symbol, 120)
    if df4 is None or df4.empty: return None
    trend_label, score_trend = compute_trend_4h(df4)

    fr = None; oi_last=None; oi_prev=None; oi_delta=None
    fut = futures_symbol(symbol)
    if fut:
        fr = fetch_funding_rate_last(fut)
        oi_hist = fetch_open_interest_hist(fut, "1h", 10)
        if isinstance(oi_hist, pd.Series) and len(oi_hist)>=2:
            oi_last = float(oi_hist.iloc[-1]); oi_prev = float(oi_hist.iloc[-2])
            oi_delta = oi_last - oi_prev

    points = 0.0
    if fr is not None:
        if fr < FUND_BULL_T: points += 3.0
        elif fr > FUND_BEAR_T: points -= 3.0
    if oi_delta is not None:
        if oi_delta > 0 and score_trend > 0: points += 2.0
        elif oi_delta > 0 and score_trend <= 0: points -= 2.0
        else: points -= 1.0

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

def _z(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.mean(); sd = s.std(ddof=0)
    if not np.isfinite(sd) or sd == 0: return pd.Series(np.zeros(len(s)), index=s.index)
    out = (s - mu) / sd; out[~np.isfinite(out)] = 0.0; return out

def build_df(limit_symbols: int|None = None, logger=log_default):
    t0 = time.time()
    TIME_BUDGET = int(os.environ.get("TIME_BUDGET_SECONDS","110"))
    LIMIT = int(os.environ.get("LIMIT_SYMBOLS", str(limit_symbols or 150)))
    MAX_WORKERS = int(os.environ.get("MAX_WORKERS","12"))

    logger(f"Iniciando job · LIMIT_SYMBOLS={LIMIT} MAX_WORKERS={MAX_WORKERS} BUDGET={TIME_BUDGET}s")

    trading = set(fetch_usdt_symbols_trading(logger))
    tickers = fetch_24h_tickers(logger)

    base_rows = []
    for sym in trading:
        t = tickers.get(sym)
        if t: base_rows.append({"symbol":sym, **t})
    if not base_rows:
        return pd.DataFrame(), {"count":0}

    base = pd.DataFrame(base_rows).sort_values("quote_volume_24h", ascending=False).head(LIMIT).reset_index(drop=True)
    logger(f"Candidatos tras cruce & rank por liquidez: {base.shape[0]}")

    out = []
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for _, r in base.iterrows():
            futures.append(ex.submit(build_one, r["symbol"], r.to_dict(), logger))
        for f in as_completed(futures, timeout=max(5, TIME_BUDGET - (time.time()-t0))):
            try:
                res = f.result(timeout=2)
                if res: out.append(res)
            except Exception as e:
                logger(f"Tarea falló: {e}")

    df = pd.DataFrame(out)
    if df.empty:
        return df, {"count":0}

    df["GHI"] = (
        4.0*_z(df["NetScore"]) + 3.0*_z(df["ScoreTrend"]) + 0.8*_z(df["pct_change_24h"]) +
        0.5*_z(df["fundingRate"]) + 0.5*_z(df["oi_delta"])
    )
    df = df.sort_values(["GHI","quote_volume_24h"], ascending=[False,False], na_position="last").reset_index(drop=True)

    try:
        tmp = Path("/tmp/usdt_screener.csv"); df.to_csv(tmp, index=False)
        logger(f"CSV guardado en {tmp}")
    except Exception as e:
        logger(f"No pude escribir CSV en /tmp: {e}")

    info = {
        "count": int(df.shape[0]),
        "elapsed_sec": round(time.time()-t0, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "params": {"LIMIT_SYMBOLS": LIMIT, "MAX_WORKERS": MAX_WORKERS, "BUDGET": TIME_BUDGET}
    }
    logger(json.dumps(info))
    return df, info

if __name__ == "__main__":
    df, info = build_df(limit_symbols=int(os.environ.get("LIMIT_SYMBOLS","120")))
    print(df.head(20).to_string(index=False))
    print(info)
