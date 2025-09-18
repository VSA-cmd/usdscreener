# -*- coding: utf-8 -*-
"""
Binance_usdt_2.py — screener USDT para Binance global con DEADLINE suave.
Genera siempre un CSV (parcial o completo) para que la app lo lea.

Vars de entorno útiles:
- TOPN_FOR_DEEP (por defecto 80)   → símbolos “profundos” con klines/futuros
- KLINES_LIMIT (por defecto 60)
- DEADLINE_S   (por defecto 180)   → tiempo máximo blando para terminar
- SCREENER_CSV (por defecto usdt_screener.csv)
"""

from __future__ import annotations
import os, time, math, random
from typing import Any, Dict, Optional, Tuple, List
import requests
import pandas as pd
import numpy as np

CSV_NAME = os.getenv("SCREENER_CSV", "usdt_screener.csv")
SPOT_BASE = "https://api.binance.com"
FAPI_BASE = "https://fapi.binance.com"

TOPN_FOR_DEEP = int(os.getenv("TOPN_FOR_DEEP", "80"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "60"))
DEADLINE_S = int(os.getenv("DEADLINE_S", "180"))

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "0.8"))

EXCLUDE_SUFFIXES = ("UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT","VENUSDT")

sess = requests.Session()
sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/118 Safari/537.36"
})

def log(msg: str):  # visible en /view (stderr/ stdout)
    try:
        print(msg, flush=True)
    except Exception:
        pass

def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    backoff = HTTP_BACKOFF
    last = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            r = sess.get(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
            if r.status_code == 451:
                raise requests.HTTPError("451 Unavailable for legal reasons")
            ct = (r.headers.get("Content-Type") or "").lower()
            if "json" not in ct and "javascript" not in ct and "text/plain" not in ct:
                if "<html" in r.text.lower()[:200]:
                    raise requests.HTTPError("Unexpected HTML")
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt >= HTTP_MAX_RETRIES:
                break
            time.sleep(backoff * (1 + random.random()))
            backoff *= 1.6
    raise requests.RequestException(f"GET failed: {last}")

def fetch_spot() -> pd.DataFrame:
    url = f"{SPOT_BASE}/api/v3/ticker/24hr"
    data = http_get(url).json()
    df = pd.DataFrame(data)
    num_cols = ["lastPrice","priceChangePercent","quoteVolume","openPrice","prevClosePrice"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[df["symbol"].str.endswith("USDT", na=False)].copy()
    for bad in EXCLUDE_SUFFIXES:
        df = df[~df["symbol"].str.endswith(bad, na=False)]
    df.rename(columns={
        "lastPrice":"price","priceChangePercent":"pct_change_24h","quoteVolume":"quote_volume_24h"
    }, inplace=True)
    df.sort_values("quote_volume_24h", ascending=False, inplace=True)
    keep = ["symbol","price","pct_change_24h","quote_volume_24h","openPrice","prevClosePrice"]
    for c in keep:
        if c not in df.columns: df[c] = np.nan
    return df.reset_index(drop=True)

def fetch_klines_4h(symbol: str, limit: int) -> Optional[pd.DataFrame]:
    url = f"{SPOT_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": "4h", "limit": limit}
    try:
        arr = http_get(url, params).json()
        if not arr: return None
        cols=["openTime","open","high","low","close","volume","closeTime","qav","numTrades","takerBase","takerQuote","ignore"]
        df = pd.DataFrame(arr, columns=cols)
        for c in ("open","high","low","close","volume","qav","takerBase","takerQuote"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception:
        return None

def compute_trend_and_48h(dfk: Optional[pd.DataFrame]) -> Tuple[Optional[str], Optional[float]]:
    if dfk is None or dfk.empty: return None, None
    close = dfk["close"].astype(float).to_numpy()
    if len(close) < 13: return None, None
    last, prev = float(close[-1]), float(close[-13])
    pct48 = (last/prev - 1.0) if prev>0 else None
    sma = pd.Series(close).rolling(3).mean().dropna()
    if len(sma) < 6: return None, pct48
    slope = float(sma.iloc[-1] - sma.iloc[-6])
    pct_slope = slope / max(1e-9, float(sma.iloc[-6]))
    if pct_slope > 0.006: tr="Alcista"
    elif pct_slope < -0.006: tr="Bajista"
    else: tr="Lateral"
    return tr, pct48

def fetch_funding(symbol: str) -> Optional[float]:
    url = f"{FAPI_BASE}/fapi/v1/fundingRate"
    try:
        data = http_get(url, {"symbol": symbol, "limit":1}).json()
        if isinstance(data, list) and data:
            return float(data[-1].get("fundingRate","nan"))
    except Exception:
        return None
    return None

def fetch_oi(symbol: str) -> Optional[float]:
    url = f"{FAPI_BASE}/futures/data/openInterestHist"
    try:
        data = http_get(url, {"symbol": symbol, "period":"8h", "limit":1}).json()
        if isinstance(data, list) and data:
            return float(data[-1].get("sumOpenInterest","nan"))
    except Exception:
        return None
    return None

def now(): return time.time()

def build_df() -> pd.DataFrame:
    t0 = now()
    log("[1/4] Descargando tickers 24h...")
    spot = fetch_spot()
    if spot.empty:
        log("[warn] spot vacío; devolviendo placeholder")
        return pd.DataFrame([{"symbol":"BTCUSDT","price":116000,"pct_change_24h":0.01,"quote_volume_24h":1.8e9,"Trend_4H":"Alcista","ScoreTrend":0.8,"fundingRate":8.1e-5,"openInterest":88000}])

    for c in ["pct_change_48h","Trend_4H","ScoreTrend","fundingRate","openInterest"]:
        if c not in spot.columns: spot[c]=np.nan

    deep = spot["symbol"].head(TOPN_FOR_DEEP).tolist()
    # Heurística: si quedan < DEADLINE/2 tras tickers, recorta profundidad
    elapsed = now()-t0
    if elapsed > DEADLINE_S*0.25 and len(deep)>40:
        deep = deep[:40]
        log(f"[heuristic] lento al inicio; recorto profundidad a {len(deep)}")

    # 2) Klines
    log(f"[2/4] Klines 4h para {len(deep)} símbolos (limit={KLINES_LIMIT})…")
    for i, sym in enumerate(deep, 1):
        if now()-t0 > DEADLINE_S*0.75:  # deja margen para futuros y CSV
            log(f"[cut] tiempo al {i-1}/{len(deep)}; corto klines")
            break
        dfk = fetch_klines_4h(sym, KLINES_LIMIT)
        tr, p48 = compute_trend_and_48h(dfk)
        if tr is not None: spot.loc[spot["symbol"]==sym,"Trend_4H"]=tr
        if p48 is not None: spot.loc[spot["symbol"]==sym,"pct_change_48h"]=p48
        if i%10==0: log(f"  klines {i}/{len(deep)}")

    # 3) ScoreTrend
    def score(tr, p24):
        base = 0.0
        if isinstance(p24,(int,float)) and not pd.isna(p24):
            base = max(-0.10,min(0.10,p24))/0.10
        if tr=="Alcista": base += 0.4
        elif tr=="Bajista": base -= 0.4
        return float(max(-1.0,min(1.0,base)))
    spot["ScoreTrend"] = [score(tr,pc) for tr,pc in zip(spot.get("Trend_4H"), spot.get("pct_change_24h"))]

    # 4) Futuros (funding/OI)
    left = DEADLINE_S - (now()-t0)
    # según tiempo restante, limita tamaño
    max_fut = len(deep)
    if left < 60: max_fut = min(25, max_fut)
    elif left < 90: max_fut = min(35, max_fut)
    deep_fut = deep[:max_fut]
    log(f"[3/4] Futuros para {len(deep_fut)} símbolos…")
    for i, sym in enumerate(deep_fut, 1):
        if now()-t0 > DEADLINE_S*0.95:
            log(f"[cut] quedando poco tiempo; corto futuros en {i-1}/{len(deep_fut)}")
            break
        fr = fetch_funding(sym)
        oi = fetch_oi(sym)
        if fr is not None: spot.loc[spot["symbol"]==sym,"fundingRate"]=fr
        if oi is not None: spot.loc[spot["symbol"]==sym,"openInterest"]=oi
        if i%10==0: log(f"  futures {i}/{len(deep_fut)}")

    spot.sort_values("quote_volume_24h", ascending=False, inplace=True)
    cols = ["symbol","price","pct_change_24h","pct_change_48h","quote_volume_24h","Trend_4H","ScoreTrend","fundingRate","openInterest"]
    for c in cols:
        if c not in spot.columns: spot[c]=np.nan
    log(f"[4/4] Listo. filas={len(spot)}  t={now()-t0:,.1f}s")
    return spot[cols].reset_index(drop=True)

def run_screener():
    df = build_df()
    try:
        df.to_csv(CSV_NAME, index=False)
        log(f"[save] CSV -> {CSV_NAME}  rows={len(df)}")
    except Exception as e:
        log(f"[warn] no se pudo guardar CSV: {e}")

   
    #----
    if __name__ == "__main__":
    df = build_df()
    # Enviar el DF al servidor por stdout (sin CSV)
    try:
        import json
        print("__DFJSON__=" + df.to_json(orient="records"), flush=True)
    except Exception as e:
        # fallback opcional: si quisieras seguir guardando CSV en caso de error, descomenta:
        # df.to_csv(os.getenv("SCREENER_CSV", "usdt_screener.csv"), index=False)
        print(f"[warn] no se pudo emitir DFJSON: {e}", flush=True)

