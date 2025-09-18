# -*- coding: utf-8 -*-
"""
Binance_usdt_2.py

Screener simple para pares USDT en Binance GLOBAL (no .US), pensado para correr
en Render (plan free) y ser consumido por una app Flask que muestra un dashboard.

Salidas (DataFrame y opcional CSV):
- symbol, price, pct_change_24h, pct_change_48h
- quote_volume_24h
- Trend_4H (Alcista|Bajista|Lateral)
- ScoreTrend (float)
- fundingRate (última)
- openInterest (hist 8h última muestra)
"""

from __future__ import annotations

import os
import time
import math
import json
import random
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np

# ==============================
# Configuración general
# ==============================
FORCE_GLOBAL = os.getenv("FORCE_GLOBAL", "1").lower() in ("1", "true", "yes")
CSV_NAME = os.getenv("SCREENER_CSV", "usdt_screener.csv")

# Endpoints "raíz" (evitamos api1/api2 por problemas de DNS/VPN)
SPOT_BASE = "https://api.binance.com"
FAPI_BASE = "https://fapi.binance.com"  # Futuros USDⓈ-M (global)

# Parámetros HTTP
HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "5"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF", "0.8"))

# Límites para mantener el runtime bajo en Render free
TOPN_FOR_DEEP = int(os.getenv("TOPN_FOR_DEEP", "80"))  # cuántos símbolos top por volumen profundizamos (klines+futuros)
KLINES_INTERVAL = "4h"
KLINES_LIMIT = 60  # ~10 días de 4h; nos da señal y 48h

# Filtros de símbolos (excluimos tokens apalancados/fiat raros)
EXCLUDE_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT", "VENUSDT")
EXCLUDE_PREFIXES = ("USD",)  # p.ej. USDUSDT si existiera
ONLY_USDT = True


# ==============================
# Utilidades HTTP con retries
# ==============================
_session = requests.Session()
_session.headers.update({
    # User-Agent "normal" para evitar filtros tontos de CDN/WAF
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0 Safari/537.36"
    )
})

def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    """
    GET con reintentos exponenciales y defensas básicas.
    Lanza HTTPError si respuesta no-OK o si devuelve HTML en vez de JSON.
    """
    backoff = HTTP_BACKOFF_BASE
    last_exc: Optional[Exception] = None

    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            r = _session.get(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
            # Manejo explícito de 451 (geoblock)
            if r.status_code == 451:
                raise requests.HTTPError("451 Unavailable for legal reasons")
            # Validar content-type (evitar parsear HTML como JSON)
            ctyp = (r.headers.get("Content-Type") or "").lower()
            if "json" not in ctyp and "javascript" not in ctyp:
                # Algunas rutas devuelven text/plain con JSON; permitirlo
                if "text/plain" not in ctyp:
                    # Intentar detectar cuando devuelven una página HTML de error/proxy
                    if "<html" in r.text.lower()[:200]:
                        raise requests.HTTPError(f"Unexpected HTML content from {url}")
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if attempt >= HTTP_MAX_RETRIES:
                break
            # Backoff con jitter
            sleep_s = backoff * (1.0 + random.random())
            time.sleep(sleep_s)
            backoff *= 1.6
    # Si llegamos aquí, falló
    if isinstance(last_exc, requests.HTTPError):
        raise last_exc
    raise requests.RequestException(f"GET {url} failed after retries: {last_exc}")


# ==============================
# Fetchers Spot
# ==============================
def fetch_tickers_24h() -> pd.DataFrame:
    """
    /api/v3/ticker/24hr -> lista con info por símbolo.
    """
    url = f"{SPOT_BASE}/api/v3/ticker/24hr"
    r = http_get(url)
    data = r.json()
    df = pd.DataFrame(data)
    # Normalizar campos clave
    # Algunos campos vienen como strings -> convertir numéricos
    num_cols = [
        "lastPrice", "priceChangePercent", "quoteVolume",
        "openPrice", "prevClosePrice"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df.rename(columns={
        "symbol": "symbol",
        "lastPrice": "price",
        "priceChangePercent": "pct_change_24h",
        "quoteVolume": "quote_volume_24h",
    }, inplace=True)

    # Filtrar USDT
    if ONLY_USDT:
        df = df[df["symbol"].str.endswith("USDT", na=False)].copy()

    # Excluir tokens apalancados comunes
    mask_ex = pd.Series(False, index=df.index)
    for suf in EXCLUDE_SUFFIXES:
        mask_ex |= df["symbol"].str.endswith(suf, na=False)
    for pre in EXCLUDE_PREFIXES:
        mask_ex |= df["symbol"].str.startswith(pre, na=False)
    df = df[~mask_ex].copy()

    # Orden por volumen descendente
    if "quote_volume_24h" in df.columns:
        df.sort_values("quote_volume_24h", ascending=False, inplace=True)

    # Mantener columnas esperadas
    keep = ["symbol", "price", "pct_change_24h", "quote_volume_24h", "openPrice", "prevClosePrice"]
    for c in keep:
        if c not in df.columns:
            df[c] = np.nan
    return df.reset_index(drop=True)


def fetch_klines_4h(symbol: str, limit: int = KLINES_LIMIT) -> Optional[pd.DataFrame]:
    """
    /api/v3/klines (4h). Devuelve OHLCV.
    """
    url = f"{SPOT_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": KLINES_INTERVAL, "limit": limit}
    try:
        r = http_get(url, params=params)
        arr = r.json()
        if not arr:
            return None
        cols = [
            "openTime","open","high","low","close","volume","closeTime",
            "qav","numTrades","takerBase","takerQuote","ignore"
        ]
        df = pd.DataFrame(arr, columns=cols)
        for c in ("open","high","low","close","volume","qav","takerBase","takerQuote"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception:
        return None


def compute_trend_and_pct48(df_k: pd.DataFrame) -> Tuple[Optional[str], Optional[float]]:
    """
    A partir de klines 4h: calcula tendencia simple y cambio 48h.
    - Tendencia: pendiente de SMA(3) en últimos 6 puntos -> Alcista/Bajista/Lateral
    - pct_change_48h: close actual vs close de 12 velas atrás (48 horas)
    """
    if df_k is None or df_k.empty:
        return None, None

    closes = df_k["close"].astype(float).to_numpy()
    if len(closes) < 13:
        return None, None

    # 48h = 12 velas de 4h
    last = float(closes[-1])
    prev_48h = float(closes[-13])
    pct48 = (last / prev_48h - 1.0) if prev_48h > 0 else None

    # Tendencia con SMA(3)
    sma = pd.Series(closes).rolling(3).mean().dropna()
    if len(sma) < 6:
        trend = None
    else:
        # pendiente simple: diferencia entre último y el de 6 atrás (≈24h)
        slope = float(sma.iloc[-1] - sma.iloc[-6])
        pct_slope = (slope / max(1e-9, float(sma.iloc[-6])))
        if pct_slope > 0.006:       # > 0.6% ascenso
            trend = "Alcista"
        elif pct_slope < -0.006:    # < -0.6% descenso
            trend = "Bajista"
        else:
            trend = "Lateral"

    return trend, pct48


# ==============================
# Fetchers Futuros (funding, OI)
# ==============================
def fetch_latest_funding(symbol: str) -> Optional[float]:
    """
    /fapi/v1/fundingRate?symbol=BTCUSDT&limit=1
    Retorna funding rate más reciente (decimal, ej. 0.0001 = 0.01%)
    """
    url = f"{FAPI_BASE}/fapi/v1/fundingRate"
    params = {"symbol": symbol, "limit": 1}
    try:
        r = http_get(url, params=params)
        data = r.json()
        if isinstance(data, list) and data:
            fr = float(data[-1].get("fundingRate", "nan"))
            return fr
    except Exception:
        return None
    return None


def fetch_open_interest(symbol: str) -> Optional[float]:
    """
    /futures/data/openInterestHist?symbol=BTCUSDT&period=8h&limit=1
    Devuelve el OI de la última muestra (contratos).
    """
    url = f"{FAPI_BASE}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "8h", "limit": 1}
    try:
        r = http_get(url, params=params)
        data = r.json()
        if isinstance(data, list) and data:
            oi = float(data[-1].get("sumOpenInterest", "nan"))
            return oi
    except Exception:
        return None
    return None


# ==============================
# Pipeline principal
# ==============================
def build_screener_df() -> pd.DataFrame:
    """
    Orquesta la descarga y el ensamblado de métricas en un DataFrame.
    Limita klines/futuros a TOPN_FOR_DEEP por volumen para no exceder tiempo.
    """
    # 1) Tickers 24h (todos USDT) y ordenar por volumen
    spot = fetch_tickers_24h()
    if spot.empty:
        # fallback mínimo para no romper UI
        return pd.DataFrame([
            {"symbol":"BTCUSDT","price":116000.0,"pct_change_24h":0.0123,"quote_volume_24h":1.8e9,
             "Trend_4H":"Alcista","ScoreTrend":0.80,"fundingRate":8.1e-5,"openInterest":8.8e4},
            {"symbol":"ETHUSDT","price":3400.0,"pct_change_24h":-0.005,"quote_volume_24h":6.2e8,
             "Trend_4H":"Lateral","ScoreTrend":0.12,"fundingRate":6.5e-5,"openInterest":3.3e4},
        ])

    # Normaliza y crea columnas destino si faltan
    for c in ["pct_change_48h","Trend_4H","ScoreTrend","fundingRate","openInterest"]:
        if c not in spot.columns:
            spot[c] = np.nan

    # 2) Selección deep
    deep_symbols = spot["symbol"].head(TOPN_FOR_DEEP).tolist()

    # 3) Klines para tendencia y 48h
    for sym in deep_symbols:
        dfk = fetch_klines_4h(sym)
        tr, pct48 = compute_trend_and_pct48(dfk)
        if tr is not None:
            spot.loc[spot["symbol"] == sym, "Trend_4H"] = tr
        if pct48 is not None:
            spot.loc[spot["symbol"] == sym, "pct_change_48h"] = pct48

    # 4) ScoreTrend (numérico) a partir de Trend_4H + magnitud de 24h
    def score_from(trend: Optional[str], pct24: Optional[float]) -> float:
        base = 0.0
        if isinstance(pct24, (int, float)) and not math.isnan(pct24):
            # saturar a +/- 10% para evitar outliers
            base = max(-0.10, min(0.10, pct24)) / 0.10  # -1..+1
        if trend == "Alcista":
            base += 0.4
        elif trend == "Bajista":
            base -= 0.4
        # clamp -1..+1
        return float(max(-1.0, min(1.0, base)))

    spot["ScoreTrend"] = [
        score_from(tr, pc24) for tr, pc24 in zip(spot.get("Trend_4H"), spot.get("pct_change_24h"))
    ]

    # 5) Futuros: funding & OI (solo deep para ahorrar tiempo)
    for sym in deep_symbols:
        try:
            fr = fetch_latest_funding(sym)
        except Exception:
            fr = None
        try:
            oi = fetch_open_interest(sym)
        except Exception:
            oi = None
        if fr is not None:
            spot.loc[spot["symbol"] == sym, "fundingRate"] = fr
        if oi is not None:
            spot.loc[spot["symbol"] == sym, "openInterest"] = oi

    # 6) Ordenar por volumen 24h y devolver columnas esperadas
    cols = [
        "symbol", "price", "pct_change_24h", "pct_change_48h",
        "quote_volume_24h", "Trend_4H", "ScoreTrend",
        "fundingRate", "openInterest"
    ]
    for c in cols:
        if c not in spot.columns:
            spot[c] = np.nan

    spot.sort_values("quote_volume_24h", ascending=False, inplace=True)
    return spot[cols].reset_index(drop=True)


# ==============================
# API pública para la app web
# ==============================
def run_screener(return_df: bool = False) -> pd.DataFrame:
    """
    Ejecuta el screener y si return_df=True devuelve el DataFrame en memoria.
    Si return_df=False y este archivo se ejecuta como script, también escribe CSV.
    """
    df = build_screener_df()

    if return_df:
        return df

    # Modo CLI: escribir CSV si se llama directamente
    try:
        df.to_csv(CSV_NAME, index=False)
    except Exception:
        pass
    return df


# ==============================
# Ejecución directa
# ==============================
if __name__ == "__main__":
    # Ejecutar y dejar CSV local (para pruebas fuera de Render)
    run_screener(return_df=False)
    # print opcional (no es necesario):
    try:
        print(f"[OK] Screener listo. CSV: {CSV_NAME}")
    except Exception:
        pass
