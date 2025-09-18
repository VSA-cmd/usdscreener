# -*- coding: utf-8 -*-
"""
USDT Screener:
- MTFA (4H Trend)
- News Sentiment (opcional)
- Candlestick Patterns (opcional con pandas-ta)
- Backtesting ligero + Fallback Global por deciles
- GHI (índice compuesto) + Buckets + Impresión compacta
- Funding Rate & Open Interest (Futuros USDT-M)
- HTTP robusto con rotación de hosts y reintentos (timeout-safe)
- Export robusto (CSV/Parquet o CSV.GZ)
- Logging y manejo de errores

Recomendado instalar (opcionales):
    pip install pyarrow
    pip install vaderSentiment textblob
    pip install pandas-ta
    pip install python-decouple
"""

import os
import time
import json
import math
import random
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np

# ------------------------------ Logging -----------------------------------------
LOG_DIR = Path("logs"); LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "usdt.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("usdt_screener")

def is_df_ok(x):
    """True si x es un DataFrame no vacío."""
    return isinstance(x, pd.DataFrame) and not x.empty

# ---------------- Env / API Keys (seguro) ----------------------------------------
def getenv(key, default=None):
    try:
        from decouple import config as env_config
        return env_config(key, default=default)
    except Exception:
        return os.environ.get(key, default)

FINNHUB_API_KEY = getenv("FINNHUB_API_KEY")
ALPACA_API_KEY  = getenv("ALPACA_API_KEY")
CRYPTOCONTROL_API_KEY = getenv("CRYPTOCONTROL_API_KEY")

# -------------------- Engines/Extras opcionales ----------------------------------
try:
    import pyarrow  # noqa
    PARQUET_ENGINE = "pyarrow"
except Exception:
    try:
        import fastparquet  # noqa
        PARQUET_ENGINE = "fastparquet"
    except Exception:
        PARQUET_ENGINE = None

# NLP
VADER = None
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER = SentimentIntensityAnalyzer()
    log.info("VADER listo para análisis de sentimiento.")
except Exception:
    log.info("VADER no disponible. Probando TextBlob...")
    try:
        from textblob import TextBlob  # noqa
        VADER = "TEXTBLOB"
        log.info("Usando TextBlob como fallback NLP.")
    except Exception:
        VADER = None
        log.info("Sin NLP (vader/textblob). SentimentScore se omitirá.")

# Candlestick patterns
PTA = False
try:
    import pandas_ta as ta  # noqa
    PTA = True
    log.info("pandas-ta disponible (patrones de velas).")
except Exception:
    PTA = False
    log.info("pandas-ta no disponible; se omiten patrones de velas.")

# ------------------------------ Config -------------------------------------------
# === Host pools para rotación si hay timeout / rate limit ===
SPOT_BASES = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]
FAPI_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]

# Endpoints como listas (probará en orden/rotación)
EXCHANGE_INFO_URLS = [f"{b}/api/v3/exchangeInfo" for b in SPOT_BASES]
TICKER_24H_URLS    = [f"{b}/api/v3/ticker/24hr"  for b in SPOT_BASES]
KLINES_URLS        = [f"{b}/api/v3/klines"       for b in SPOT_BASES]

FAPI_EXCHANGE_INFO_URLS = [f"{b}/fapi/v1/exchangeInfo"          for b in FAPI_BASES]
FAPI_FUNDING_RATE_URLS  = [f"{b}/fapi/v1/fundingRate"           for b in FAPI_BASES]
FAPI_OI_HIST_URLS       = [f"{b}/futures/data/openInterestHist" for b in FAPI_BASES]

# MTFA
MTFA_INTERVAL_LOW   = "1h"
MTFA_INTERVAL_HIGH  = "4h"
TREND_EMA_PERIOD    = 50

# Indicadores / contexto
ENABLE_INDICATORS = True
ENABLE_CONTEXT_4H = True  # MTFA

# Orden principal por defecto
ORDER_BY = "GHI"   # "WeightedNetScore" o "NetScore"
DESC     = True

# Concurrencia
MAX_WORKERS = 8

# Export
EXPORT_CSV_PATH          = "usdt_screener.csv"
EXPORT_PARQUET_PATH      = "usdt_screener.parquet"
EXPORT_MODELS_JSON_PATH  = "usdt_models.json"

# Backtesting (ligero)
BACKTEST_ENABLE            = True
BACKTEST_LOOKBACK_HOURS    = 24 * 30
BACKTEST_FORWARD_HOURS_SET = [6, 12]
BACKTEST_TOP_LIQ_SYMBOLS   = 200
RIDGE_L2                   = 1e-2

# Señales/umbral
VOL_SPIKE_FACTOR = 1.5

# Sentiment config
ENABLE_NEWS_SENTIMENT = True
NEWS_WINDOW_HOURS     = 24
NEWS_MAX_SYMBOLS      = 30
SENTIMENT_ALPHA       = 0.5  # ScoreAdj *= (1 + alpha * SentimentScore)

# Trend boosts
TREND_BOOST_UP_POS    = 1.50
TREND_BOOST_UP_NEG    = 0.80
TREND_BOOST_DOWN_POS  = 0.75
TREND_BOOST_DOWN_NEG  = 1.20
TREND_LATERAL_FACTOR  = 1.00

# Funding/Open Interest reglas (puntos que se añaden al NetScore)
FUNDING_BULL_THRESH   = -0.0005   # < -0.05% -> +3
FUNDING_BEAR_THRESH   =  0.0010   # > +0.10% -> -3
FUNDING_BULL_POINTS   = 3.0
FUNDING_BEAR_POINTS   = -3.0
OI_TREND_CONFIRM_UP   = 2.0
OI_TREND_CONFIRM_DOWN = -2.0
OI_WEAKEN_POINTS      = -1.0

# ----------------------- HTTP robusto: sesión + reintentos + rotación ------------
HTTP_CONNECT_TIMEOUT = 6
HTTP_READ_TIMEOUT    = 20
HTTP_MAX_RETRIES     = 6
HTTP_BACKOFF         = 0.8
HTTP_JITTER          = (0.0, 0.6)

_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=Retry(total=0))
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

def http_get(url_or_urls, params=None, timeout=None, max_retries=None, backoff=None):
    """
    Acepta str o list[str] de URLs. Rota hosts si falla (timeout, 429, 5xx).
    Usa backoff exponencial con jitter.
    """
    urls = [url_or_urls] if isinstance(url_or_urls, str) else list(url_or_urls)
    if not urls:
        raise ValueError("http_get: lista de URLs vacía")

    ct = HTTP_CONNECT_TIMEOUT
    rt = HTTP_READ_TIMEOUT
    if isinstance(timeout, (int, float)):
        rt = float(timeout)
    elif isinstance(timeout, tuple) and len(timeout) == 2:
        ct, rt = float(timeout[0]), float(timeout[1])

    retries = HTTP_MAX_RETRIES if max_retries is None else int(max_retries)
    base_bo = HTTP_BACKOFF if backoff is None else float(backoff)

    last_exc = None
    for attempt in range(1, retries + 1):
        try_hosts = urls[:]
        if attempt > 1:
            random.shuffle(try_hosts)

        for u in try_hosts:
            try:
                r = _session.get(u, params=params, timeout=(ct, rt))
                if r.status_code in (429, 418):
                    log.warning(f"Rate limit {r.status_code} en {u}. Probando siguiente host…")
                    last_exc = requests.HTTPError(f"Rate limit {r.status_code}")
                    continue
                if 500 <= r.status_code < 600:
                    log.warning(f"HTTP {r.status_code} en {u}. Probando siguiente host…")
                    last_exc = requests.HTTPError(f"Server error {r.status_code}")
                    continue
                r.raise_for_status()
                return r
            except (requests.ReadTimeout, requests.ConnectTimeout) as e:
                log.warning(f"http_get intento {attempt} host {u} timeout: {e}")
                last_exc = e
                continue
            except requests.RequestException as e:
                log.warning(f"http_get intento {attempt} host {u} falló: {e}")
                last_exc = e
                continue

        sleep_s = base_bo * attempt + random.uniform(*HTTP_JITTER)
        time.sleep(sleep_s)

    if last_exc:
        raise last_exc
    raise RuntimeError("http_get: falló sin excepción específica")

# -------------------------------- Conexión/base -----------------------------------
def connect_to_binance():
    return True

def obtener_simbolos_usdt_trading():
    """Devuelve todos los símbolos SPOT en estado TRADING con quoteAsset == 'USDT'."""
    resp = http_get(EXCHANGE_INFO_URLS)
    data = resp.json()
    out = []
    for s in data.get("symbols", []):
        if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
            out.append({"symbol": s["symbol"], "base": s["baseAsset"], "quote": s["quoteAsset"]})
    log.info(f"Símbolos USDT en TRADING: {len(out)}")
    return out

def obtener_ticker_24h_todos():
    resp = http_get(TICKER_24H_URLS)
    arr = resp.json()
    out = {}
    for row in arr:
        sym = row.get("symbol")
        if not sym: continue
        out[sym] = {
            "last_price": float(row.get("lastPrice", 0.0) or 0.0),
            "pct_24h":    float(row.get("priceChangePercent", 0.0) or 0.0),
            "quoteVolume":float(row.get("quoteVolume", 0.0) or 0.0)
        }
    log.info(f"Ticker 24h recibidos: {len(out)} símbolos.")
    return out

# ------------------------------ Klines/Precios ------------------------------------
def klines_raw(symbol, interval="1h", start_ms=None, limit=200, end_ms=None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None: params["startTime"] = int(start_ms)
    if end_ms   is not None: params["endTime"]   = int(end_ms)
    r = http_get(KLINES_URLS, params=params)
    return r.json()

def fetch_data(symbol, interval="1h", lookback_hours=120):
    limit = min(max(int(lookback_hours / (1 if interval=="1h" else 4)) + 10, 60), 1000)
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - int(lookback_hours * 3600 * 1000)
    data = klines_raw(symbol, interval, start_ms=start_ms, limit=limit)
    if not data:
        return pd.DataFrame()
    cols = ["openTime","open","high","low","close","volume",
            "closeTime","qav","numTrades","takerBuyBase","takerBuyQuote","ignore"]
    df = pd.DataFrame(data, columns=cols)
    df["openTime"]  = pd.to_datetime(df["openTime"], unit="ms", utc=True)
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms", utc=True)
    for c in ["open","high","low","close","volume","qav","takerBuyBase","takerBuyQuote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}, inplace=True)
    df.set_index("closeTime", inplace=True)
    return df

def precio_interpolado(symbol, target_ms, interval_ms=3600_000):
    start = target_ms - (3 * interval_ms)
    kl = klines_raw(symbol, "1h", start_ms=start, limit=6)
    if not kl:
        return None
    closes = [(int(row[6]), float(row[4])) for row in kl]
    closes.sort(key=lambda x: x[0])
    prev_pt = None; next_pt = None
    for (ct, cp) in closes:
        if ct <= target_ms: prev_pt = (ct, cp)
        if ct >= target_ms and next_pt is None:
            next_pt = (ct, cp); break
    if prev_pt and next_pt and next_pt[0] != prev_pt[0]:
        t0, p0 = prev_pt; t1, p1 = next_pt
        w = (target_ms - t0) / (t1 - t0)
        return p0 + w * (p1 - p0)
    nearest = min(closes, key=lambda x: abs(x[0] - target_ms))
    return nearest[1]

# ---------------------------- Indicadores / Patrones ------------------------------
def calculate_technical_indicators(df):
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return df

    try:
        df = df.copy()
        # EMAs
        df["EMA_12"] = df["Close"].ewm(span=12, adjust=False).mean()
        df["EMA_26"] = df["Close"].ewm(span=26, adjust=False).mean()
        df["EMA_50"] = df["Close"].ewm(span=50, adjust=False).mean()
        # RSI
        delta = df["Close"].diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)
        roll_up   = gain.ewm(alpha=1/14, adjust=False).mean()
        roll_down = loss.ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-12)
        RS = roll_up / roll_down
        df["RSI_14"] = 100 - (100 / (1 + RS))
        # MACD
        macd_line = df["EMA_12"] - df["EMA_26"]
        signal    = macd_line.ewm(span=9, adjust=False).mean()
        df["MACD"] = macd_line; df["MACD_signal"] = signal; df["MACD_hist"] = macd_line - signal
        # Volumen SMA
        df["Vol_SMA20"]  = df["Volume"].rolling(window=20, min_periods=1).mean()

        # Patrones de velas (pandas-ta) en DF temporal plano
        if PTA:
            try:
                import pandas_ta as ta  # noqa
                df_tmp = df[["Open","High","Low","Close"]].rename(
                    columns={"Open":"open","High":"high","Low":"low","Close":"close"}
                ).reset_index(drop=True)
                patframes = []
                for pat in ["engulfing","hammer","morningstar","shootingstar","doji"]:
                    pf = df_tmp.ta.cdl_pattern(name=pat)
                    if pf is not None and not pf.empty:
                        patframes.append(pf)
                if patframes:
                    pats = pd.concat(patframes, axis=1)
                    for c in pats.columns:
                        df[c] = pats[c].values
            except Exception as e:
                log.warning(f"No se pudieron calcular patrones de velas (pandas-ta): {e}")

        return df
    except Exception as e:
        log.warning(f"calculate_technical_indicators fallo: {e}")
        return df

def señales_vector(df, ctx_df=None):
    if not is_df_ok(df):
        return pd.DataFrame()
    s = pd.DataFrame(index=df.index)
    # RSI
    s["rsi_sig"] = 0
    s.loc[df["RSI_14"] < 30, "rsi_sig"] =  1
    s.loc[df["RSI_14"] > 70, "rsi_sig"] = -1
    # MACD cross
    macd_cross_up   = (df["MACD"] > df["MACD_signal"]) & (df["MACD"].shift(1) <= df["MACD_signal"].shift(1))
    macd_cross_down = (df["MACD"] < df["MACD_signal"]) & (df["MACD"].shift(1) >= df["MACD_signal"].shift(1))
    s["macd_cross"] = 0
    s.loc[macd_cross_up,  "macd_cross"] =  1
    s.loc[macd_cross_down,"macd_cross"] = -1
    # MACD hist slope
    s["macd_hist_slope"] = 0
    hist_diff = df["MACD_hist"] - df["MACD_hist"].shift(1)
    s.loc[hist_diff > 0, "macd_hist_slope"] =  1
    s.loc[hist_diff < 0, "macd_hist_slope"] = -1
    # EMA 12/26
    s["ema_bull"] = 0
    s.loc[df["EMA_12"] > df["EMA_26"], "ema_bull"] =  1
    s.loc[df["EMA_12"] < df["EMA_26"], "ema_bull"] = -1
    # Close vs EMA50
    s["above_ema50"] = 0
    s.loc[df["Close"] > df["EMA_50"], "above_ema50"] =  1
    s.loc[df["Close"] < df["EMA_50"], "above_ema50"] = -1
    # Vol spike
    s["vol_spike"] = 0
    spike = df["Volume"] > (VOL_SPIKE_FACTOR * df["Vol_SMA20"])
    green = df["Close"] > df["Open"]; red = df["Close"] < df["Open"]
    s.loc[ spike & green, "vol_spike"] =  1
    s.loc[ spike & red,   "vol_spike"] = -1

    # Contexto 4H
    if is_df_ok(ctx_df) and "EMA_50" in ctx_df and "Close" in ctx_df:
        ctx = ctx_df[["EMA_50","Close"]].copy()
        ctx = ctx.rename(columns={"EMA_50":"EMA50_4H","Close":"Close_4H"})
        ctx = ctx.reindex(df.index.union(ctx.index)).sort_index().ffill()
        s["ctx_up"] = 0
        s.loc[ctx.loc[df.index, "Close_4H"] > ctx.loc[df.index, "EMA50_4H"], "ctx_up"] =  1
        s.loc[ctx.loc[df.index, "Close_4H"] < ctx.loc[df.index, "EMA50_4H"], "ctx_up"] = -1
    else:
        s["ctx_up"] = 0

    # Patrones (si existen)
    if PTA:
        for name in ["CDL_ENGULFING","CDL_HAMMER","CDL_MORNINGSTAR","CDL_SHOOTINGSTAR","CDL_DOJI"]:
            if name in df.columns:
                s[name] = np.sign(df[name]).fillna(0)

    s = s.dropna()
    return s

# ---------------------------- Backtest / Ridge -----------------------------------
def ridge_fit(X, y, l2=RIDGE_L2):
    Xb = np.hstack([X, np.ones((X.shape[0], 1))])
    I  = np.eye(Xb.shape[1]); I[-1, -1] = 0.0
    w  = np.linalg.pinv(Xb.T @ Xb + l2 * I) @ (Xb.T @ y)
    coef, bias = w[:-1], w[-1]
    return coef, bias

def backtest_calibrar(df_1h, df_4h, horizons):
    res = {}
    if not is_df_ok(df_1h):
        return res
    S = señales_vector(df_1h, df_4h)
    feat_cols = ["rsi_sig","macd_cross","macd_hist_slope","ema_bull","above_ema50","vol_spike","ctx_up"]
    if PTA:
        for name in ["CDL_ENGULFING","CDL_HAMMER","CDL_MORNINGSTAR","CDL_SHOOTINGSTAR","CDL_DOJI"]:
            if name in S.columns:
                feat_cols.append(name)

    S = S.loc[:, feat_cols].copy()
    close = df_1h["Close"].reindex(S.index)

    for H in horizons:
        fwd = (close.shift(-H) / close - 1.0).dropna()
        common_idx = S.index.intersection(fwd.index)
        X = S.loc[common_idx].values.astype(float)
        y = fwd.loc[common_idx].values.astype(float)
        if len(y) < 50:
            continue
        coef, bias = ridge_fit(X, y, l2=RIDGE_L2)
        scores_hist = (X @ coef + bias)
        bins = np.quantile(scores_hist, np.linspace(0, 1, 11))
        bin_idx = np.digitize(scores_hist, bins[1:-1], right=True)
        prob_up, ret_med = [], []
        for b in range(10):
            mask = (bin_idx == b)
            if mask.sum() == 0:
                prob_up.append(np.nan); ret_med.append(np.nan)
            else:
                ys = y[mask]
                prob_up.append(float((ys > 0).mean()))
                ret_med.append(float(np.nanmedian(ys)))
        res[H] = {"feat_cols": feat_cols, "weights": coef.tolist(), "bias": float(bias),
                  "bins": bins.tolist(), "prob_up_per_bin": prob_up, "ret_median_per_bin": ret_med}
    return res

def score_actual_y_proyeccion(S_row, model):
    feat_cols = model["feat_cols"]
    x = S_row.reindex(feat_cols).values.astype(float)
    score = float(np.dot(x, np.array(model["weights"])) + model["bias"])
    bins = np.array(model["bins"])
    b = int(np.digitize(score, bins[1:-1], right=True))
    b = max(0, min(9, b))
    prob = model["prob_up_per_bin"][b]
    rexp = model["ret_median_per_bin"][b]
    return score, b, prob, rexp

# --------------------------- News Sentiment (opcional) ----------------------------
def sentiment_score_text(text: str) -> float | None:
    if not text:
        return None
    try:
        if VADER and hasattr(VADER, "polarity_scores"):
            s = VADER.polarity_scores(text)  # type: ignore
            return float(s.get("compound", 0.0))
    except Exception:
        pass
    if VADER == "TEXTBLOB":
        try:
            from textblob import TextBlob
            return float(TextBlob(text).sentiment.polarity)
        except Exception:
            return None
    return None

def fetch_news_sentiment(base_symbol: str) -> tuple[float | None, int]:
    headlines: list[str] = []
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=NEWS_WINDOW_HOURS)

    if FINNHUB_API_KEY:
        try:
            url = "https://finnhub.io/api/v1/news"
            params = {"category": "crypto", "token": FINNHUB_API_KEY}
            r = http_get(url, params=params, timeout=12)
            for item in r.json():
                ts = item.get("datetime")
                if ts:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    if dt < since: continue
                title = item.get("headline") or item.get("title")
                if title and base_symbol.upper() in title.upper():
                    headlines.append(title)
        except Exception as e:
            log.warning(f"Finnhub news fallo: {e}")

    if ALPACA_API_KEY:
        try:
            url = "https://data.alpaca.markets/v1beta1/news"
            params = {"symbols": f"{base_symbol.upper()}USD", "start": since.isoformat().replace("+00:00","Z")}
            r = http_get(url, params=params, timeout=12)
            data = r.json().get("news", [])
            for item in data:
                title = item.get("headline") or item.get("title")
                if title:
                    headlines.append(title)
        except Exception as e:
            log.warning(f"Alpaca news fallo: {e}")

    if CRYPTOCONTROL_API_KEY:
        try:
            url = "https://cryptocontrol.io/api/v1/public/news/coin/" + base_symbol.upper()
            params = {"key": CRYPTOCONTROL_API_KEY, "latest": "true"}
            r = http_get(url, params=params, timeout=12)
            for item in r.json():
                pub = item.get("publishedAt")
                if pub:
                    try:
                        dt = datetime.fromisoformat(pub.replace("Z","+00:00"))
                        if dt < since: continue
                    except Exception:
                        pass
                title = item.get("title")
                if title:
                    headlines.append(title)
        except Exception as e:
            log.warning(f"CryptoControl news fallo: {e}")

    if not headlines:
        return None, 0

    scores = [sentiment_score_text(t) for t in headlines]
    scores = [s for s in scores if s is not None]
    if not scores:
        return None, 0

    avg = float(np.mean(scores))
    avg = max(-1.0, min(1.0, avg))
    return avg, len(scores)

# ----------------------- Funding Rate & Open Interest (USDT-M) --------------------
_fut_symbol_cache: dict[str, str | None] = {}

def futures_symbol_for_base(base: str) -> str | None:
    """
    Heurística: mapear 'BTC' -> 'BTCUSDT' (USDT-M). Verifica en exchangeInfo de FAPI.
    """
    base_up = base.upper()
    if base_up in _fut_symbol_cache:
        return _fut_symbol_cache[base_up]
    cand = f"{base_up}USDT"
    try:
        r = http_get(FAPI_EXCHANGE_INFO_URLS, params={"symbol": cand}, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
        js = r.json()
        if js and js.get("symbols"):
            _fut_symbol_cache[base_up] = cand
            return cand
    except Exception as e:
        log.warning(f"Futures symbol check fallo {base}: {e}")
    _fut_symbol_cache[base_up] = None
    return None

def fetch_funding_rate_history_usdt(symbol_fut: str, limit=72) -> pd.DataFrame:
    """
    Historial de funding rate (USDT-M). Devuelve DataFrame indexado por fundingTime, columna 'fundingRate' float.
    """
    try:
        r = http_get(FAPI_FUNDING_RATE_URLS, params={"symbol": symbol_fut, "limit": int(limit)})
        arr = r.json()
        if not arr:
            return pd.DataFrame()
        df = pd.DataFrame(arr)
        df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df = df.set_index("fundingTime").sort_index()
        return df[["fundingRate"]]
    except Exception as e:
        log.warning(f"FundingRate fallo {symbol_fut}: {e}")
        return pd.DataFrame()

def fetch_open_interest_history_usdt(symbol_fut: str, period='1h', limit=30) -> pd.Series:
    """
    Historial de Open Interest (USDT-M). Devuelve Series indexada por timestamp (UTC), valores float (sumOpenInterest).
    """
    try:
        r = http_get(FAPI_OI_HIST_URLS, params={"symbol": symbol_fut, "period": period, "limit": int(limit)})
        arr = r.json()
        if not arr:
            return pd.Series(dtype=float)
        df = pd.DataFrame(arr)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
        s = df.set_index("timestamp")["sumOpenInterest"].sort_index()
        return s
    except Exception as e:
        log.warning(f"OpenInterest fallo {symbol_fut}: {e}")
        return pd.Series(dtype=float)

# ------------------- Fallback global + GHI + Buckets + Utils ----------------------
def _score_of(it):
    if it.get("ScoreAdj") is not None:
        return it.get("ScoreAdj")
    w = it.get("WeightedNetScore")
    return w if w is not None else it.get("NetScore")

def build_global_fallback(resultados, n_bins=10):
    rows = []
    for it in resultados:
        sc = _score_of(it)
        if sc is None:
            continue
        rows.append({
            "Score": sc,
            "ProbUp_6h": it.get("ProbUp_6h"),
            "ExpRet_6h": it.get("ExpRet_6h"),
            "ProbUp_12h": it.get("ProbUp_12h"),
            "ExpRet_12h": it.get("ExpRet_12h"),
        })
    if not rows:
        return None

    df = pd.DataFrame(rows)
    score_all = df["Score"].dropna().values
    if score_all.size == 0:
        return None
    if len(score_all) < 10:
        qs = np.linspace(0, 1, max(3, len(score_all)) + 1)
    else:
        qs = np.linspace(0, 1, n_bins + 1)
    bins = np.quantile(score_all, qs)
    n_bins = len(bins) - 1

    def agg(prob_col, ret_col):
        use = df[prob_col].notna() | df[ret_col].notna()
        if not use.any():
            return [None]*n_bins, [None]*n_bins
        dfx = df.loc[use].copy()
        dfx["bin"] = np.digitize(dfx["Score"], bins[1:-1], right=True)
        prob_list, ret_list = [], []
        for b in range(n_bins):
            seg = dfx[dfx["bin"] == b]
            if len(seg) == 0:
                prob_list.append(None); ret_list.append(None)
            else:
                p = seg[prob_col].mean(skipna=True) if prob_col in seg.columns else None
                r = seg[ret_col].median(skipna=True) if ret_col in seg.columns else None
                prob_list.append(None if (p is None or pd.isna(p)) else float(p))
                ret_list.append(None if (r is None or pd.isna(r)) else float(r))
        return prob_list, ret_list

    prob6, ret6 = agg("ProbUp_6h", "ExpRet_6h")
    prob12, ret12 = agg("ProbUp_12h", "ExpRet_12h")

    return {"bins": bins.tolist(), "n_bins": n_bins,
            "prob_up_6h": prob6, "ret_med_6h": ret6,
            "prob_up_12h": prob12, "ret_med_12h": ret12}

def apply_global_fallback(resultados, fb):
    if not fb: return
    bins = np.array(fb["bins"]); n_bins = fb["n_bins"]
    for it in resultados:
        sc = _score_of(it)
        if sc is None:
            continue
        b = int(np.digitize(sc, bins[1:-1], right=True))
        b = max(0, min(n_bins - 1, b))
        # 6h
        if it.get("ProbUp_6h") is None or it.get("ExpRet_6h") is None:
            p = fb["prob_up_6h"][b]; r = fb["ret_med_6h"][b]
            if it.get("ProbUp_6h") is None and p is not None:
                it["ProbUp_6h"] = float(p)
            if it.get("ExpRet_6h") is None and r is not None:
                it["ExpRet_6h"] = float(r)
            if it.get("Target_6h") is None and it.get("ExpRet_6h") is not None and it.get("last_price") is not None:
                it["Target_6h"] = float(it["last_price"] * (1.0 + it["ExpRet_6h"]))
        # 12h
        if it.get("ProbUp_12h") is None or it.get("ExpRet_12h") is None:
            p = fb["prob_up_12h"][b]; r = fb["ret_med_12h"][b]
            if it.get("ProbUp_12h") is None and p is not None:
                it["ProbUp_12h"] = float(p)
            if it.get("ExpRet_12h") is None and r is not None:
                it["ExpRet_12h"] = float(r)
            if it.get("Target_12h") is None and it.get("ExpRet_12h") is not None and it.get("last_price") is not None:
                it["Target_12h"] = float(it["last_price"] * (1.0 + it["ExpRet_12h"]))

def _vec(resultados, col, fn=None):
    return np.array([np.nan if (fn(it) if fn else it.get(col)) is None else (fn(it) if fn else it.get(col)) for it in resultados], dtype=float)

def _zscore(v):
    v = np.asarray(v, dtype=float)
    if v.size == 0 or np.all(np.isnan(v)):
        return np.zeros_like(v)
    with np.errstate(invalid="ignore"):
        m = np.nanmean(v)
    with np.errstate(invalid="ignore"):
        s = np.nanstd(v, ddof=0)
    if not np.isfinite(s) or s == 0:
        return np.zeros_like(v)
    z = (v - m) / s
    z = np.where(np.isfinite(z), z, 0.0)
    return z

def attach_GHI(resultados):
    score_vec = _vec(resultados, "ScoreAdj", fn=lambda it: _score_of(it))
    cols = {
        "Score":     score_vec,
        "ProbUp_6h": _vec(resultados, "ProbUp_6h"),
        "ExpRet_6h": _vec(resultados, "ExpRet_6h"),
        "ProbUp_12h":_vec(resultados, "ProbUp_12h"),
        "ExpRet_12h":_vec(resultados, "ExpRet_12h"),
        "pct_24h":   _vec(resultados, "pct_24h"),
        "pct_48h":   _vec(resultados, "pct_48h"),
    }
    weights = {"Score":4.0, "ProbUp_6h":3.0, "ExpRet_6h":2.5, "ProbUp_12h":1.5, "ExpRet_12h":1.0, "pct_24h":0.8, "pct_48h":0.5}
    Z = {k: _zscore(v) for k, v in cols.items()}
    n = len(resultados); GHI = np.zeros(n, dtype=float)
    for k, w in weights.items():
        GHI += w * np.nan_to_num(Z[k], nan=0.0)
    for i, it in enumerate(resultados):
        it["GHI"] = float(GHI[i])

def assign_buckets_relaxed(resultados):
    ghi_all = np.array([it.get("GHI") for it in resultados], dtype=float)
    ghi_all = ghi_all[np.isfinite(ghi_all)]
    if len(ghi_all) == 0:
        p80 = p60 = 0.0
    else:
        p80 = float(np.nanpercentile(ghi_all, 80))
        p60 = float(np.nanpercentile(ghi_all, 60))
    for it in resultados:
        ghi = it.get("GHI"); p24 = it.get("pct_24h")
        p6  = it.get("ProbUp_6h"); er6 = it.get("ExpRet_6h")
        bucket = "Other"
        if (ghi is not None and ghi >= p80) and (p24 is None or p24 <= 10.0) and ((p6 is not None and p6 >= 0.52) or (er6 is not None and er6 >= 0.003) or p6 is None):
            bucket = "Next-Hours Long"
        elif (ghi is not None and ghi >= p60) and (p24 is not None and p24 <= -1.5):
            bucket = "Pullback Buy"
        elif (p24 is not None and p24 >= 8.0) and (ghi is None or ghi < p60 or (er6 is not None and er6 <= 0)):
            bucket = "Exhausted"
        it["Bucket"] = bucket

def print_compact_views(resultados, top_long=15, top_pull=10, top_exh=10):
    def key_rows(rows):
        return sorted(rows, key=lambda x: (x.get("GHI", -1e18), x.get("quoteVolume24h", 0.0)), reverse=True)
    longs = [r for r in resultados if r.get("Bucket") == "Next-Hours Long"]
    pulls = [r for r in resultados if r.get("Bucket") == "Pullback Buy"]
    exhs  = [r for r in resultados if r.get("Bucket") == "Exhausted"]

    for name, rows, topn in [
        ("NEXT-HOURS LONG (momentum inmediato)", key_rows(longs), top_long),
        ("PULLBACK BUYS (setup)",                 key_rows(pulls), top_pull),
        ("EXHAUSTED (riesgo de agotamiento)",     key_rows(exhs),  top_exh),
    ]:
        if not rows:
            print(f"\n[ {name} ] — (sin candidatos con criterios actuales)")
            continue
        rows = rows[:topn]
        print(f"\n[ {name} ]  Top {len(rows)}")
        print(f"{'Par':16} {'Precio':>11} {'%24h':>7} {'GHI':>8} {'P6h':>7} {'E6h%':>7} {'Target6h':>12} {'Liq24h':>12} {'Trend_4H':>10} {'Sent':>6} {'FR':>7} {'OI':>10}")
        for r in rows:
            par = f"{r['base']} / {r['quote']}"
            lp  = "-" if r.get('last_price') is None else f"{r['last_price']:.8f}"
            p24 = "-" if r.get('pct_24h')   is None else f"{r['pct_24h']:.2f}%"
            g   = "-" if r.get('GHI')       is None else f"{r['GHI']:.3f}"
            p6  = "-" if r.get('ProbUp_6h') is None else f"{100*r['ProbUp_6h']:.1f}%"
            e6  = "-" if r.get('ExpRet_6h') is None else f"{100*r['ExpRet_6h']:.2f}%"
            t6  = "-" if r.get('Target_6h') is None else f"{r['Target_6h']:.8f}"
            qv  = f"{r.get('quoteVolume24h', 0):,.0f}"
            tr  = r.get("Trend_4H") or "-"
            ss  = "-" if r.get("SentimentScore") is None else f"{r['SentimentScore']:+.2f}"
            fr  = "-" if r.get("fundingRate")    is None else f"{100*r['fundingRate']:.3f}%"
            oi  = "-" if r.get("openInterest")   is None else f"{r['openInterest']:,.0f}"
            print(f"{par:16} {lp:>11} {p24:>7} {g:>8} {p6:>7} {e6:>7} {t6:>12} {qv:>12} {tr:>10} {ss:>6} {fr:>7} {oi:>10}")

# ---------------------- Sanitizado + Export robusto -------------------------------
def _is_scalar(v):
    return v is None or isinstance(v, (str, bool, int, float, np.number, np.bool_))

def sanitize_rows(rows):
    clean = []
    for r in rows:
        c = {}
        for k, v in r.items():
            if k == "__models__":
                continue
            if _is_scalar(v):
                c[k] = v
            else:
                try: c[k] = json.dumps(v, ensure_ascii=False)
                except Exception: c[k] = str(v)
        clean.append(c)
    return clean

def _candidate_dirs():
    return [Path.cwd() / "exports",
            Path.home() / "Documents" / "usdt_exports",
            Path(tempfile.gettempdir()) / "usdt_exports"]

def _timestamped_path(original_path: str, new_ext: str | None = None, subdir: Path | None = None) -> Path:
    p = Path(original_path)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = p.stem; ext = (new_ext if new_ext else p.suffix).lstrip(".")
    fname = f"{stem}_{ts}.{ext}" if ext else f"{stem}_{ts}"
    return (subdir / fname) if subdir else p.with_name(fname)

def _safe_write_csv(df, path: str | Path) -> Path | None:
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        print(f"CSV exportado -> {path}")
        return path
    except PermissionError as e:
        print(f"Permiso denegado al escribir CSV en {path}: {e}")
        for base in _candidate_dirs():
            try:
                base.mkdir(parents=True, exist_ok=True)
                alt = _timestamped_path(path, new_ext="csv", subdir=base)
                df.to_csv(alt, index=False); print(f"CSV exportado (fallback) -> {alt}")
                return alt
            except Exception as ex:
                print(f"Fallback CSV falló en {base}: {ex}")
        return None

def _safe_write_parquet_or_gzip(df, path: str | Path) -> Path | None:
    path = Path(path)
    if PARQUET_ENGINE:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, index=False, engine=PARQUET_ENGINE)
            print(f"Parquet exportado -> {path} (engine={PARQUET_ENGINE})")
            return path
        except Exception as e:
            print(f"No se pudo exportar Parquet en {path}: {e}")
    for base in _candidate_dirs():
        try:
            base.mkdir(parents=True, exist_ok=True)
            alt = _timestamped_path(path, new_ext="csv.gz", subdir=base)
            df.to_csv(alt, index=False, compression="gzip")
            print(f"Fallback Parquet -> exportado como CSV comprimido: {alt}")
            return alt
        except Exception as ex:
            print(f"Fallback .csv.gz falló en {base}: {ex}")
    return None

def export_results(df_out, csv_path, parquet_path):
    if csv_path:
        _safe_write_csv(df_out, csv_path)
    if parquet_path:
        _safe_write_parquet_or_gzip(df_out, parquet_path)

def export_models(models_by_symbol, path):
    if not path:
        return
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "horizons": BACKTEST_FORWARD_HOURS_SET, "symbols": models_by_symbol}
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Modelos exportados -> {p}")
    except Exception as e:
        print(f"No se pudo exportar modelos JSON: {e}")

# ---------------------------------- Motor -----------------------------------------
def analizar_usdt_mercado():
    connect_to_binance()

    # 1) símbolos y tickers
    simbolos = obtener_simbolos_usdt_trading()
    if not simbolos:
        return [], {}
    tickers_24h = obtener_ticker_24h_todos()

    # 2) base resultados
    resultados = []
    for meta in simbolos:
        sym = meta["symbol"]; t = tickers_24h.get(sym)
        if not t: continue
        last_price = t["last_price"]; pct_24h = t["pct_24h"]; qv = t["quoteVolume"]
        denom = (1.0 + (pct_24h / 100.0)); price_24h_ago = last_price / denom if denom != 0 else None
        resultados.append({
            "symbol": sym, "base": meta["base"], "quote": meta["quote"],
            "last_price": last_price, "price_24h_ago": price_24h_ago, "pct_24h": pct_24h,
            "quoteVolume24h": qv,
            "price_48h_ago": None, "pct_48h": None,
            "NetScore": None, "WeightedNetScore": None,
            "ProbUp_6h": None, "ExpRet_6h": None, "Target_6h": None,
            "ProbUp_12h": None, "ExpRet_12h": None, "Target_12h": None,
            "Trend_4H": None, "SentimentScore": None, "SentimentN": 0,
            "ScoreAdj": None,
            # Derivados
            "fundingRate": None, "openInterest": None, "oi_prev": None, "oi_change": None
        })

    # 3) top por liquidez y candidatos de noticias
    top_liq = sorted(resultados, key=lambda r: r["quoteVolume24h"], reverse=True)[:BACKTEST_TOP_LIQ_SYMBOLS]
    top_syms = set(r["symbol"] for r in top_liq)
    news_candidates = sorted(resultados, key=lambda r: (r["quoteVolume24h"]), reverse=True)[:NEWS_MAX_SYMBOLS]

    # 4) tareas
    def tarea(item):
        now_ms = int(time.time() * 1000)
        # 48h interpolado
        p48 = precio_interpolado(item["symbol"], now_ms - 48 * 3600_000, interval_ms=3600_000)
        item["price_48h_ago"] = p48
        if p48 and p48 > 0:
            item["pct_48h"] = ((item["last_price"] - p48) / p48) * 100.0

        # Low TF: 1h
        df_1h = fetch_data(item["symbol"], MTFA_INTERVAL_LOW, lookback_hours=120)
        df_1h = calculate_technical_indicators(df_1h)

        # High TF: 4h (MTFA)
        df_4h = None
        try:
            df_4h = fetch_data(item["symbol"], MTFA_INTERVAL_HIGH, lookback_hours=240)
            df_4h = calculate_technical_indicators(df_4h)
            if is_df_ok(df_4h) and "EMA_50" in df_4h.columns and "Close" in df_4h.columns:
                c = df_4h.iloc[-1]
                ema = float(c["EMA_50"]); close = float(c["Close"])
                slope = float(df_4h["EMA_50"].diff().tail(5).mean()) if len(df_4h) >= 5 else 0.0
                if close > ema and slope > 0:
                    item["Trend_4H"] = "Alcista"
                elif close < ema and slope < 0:
                    item["Trend_4H"] = "Bajista"
                else:
                    item["Trend_4H"] = "Lateral"
            else:
                item["Trend_4H"] = "Lateral"
        except Exception as e:
            log.warning(f"4H error {item['symbol']}: {e}")
            item["Trend_4H"] = "Lateral"

        # Señales actuales
        if not is_df_ok(df_1h):
            return item
        S = señales_vector(df_1h, df_4h)
        if S.empty:
            return item
        S_last = S.iloc[-1]

        # NetScore base (técnico + patrones)
        base_feats = ["rsi_sig","macd_cross","macd_hist_slope","ema_bull","above_ema50","vol_spike","ctx_up"]
        w = np.array([2.0, 3.0, 1.0, 2.0, 1.0, 2.0, 1.0])
        if PTA:
            for name in ["CDL_ENGULFING","CDL_HAMMER","CDL_MORNINGSTAR","CDL_SHOOTINGSTAR","CDL_DOJI"]:
                if name in S_last.index:
                    base_feats.append(name); w = np.append(w, [1.0])
        x = S_last.reindex(base_feats).values.astype(float)
        NetScore = float(np.dot(x, w))

        # -------- Funding Rate & Open Interest (Futuros USDT-M) ----------
        fut_symbol = futures_symbol_for_base(item["base"])
        fund_score = 0.0
        if fut_symbol:
            try:
                fr_df = fetch_funding_rate_history_usdt(fut_symbol, limit=72)  # ~24h (8h por funding)
                if is_df_ok(fr_df):
                    fr_last = float(fr_df["fundingRate"].iloc[-1])
                    item["fundingRate"] = fr_last
                    if fr_last < FUNDING_BULL_THRESH:
                        fund_score += FUNDING_BULL_POINTS
                    elif fr_last > FUNDING_BEAR_THRESH:
                        fund_score += FUNDING_BEAR_POINTS
            except Exception as e:
                log.warning(f"Funding calc error {fut_symbol}: {e}")

            try:
                oi_s = fetch_open_interest_history_usdt(fut_symbol, period='1h', limit=30)
                if isinstance(oi_s, pd.Series) and not oi_s.empty:
                    item["openInterest"] = float(oi_s.iloc[-1])
                    if len(oi_s) >= 2:
                        item["oi_prev"] = float(oi_s.iloc[-2])
                        item["oi_change"] = float(item["openInterest"] - item["oi_prev"])
                        oi_increasing  = item["openInterest"] > item["oi_prev"]
                        price_increasing = False
                        if len(df_1h) >= 2:
                            price_increasing = float(df_1h["Close"].iloc[-1]) > float(df_1h["Close"].iloc[-2])

                        if oi_increasing and price_increasing:
                            fund_score += OI_TREND_CONFIRM_UP
                        elif oi_increasing and not price_increasing:
                            fund_score += OI_TREND_CONFIRM_DOWN
                        else:
                            fund_score += OI_WEAKEN_POINTS
            except Exception as e:
                log.warning(f"OpenInterest calc error {fut_symbol}: {e}")

        # sumar al NetScore técnico
        NetScore += fund_score
        item["NetScore"] = round(NetScore, 2)

        # Backtesting (solo top liquidez)
        if BACKTEST_ENABLE and item["symbol"] in top_syms:
            df_1h_bt = fetch_data(item["symbol"], "1h", lookback_hours=BACKTEST_LOOKBACK_HOURS)
            df_1h_bt = calculate_technical_indicators(df_1h_bt)
            df_4h_bt = None
            try:
                df_4h_bt = fetch_data(item["symbol"], "4h", lookback_hours=max(240, BACKTEST_LOOKBACK_HOURS))
                df_4h_bt = calculate_technical_indicators(df_4h_bt)
            except Exception as e:
                log.warning(f"Backtest 4h error {item['symbol']}: {e}")

            model_map = backtest_calibrar(df_1h_bt, df_4h_bt, BACKTEST_FORWARD_HOURS_SET)

            WNS_list = []
            for H in BACKTEST_FORWARD_HOURS_SET:
                model = model_map.get(H)
                if not model: continue
                score, b, prob, rexp = score_actual_y_proyeccion(S_last, model)
                item[f"ProbUp_{H}h"] = None if (prob is None or np.isnan(prob)) else float(prob)
                item[f"ExpRet_{H}h"] = None if (rexp is None or np.isnan(rexp)) else float(rexp)
                if item[f"ExpRet_{H}h"] is not None and item["last_price"] is not None:
                    item[f"Target_{H}h"] = float(item["last_price"] * (1.0 + item[f"ExpRet_{H}h"]))
                feat_cols = model["feat_cols"]; wv = np.array(model["weights"]); b0 = model["bias"]
                xh = S_last.reindex(feat_cols).values.astype(float)
                WNS_list.append(float(np.dot(xh, wv) + b0))
            if WNS_list:
                item["WeightedNetScore"] = float(np.mean(WNS_list))
            item["__models__"] = model_map

        # Trend boost sobre TechnicalScore (usa Weighted si existe, si no NetScore)
        technical = item.get("WeightedNetScore") if item.get("WeightedNetScore") is not None else item.get("NetScore")
        if technical is not None:
            trend = item.get("Trend_4H")
            boosted = technical
            if trend == "Alcista":
                boosted = technical * (TREND_BOOST_UP_POS if technical > 0 else TREND_BOOST_UP_NEG)
            elif trend == "Bajista":
                boosted = technical * (TREND_BOOST_DOWN_POS if technical > 0 else TREND_BOOST_DOWN_NEG)
            else:
                boosted = technical * TREND_LATERAL_FACTOR
            item["ScoreTrend"] = boosted
        else:
            item["ScoreTrend"] = None

        # Sentimiento de noticias (limitado a top liquidez)
        if ENABLE_NEWS_SENTIMENT and item in news_candidates and VADER is not None:
            try:
                sent, n = fetch_news_sentiment(item["base"])
                item["SentimentScore"] = sent
                item["SentimentN"] = n
            except Exception as e:
                log.warning(f"Sentiment error {item['symbol']}: {e}")

        # Score ajustado (Trend + Sentiment)
        score_for_adj = item.get("ScoreTrend")
        ss = item.get("SentimentScore")
        if score_for_adj is not None:
            if ss is not None:
                item["ScoreAdj"] = float(score_for_adj * (1.0 + SENTIMENT_ALPHA * ss))
            else:
                item["ScoreAdj"] = float(score_for_adj)

        return item

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(tarea, it) for it in resultados]
        for _ in as_completed(futures):
            pass

    # 5) recolectar modelos
    models_by_symbol = {}
    for it in resultados:
        md = it.pop("__models__", None)
        if md:
            models_by_symbol[it["symbol"]] = md

    # === Fallback global + GHI + Buckets ===
    fb = build_global_fallback(resultados, n_bins=10)
    apply_global_fallback(resultados, fb)
    attach_GHI(resultados)
    assign_buckets_relaxed(resultados)

    # 6) ordenar
    def sort_key(x):
        primary = x.get(ORDER_BY)
        secondary = x.get("quoteVolume24h", 0.0)
        return (primary is None, primary, secondary)
    resultados.sort(key=sort_key, reverse=DESC)

    return resultados, models_by_symbol

# -------------------------------------- CLI --------------------------------------
if __name__ == "__main__":
    try:
        filas, models_map = analizar_usdt_mercado()
        if not filas:
            print("No se encontraron pares USDT en TRADING.")
        else:
            header = (
                f"{'Par':16} {'Precio':>11} {'%24h':>7} {'%48h':>7} "
                f"{'Liq(24h)':>12} {'Net':>7} {'WNet':>8} {'ScoreAdj':>9} {'GHI':>8} "
                f"{'P(up)6h':>8} {'E[6h]%':>8} {'Target6h':>12} "
                f"{'P(up)12h':>9} {'E[12h]%':>9} {'Target12h':>12} "
                f"{'Trend_4H':>10} {'Sent':>6} {'FR':>7} {'OI':>12} {'Bucket':>14}"
            )
            print(f"\n--- USDT (ordenado por {ORDER_BY} desc; tie por liquidez) ---")
            print(header)
            print("-" * len(header))
            for r in filas:
                par   = f"{r['base']} / {r['quote']}"
                lp    = "-" if r.get('last_price') is None else f"{r['last_price']:.8f}"
                p24   = "-" if r.get('pct_24h')   is None else f"{r['pct_24h']:.2f}%"
                p48   = "-" if r.get('pct_48h')   is None else f"{r['pct_48h']:.2f}%"
                liq   = f"{r.get('quoteVolume24h', 0):.0f}"
                net   = "-" if r.get('NetScore')          is None else f"{r['NetScore']:.2f}"
                wnet  = "-" if r.get('WeightedNetScore')  is None else f"{r['WeightedNetScore']:.4f}"
                sadj  = "-" if r.get('ScoreAdj')          is None else f"{r['ScoreAdj']:.3f}"
                ghi   = "-" if r.get('GHI')               is None else f"{r['GHI']:.3f}"

                def fmt_prob(v):  return "-" if v is None else f"{100*v:,.1f}%"
                def fmt_rexp(v):  return "-" if v is None else f"{100*v:,.2f}%"
                def fmt_price(v): return "-" if v is None else f"{v:.8f}"

                trend  = r.get("Trend_4H") or "-"
                sent   = "-" if r.get("SentimentScore") is None else f"{r['SentimentScore']:+.2f}"
                fr     = "-" if r.get("fundingRate")    is None else f"{100*r['fundingRate']:.3f}%"
                oi     = "-" if r.get("openInterest")   is None else f"{r['openInterest']:,.0f}"
                bucket = r.get("Bucket") or "-"

                print(
                    f"{par:16} {lp:>11} {p24:>7} {p48:>7} "
                    f"{liq:>12} {net:>7} {wnet:>8} {sadj:>9} {ghi:>8} "
                    f"{fmt_prob(r.get('ProbUp_6h')):>8} {fmt_rexp(r.get('ExpRet_6h')):>8} {fmt_price(r.get('Target_6h')):>12} "
                    f"{fmt_prob(r.get('ProbUp_12h')):>9} {fmt_rexp(r.get('ExpRet_12h')):>9} {fmt_price(r.get('Target_12h')):>12} "
                    f"{trend:>10} {sent:>6} {fr:>7} {oi:>12} {bucket:>14}"
                )

            print(f"\nTotal de pares USDT: {len(filas)}")

            # Vistas compactas por buckets (con FR y OI)
            print_compact_views(filas, top_long=15, top_pull=10, top_exh=10)

            # Export
            df_out = pd.DataFrame(sanitize_rows(filas))
            export_results(df_out, EXPORT_CSV_PATH, EXPORT_PARQUET_PATH)

            if models_map:
                export_models(models_map, EXPORT_MODELS_JSON_PATH)
            else:
                print("No hay modelos calibrados para exportar (liquidez insuficiente o backtesting desactivado).")

    except Exception as e:
        log.exception(f"Error durante el análisis: {e}")
        print(f"Error durante el análisis: {e}")
