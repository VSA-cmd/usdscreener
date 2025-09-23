"""
Motor sencillo para generar /tmp/usdt_screener.csv con los pares *USDT
de Binance ordenados por liquidez (quoteVolume).

Requiere: requests, pandas (ya están en requirements.txt).
"""

import os
import sys
import time
from typing import List, Dict, Any
from pathlib import Path

import requests
import pandas as pd


BINANCE_BASE = "https://api.binance.com"
CSV_PATH = Path("/tmp/usdt_screener.csv")


def _get_json(url: str, params: Dict[str, Any] | None = None, timeout: int = 20) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_trading_usdt_symbols() -> List[str]:
    info = _get_json(f"{BINANCE_BASE}/api/v3/exchangeInfo")
    symbols = info.get("symbols", [])
    usdt = [s["symbol"] for s in symbols if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"]
    return usdt


def fetch_ticker_24h_all() -> List[Dict[str, Any]]:
    data = _get_json(f"{BINANCE_BASE}/api/v3/ticker/24hr")
    # Devuelve lista grande (todos los símbolos)
    return data if isinstance(data, list) else []


def rank_candidates(limit_symbols: int) -> pd.DataFrame:
    # 1) Símbolos USDT en estado TRADING
    usdt_symbols = fetch_trading_usdt_symbols()
    print(f"{time.strftime('%H:%M:%S')} TRADING USDT symbols: {len(usdt_symbols)}", flush=True)

    # 2) Tickers 24h (todos) y filtramos por USDT
    tickers = fetch_ticker_24h_all()
    print(f"{time.strftime('%H:%M:%S')} Ticker 24h objetos: {len(tickers)}", flush=True)

    rows: List[Dict[str, Any]] = []
    usdt_set = set(usdt_symbols)

    for t in tickers:
        sym = t.get("symbol")
        if sym not in usdt_set:
            continue
        try:
            quote_vol = float(t.get("quoteVolume", "0") or 0.0)
            last_price = float(t.get("lastPrice", "0") or 0.0)
            change_pct = float(t.get("priceChangePercent", "0") or 0.0)
            high = float(t.get("highPrice", "0") or 0.0)
            low = float(t.get("lowPrice", "0") or 0.0)
            count = int(t.get("count", 0) or 0)
            weighted = float(t.get("weightedAvgPrice", "0") or 0.0)
        except Exception:
            # Si viene algo raro, lo saltamos
            continue

        rows.append(
            {
                "symbol": sym,
                "lastPrice": last_price,
                "priceChangePercent": change_pct,
                "highPrice": high,
                "lowPrice": low,
                "count": count,
                "quoteVolume": quote_vol,
                "weightedAvgPrice": weighted,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # 3) Orden por liquidez (quoteVolume) desc
    df = df.sort_values(["quoteVolume", "count"], ascending=[False, False]).head(limit_symbols).reset_index(drop=True)
    print(f"{time.strftime('%H:%M:%S')} Candidatos tras cruce & rank por liquidez: {df.shape[0]}", flush=True)
    return df


def write_csv(df: pd.DataFrame) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_PATH, index=False)
    print("CSV guardado en /tmp/usdt_screener.csv", flush=True)


def run_screener(LIMIT_SYMBOLS: int = 150, MAX_WORKERS: int = 12, BUDGET: int = 110) -> None:
    """
    Punto de entrada invocable desde la app. MAX_WORKERS y BUDGET están
    por compatibilidad con logs; aquí no usamos concurrencia.
    """
    _ = MAX_WORKERS  # sin uso
    _ = BUDGET       # sin uso

    df = rank_candidates(LIMIT_SYMBOLS)
    if df.empty:
        # Escribimos CSV vacío para diagnóstico (la app luego marcará error si no hay filas).
        CSV_PATH.write_text("", encoding="utf-8")
        return
    write_csv(df)


def main(LIMIT_SYMBOLS: int = 150, MAX_WORKERS: int = 12, BUDGET: int = 110) -> None:
    # Permite ejecución como script directo
    run_screener(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET)


if __name__ == "__main__":
    # Parámetros opcionales desde entorno/argv
    lim = int(os.environ.get("LIMIT_SYMBOLS", "150"))
    mw = int(os.environ.get("MAX_WORKERS", "12"))
    bdg = int(os.environ.get("BUDGET", "110"))

    # Permite override rápido por argumentos: python Binance_usdt_2.py 200
    if len(sys.argv) >= 2:
        try:
            lim = int(sys.argv[1])
        except Exception:
            pass

    main(LIMIT_SYMBOLS=lim, MAX_WORKERS=mw, BUDGET=bdg)
