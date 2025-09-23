# engines/binance_usdt_2.py
"""
Motor: Binance_usdt_2

- Toma el ticker 24h de Binance.
- Filtra pares que terminan en USDT y están en estado TRADING.
- Ordena el resultado por priceChangePercent (descendente).
- Limita por LIMIT_SYMBOLS.
- Devuelve un dict con: rows, count, elapsed_sec, etc.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

import requests


BINANCE_API = "https://api.binance.com"


def _get_trading_usdt_symbols() -> List[str]:
    """Lista de símbolos USDT con estado TRADING."""
    info = requests.get(f"{BINANCE_API}/api/v3/exchangeInfo", timeout=30).json()
    syms = []
    for s in info.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            syms.append(s["symbol"])
    return syms


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.time()
    limit = int(params.get("LIMIT_SYMBOLS", 150))

    # 1) Símbolos USDT/TRADING
    trading = set(_get_trading_usdt_symbols())

    # 2) 24h tickers
    tickers = requests.get(f"{BINANCE_API}/api/v3/ticker/24hr", timeout=60).json()

    # 3) Filtrar a sólo USDT TRADING y mapear campos
    rows: List[Dict[str, Any]] = []
    for t in tickers:
        sym = t.get("symbol", "")
        if sym not in trading:
            continue

        # Mapeo de campos esperados
        rows.append({
            "symbol": sym,
            "lastPrice": t.get("lastPrice"),
            "priceChangePercent": t.get("priceChangePercent"),
            "highPrice": t.get("highPrice"),
            "lowPrice": t.get("lowPrice"),
            "count": t.get("count"),
            "quoteVolume": t.get("quoteVolume"),
            "weightedAvgPrice": t.get("weightedAvgPrice"),
        })

    # 4) ORDENAR por priceChangePercent (DESC)  ← **SOLICITADO**
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return float("-inf")

    rows.sort(key=lambda r: _to_float(r.get("priceChangePercent", 0.0)), reverse=True)

    # 5) Limitar filas
    if limit and limit > 0:
        rows = rows[:limit]

    elapsed = round(time.time() - t0, 3)
    return {
        "rows": rows,
        "count": len(rows),
        "elapsed_sec": elapsed,
    }
