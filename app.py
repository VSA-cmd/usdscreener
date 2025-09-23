# app.py
from __future__ import annotations

import csv
import io
import json
import secrets
import threading
from datetime import datetime, timezone
from textwrap import dedent
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse, StreamingResponse, JSONResponse
from python_decouple import config

# ====== Config ======
ENGINE_NAME = config("ENGINE", default="Binance_usdt_2")
LIMIT_SYMBOLS = config("LIMIT_SYMBOLS", cast=int, default=150)
MAX_WORKERS = config("MAX_WORKERS", cast=int, default=12)
BUDGET = config("BUDGET", cast=int, default=110)

# ====== Motor (engines) ======
# Por ahora sólo registramos el motor Binance_usdt_2
from engines.binance_usdt_2 import run as binance_usdt_2_run  # noqa: E402

ENGINES = {
    "Binance_usdt_2": binance_usdt_2_run,
}

# ====== App / Estado ======
app = FastAPI(title="USDT Screener")

_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _new_job_id() -> str:
    return secrets.token_hex(5)  # p.ej. '65bf164352'


def _start_job(job_id: str, engine: str, params: Dict[str, Any]) -> None:
    """Lanza el motor en un hilo para no bloquear el servidor."""
    def _worker():
        try:
            func = ENGINES.get(engine)
            if not func:
                with _jobs_lock:
                    _jobs[job_id]["status"] = "error"
                    _jobs[job_id]["ended"] = _now_utc_str()
                    _jobs[job_id]["error"] = f"No encontré una función de entrada usable en el motor. Probé: {list(ENGINES.keys())}"
                return

            result = func(params)  # debe devolver dict con keys: rows, count, etc.
            # Asegurar consistencia mínima:
            rows = result.get("rows", [])
            count = result.get("count", len(rows))

            with _jobs_lock:
                _jobs[job_id].update({
                    "status": "done",
                    "rows": rows,
                    "count": count,
                    "ended": _now_utc_str(),
                    "_engine": engine,
                    "params": params,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as exc:  # pragma: no cover
            with _jobs_lock:
                _jobs[job_id]["status"] = "error"
                _jobs[job_id]["ended"] = _now_utc_str()
                _jobs[job_id]["error"] = f"{type(exc).__name__}: {exc}"

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


@app.get("/", response_class=RedirectResponse)
def root():
    """Crea un job y redirige a /view?job=..."""
    job_id = _new_job_id()
    with _jobs_lock:
        _jobs[job_id] = {
            "job": job_id,
            "status": "running",
            "started": _now_utc_str(),
            "rows": [],
            "count": 0,
        }
    params = {
        "LIMIT_SYMBOLS": LIMIT_SYMBOLS,
        "MAX_WORKERS": MAX_WORKERS,
        "BUDGET": BUDGET,
    }
    _start_job(job_id, ENGINE_NAME, params)
    return f"/view?job={job_id}"


@app.get("/api", response_class=JSONResponse)
def api(job: str):
    with _jobs_lock:
        data = _jobs.get(job)
        if not data:
            raise HTTPException(404, "Job no encontrado")
        return data


@app.get("/csv", response_class=StreamingResponse)
def csv_endpoint(job: str):
    with _jobs_lock:
        data = _jobs.get(job)
        if not data:
            raise HTTPException(404, "Job no encontrado")
        if data["status"] == "running":
            # 202 Accepted mientras el job corre
            return Response("Aún generando…", status_code=202, media_type="text/plain")

        if data["status"] == "error":
            return Response(f"error: {data.get('error','')}", status_code=400, media_type="text/plain")

        rows = list(data.get("rows", []))
    # Seguridad: reordenamos aquí por si el motor futuro no lo hiciera.
    def _to_float(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return float("nan")

    rows.sort(key=lambda r: _to_float(r.get("priceChangePercent", 0.0)), reverse=True)

    # CSV
    if not rows:
        header = ["symbol", "lastPrice", "priceChangePercent", "highPrice", "lowPrice",
                  "count", "quoteVolume", "weightedAvgPrice"]
    else:
        header = list(rows[0].keys())

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    filename = f"usdt_screener_{job}.csv"
    return StreamingResponse(
        iter([buf.read()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.get("/view", response_class=HTMLResponse)
def view(job: str):
    # HTML + JS. OJO con las llaves: se escapan como {{ }} para que el f-string no las interprete.
    html = dedent(f"""
    <!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Job {job}</title>
      <style>
        :root {{
          --fg:#222; --muted:#666; --ok:#0a7; --err:#c33;
          --row:#fafafa; --row2:#f3f3f3; --bd:#e5e5e5;
        }}
        body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; color:var(--fg); margin:18px; }}
        .badge {{ font-size:12px; padding:2px 6px; border-radius:10px; border:1px solid var(--bd); }}
        .running {{ background:#fff7e6; border-color:#ffd28c; }}
        .done {{ background:#e7fbf3; border-color:#b3ecd8; }}
        .error {{ background:#fdecec; border-color:#f5bcbc; }}
        table {{ border-collapse: collapse; width: 100%; margin-top:12px; }}
        th, td {{ border:1px solid var(--bd); padding:6px 8px; font-size: 13px; }}
        th {{ background:#f8f8f8; position: sticky; top:0; z-index:1; }}
        tr:nth-child(odd) td {{ background: var(--row); }}
        tr:nth-child(even) td {{ background: var(--row2); }}
        .muted {{ color:var(--muted); }}
        .controls {{ margin: 12px 0; display:flex; gap:10px; align-items: center; flex-wrap: wrap; }}
        .hint {{ font-size:12px; color:var(--muted); }}
        .right {{ text-align:right; }}
      </style>
    </head>
    <body>
      <h1>Job {job}</h1>
      <div id="meta" class="muted">Cargando…</div>

      <div class="controls">
        <a id="jsonLink" class="badge" href="/api?job={job}">Ver JSON</a>
        <a id="csvLink"  class="badge" href="/csv?job={job}">Descargar CSV</a>
        <span class="hint">Orden: <b>priceChangePercent</b> de mayor a menor.</span>
      </div>

      <div id="status"></div>
      <div id="error" class="error" style="display:none; padding:8px; border-radius:8px;"></div>

      <div id="tableWrap"></div>

      <script>
        const job = "{job}";

        const fmt = (s) => s ? new Date(s.replace(" UTC","Z")).toLocaleString() : "—";

        async function refresh() {{
          const res = await fetch(`/api?job=${{job}}`, {{ cache: "no-store" }});
          const data = await res.json();

          const meta = document.getElementById("meta");
          meta.textContent = `Estado: ${{data.status}} · inicio: ${{fmt(data.started)}} · fin: ${{fmt(data.ended || '')}} · Filas: ${{data.count||0}}`;

          const errBox = document.getElementById("error");
          if (data.status === "error") {{
            errBox.style.display = "block";
            errBox.textContent = data.error || "Error";
            return;
          }}

          if (data.status !== "done") {{
            // Mientras corre, evitamos renderizar una tabla vacía
            setTimeout(refresh, 1500);
            return;
          }}

          // 1) Ordenar por priceChangePercent (desc)
          const rows = (data.rows||[]).slice().sort((a,b) => {{
            const A = parseFloat(a.priceChangePercent || 0);
            const B = parseFloat(b.priceChangePercent || 0);
            if (Number.isNaN(A) && Number.isNaN(B)) return 0;
            if (Number.isNaN(A)) return 1;
            if (Number.isNaN(B)) return -1;
            return B - A;
          }});

          // 2) Render
          const headers = rows.length ? Object.keys(rows[0]) : ["symbol","lastPrice","priceChangePercent","highPrice","lowPrice","count","quoteVolume","weightedAvgPrice"];
          const wrap = document.getElementById("tableWrap");
          const tbl = document.createElement("table");
          const thead = document.createElement("thead");
          const trh = document.createElement("tr");
          headers.forEach(h => {{
            const th = document.createElement("th");
            th.textContent = h;
            trh.appendChild(th);
          }});
          thead.appendChild(trh);
          tbl.appendChild(thead);

          const tbody = document.createElement("tbody");
          rows.forEach(r => {{
            const tr = document.createElement("tr");
            headers.forEach(h => {{
              const td = document.createElement("td");
              td.textContent = (r[h] === null || r[h] === undefined) ? "" : r[h];
              if (["lastPrice","priceChangePercent","highPrice","lowPrice","count","quoteVolume","weightedAvgPrice"].includes(h)) {{
                td.classList.add("right");
              }}
              tr.appendChild(td);
            }});
            tbody.appendChild(tr);
          }});
          tbl.appendChild(tbody);

          wrap.innerHTML = "";
          wrap.appendChild(tbl);
        }}

        refresh();
      </script>
    </body>
    </html>
    """)
    return HTMLResponse(html)


# Para ejecutar local si hace falta: uvicorn app:app --reload
if __name__ == "__main__":  # pragma: no cover
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
