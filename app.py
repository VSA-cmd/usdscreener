# -*- coding: utf-8 -*-
import io
import os
import json
import uuid
import threading
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request, Response, send_file, render_template_string, redirect, url_for

# ---- importa el motor ligero para Render ----
import Binance_usdt_2 as engine  # asegúrate que el nombre del archivo coincide

# ===================== Flask app =====================
app = Flask(__name__)

# Memoria en proceso para jobs efímeros
JOBS = {}  # job_id -> dict(status, started, ended, rows, raw_rows, info)

# Parámetros de presentación
TOP_N = int(os.environ.get("TOP_N", "100"))

# ===================== Helpers =====================
def _now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _rows_from_df(df: pd.DataFrame, top_n: int) -> list[dict]:
    """Devuelve filas simples (tipos JSON-friendly)."""
    cols_pref = [
        "symbol", "price", "pct_change_24h", "quote_volume_24h",
        "Trend_4H", "ScoreTrend", "fundingRate", "openInterest",
        "oi_delta", "NetScore", "GHI"
    ]
    cols = [c for c in cols_pref if c in df.columns]
    slim = df[cols].head(top_n).copy()
    # convierte NaN -> None para JSON limpio
    out = []
    for _, r in slim.iterrows():
        d = {k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else v)) for k, v in r.to_dict().items()}
        out.append(d)
    return out


def _start_job(job_id: str):
    """Hilo que ejecuta el cálculo y guarda resultados en JOBS."""
    try:
        df, info = engine.build_df()  # respeta LIMIT_SYMBOLS/MAX_WORKERS/BUDGET desde ENV
        rows = _rows_from_df(df, TOP_N) if df is not None and not df.empty else []
        JOBS[job_id].update({
            "status": "done",
            "ended": _now_utc_str(),
            "rows": rows,
            "raw_rows": rows,   # mantenemos las mismas filas para CSV estable
            "info": info or {},
        })
    except Exception as e:
        JOBS[job_id].update({
            "status": "error",
            "ended": _now_utc_str(),
            "error": str(e),
        })


# ===================== Rutas =====================

@app.get("/")
def home():
    """Lanza un job y redirige a la vista con auto-refresh."""
    job_id = uuid.uuid4().hex[:10]
    JOBS[job_id] = {"status": "running", "started": _now_utc_str()}
    t = threading.Thread(target=_start_job, args=(job_id,), daemon=True)
    t.start()
    return redirect(url_for("view_job", job=job_id))


@app.get("/view")
def view_job():
    """HTML simple con auto-refresh; muestra tabla cuando termina."""
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return "Job not found", 404

    # Pequeña plantilla inline para evitar archivos estáticos
    tpl = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Job {{ job }}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 6px 8px; text-align: right; }
    th { background: #f5f5f5; }
    td:first-child, th:first-child { text-align: left; }
    .muted { color: #777; }
  </style>
</head>
<body>
  <h3>Job {{ job }}</h3>
  <p>
    Estado: <b>{{ j["status"] }}</b>
    {% if j.get("started") %} · inicio: {{ j["started"] }} {% endif %}
    {% if j.get("ended") %} · fin: {{ j["ended"] }} {% endif %}
    · <a id="jsonlink" href="{{ url_for('job_json', job=job) }}" target="_blank">Ver JSON</a>
    · <a id="csvlink" href="{{ url_for('job_csv', job=job) }}" target="_blank">Descargar CSV</a>
  </p>

  {% if j["status"] != "done" %}
    <p class="muted">Actualiza en unos segundos…</p>
    <script>
      setTimeout(function(){ location.reload(); }, 4000);
    </script>
  {% elif j.get("error") %}
    <pre>{{ j["error"] }}</pre>
  {% else %}
    <h4>Top {{ rows|length }}</h4>
    <table>
      <thead>
        <tr>
          <th>symbol</th>
          <th>price</th>
          <th>% 24h</th>
          <th>quote volume 24h</th>
          <th>Trend 4H</th>
          <th>ScoreTrend</th>
          <th>fundingRate</th>
          <th>openInterest</th>
          <th>oi_delta</th>
          <th>NetScore</th>
          <th>GHI</th>
        </tr>
      </thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td style="text-align:left">{{ r.get("symbol","") }}</td>
          <td>{{ "%.6g"|format(r.get("price") or 0) }}</td>
          <td>{{ "%.3f"|format(100*(r.get("pct_change_24h") or 0)) }}%</td>
          <td>{{ "{:,.0f}".format(r.get("quote_volume_24h") or 0) }}</td>
          <td>{{ r.get("Trend_4H") or "-" }}</td>
          <td>{{ "%.3f"|format(r.get("ScoreTrend") or 0) }}</td>
          <td>{{ ("%.6f"%r.get("fundingRate")).rstrip('0').rstrip('.') if r.get("fundingRate") is not none else "-" }}</td>
          <td>{{ "{:,.0f}".format(r.get("openInterest") or 0) if r.get("openInterest") is not none else "-" }}</td>
          <td>{{ "{:,.0f}".format(r.get("oi_delta") or 0) if r.get("oi_delta") is not none else "-" }}</td>
          <td>{{ "%.2f"|format(r.get("NetScore") or 0) }}</td>
          <td>{{ "%.3f"|format(r.get("GHI") or 0) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  {% endif %}
</body>
</html>
    """
    rows = j.get("rows") or []
    return render_template_string(tpl, job=job, j=j, rows=rows)


@app.get("/api")
def job_json():
    """Devuelve el resultado del job en JSON."""
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return Response(json.dumps({"status": "not_found"}), status=404, mimetype="application/json")
    payload = {
        "job": job,
        "status": j.get("status"),
        "started": j.get("started"),
        "ended": j.get("ended"),
        "rows": j.get("rows") or [],
        "info": j.get("info") or {},
        "error": j.get("error"),
    }
    return Response(json.dumps(payload, ensure_ascii=False), mimetype="application/json")


@app.get("/csv")
def job_csv():
    """Genera un CSV directamente desde el resultado del job (estable)."""
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return "Job not found", 404
    if j["status"] != "done":
        return "Job still running", 202

    rows = j.get("raw_rows") or []
    if not rows:
        return "No data", 204

    df = pd.DataFrame(rows)
    cols = ["symbol","price","pct_change_24h","quote_volume_24h",
            "Trend_4H","ScoreTrend","fundingRate","openInterest",
            "oi_delta","NetScore","GHI"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    csv_bytes = buf.getvalue().encode("utf-8")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=usdt_screener_{job}.csv"}
    )


@app.get("/csv_tmp")
def csv_tmp():
    """Sirve el archivo temporal /tmp/usdt_screener.csv si existe."""
    from pathlib import Path
    p = Path("/tmp/usdt_screener.csv")
    if not p.exists():
        return "El archivo /tmp/usdt_screener.csv no existe", 404
    return send_file(
        str(p),
        mimetype="text/csv",
        as_attachment=True,
        download_name="usdt_screener.csv",
    )


# Salud
@app.get("/healthz")
def healthz():
    return "ok", 200


# ============== main (local) ==============
if __name__ == "__main__":
    # Para probar local: FLASK_DEBUG=1 python app.py
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), debug=True)
