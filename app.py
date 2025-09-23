# -*- coding: utf-8 -*-
import os
import uuid
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, redirect, url_for, render_template_string

import pandas as pd

# Motor
import Binance_usdt_2 as engine

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=2)
JOBS = {}  # job_id -> dict(status, started, ended, df, log)

HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding:18px; color:#111;}
    table{border-collapse:collapse; width:100%; font-size:14px}
    th,td{border:1px solid #ddd; padding:6px 8px; text-align:right;}
    th{background:#f6f6f6; position:sticky; top:0}
    td:first-child, th:first-child{text-align:left}
    .tag{display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px}
    .ok{background:#e8fff0; color:#126f3d}
    .warn{background:#fff6e0; color:#7a6200}
    .err{background:#ffefef; color:#9c1a1a}
    .muted{color:#777}
    .mono{font-family:ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px}
    .toolbar{margin-bottom:10px}
  </style>
</head>
<body>
  <h2>{{ title }}</h2>
  {% if job %}
    <div class="toolbar">
      Job <span class="mono">{{ job }}</span> ·
      Estado: 
      {% if status == 'done' %}
        <span class="tag ok">done</span>
      {% elif status == 'running' %}
        <span class="tag warn">running</span>
      {% else %}
        <span class="tag err">{{ status }}</span>
      {% endif %}
      {% if started %} · Inicio: {{ started }}{% endif %}
      {% if ended %} · Fin: {{ ended }}{% endif %}
      · <a href="{{ url_for('job_json', job=job) }}">Ver JSON</a>
    </div>
  {% endif %}

  {% if status != 'done' %}
    <p class="muted">Actualiza en unos segundos…</p>
  {% else %}
    <h3>Top {{ df.shape[0] }}</h3>
    <table>
      <thead>
        <tr>
          {% for c in columns %}
            <th>{{ c }}</th>
          {% endfor %}
        </tr>
      </thead>
      <tbody>
        {% for _, row in df.iterrows() %}
          <tr>
            {% for c in columns %}
              <td>{{ row[c] }}</td>
            {% endfor %}
          </tr>
        {% endfor %}
      </tbody>
    </table>
    {% if log %}
      <h3>Log</h3>
      <pre class="mono">{{ log }}</pre>
    {% endif %}
  {% endif %}
</body>
</html>
"""

def run_job(job_id: str, limit: int):
    log = []
    def logp(msg):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        m = f"{ts} {msg}"
        print(m)
        log.append(m)

    try:
        JOBS[job_id]["status"] = "running"
        JOBS[job_id]["started"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        df, info = engine.build_df(limit_symbols=limit, logger=logp)

        # Selección de columnas “web-friendly”
        cols = [
            "symbol", "price", "pct_change_24h", "quote_volume_24h",
            "Trend_4H", "ScoreTrend", "fundingRate", "openInterest",
            "oi_delta", "NetScore", "GHI"
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None

        # Formatos
        fmt = df.copy()
        fmt["price"] = fmt["price"].map(lambda v: f"{v:.8f}" if pd.notna(v) else "-")
        fmt["pct_change_24h"] = (fmt["pct_change_24h"]*100).map(lambda v: f"{v:.3f}%" if pd.notna(v) else "-")
        fmt["quote_volume_24h"] = fmt["quote_volume_24h"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        fmt["ScoreTrend"] = fmt["ScoreTrend"].map(lambda v: f"{v:+.3f}" if pd.notna(v) else "-")
        fmt["fundingRate"] = (fmt["fundingRate"]*100).map(lambda v: f"{v:.4f}%" if pd.notna(v) else "-")
        fmt["openInterest"] = fmt["openInterest"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        fmt["oi_delta"] = fmt["oi_delta"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        fmt["NetScore"] = fmt["NetScore"].map(lambda v: f"{v:+.2f}" if pd.notna(v) else "-")
        fmt["GHI"] = fmt["GHI"].map(lambda v: f"{v:.3f}" if pd.notna(v) else "-")

        JOBS[job_id]["df"] = fmt[cols]
        JOBS[job_id]["raw"] = df
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["ended"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        JOBS[job_id]["log"] = "\n".join(log)
    except Exception as e:
        JOBS[job_id]["status"] = f"error: {e}"
        JOBS[job_id]["ended"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        JOBS[job_id]["log"] = "\n".join(log) + f"\nERROR: {e}"

@app.get("/")
def index():
    # arranca un job y redirige a /view
    limit = int(request.args.get("limit") or os.environ.get("LIMIT_SYMBOLS", "150"))
    job = uuid.uuid4().hex[:10]
    JOBS[job] = {"status":"queued", "started":None, "ended":None, "df":None, "log":None}
    executor.submit(run_job, job, limit)
    return redirect(url_for("view", job=job))

@app.get("/view")
def view():
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return "Job not found", 404
    df = j.get("df")
    status = j["status"]
    ctx = {
        "title": f"Job {job}",
        "job": job,
        "status": status,
        "started": j.get("started"),
        "ended": j.get("ended"),
        "columns": list(df.columns) if isinstance(df, pd.DataFrame) else [],
        "df": df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
        "log": j.get("log"),
    }
    return render_template_string(HTML, **ctx)

@app.get("/api")
def job_json():
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return jsonify({"error":"job not found"}), 404
    if j["status"] != "done":
        return jsonify({"job":job, "status":j["status"]})
    return jsonify({
        "job": job,
        "status": "done",
        "started": j.get("started"),
        "ended": j.get("ended"),
        "rows": j["raw"].to_dict(orient="records"),
    })

if __name__ == "__main__":
    # Para pruebas locales
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=False)
