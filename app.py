# -*- coding: utf-8 -*-
import io
import os
import json
import uuid
import threading
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from flask import (
    Flask, request, Response, send_file,
    render_template_string, redirect, url_for
)

# Motor de cálculo (ligero para Render)
import Binance_usdt_2 as engine  # asegura el nombre correcto del archivo

# =================== Config ===================
app = Flask(__name__)

TOP_N = int(os.environ.get("TOP_N", "100"))  # filas a mostrar
JOBS_DIR = Path("/tmp/jobs")
JOBS_DIR.mkdir(parents=True, exist_ok=True)  # storage compartido entre workers
CSV_GLOBAL = Path("/tmp/usdt_screener.csv")  # si el motor lo guarda aquí

# =================== Utilidades ===================
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def write_json_atomic(path: Path, payload: dict):
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)  # atómico en el mismo filesystem


def read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def rows_from_df(df: pd.DataFrame, top_n: int) -> list[dict]:
    cols_pref = [
        "symbol", "price", "pct_change_24h", "quote_volume_24h",
        "Trend_4H", "ScoreTrend", "fundingRate",
        "openInterest", "oi_delta", "NetScore", "GHI"
    ]
    cols = [c for c in cols_pref if c in df.columns]
    slim = df[cols].head(top_n).copy()

    out = []
    for _, r in slim.iterrows():
        d = {}
        for k, v in r.items():
            if pd.isna(v):
                d[k] = None
            elif isinstance(v, (int, float)):
                d[k] = float(v)
            else:
                d[k] = v
        out.append(d)
    return out


def _run_job(job_id: str):
    """Ejecuta el motor en un hilo, guarda JSON y CSV de ese job."""
    jp = job_path(job_id)
    payload = {"job": job_id, "status": "running", "started": now_utc()}
    write_json_atomic(jp, payload)

    try:
        # Ejecuta motor: debe devolver (DataFrame, info_dict)
        df, info = engine.build_df()  # respeta LIMIT_SYMBOLS/MAX_WORKERS/BUDGET via ENV

        if df is None or df.empty:
            payload.update({
                "status": "error",
                "ended": now_utc(),
                "error": "No data returned by engine",
            })
            write_json_atomic(jp, payload)
            return

        # Guardamos CSV específico por job
        csv_job = JOBS_DIR / f"usdt_screener_{job_id}.csv"
        try:
            df.to_csv(csv_job, index=False)
        except Exception:
            # backup: generar CSV solo con columnas visibles
            rows = rows_from_df(df, len(df))
            pd.DataFrame(rows).to_csv(csv_job, index=False)

        rows = rows_from_df(df, TOP_N)

        payload.update({
            "status": "done",
            "ended": now_utc(),
            "rows": rows,
            "info": info or {},
            "csv_path": str(csv_job),
        })
        write_json_atomic(jp, payload)

    except Exception as e:
        payload.update({
            "status": "error",
            "ended": now_utc(),
            "error": str(e),
        })
        write_json_atomic(jp, payload)

# =================== Rutas ===================

@app.get("/")
def home():
    """Lanza un job y redirige a /view?job=..."""
    job_id = uuid.uuid4().hex[:10]
    # crea archivo de estado antes de lanzar el hilo (visible a todos los workers)
    write_json_atomic(job_path(job_id), {
        "job": job_id, "status": "running", "started": now_utc()
    })
    t = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    t.start()
    return redirect(url_for("view_job", job=job_id))


@app.get("/view")
def view_job():
    job = request.args.get("job")
    jp = job_path(job) if job else None
    j = read_json(jp) if jp else None
    if not j:
        return "Job not found", 404

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
    · <a href="{{ url_for('job_json', job=job) }}" target="_blank">Ver JSON</a>
    · <a href="{{ url_for('job_csv', job=job) }}" target="_blank">Descargar CSV</a>
  </p>

  {% if j["status"] != "done" %}
    <p class="muted">Actualiza en unos segundos…</p>
    <script> setTimeout(function(){ location.reload(); }, 4000); </script>
  {% elif j.get("error") %}
    <pre>{{ j["error"] }}</pre>
  {% else %}
    <h4>Top {{ rows|length }}</h4>
    <table>
      <thead>
        <tr>
          <th>symbol</th><th>price</th><th>% 24h</th><th>quote volume 24h</th>
          <th>Trend 4H</th><th>ScoreTrend</th><th>fundingRate</th>
          <th>openInterest</th><th>oi_delta</th><th>NetScore</th><th>GHI</th>
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
    job = request.args.get("job")
    j = read_json(job_path(job)) if job else None
    if not j:
        return Response(json.dumps({"status": "not_found"}), status=404, mimetype="application/json")
    return Response(json.dumps(j, ensure_ascii=False), mimetype="application/json")


@app.get("/csv")
def job_csv():
    """Devuelve el CSV del job si existe; de lo contrario 404/202 según estado."""
    job = request.args.get("job")
    j = read_json(job_path(job)) if job else None
    if not j:
        return "Job not found", 404
    if j.get("status") != "done":
        return "Job still running", 202

    csv_path = j.get("csv_path")
    if csv_path and Path(csv_path).exists():
        return send_file(csv_path, mimetype="text/csv", as_attachment=True,
                         download_name=f"usdt_screener_{job}.csv")

    # fallback: arme CSV desde las filas guardadas
    rows = j.get("rows") or []
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
    return Response(
        buf.getvalue().encode("utf-8"),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=usdt_screener_{job}.csv"}
    )


@app.get("/csv_tmp")
def csv_tmp():
    """Sirve el CSV global que el motor escriba en /tmp/usdt_screener.csv (si existe)."""
    if not CSV_GLOBAL.exists():
        return "El archivo /tmp/usdt_screener.csv no existe", 404
    return send_file(str(CSV_GLOBAL), mimetype="text/csv",
                     as_attachment=True, download_name="usdt_screener.csv")


@app.get("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    # Para pruebas locales: FLASK_DEBUG=1 python app.py
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), debug=True)
