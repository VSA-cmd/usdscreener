from flask import Flask, jsonify, request, Response, redirect, url_for
import threading, time, uuid, json, os
import pandas as pd
import plotly.express as px

"""
USO:
- /            : Home con botón "Run screener"
- /start       : Lanza job en background y redirige a /view?job=...
- /status?job  : JSON con estado
- /view?job    : Dashboard HTML (auto-refresh hasta terminar)

Internamente llamamos a Binance_usdt_2.py como subproceso para que ejecute
SU pipeline normal; luego leemos 'usdt_screener.csv' si fue creado y lo
convertimos a DataFrame (en memoria) y eliminamos el CSV.
"""

app = Flask(__name__)

JOBS = {}        # job_id -> {"status": "...", "df_json": str|None, "err": str|None, "started": float}
LAST_JOB = None  # guardar último job exitoso

CSV_NAME = os.getenv("SCREENER_CSV", "usdt_screener.csv")
SCRIPT    = os.getenv("SCREENER_SCRIPT", "Binance_usdt_2.py")
TIMEOUT_S = int(os.getenv("SCREENER_TIMEOUT", "240"))  # 4 min

# ---------- Helpers ----------
def _run_pipeline_and_load_df():
    """
    Ejecuta tu script como CLI, espera a que termine, lee el CSV si existe,
    lo elimina y devuelve un DataFrame. Lanza excepción si algo falla.
    """
    import subprocess
    # Ejecuta el script normal
    r = subprocess.run(["python", SCRIPT], capture_output=True, text=True, timeout=TIMEOUT_S)
    if r.returncode != 0:
        raise RuntimeError(f"script exit {r.returncode}\nSTDERR tail:\n{(r.stderr or '')[-4000:]}")
    # Lee CSV si existe
    if not os.path.exists(CSV_NAME):
        # si tu script escribe otro nombre, ponlo en SCREENER_CSV (env var)
        raise FileNotFoundError(f"CSV '{CSV_NAME}' no fue creado por {SCRIPT}")
    try:
        df = pd.read_csv(CSV_NAME)
    finally:
        # limpiar para no dejar archivos
        try:
            os.remove(CSV_NAME)
        except Exception:
            pass
    return df

def _job_runner(job_id: str):
    JOBS[job_id]["status"] = "running"
    try:
        df = _run_pipeline_and_load_df()
        # columnas preferidas si existen
        preferred = [
            "symbol","price","pct_change_24h","pct_change_48h",
            "quote_volume_24h","Trend_4H","ScoreAdj","ScoreTrend",
            "fundingRate","openInterest"
        ]
        cols = [c for c in preferred if c in df.columns]
        if cols:
            df = df[cols].copy()
        # coerción numérica
        for c in ("price","pct_change_24h","pct_change_48h","quote_volume_24h","fundingRate","openInterest","ScoreAdj","ScoreTrend"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if len(df) > 500:
            df = df.head(500)
        JOBS[job_id]["df_json"] = df.to_json(orient="records")
        JOBS[job_id]["status"] = "done"
    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["err"] = repr(e)

def _html_wrapper(body: str, title="USDT Screener"):
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 20px; color: #111; background:#fafafa; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(360px,1fr)); gap:16px; }}
    .card {{ background:#fff; border:1px solid #e5e7eb; border-radius:14px; padding:16px; box-shadow:0 1px 2px rgba(0,0,0,.04); }}
    .btn {{ display:inline-block; padding:10px 14px; border-radius:10px; border:1px solid #e5e7eb; text-decoration:none; background:#fff; }}
    .muted {{ color:#6b7280; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ padding:8px; border-bottom:1px solid #eee; text-align:right; }}
    th:first-child,td:first-child {{ text-align:left; }}
    .badge {{ display:inline-block; padding:.25rem .5rem; border-radius:999px; font-size:12px; border:1px solid #e5e7eb; background:#fff; }}
    .header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }}
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
{body}
</body>
</html>"""

def _render_dashboard(df: pd.DataFrame) -> str:
    has_pct = "pct_change_24h" in df.columns
    has_vol = "quote_volume_24h" in df.columns
    has_trend = "Trend_4H" in df.columns
    has_score = "ScoreTrend" in df.columns
    has_oi = "openInterest" in df.columns
    has_fund = "fundingRate" in df.columns

    total = len(df)
    trends = df["Trend_4H"].value_counts().to_dict() if has_trend else {}
    summary = f"""
    <div class="grid">
      <div class="card">
        <div class="header"><h3>Resumen</h3><span class="badge">{total} símbolos</span></div>
        <div class="muted">4H Trend breakdown:</div>
        <div style="margin-top:8px">
          {"".join([f'<span class="badge" style="margin-right:6px">{k}: {v}</span>' for k,v in trends.items()]) if trends else "<span class='muted'>N/A</span>"}
        </div>
      </div>
    """

    # Chart 1: Movers
    if has_pct:
        movers = df.dropna(subset=["pct_change_24h"]).copy()
        movers["abs_move"] = movers["pct_change_24h"].abs()
        movers = movers.sort_values("abs_move", ascending=False).head(20)
        fig1 = px.bar(movers, x="symbol", y="pct_change_24h", title="Top 20 Movers (24h %)")
        fig1.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        movers_div = fig1.to_html(full_html=False, include_plotlyjs=False)
    else:
        movers_div = "<div class='muted'>No pct_change_24h</div>"

    # Chart 2: Volume
    if has_vol:
        vol = df.dropna(subset=["quote_volume_24h"]).sort_values("quote_volume_24h", ascending=False).head(20)
        fig2 = px.bar(vol, x="symbol", y="quote_volume_24h", title="Top 20 by 24h Quote Volume (USDT)")
        fig2.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        vol_div = fig2.to_html(full_html=False, include_plotlyjs=False)
    else:
        vol_div = "<div class='muted'>No quote_volume_24h</div>"

    # Chart 3: Funding vs OI
    if has_fund and has_oi:
        fd = df.dropna(subset=["fundingRate","openInterest"]).copy()
        fd = fd.sort_values("openInterest", ascending=False).head(60)
        fig3 = px.scatter(fd, x="fundingRate", y="openInterest", color="fundingRate",
                          hover_name="symbol", size=("ScoreTrend" if has_score else None),
                          title="Funding Rate vs Open Interest (top by OI)")
        fig3.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=400)
        fdo_div = fig3.to_html(full_html=False, include_plotlyjs=False)
    else:
        fdo_div = "<div class='muted'>Funding/OI not available</div>"

    summary += f"""
      <div class="card">{movers_div}</div>
      <div class="card">{vol_div}</div>
      <div class="card">{fdo_div}</div>
    </div>
    """

    # Table
    table_cols = [c for c in ["symbol","price","pct_change_24h","pct_change_48h","quote_volume_24h","Trend_4H","ScoreTrend","fundingRate","openInterest"] if c in df.columns]
    table_df = df.sort_values("quote_volume_24h", ascending=False).head(100)[table_cols] if has_vol and table_cols else df.head(100)[table_cols] if table_cols else df.head(100).copy()

    # Format
    def fmt_num(v, pct=False):
        if pd.isna(v): return "—"
        try:
            return f"{v:,.4%}" if pct else f"{v:,.4f}"
        except Exception:
            return str(v)

    rows = []
    rows.append("<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in table_df.columns) + "</tr></thead><tbody>")
    for _, r in table_df.iterrows():
        tds = []
        for c in table_df.columns:
            if c.startswith("pct_") or c in ("fundingRate",):
                tds.append(f"<td>{fmt_num(r[c], pct=True)}</td>")
            elif c in ("price","quote_volume_24h","openInterest","ScoreTrend"):
                tds.append(f"<td>{fmt_num(r[c])}</td>")
            else:
                tds.append(f"<td>{r[c]}</td>")
        rows.append("<tr>" + "".join(tds) + "</tr>")
    rows.append("</tbody></table>")
    table_html = "\n".join(rows)

    body = f"""
    <div class="header">
      <h1>USDT Screener</h1>
      <div>
        <a class="btn" href="/" title="Home">Home</a>
        <a class="btn" href="/start" title="Run again">Run again</a>
        <span class="muted">Free tier: primer acceso puede tardar mientras despierta.</span>
      </div>
    </div>
    {summary}
    <div class="card" style="margin-top:16px">
      <div class="header"><h3>Top 100</h3><span class="muted">sorted by 24h volume (if available)</span></div>
      {table_html}
    </div>
    """
    return _html_wrapper(body, "USDT Screener")

# ---------- Routes ----------
@app.get("/")
def home():
    global LAST_JOB
    tip = "<div class='card'><p>Haz clic en <b>Run screener</b> para iniciar un job.</p></div>"
    last = ""
    if LAST_JOB and LAST_JOB in JOBS and JOBS[LAST_JOB]["status"] == "done":
        last = f"<div class='card'><p>Último resultado: <a class='btn' href='/view?job={LAST_JOB}'>ver dashboard</a></p></div>"
    body = f"""
    <div class="header">
      <h1>USDT Screener</h1>
      <a class="btn" href="/start">Run screener</a>
    </div>
    {tip}
    {last}
    <div class="card"><h3>Endpoints</h3>
      <ul>
        <li><code>/start</code> – lanza ejecución en background</li>
        <li><code>/status?job=&lt;id&gt;</code> – estado</li>
        <li><code>/view?job=&lt;id&gt;</code> – dashboard HTML</li>
      </ul>
    </div>
    """
    return Response(_html_wrapper(body, "Service Ready"), mimetype="text/html")

@app.get("/start")
def start():
    job = uuid.uuid4().hex[:12]
    JOBS[job] = {"status": "queued", "df_json": None, "err": None, "started": time.time()}
    t = threading.Thread(target=_job_runner, args=(job,), daemon=True)
    t.start()
    # redirige directo al viewer (auto-refresh hasta terminar)
    return redirect(url_for('view', job=job), code=302)

@app.get("/status")
def status():
    job = request.args.get("job")
    if not job or job not in JOBS:
        return jsonify({"error": "invalid job id"}), 400
    info = JOBS[job].copy()
    if info.get("df_json"):
        info["rows"] = len(json.loads(info["df_json"]))
    return jsonify({"job": job, **info})

@app.get("/view")
def view():
    global LAST_JOB
    job = request.args.get("job")
    if not job or job not in JOBS:
        return jsonify({"error": "invalid job id"}), 400
    info = JOBS[job]
    st = info["status"]
    if st == "error":
        return Response(_html_wrapper(f"<div class='card'><h3>Error</h3><pre>{info.get('err')}</pre><a class='btn' href='/start'>Run again</a></div>","Error"), mimetype="text/html")
    if st != "done":
        # simple auto refresh page
        body = f"""
        <div class="card">
          <p>Job <b>{job}</b> status: <span class="badge">{st}</span></p>
          <p class="muted">Esta página se recarga cada 3s…</p>
          <meta http-equiv="refresh" content="3" />
          <a class="btn" href="/status?job={job}" target="_blank">Ver JSON</a>
        </div>
        """
        return Response(_html_wrapper(body, "Working…"), mimetype="text/html")
    # render dashboard
    LAST_JOB = job
    df = pd.DataFrame(json.loads(info["df_json"]))
    return Response(_render_dashboard(df), mimetype="text/html")
