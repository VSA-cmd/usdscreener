from flask import Flask, jsonify, request, Response
import threading, time, uuid, json
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

"""
APP BEHAVIOR
- /           : health & quick links
- /start      : kicks off a background job; returns {"job": "..."}
- /status?job : returns {"status": "queued|running|done|error", ...}
- /view?job   : renders an HTML dashboard (tables + charts) when ready

We import a function from Binance_usdt_2.py that returns a pandas DataFrame
with your screener results entirely IN MEMORY (no CSV on disk).
"""

# ------------------------ JOB REGISTRY ------------------------
JOBS = {}  # job_id -> dict(status, started, df_json, err)

app = Flask(__name__)

# ------------------------ DATA PIPE ---------------------------
def _load_dataframe():
    """Call your screener to get a pandas DataFrame in-memory."""
    # We expect the helper to exist in Binance_usdt_2.py (see patched file below)
    from Binance_usdt_2 import run_screener
    df = run_screener(return_df=True)  # <-- in-memory, no CSV
    # Minimal clean-up: keep key columns if present; otherwise show whatever we have.
    preferred_cols = [
        "symbol", "price", "pct_change_24h", "pct_change_48h",
        "quote_volume_24h", "Trend_4H", "ScoreAdj", "ScoreTrend",
        "fundingRate", "openInterest"
    ]
    cols = [c for c in preferred_cols if c in df.columns]
    if cols:
        df = df[cols].copy()
    # coerce types we’ll graph
    for c in ("price","pct_change_24h","pct_change_48h","quote_volume_24h","fundingRate","openInterest","ScoreAdj","ScoreTrend"):
        if c in df.columns:
            with pd.option_context('future.no_silent_downcasting', True):
                df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _job_runner(job_id: str):
    JOBS[job_id]["status"] = "running"
    try:
        df = _load_dataframe()
        # keep only top 400 rows to keep HTML light (adjust if you like)
        if len(df) > 400:
            df = df.head(400)
        # Save to JSON (records) so we don’t keep a heavy object in memory
        JOBS[job_id]["df_json"] = df.to_json(orient="records")
        JOBS[job_id]["status"] = "done"
    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["err"] = repr(e)

# ------------------------ HTML RENDERING ----------------------
def _html_page(body: str, title: str = "USDT Screener"):
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 20px; color: #111; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 14px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px; border-bottom: 1px solid #eee; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    .badge {{ display:inline-block; padding:.25rem .5rem; border-radius:999px; font-size:12px; border:1px solid #e5e7eb; }}
    .status {{ margin-bottom:12px; }}
    .btn {{ display:inline-block; padding:10px 14px; border-radius:10px; border:1px solid #e5e7eb; text-decoration:none; }}
    .muted {{ color:#6b7280; }}
    .ok {{ color:#059669; }}
    .err {{ color:#b91c1c; }}
    .header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }}
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
{body}
</body>
</html>"""

def _render_dashboard(df: pd.DataFrame) -> str:
    # Safeguards if columns missing
    has_pct = "pct_change_24h" in df.columns
    has_vol = "quote_volume_24h" in df.columns
    has_trend = "Trend_4H" in df.columns
    has_score = "ScoreTrend" in df.columns
    has_oi = "openInterest" in df.columns
    has_fund = "fundingRate" in df.columns

    # Cards: counts
    total = len(df)
    trends = (df["Trend_4H"].value_counts().to_dict() if has_trend else {})
    card_html = f"""
    <div class="grid">
      <div class="card">
        <div class="header"><h3>Resumen</h3><span class="badge">{total} símbolos</span></div>
        <div class="muted">4H Trend breakdown:</div>
        <div style="margin-top:8px">
          {"".join([f'<span class="badge" style="margin-right:6px">{k}: {v}</span>' for k,v in trends.items()]) if trends else "<span class='muted'>N/A</span>"}
        </div>
      </div>
    """

    # Chart 1: Top % movers (absolute)
    movers_div = ""
    if has_pct:
        movers = df.dropna(subset=["pct_change_24h"]).copy()
        movers["abs_move"] = movers["pct_change_24h"].abs()
        movers = movers.sort_values("abs_move", ascending=False).head(20)
        fig1 = px.bar(movers, x="symbol", y="pct_change_24h", title="Top 20 Movers (24h %)")
        fig1.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        movers_div = fig1.to_html(full_html=False, include_plotlyjs=False)
    else:
        movers_div = "<div class='muted'>No pct_change_24h column</div>"

    # Chart 2: Volume leaders
    vol_div = ""
    if has_vol:
        vol = df.dropna(subset=["quote_volume_24h"]).sort_values("quote_volume_24h", ascending=False).head(20)
        fig2 = px.bar(vol, x="symbol", y="quote_volume_24h", title="Top 20 by 24h Quote Volume (USDT)")
        fig2.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        vol_div = fig2.to_html(full_html=False, include_plotlyjs=False)
    else:
        vol_div = "<div class='muted'>No quote_volume_24h column</div>"

    # Chart 3: Funding vs OI scatter (if both exist)
    fdo_div = ""
    if has_fund and has_oi:
        fd = df.dropna(subset=["fundingRate", "openInterest"]).copy()
        fd = fd.sort_values("openInterest", ascending=False).head(60)
        size = None
        if has_score:
            size = "ScoreTrend"
        fig3 = px.scatter(
            fd, x="fundingRate", y="openInterest", size=size, color="fundingRate",
            hover_name="symbol", title="Funding Rate vs Open Interest (top by OI)"
        )
        fig3.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=400)
        fdo_div = fig3.to_html(full_html=False, include_plotlyjs=False)
    else:
        fdo_div = "<div class='muted'>Funding/OI not available</div>"

    card_html += f"""
      <div class="card">{movers_div}</div>
      <div class="card">{vol_div}</div>
      <div class="card">{fdo_div}</div>
    </div>
    """

    # Table (top 100 by volume if present, else first 100)
    table_cols = [c for c in ["symbol","price","pct_change_24h","pct_change_48h","quote_volume_24h","Trend_4H","ScoreTrend","fundingRate","openInterest"] if c in df.columns]
    if "quote_volume_24h" in table_cols:
        table_df = df.sort_values("quote_volume_24h", ascending=False).head(100)[table_cols].copy()
    else:
        table_df = df.head(100)[table_cols].copy() if table_cols else df.head(100).copy()
    # formatting
    fmt = {}
    for c in table_df.columns:
        if "pct_" in c or c in ("fundingRate",):
            fmt[c] = "{:,.4%}".format if table_df[c].dtype.kind in "fc" else str
        elif "volume" in c or c in ("price","openInterest","ScoreTrend"):
            fmt[c] = "{:,.4f}".format if table_df[c].dtype.kind in "fc" else str
        else:
            fmt[c] = str
    rows = []
    rows.append("<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in table_df.columns) + "</tr></thead><tbody>")
    for _, row in table_df.iterrows():
        tds = []
        for c in table_df.columns:
            v = row[c]
            try:
                if pd.isna(v):
                    tds.append("<td class='muted'>—</td>")
                else:
                    tds.append(f"<td>{fmt[c](v)}</td>")
            except Exception:
                tds.append(f"<td>{v}</td>")
        rows.append("<tr>" + "".join(tds) + "</tr>")
    rows.append("</tbody></table>")
    table_html = "\n".join(rows)

    body = f"""
    <div class="header">
      <h1>USDT Screener</h1>
      <div>
        <a class="btn" href="/">Home</a>
        <span class="muted">Render free tier: first request may take longer while waking up.</span>
      </div>
    </div>
    {card_html}
    <div class="card" style="margin-top:16px">
      <div class="header"><h3>Top 100</h3><span class="muted">sorted by 24h volume (if available)</span></div>
      {table_html}
    </div>
    """
    return _html_page(body, title="USDT Screener")

# ------------------------ ROUTES ------------------------------
@app.get("/")
def health():
    links = """
    <div class="card">
      <div class="header"><h3>Endpoints</h3></div>
      <ul>
        <li><a class="btn" href="/start">/start</a> - inicia una corrida en background</li>
        <li>/status?job=&lt;id&gt; - estado de la corrida</li>
        <li>/view?job=&lt;id&gt; - dashboard HTML</li>
      </ul>
    </div>
    """
    return Response(_html_page(links, "Service Ready"), mimetype="text/html")

@app.get("/start")
def start():
    job = uuid.uuid4().hex[:12]
    JOBS[job] = {"status": "queued", "started": time.time(), "df_json": None, "err": None}
    t = threading.Thread(target=_job_runner, args=(job,), daemon=True)
    t.start()
    return jsonify({"job": job, "status": "queued"})

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
    job = request.args.get("job")
    if not job or job not in JOBS:
        return jsonify({"error": "invalid job id"}), 400
    info = JOBS[job]
    st = info["status"]
    if st == "error":
        return Response(_html_page(f"<div class='card'><h3>Error</h3><pre>{info.get('err')}</pre></div>", "Error"), mimetype="text/html")
    if st != "done":
        # Small auto-refresh page
        body = f"""
        <div class="card">
          <div class="status">Job <b>{job}</b> status: <span class="badge">{st}</span></div>
          <a class="btn" href="/status?job={job}" target="_blank">Check JSON status</a>
          <meta http-equiv="refresh" content="3">
          <div class="muted">This page will auto-refresh every 3s…</div>
        </div>
        """
        return Response(_html_page(body, "Working…"), mimetype="text/html")
    # Render dashboard from JSON
    df = pd.DataFrame(json.loads(info["df_json"]))
    html = _render_dashboard(df)
    return Response(html, mimetype="text/html")
