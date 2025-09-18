from flask import Flask, jsonify, request, Response, redirect, url_for
import threading, time, uuid, json, os, subprocess
import pandas as pd

app = Flask(__name__)

JOBS = {}  # job_id -> {"status": "...", "df_json": str|None, "err": str|None, "log": list[str]}

CSV_NAME = os.getenv("SCREENER_CSV", "usdt_screener.csv")
SCRIPT    = os.getenv("SCREENER_SCRIPT", "Binance_usdt_2.py")
DFJSON_MARK = "__DFJSON__="  # marcador para recibir el DF por stdout

def _append_log(job_id: str, line: str):
    if not line:
        return
    # <= arregla IndexError; guardamos hasta 400 chars de caudal
    JOBS[job_id]["log"].append(line[-400:])

def _run_pipeline_and_get_df(job_id: str) -> pd.DataFrame:
    """
    Lanza el script SIN timeout duro. Si el script imprime una línea
    que empieza por '__DFJSON__=', tomamos ese JSON como DataFrame.
    Si no, intentamos cargar el CSV si existe (fallback).
    """
    env = os.environ.copy()
    env.setdefault("DEADLINE_S", "180")  # control del propio script

    proc = subprocess.Popen(
        ["python", SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        bufsize=1
    )

    df_json_line = None

    # leer ambas tuberías de forma incremental
    while True:
        ret = proc.poll()

        # stdout
        if proc.stdout:
            s = proc.stdout.readline()
            if s:
                s = s.rstrip("\n")
                if s.startswith(DFJSON_MARK) and df_json_line is None:
                    # capturamos el JSON de datos
                    df_json_line = s[len(DFJSON_MARK):].strip()
                else:
                    _append_log(job_id, s)

        # stderr
        if proc.stderr:
            e = proc.stderr.readline()
            if e:
                _append_log(job_id, e.rstrip("\n"))

        if ret is not None:
            break
        time.sleep(0.02)

    # 1) Si recibimos JSON por stdout, lo usamos (preferido)
    if df_json_line:
        try:
            data = json.loads(df_json_line)
            return pd.DataFrame(data)
        except Exception as ex:
            _append_log(job_id, f"[warn] DFJSON inválido: {ex!r}")

    # 2) Fallback: intentar CSV si existe (para compatibilidad)
    if os.path.exists(CSV_NAME):
        try:
            df = pd.read_csv(CSV_NAME)
            try:
                os.remove(CSV_NAME)
            except Exception:
                pass
            return df
        except Exception as ex:
            _append_log(job_id, f"[warn] CSV inválido: {ex!r}")

    # 3) Error si no hay ni JSON ni CSV
    raise RuntimeError("No se recibió DF por stdout ni se generó CSV")

def _job_runner(job_id: str):
    JOBS[job_id]["status"] = "running"
    try:
        df = _run_pipeline_and_get_df(job_id)

        # Normalización suave de columnas esperadas
        preferred = [
            "symbol","price","pct_change_24h","pct_change_48h",
            "quote_volume_24h","Trend_4H","ScoreAdj","ScoreTrend",
            "fundingRate","openInterest"
        ]
        cols = [c for c in preferred if c in df.columns]
        if cols:
            df = df[cols].copy()

        for c in ("price","pct_change_24h","pct_change_48h","quote_volume_24h",
                  "fundingRate","openInterest","ScoreAdj","ScoreTrend"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        if len(df) > 500:
            df = df.head(500)

        JOBS[job_id]["df_json"] = df.to_json(orient="records")
        JOBS[job_id]["status"] = "done"
        _append_log(job_id, f"[ok] filas={len(df)}")
    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["err"] = repr(e)
        _append_log(job_id, f"[error] {repr(e)}")

def _html_wrapper(body: str, title="USDT Screener"):
    return f"""<!doctype html><html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;background:#fafafa;color:#111}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:16px}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:16px;box-shadow:0 1px 2px rgba(0,0,0,.04)}}
.btn{{display:inline-block;padding:10px 14px;border-radius:10px;border:1px solid #e5e7eb;text-decoration:none;background:#fff}}
.muted{{color:#6b7280}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{padding:8px;border-bottom:1px solid #eee;text-align:right}}
th:first-child,td:first-child{{text-align:left}}
.badge{{display:inline-block;padding:.25rem .5rem;border-radius:999px;font-size:12px;border:1px solid #e5e7eb;background:#fff}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}}
pre{{white-space:pre-wrap;max-height:220px;overflow:auto;background:#0b1220;color:#d1e0ff;padding:10px;border-radius:10px}}
</style>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head><body>
{body}
</body></html>"""

def _render_dashboard(df: pd.DataFrame) -> str:
    import plotly.express as px
    has_pct = "pct_change_24h" in df.columns
    has_vol = "quote_volume_24h" in df.columns
    has_trend = "Trend_4H" in df.columns
    has_score = "ScoreTrend" in df.columns
    has_oi = "openInterest" in df.columns
    has_fund = "fundingRate" in df.columns

    total = len(df)

    if has_pct:
        movers = df.dropna(subset=["pct_change_24h"]).copy()
        movers["abs_move"] = movers["pct_change_24h"].abs()
        movers = movers.sort_values("abs_move", ascending=False).head(20)
        fig1 = px.bar(movers, x="symbol", y="pct_change_24h", title="Top 20 Movers (24h %)")
        fig1.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        movers_div = fig1.to_html(full_html=False, include_plotlyjs=False)
    else:
        movers_div = "<div class='muted'>No pct_change_24h</div>"

    if has_vol:
        vol = df.dropna(subset=["quote_volume_24h"]).sort_values("quote_volume_24h", ascending=False).head(20)
        fig2 = pd.DataFrame(vol)
        import plotly.express as px
        fig2 = px.bar(vol, x="symbol", y="quote_volume_24h", title="Top 20 by 24h Quote Volume (USDT)")
        fig2.update_layout(margin=dict(l=20,r=20,t=40,b=20), height=360)
        vol_div = fig2.to_html(full_html=False, include_plotlyjs=False)
    else:
        vol_div = "<div class='muted'>No quote_volume_24h</div>"

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

    table_cols = [c for c in ["symbol","price","pct_change_24h","pct_change_48h","quote_volume_24h","Trend_4H","ScoreTrend","fundingRate","openInterest"] if c in df.columns]
    if "quote_volume_24h" in table_cols:
        table_df = df.sort_values("quote_volume_24h", ascending=False).head(100)[table_cols]
    else:
        table_df = df.head(100)[table_cols]

    def fmt_num(v, pct=False):
        if pd.isna(v): return "—"
        try: return f"{v:,.4%}" if pct else f"{v:,.4f}"
        except Exception: return str(v)

    rows = ["<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in table_df.columns) + "</tr></thead><tbody>"]
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
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="header"><h3>Resumen</h3><span class="badge">{total} símbolos</span></div>
        <div class="muted">4H Trend breakdown visible en tabla.</div>
      </div>
      <div class="card">{movers_div}</div>
      <div class="card">{vol_div}</div>
      <div class="card">{fdo_div}</div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="header"><h3>Top 100</h3><span class="muted">sorted by 24h volume (if available)</span></div>
      {table_html}
    </div>
    """
    return _html_wrapper(body, "USDT Screener")

@app.get("/")
def home():
    body = f"""
    <div class="header">
      <h1>USDT Screener</h1>
      <a class="btn" href="/start">Run screener</a>
    </div>
    <div class="card"><h3>Endpoints</h3>
      <ul>
        <li><code>/start</code> – lanza ejecución en background</li>
        <li><code>/status?job=&lt;id&gt;</code> – estado + log</li>
        <li><code>/view?job=&lt;id&gt;</code> – dashboard HTML</li>
      </ul>
    </div>
    """
    return Response(_html_wrapper(body, "Service Ready"), mimetype="text/html")

@app.get("/start")
def start():
    job = uuid.uuid4().hex[:12]
    JOBS[job] = {"status": "queued", "df_json": None, "err": None, "log": []}
    t = threading.Thread(target=_job_runner, args=(job,), daemon=True)
    t.start()
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
    job = request.args.get("job")
    if not job or job not in JOBS:
        return jsonify({"error": "invalid job id"}), 400
    info = JOBS[job]
    st = info["status"]
    if st == "error":
        log_html = "<br>".join(info.get("log", [])[-80:])
        return Response(_html_wrapper(f"<div class='card'><h3>Error</h3><pre>{log_html or info.get('err')}</pre><a class='btn' href='/start'>Run again</a></div>","Error"), mimetype="text/html")
    if st != "done":
        log_html = "<br>".join(info.get("log", [])[-80:])
        body = f"""
        <div class="card">
          <p>Job <b>{job}</b> status: <span class="badge">{st}</span></p>
          <p class="muted">Esta página se recarga cada 3s…</p>
          <meta http-equiv="refresh" content="3" />
          <a class="btn" href="/status?job={job}" target="_blank">Ver JSON</a>
          <pre>{log_html}</pre>
        </div>
        """
        return Response(_html_wrapper(body, "Working…"), mimetype="text/html")
    df = pd.DataFrame(json.loads(info["df_json"]))
    return Response(_render_dashboard(df), mimetype="text/html")
