# -*- coding: utf-8 -*-
import os
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, redirect, url_for, render_template_string, Response, send_file
import io 
import pandas as pd

import Binance_usdt_2 as engine
from pathlib import Path

@app.get("/csv_tmp")
def csv_tmp():
    p = Path("/tmp/usdt_screener.csv")
    if not p.exists():
        return "El archivo /tmp/usdt_screener.csv no existe", 404
    return send_file(
        str(p),
        mimetype="text/csv",
        as_attachment=True,
        download_name="usdt_screener.csv"
    )


app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=2)
JOBS = {}  # job_id -> dict(status, started, ended, raw_rows(list[dict]), log(str))

HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:18px;color:#111}
    table{border-collapse:collapse;width:100%;font-size:14px}
    th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;white-space:nowrap}
    th{background:#f6f6f6;position:sticky;top:0}
    td:first-child,th:first-child{text-align:left}
    .tag{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px}
    .ok{background:#e8fff0;color:#126f3d}
    .warn{background:#fff6e0;color:#7a6200}
    .err{background:#ffefef;color:#9c1a1a}
    .muted{color:#777}
    .mono{font-family:ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px}
    .toolbar{margin-bottom:10px}
    .spinner{display:inline-block;width:12px;height:12px;border:2px solid #ccc;border-top-color:#333;border-radius:50%;animation:spin 0.9s linear infinite;margin-right:6px}
    @keyframes spin{to{transform:rotate(360deg)}}
  </style>
</head>
<body>
  <h2>{{ title }}</h2>

  <div class="toolbar">
    Job <span class="mono">{{ job }}</span> ·
    Estado: <span id="status" class="tag warn">running</span>
    · Inicio: <span id="started">{{ started }}</span>
    · <a id="jsonlink" href="{{ url_for('job_json', job=job) }}" target="_blank">Ver JSON</a>
    · <a id="csvlink" href="{{ url_for('job_csv', job=job) }}" target="_blank">Descargar CSV</a>
  </div>

  <div id="hint" class="muted"><span class="spinner"></span>Calculando… esta página se actualizará automáticamente.</div>

  <h3 id="titleTop" style="display:none">Top</h3>
  <div id="tableWrap"></div>

  <h3>Log</h3>
  <pre id="log" class="mono">{{ log or '' }}</pre>

<script>
const jobId = "{{ job }}";
const apiUrl = "{{ url_for('job_json', job=job) }}";
const statusEl = document.getElementById('status');
const hintEl = document.getElementById('hint');
const tableWrap = document.getElementById('tableWrap');
const titleTop = document.getElementById('titleTop');
const logEl = document.getElementById('log');

function fmtNum(n, digits){ if(n===null||n===undefined||isNaN(n)) return "-"; return Number(n).toFixed(digits); }
function fmtPct(n, digits){ if(n===null||n===undefined||isNaN(n)) return "-"; return (Number(n)*100).toFixed(digits)+"%"; }
function fmtInt(n){ if(n===null||n===undefined||isNaN(n)) return "-"; return Number(n).toLocaleString(); }

function render(rows){
  const cols = ["symbol","price","pct_change_24h","quote_volume_24h","Trend_4H","ScoreTrend","fundingRate","openInterest","oi_delta","NetScore","GHI"];
  let thead = "<thead><tr>" + cols.map(c=>`<th>${c}</th>`).join("") + "</tr></thead>";
  let body = rows.map(r=>{
    return "<tr>"
      + `<td>${r.symbol}</td>`
      + `<td>${fmtNum(r.price, 8)}</td>`
      + `<td>${fmtPct(r.pct_change_24h, 3)}</td>`
      + `<td>${fmtInt(r.quote_volume_24h)}</td>`
      + `<td>${r.Trend_4H||"-"}</td>`
      + `<td>${fmtNum(r.ScoreTrend, 3)}</td>`
      + `<td>${r.fundingRate===null||r.fundingRate===undefined||isNaN(r.fundingRate)?"-":(Number(r.fundingRate)*100).toFixed(4)+"%"}</td>`
      + `<td>${fmtInt(r.openInterest)}</td>`
      + `<td>${fmtInt(r.oi_delta)}</td>`
      + `<td>${r.NetScore===null||r.NetScore===undefined?"-":(Number(r.NetScore).toFixed(2))}</td>`
      + `<td>${fmtNum(r.GHI, 3)}</td>`
      + "</tr>";
  }).join("");
  tableWrap.innerHTML = `<table>${thead}<tbody>${body}</tbody></table>`;
  titleTop.style.display = "";
  titleTop.textContent = "Top " + rows.length;
}

async function poll(){
  try{
    const r = await fetch(apiUrl, {cache:"no-store"});
    if(r.status === 404){
      statusEl.className = "tag err";
      statusEl.textContent = "not found";
      hintEl.textContent = "Este job ya no existe (posible redeploy o expiración). Lanza uno nuevo con /";
      return;
    }
    const j = await r.json();
    if(j.status !== "done"){
      statusEl.className = "tag warn";
      statusEl.textContent = j.status || "running";
      setTimeout(poll, 2000);
      return;
    }
    statusEl.className = "tag ok";
    statusEl.textContent = "done";
    hintEl.style.display = "none";
    render(j.rows || []);
  }catch(e){
    statusEl.className = "tag err";
    statusEl.textContent = "error";
    hintEl.textContent = "Error consultando el API del job. Reintenta.";
  }
}
poll();
</script>
</body>
</html>
"""

def run_job(job_id: str, limit: int):
    logs = []
    def logp(msg):
        print(msg)
        logs.append(str(msg))

    try:
        JOBS[job_id] = {"status":"running","started":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "ended":None,"raw_rows":None,"log":None}
        df, info = engine.build_df(limit_symbols=limit, logger=logp)
        rows = df.to_dict(orient="records")
        JOBS[job_id]["raw_rows"] = rows
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["ended"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        JOBS[job_id]["log"] = "\n".join(logs + [str(info)])
    except Exception as e:
        JOBS[job_id]["status"] = f"error: {e}"
        JOBS[job_id]["ended"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        JOBS[job_id]["log"] = "\n".join(logs) + f"\nERROR: {e}"

@app.get("/")
def index():
    limit = int(request.args.get("limit") or os.environ.get("LIMIT_SYMBOLS", "150"))
    job = uuid.uuid4().hex[:10]
    # arranca en background
    executor.submit(run_job, job, limit)
    return redirect(url_for("view", job=job))

@app.get("/view")
def view():
    job = request.args.get("job")
    j = JOBS.get(job)
    # si aún no existe (arrancando), crea stub para que el HTML pueda “pullear”
    if not j:
        JOBS[job] = {"status":"running","started":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                     "ended":None,"raw_rows":None,"log":None}
        j = JOBS[job]
    return render_template_string(
        HTML,
        title=f"Job {job}",
        job=job,
        started=j.get("started"),
        log=j.get("log"),
    )

@app.get("/api")
@app.get("/csv")
def job_csv():
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
    # Orden “web-friendly” (ajústalo si quieres)
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

def job_json():
    job = request.args.get("job")
    j = JOBS.get(job)
    if not j:
        return jsonify({"error":"job not found"}), 404
    if j["status"] != "done":
        return jsonify({"job":job, "status":j["status"], "started": j.get("started")})
    return jsonify({
        "job": job,
        "status": "done",
        "started": j.get("started"),
        "ended": j.get("ended"),
        "rows": j.get("raw_rows") or [],
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT","8000")), debug=False)
