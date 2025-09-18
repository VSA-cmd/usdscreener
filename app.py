import os
import json
import threading
import subprocess
import time
from flask import Flask, jsonify, redirect, render_template_string, request, url_for

app = Flask(__name__)

JOBS = {}  # job_id -> {"status": "...", "started": ts, "df_json": None, "log": []}

HOME_HTML = """
<!doctype html>
<title>USDT Screener</title>
<div style="font-family:system-ui, Segoe UI, Arial; max-width: 980px; margin: 32px auto;">
  <h1>USDT Screener</h1>
  <p>Render free tier: la primera petición puede tardar un poco en “despertar”.</p>
  <form action="{{ url_for('start') }}" method="get">
    <button style="padding:10px 16px; font-size:16px;">Run screener</button>
  </form>
</div>
"""

VIEW_HTML = """
<!doctype html>
<title>USDT Screener</title>
<div style="font-family:system-ui, Segoe UI, Arial; max-width: 1100px; margin: 28px auto;">
  <h2>Job {{ job_id }}</h2>
  {% if status == "running" %}
    <p><b>Status:</b> {{ status }} — Esta página se recarga cada 3s…</p>
    <form action="" method="get"><button>Refresh</button></form>
    <details style="margin-top:12px;"><summary>Ver JSON</summary><pre>{{ job_state|tojson(indent=2) }}</pre></details>
    <script>setTimeout(()=>location.reload(), 3000);</script>
  {% elif status == "done" %}
    <p><b>Status:</b> {{ status }}</p>
    <details style="margin-top:12px;"><summary>Ver JSON</summary><pre>{{ job_state|tojson(indent=2) }}</pre></details>
    {% if df and (df|length) > 0 %}
      <hr/>
      <h3>Top 100</h3>
      <table border="1" cellspacing="0" cellpadding="6">
        <thead>
          <tr>
            <th>symbol</th><th>price</th><th>pct_change_24h</th>
            <th>quote_volume_24h</th><th>Trend_4H</th><th>ScoreTrend</th>
            <th>fundingRate</th><th>openInterest</th>
          </tr>
        </thead>
        <tbody>
          {% for row in df %}
          <tr>
            <td>{{ row.symbol }}</td>
            <td>{{ "%.4f"|format(row.price|float) if row.price is not none else "" }}</td>
            <td>{{ "%.4f%%"|format(row.pct_change_24h*100) if row.pct_change_24h is not none else "" }}</td>
            <td>{{ "{:,.0f}".format(row.quote_volume_24h) if row.quote_volume_24h is not none else "" }}</td>
            <td>{{ row.Trend_4H or "" }}</td>
            <td>{{ "%.3f"|format(row.ScoreTrend) if row.ScoreTrend is not none else "" }}</td>
            <td>{{ ("%.5f%%"|format(row.fundingRate*100)) if row.fundingRate is not none else "" }}</td>
            <td>{{ "{:,.0f}".format(row.openInterest) if row.openInterest is not none else "" }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    {% else %}
      <p>No llegó DataFrame (o llegó vacío). Revisa el log debajo.</p>
    {% endif %}
    <hr/><h3>Log</h3>
    <pre style="background:#111;color:#0f0;padding:12px; white-space:pre-wrap; max-height:320px; overflow:auto;">
{{ log_txt }}
    </pre>
    <p><a href="{{ url_for('home') }}">Home</a></p>
  {% else %}
    <h3>Error</h3>
    <pre style="background:#111;color:#f66;padding:12px;">{{ err_msg }}</pre>
    <p><a href="{{ url_for('home') }}">Run again</a></p>
  {% endif %}
</div>
"""

def _append_log(job_id: str, line: str):
    if not line:
        return
    # corte seguro a 400 chars como slice (NO índice)
    JOBS[job_id]["log"].append(line[-400:])

def _run_pipeline_and_load_df(job_id: str, timeout_sec: int = 240):
    """
    Ejecuta Binance_usdt_2.py y extrae el DataFrame desde stdout buscando la marca __DFJSON__=
    """
    cmd = ["python", "Binance_usdt_2.py"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    df_json = None
    try:
        t0 = time.time()
        for line in proc.stdout:
            _append_log(job_id, line.strip())
            if "__DFJSON__=" in line:
                try:
                    payload = line.split("__DFJSON__=", 1)[1].strip()
                    df_json = json.loads(payload)
                except Exception as e:
                    _append_log(job_id, f"[parse-error] {repr(e)}")
            if time.time() - t0 > timeout_sec:
                proc.kill()
                raise TimeoutError(f"TimeoutExpired({cmd}, {timeout_sec})")
        rc = proc.wait(timeout=5)
        if rc != 0:
            _append_log(job_id, f"[exit-code] {rc}")
    except Exception as e:
        _append_log(job_id, f"[error] {repr(e)}")

    return df_json

def _job_runner(job_id: str):
    JOBS[job_id]["status"] = "running"
    try:
        df = _run_pipeline_and_load_df(job_id)
        JOBS[job_id]["df_json"] = df or []
        JOBS[job_id]["status"] = "done"
    except Exception as e:
        _append_log(job_id, f"[error-final] {repr(e)}")
        JOBS[job_id]["status"] = "error"

@app.route("/")
def home():
    return render_template_string(HOME_HTML)

@app.route("/start")
def start():
    job_id = f"{int(time.time()*1000):x}"[-12:]
    JOBS[job_id] = {"status": "queued", "started": time.time(), "df_json": None, "log": []}
    t = threading.Thread(target=_job_runner, args=(job_id,), daemon=True)
    t.start()
    return redirect(url_for("view", job=job_id), code=302)

@app.route("/status")
def status():
    job_id = request.args.get("job", "")
    return jsonify({**JOBS.get(job_id, {}), "job": job_id})

@app.route("/view")
def view():
    job_id = request.args.get("job", "")
    state = JOBS.get(job_id)
    if not state:
        return render_template_string(VIEW_HTML, job_id=job_id, status="error", err_msg="Job no encontrado", job_state={}, log_txt="")
    log_txt = "\n".join(state.get("log", []))
    df = state.get("df_json") or []
    return render_template_string(
        VIEW_HTML,
        job_id=job_id,
        status=state.get("status"),
        job_state={**state, "job": job_id},
        df=df,
        log_txt=log_txt,
        err_msg="",
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
