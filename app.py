# app.py
import os
import json
import time
import uuid
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, redirect, request, jsonify, send_file, url_for, Response, make_response

# ────────────────────────────────────────────────────────────────────────────────
# Configuración
# ────────────────────────────────────────────────────────────────────────────────
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "150"))
MAX_WORKERS   = int(os.getenv("MAX_WORKERS", "12"))
BUDGET_SEC    = int(os.getenv("BUDGET", "110"))  # segundos

CSV_TMP_PATH  = Path("/tmp/usdt_screener.csv")   # donde el motor guarda siempre
JOBS_DIR      = Path("/tmp/jobs")                # persistimos estado por job
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Carga del motor (soportamos ambos nombres)
# ────────────────────────────────────────────────────────────────────────────────
engine = None
engine_name = None
try:
    import Binance_usdt_2 as engine  # type: ignore
    engine_name = "Binance_usdt_2"
except Exception:
    try:
        import Binance_usdt as engine  # type: ignore
        engine_name = "Binance_usdt"
    except Exception:
        engine = None
        engine_name = None

# ────────────────────────────────────────────────────────────────────────────────
# App
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

JOBS_LOCK = threading.Lock()
JOBS = {}  # job_id -> dict

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def job_json_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def job_csv_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

def save_job(job_id: str, payload: dict) -> None:
    payload["_engine"] = engine_name
    payload["_updated_utc"] = now_utc_iso()
    job_json_path(job_id).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with JOBS_LOCK:
        JOBS[job_id] = payload

def load_job(job_id: str) -> dict | None:
    p = job_json_path(job_id)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8") or "{}")
            with JOBS_LOCK:
                JOBS[job_id] = data
            return data
        except Exception:
            return None
    with JOBS_LOCK:
        return JOBS.get(job_id)

def call_engine_and_wait():
    """
    Ejecuta la función principal del motor y deja el CSV en CSV_TMP_PATH.
    """
    if engine is None:
        raise RuntimeError("No se pudo importar el motor (Binance_usdt_2 / Binance_usdt).")

    tried = []

    if hasattr(engine, "run_screener"):
        tried.append("run_screener")
        return engine.run_screener(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore

    if hasattr(engine, "run"):
        tried.append("run")
        return engine.run(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore

    if hasattr(engine, "main"):
        tried.append("main")
        try:
            return engine.main(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore
        except TypeError:
            return engine.main()  # type: ignore

    for fname in ("screener", "start", "execute"):
        if hasattr(engine, fname):
            tried.append(fname)
            fn = getattr(engine, fname)
            try:
                return fn(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore
            except TypeError:
                return fn()  # type: ignore

    raise RuntimeError(f"No encontré una función de entrada usable en el motor. Probé: {tried}")

def background_job(job_id: str):
    started = time.perf_counter()
    meta = {
        "job": job_id,
        "status": "running",
        "started": now_utc_iso(),
        "params": {"LIMIT_SYMBOLS": LIMIT_SYMBOLS, "MAX_WORKERS": MAX_WORKERS, "BUDGET": BUDGET_SEC},
    }
    print(f"{datetime.utcnow().strftime('%H:%M:%S')} Iniciando job · LIMIT_SYMBOLS={LIMIT_SYMBOLS} "
          f"MAX_WORKERS={MAX_WORKERS} BUDGET={BUDGET_SEC}s")
    save_job(job_id, meta)

    try:
        call_engine_and_wait()

        if CSV_TMP_PATH.exists():
            shutil.copyfile(CSV_TMP_PATH, job_csv_path(job_id))
        else:
            print("⚠️  El motor terminó pero no encontré /tmp/usdt_screener.csv")

        rows = []
        if job_csv_path(job_id).exists():
            try:
                import pandas as pd
                df = pd.read_csv(job_csv_path(job_id))
                rows = df.to_dict(orient="records")
            except Exception as e:
                print(f"⚠️  No pude leer el CSV del job {job_id}: {e}")

        meta.update({
            "status": "done",
            "ended": now_utc_iso(),
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "count": len(rows),
            "rows": rows,
            "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        })
        save_job(job_id, meta)
    except Exception as e:
        rows = []
        if job_csv_path(job_id).exists():
            try:
                import pandas as pd
                df = pd.read_csv(job_csv_path(job_id))
                rows = df.to_dict(orient="records")
            except Exception:
                pass

        meta.update({
            "status": "error",
            "ended": now_utc_iso(),
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "error": str(e),
            "count": len(rows),
            "rows": rows,
        })
        save_job(job_id, meta)
        print(f"❌ Job {job_id} error: {e}")

def start_new_job() -> str:
    job_id = uuid.uuid4().hex[:10]
    save_job(job_id, {
        "job": job_id,
        "status": "queued",
        "started": now_utc_iso(),
        "params": {"LIMIT_SYMBOLS": LIMIT_SYMBOLS, "MAX_WORKERS": MAX_WORKERS, "BUDGET": BUDGET_SEC},
    })
    t = threading.Thread(target=background_job, args=(job_id,), daemon=True)
    t.start()
    return job_id

# ────────────────────────────────────────────────────────────────────────────────
# Rutas
# ────────────────────────────────────────────────────────────────────────────────
@app.after_request
def no_cache(resp: Response):
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp

@app.get("/")
def index():
    job_id = start_new_job()
    return redirect(url_for("view_job", job=job_id), code=302)

@app.get("/view")
def view_job():
    job_id = request.args.get("job", "").strip() or start_new_job()

    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Job {job_id}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, 'Helvetica Neue', Arial, sans-serif;
           line-height:1.45; margin: 24px; color:#0f172a; }}
    .muted {{ color:#64748b; font-size:.9rem; }}
    .pill  {{ display:inline-block; padding:.2rem .5rem; border-radius:999px; background:#e2e8f0; }}
    .ok    {{ background:#dcfce7; }}
    .err   {{ background:#fee2e2; }}
    a {{ color:#2563eb; text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    .row {{ margin-top:.35rem; }}
    .tiny {{ font-size:.85rem; }}
  </style>
</head>
<body>
  <h3 style="margin:0 0 12px 0;">Job {job_id}</h3>

  <div id="status" class="row muted">Cargando estado...</div>
  <div class="row tiny muted">Actualiza en unos segundos...</div>
  <div id="links" class="row" style="margin-top:12px;"></div>

  <script>
    const jobId = {json.dumps(job_id)};
    const st    = document.getElementById('status');
    const links = document.getElementById('links');

    function fmt(d) {{
      if (!d) return '—';
      return d;
    }}

    function render(data) {{
      let badge = '<span class="pill">' + data.status + '</span>';
      if (data.status === 'done')  badge = '<span class="pill ok">ok</span>';
      if (data.status === 'error') badge = '<span class="pill err">error</span>';

      st.innerHTML = `
        <div class="row">Estado: ${{badge}}
          · inicio: ${{fmt(data.started)}} · fin: ${{fmt(data.ended || '')}}
          · <a href="/api?job=${{jobId}}" target="_blank">Ver JSON</a>
          · <a href="/csv?job=${{jobId}}" id="csvlink">Descargar CSV</a>
        </div>
      `;

      links.innerHTML = '';
      if (data.error) {{
        const p = document.createElement('div');
        p.className = 'row tiny err';
        p.textContent = data.error;
        links.appendChild(p);
      }}
      if (data.count !== undefined) {{
        const p = document.createElement('div');
        p.className = 'row tiny muted';
        p.textContent = 'Filas: ' + data.count + (data.elapsed_sec ? ' · t=' + data.elapsed_sec + 's' : '');
        links.appendChild(p);
      }}
    }}

    async function tick() {{
      try {{
        const r = await fetch('/api?job=' + jobId, {{ cache:'no-store' }});
        if (r.status === 200) {{
          const data = await r.json();
          render(data);
          if (data.status === 'done' || data.status === 'error') {{
            clearInterval(h);
          }}
        }} else {{
          st.textContent = 'Job no encontrado (aún).';
        }}
      }} catch(e) {{
        st.textContent = 'Esperando...';
      }}
    }}
    const h = setInterval(tick, 3500);
    tick();
  </script>
</body>
</html>"""
    return make_response(html, 200)

@app.get("/api")
def job_api():
    job_id = (request.args.get("job") or "").strip()
    if not job_id:
        return jsonify({"error": "job requerido"}), 400
    data = load_job(job_id)
    if not data:
        return jsonify({"error": "Job not found", "job": job_id}), 404
    return jsonify(data)

@app.get("/csv")
def job_csv():
    job_id = (request.args.get("job") or "").strip()
    if not job_id:
        return jsonify({"error": "job requerido"}), 400
    csvp = job_csv_path(job_id)
    if not csvp.exists():
        return Response("CSV no listo", 202, mimetype="text/plain")
    return send_file(csvp, as_attachment=True, download_name=f"usdt_screener_{job_id}.csv")

@app.get("/csv_tmp")
def csv_tmp():
    """Descarga directa del último CSV que dejó el motor."""
    if not CSV_TMP_PATH.exists():
        return Response("No existe /tmp/usdt_screener.csv aún.", 404, mimetype="text/plain")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return send_file(CSV_TMP_PATH, as_attachment=True, download_name=f"usdt_screener_{ts}.csv")

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "time": now_utc_iso(), "engine": engine_name})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
