import os
import json
import time
import uuid
import shutil
import threading
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List

from flask import Flask, Response, jsonify, redirect, request, send_file, make_response
from python_decouple import config

# ---------------------------
# Config
# ---------------------------
LIMIT_SYMBOLS = int(config("LIMIT_SYMBOLS", default="150"))
MAX_WORKERS = int(config("MAX_WORKERS", default="12"))
BUDGET_SEC = int(config("BUDGET", default="110"))

CSV_TMP_PATH = Path("/tmp/usdt_screener.csv")
JOBS_DIR = Path("/tmp/usd_jobs")
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------
# App
# ---------------------------
app = Flask(__name__)

# ---------------------------
# Utilidades
# ---------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def job_json_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def job_csv_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

def save_job(job_id: str, meta: Dict[str, Any]) -> None:
    p = job_json_path(job_id)
    with p.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    # Evitar caching de proxies
    p.touch()

def read_job(job_id: str) -> Optional[Dict[str, Any]]:
    p = job_json_path(job_id)
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def gen_job_id() -> str:
    return uuid.uuid4().hex[:10]

def wait_for_file(path: Path, timeout: float = 12.0, poll: float = 0.25) -> bool:
    """Espera hasta 'timeout' a que 'path' exista y tenga tamaño > 0."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        try:
            if path.exists() and path.stat().st_size > 0:
                return True
        except Exception:
            pass
        time.sleep(poll)
    return path.exists() and path.stat().st_size > 0

def call_engine_and_wait() -> None:
    """
    Intenta ejecutar el motor Binance_usdt_2 de varias formas.
    Debe escribir /tmp/usdt_screener.csv.
    """
    tried: List[str] = []

    # 1) Importar y llamar funciones conocidas
    try:
        import Binance_usdt_2 as engine  # type: ignore
        for fname in ("run_screener", "run", "main"):
            fn = getattr(engine, fname, None)
            if callable(fn):
                tried.append(f"import:{fname}")
                fn(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)
                return
            else:
                tried.append(f"no:{fname}")
    except Exception as e:
        tried.append(f"import_error:{type(e).__name__}")

    # 2) Ejecutar como módulo
    try:
        tried.append("subprocess:-m Binance_usdt_2")
        subprocess.run(
            ["python", "-m", "Binance_usdt_2"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=BUDGET_SEC + 30,
        )
        return
    except Exception as e:
        tried.append(f"mod_error:{type(e).__name__}")

    # 3) Ejecutar el archivo directamente si existe
    py_file = Path("Binance_usdt_2.py")
    if py_file.exists():
        try:
            tried.append("subprocess:Binance_usdt_2.py")
            subprocess.run(
                ["python", str(py_file)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=BUDGET_SEC + 30,
            )
            return
        except Exception as e:
            tried.append(f"file_error:{type(e).__name__}")

    raise RuntimeError(f"No encontré una función de entrada usable en el motor. Probé: {tried}")

# ---------------------------
# Trabajo en background
# ---------------------------
def background_job(job_id: str):
    started = time.perf_counter()
    meta: Dict[str, Any] = {
        "job": job_id,
        "status": "running",
        "started": now_utc_iso(),
        "params": {"LIMIT_SYMBOLS": LIMIT_SYMBOLS, "MAX_WORKERS": MAX_WORKERS, "BUDGET": BUDGET_SEC},
    }
    print(f"{datetime.utcnow().strftime('%H:%M:%S')} Iniciando job · LIMIT_SYMBOLS={LIMIT_SYMBOLS} "
          f"MAX_WORKERS={MAX_WORKERS} BUDGET={BUDGET_SEC}s")
    save_job(job_id, meta)

    try:
        # Ejecuta el motor
        call_engine_and_wait()

        # Espera a que /tmp/usdt_screener.csv aparezca
        csv_ready = CSV_TMP_PATH.exists() and wait_for_file(CSV_TMP_PATH, timeout=12.0, poll=0.25)

        src_csv = None
        if csv_ready:
            src_csv = CSV_TMP_PATH
        else:
            # Buscar cualquier *.csv reciente en /tmp (fallback)
            for cand in sorted(Path("/tmp").glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True):
                if wait_for_file(cand, timeout=2.0, poll=0.2):
                    src_csv = cand
                    break

        if not src_csv:
            raise RuntimeError("El motor no produjo un CSV en /tmp (usdt_screener.csv u otro *.csv).")

        # Copiar al CSV del job
        dest = job_csv_path(job_id)
        shutil.copyfile(src_csv, dest)

        # Contar filas
        count_rows = 0
        try:
            import pandas as pd  # noqa
            import pandas  # noqa
            df = pandas.read_csv(dest)
            count_rows = int(df.shape[0])
        except Exception as e:
            raise RuntimeError(f"CSV copiado pero no se pudo leer: {e}")

        if count_rows == 0:
            raise RuntimeError("CSV generado pero sin filas.")

        meta.update({
            "status": "done",
            "ended": now_utc_iso(),
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "count": count_rows,
            "_engine": "Binance_usdt_2",
            "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        })
        save_job(job_id, meta)

    except Exception as e:
        meta.update({
            "status": "error",
            "ended": now_utc_iso(),
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "error": str(e),
            "count": 0,
        })
        save_job(job_id, meta)
        print(f"❌ Job {job_id} error: {e}")

# ---------------------------
# HTML (sin f-strings: JS toma ?job= de la URL)
# ---------------------------
VIEW_HTML = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>USD Screener · Job</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Arial,sans-serif;margin:18px;color:#222}
  .muted{color:#666;font-size:12px}
  .pill{display:inline-block;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:600}
  .ok{background:#e6ffed;color:#036e1e;border:1px solid #9de2b0}
  .run{background:#fffbe6;color:#8a6d3b;border:1px solid #eed68a}
  .err{background:#ffecec;color:#b00020;border:1px solid #ff9aa2}
  .btn{color:#0b57d0;text-decoration:none}
  pre{background:#f6f8fa;border:1px solid #e5e7eb;border-radius:8px;padding:12px;overflow:auto}
  .row{margin:8px 0}
  .hidden{display:none}
</style>
</head>
<body>
  <div id="app">
    <div class="row"><strong id="title">Job</strong></div>
    <div class="row" id="statusLine">
      Estado: <span id="statusPill" class="pill run">cargando…</span>
      <span class="muted" id="times"></span>
      · <a id="jsonLink" class="btn" href="#" target="_blank" rel="noreferrer">Ver JSON</a>
      · <a id="csvLink" class="btn" href="#" download>Descargar CSV</a>
    </div>
    <div id="errorBox" class="row hidden"></div>
    <div class="row muted">Actualiza en unos segundos…</div>
    <div class="row muted" id="rowsInfo"></div>
  </div>

<script>
(function(){
  const qs = new URLSearchParams(window.location.search);
  const job = qs.get("job") || "";
  const title = document.getElementById("title");
  const pill = document.getElementById("statusPill");
  const times = document.getElementById("times");
  const jsonLink = document.getElementById("jsonLink");
  const csvLink = document.getElementById("csvLink");
  const errBox = document.getElementById("errorBox");
  const rowsInfo = document.getElementById("rowsInfo");

  function fmt(s){ return s ? s : ""; }
  function setPill(status){
    pill.textContent = status;
    pill.classList.remove("ok","run","err");
    if(status === "done") pill.classList.add("ok");
    else if(status === "error") pill.classList.add("err");
    else pill.classList.add("run");
  }

  function tick(){
    if(!job){ title.textContent = "Sin job"; return; }
    title.textContent = "Job " + job;
    fetch("/api?job=" + encodeURIComponent(job), {cache:"no-store"})
      .then(r => r.json())
      .then(data => {
        setPill(data.status || "running");
        times.textContent = "· inicio: " + fmt(data.started) + " · fin: " + fmt(data.ended || "");
        jsonLink.href = "/api?job=" + encodeURIComponent(job);
        csvLink.href = "/csv?job=" + encodeURIComponent(job);

        if (data.status === "done") {
          rowsInfo.textContent = "Filas: " + (data.count || 0);
        } else {
          rowsInfo.textContent = "";
        }

        if (data.status === "error") {
          errBox.classList.remove("hidden");
          errBox.innerHTML = '<pre>' + (data.error || "Error desconocido") + '</pre>';
        } else {
          errBox.classList.add("hidden");
          errBox.textContent = "";
        }
      })
      .catch(_ => {});
  }

  tick();
  setInterval(tick, 5000);
})();
</script>
</body>
</html>
"""

# ---------------------------
# Rutas
# ---------------------------
@app.after_request
def no_cache(resp: Response):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@app.get("/")
def index():
    # Crea un job nuevo y redirige a /view
    job_id = gen_job_id()
    t = threading.Thread(target=background_job, args=(job_id,), daemon=True)
    t.start()
    return redirect(f"/view?job={job_id}", code=302)

@app.get("/view")
def view():
    # HTML plano (no f-string) que lee ?job=... desde el propio JS
    resp = make_response(VIEW_HTML, 200)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return resp

@app.get("/api")
def api():
    job_id = request.args.get("job", "").strip()
    if not job_id:
        return jsonify({"error": "job requerido"}), 400
    meta = read_job(job_id)
    if not meta:
        return jsonify({"error": "job no encontrado", "job": job_id}), 404
    return jsonify(meta)

@app.get("/csv")
def csv_for_job():
    job_id = request.args.get("job", "").strip()
    if not job_id:
        return Response("job requerido", status=400)
    p = job_csv_path(job_id)
    if not p.exists():
        # Aún procesando
        return Response("CSV aún no disponible", status=202)
    return send_file(
        str(p),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"usdt_screener_{job_id}.csv",
        max_age=0,
        conditional=False,
        etag=False,
        last_modified=None,
    )

@app.get("/csv_tmp")
def csv_tmp():
    # Sirve el CSV global en /tmp si existe (o el *.csv más reciente)
    if CSV_TMP_PATH.exists() and CSV_TMP_PATH.stat().st_size > 0:
        p = CSV_TMP_PATH
    else:
        cands = sorted(Path("/tmp").glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
        p = cands[0] if cands else None
    if not p:
        return Response("No hay CSV en /tmp", status=404)
    return send_file(
        str(p),
        mimetype="text/csv",
        as_attachment=True,
        download_name=p.name,
        max_age=0,
        conditional=False,
        etag=False,
        last_modified=None,
    )

# ---------------------------
# WSGI (para gunicorn)
# ---------------------------
if __name__ == "__main__":
    # Útil en local
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True, threaded=True)
