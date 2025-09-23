# app.py
import os
import sys
import json
import time
import uuid
import shutil
import threading
import subprocess
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from flask import Flask, redirect, request, jsonify, send_file, url_for, Response, make_response

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "150"))
MAX_WORKERS   = int(os.getenv("MAX_WORKERS", "12"))
BUDGET_SEC    = int(os.getenv("BUDGET", "110"))

CSV_TMP_PATH  = Path("/tmp/usdt_screener.csv")
JOBS_DIR      = Path("/tmp/jobs")
JOBS_DIR.mkdir(parents=True, exist_ok=True)

ENGINE_CANDIDATES = ["Binance_usdt_2", "Binance_usdt"]

# ────────────────────────────────────────────────────────────────────────────────
# Carga de motor (no obligatorio para fallback)
# ────────────────────────────────────────────────────────────────────────────────
engine = None
engine_name: Optional[str] = None
for mod in ENGINE_CANDIDATES:
    try:
        engine = __import__(mod)
        engine_name = mod
        break
    except Exception:
        continue

# ────────────────────────────────────────────────────────────────────────────────
# App
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

JOBS_LOCK = threading.Lock()
JOBS: Dict[str, Dict[str, Any]] = {}

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def job_json_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"

def job_csv_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"

def save_job(job_id: str, payload: Dict[str, Any]) -> None:
    payload["_engine"] = engine_name
    payload["_updated_utc"] = now_utc_iso()
    job_json_path(job_id).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with JOBS_LOCK:
        JOBS[job_id] = payload

def load_job(job_id: str) -> Optional[Dict[str, Any]]:
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

# ────────────────────────────────────────────────────────────────────────────────
# Ejecución del motor
# ────────────────────────────────────────────────────────────────────────────────
def _call_engine_by_reflection() -> None:
    """
    Intenta invocar funciones típicas si el módulo se importa.
    Si ninguna está presente, lanza RuntimeError para que el caller haga fallback.
    """
    if engine is None:
        raise RuntimeError("No se pudo importar el motor (Binance_usdt_2 / Binance_usdt).")

    import inspect

    tried = []
    preferred = [
        "run_screener", "run", "main", "screener", "start", "execute",
    ]
    # También descubrimos funciones que contengan estas palabras
    dynamic = []
    for name in dir(engine):
        if name.startswith("_"):
            continue
        obj = getattr(engine, name)
        if callable(obj) and any(k in name.lower() for k in ("run", "main", "screener", "start", "exec")):
            dynamic.append(name)

    candidates = []
    # mantén orden y unicidad
    for n in preferred + dynamic:
        if n not in candidates and hasattr(engine, n) and callable(getattr(engine, n)):
            candidates.append(n)

    if not candidates:
        raise RuntimeError(f"No encontré una función de entrada usable en el motor. Probé: {tried}")

    # intentamos pasar sólo los kwargs aceptados por la firma
    KW = {
        "LIMIT_SYMBOLS": LIMIT_SYMBOLS,
        "MAX_WORKERS": MAX_WORKERS,
        "BUDGET": BUDGET_SEC,
        # variantes en minúsculas por si las usan
        "limit_symbols": LIMIT_SYMBOLS,
        "max_workers": MAX_WORKERS,
        "budget": BUDGET_SEC,
    }

    last_err = None
    for name in candidates:
        fn = getattr(engine, name)
        tried.append(name)
        try:
            sig = inspect.signature(fn)
            kwargs = {}
            for p in sig.parameters.values():
                if p.kind in (p.POSITIONAL_ONLY, p.VAR_POSITIONAL):
                    continue
                if p.name in KW:
                    kwargs[p.name] = KW[p.name]
            # preferimos kwargs; si ninguno coincide, llamamos sin args
            if kwargs:
                fn(**kwargs)  # type: ignore
            else:
                fn()  # type: ignore
            return
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(
        f"No pude ejecutar ninguna función del motor. Intentos={tried}. Último error: {last_err}"
    )

def _call_engine_as_script() -> None:
    """
    Ejecuta el motor como script con subprocess y espera a que termine.
    Prioriza `python -m Binance_usdt_2` y `python Binance_usdt_2.py`.
    Propaga LIMIT_SYMBOLS/MAX_WORKERS/BUDGET por variables de entorno.
    """
    env = os.environ.copy()
    env.update({
        "LIMIT_SYMBOLS": str(LIMIT_SYMBOLS),
        "MAX_WORKERS": str(MAX_WORKERS),
        "BUDGET": str(BUDGET_SEC),
        "PYTHONUNBUFFERED": "1",
    })

    cmd_variants = []

    # Si conocemos el nombre importable, primero prueba como módulo
    if engine_name:
        cmd_variants.append([sys.executable, "-u", "-m", engine_name])

    # Después probamos posibles rutas de archivo
    project_dir = Path(__file__).resolve().parent
    for base in ENGINE_CANDIDATES:
        py = project_dir / f"{base}.py"
        if py.exists():
            cmd_variants.append([sys.executable, "-u", str(py)])

    if not cmd_variants:
        raise RuntimeError("No encontré archivos del motor (Binance_usdt_2.py / Binance_usdt.py) en el proyecto.")

    last_rc = None
    last_out = None
    last_err = None

    for cmd in cmd_variants:
        try:
            print(f"→ Ejecutando motor como script: {' '.join(cmd)}")
            cp = subprocess.run(
                cmd,
                env=env,
                cwd=str(project_dir),
                capture_output=True,
                text=True,
                timeout=max(BUDGET_SEC + 90, 180),   # margen de seguridad
            )
            last_rc, last_out, last_err = cp.returncode, cp.stdout, cp.stderr
            print(last_out or "", end="")
            if last_err:
                print(last_err, file=sys.stderr, end="")
            if cp.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            raise RuntimeError("El motor excedió el tiempo máximo de ejecución (timeout).")

    raise RuntimeError(f"El motor terminó con código {last_rc}. STDERR: {last_err}")

def call_engine_and_wait() -> None:
    """
    Deja el CSV en /tmp/usdt_screener.csv (o donde el motor lo genere).
    Intenta reflexión; si no, fallback a script.
    """
    # 1) Intento por reflexión si el import funcionó
    if engine is not None:
        try:
            _call_engine_by_reflection()
        except RuntimeError:
            # Si falla por “no hay funciones”, probamos como script
            _call_engine_as_script()
    else:
        # Si no se pudo importar, vamos directo a script
        _call_engine_as_script()

# ────────────────────────────────────────────────────────────────────────────────
# Job runner
# ────────────────────────────────────────────────────────────────────────────────
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
        call_engine_and_wait()

        # Copiamos CSV global (si existe) a la carpeta del job
        if CSV_TMP_PATH.exists():
            shutil.copyfile(CSV_TMP_PATH, job_csv_path(job_id))
        else:
            # si el motor escribe en otro nombre, intenta detectarlo en /tmp
            for cand in Path("/tmp").glob("*.csv"):
                try:
                    shutil.copyfile(cand, job_csv_path(job_id))
                    break
                except Exception:
                    pass

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
