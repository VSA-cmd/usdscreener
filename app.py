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

CSV_TMP_PATH  = Path("/tmp/usdt_screener.csv")         # donde el motor guarda “siempre”
JOBS_DIR      = Path("/tmp/jobs")                      # persistimos estado por job
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

# Estructuras en memoria (por worker). Persistimos a disco para compartir entre workers.
JOBS_LOCK = threading.Lock()
JOBS = {}  # job_id -> dict en memoria (opcional, la verdad mandamos todo a disco)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")


def job_json_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def job_csv_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.csv"


def save_job(job_id: str, payload: dict) -> None:
    """Persistimos a disco (para que otros workers vean el estado)."""
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
    Intentamos ejecutar el motor sin suponer nombres exactos.
    Convención: el motor escribe /tmp/usdt_screener.csv.
    """
    if engine is None:
        raise RuntimeError("No se pudo importar el motor (Binance_usdt_2 / Binance_usdt).")

    # Probamos varias firmas. Si alguna existe, la usamos.
    tried = []

    # 1) run_screener(LIMIT_SYMBOLS, MAX_WORKERS, BUDGET)
    if hasattr(engine, "run_screener"):
        tried.append("run_screener")
        return engine.run_screener(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore

    # 2) run(**kwargs)
    if hasattr(engine, "run"):
        tried.append("run")
        return engine.run(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore

    # 3) main(**kwargs) o main() a secas
    if hasattr(engine, "main"):
        tried.append("main")
        try:
            return engine.main(LIMIT_SYMBOLS=LIMIT_SYMBOLS, MAX_WORKERS=MAX_WORKERS, BUDGET=BUDGET_SEC)  # type: ignore
        except TypeError:
            return engine.main()  # type: ignore

    # 4) fallback: screener(), start(), etc.
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
        # Ejecutamos el motor (debería dejar el CSV en CSV_TMP_PATH)
        call_engine_and_wait()

        # Si existe el CSV temporal del motor, lo copiamos y lo usamos como fuente de verdad por job
        if CSV_TMP_PATH.exists():
            # Copia per-job
            shutil.copyfile(CSV_TMP_PATH, job_csv_path(job_id))
        else:
            # No hay CSV → seguimos, pero lo registramos
            print("⚠️  El motor terminó pero no encontré /tmp/usdt_screener.csv")

        # Cargamos filas si hay CSV (aunque sea parcial)
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
        # Si hay CSV parcial, igual lo exponemos
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
    # Creamos un JSON inicial para que /view lo encuentre de inmediato
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
    # Cada visita dispara un job nuevo y redirige al viewer
    job_id = start_new_job()
    return redirect(url_for("view_job", job=job_id), code=302)


@app.get("/view")
def view_job():
    # Importante: NO 404 aunque no exista todavía el JSON. El frontend consulta /api.
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
        <div class="row">Estado: ${badge}
          · inicio: ${fmt(data.started)} · fin: ${fmt(data.ended || '')}
          · <a href="/api?job=${jobId}" target="_blank">Ver JSON</a>
