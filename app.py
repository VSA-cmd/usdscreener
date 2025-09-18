from flask import Flask, jsonify, send_file
import subprocess, os

app = Flask(__name__)

@app.get("/")
def health():
    return {"ok": True, "service": "usdt screener"}

@app.get("/run")
def run_now():
    out_csv = "usdt_screener.csv"   # ajusta si tu script escribe otro nombre
    # ejecuta el screener
    r = subprocess.run(["python", "Binance_usdt_2.py"], capture_output=True, text=True)
    if r.returncode != 0:
        return jsonify({"error": "script failed", "stderr": r.stderr[-2000:]}), 500
    if not os.path.exists(out_csv):
        return jsonify({"error": f"{out_csv} not found"}), 500
    return send_file(out_csv, mimetype="text/csv")
