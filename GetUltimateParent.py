
import os
import sys
import socket
import csv
import time

import requests
import pandas as pd
from dotenv import load_dotenv

# --- 0) Boot ---
print("BOOT 1", flush=True)

# --- 1) Charger .env ---
load_dotenv()
print("Python:", sys.version, flush=True)
print("Dossier courant:", os.getcwd(), flush=True)
print("OPENFIGI_API_KEY présent ?", bool(os.getenv("OPENFIGI_API_KEY")), flush=True)
print("Nom de machine:", socket.gethostname(), flush=True)

API_KEY = os.getenv("OPENFIGI_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENFIGI_API_KEY manquante (.env). Copie .env.example en .env puis renseigne ta clé.")

headers = {
    "Content-Type": "application/json",
    "X-OPENFIGI-APIKEY": API_KEY,
}

# --- 2) Lecture Excel (optimisée + logs) ---
print("Lecture Excel: FIGICLO.xlsx / feuille 'ListeFIGI' / usecols=['FIGI'] ...", flush=True)
df = pd.read_excel(
    "FIGICLO.xlsx",
    sheet_name="ListeFIGI",
    usecols=["FIGI"],
    dtype={"FIGI": str},
    engine="openpyxl",
)
figi_list = df["FIGI"].dropna().astype(str).tolist()
print(f"FIGI détectés: {len(figi_list)}", flush=True)

if not figi_list:
    print("Aucun FIGI trouvé. Arrêt propre.", flush=True)
    sys.exit(0)

# --- 3) Préparer jobs (smoke test: limiter à 3 pour valider le flux réseau) ---
figi_list = figi_list[:3]  # <<< retire cette ligne quand tout est OK
jobs = [{"idType": "ID_BB_GLOBAL", "idValue": figi} for figi in figi_list]

# --- 4) Appel API avec timeout + petit retry ---
session = requests.Session()
session.headers.update(headers)

def post_with_retry(url, payload, timeout=15, max_retries=1):
    attempt = 0
    while True:
        attempt += 1
        try:
            return session.post(url, json=payload, timeout=timeout)
        except requests.Timeout:
            if attempt <= max_retries:
                print(f"⏳ Timeout ({timeout}s) → retry {attempt}/{max_retries}...", flush=True)
                time.sleep(2 ** attempt)
                continue
            raise

results_all = []
batch_size = 100
total = len(jobs)
for i in range(0, total, batch_size):
    batch = jobs[i:i + batch_size]
    print(f"🔄 Envoi lot {i // batch_size + 1}/{(total + batch_size - 1)//batch_size} | taille={len(batch)}", flush=True)
    try:
        resp = post_with_retry("https://api.openfigi.com/v3/mapping", payload=batch, timeout=15, max_retries=1)
    except Exception as e:
        print(f"❌ Échec appel API: {e}", flush=True)
        results_all.extend([{}] * len(batch))
        continue

    print("HTTP:", resp.status_code, flush=True)
    if 200 <= resp.status_code < 300:
        try:
            results_all.extend(resp.json())
        except ValueError:
            print("❌ Réponse non-JSON", flush=True)
            results_all.extend([{}] * len(batch))
    else:
        print(f"❌ Erreur API: {resp.status_code} - {resp.text[:200]}", flush=True)
        results_all.extend([{}] * len(batch))

# --- 5) Écriture CSV ---
with open("figi_metadata.csv", mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["FIGI", "Issuer", "Name", "Security Type", "Ticker", "Exchange Code"])
    for figi, result in zip(figi_list, results_all):
        if isinstance(result, dict) and "data" in result:
            for item in result["data"]:
                writer.writerow([
                    figi,
                    item.get("issuer", "N/A"),
                    item.get("name", "N/A"),
                    item.get("securityType", "N/A"),
                    item.get("ticker", "N/A"),
                    item.get("exchCode", item.get("micCode", "N/A")),
                ])
        else:
            writer.writerow([figi, "N/A", "N/A", "N/A", "N/A", "N/A"])

print("✅ Terminé. Fichier: figi_metadata.csv", flush=True)



