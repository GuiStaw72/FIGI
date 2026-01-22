
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ISIN -> Ticker via OpenFIGI (mapping API)
- Gère le traitement en masse, la limitation de débit et l'export CSV/Excel.
- Priorise des résultats "Equity" et peut favoriser certains échanges (exchCode).
"""

from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("OPENFIGI_API_KEY")


import os
import time
import pandas as pd
import requests
from typing import List, Optional, Dict
# Clé API OpenFIGI (à remplacer par votre propre clé)
##API_KEY = '6ff848ae-85fa-472e-9e59-a37f6f3ff963'

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"  # Endpoint officiel de mapping

def chunk(lst: List[str], size: int):
    """Découpe une liste en blocs de taille `size`."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def make_mapping_jobs(isins: List[str], mic: Optional[str] = None, exch_code: Optional[str] = None) -> List[Dict]:
    """
    Crée la liste de 'jobs' pour OpenFIGI.
    Vous pouvez contraindre la recherche par exchange code (exchCode) ou MIC (micCode).
    """
    jobs = []
    for isin in isins:
        job = {"idType": "ID_ISIN", "idValue": isin}
        if mic:
            job["micCode"] = mic  # Paramètre supporté par les clients/wrappers OpenFIGI
        if exch_code:
            job["exchCode"] = exch_code
        jobs.append(job)
    return jobs

def call_openfigi(jobs: List[Dict], api_key: Optional[str] = None) -> List[Dict]:
    """
    Envoie une requête POST vers /v3/mapping et gère un 429 (rate-limit) simple
    via l'en-tête 'ratelimit-reset' quand il est présent.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    resp = requests.post(OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
    if resp.status_code == 429:
        # Attente basée sur l'en-tête de reset si disponible, sinon 5s
        reset_sec = float(resp.headers.get("ratelimit-reset", "5"))
        time.sleep(reset_sec + 0.5)
        resp = requests.post(OPENFIGI_URL, json=jobs, headers=headers, timeout=30)

    resp.raise_for_status()
    return resp.json()

def select_best_result(results: List[Dict], preferred_exch: Optional[List[str]] = None) -> Optional[Dict]:
    """Sélectionne le 'meilleur' enregistrement parmi plusieurs data renvoyées pour un même ISIN."""
    if not results:
        return None

    def score(r: Dict) -> int:
        s = 0
        if r.get("marketSector") == "Equity":
            s += 1
        if preferred_exch and r.get("exchCode") in preferred_exch:
            s += 2
        if r.get("compositeFIGI"):
            s += 1
        return s

    return sorted(results, key=score, reverse=True)[0]

def map_isins_to_tickers(
    isins: List[str],
    api_key: Optional[str] = None,
    preferred_exch: Optional[List[str]] = None,
    mic: Optional[str] = None,
    exch_code: Optional[str] = None,
    batch_size: Optional[int] = None,
) -> pd.DataFrame:
    """Effectue le mapping ISIN -> ticker et retourne un DataFrame consolidé."""
    isins = [s.strip() for s in isins if isinstance(s, str) and s.strip()]

    # Taille de paquet: 100 avec clé API, 10 sans (spécifications OpenFIGI)
    if batch_size is None:
        batch_size = 100 if api_key else 10

    rows = []
    for isins_block in chunk(isins, batch_size):
        jobs = make_mapping_jobs(isins_block, mic=mic, exch_code=exch_code)
        response = call_openfigi(jobs, api_key=api_key)

        # Chaque élément de response correspond à un job
        for req_job, job_result in zip(jobs, response):
            isin = req_job["idValue"]
            entry = {
                "ISIN": isin,
                "ticker": None,
                "exchCode": None,
                "name": None,
                "figi": None,
                "shareClassFIGI": None,
                "compositeFIGI": None,
                "marketSector": None,
                "securityType": None,
                "error": None,
            }

            if "error" in job_result:
                entry["error"] = job_result.get("error")
            else:
                best = select_best_result(job_result.get("data", []), preferred_exch=preferred_exch)
                if best:
                    entry.update({
                        "ticker": best.get("ticker"),
                        "exchCode": best.get("exchCode"),
                        "name": best.get("name"),
                        "figi": best.get("figi"),
                        "shareClassFIGI": best.get("shareClassFIGI"),
                        "compositeFIGI": best.get("compositeFIGI"),
                        "marketSector": best.get("marketSector"),
                        "securityType": best.get("securityType"),
                    })
            rows.append(entry)

    return pd.DataFrame(rows)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ISIN → Ticker via OpenFIGI")
    parser.add_argument("--input", help="Chemin vers un Excel (.xlsx) ou CSV contenant une colonne ISIN")
    parser.add_argument("--column", default="ISIN", help="Nom de la colonne des ISIN (défaut: ISIN)")
    parser.add_argument("--output", default="isin_to_ticker.xlsx", help="Fichier de sortie (.xlsx ou .csv)")
    parser.add_argument("--api-key", default=os.getenv("OPENFIGI_API_KEY"), help="Clé API OpenFIGI (ou variable env OPENFIGI_API_KEY)")
    parser.add_argument("--preferred-exch", nargs="*", help="Codes d'échanges à privilégier (ex: US LN FR)")
    parser.add_argument("--mic", help="MIC ISO 10383 à utiliser pour filtrer la requête (ex: XPAR, XLON)")
    parser.add_argument("--exch-code", help="Exchange code OpenFIGI à utiliser pour filtrer la requête (ex: US, LN)")
    parser.add_argument("--batch-size", type=int, help="Taille de paquet (défaut: 100 avec clé, 10 sans clé)")
    args = parser.parse_args()

    # Lecture des ISIN
    if not args.input:
        # Démo : remplacez par votre fichier ou passez --input
        isins = ["US0378331005", "US5949181045"]
    else:
        if args.input.lower().endswith(".xlsx"):
            isins = pd.read_excel(args.input, engine="openpyxl")[args.column].dropna().tolist()
        else:
            isins = pd.read_csv(args.input)[args.column].dropna().tolist()

    df = map_isins_to_tickers(
        isins,
       # api_key=args.api_key,
        api_key=API_KEY,
        preferred_exch=args.preferred_exch,
        mic=args.mic,
        exch_code=args.exch_code,
        batch_size=args.batch_size,
    )

    # Export
    if args.output.lower().endswith(".xlsx"):
        df.to_excel(args.output, index=False, engine="openpyxl")
    else:
        df.to_csv(args.output, index=False)

    print(f"✅ {len(df)} lignes sauvegardées dans {args.output}")
    print(df.head(10).to_string(index=False))
