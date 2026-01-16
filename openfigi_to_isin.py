


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
openfigi_to_isin.py
Récupère l'ISIN à partir d'un OpenFIGI (FIGI) via l'API OpenFIGI.

Prérequis:
    pip install requests pandas

Auth:
    OPENFIGI_API_KEY  (variable d'environnement)
    -> Optionnellement, un module FIGI_KEY.py peut exister avec une variable API_KEY = "......"
       mais la variable d'environnement reste prioritaire.

Usages:
    # 1) FIGI unique
    python openfigi_to_isin.py --figi BBG000B9XRY4

    # 2) Fichier CSV en entrée (colonne 'figi' par défaut), crée un CSV de sortie
    python openfigi_to_isin.py --csv input_figi.csv --out output_isin.csv

    # 3) JSONL entrée (une ligne = dict avec 'idType','idValue'), utile pour formats avancés
    python openfigi_to_isin.py --jsonl requests.jsonl --out output_isin.csv

    # Paramètre de batch (par défaut 10) + traces
    python openfigi_to_isin.py --csv input.csv --out out.csv --batch-size 10 --verbose
"""

import os
import sys
import time
import json
import argparse
from typing import List, Dict, Any, Optional

# --- Optionnel: module FIGI_KEY (fallback)
FIGI_KEY_FROM_MODULE: Optional[str] = None
try:
    import FIGI_KEY  # facultatif
    FIGI_KEY_FROM_MODULE = getattr(FIGI_KEY, "API_KEY", None)
except Exception:
    pass

# --- Dépendances externes
try:
    import requests
except ImportError:
    print("Le module 'requests' est requis. Installez-le: pip install requests", file=sys.stderr)
    sys.exit(1)

# pandas est facultatif (pour le mode CSV)
try:
    import pandas as pd
except Exception:
    pd = None

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# ----------------------------- Utilitaires -----------------------------
def get_api_key() -> Optional[str]:
    """Récupère la clé API depuis l'env, ou à défaut depuis FIGI_KEY.API_KEY."""
    return os.getenv("OPENFIGI_API_KEY") or FIGI_KEY_FROM_MODULE

def chunked(seq: List[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def build_headers(api_key: Optional[str]) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    return headers

# ------------------------- Appel API (bas niveau) ----------------------
def map_figi_batch(payload: List[Dict[str, Any]], api_key: Optional[str],
                   retry: int = 3, backoff: float = 2.0, verbose: bool = False):
    """
    Envoie un batch de 'mapping jobs' à OpenFIGI.
    - Gère le throttling 429 par retry/backoff
    - Remonte une RuntimeError pour les autres codes (dont 413)
    """
    headers = build_headers(api_key)

    for attempt in range(1, retry + 1):
        if verbose:
            print(f"[INFO] POST {OPENFIGI_URL} (jobs={len(payload)}, attempt={attempt})")
        resp = requests.post(OPENFIGI_URL, headers=headers, data=json.dumps(payload), timeout=30)

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            # Rate limit: on attend puis on retente
            sleep_for = backoff * attempt
            if verbose:
                print(f"[WARN] 429 Too Many Requests - sleep {sleep_for:.1f}s puis retry…")
            time.sleep(sleep_for)
            continue
        elif resp.status_code == 413:
            # Taille de batch trop grande: on remonte une erreur explicite pour que l'appelant réduise le batch
            raise RuntimeError(f"HTTP 413: Request too large or too many jobs (limit atteinte).")
        else:
            # Erreur autre
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise RuntimeError(f"Erreur API OpenFIGI (HTTP {resp.status_code}): {detail}")

    raise RuntimeError("Trop de tentatives (throttling 429). Réessayez plus tard ou réduisez le débit.")

# ----------------------- Appel API (haut niveau) -----------------------
def single_figi_request(figi: str, api_key: Optional[str]) -> Dict[str, Any]:
    payload = [{"idType": "ID_BB_GLOBAL", "idValue": figi}]
    result = map_figi_batch(payload, api_key)
    # result est une liste alignée sur le payload; chaque entrée a "data" ou "error"
    if not result or len(result) != 1:
        return {"figi": figi, "error": "Réponse inattendue de l'API"}
    entry = result[0]
    if "error" in entry and entry["error"]:
        return {"figi": figi, "error": entry["error"]}
    data = entry.get("data") or []
    if not data:
        return {"figi": figi, "error": "Aucune correspondance"}
    # On prend la première correspondance (OpenFIGI peut en donner plusieurs)
    top = data[0]
    return {
        "figi": figi,
        "isin": top.get("isin"),
        "name": top.get("name"),
        "ticker": top.get("ticker"),
        "exchCode": top.get("exchCode"),
        "marketSector": top.get("marketSector"),
        "error": None
    }

# ----------------------------- CSV mode --------------------------------
def _map_with_fallback(payloads: List[Dict[str, Any]],
                       api_key: Optional[str],
                       max_batch: int,
                       verbose: bool) -> List[Dict[str, Any]]:
    """
    Envoie la liste de jobs avec chunking, et fallback automatique si 413:
      - essaie avec max_batch
      - si 413 -> réduit par 2 et réessaie (jusqu'à 1)
    Retourne la liste des réponses (alignée avec payloads).
    """
    responses: List[Dict[str, Any]] = []
    current_batch_size = max(1, max_batch)

    i = 0
    while i < len(payloads):
        # Tant que le batch courant ne passe pas (413), on diminue
        local_batch_size = current_batch_size
        while True:
            batch = payloads[i:i + local_batch_size]
            try:
                resp = map_figi_batch(batch, api_key, verbose=verbose)
                # succès -> on agrège et on sort de la boucle interne
                if len(resp) != len(batch):
                    raise RuntimeError("Taille de réponse inattendue (désalignement batch).")
                responses.extend(resp)
                if verbose:
                    print(f"[INFO] Batch OK ({len(batch)} jobs).")
                break
            except RuntimeError as e:
                msg = str(e)
                if "HTTP 413" in msg and local_batch_size > 1:
                    new_size = max(1, local_batch_size // 2)
                    if verbose:
                        print(f"[WARN] 413 reçu: réduction du batch {local_batch_size} → {new_size} et retry…")
                    local_batch_size = new_size
                    continue
                # autre erreur -> on propage
                raise

        # avance l'index global du nombre de jobs effectivement traités
        i += len(batch)

        # Optionnel: si on a réduit localement, on garde cette taille pour la suite
        current_batch_size = local_batch_size

    return responses

def csv_mode(input_csv: str, output_csv: str, column: str,
             api_key: Optional[str], max_batch: int, verbose: bool) -> None:
    if pd is None:
        raise RuntimeError("Le mode CSV requiert pandas. Installez-le: pip install pandas")

    df = pd.read_csv(input_csv)
    if column not in df.columns:
        raise ValueError(f"La colonne '{column}' est introuvable dans {input_csv}. Colonnes: {list(df.columns)}")

    requests_payload = [{"idType": "ID_BB_GLOBAL", "idValue": str(v)} for v in df[column].astype(str).tolist()]
    if verbose:
        print(f"[INFO] Total FIGI à traiter: {len(requests_payload)} | batch-size initial: {max_batch}")

    responses = _map_with_fallback(requests_payload, api_key, max_batch, verbose)

    # Assemblage des résultats
    results = []
    for req, res in zip(requests_payload, responses):
        figi_value = req["idValue"]
        if "error" in res and res["error"]:
            results.append({"figi": figi_value, "isin": None, "name": None, "ticker": None,
                            "exchCode": None, "marketSector": None, "error": res["error"]})
            continue
        data = res.get("data") or []
        if not data:
            results.append({"figi": figi_value, "isin": None, "name": None, "ticker": None,
                            "exchCode": None, "marketSector": None, "error": "Aucune correspondance"})
            continue
        top = data[0]
        results.append({
            "figi": figi_value,
            "isin": top.get("isin"),
            "name": top.get("name"),
            "ticker": top.get("ticker"),
            "exchCode": top.get("exchCode"),
            "marketSector": top.get("marketSector"),
            "error": None
        })

    out_df = pd.DataFrame(results, columns=["figi", "isin", "name", "ticker", "exchCode", "marketSector", "error"])
    out_df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Résultats écrits dans: {output_csv}")

# ---------------------------- JSONL mode -------------------------------
def jsonl_mode(input_jsonl: str, output_csv: str,
               api_key: Optional[str], max_batch: int, verbose: bool) -> None:
    """
    Mode avancé: chaque ligne de input_jsonl doit contenir un objet JSON représentant une requête OpenFIGI,
    par ex. {"idType":"ID_BB_GLOBAL","idValue":"BBG000B9XRY4"} ou {"idType":"ID_CUSIP","idValue":"037833100"}.
    """
    # Lecture
    payloads = []
    with open(input_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "idType" not in obj or "idValue" not in obj:
                raise ValueError("Chaque ligne JSONL doit contenir 'idType' et 'idValue'.")
            payloads.append(obj)

    if verbose:
        print(f"[INFO] Total jobs à traiter: {len(payloads)} | batch-size initial: {max_batch}")

    responses = _map_with_fallback(payloads, api_key, max_batch, verbose)

    # Écriture CSV
    import csv
    fieldnames = ["idValue", "isin", "name", "ticker", "exchCode", "marketSector", "error"]
    with open(output_csv, "w", encoding="utf-8", newline="") as fw:
        writer = csv.DictWriter(fw, fieldnames=fieldnames)
        writer.writeheader()

        for req, res in zip(payloads, responses):
            id_value = req["idValue"]
            if "error" in res and res["error"]:
                writer.writerow({"idValue": id_value, "isin": None, "name": None, "ticker": None,
                                 "exchCode": None, "marketSector": None, "error": res["error"]})
                continue
            data = res.get("data") or []
            if not data:
                writer.writerow({"idValue": id_value, "isin": None, "name": None, "ticker": None,
                                 "exchCode": None, "marketSector": None, "error": "Aucune correspondance"})
                continue
            top = data[0]
            writer.writerow({
                "idValue": id_value,
                "isin": top.get("isin"),
                "name": top.get("name"),
                "ticker": top.get("ticker"),
                "exchCode": top.get("exchCode"),
                "marketSector": top.get("marketSector"),
                "error": None
            })

    print(f"Résultats écrits dans: {output_csv}")

# ------------------------------- Main ----------------------------------
def main():
    parser = argparse.ArgumentParser(description="OpenFIGI → ISIN (via API)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--figi", help="FIGI unique (ex: BBG000B9XRY4)")
    group.add_argument("--csv", help="Fichier CSV contenant une colonne de FIGI (par défaut 'figi')")
    group.add_argument("--jsonl", help="Fichier JSONL de requêtes (idType/idValue par ligne)")

    parser.add_argument("--column", default="figi", help="Nom de la colonne FIGI dans le CSV (défaut: figi)")
    parser.add_argument("--out", default="openfigi_isin_output.csv", help="Fichier CSV de sortie")

    # Nouveaux paramètres
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Nb max de mapping jobs par requête (défaut: 10)")
    parser.add_argument("--verbose", action="store_true", help="Affiche des logs d'exécution détaillés")

    args = parser.parse_args()
    api_key = get_api_key()

    try:
        if args.figi:
            res = single_figi_request(args.figi.strip(), api_key)
            if res.get("error"):
                print(f"Erreur: {res['error']}", file=sys.stderr)
                sys.exit(2)
            print(json.dumps(res, ensure_ascii=False, indent=2))

        elif args.csv:
            csv_mode(args.csv, args.out, args.column, api_key, args.batch_size, args.verbose)

        elif args.jsonl:
            jsonl_mode(args.jsonl, args.out, api_key, args.batch_size, args.verbose)

    except Exception as e:
        print(f"Erreur: {e}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()


