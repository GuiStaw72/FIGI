


import requests
import pandas as pd
import csv

# Charger les FIGI depuis le fichier Excel
df = pd.read_excel("FIGICLO.xlsx", sheet_name="ListeFIGI", engine="openpyxl")
figi_list = df['FIGI'].dropna().tolist()

# Clé API OpenFIGI (à remplacer par votre propre clé)
API_KEY = '6ff848ae-85fa-472e-9e59-a37f6f3ff963'

headers = {
    'Content-Type': 'application/json',
    'X-OPENFIGI-APIKEY': API_KEY
}

# Préparation des requêtes
jobs = [{'idType': 'ID_BB_GLOBAL', 'idValue': figi} for figi in figi_list]


# Envoi des requêtes par lots de 100 (limite API)
results_all = []
for i in range(0, len(jobs), 100):
    batch = jobs[i:i+100]
    response = requests.post('https://api.openfigi.com/v3/mapping', headers=headers, json=batch)
    if response.status_code == 200:
        results_all.extend(response.json())
    else:
        print(f"Erreur API : {response.status_code} - {response.text}")
        results_all.extend([{}] * len(batch))

# Écriture dans un fichier CSV
with open('figi_metadata.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['FIGI', 'Issuer', 'Name', 'Security Type', 'Ticker', 'Exchange Code'])
    for figi, result in zip(figi_list, results_all):
        if 'data' in result:
            for item in result['data']:

                     writer.writerow([
                    figi,
                    item.get('issuer', 'N/A'),
                    item.get('name', 'N/A'),
                    item.get('securityType', 'N/A'),
                    item.get('ticker', 'N/A'),
                    item.get('exchCode', item.get('micCode', 'N/A'))
                ])
        else:
            writer.writerow([figi, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'])

print("✅ Le fichier figi_metadata.csv a été généré avec les métadonnées extraites depuis FIGICLO.xlsx.")

