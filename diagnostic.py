#!/usr/bin/env python3
"""
diagnostic.py — Lance ce script dans le dossier mobility-copilot/
    python diagnostic.py
"""
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

print(f"\n📁 Dossier data : {DATA_DIR}")
print(f"   Existe : {os.path.exists(DATA_DIR)}")
if os.path.exists(DATA_DIR):
    print(f"   Contenu : {os.listdir(DATA_DIR)}\n")
else:
    print("   ❌ Le dossier data/ n'existe pas !\n")

files = {
    "311.csv":        ("utf-8-sig", "DATE_CREATION"),
    "collisions.csv": ("latin1",    "DT_ACCDN"),
    "stops.txt":      ("utf-8",     "stop_lat"),
    "routes.txt":     ("utf-8",     "route_id"),
    "meteo.csv":      ("utf-8-sig", "LOCAL_DATE"),
}

for filename, (enc, expected_col) in files.items():
    path = os.path.join(DATA_DIR, filename)
    print(f"=== {filename} ===")
    if not os.path.exists(path):
        print(f"  ❌ FICHIER ABSENT : {path}")
        print()
        continue

    size_mb = os.path.getsize(path) / 1_000_000
    print(f"  ✅ Présent ({size_mb:.1f} MB)")

    # Essayer plusieurs encodages
    df = None
    for e in [enc, "utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(path, nrows=3, low_memory=False, encoding=e)
            print(f"  ✅ Encodage : {e}")
            break
        except Exception as ex:
            continue

    if df is None:
        print(f"  ❌ Impossible de lire avec aucun encodage")
    else:
        print(f"  📋 TOUTES les colonnes ({len(df.columns)}) :")
        for i, col in enumerate(df.columns):
            print(f"      {i+1:2}. {col}")
        if expected_col in df.columns:
            print(f"  ✅ Colonne '{expected_col}' trouvée")
        else:
            print(f"  ⚠️  Colonne '{expected_col}' ABSENTE — voir liste ci-dessus")
    print()
