#!/usr/bin/env python3
"""
download_data.py
────────────────────────────────────────────────────────────────────────────
Lance ce script UNE SEULE FOIS pour télécharger les vrais datasets.
Ensuite l'app les charge automatiquement depuis le dossier data/.

    python download_data.py

────────────────────────────────────────────────────────────────────────────
"""

import os, sys, requests, zipfile, io
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

TIMEOUT = 60
HEADERS = {"User-Agent": "MobilityCopilot/1.0"}


def _download(url, dest, label, chunk_mb=10):
    print(f"\n📥 {label}")
    print(f"   URL : {url}")
    print(f"   → {dest}")
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT, headers=HEADERS) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(f"   {pct:.0f}% ({downloaded // 1_000_000} MB / {total // 1_000_000} MB)", end="\r")
        size_mb = os.path.getsize(dest) / 1_000_000
        print(f"\n   ✅ Téléchargé : {size_mb:.1f} MB")
        return True
    except Exception as e:
        print(f"\n   ❌ Échec : {e}")
        if os.path.exists(dest):
            os.remove(dest)
        return False


def download_311():
    """
    Requêtes 311 — CSV complet (2022 à ce jour, ~673 MB).
    Source : https://donnees.montreal.ca/dataset/requete-311
    """
    dest = os.path.join(DATA_DIR, "311.csv")
    if os.path.exists(dest):
        size = os.path.getsize(dest) / 1_000_000
        print(f"⏭  311.csv déjà présent ({size:.0f} MB) — ignoré.")
        return True

    # URL de téléchargement direct du CSV (resource_id du portail CKAN Montréal)
    url = "https://donnees.montreal.ca/dataset/requete-311/resource/9a2e0603-5f1e-4cfa-a07e-1d39e2e9a6e1/download/requetes-311.csv"
    ok = _download(url, dest, "Requêtes 311 (CSV, ~673 MB)")
    if not ok:
        # Fallback via API CKAN (échantillon 5000 lignes)
        print("   ↩  Tentative via API CKAN (5000 lignes)...")
        api_url = "https://donnees.montreal.ca/api/3/action/datastore_search?resource_id=9a2e0603-5f1e-4cfa-a07e-1d39e2e9a6e1&limit=5000"
        try:
            resp = requests.get(api_url, timeout=TIMEOUT, headers=HEADERS)
            records = resp.json().get("result", {}).get("records", [])
            if records:
                import pandas as pd
                pd.DataFrame(records).to_csv(dest, index=False, encoding="utf-8-sig")
                print(f"   ✅ {len(records)} lignes via API CKAN")
                return True
        except Exception as e:
            print(f"   ❌ API CKAN aussi : {e}")
    return ok


def download_collisions():
    """
    Collisions routières — CSV complet (Données Québec).
    Source : https://www.donneesquebec.ca/recherche/dataset/vmtl-collisions-routieres
    """
    dest = os.path.join(DATA_DIR, "collisions.csv")
    if os.path.exists(dest):
        size = os.path.getsize(dest) / 1_000_000
        print(f"⏭  collisions.csv déjà présent ({size:.0f} MB) — ignoré.")
        return True
    url = "https://www.donneesquebec.ca/recherche/datastore/dump/05deae93-d9fc-4acb-9779-e0942b5e962f?bom=True"
    return _download(url, dest, "Collisions routières (CSV, Données Québec)")


def download_stm():
    """
    GTFS STM — ZIP planifié (stops.txt, routes.txt, stop_times.txt…).
    Source : https://www.stm.info/fr/a-propos/developpeurs/description-des-donnees-disponibles
    """
    dest = os.path.join(DATA_DIR, "gtfs_stm.zip")
    if os.path.exists(dest):
        size = os.path.getsize(dest) / 1_000_000
        print(f"⏭  gtfs_stm.zip déjà présent ({size:.0f} MB) — ignoré.")
        return True
    url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"
    ok = _download(url, dest, "GTFS STM (ZIP horaires et arrêts)")
    if ok:
        # Vérification du contenu
        try:
            z = zipfile.ZipFile(dest)
            files = z.namelist()
            print(f"   📂 Fichiers dans le ZIP : {', '.join(files[:6])}")
        except Exception as e:
            print(f"   ⚠  Impossible de lire le ZIP : {e}")
    return ok


def download_meteo():
    """
    Météo Canada — API GeoMet OGC climate-daily.
    Observations quotidiennes Montréal (bbox), 365 derniers jours.
    Source : https://api.weather.gc.ca/collections/climate-daily
    Endpoint public, sans clé.
    """
    dest = os.path.join(DATA_DIR, "meteo.csv")
    if os.path.exists(dest):
        size = os.path.getsize(dest) / 1_000_000
        print(f"⏭  meteo.csv déjà présent ({size:.2f} MB) — ignoré.")
        return True

    start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")
    url   = (
        f"https://api.weather.gc.ca/collections/climate-daily/items"
        f"?bbox=-74.0,45.4,-73.4,45.7"
        f"&datetime={start}/{end}"
        f"&limit=500&f=json"
    )

    print(f"\n📥 Météo Canada GeoMet (climate-daily, bbox Montréal)")
    print(f"   URL : {url[:80]}...")
    print(f"   → {dest}")

    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        if not features:
            print("   ❌ Aucune donnée retournée (bbox peut-être trop restrictive).")
            return False

        rows = []
        for feat in features:
            p = feat.get("properties", {})
            rows.append({
                "LOCAL_DATE":          p.get("LOCAL_DATE", ""),
                "MAX_TEMPERATURE":     p.get("MAX_TEMPERATURE"),
                "MIN_TEMPERATURE":     p.get("MIN_TEMPERATURE"),
                "TOTAL_PRECIPITATION": p.get("TOTAL_PRECIPITATION", 0),
                "TOTAL_SNOWFALL":      p.get("TOTAL_SNOWFALL", 0),
                "STATION_NAME":        p.get("STATION_NAME", "Montréal"),
            })

        import pandas as pd
        df = pd.DataFrame(rows)
        df.to_csv(dest, index=False, encoding="utf-8-sig")
        print(f"   ✅ {len(rows)} observations — station(s) : {df['STATION_NAME'].unique()[:3]}")
        return True

    except Exception as e:
        print(f"   ❌ Échec : {e}")
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  MOBILITY COPILOT — Téléchargement des datasets ouverts")
    print("=" * 65)
    print(f"\n📁 Dossier de destination : {DATA_DIR}\n")

    results = {
        "311":        download_311(),
        "collisions": download_collisions(),
        "stm":        download_stm(),
        "meteo":      download_meteo(),
    }

    print("\n" + "=" * 65)
    print("  RÉSUMÉ")
    print("=" * 65)

    labels = {
        "311":        "Requêtes 311        (CSV, donnees.montreal.ca)",
        "collisions": "Collisions routières (CSV, donneesquebec.ca)",
        "stm":        "GTFS STM            (ZIP, stm.info)",
        "meteo":      "Météo Canada GeoMet (JSON→CSV, api.weather.gc.ca)",
    }

    all_ok = True
    for key, ok in results.items():
        icon = "✅" if ok else "❌"
        print(f"  {icon}  {labels[key]}")
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("  🚀 Tout est prêt ! Lance l'app avec : streamlit run app.py")
    else:
        print("  ⚠  Certains fichiers n'ont pas pu être téléchargés.")
        print("     L'app utilisera les données de démonstration pour ces sources.")
        print("     Lance quand même : streamlit run app.py")
    print()
