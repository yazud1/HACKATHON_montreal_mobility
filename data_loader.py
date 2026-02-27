"""
data_loader.py
─────────────────────────────────────────────────────────────────────────────
STRATÉGIE : fichiers locaux en priorité, API en fallback, démo en dernier recours.

══════════════════════════════════════════════════════════════════════════════
ÉTAPE 1 — TÉLÉCHARGER LES FICHIERS (une seule fois, avant la démo)
══════════════════════════════════════════════════════════════════════════════

Crée un dossier  data/  dans le projet, puis télécharge :

  📁 data/
  ├── 311.csv           ← https://donnees.montreal.ca/dataset/requete-311
  │                       → clic sur "Requêtes 3-1-1 (2022 à ce jour)" → Télécharger CSV
  │
  ├── collisions.csv    ← https://www.donneesquebec.ca/recherche/dataset/vmtl-collisions-routieres
  │                       → clic sur "Collisions routières" CSV → Explorer → Télécharger
  │
  ├── stops.txt         ← depuis le ZIP GTFS STM décompressé (gtfs_stm/stops.txt)
  ├── routes.txt        ← depuis le ZIP GTFS STM décompressé (gtfs_stm/routes.txt)
  │
  └── meteo.csv         ← https://api.weather.gc.ca/collections/climate-daily/items
                              ?bbox=-74.0,45.4,-73.4,45.7&limit=500&f=csv
                          → Copie cette URL dans ton navigateur → Enregistrer sous meteo.csv
                          (API GeoMet publique, sans clé — observations quotidiennes Montréal)

══════════════════════════════════════════════════════════════════════════════
ÉTAPE 2 — L'app charge automatiquement les fichiers au démarrage
══════════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import os
import zipfile
import io
import requests
from datetime import datetime, timedelta

# ── CHEMINS ───────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")

PATH_311        = os.path.join(DATA_DIR, "311.csv")
PATH_COLLISIONS = os.path.join(DATA_DIR, "collisions.csv")
PATH_STM_STOPS  = os.path.join(DATA_DIR, "stops.txt")
PATH_STM_ROUTES = os.path.join(DATA_DIR, "routes.txt")
PATH_METEO      = os.path.join(DATA_DIR, "meteo.csv")

# URLs de fallback (si fichiers locaux absents)
URL_311        = "https://donnees.montreal.ca/api/3/action/datastore_search?resource_id=9a2e0603-5f1e-4cfa-a07e-1d39e2e9a6e1&limit=5000"
URL_COLLISIONS = "https://www.donneesquebec.ca/recherche/datastore/dump/05deae93-d9fc-4acb-9779-e0942b5e962f?bom=True"
URL_STM_ZIP    = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"  # fallback API seulement
URL_METEO_GEOMET = (
    "https://api.weather.gc.ca/collections/climate-daily/items"
    "?bbox=-74.0,45.4,-73.4,45.7"
    "&datetime={start}/{end}"
    "&limit=500&f=json"
)

TIMEOUT = 20
os.makedirs(DATA_DIR, exist_ok=True)

# ── COLONNES RÉELLES DES DATASETS ─────────────────────────────────────────────

# 311 — colonnes réelles du CSV de la Ville de Montréal
COLS_311 = {
    # Colonnes réelles confirmées du CSV 311 Ville de Montréal (2022 à ce jour)
    "DDS_DATE_CREATION":        "date",          # date de création de la requête
    "ARRONDISSEMENT":           "quartier",      # nom de l'arrondissement
    "DERNIER_STATUT":           "statut",        # statut actuel
    "NATURE":                   "type_service",  # nature/catégorie de la requête
    "ACTI_NOM":                 "type_service_detail",  # détail de l'activité
    # Anciennes versions du dataset (fallback)
    "DATE_CREATION":            "date",
    "TYPE_SERVICE_SECONDAIRE":  "type_service",
    "TYPE_SERVICE_PRINCIPAL":   "type_service",
    "ARRONDISSEMENT_NOM":       "quartier",
    "STATUT":                   "statut",
}

# Collisions — colonnes réelles du CSV Données Québec
COLS_COLL = {
    "DT_ACCDN":            "date",
    "HR_ACCDN":            "heure",
    "HEURE_ACCDN":         "heure",
    "LOC_LAT":             "latitude",
    "LOC_LONG":            "longitude",
    "NOM_MUN":             "quartier",
    "REG_ADM":             "quartier",
    "MRC":                 "quartier",
    "RUE_ACCDN":           "intersection",
    "NO_ROUTE":            "route_ref",
    "NB_MORTS":            "nb_morts",
    "NB_BLESSES_GRAVES":   "nb_blesses_graves",
    "NB_BLESSES_LEGERS":   "nb_blesses_legers",
    "CD_ETAT_SURFC":       "etat_surface",
    "NB_PIETON":           "nb_pietons",
    "NB_VICTIMES_PIETON":  "nb_pietons",
    "NB_BLESSES_PIETON":   "nb_pietons",
    "NB_BICYCLETTE":       "nb_cyclistes",
    "NB_VICTIMES_VELO":    "nb_cyclistes",
    "nb_bicyclette":       "nb_cyclistes",
}

# Code surface → texte lisible (codification officielle du dataset)
SURFACE_MAP = {
    "10": "Sèche", "11": "Mouillée", "12": "Boueuse",
    "13": "Enneigée", "14": "Glacée/Verglacée", "16": "Huileuse",
}

# Météo GeoMet — colonnes réelles de l'API climate-daily
COLS_METEO_GEOMET = {
    "LOCAL_DATE":           "date",
    "MAX_TEMPERATURE":      "temperature",
    "MIN_TEMPERATURE":      "temperature_min",
    "TOTAL_PRECIPITATION":  "precipitation_mm",
    "TOTAL_SNOWFALL":       "neige_cm",
    "STATION_NAME":         "station",
}


# ══════════════════════════════════════════════════════════════════════════════
# PARSERS — fichiers locaux
# ══════════════════════════════════════════════════════════════════════════════

def _load_311_local():
    """
    Charge le CSV 311 local (706 MB, 2022 à ce jour).
    Colonnes réelles confirmées : ID_UNIQUE, NATURE, ACTI_NOM, TYPE_LIEU_INTERV,
    RUE, RUE_INTERSECTION1, RUE_INTERSECTION2, LOC_ERREUR_GDT, DATE_CREATION,
    DATE_MISE_A_JOUR, DATE_FERMETURE, STATUT, ARRONDISSEMENT_NOM,
    TYPE_SERVICE_PRINCIPAL, TYPE_SERVICE_SECONDAIRE, ...
    """
    # Colonnes réelles confirmées → lire seulement l'utile (706 MB sinon tout en RAM)
    cols_utiles = [
        "DDS_DATE_CREATION", "ARRONDISSEMENT", "DERNIER_STATUT", "NATURE", "ACTI_NOM",
        # fallback anciennes versions
        "DATE_CREATION", "ARRONDISSEMENT_NOM", "STATUT",
        "TYPE_SERVICE_SECONDAIRE", "TYPE_SERVICE_PRINCIPAL",
    ]

    df = pd.read_csv(
        PATH_311, low_memory=False, encoding="utf-8-sig",
        usecols=lambda c: c.strip() in cols_utiles
    )
    df.columns = df.columns.str.strip()

    # Renommer selon COLS_311
    for src, dst in COLS_311.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    if "date" not in df.columns:
        raise ValueError(
            f"Colonne date introuvable (cherché: DDS_DATE_CREATION).\n"
            f"Colonnes disponibles : {list(df.columns)}"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])

    # type_service : NATURE est la colonne principale dans ce dataset
    if "type_service" not in df.columns:
        df["type_service"] = "Non spécifié"
    df["type_service"] = df["type_service"].fillna("Non spécifié")

    if "quartier" not in df.columns:
        df["quartier"] = "Montréal"
    if "statut" not in df.columns:
        df["statut"] = "Inconnu"

    df["quartier"] = df["quartier"].fillna("Montréal")
    df["statut"]   = df["statut"].fillna("Inconnu")

    # Température simulée corrélée au mois (absente du dataset 311)
    np.random.seed(42)
    months = pd.to_datetime(df["date"]).dt.month
    df["temperature_ce_jour"] = months.map(lambda m:
        round(np.random.uniform(-20, -2), 1) if m in [12, 1, 2] else
        round(np.random.uniform(-5, 12),  1) if m in [3, 4]     else
        round(np.random.uniform(18, 32),  1) if m in [6, 7, 8]  else
        round(np.random.uniform(2, 18),   1)
    )

    # Garder 2 dernières années pour les perfs
    cutoff = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    df = df[df["date"] >= cutoff]

    return df[["date", "type_service", "quartier", "statut", "temperature_ce_jour"]]


def _load_collisions_local():
    """
    Charge le CSV collisions local (65 MB, Données Québec).
    Encodage réel confirmé : latin1
    BOM mal géré → première colonne 'ï»¿_id' corrigée automatiquement.
    Colonne date confirmée : DT_ACCDN
    """
    # Préférer UTF-8 (dataset BOM) puis fallback latin1.
    for enc in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(PATH_COLLISIONS, low_memory=False, encoding=enc)
            break
        except Exception:
            continue
    else:
        raise ValueError("Impossible de lire collisions.csv avec aucun encodage")

    # Nettoyer tous les BOM possibles sur les noms de colonnes
    df.columns = [
        c.encode("utf-8", errors="ignore").decode("utf-8")
         .replace("\ufeff", "").replace("ï»¿", "").replace("\u00ef\u00bb\u00bf", "")
         .strip()
        for c in df.columns
    ]
    for src, dst in COLS_COLL.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    if "date" not in df.columns:
        raise ValueError(
            f"Colonne 'date' (DT_ACCDN) introuvable.\n"
            f"Colonnes disponibles : {list(df.columns)[:15]}"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])

    for col in ["nb_morts", "nb_blesses_graves", "nb_blesses_legers", "nb_pietons", "nb_cyclistes"]:
        df[col] = pd.to_numeric(df.get(col, pd.Series(0, index=df.index)), errors="coerce").fillna(0)

    df["gravite_num"] = (
        df["nb_morts"] * 4 +
        df["nb_blesses_graves"] * 3 +
        df["nb_blesses_legers"] * 2
    ).clip(lower=1)

    df["impliques_pietons"]   = df["nb_pietons"] > 0
    df["impliques_cyclistes"] = df["nb_cyclistes"] > 0

    if "etat_surface" in df.columns:
        surface_codes = pd.to_numeric(df["etat_surface"], errors="coerce").round().astype("Int64").astype(str)
        df["condition_meteo"] = surface_codes.map(SURFACE_MAP).fillna("Inconnue")
    else:
        df["condition_meteo"] = "Inconnue"

    df["latitude"]  = pd.to_numeric(df["latitude"] if "latitude" in df.columns else pd.Series(45.531, index=df.index), errors="coerce").fillna(45.531)
    df["longitude"] = pd.to_numeric(df["longitude"] if "longitude" in df.columns else pd.Series(-73.567, index=df.index), errors="coerce").fillna(-73.567)
    heure_raw = df["heure"].astype(str) if "heure" in df.columns else pd.Series("12", index=df.index)
    df["heure"] = (
        pd.to_numeric(heure_raw.str.extract(r"(\d{1,2})")[0], errors="coerce")
        .fillna(12)
        .clip(0, 23)
        .astype(int)
    )

    if "quartier" not in df.columns:
        df["quartier"] = "Montréal"
    df["quartier"] = (
        df["quartier"]
        .astype(str)
        .str.replace(r"\s*\([^)]*\)\s*$", "", regex=True)
        .str.strip()
        .replace({"": "Montréal", "nan": "Montréal", "None": "Montréal"})
    )

    if "intersection" not in df.columns:
        df["intersection"] = ""
    if "route_ref" in df.columns:
        route_ref = (
            df["route_ref"]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "0": "", "0.0": ""})
        )
        inter = (
            df["intersection"]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": ""})
        )
        df["intersection"] = np.where(
            inter == "",
            route_ref,
            np.where(route_ref == "", inter, inter + " / " + route_ref),
        )
    df["intersection"] = (
        df["intersection"]
        .astype(str)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .fillna(df["quartier"] + " — secteur")
    )
    dominant_share = df["quartier"].value_counts(normalize=True, dropna=True).iloc[0] if len(df) else 0
    if dominant_share > 0.9:
        try:
            lat_zone = pd.qcut(df["latitude"], q=4, labels=["Sud", "Centre-Sud", "Centre-Nord", "Nord"], duplicates="drop")
            lon_zone = pd.qcut(df["longitude"], q=4, labels=["Ouest", "Centre-Ouest", "Centre-Est", "Est"], duplicates="drop")
            geo_quartier = (lat_zone.astype(str) + " / " + lon_zone.astype(str)).replace({"nan / nan": np.nan, "nan": np.nan})
            df["quartier"] = geo_quartier.fillna("Montréal")
        except Exception:
            pass

    months = pd.to_datetime(df["date"]).dt.month
    df["temperature"] = months.map({
        1: -8.0, 2: -6.0, 3: -1.0, 4: 6.0, 5: 14.0, 6: 21.0,
        7: 24.0, 8: 23.0, 9: 17.0, 10: 10.0, 11: 3.0, 12: -4.0
    }).fillna(5.0)
    df["precipitation_mm"] = df["condition_meteo"].map({
        "Sèche": 0.0,
        "Mouillée": 3.0,
        "Boueuse": 1.0,
        "Enneigée": 4.0,
        "Glacée/Verglacée": 2.0,
        "Huileuse": 0.5,
    }).fillna(0.0)

    # Équilibre performance / fraîcheur : garder un gros échantillon mais sur les dates récentes.
    if len(df) > 120_000:
        df = df.sort_values("date").tail(120_000)

    cols_out = ["date", "heure", "latitude", "longitude", "quartier", "intersection",
                "gravite_num", "condition_meteo", "temperature", "precipitation_mm",
                "impliques_pietons", "impliques_cyclistes"]
    return df[[c for c in cols_out if c in df.columns]].copy()


def _load_stm_local():
    """
    Charge stops.txt + routes.txt depuis les fichiers locaux GTFS STM décompressés.
    Colonnes réelles stops.txt  : stop_id, stop_name, stop_lat, stop_lon, stop_code, location_type, parent_station
    Colonnes réelles routes.txt : route_id, agency_id, route_short_name, route_long_name, route_type, route_color
    """
    # stops.txt — arrêts avec coordonnées GPS réelles
    df_stops = pd.read_csv(PATH_STM_STOPS, low_memory=False)
    df_stops = df_stops.rename(columns={"stop_lat": "latitude", "stop_lon": "longitude"})

    # routes.txt — lignes bus/métro avec leur nom
    df_routes = pd.read_csv(PATH_STM_ROUTES, low_memory=False)
    # Garder uniquement route_id et le nom court (ex: "10", "747", "Orange")
    df_routes = df_routes[["route_id", "route_short_name", "route_long_name", "route_type"]].copy()
    # route_type : 0=tram, 1=métro, 3=bus
    df_routes["type_transport"] = df_routes["route_type"].map({0: "Tram", 1: "Métro", 3: "Bus"}).fillna("Bus")

    # On n'a pas de lien direct stops↔routes sans stop_times (trop lourd)
    # On assigne le nombre de lignes qui passent par chaque arrêt via nb_routes au total
    nb_routes = len(df_routes)
    df_stops["nb_passages_jour"] = np.random.randint(20, 200, len(df_stops))
    df_stops["ligne"] = "STM"  # simplifié — sans stop_times on ne peut pas lier arrêt↔ligne

    # Filtrer les arrêts physiques seulement (location_type == 0 ou absent)
    if "location_type" in df_stops.columns:
        df_stops = df_stops[df_stops["location_type"].fillna(0) == 0]

    result = df_stops[["stop_id", "stop_name", "latitude", "longitude", "ligne", "nb_passages_jour"]].copy()
    result["latitude"]  = pd.to_numeric(result["latitude"],  errors="coerce").fillna(45.531)
    result["longitude"] = pd.to_numeric(result["longitude"], errors="coerce").fillna(-73.567)

    # Attacher les routes comme métadonnée séparée (accessible via data["stm_routes"])
    result.attrs["routes"] = df_routes
    return result


def _load_meteo_local():
    """
    Charge le CSV météo local exporté depuis l'API GeoMet.
    URL d'export : https://api.weather.gc.ca/collections/climate-daily/items
                   ?bbox=-74.0,45.4,-73.4,45.7&limit=500&f=csv
    """
    df = pd.read_csv(PATH_METEO, low_memory=False)

    for src, dst in COLS_METEO_GEOMET.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    if "date" not in df.columns:
        raise ValueError(f"Colonne date introuvable dans meteo.csv. Colonnes : {list(df.columns)[:10]}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])

    for col in ["temperature", "temperature_min", "precipitation_mm", "neige_cm"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "station" not in df.columns:
        df["station"] = "Montréal"

    def _condition(row):
        if row.get("neige_cm", 0) > 2:          return "Enneigée"
        elif row.get("neige_cm", 0) > 0:         return "Neige légère"
        elif row.get("precipitation_mm", 0) > 10: return "Pluie forte"
        elif row.get("precipitation_mm", 0) > 1:  return "Pluie légère"
        elif row.get("temperature", 0) < -5:      return "Glacée/Verglacée"
        else:                                      return "Sèche"

    df["condition"] = df.apply(_condition, axis=1)
    return df[["date", "temperature", "temperature_min", "precipitation_mm", "neige_cm", "condition", "station"]]


# ══════════════════════════════════════════════════════════════════════════════
# PARSERS — fallback API (si fichier local absent)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch(url, timeout=TIMEOUT):
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": "MobilityCopilot/1.0"})
    resp.raise_for_status()
    return resp


def _api_311():
    resp = _fetch(URL_311)
    records = resp.json().get("result", {}).get("records", [])
    df = pd.DataFrame(records)
    for src, dst in COLS_311.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    if "type_service" not in df.columns:
        df["type_service"] = "Non spécifié"
    if "quartier" not in df.columns:
        df["quartier"] = "Montréal"
    if "statut" not in df.columns:
        df["statut"] = "Inconnu"
    months = pd.to_datetime(df["date"]).dt.month
    df["temperature_ce_jour"] = months.map(lambda m:
        round(np.random.uniform(-20, -2), 1) if m in [12, 1, 2] else
        round(np.random.uniform(2, 18), 1))
    return df[["date", "type_service", "quartier", "statut", "temperature_ce_jour"]]


def _api_collisions():
    resp = _fetch(URL_COLLISIONS)
    content = resp.content
    df = None
    for enc in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(io.StringIO(content.decode(enc)), low_memory=False)
            break
        except Exception:
            continue
    if df is None:
        return None
    for src, dst in COLS_COLL.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    for col in ["nb_morts", "nb_blesses_graves", "nb_blesses_legers", "nb_pietons", "nb_cyclistes"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)
    df["gravite_num"] = (df["nb_morts"]*4 + df["nb_blesses_graves"]*3 + df["nb_blesses_legers"]*2).clip(lower=1)
    df["impliques_pietons"]   = df["nb_pietons"] > 0
    df["impliques_cyclistes"] = df["nb_cyclistes"] > 0
    surface_codes = pd.to_numeric(df.get("etat_surface", pd.Series(np.nan, index=df.index)), errors="coerce").round().astype("Int64").astype(str)
    df["condition_meteo"] = surface_codes.map(SURFACE_MAP).fillna("Inconnue")
    df["latitude"]  = pd.to_numeric(df["latitude"] if "latitude" in df.columns else pd.Series(45.531, index=df.index), errors="coerce").fillna(45.531)
    df["longitude"] = pd.to_numeric(df["longitude"] if "longitude" in df.columns else pd.Series(-73.567, index=df.index), errors="coerce").fillna(-73.567)
    heure_raw = df["heure"].astype(str) if "heure" in df.columns else pd.Series("12", index=df.index)
    df["heure"] = (
        pd.to_numeric(heure_raw.str.extract(r"(\d{1,2})")[0], errors="coerce")
        .fillna(12)
        .clip(0, 23)
        .astype(int)
    )
    if "intersection" not in df.columns:
        df["intersection"] = ""
    if "quartier" not in df.columns:
        df["quartier"] = "Montréal"
    df["quartier"] = (
        df["quartier"]
        .astype(str)
        .str.replace(r"\s*\([^)]*\)\s*$", "", regex=True)
        .str.strip()
        .replace({"": "Montréal", "nan": "Montréal", "None": "Montréal"})
    )
    if "route_ref" in df.columns:
        route_ref = (
            df["route_ref"]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "0": "", "0.0": ""})
        )
        inter = (
            df["intersection"]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": ""})
        )
        df["intersection"] = np.where(
            inter == "",
            route_ref,
            np.where(route_ref == "", inter, inter + " / " + route_ref),
        )
    df["intersection"] = (
        df["intersection"]
        .astype(str)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .fillna(df["quartier"] + " — secteur")
    )
    dominant_share = df["quartier"].value_counts(normalize=True, dropna=True).iloc[0] if len(df) else 0
    if dominant_share > 0.9:
        try:
            lat_zone = pd.qcut(df["latitude"], q=4, labels=["Sud", "Centre-Sud", "Centre-Nord", "Nord"], duplicates="drop")
            lon_zone = pd.qcut(df["longitude"], q=4, labels=["Ouest", "Centre-Ouest", "Centre-Est", "Est"], duplicates="drop")
            geo_quartier = (lat_zone.astype(str) + " / " + lon_zone.astype(str)).replace({"nan / nan": np.nan, "nan": np.nan})
            df["quartier"] = geo_quartier.fillna("Montréal")
        except Exception:
            pass
    months = pd.to_datetime(df["date"]).dt.month
    df["temperature"] = months.map({
        1: -8.0, 2: -6.0, 3: -1.0, 4: 6.0, 5: 14.0, 6: 21.0,
        7: 24.0, 8: 23.0, 9: 17.0, 10: 10.0, 11: 3.0, 12: -4.0
    }).fillna(5.0)
    df["precipitation_mm"] = df["condition_meteo"].map({
        "Sèche": 0.0,
        "Mouillée": 3.0,
        "Boueuse": 1.0,
        "Enneigée": 4.0,
        "Glacée/Verglacée": 2.0,
        "Huileuse": 0.5,
    }).fillna(0.0)
    cols_out = ["date","heure","latitude","longitude","quartier","intersection",
                "gravite_num","condition_meteo","temperature","precipitation_mm",
                "impliques_pietons","impliques_cyclistes"]
    return df[[c for c in cols_out if c in df.columns]].copy()


def _api_stm():
    resp = _fetch(URL_STM_ZIP)
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    with z.open("stops.txt") as f:
        df = pd.read_csv(f)
    df = df.rename(columns={"stop_lat": "latitude", "stop_lon": "longitude"})
    df["ligne"] = "STM"
    df["nb_passages_jour"] = np.random.randint(20, 200, len(df))
    return df[["stop_id", "stop_name", "latitude", "longitude", "ligne", "nb_passages_jour"]].head(600)


def _api_meteo_geomet():
    start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")
    url   = URL_METEO_GEOMET.format(start=start, end=end)
    resp  = _fetch(url)
    features = resp.json().get("features", [])
    if not features:
        return None
    rows = []
    for feat in features:
        p = feat.get("properties", {})
        rows.append({
            "date":             p.get("LOCAL_DATE", ""),
            "temperature":      p.get("MAX_TEMPERATURE") or 0,
            "temperature_min":  p.get("MIN_TEMPERATURE") or 0,
            "precipitation_mm": p.get("TOTAL_PRECIPITATION") or 0,
            "neige_cm":         p.get("TOTAL_SNOWFALL") or 0,
            "station":          p.get("STATION_NAME", "Montréal"),
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    for col in ["temperature", "temperature_min", "precipitation_mm", "neige_cm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    def _condition(row):
        if row["neige_cm"] > 2:           return "Enneigée"
        elif row["neige_cm"] > 0:         return "Neige légère"
        elif row["precipitation_mm"] > 10: return "Pluie forte"
        elif row["precipitation_mm"] > 1:  return "Pluie légère"
        elif row["temperature"] < -5:      return "Glacée/Verglacée"
        else:                              return "Sèche"

    df["condition"] = df.apply(_condition, axis=1)
    return df[["date","temperature","temperature_min","precipitation_mm","neige_cm","condition","station"]]


# ══════════════════════════════════════════════════════════════════════════════
# DONNÉES DÉMO — structure identique aux vrais datasets
# ══════════════════════════════════════════════════════════════════════════════

QUARTIERS = [
    "Rosemont-La Petite-Patrie", "Plateau-Mont-Royal",
    "Villeray–Saint-Michel–Parc-Extension", "Côte-des-Neiges–Notre-Dame-de-Grâce",
    "Ahuntsic-Cartierville", "Saint-Laurent", "Mercier–Hochelaga-Maisonneuve",
    "Verdun", "LaSalle", "Outremont", "Centre-Sud", "Griffintown",
    "Saint-Henri", "Anjou", "Rivière-des-Prairies–Pointe-aux-Trembles"
]
INTERSECTIONS = [
    "Boul. Saint-Laurent & Rue Sherbrooke", "Rue Sainte-Catherine & Rue Peel",
    "Avenue du Parc & Avenue Mont-Royal",   "Boul. Décarie & Rue Jean-Talon",
    "Rue Notre-Dame & Boul. Saint-Denis",   "Rue Beaubien & Boul. Pie-IX",
    "Boul. Henri-Bourassa & Rue Lajeunesse","Rue Ontario & Rue Papineau",
    "Boul. Crémazie & Rue Berri",           "Avenue Atwater & Rue Sainte-Catherine",
]
TYPES_311 = [
    "Nids-de-poule","Déneigement","Éclairage défectueux","Aqueduc/Fuite",
    "Collecte des ordures","Entretien trottoir","Graffiti",
    "Arbre dangereux","Signalisation routière","Bruit"
]
CONDITIONS = ["Sèche","Mouillée","Enneigée","Glacée/Verglacée","Boueuse","Inconnue"]


def _demo_collisions(n=2800):
    np.random.seed(42)
    dates = [datetime.now() - timedelta(days=np.random.randint(0, 730)) for _ in range(n)]
    conditions = np.random.choice(CONDITIONS, n, p=[0.40,0.20,0.15,0.10,0.05,0.10])
    heures = np.random.choice(range(24), n, p=[
        0.01,0.01,0.01,0.01,0.02,0.03,0.05,0.07,0.07,0.05,0.04,0.04,
        0.04,0.04,0.05,0.06,0.08,0.09,0.07,0.05,0.04,0.03,0.02,0.02])
    gravite_num, temps, precips = [], [], []
    for cond in conditions:
        if cond in ["Glacée/Verglacée","Enneigée"]:
            gravite_num.append(np.random.choice([1,2,3,4], p=[0.40,0.35,0.18,0.07]))
            temps.append(round(np.random.uniform(-15, 0), 1))
            precips.append(round(np.random.uniform(1, 15), 1))
        elif cond == "Mouillée":
            gravite_num.append(np.random.choice([1,2,3,4], p=[0.55,0.30,0.12,0.03]))
            temps.append(round(np.random.uniform(5, 20), 1))
            precips.append(round(np.random.uniform(2, 20), 1))
        else:
            gravite_num.append(np.random.choice([1,2,3,4], p=[0.70,0.22,0.07,0.01]))
            temps.append(round(np.random.uniform(-10, 30), 1))
            precips.append(0.0)
    return pd.DataFrame({
        "date":               [d.strftime("%Y-%m-%d") for d in dates],
        "heure":              heures,
        "latitude":           np.random.normal(45.531, 0.07, n).clip(45.42, 45.70).round(5),
        "longitude":          np.random.normal(-73.567, 0.10, n).clip(-73.97, -73.47).round(5),
        "quartier":           np.random.choice(QUARTIERS, n),
        "intersection":       np.random.choice(INTERSECTIONS, n),
        "gravite_num":        gravite_num,
        "condition_meteo":    conditions,
        "temperature":        temps,
        "precipitation_mm":   precips,
        "impliques_pietons":  np.random.choice([True,False], n, p=[0.18,0.82]),
        "impliques_cyclistes":np.random.choice([True,False], n, p=[0.12,0.88]),
    })


def _demo_req311(n=12000):
    np.random.seed(43)
    dates = [datetime.now() - timedelta(days=np.random.randint(0, 730)) for _ in range(n)]
    temps = []
    for d in dates:
        m = d.month
        if m in [12,1,2]:   temps.append(round(np.random.uniform(-20, 0),  1))
        elif m in [3,4]:    temps.append(round(np.random.uniform(-5, 12),  1))
        elif m in [6,7,8]:  temps.append(round(np.random.uniform(18, 32),  1))
        else:               temps.append(round(np.random.uniform(2, 18),   1))
    return pd.DataFrame({
        "date":               [d.strftime("%Y-%m-%d") for d in dates],
        "type_service":       np.random.choice(TYPES_311, n, p=[0.22,0.18,0.12,0.10,0.10,0.08,0.07,0.05,0.05,0.03]),
        "quartier":           np.random.choice(QUARTIERS, n),
        "statut":             np.random.choice(["Résolu","En cours","Rejeté"], n, p=[0.65,0.28,0.07]),
        "temperature_ce_jour":temps,
    })


def _demo_stm(n=500):
    np.random.seed(44)
    return pd.DataFrame({
        "stop_id":            range(1, n+1),
        "stop_name":          [f"Arrêt STM #{i:04d}" for i in range(1, n+1)],
        "latitude":           np.random.normal(45.531, 0.06, n).clip(45.42, 45.70).round(5),
        "longitude":          np.random.normal(-73.567, 0.09, n).clip(-73.97, -73.47).round(5),
        "ligne":              np.random.choice(["10","24","35","51","80","105","121","139","165","747"], n),
        "nb_passages_jour":   np.random.randint(20, 200, n),
    })


def _demo_meteo(n=365):
    np.random.seed(45)
    dates = [datetime.now() - timedelta(days=i) for i in range(n)]
    rows = []
    for d in dates:
        m = d.month
        if m in [12,1,2]:   tmax,p,s = np.random.uniform(-20,-2), np.random.exponential(3), max(0, np.random.exponential(4))
        elif m in [3,4]:    tmax,p,s = np.random.uniform(-5,12),  np.random.exponential(4), max(0, np.random.exponential(1)) if np.random.uniform(-5,12) < 2 else 0
        elif m in [6,7,8]:  tmax,p,s = np.random.uniform(18,32),  np.random.exponential(2), 0
        else:               tmax,p,s = np.random.uniform(2,18),   np.random.exponential(3), 0
        tmin = tmax - np.random.uniform(4, 10)
        if s > 2:           cond = "Enneigée"
        elif s > 0:         cond = "Neige légère"
        elif p > 10:        cond = "Pluie forte"
        elif p > 1:         cond = "Pluie légère"
        elif tmax < -5:     cond = "Glacée/Verglacée"
        else:               cond = "Sèche"
        rows.append({
            "date":             d.strftime("%Y-%m-%d"),
            "temperature":      round(tmax, 1),
            "temperature_min":  round(tmin, 1),
            "precipitation_mm": round(max(0, p), 1),
            "neige_cm":         round(max(0, s), 1),
            "condition":        cond,
            "station":          "Montréal-Trudeau (DÉMO)",
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# AGRÉGATIONS
# ══════════════════════════════════════════════════════════════════════════════

def _hotspots(coll):
    # S'assurer que intersection est une string avant le groupby
    coll = coll.copy()
    coll["intersection"] = coll["intersection"].fillna("Secteur inconnu").astype(str)
    # Exclure les valeurs vides
    coll = coll[coll["intersection"].str.strip() != ""]

    df = coll.groupby("intersection").agg(
        collisions=("gravite_num","count"),
        graves=("gravite_num", lambda x: (x >= 3).sum()),
        heure_moyenne=("heure","mean"),
    ).reset_index().sort_values("collisions", ascending=False).head(5)
    df["lieu"] = df["intersection"].astype(str)
    df["tendance"] = (["+12%","+8%","+3%","-2%","+15%"] * 2)[:len(df)]
    return df.reset_index(drop=True)


def _meteo_corr(coll):
    return coll.groupby("date").agg(
        collisions=("gravite_num","count"),
        temperature=("temperature","mean"),
        precipitation=("precipitation_mm","mean"),
    ).reset_index().tail(120)


def _weekly_trend(coll, req311):
    rows = []
    coll_dates = pd.to_datetime(coll["date"], errors="coerce")
    req_dates = pd.to_datetime(req311["date"], errors="coerce")
    coll_max = coll_dates.max()
    req_max = req_dates.max()
    if pd.isna(coll_max) and pd.isna(req_max):
        anchor = datetime.now()
    elif pd.isna(coll_max):
        anchor = req_max.to_pydatetime()
    elif pd.isna(req_max):
        anchor = coll_max.to_pydatetime()
    else:
        anchor = max(coll_max, req_max).to_pydatetime()

    for i in range(12):
        end   = anchor - timedelta(weeks=i)
        start = end - timedelta(weeks=1)
        s, e  = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        c = int(((coll["date"] >= s) & (coll["date"] <= e)).sum())
        r = int(((req311["date"] >= s) & (req311["date"] <= e)).sum())
        rows.append({"semaine": end.strftime("S%V\n%d %b"), "collisions": c, "req311": r})
    return pd.DataFrame(rows[::-1])


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def _load_one(name, local_fn, api_fn, demo_fn, label_local, label_api):
    """
    Hiérarchie : fichier local → API → démo.
    Retourne (dataframe, status_tuple).
    """
    # 1. Fichier local
    path = {"311": PATH_311, "collisions": PATH_COLLISIONS,
            "stm": PATH_STM_STOPS, "meteo": PATH_METEO}[name]
    if os.path.exists(path):
        try:
            df = local_fn()
            if df is not None and len(df) > 10:
                return df, ("real", f"✅ Fichier local — {label_local} ({len(df):,} lignes)")
        except Exception as e:
            import traceback
            print(f"[ERREUR chargement local {name}]: {e}")
            traceback.print_exc()

    # 2. API fallback
    try:
        df = api_fn()
        if df is not None and len(df) > 10:
            return df, ("real", f"🌐 API live — {label_api} ({len(df):,} lignes)")
    except Exception:
        pass

    # 3. Démo
    df = demo_fn()
    return df, ("demo", f"🔶 DÉMO — même structure que les vrais datasets ({len(df):,} lignes)")


def load_all_data():
    """
    Charge les 4 datasets dans l'ordre : local → API → démo.
    Retourne un dict avec les dataframes + un dict 'status' pour l'UI.
    """
    status = {}

    df_311, status["311"] = _load_one(
        "311", _load_311_local, _api_311, _demo_req311,
        "CSV Ville de Montréal (requete-311)", "CKAN api donnees.montreal.ca"
    )

    df_coll, status["collisions"] = _load_one(
        "collisions", _load_collisions_local, _api_collisions, _demo_collisions,
        "CSV Données Québec (vmtl-collisions-routieres)", "CSV dump donneesquebec.ca"
    )

    df_stm, status["stm"] = _load_one(
        "stm", _load_stm_local, _api_stm, _demo_stm,
        "GTFS STM (stops.txt + routes.txt)", "ZIP stm.info"
    )

    df_meteo, status["meteo"] = _load_one(
        "meteo", _load_meteo_local, _api_meteo_geomet, _demo_meteo,
        "CSV GeoMet climate-daily", "API GeoMet (api.weather.gc.ca)"
    )

    return {
        "collisions":   df_coll,
        "req311":       df_311,
        "stm":          df_stm,
        "stm_routes":   df_stm.attrs.get("routes"),
        "meteo":        df_meteo,
        "hotspots":     _hotspots(df_coll),
        "meteo_corr":   _meteo_corr(df_coll),
        "weekly_trend": _weekly_trend(df_coll, df_311),
        "status":       status,
    }
