"""
rag_engine.py
Moteur RAG léger basé sur un corpus de connaissances statique.
Simule ChromaDB/LlamaIndex pour la démo — structure prête pour intégration réelle.
"""

import re
import unicodedata


# ─── CORPUS DE CONNAISSANCES ──────────────────────────────────────────────────
CORPUS = {
    "dataset_311": {
        "titre": "Requêtes 311 – Ville de Montréal",
        "description": "Le service 311 reçoit les demandes citoyennes pour des problèmes urbains non-urgents. Chaque requête contient : type de service, date, arrondissement, statut de traitement.",
        "categories": {
            "Nids-de-poule": "Dégradations de la chaussée, typiquement post-gel/dégel (mars–avril).",
            "Déneigement": "Demandes liées au déblayage de neige, pics en décembre–février.",
            "Éclairage défectueux": "Lampadaires, signaux ou zones mal éclairées.",
            "Aqueduc/Fuite": "Problèmes d'infrastructure hydraulique souterraine.",
            "Collecte des ordures": "Ramassage manqué ou contenants endommagés.",
            "Entretien trottoir": "Trottoirs dégradés, craquelés ou dangereux.",
        },
        "source": "https://donnees.montreal.ca/dataset/requete-311"
    },
    "dataset_collisions": {
        "titre": "Collisions routières – Ville de Montréal",
        "description": "Données géolocalisées des accidents de la route sur l'île de Montréal.",
        "champs": {
            "gravite": "Classification officielle : Dommages matériels → Blessés légers → Blessés graves → Mortel",
            "condition_meteo": "Condition atmosphérique au moment de l'accident.",
            "heure": "Heure de survenance (0–23). Pic habituel : 7h–9h (matin) et 16h–19h (soir).",
            "pietons": "Indique si des piétons sont impliqués.",
            "cyclistes": "Indique si des cyclistes sont impliqués.",
        },
        "source": "https://www.donneesquebec.ca/recherche/dataset/vmtl-collisions-routieres"
    },
    "dataset_stm": {
        "titre": "Transport collectif STM – GTFS",
        "description": "Données du réseau de bus et métro de la Société de transport de Montréal.",
        "champs": {
            "stop_id": "Identifiant unique de l'arrêt.",
            "ligne": "Numéro ou nom de la ligne de bus/métro.",
            "nb_passages_jour": "Fréquence de passage quotidienne estimée.",
        },
        "source": "https://www.stm.info/fr/a-propos/developpeurs"
    },
    "dataset_meteo": {
        "titre": "Météo Canada – API GeoMet OGC (climate-daily)",
        "description": (
            "Observations climatiques quotidiennes du Service météorologique du Canada (SMC), "
            "accessibles via l'API GeoMet OGC standard (api.weather.gc.ca/collections/climate-daily). "
            "Données filtrées sur la bbox de l'île de Montréal (-74.0, 45.4, -73.4, 45.7). "
            "Champs disponibles : température max/min (°C), précipitations totales (mm), "
            "chutes de neige (cm), station météo la plus proche."
        ),
        "champs": {
            "LOCAL_DATE":           "Date de l'observation (YYYY-MM-DD).",
            "MAX_TEMPERATURE":      "Température maximale du jour (°C).",
            "MIN_TEMPERATURE":      "Température minimale du jour (°C).",
            "TOTAL_PRECIPITATION":  "Précipitations totales (mm) — pluie + neige fondue.",
            "TOTAL_SNOWFALL":       "Chutes de neige (cm).",
            "STATION_NAME":         "Nom de la station d'observation (ex: MONTREAL/TRUDEAU INTL A).",
        },
        "seuils_critiques": {
            "verglas":        "Température entre -5°C et 2°C + précipitations > 0 → risque de verglas.",
            "tempete_neige":  "TOTAL_SNOWFALL > 15cm en 24h → tempête de neige, impacts mobilité majeurs.",
            "pluie_forte":    "TOTAL_PRECIPITATION > 10mm → chaussée glissante, visibilité réduite.",
            "grand_froid":    "MAX_TEMPERATURE < -15°C → conditions extrêmes, hausse requêtes 311 déneigement.",
        },
        "endpoint": "https://api.weather.gc.ca/collections/climate-daily/items?bbox=-74.0,45.4,-73.4,45.7&f=json",
        "source": "https://api.weather.gc.ca/ (GeoMet-OGC-API, accès public, sans clé)"
    },
    "definitions": {
        "hotspot": "Zone géographique présentant une concentration anormalement élevée d'incidents sur une période donnée.",
        "signal_faible": "Tendance émergente de faible volume mais persistante, pouvant annoncer un problème futur.",
        "tendance": "Évolution d'un indicateur dans le temps, comparée à une période de référence (semaine/mois/année précédente).",
        "RAG": "Retrieval-Augmented Generation : approche qui ancre les réponses du LLM sur un corpus de faits vérifiés pour éviter les hallucinations.",
    }
}


class RAGEngine:
    """
    Moteur RAG léger.
    Récupère les chunks de connaissances pertinents selon la question.
    En production : ChromaDB + embeddings OpenAI/Claude.
    """
    
    def __init__(self):
        self.corpus = CORPUS
        self._build_index()
    
    def _build_index(self):
        """Construit un index simple par mots-clés."""
        self.index = {}
        keywords_map = {
            "311": ["dataset_311"],
            "requête": ["dataset_311"],
            "nid": ["dataset_311"],
            "déneig": ["dataset_311"],
            "ordure": ["dataset_311"],
            "trottoir": ["dataset_311"],
            "collision": ["dataset_collisions"],
            "accident": ["dataset_collisions"],
            "gravité": ["dataset_collisions"],
            "piéton": ["dataset_collisions"],
            "cycliste": ["dataset_collisions"],
            "stm": ["dataset_stm"],
            "bus": ["dataset_stm"],
            "arrêt": ["dataset_stm"],
            "métro": ["dataset_stm"],
            "météo": ["dataset_meteo"],
            "pluie": ["dataset_meteo"],
            "neige": ["dataset_meteo"],
            "température": ["dataset_meteo"],
            "verglas": ["dataset_meteo"],
            "hotspot": ["definitions"],
            "signal": ["definitions"],
            "tendance": ["definitions"],
        }
        for kw, sources in keywords_map.items():
            self.index[kw] = sources
    
    def retrieve(self, question: str, top_k: int = 3) -> list[dict]:
        """Récupère les chunks pertinents pour une question."""
        question_lower = question.lower()
        relevant_sources = set()
        
        for kw, sources in self.index.items():
            if kw in question_lower:
                for s in sources:
                    relevant_sources.add(s)
        
        # Par défaut, inclure collisions + 311
        if not relevant_sources:
            relevant_sources = {"dataset_collisions", "dataset_311"}
        
        results = []
        for source_key in list(relevant_sources)[:top_k]:
            if source_key in self.corpus:
                results.append({
                    "source": source_key,
                    "content": self.corpus[source_key]
                })
        
        return results
    
    def get_glossary_context(self, question: str) -> str:
        """Retourne un contexte textuel formaté pour le LLM."""
        chunks = self.retrieve(question)
        context_parts = []
        
        for chunk in chunks:
            content = chunk['content']
            titre = content.get('titre', chunk['source'])
            desc = content.get('description', '')
            context_parts.append(f"[SOURCE: {titre}]\n{desc}")
            
            if 'categories' in content:
                context_parts.append("Catégories: " + ", ".join(content['categories'].keys()))
            if 'seuils_critiques' in content:
                context_parts.append("Seuils critiques: " + str(content['seuils_critiques']))
        
        return "\n\n".join(context_parts)
    
    def detect_ambiguity(self, question: str) -> dict:
        """
        Détecte si une question est ambiguë.
        Retourne {'is_ambiguous': bool, 'reason': str, 'clarifications': list}
        """
        question_lower = (question or "").lower()
        question_norm = unicodedata.normalize("NFKD", question_lower)
        question_norm = "".join(ch for ch in question_norm if not unicodedata.combining(ch))
        
        # Mots déclencheurs d'ambiguïté
        ambiguous_patterns = {
            "ça coince": {
                "reason": "L'expression 'ça coince' peut désigner plusieurs phénomènes.",
                "clarifications": [
                    "🚗 Embouteillages / ralentissements de trafic",
                    "⚠️ Zones à fort taux de collisions",
                    "📋 Secteurs avec beaucoup de requêtes 311 non résolues",
                ]
            },
            "ça bloque": {
                "reason": "L'expression 'ça bloque' peut désigner plusieurs phénomènes.",
                "clarifications": [
                    "🚗 Embouteillages / ralentissements de trafic",
                    "⚠️ Zones à fort taux de collisions",
                    "📋 Secteurs avec beaucoup de requêtes 311 non résolues",
                ]
            },
            "incidents": {
                "reason": "Le terme 'incidents' peut couvrir différents types de données.",
                "clarifications": [
                    "💥 Collisions routières (base de données accidents)",
                    "📋 Requêtes 311 (problèmes signalés par citoyens)",
                    "🚌 Perturbations du réseau STM",
                ]
            },
            "problèmes": {
                "reason": "Plusieurs types de problèmes sont disponibles dans les données.",
                "clarifications": [
                    "🛣️ Problèmes de voirie (nids-de-poule, trottoirs)",
                    "🚨 Problèmes de sécurité (collisions, zones dangereuses)",
                    "💡 Problèmes d'infrastructure (éclairage, aqueduc)",
                ]
            },
        }
        
        for pattern, info in ambiguous_patterns.items():
            pattern_norm = unicodedata.normalize("NFKD", pattern.lower())
            pattern_norm = "".join(ch for ch in pattern_norm if not unicodedata.combining(ch))
            if pattern in question_lower or pattern_norm in question_norm:
                return {
                    "is_ambiguous": True,
                    "reason": info["reason"],
                    "clarifications": info["clarifications"]
                }

        # Variantes fréquentes non accentuées.
        if (
            re.search(r"\b(ca|ça)\s+(coince|bloque)\b", question_lower)
            or re.search(r"\bou\s+ca\s+(coince|bloque)\b", question_norm)
        ):
            info = ambiguous_patterns["ça coince"]
            return {
                "is_ambiguous": True,
                "reason": info["reason"],
                "clarifications": info["clarifications"],
            }
        
        return {"is_ambiguous": False}
