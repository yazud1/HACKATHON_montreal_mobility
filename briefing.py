"""
briefing.py
Briefing hebdomadaire data-driven (municipalite / grand public).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext
import re
import pandas as pd
import numpy as np

getcontext().prec = 28


C = {
    "bg": "var(--mc-card-bg)",
    "subtle": "var(--mc-surface)",
    "border": "var(--mc-border)",
    "text": "var(--mc-text)",
    "text2": "var(--mc-text-muted)",
    "text3": "var(--mc-text-subtle)",
    "green": "var(--mc-success)",
    "green_bg": "rgba(22, 163, 74, 0.12)",
    "red": "var(--mc-danger)",
    "red_bg": "rgba(220, 38, 38, 0.12)",
    "orange": "var(--mc-warn)",
    "orange_bg": "var(--mc-warn-bg)",
    "blue": "var(--mc-accent)",
    "blue_bg": "rgba(37, 99, 235, 0.12)",
    "mono": "'Geist Mono', 'SF Mono', monospace",
    "sans": "'Geist', -apple-system, sans-serif",
}

HOTSPOT_311_REGEX = r"nid|deneig|déneig|eclair|éclair"
PERIOD_TO_DAYS = {
    "7 derniers jours": 7,
    "30 derniers jours": 30,
    "3 derniers mois": 90,
    "12 derniers mois": 365,
}


def _kpi(label: str, value: str, sub: str = "", color: str | None = None) -> str:
    color = color or C["text"]
    return (
        f"""<div style="border:1px solid {C['border']};border-radius:10px;padding:16px 18px;background:{C['subtle']};flex:1;min-width:140px;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;font-weight:500;color:{C['text3']};letter-spacing:0.06em;text-transform:uppercase;margin-bottom:6px;">{label}</div>"""
        f"""<div style="font-family:{C['mono']};font-size:22px;font-weight:700;color:{color};letter-spacing:-0.02em;">{value}</div>"""
        f"""<div style="font-size:11px;color:{C['text3']};margin-top:3px;">{sub}</div>"""
        "</div>"
    )


def _card(title: str, body: str, accent: str | None = None) -> str:
    accent = accent or C["border"]
    title_html = (
        f"""<div style="font-size:14px;font-weight:600;color:{C['text']};margin-bottom:8px;">{title}</div>"""
        if title
        else ""
    )
    return (
        f"""<div style="border:1px solid {C['border']};border-left:3px solid {accent};border-radius:10px;padding:16px 18px;margin-bottom:10px;background:{C['bg']};">"""
        f"""{title_html}<div style="font-size:13px;color:{C['text2']};line-height:1.75;">{body}</div>"""
        "</div>"
    )


def _accordion(title: str, content: str, subtitle: str = "", opened: bool = False) -> str:
    open_attr = " open" if opened else ""
    return (
        f"""<details class="brief-accordion"{open_attr}>"""
        f"""<summary><span class="brief-acc-title">{title}</span></summary>"""
        f"""<div class="brief-accordion-body">{content}</div>"""
        "</details>"
    )


def _tag(text: str, color: str, bg: str) -> str:
    return f"""<span style="font-family:{C['mono']};font-size:11px;font-weight:600;color:{color};background:{bg};padding:2px 8px;border-radius:4px;">{text}</span>"""


def _compute_briefing_status(
    coll_curr: pd.DataFrame,
    req_curr: pd.DataFrame,
    coll_anchor: pd.Timestamp,
    req_anchor: pd.Timestamp,
) -> tuple[str, str, str]:
    coll_n = len(coll_curr)
    req_n = len(req_curr)
    if coll_n == 0 and req_n == 0:
        return (
            "Données insuffisantes",
            "Fenêtre vide sur collisions et 311 : élargir la période ou vérifier le chargement des sources.",
            "insufficient",
        )
    if coll_n == 0 or req_n == 0:
        missing = "collisions" if coll_n == 0 else "requêtes 311"
        return (
            "Données insuffisantes",
            f"Source principale manquante sur la période ({missing}) : briefing incomplet.",
            "insufficient",
        )
    if pd.notna(coll_anchor) and pd.notna(req_anchor):
        anchor_gap_days = abs(int((coll_anchor - req_anchor).days))
        if anchor_gap_days > 14:
            return (
                "Analyse partielle",
                "Note méthodologique : les sources de données couvrent des périodes de référence différentes. Les corrélations présentées sont descriptives et doivent être interprétées avec prudence.",
                "partial",
            )
    return (
        "Analyse vérifiée",
        "Sources synchronisées sur la fenêtre avec volumes suffisants pour un suivi descriptif fiable.",
        "verified",
    )


def _status_block(label: str, detail: str, level: str) -> str:
    palettes = {
        "verified": {"color": C["green"], "bg": C["green_bg"], "border": "rgba(22, 163, 74, 0.32)"},
        "partial": {"color": C["orange"], "bg": C["orange_bg"], "border": C["orange"]},
        "insufficient": {"color": C["red"], "bg": C["red_bg"], "border": "rgba(220, 38, 38, 0.32)"},
    }
    p = palettes.get(level, palettes["partial"])
    return (
        f"""<div style="border:1px solid {C['border']};border-radius:10px;padding:10px 12px;margin-bottom:12px;background:{C['bg']};">"""
        f"""<div style="margin-bottom:6px;"><span title="{detail}" style="display:inline-flex;align-items:center;border:1px solid {p['border']};background:{p['bg']};color:{p['color']};border-radius:999px;padding:4px 10px;font-family:{C['mono']};font-size:10px;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;">{label}</span></div>"""
        f"""<div style="font-size:12px;color:{C['text2']};line-height:1.6;">{detail}</div>"""
        "</div>"
    )


def _tone_profile(tone: str) -> dict:
    if tone == "municipal":
        return {
            "role_label": "Municipalité",
            "icon": "M",
            "accent": C["orange"],
            "accent_bg": C["orange_bg"],
            "role_summary": "Lecture opérationnelle pour décideurs, ingénieurs et chefs d'arrondissement.",
            "lexicon": "Vocabulaire: taux de gravité pondéré, usager vulnérable, période glissante.",
            "finality": "Action terrain",
            "usage": "Arbitrage opérationnel",
            "output": "Priorisation + impact attendu",
        }
    return {
        "role_label": "Grand public",
        "icon": "C",
        "accent": C["blue"],
        "accent_bg": C["blue_bg"],
        "role_summary": "Lecture citoyenne simple pour comprendre les zones de vigilance et les bons réflexes.",
        "lexicon": "Vocabulaire: accidents, signalements, zones à surveiller, conseils pratiques.",
        "finality": "Prévention",
        "usage": "Information citoyenne",
        "output": "Vigilance + gestes concrets",
    }


def _role_strip(tone: str, period_label: str) -> str:
    p = _tone_profile(tone)
    return (
        f"""<div style="border:1px solid {C['border']};border-left:3px solid {p['accent']};border-radius:10px;padding:10px 12px;margin-bottom:12px;background:{C['subtle']};">"""
        f"""<div style="display:flex;align-items:flex-start;gap:10px;">"""
        f"""<div style="width:18px;height:18px;border-radius:999px;border:1px solid {p['accent']};background:{p['accent_bg']};color:{p['accent']};display:inline-flex;align-items:center;justify-content:center;font-family:{C['mono']};font-size:10px;font-weight:700;line-height:1;">{p['icon']}</div>"""
        f"""<div style="min-width:0;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;font-weight:600;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;margin-bottom:4px;">Mode {p['role_label']}</div>"""
        f"""<div style="font-size:13px;color:{C['text2']};line-height:1.6;">{p['role_summary']} Fenêtre active: <strong>{period_label}</strong>.</div>"""
        f"""<div style="font-size:12px;color:{C['text3']};line-height:1.6;margin-top:3px;">{p['lexicon']}</div>"""
        "</div>"
        "</div>"
        "</div>"
    )


def _finality_strip(tone: str) -> str:
    p = _tone_profile(tone)
    items = [
        f"Finalité: {p['finality']}",
        f"Usage: {p['usage']}",
        f"Sortie: {p['output']}",
    ]
    chips = "".join(
        f"""<span style="display:inline-flex;align-items:center;border:1px solid {C['border']};background:{C['subtle']};border-radius:999px;padding:4px 10px;font-family:{C['mono']};font-size:10px;color:{C['text2']};letter-spacing:0.04em;">{it}</span>"""
        for it in items
    )
    return f"""<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">{chips}</div>"""


def _reading_path_strip(tone: str, period_label: str) -> str:
    if tone == "municipal":
        steps = [
            "1) Zones prioritaires",
            "2) Synthèse d'arbitrage",
            "3) Plan d'action terrain",
            "4) Tendances à monitorer",
        ]
    else:
        steps = [
            "1) Zones de vigilance",
            "2) Conseils pratiques",
            "3) Tendances locales",
            "4) À retenir",
        ]
    lis = "".join(
        f"""<li style="margin-bottom:4px;">{s}</li>""" for s in steps
    )
    return (
        f"""<div style="border:1px dashed {C['border']};border-radius:10px;background:{C['subtle']};padding:10px 12px;margin-bottom:10px;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;margin-bottom:6px;">Parcours de lecture ({period_label})</div>"""
        f"""<ul style="margin:0;padding-left:16px;font-size:12px;color:{C['text2']};line-height:1.55;">{lis}</ul>"""
        "</div>"
    )


def _main_insight_block(
    tone: str,
    hotspots: list[dict],
    coll_curr_n: int,
    coll_var: float,
    period_label: str,
) -> str:
    profile = _tone_profile(tone)
    top = hotspots[0] if hotspots else {}
    zone_txt = _hotspot_line_for_tone(top, tone) if top else "Aucune zone prioritaire détectée sur la fenêtre."
    trend_txt = "en hausse" if coll_var > 0 else "en baisse" if coll_var < 0 else "stable"
    if tone == "municipal":
        insight = (
            f"Priorité opérationnelle: {zone_txt} "
            f"Sur {period_label}, volume collisions = {coll_curr_n} ({trend_txt}, {coll_var:+.1f}%)."
        )
    else:
        insight = (
            f"Point clé à retenir: {zone_txt} "
            f"Sur {period_label}, les accidents sont {trend_txt} ({coll_var:+.1f}%)."
        )
    return (
        f"""<div style="border:1px solid {C['border']};border-left:4px solid {profile['accent']};border-radius:12px;padding:12px 14px;margin-bottom:12px;background:{C['subtle']};">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;font-weight:600;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;margin-bottom:6px;">Insight principal</div>"""
        f"""<div style="font-size:15px;color:{C['text']};line-height:1.6;font-weight:600;">{insight}</div>"""
        "</div>"
    )


def _briefing_decision_block(
    tone: str,
    period_label: str,
    coll_curr: pd.DataFrame,
    req_focus_curr_n: int,
    hotspots: list[dict],
    coll_start: pd.Timestamp,
    coll_anchor: pd.Timestamp,
    req_start: pd.Timestamp,
    req_anchor: pd.Timestamp,
    recommendations: list[str],
) -> str:
    profile = _tone_profile(tone)
    accent = profile["accent"]
    accent_bg = profile["accent_bg"]
    coll_n = len(coll_curr)
    coll_period = f"{coll_start.strftime('%Y-%m-%d')} -> {coll_anchor.strftime('%Y-%m-%d')}"
    req_period = f"{req_start.strftime('%Y-%m-%d')} -> {req_anchor.strftime('%Y-%m-%d')}"
    top_hotspot = hotspots[0] if hotspots else {
        "source": "Collisions",
        "line_municipal": "Aucune zone prioritaire détectée.",
        "line_public": "Aucune zone de vigilance détectée.",
    }
    dominant_weather = _mode_text(coll_curr.get("condition_meteo", pd.Series(dtype=str)), default="Inconnue").lower() if not coll_curr.empty else "inconnue"
    hotspot_line = _hotspot_line_for_tone(top_hotspot, tone)
    retenir_items = [
        f"Volumes clés: {coll_n} collisions observées et {req_focus_curr_n} signalements 311 ciblés sur {period_label}.",
        f"Fenêtres analysées: collisions {coll_period} ; 311 {req_period}.",
        f"Zone prioritaire: {hotspot_line}",
        f"Condition dominante observée: {dominant_weather}.",
    ]
    action = recommendations[0] if recommendations else "Prioriser un contrôle ciblé sur la zone la plus exposée."
    if tone == "municipal":
        verification = "Confirmer l'arbitrage avec un indicateur normalisé (trafic, population, longueur de voirie) avant déploiement large."
        impact = "Impact attendu (à suivre): baisse des collisions graves sur les zones ciblées après action signalisation/contrôle."
        retain_title = "À retenir (opérationnel)"
        decision_title = "Décision possible (opérationnelle)"
    else:
        verification = "Vérifier ensuite la persistance sur 2 périodes et compléter avec des indicateurs normalisés pour éviter la sur-interprétation."
        impact = "Effet attendu: réduction de l'exposition individuelle via vigilance sur zones/horaires à risque."
        retain_title = "À retenir (citoyen)"
        decision_title = "Décision possible (action citoyenne)"
    focus_badge = _tag("Bloc prioritaire", accent, accent_bg)
    retenir_html = "".join(
        f"""<li style="margin-bottom:7px;">{item}</li>""" for item in retenir_items
    )
    return (
        f"""<div style="border:1px solid {C['border']};border-left:6px solid {accent};border-radius:12px;padding:14px;background:linear-gradient(180deg, {accent_bg} 0%, {C['bg']} 60%);box-shadow:0 10px 24px rgba(15, 23, 42, 0.08);">"""
        f"""<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:10px;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;font-weight:600;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;">Synthèse orientée décision</div>"""
        f"""{focus_badge}"""
        "</div>"
        f"""<div class="brief-decision-grid">"""
        f"""<div style="border:1px solid {C['border']};border-radius:10px;background:{C['subtle']};padding:10px 12px;box-shadow:0 1px 0 rgba(255,255,255,0.08) inset;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;color:{C['text3']};letter-spacing:0.08em;margin-bottom:6px;">{retain_title}</div>"""
        f"""<ul style="margin:0;padding-left:18px;font-size:13px;color:{C['text2']};line-height:1.6;">{retenir_html}</ul>"""
        "</div>"
        f"""<div style="border:1px solid {C['border']};border-radius:10px;background:{C['subtle']};padding:10px 12px;box-shadow:0 1px 0 rgba(255,255,255,0.08) inset;">"""
        f"""<div style="font-family:{C['mono']};font-size:10px;color:{C['text3']};letter-spacing:0.08em;margin-bottom:6px;">{decision_title}</div>"""
        f"""<div style="font-size:13px;color:{C['text2']};line-height:1.7;margin-bottom:8px;">{action}</div>"""
        f"""<div style="font-size:12px;color:{C['text3']};line-height:1.6;"><strong style="color:{C['text2']};">Vérification prioritaire :</strong> {verification}</div>"""
        f"""<div style="font-size:12px;color:{C['text3']};line-height:1.6;margin-top:6px;"><strong style="color:{C['text2']};">Impact attendu :</strong> {impact}</div>"""
        "</div>"
        "</div>"
        "</div>"
    )


def _safe_pct(curr: float, prev: float) -> float:
    if prev <= 0:
        return 100.0 if curr > 0 else 0.0
    return round((curr - prev) / prev * 100, 1)


def _fmt_pct(v: float) -> str:
    return f"+{v:.1f}%" if v > 0 else f"{v:.1f}%"


def _raw_variation(curr: int, prev: int) -> tuple[str, float | None]:
    delta = int(curr) - int(prev)
    if prev <= 0:
        if curr > 0:
            return f"{delta:+d} / {prev} (base nulle)", None
        return "0 / 0", 0.0
    pct = (Decimal(delta) / Decimal(int(prev))) * Decimal(100)
    pct_txt = format(pct, "f")
    return f"{delta:+d} / {prev} = {pct_txt}%", float(pct)


def _slot_label(hour: float | int | None) -> str:
    if hour is None or (isinstance(hour, float) and np.isnan(hour)):
        return "la journée"
    h = int(round(float(hour)))
    if 6 <= h < 10:
        return "7h-10h"
    if 10 <= h < 13:
        return "10h-13h"
    if 13 <= h < 16:
        return "13h-16h"
    if 16 <= h < 19:
        return "16h-19h"
    if 19 <= h < 22:
        return "19h-22h"
    return "22h-7h"


def _mode_num(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mode().iloc[0])


def _mode_text(series: pd.Series, default: str = "conditions mixtes") -> str:
    s = series.dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return default
    return str(s.value_counts().idxmax())


def _prepare_frames(data: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    collisions = data.get("collisions", pd.DataFrame()).copy()
    req311 = data.get("req311", pd.DataFrame()).copy()
    stm = data.get("stm", pd.DataFrame()).copy()

    collisions["_dt"] = pd.to_datetime(collisions.get("date"), errors="coerce")
    req311["_dt"] = pd.to_datetime(req311.get("date"), errors="coerce")

    anchors = [collisions["_dt"].max(), req311["_dt"].max()]
    anchor = max([a for a in anchors if pd.notna(a)], default=pd.Timestamp(datetime.now()))

    return collisions, req311, stm, anchor


def _period_days(periode: str) -> int:
    m = re.search(
        r"Personnalisée\s*:\s*(\d{4}-\d{2}-\d{2})\s*(?:->|→)\s*(\d{4}-\d{2}-\d{2})",
        str(periode),
        flags=re.IGNORECASE,
    )
    if m:
        start = pd.to_datetime(m.group(1), errors="coerce")
        end = pd.to_datetime(m.group(2), errors="coerce")
        if pd.notna(start) and pd.notna(end):
            if start > end:
                start, end = end, start
            return max(1, int((end - start).days) + 1)
    return PERIOD_TO_DAYS.get(periode, 30)


def _slice_period(curr_df: pd.DataFrame, ref_df: pd.DataFrame, days: int):
    anchor_candidates = []
    if not curr_df.empty:
        anchor_candidates.append(curr_df["_dt"].max())
    if not ref_df.empty:
        anchor_candidates.append(ref_df["_dt"].max())
    anchor = max([a for a in anchor_candidates if pd.notna(a)], default=pd.Timestamp(datetime.now()))

    curr_start = anchor - pd.Timedelta(days=days - 1)
    prev_start = anchor - pd.Timedelta(days=2 * days - 1)
    prev_end = anchor - pd.Timedelta(days=days)

    curr = curr_df[(curr_df["_dt"] >= curr_start) & (curr_df["_dt"] <= anchor)].copy()
    prev = ref_df[(ref_df["_dt"] >= prev_start) & (ref_df["_dt"] <= prev_end)].copy()
    return curr, prev, curr_start, anchor


def _period_windows(
    collisions: pd.DataFrame,
    req311: pd.DataFrame,
    ref_collisions: pd.DataFrame,
    ref_req311: pd.DataFrame,
    days: int,
):
    coll_curr, coll_prev, coll_start, coll_anchor = _slice_period(collisions, ref_collisions, days)
    req_curr, req_prev, req_start, req_anchor = _slice_period(req311, ref_req311, days)
    return coll_curr, coll_prev, req_curr, req_prev, coll_start, coll_anchor, req_start, req_anchor


def _build_hotspots(coll_curr: pd.DataFrame, req_curr: pd.DataFrame, stm: pd.DataFrame, days: int) -> list[dict]:
    candidates: list[dict] = []

    if not coll_curr.empty:
        coll = coll_curr.copy()
        coll["intersection"] = coll.get("intersection", "Secteur inconnu").fillna("Secteur inconnu").astype(str)
        coll["condition_meteo"] = coll.get("condition_meteo", "Inconnue").fillna("Inconnue").astype(str)

        by_inter = (
            coll.groupby("intersection")
            .agg(
                collisions=("gravite_num", "count"),
                graves=("gravite_num", lambda x: int((x >= 3).sum())),
                heure_mode=("heure", _mode_num),
                meteo_mode=("condition_meteo", _mode_text),
            )
            .sort_values("collisions", ascending=False)
        )

        for intersection, row in by_inter.head(5).iterrows():
            slot = _slot_label(row["heure_mode"])
            meteo = str(row["meteo_mode"]).lower()
            line_municipal = (
                f"Intersection {intersection} - {int(row['collisions'])} collisions "
                f"(dont {int(row['graves'])} graves), surtout entre {slot}, {meteo}."
            )
            line_public = (
                f"Zone {intersection} - {int(row['collisions'])} collisions signalees, "
                f"vigilance surtout entre {slot} ({meteo})."
            )
            candidates.append(
                {
                    "source": "Collisions",
                    "score": float(row["collisions"]) + float(row["graves"]) * 0.6,
                    "line": line_municipal,
                    "line_municipal": line_municipal,
                    "line_public": line_public,
                    "name": str(intersection),
                    "count": int(row["collisions"]),
                    "graves": int(row["graves"]),
                    "heure_mode": row["heure_mode"],
                    "meteo_mode": str(row["meteo_mode"]),
                }
            )

    if not req_curr.empty:
        req = req_curr.copy()
        req["type_service"] = req.get("type_service", "Non specifie").fillna("Non specifie").astype(str)
        req["quartier"] = req.get("quartier", "Montreal").fillna("Montreal").astype(str)
        req_focus = req[req["type_service"].str.contains(HOTSPOT_311_REGEX, case=False, na=False, regex=True)]
        if req_focus.empty:
            req_focus = req

        by_311 = (
            req_focus.groupby(["quartier", "type_service"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(4)
        )

        for _, row in by_311.iterrows():
            line_municipal = (
                f"Zone {row['quartier']} - {int(row['count'])} requetes 311 "
                f"« {row['type_service']} » en {days} jours."
            )
            line_public = (
                f"Zone {row['quartier']} - {int(row['count'])} signalements 311 "
                f"« {row['type_service']} » sur {days} jours."
            )
            candidates.append(
                {
                    "source": "311",
                    "score": float(row["count"]) * 0.95,
                    "line": line_municipal,
                    "line_municipal": line_municipal,
                    "line_public": line_public,
                    "name": str(row["quartier"]),
                    "count": int(row["count"]),
                    "type_service": str(row["type_service"]),
                }
            )

    if not coll_curr.empty and not stm.empty:
        coll = coll_curr.copy()
        coll["lat_zone"] = (coll["latitude"] / 0.004).round() * 0.004
        coll["lon_zone"] = (coll["longitude"] / 0.005).round() * 0.005

        stm_z = stm.copy()
        stm_z["lat_zone"] = (stm_z["latitude"] / 0.004).round() * 0.004
        stm_z["lon_zone"] = (stm_z["longitude"] / 0.005).round() * 0.005

        by_zone = (
            coll.groupby(["lat_zone", "lon_zone"])
            .agg(total=("gravite_num", "count"), graves=("gravite_num", lambda x: int((x >= 3).sum())))
            .reset_index()
        )
        stops = (
            stm_z.groupby(["lat_zone", "lon_zone"])
            .agg(stop_name=("stop_name", lambda x: ", ".join(pd.Series(x).dropna().astype(str).head(2))))
            .reset_index()
        )
        merged = by_zone.merge(stops, on=["lat_zone", "lon_zone"], how="inner").sort_values("total", ascending=False).head(3)

        for _, row in merged.iterrows():
            line_municipal = (
                f"Arrets {row['stop_name']} - {int(row['total'])} collisions "
                f"(dont {int(row['graves'])} graves) dans un rayon ~300 m."
            )
            line_public = (
                f"Autour des arrets {row['stop_name']} - {int(row['total'])} collisions "
                f"dans un rayon ~300 m."
            )
            candidates.append(
                {
                    "source": "STM",
                    "score": float(row["total"]) * 0.85,
                    "line": line_municipal,
                    "line_municipal": line_municipal,
                    "line_public": line_public,
                    "name": str(row["stop_name"]),
                    "count": int(row["total"]),
                    "graves": int(row["graves"]),
                }
            )

    if not candidates:
        return [{
            "source": "Collisions",
            "score": 0,
            "line": "Aucun hotspot detecte sur la periode selectionnee.",
            "line_municipal": "Aucun hotspot detecte sur la periode selectionnee.",
            "line_public": "Aucun point de vigilance detecte sur la periode selectionnee.",
            "name": "Aucun",
            "count": 0,
            "graves": 0,
        }]

    by_source = {"Collisions": [], "311": [], "STM": []}
    for c in sorted(candidates, key=lambda x: x["score"], reverse=True):
        by_source.setdefault(c["source"], []).append(c)

    selected: list[dict] = []
    quotas = [("Collisions", 2), ("311", 2), ("STM", 1)]
    for src, limit in quotas:
        selected.extend(by_source.get(src, [])[:limit])

    if len(selected) < 5:
        selected_ids = {id(x) for x in selected}
        leftovers = [c for c in sorted(candidates, key=lambda x: x["score"], reverse=True) if id(c) not in selected_ids]
        selected.extend(leftovers[: 5 - len(selected)])

    return selected[:5]


def _build_trends(
    coll_curr: pd.DataFrame,
    coll_prev: pd.DataFrame,
    req_curr: pd.DataFrame,
    req_prev: pd.DataFrame,
    coll_start: pd.Timestamp,
    coll_anchor: pd.Timestamp,
    req_start: pd.Timestamp,
    req_anchor: pd.Timestamp,
    days: int,
    tone: str = "municipal",
) -> list[str]:
    coll_curr_n = len(coll_curr)
    coll_prev_n = len(coll_prev)
    coll_var = _safe_pct(coll_curr_n, coll_prev_n)

    graves_curr = int((coll_curr.get("gravite_num", pd.Series(dtype=float)) >= 3).sum()) if not coll_curr.empty else 0
    graves_prev = int((coll_prev.get("gravite_num", pd.Series(dtype=float)) >= 3).sum()) if not coll_prev.empty else 0
    graves_var = _safe_pct(graves_curr, graves_prev)

    req_curr_focus = req_curr[req_curr.get("type_service", pd.Series(dtype=str)).astype(str).str.contains(HOTSPOT_311_REGEX, case=False, na=False, regex=True)]
    req_prev_focus = req_prev[req_prev.get("type_service", pd.Series(dtype=str)).astype(str).str.contains(HOTSPOT_311_REGEX, case=False, na=False, regex=True)]
    req_curr_n = len(req_curr_focus)
    req_prev_n = len(req_prev_focus)
    req_var = _safe_pct(req_curr_n, req_prev_n)

    coll_phrase = "en hausse" if coll_var > 0 else "en baisse" if coll_var < 0 else "stable"
    graves_phrase = "augmente" if graves_var > 0 else "baisse" if graves_var < 0 else "reste stable"
    req_phrase = "augmente" if req_var > 0 else "baisse" if req_var < 0 else "reste stable"

    coll_period_txt = f"{coll_start.strftime('%d %b %Y')} au {coll_anchor.strftime('%d %b %Y')}"
    req_period_txt = f"{req_start.strftime('%d %b %Y')} au {req_anchor.strftime('%d %b %Y')}"

    if tone == "municipal":
        grav_pond_curr = float(pd.to_numeric(coll_curr.get("gravite_num"), errors="coerce").mean()) if not coll_curr.empty else 0.0
        grav_pond_prev = float(pd.to_numeric(coll_prev.get("gravite_num"), errors="coerce").mean()) if not coll_prev.empty else 0.0
        vuln_curr = int(((coll_curr.get("impliques_pietons", False)) | (coll_curr.get("impliques_cyclistes", False))).sum()) if not coll_curr.empty else 0
        vuln_prev = int(((coll_prev.get("impliques_pietons", False)) | (coll_prev.get("impliques_cyclistes", False))).sum()) if not coll_prev.empty else 0
        coll_var_raw_txt, _ = _raw_variation(coll_curr_n, coll_prev_n)
        vuln_var_raw_txt, _ = _raw_variation(vuln_curr, vuln_prev)

        return [
            (
                f"Periode {days}J glissante collisions ({coll_period_txt}) : volume brut {coll_curr_n} vs {coll_prev_n} "
                f"(periode precedente), variation brute {coll_var_raw_txt}."
            ),
            (
                f"Taux de gravite pondere moyen: {grav_pond_curr} vs {grav_pond_prev}; "
                f"collisions graves {graves_curr} vs {graves_prev} ({graves_phrase})."
            ),
            (
                f"Collisions impliquant un usager vulnerable: {vuln_curr} vs {vuln_prev} "
                f"(variation brute {vuln_var_raw_txt}) ; requetes 311 ciblees {req_curr_n} vs {req_prev_n} "
                f"sur {req_period_txt} ({req_phrase})."
            ),
        ]

    coll_ctx = f"{abs(coll_var):.1f}% de plus" if coll_var > 0 else f"{abs(coll_var):.1f}% de moins" if coll_var < 0 else "au meme niveau"
    graves_ctx = f"{abs(graves_var):.1f}% de plus" if graves_var > 0 else f"{abs(graves_var):.1f}% de moins" if graves_var < 0 else "au meme niveau"
    req_ctx = f"{abs(req_var):.1f}% de plus" if req_var > 0 else f"{abs(req_var):.1f}% de moins" if req_var < 0 else "au meme niveau"
    return [
        (
            f"{coll_curr_n} accidents sur les {days} derniers jours: c'est {coll_ctx} que la periode precedente ({coll_prev_n})."
        ),
        (
            f"Les accidents graves sont {graves_ctx} ({graves_curr} contre {graves_prev}) sur la meme fenetre."
        ),
        (
            f"Cote signalements de voirie (311), on est {req_ctx} ({req_curr_n} vs {req_prev_n}) sur {req_period_txt}."
        ),
    ]


def _build_weak_signals(collisions: pd.DataFrame, req311: pd.DataFrame, days: int) -> list[str]:
    signals: list[tuple[float, str]] = []

    req_anchor = req311["_dt"].max() if not req311.empty else pd.Timestamp(datetime.now())
    coll_anchor = collisions["_dt"].max() if not collisions.empty else pd.Timestamp(datetime.now())
    lookback = max(42, days * 2)

    req6 = req311[(req311["_dt"] >= req_anchor - pd.Timedelta(days=lookback)) & (req311["_dt"] <= req_anchor)].copy()
    if not req6.empty:
        req6["week"] = req6["_dt"].dt.to_period("W").astype(str)
        grouped = (
            req6.groupby(["quartier", "type_service", "week"])
            .size()
            .reset_index(name="n")
        )
        for (quartier, t), g in grouped.groupby(["quartier", "type_service"]):
            vals = g.sort_values("week")["n"].to_numpy()
            if len(vals) < 4:
                continue
            cut = len(vals) // 2
            base = float(vals[:cut].mean())
            recent = float(vals[cut:].mean())
            if recent >= 5 and recent > base * 1.6 and recent <= 40:
                gain = recent - base
                signals.append((gain, f"Dans {quartier}, les requetes 311 « {t} » passent de {base:.1f}/sem a {recent:.1f}/sem sur 6 semaines."))

    coll6 = collisions[(collisions["_dt"] >= coll_anchor - pd.Timedelta(days=lookback)) & (collisions["_dt"] <= coll_anchor)].copy()
    if not coll6.empty:
        coll6["week"] = coll6["_dt"].dt.to_period("W").astype(str)
        coll_w = coll6.groupby(["intersection", "week"]).size().reset_index(name="n")
        for inter, g in coll_w.groupby("intersection"):
            vals = g.sort_values("week")["n"].to_numpy()
            if len(vals) < 4:
                continue
            cut = len(vals) // 2
            base = float(vals[:cut].mean())
            recent = float(vals[cut:].mean())
            if recent >= 3 and recent <= 12 and recent > base * 1.7:
                gain = recent - base
                signals.append((gain, f"Micro-hotspot emergent a {inter}: {base:.1f}/sem -> {recent:.1f}/sem sur 6 semaines."))

    coll_curr = collisions[(collisions["_dt"] >= coll_anchor - pd.Timedelta(days=days - 1)) & (collisions["_dt"] <= coll_anchor)].copy()
    coll_prev = collisions[(collisions["_dt"] >= coll_anchor - pd.Timedelta(days=2 * days - 1)) & (collisions["_dt"] <= coll_anchor - pd.Timedelta(days=days))].copy()
    for cond in ["Glacée/Verglacée", "Enneigée", "Mouillée"]:
        c_now = int((coll_curr.get("condition_meteo", pd.Series(dtype=str)) == cond).sum())
        c_prev = int((coll_prev.get("condition_meteo", pd.Series(dtype=str)) == cond).sum())
        if c_now >= 5 and c_now > c_prev * 1.3 and c_now <= 120:
            signals.append((float(c_now - c_prev), f"Collisions en condition « {cond} » en hausse ({c_prev} -> {c_now}) sur la derniere semaine."))

    if not signals and not coll_curr.empty:
        by_inter = (
            coll_curr.groupby("intersection")
            .agg(total=("gravite_num", "count"), graves=("gravite_num", lambda x: int((x >= 3).sum())))
            .reset_index()
        )
        by_inter["grave_ratio"] = by_inter["graves"] / by_inter["total"].clip(lower=1)
        top = by_inter.sort_values(["grave_ratio", "total"], ascending=False).head(1)
        if not top.empty:
            r = top.iloc[0]
            signals.append((r["grave_ratio"], f"Surveillance prioritaire a {r['intersection']}: {int(r['graves'])}/{int(r['total'])} collisions graves cette semaine."))

    signals.sort(key=lambda x: x[0], reverse=True)
    lines = [s[1] for s in signals[:3]]
    while len(lines) < 3:
        lines.append("Signal faible: variation locale en cours de validation, a confirmer avec 1-2 semaines supplementaires.")
    return lines


def _hotspot_line_for_tone(hotspot: dict, tone: str) -> str:
    if tone == "municipal":
        return hotspot.get("line_municipal", hotspot.get("line", ""))
    return hotspot.get("line_public", hotspot.get("line", ""))


def _signal_for_tone(signal: str, tone: str) -> str:
    if tone == "municipal":
        return signal
    s = signal
    s = s.replace("Micro-hotspot emergent", "Zone a surveiller")
    s = s.replace("Surveillance prioritaire", "Point de vigilance")
    s = s.replace("requetes 311", "signalements 311")
    s = s.replace("sur 6 semaines", "sur les dernieres semaines")
    return s


def _build_recommendations(hotspots: list[dict], trends: list[str], weak_signals: list[str], tone: str) -> list[str]:
    coll_hotspots = [h for h in hotspots if h["source"] == "Collisions"]
    req_hotspots = [h for h in hotspots if h["source"] == "311"]
    stm_hotspots = [h for h in hotspots if h["source"] == "STM"]

    if tone == "municipal":
        recos = []
        if req_hotspots:
            r = req_hotspots[0]
            recos.append(
                f"Pre-positionner equipes deneigement/voirie sur le secteur {r.get('name', 'prioritaire')} "
                f"(volume 311 brut: {int(r.get('count', 0))})."
            )
        if coll_hotspots:
            c = coll_hotspots[0]
            recos.append(
                f"Audit signalisation et phasage feux a l'intersection {c.get('name', 'prioritaire')} "
                f"(collisions graves: {int(c.get('graves', 0))})."
            )
            c_patrol = coll_hotspots[1] if len(coll_hotspots) > 1 else c
            recos.append(
                f"Intensifier patrouilles 16h-19h sur {c_patrol.get('name', 'axe prioritaire')} "
                f"et controle vitesse cible."
            )
        if stm_hotspots:
            s = stm_hotspots[0]
            recos.append(
                f"Inspection securite autour des arrets {s.get('name', 'STM prioritaire')} "
                f"(collisions dans le rayon: {int(s.get('count', 0))})."
            )
        recos.append("Declencher un suivi hebdomadaire en periode 30J glissante pour mesurer l'effet des actions correctives.")
        return recos[:5]

    recos = []
    if coll_hotspots:
        c = coll_hotspots[0]
        recos.append(
            f"Evitez la zone {c.get('name', 'la plus exposee')} entre 16h et 19h quand c'est possible."
        )
    if req_hotspots:
        r = req_hotspots[0]
        recos.append(
            f"Si vous voyez des problemes de voirie dans {r.get('name', 'votre quartier')}, signalez-les vite au 311."
        )
    recos.append(f"Cette semaine, point de vigilance: {weak_signals[0]}")
    recos.append("Par verglas ou neige, prevoyez 10 minutes de plus et gardez plus de distance de securite.")
    recos.append("A pied ou a velo, privilegiez les axes eclaires et les traversees balisees aux heures de pointe.")
    return recos[:5]


def _build_municipal_hotspot_table(coll_curr: pd.DataFrame, coll_prev: pd.DataFrame, days: int) -> str:
    if coll_curr.empty:
        return (
            f"""<div style="font-size:12px;color:{C['text3']};">"""
            "Aucune collision exploitable pour la table technique sur la periode selectionnee."
            "</div>"
        )

    curr = (
        coll_curr.groupby("intersection")
        .agg(
            nb_collisions=("gravite_num", "count"),
            nb_graves=("gravite_num", lambda x: int((x >= 3).sum())),
            gravite_ponderee=("gravite_num", "mean"),
            heure_moyenne=("heure", "mean"),
        )
        .reset_index()
    )
    prev = coll_prev.groupby("intersection").size().reset_index(name="prev_collisions")
    merged = curr.merge(prev, on="intersection", how="left").fillna({"prev_collisions": 0})
    merged = merged.sort_values("nb_collisions", ascending=False).head(5)

    rows = ""
    for _, row in merged.iterrows():
        var_txt, var_val = _raw_variation(int(row["nb_collisions"]), int(row["prev_collisions"]))
        var_color = C["red"] if (var_val is not None and var_val > 0) else C["green"] if (var_val is not None and var_val < 0) else C["text2"]
        rows += (
            "<tr>"
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-size:12px;color:{C['text2']};">{row['intersection']}</td>"""
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:12px;color:{C['text']};">{int(row['nb_collisions'])}</td>"""
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:12px;color:{C['text']};">{int(row['nb_graves'])}</td>"""
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:12px;color:{C['text']};">{float(row['gravite_ponderee'])}</td>"""
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:12px;color:{C['text']};">{float(row['heure_moyenne'])}h</td>"""
            f"""<td style="padding:8px 6px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:11px;color:{var_color};">{var_txt}</td>"""
            "</tr>"
        )

    header_row = (
        "<tr>"
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Zone / intersection</th>"""
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Collisions</th>"""
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Collisions graves</th>"""
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Taux gravite pondere</th>"""
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Heure moyenne</th>"""
        f"""<th style="text-align:left;padding:8px 6px;font-family:{C['mono']};font-size:10px;color:{C['text3']};">Tendance % ({days}J)</th>"""
        "</tr>"
    )
    return (
        f"""<div style="overflow-x:auto;">"""
        f"""<table style="width:100%;border-collapse:collapse;">{header_row}{rows}</table>"""
        "</div>"
    )


def generate_briefing(
    data: dict,
    tone: str = "municipal",
    periode: str = "30 derniers jours",
    reference_data: dict | None = None,
) -> str:
    days = _period_days(periode)
    period_label = f"{days} jours"
    tone_profile = _tone_profile(tone)

    collisions, req311, stm, anchor = _prepare_frames(data)
    if reference_data is not None:
        ref_collisions, ref_req311, _, _ = _prepare_frames(reference_data)
    else:
        ref_collisions, ref_req311 = collisions, req311

    coll_curr, coll_prev, req_curr, req_prev, coll_start, coll_anchor, req_start, req_anchor = _period_windows(
        collisions,
        req311,
        ref_collisions,
        ref_req311,
        days,
    )

    hotspots = _build_hotspots(coll_curr, req_curr, stm, days)
    trends = _build_trends(
        coll_curr,
        coll_prev,
        req_curr,
        req_prev,
        coll_start,
        coll_anchor,
        req_start,
        req_anchor,
        days,
        tone=tone,
    )
    weak_signals = _build_weak_signals(ref_collisions, ref_req311, days)
    weak_signals_display = [_signal_for_tone(s, tone) for s in weak_signals]
    recommendations = _build_recommendations(hotspots, trends, weak_signals_display, tone)

    coll_curr_n = len(coll_curr)
    coll_prev_n = len(coll_prev)
    coll_var = _safe_pct(coll_curr_n, coll_prev_n)
    coll_var_raw_txt, coll_var_raw_num = _raw_variation(coll_curr_n, coll_prev_n)
    var_color = C["red"] if coll_var > 0 else C["green"] if coll_var < 0 else C["blue"]
    var_bg = C["red_bg"] if coll_var > 0 else C["green_bg"] if coll_var < 0 else C["blue_bg"]

    req_focus_curr = req_curr[req_curr.get("type_service", pd.Series(dtype=str)).astype(str).str.contains(HOTSPOT_311_REGEX, case=False, na=False, regex=True)]
    req_focus_prev = req_prev[req_prev.get("type_service", pd.Series(dtype=str)).astype(str).str.contains(HOTSPOT_311_REGEX, case=False, na=False, regex=True)]
    req_curr_total_n = len(req_curr)
    req_focus_curr_n = len(req_focus_curr)
    req_var = _safe_pct(req_focus_curr_n, len(req_focus_prev))
    req_var_raw_txt, req_var_raw_num = _raw_variation(req_focus_curr_n, len(req_focus_prev))
    req_color = C["red"] if req_var > 0 else C["green"] if req_var < 0 else C["blue"]

    badge_label = "Municipalite" if tone == "municipal" else "Grand public"
    badge = _tag(badge_label, C["text2"], C["subtle"])
    global_anchor = max([a for a in [coll_anchor, req_anchor] if pd.notna(a)], default=anchor)
    week_num = global_anchor.isocalendar().week
    now_str = global_anchor.strftime("%d %B %Y")

    if tone == "municipal":
        intro = _card(
            "Perspective municipalite",
            (
                f"Lecture technique pour decideurs, ingenieurs et chefs d'arrondissement sur une "
                f"<strong>periode {days}J glissante</strong> : indicateurs bruts, risque pondere et actions operationnelles."
            ),
            tone_profile["accent"],
        )
    else:
        intro = _card(
            "Perspective grand public",
            (
                f"Lecture citoyenne pour les <strong>{days} derniers jours</strong> : ou faire attention, "
                "quoi signaler et quels gestes utiles adopter au quotidien."
            ),
            tone_profile["accent"],
        )

    briefing_title = "Briefing technique mobilite &amp; securite" if tone == "municipal" else "Briefing citoyen mobilite &amp; securite"
    header = (
        f"""<div style="border-bottom:1px solid {C['border']};padding-bottom:18px;margin-bottom:20px;">"""
        f"""<div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px;">"""
        f"""<div><div style="font-size:18px;font-weight:600;color:{C['text']};letter-spacing:-0.02em;margin-bottom:6px;">{briefing_title}</div>"""
        f"""<div style="font-family:{C['mono']};font-size:11px;color:{C['text3']};">Semaine {week_num} · {now_str} · {badge} · Fenetre: {periode}</div></div>"""
        f"""<div style="text-align:right;"><div style="font-family:{C['mono']};font-size:28px;font-weight:700;color:{C['text']};line-height:1;">{coll_curr_n}</div>"""
        f"""<div style="font-family:{C['mono']};font-size:10px;color:{C['text3']};margin-top:2px;">collisions ({period_label})</div></div>"""
        "</div></div>"
    )
    status_label, status_detail, status_level = _compute_briefing_status(coll_curr, req_curr, coll_anchor, req_anchor)
    status_html = _status_block(status_label, status_detail, status_level)
    role_html = _role_strip(tone, period_label)
    insight_html = _main_insight_block(
        tone=tone,
        hotspots=hotspots,
        coll_curr_n=coll_curr_n,
        coll_var=coll_var,
        period_label=period_label,
    )

    if tone == "municipal":
        kpi_1_label = "Collisions (brut)"
        if coll_curr_n == 0:
            kpi_1_sub = "Aucune collision enregistree sur cette fenetre temporelle."
        else:
            kpi_1_sub = f"{coll_curr_n} en {days}J glissante | precedent: {coll_prev_n}"
        kpi_2_label = "Variation brute collisions"
        kpi_2_sub = coll_var_raw_txt
        kpi_3_label = "311 cibles (brut)"
        if req_focus_curr_n == 0 and req_curr_total_n == 0:
            kpi_3_sub = "Donnees 311 non disponibles sur cette fenetre temporelle."
        elif req_focus_curr_n == 0:
            kpi_3_sub = "Aucun signalement 311 cible sur la fenetre active."
        else:
            kpi_3_sub = req_var_raw_txt
        kpi_2_value = f"{coll_curr_n - coll_prev_n:+d}"
    else:
        kpi_1_label = "Accidents"
        if coll_curr_n == 0:
            kpi_1_sub = "Aucun accident enregistre sur la fenetre selectionnee."
        else:
            kpi_1_sub = f"{abs(coll_var):.1f}% de {'plus' if coll_var > 0 else 'moins' if coll_var < 0 else 'variation'} que la periode precedente"
        kpi_2_label = "Evolution du risque"
        kpi_2_sub = "comparaison avec la periode precedente"
        kpi_3_label = "Signalements voirie"
        if req_focus_curr_n == 0 and req_curr_total_n == 0:
            kpi_3_sub = "Donnees de signalements non disponibles sur cette fenetre temporelle."
        elif req_focus_curr_n == 0:
            kpi_3_sub = "Aucun signalement voirie cible sur cette fenetre."
        else:
            kpi_3_sub = f"{abs(req_var):.1f}% de {'plus' if req_var > 0 else 'moins' if req_var < 0 else 'variation'} que la periode precedente"
        kpi_2_value = _fmt_pct(coll_var)

    kpis = (
        f"""<div style="display:flex;gap:12px;margin-bottom:18px;flex-wrap:wrap;">"""
        f"""{_kpi(kpi_1_label, str(coll_curr_n), kpi_1_sub)}"""
        f"""{_kpi(kpi_2_label, kpi_2_value, kpi_2_sub, var_color)}"""
        f"""{_kpi(kpi_3_label, str(req_focus_curr_n), kpi_3_sub, req_color)}"""
        "</div>"
    )

    zero_notes: list[str] = []
    if tone == "municipal":
        zero_note_title = "Contexte operationnel des metriques a 0"
        if coll_curr_n == 0 and req_focus_curr_n > 0:
            zero_notes.append(
                "Aucune collision enregistree sur la fenetre active : le classement technique est principalement alimente par les volumes 311 cibles."
            )
        elif coll_curr_n == 0 and req_focus_curr_n == 0:
            zero_notes.append(
                "Aucun evenement exploitable (collisions / 311 cibles) sur cette fenetre : verifier l'ingestion ou elargir la plage temporelle."
            )

        if req_focus_curr_n == 0 and req_curr_total_n > 0:
            zero_notes.append(
                "Flux 311 present, mais aucun enregistrement n'entre dans le panier operationnel voirie/de-neigement/eclairage."
            )
        elif req_focus_curr_n == 0 and req_curr_total_n == 0:
            zero_notes.append(
                "Flux 311 indisponible sur cette fenetre temporelle."
            )
    else:
        zero_note_title = "Contexte des metriques a 0"
        if coll_curr_n == 0 and req_focus_curr_n > 0:
            zero_notes.append(
                "Aucun accident enregistre sur cette periode : le classement affiché s'appuie surtout sur les signalements 311."
            )
        elif coll_curr_n == 0 and req_focus_curr_n == 0:
            zero_notes.append(
                "Aucun accident ni signalement cible sur cette periode : essayez une plage plus large."
            )

        if req_focus_curr_n == 0 and req_curr_total_n > 0:
            zero_notes.append(
                "Des signalements 311 existent, mais aucun ne concerne le panier voirie/de-neigement/eclairage de ce briefing."
            )
        elif req_focus_curr_n == 0 and req_curr_total_n == 0:
            zero_notes.append(
                "Les donnees de signalements 311 ne sont pas disponibles sur cette fenetre temporelle."
            )

    zero_note_html = ""
    if zero_notes:
        zero_note_items = "".join(
            f"""<li style="margin-bottom:5px;">{n}</li>""" for n in zero_notes
        )
        zero_note_html = (
            f"""<div style="border:1px solid {C['border']};border-radius:10px;padding:10px 12px;background:{C['subtle']};margin:-4px 0 14px 0;">"""
            f"""<div style="font-family:{C['mono']};font-size:10px;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;margin-bottom:6px;">{zero_note_title}</div>"""
            f"""<ul style="margin:0;padding-left:16px;font-size:12px;color:{C['text2']};line-height:1.6;">{zero_note_items}</ul>"""
            "</div>"
        )

    section_title = f"font-family:{C['mono']};font-size:10px;font-weight:600;color:{C['text3']};letter-spacing:0.08em;text-transform:uppercase;margin-bottom:10px;"

    if tone == "municipal":
        hotspots_table = _build_municipal_hotspot_table(coll_curr, coll_prev, days)
        hotspots_sec = (
            f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['subtle']};">"""
            f"""<div style="{section_title}">① Tableau des hotspots techniques</div>"""
            f"""<div style="font-size:12px;color:{C['text3']};margin-bottom:10px;">"""
            f"""Critere: concentration d'evenements sur {days}J glissante, avec colonnes techniques brutes."""
            "</div>"
            f"""{hotspots_table}"""
            "</div>"
        )
    else:
        hotspot_tiles = ""
        for i, h in enumerate(hotspots, start=1):
            src_color = C["red"] if h["source"] == "Collisions" else C["orange"] if h["source"] == "311" else C["blue"]
            src_bg = C["red_bg"] if h["source"] == "Collisions" else C["orange_bg"] if h["source"] == "311" else C["blue_bg"]
            featured = "grid-column:span 2;" if i == 1 else ""
            label = "Zone" if tone == "public" else "Hotspot"
            source_label = "Accidents" if h["source"] == "Collisions" else "Signalements" if h["source"] == "311" else "STM"
            hotspot_tiles += (
                f"""<div style="border:1px solid {C['border']};border-radius:10px;padding:12px 14px;background:{C['bg']};{featured}">"""
                f"""<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:7px;">"""
                f"""<span style="font-family:{C['mono']};font-size:10px;color:{C['text3']};">{label} #{i}</span>"""
                f"""<span style="font-family:{C['mono']};font-size:10px;color:{src_color};background:{src_bg};padding:2px 7px;border-radius:999px;">{source_label}</span>"""
                "</div>"
                f"""<div style="font-size:13px;line-height:1.65;color:{C['text2']};">{_hotspot_line_for_tone(h, tone)}</div>"""
                "</div>"
            )
        hotspots_sec = (
            f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['subtle']};">"""
            f"""<div style="{section_title}">① Top 5 zones dangereuses</div>"""
            f"""<div style="font-size:12px;color:{C['text3']};margin-bottom:10px;">"""
            f"""Critere: endroits avec le plus d'accidents et de signalements sur {period_label}."""
            "</div>"
            f"""<div class="brief-zone-grid">{hotspot_tiles}</div>"""
            "</div>"
        )

    trend_items = "".join(
        f"""<li style="padding:9px 0;border-bottom:1px solid {C['border']};font-size:13px;color:{C['text2']};line-height:1.6;">{t}</li>"""
        for t in trends
    )
    trend_title = "② Tendances techniques" if tone == "municipal" else "② Ce qui change sur la periode"
    trends_sec = (
        f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['subtle']};">"""
        f"""<div style="{section_title}">{trend_title}</div>"""
        f"""<ul style="list-style:none;padding:0;margin:0;">{trend_items}</ul>"""
        "</div>"
    )

    signal_items = "".join(
        f"""<li style="padding:9px 0;border-bottom:1px solid {C['border']};font-size:13px;color:{C['text2']};line-height:1.6;">{s}</li>"""
        for s in weak_signals_display
    )
    weak_title = "③ Signaux faibles" if tone == "municipal" else "③ Tendances locales a surveiller"
    weak_sec = (
        f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['subtle']};">"""
        f"""<div style="{section_title}">{weak_title}</div>"""
        f"""<ul style="list-style:none;padding:0;margin:0;">{signal_items}</ul>"""
        "</div>"
    )

    reco_title = "④ Recommandations operationnelles" if tone == "municipal" else "④ Recommandations grand public"
    reco_sub = (
        "Plan d'action priorise pour les equipes terrain."
        if tone == "municipal"
        else "Conseils pratiques a appliquer des maintenant."
    )
    reco_items = ""
    for i, r in enumerate(recommendations, start=1):
        reco_items += (
            f"""<div style="border:1px solid {C['border']};border-radius:10px;padding:10px 12px;background:{C['bg']};display:flex;gap:10px;align-items:flex-start;">"""
            f"""<span style="font-family:{C['mono']};font-size:10px;min-width:18px;height:18px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;background:{C['text']};color:var(--bg);">{i}</span>"""
            f"""<span style="font-size:13px;color:{C['text2']};line-height:1.6;">{r}</span>"""
            "</div>"
        )
    reco_sec = (
        f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['subtle']};">"""
        f"""<div style="{section_title}">{reco_title}</div>"""
        f"""<div style="font-size:12px;color:{C['text3']};margin-bottom:6px;">{reco_sub}</div>"""
        f"""<div class="brief-reco-grid">{reco_items}</div>"""
        "</div>"
    )

    coll_trend_word = "hausse" if coll_var > 0 else "baisse" if coll_var < 0 else "stable"
    quick_title = "A retenir (operationnel)" if tone == "municipal" else "A retenir (citoyen)"
    quick_last_label = "Zone prioritaire" if tone == "municipal" else "Zone dangereuse principale"
    quick_hotspot_line = _hotspot_line_for_tone(hotspots[0], tone) if hotspots else "Aucune zone prioritaire detectee sur la fenetre."

    if tone == "municipal":
        quick_items = (
            f"""<li>Volume brut collisions: {coll_curr_n} (periode glissante {days}J), precedent: {coll_prev_n}.</li>"""
            f"""<li>Variation brute collisions: {coll_var_raw_txt}.</li>"""
            f"""<li>{quick_last_label}: {quick_hotspot_line}</li>"""
        )
    else:
        quick_items = (
            f"""<li>{coll_curr_n} accidents sur {period_label} ({coll_trend_word} {abs(coll_var):.1f}%).</li>"""
            f"""<li>{req_focus_curr_n} signalements 311 cibles sur la fenetre.</li>"""
            f"""<li>{quick_last_label}: {quick_hotspot_line}</li>"""
        )
    quick_sec = (
        f"""<div style="border:1px solid {C['border']};border-radius:12px;padding:14px;background:{C['bg']};">"""
        f"""<div style="{section_title}">{quick_title}</div>"""
        f"""<ul style="margin:0;padding-left:16px;font-size:13px;color:{C['text2']};line-height:1.7;">{quick_items}</ul>"""
        "</div>"
    )
    decision_sec = _briefing_decision_block(
        tone=tone,
        period_label=period_label,
        coll_curr=coll_curr,
        req_focus_curr_n=req_focus_curr_n,
        hotspots=hotspots,
        coll_start=coll_start,
        coll_anchor=coll_anchor,
        req_start=req_start,
        req_anchor=req_anchor,
        recommendations=recommendations,
    )

    footer = (
        f"""<div style="margin-top:18px;padding-top:12px;border-top:1px solid {C['border']};font-family:{C['mono']};font-size:10px;color:{C['text3']};letter-spacing:0.04em;">"""
        f"""Sources : 311 Montreal · Collisions QC · STM GTFS · Fenetre selectionnee : {periode} · Periode collisions : {coll_start.strftime('%Y-%m-%d')} -> {coll_anchor.strftime('%Y-%m-%d')} · Periode 311 : {req_start.strftime('%Y-%m-%d')} -> {req_anchor.strftime('%Y-%m-%d')}"""
        "</div>"
    )

    overview_block = (
        f"""<div class="brief-top-grid">"""
        f"""<div>{hotspots_sec}</div>"""
        f"""<div>{quick_sec}</div>"""
        "</div>"
    )
    trends_block = (
        f"""<div class="brief-bottom-grid">"""
        f"""<div>{trends_sec}</div>"""
        f"""<div>{weak_sec}</div>"""
        "</div>"
    )

    context_sec = _accordion(
        "Contexte d'analyse",
        _finality_strip(tone) + _reading_path_strip(tone, period_label) + role_html + intro,
        subtitle="Fiabilité, rôle et périmètre",
        opened=False,
    )

    if tone == "municipal":
        zone_title = "01 · Zones prioritaires opérationnelles"
        zone_subtitle = "Où agir en priorité"
        reco_title_acc = "02 · Plan d'action terrain"
        reco_subtitle_acc = "Actions opérationnelles priorisées"
        trend_title_acc = "03 · Tendances de pilotage"
        trend_subtitle_acc = "Variables à monitorer"
        sources_title_acc = "04 · Sources & période"
        sources_subtitle_acc = "Traçabilité des données"

        body_sections = (
            context_sec
            + _accordion(zone_title, overview_block, subtitle=zone_subtitle, opened=True)
            + _accordion(reco_title_acc, reco_sec, subtitle=reco_subtitle_acc, opened=True)
            + _accordion(trend_title_acc, trends_block, subtitle=trend_subtitle_acc, opened=False)
            + _accordion(sources_title_acc, footer, subtitle=sources_subtitle_acc, opened=False)
        )
    else:
        zone_title = "01 · Zones de vigilance"
        zone_subtitle = "Où rester prudent"
        reco_title_acc = "02 · Conseils pratiques"
        reco_subtitle_acc = "Gestes utiles au quotidien"
        trend_title_acc = "03 · Tendances locales"
        trend_subtitle_acc = "Signaux à surveiller"
        sources_title_acc = "04 · Sources & période"
        sources_subtitle_acc = "Transparence des données"

        body_sections = (
            context_sec
            + _accordion(zone_title, overview_block, subtitle=zone_subtitle, opened=True)
            + _accordion(reco_title_acc, reco_sec, subtitle=reco_subtitle_acc, opened=True)
            + _accordion(trend_title_acc, trends_block, subtitle=trend_subtitle_acc, opened=False)
            + _accordion(sources_title_acc, footer, subtitle=sources_subtitle_acc, opened=False)
        )

    layout_css = f"""
<style>
.brief-accordion {{
  border:1px solid {C['border']};
  border-radius:12px;
  background:{C['bg']};
  margin:0 0 14px 0;
  overflow:hidden;
}}
.brief-accordion > summary {{
  list-style:none;
  cursor:pointer;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  padding:11px 14px;
  background:{C['bg']};
  border-bottom:1px solid transparent;
  transition:background 160ms ease, border-color 160ms ease;
}}
.brief-accordion > summary::-webkit-details-marker {{
  display:none;
}}
.brief-accordion > summary::after {{
  content:"+";
  font-family:{C['mono']};
  font-size:14px;
  color:{C['text3']};
  line-height:1;
}}
.brief-accordion[open] > summary {{
  border-bottom-color:{C['border']};
  background:{C['subtle']};
}}
.brief-accordion[open] > summary::after {{
  content:"−";
  color:{C['text2']};
}}
.brief-acc-title {{
  font-family:{C['mono']};
  font-size:11px;
  font-weight:600;
  color:{C['text2']};
  letter-spacing:0.08em;
  text-transform:uppercase;
}}
.brief-accordion-body {{
  padding:14px 14px 12px 14px;
  background:{C['bg']};
}}
.brief-top-grid {{
  display:grid;
  grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
  gap:12px;
  align-items:start;
}}
.brief-bottom-grid {{
  display:grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap:12px;
  align-items:start;
}}
.brief-zone-grid {{
  display:grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap:10px;
}}
.brief-reco-grid {{
  display:grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap:10px;
}}
.brief-decision-grid {{
  display:grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap:10px;
}}
@media (max-width: 980px) {{
  .brief-top-grid,
  .brief-bottom-grid,
  .brief-zone-grid,
  .brief-reco-grid,
  .brief-decision-grid {{
    grid-template-columns: 1fr !important;
  }}
  .brief-accordion > summary {{
    flex-direction:column;
    align-items:flex-start;
  }}
  .brief-zone-grid > div {{
    grid-column: auto !important;
  }}
}}
</style>
"""

    decision_top_html = f"""<div style="margin:2px 0 12px 0;">{decision_sec}</div>"""
    return f"""<div style="font-family:{C['sans']};background:{C['bg']};border:1px solid {C['border']};border-radius:16px;padding:14px 14px 12px 14px;overflow:hidden;">{layout_css}{header}{insight_html}{status_html}{kpis}{decision_top_html}{zero_note_html}{body_sections}</div>"""
