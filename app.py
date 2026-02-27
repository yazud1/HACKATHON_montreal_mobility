import streamlit as st
import pandas as pd
import numpy as np
import json
import re
import html as html_lib
from datetime import datetime, timedelta, date
import plotly.express as px
import plotly.graph_objects as go
import streamlit.components.v1 as components
from data_loader import load_all_data
from rag_engine import RAGEngine
from query_engine import QueryEngine
from briefing import generate_briefing
import base64, pathlib


# ─── LOGO LOADER ──────────────────────────────────────────────────────────────
def _image_data_uri(*filenames: str) -> str | None:
    """Retourne un data URI pour la première image locale trouvée."""
    base = pathlib.Path(__file__).parent
    mime_map = {
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    for filename in filenames:
        path = base / filename
        if path.exists() and path.is_file():
            mime = mime_map.get(path.suffix.lower(), "application/octet-stream")
            return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}"
    return None

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mobility Copilot · Montréal",
    page_icon="logo.svg",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── UI THEME STATE ──────────────────────────────────────────────────────────
if "ui_theme" not in st.session_state:
    st.session_state.ui_theme = "Clair"
if st.session_state.ui_theme not in {"Clair", "Sombre"}:
    st.session_state.ui_theme = "Clair"
if "theme_dark_toggle" not in st.session_state:
    st.session_state.theme_dark_toggle = st.session_state.ui_theme == "Sombre"
st.session_state.ui_theme = "Sombre" if st.session_state.get("theme_dark_toggle", False) else "Clair"

PAGE_OPTIONS = ["Chat analytique", "Briefing", "Dashboard"]
if "current_page" not in st.session_state:
    st.session_state.current_page = "Chat analytique"
if st.session_state.current_page not in PAGE_OPTIONS:
    st.session_state.current_page = "Chat analytique"
# Synchronise dès le début du run pour éviter tout décalage header/contenu.
if st.session_state.get("sidebar_page") in PAGE_OPTIONS:
    st.session_state.current_page = st.session_state["sidebar_page"]

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None
if "pending_ambiguity" not in st.session_state:
    st.session_state.pending_ambiguity = None


def _reset_chat_state() -> None:
    st.session_state.chat_history = []
    st.session_state.pending_question = None
    st.session_state.pending_ambiguity = None
    if "amb_choice_idx" in st.session_state:
        del st.session_state["amb_choice_idx"]


# ─── PREMIUM MINIMAL CSS ─────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@300;400;500;600&family=Geist+Mono:wght@400;500&display=swap');

/* ── Reset & base ── */
*, *::before, *::after { box-sizing: border-box; margin: 0; }

:root {
    --white:      #ffffff;
    --bg:         #f4f9ff;
    --bg-subtle:  #ebf3fe;
    --border:     #d4e2f4;
    --border-mid: #b8cfe8;
    --text:       #16324f;
    --text-2:     #2a4f77;
    --text-3:     #6f8faf;
    --accent:     #6ea3d4;
    --accent-strong: #4f86b9;
    --accent-dim: #6ea3d41f;
    --green:      #16a34a;
    --orange:     #ea580c;
    --red:        #dc2626;
    --radius:     10px;
    --font:       'Geist', -apple-system, sans-serif;
    --font-mono:  'Geist Mono', 'SF Mono', monospace;

    --mc-card-bg: var(--white);
    --mc-surface: var(--bg-subtle);
    --mc-border: var(--border);
    --mc-text: var(--text);
    --mc-text-muted: var(--text-2);
    --mc-text-subtle: var(--text-3);
    --mc-accent: var(--accent);
    --mc-accent-strong: var(--accent-strong);
    --mc-warn-bg: #fff7ed;
    --mc-warn-border: #fed7aa;
    --mc-warn: var(--orange);
    --mc-warn-soft: #ea580c33;
    --mc-danger: var(--red);
    --mc-success: var(--green);
    --mc-input-bg-start: var(--white);
    --mc-input-bg-end: var(--bg-subtle);
    --mc-input-border: var(--border-mid);
    --mc-input-border-hover: var(--accent-strong);
    --mc-chat-field-bg: var(--mc-card-bg);
}

html, body, [class*="css"], .main, .block-container {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-size: 14px !important;
    line-height: 1.6 !important;
    -webkit-font-smoothing: antialiased !important;
}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, .stDeployButton,
[data-testid="stToolbar"], [data-testid="stDecoration"] {
    display: none !important;
}

/* Conserve un header technique minimal pour garder le contrôle de sidebar */
[data-testid="stHeader"] {
    background: transparent !important;
    height: 2.6rem !important;
    min-height: 2.6rem !important;
    border: none !important;
    position: sticky !important;
    top: 0 !important;
    z-index: 1001 !important;
}

/* Boutons sidebar robustes (Windows/macOS): laisser le contrôle natif cliquable */
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapsedControl"] {
    z-index: 1100 !important;
}

[data-testid="stSidebarCollapseButton"] button,
[data-testid="collapsedControl"] button,
[data-testid="stSidebarCollapsedControl"] button {
    position: relative !important;
    background: color-mix(in srgb, var(--mc-surface) 88%, transparent) !important;
    border: 1px solid color-mix(in srgb, var(--mc-border) 90%, transparent) !important;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.12) !important;
    padding: 0 !important;
    margin: 0 !important;
    color: transparent !important;
    font-size: 0 !important;
}
[data-testid="stSidebarCollapseButton"] button:hover,
[data-testid="collapsedControl"] button:hover,
[data-testid="stSidebarCollapsedControl"] button:hover {
    border-color: var(--mc-accent-strong) !important;
    background: color-mix(in srgb, var(--mc-surface) 72%, var(--mc-card-bg) 28%) !important;
}

/* Sidebar ouverte: bouton discret intégré */
[data-testid="stSidebarCollapseButton"] button {
    border-radius: 8px !important;
    width: 28px !important;
    min-width: 28px !important;
    height: 28px !important;
    min-height: 28px !important;
}


[data-testid="collapsedControl"] button,
[data-testid="stSidebarCollapsedControl"] button {
    border-left: none !important;
    border-radius: 0 12px 12px 0 !important;
    width: 30px !important;
    min-width: 30px !important;
    height: 44px !important;
    min-height: 44px !important;
}

/* On masque seulement le texte ligature, sans supprimer le bouton (évite le bug Windows) */
[data-testid="stSidebarCollapseButton"] button > *,
[data-testid="collapsedControl"] button > *,
[data-testid="stSidebarCollapsedControl"] button > * {
    opacity: 0 !important;
    pointer-events: none !important;
}

/* Flèche fallback stable si l'icône native ne s'affiche pas */
[data-testid="stSidebarCollapseButton"] button::after,
[data-testid="collapsedControl"] button::after,
[data-testid="stSidebarCollapsedControl"] button::after {
    position: absolute !important;
    inset: 0 !important;
    display: grid !important;
    place-items: center !important;
    font-family: var(--font) !important;
    font-size: 21px !important;
    font-weight: 500 !important;
    line-height: 1 !important;
    color: var(--mc-text-muted) !important;
}
[data-testid="stSidebarCollapseButton"] button::after { content: "‹"; }
[data-testid="collapsedControl"] button::after,
[data-testid="stSidebarCollapsedControl"] button::after { content: "›"; }

/* Fallback réouverture sidebar (affiché uniquement si contrôle natif absent) */
.mc-sidebar-fallback-reopen {
    position: fixed !important;
    left: 0 !important;
    top: 82px !important;
    width: 30px !important;
    min-width: 30px !important;
    height: 44px !important;
    min-height: 44px !important;
    display: none !important;
    place-items: center !important;
    border-left: none !important;
    border-radius: 0 12px 12px 0 !important;
    border: 1px solid color-mix(in srgb, var(--mc-border) 90%, transparent) !important;
    background: color-mix(in srgb, var(--mc-surface) 88%, transparent) !important;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.12) !important;
    color: var(--mc-text-muted) !important;
    font-family: var(--font) !important;
    font-size: 21px !important;
    line-height: 1 !important;
    z-index: 1300 !important;
    cursor: pointer !important;
}
.mc-sidebar-fallback-reopen:hover {
    border-color: var(--mc-accent-strong) !important;
    background: color-mix(in srgb, var(--mc-surface) 72%, var(--mc-card-bg) 28%) !important;
}

/* ── Main padding ── */
.block-container {
    padding: 2rem 2.5rem 4rem !important;
    max-width: 1200px !important;
    padding-bottom: 9rem !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--bg-subtle) 0%, var(--bg) 100%) !important;
    border-right: 1px solid var(--border) !important;
    padding-top: 1rem !important;
}
section[data-testid="stSidebar"] > div {
    padding: 0 0.9rem !important;
    display: flex !important;
    flex-direction: column !important;
    height: 100% !important;
}
section[data-testid="stSidebar"] * {
    color: var(--text) !important;
    font-family: var(--font) !important;
}
[data-testid="stSidebarNav"] { display: none; }
.sidebar-bottom { margin-top: auto !important; padding-top: 18px !important; }
section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    min-height: 36px !important;
    padding-top: 0 !important;
    padding-bottom: 0 !important;
}
@media (max-height: 920px) {
    .sidebar-bottom { margin-top: 10px !important; padding-top: 10px !important; }
}

/* ── Sidebar nav (radio -> pills fluides) ── */
section[data-testid="stSidebar"] [data-testid="stRadio"] > div[role="radiogroup"] {
    gap: 4px !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label {
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: 10px !important;
    padding: 6px 8px !important;
    margin: 0 !important;
    transition: background 180ms ease, border-color 180ms ease, transform 180ms ease, box-shadow 180ms ease !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: var(--mc-surface) !important;
    border-color: var(--border-mid) !important;
    transform: translateX(2px) !important;
    box-shadow: 0 2px 8px rgba(79, 134, 185, 0.14) !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: linear-gradient(180deg, var(--mc-card-bg) 0%, var(--mc-surface) 100%) !important;
    border-color: var(--accent) !important;
    transform: translateX(2px) !important;
    box-shadow: 0 3px 10px rgba(79, 134, 185, 0.2) !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label > div:first-child {
    display: none !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label > div:last-child {
    padding-left: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label > div:last-child p {
    font-family: var(--font) !important;
    font-size: 13px !important;
    line-height: 1.35 !important;
    font-weight: 500 !important;
    color: var(--text) !important;
    letter-spacing: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) > div:last-child p {
    color: var(--accent-strong) !important;
    font-weight: 600 !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border) !important;
    border-radius: 0 !important;
    padding: 0 !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--text-3) !important;
    font-family: var(--font) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    border-radius: 0 !important;
    padding: 10px 18px !important;
    border-bottom: 2px solid transparent !important;
    margin-bottom: -1px !important;
    transition: color 0.15s !important;
}
.stTabs [aria-selected="true"] {
    color: var(--accent-strong) !important;
    border-bottom: 2px solid var(--accent-strong) !important;
    background: transparent !important;
    font-weight: 600 !important;
}
.stTabs [data-baseweb="tab"]:hover { color: var(--text-2) !important; }

/* ── Chat messages ── */
.stChatMessage {
    background: var(--mc-card-bg) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    box-shadow: none !important;
    padding: 10px 12px !important;
    margin: 0 0 8px 0 !important;
}
[data-testid="chatAvatarIcon-user"],
[data-testid="chatAvatarIcon-assistant"] {
    display: none !important;
}
[data-testid="stChatMessageAvatar"] { display: none !important; }

.chat-user-row {
    margin: 0 0 10px 0;
}
.chat-user-bubble {
    border: 1px solid var(--accent-strong);
    border-radius: 10px;
    background: var(--mc-card-bg);
    color: var(--text);
    font-family: var(--font);
    font-size: 14px;
    line-height: 1.45;
    padding: 11px 14px;
}

/* ── Chat input ── */
[data-testid="stChatInput"] {
    --mc-sidebar-width: 340px;
    position: fixed !important;
    left: calc(50% + (var(--mc-sidebar-width) / 2)) !important;
    transform: translateX(-50%) !important;
    width: min(940px, calc(100vw - var(--mc-sidebar-width) - 2.8rem)) !important;
    bottom: 26px !important;
    z-index: 1000 !important;
    margin: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
    backdrop-filter: blur(8px) !important;
    -webkit-backdrop-filter: blur(8px) !important;
    background: linear-gradient(180deg, var(--mc-input-bg-start) 0%, var(--mc-input-bg-end) 100%) !important;
    border: 1px solid var(--accent) !important;
    border-radius: 22px !important;
    box-shadow: 0 12px 32px rgba(79, 134, 185, 0.22), 0 2px 8px rgba(79, 134, 185, 0.14), inset 0 0 0 1px rgba(79, 134, 185, 0.09) !important;
    transition: box-shadow 0.2s ease, border-color 0.2s ease !important;
    top: auto !important;
}
[data-testid="stChatInput"] > div {
    border: none !important;
    border-radius: 0 !important;
    background: var(--mc-chat-field-bg) !important;
    box-shadow: none !important;
    padding: 8px 10px !important;
}
[data-testid="stChatInput"] [data-baseweb="input"],
[data-testid="stChatInput"] [data-baseweb="textarea"],
[data-testid="stChatInput"] [data-baseweb="input"] > div,
[data-testid="stChatInput"] [data-baseweb="textarea"] > div {
    background: var(--mc-chat-field-bg) !important;
    border: none !important;
    box-shadow: none !important;
}
[data-testid="stChatInput"] input,
[data-testid="stChatInput"] textarea,
[data-testid="stChatInput"] [data-baseweb="input"] > div,
[data-testid="stChatInput"] [data-baseweb="textarea"] > div {
    background: transparent !important;
    color: var(--mc-text) !important;
    border: none !important;
}
[data-testid="stChatInput"] input::placeholder,
[data-testid="stChatInput"] textarea::placeholder {
    color: var(--mc-text-subtle) !important;
    opacity: 1 !important;
}
[data-testid="stBottom"],
[data-testid="stBottom"] > div,
.stChatFloatingInputContainer,
div:has(> [data-testid="stChatInput"]) {
    background: transparent !important;
}
[data-testid="stChatInput"] [data-baseweb="input"] > div,
[data-testid="stChatInput"] [data-baseweb="textarea"] > div,
[data-testid="stChatInput"] [data-baseweb="input"] input,
[data-testid="stChatInput"] [data-baseweb="textarea"] textarea {
    background: transparent !important;
    color: var(--mc-text) !important;
}

/* ── Theme toggle visibility ── */
[data-testid="stToggle"] {
    display: flex !important;
    justify-content: flex-end !important;
}
[data-testid="stToggle"] label[data-baseweb="checkbox"] > div {
    background: color-mix(in srgb, var(--mc-surface) 78%, var(--mc-border) 22%) !important;
    border: 1px solid var(--mc-border) !important;
    box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--mc-border) 70%, transparent) !important;
}
[data-testid="stToggle"] label[data-baseweb="checkbox"] input:checked + div {
    background: var(--mc-accent) !important;
    border-color: var(--mc-accent) !important;
}
[data-testid="stToggle"] label[data-baseweb="checkbox"] input + div > div {
    background: #ffffff !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.28) !important;
}
[data-testid="stChatInput"]:hover {
    border-color: var(--accent-strong) !important;
    box-shadow: 0 14px 36px rgba(79, 134, 185, 0.27), 0 3px 10px rgba(79, 134, 185, 0.16), inset 0 0 0 1px rgba(79, 134, 185, 0.13) !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: var(--accent-strong) !important;
    box-shadow: 0 0 0 2px rgba(110, 163, 212, 0.3), 0 14px 36px rgba(79, 134, 185, 0.27), inset 0 0 0 1px rgba(79, 134, 185, 0.15) !important;
}
[data-testid="stChatInput"] textarea {
    font-family: var(--font) !important;
    font-size: 14px !important;
    color: var(--text) !important;
    line-height: 1.45 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 10px 8px !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: var(--text-3) !important;
}
[data-testid="stChatInput"] button {
    width: 36px !important;
    height: 36px !important;
    min-width: 36px !important;
    border-radius: 999px !important;
    border: none !important;
    background: var(--accent-strong) !important;
    color: #ffffff !important;
    box-shadow: none !important;
    transition: transform 0.15s ease, background 0.15s ease !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
}
[data-testid="stChatInput"] button:hover {
    background: var(--mc-accent) !important;
    transform: translateY(-1px) !important;
}
[data-testid="stChatInput"] button:disabled {
    background: var(--mc-surface) !important;
    border: 1px solid var(--border) !important;
    color: var(--mc-text-subtle) !important;
}
[data-testid="stChatInput"] button svg {
    width: 16px !important;
    height: 16px !important;
    display: block !important;
}

/* Sidebar repliée: recentre la barre sur toute la zone visible */
body:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stChatInput"] {
    left: 50% !important;
    width: min(940px, calc(100vw - 2.8rem)) !important;
}
section[data-testid="stSidebar"][aria-expanded="false"] + div [data-testid="stChatInput"] {
    left: 50% !important;
    width: min(940px, calc(100vw - 2.8rem)) !important;
}

@media (max-width: 1280px) {
    [data-testid="stChatInput"] {
        --mc-sidebar-width: 300px;
        left: calc(50% + (var(--mc-sidebar-width) / 2)) !important;
        width: calc(100vw - var(--mc-sidebar-width) - 1.8rem) !important;
    }
    body:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stChatInput"] {
        left: 50% !important;
        width: calc(100vw - 1.8rem) !important;
    }
    section[data-testid="stSidebar"][aria-expanded="false"] + div [data-testid="stChatInput"] {
        left: 50% !important;
        width: calc(100vw - 1.8rem) !important;
    }
}
@media (max-width: 900px) {
    [data-testid="stChatInput"] {
        left: 50% !important;
        width: calc(100vw - 1rem) !important;
        bottom: 12px !important;
        border-radius: 18px !important;
    }
    .block-container { padding-bottom: 8rem !important; }
}

/* ── Buttons ── */
.stButton > button,
.stFormSubmitButton > button,
[data-testid="stPopover"] button {
    font-family: var(--font) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    color: var(--text-2) !important;
    background: var(--mc-card-bg) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    padding: 7px 14px !important;
    transition: all 0.15s ease !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
    cursor: pointer !important;
}
.stButton > button:hover,
.stFormSubmitButton > button:hover,
[data-testid="stPopover"] button:hover {
    color: var(--text) !important;
    border-color: var(--accent) !important;
    background: var(--mc-surface) !important;
    box-shadow: 0 1px 3px rgba(79,134,185,0.18) !important;
}
[data-testid="stPopover"] button {
    width: 100% !important;
}
.stFormSubmitButton > button:disabled,
[data-testid="stPopover"] button:disabled {
    background: var(--mc-surface) !important;
    border-color: var(--mc-border) !important;
    color: var(--mc-text-subtle) !important;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
    background: var(--mc-surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    padding: 16px 18px !important;
    box-shadow: none !important;
}
[data-testid="stMetricLabel"] {
    font-family: var(--font) !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: var(--text-3) !important;
}
[data-testid="stMetricValue"] {
    font-family: var(--font-mono) !important;
    font-size: 22px !important;
    font-weight: 600 !important;
    color: var(--text) !important;
    letter-spacing: -0.02em !important;
}

/* ── Expander ── */
[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    overflow: hidden !important;
    background: var(--white) !important;
}
[data-testid="stExpander"] summary {
    font-family: var(--font-mono) !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0.05em !important;
    color: var(--text-3) !important;
    padding: 10px 14px !important;
    background: var(--mc-surface) !important;
}

/* ── Code ── */
code, pre {
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
    background: var(--mc-surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    color: var(--text-2) !important;
}

/* ── Select / dropdown ── */
[data-baseweb="select"] > div {
    background: var(--mc-card-bg) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    font-family: var(--font) !important;
    font-size: 13px !important;
    color: var(--text) !important;
}
[data-testid="stTextInput"] input::placeholder {
    color: var(--text-3) !important;
    opacity: 1 !important;
}

/* ── Chat hero (état vide) ── */
.chat-hero-spacer { height: 14vh; }
.chat-hero-spacer-bottom { height: 10vh; }
.chat-hero-card {
    border: 1px solid var(--border);
    border-radius: 14px;
    background: linear-gradient(180deg, var(--mc-card-bg) 0%, var(--mc-surface) 100%);
    padding: 20px 18px 14px 18px;
    margin-bottom: 10px;
    text-align: center;
    box-shadow: 0 8px 28px rgba(79,134,185,0.14);
}
.chat-hero-title {
    font-family: var(--font);
    font-size: 24px;
    line-height: 1.25;
    letter-spacing: -0.02em;
    color: var(--text);
    font-weight: 600;
    margin-bottom: 6px;
}
.chat-hero-subtitle {
    font-family: var(--font);
    font-size: 13px;
    color: var(--text-3);
    line-height: 1.5;
}
@media (max-width: 900px) {
    .chat-hero-spacer { height: 6vh; }
    .chat-hero-spacer-bottom { height: 5vh; }
    .chat-hero-title { font-size: 20px; }
}

/* ── Divider ── */
hr { border: none; border-top: 1px solid var(--border) !important; margin: 1.5rem 0 !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: var(--accent); }

/* ── Alerts ── */
.stAlert { border-radius: var(--radius) !important; border: 1px solid var(--border) !important; font-family: var(--font) !important; }

/* ── Dataframe ── */
.stDataFrame { border: 1px solid var(--border) !important; border-radius: var(--radius) !important; overflow: hidden !important; }
</style>
""", unsafe_allow_html=True)

_theme_tokens = {
    "Clair": {
        "white": "#ffffff",
        "bg": "#f7f8fa",
        "bg_subtle": "#f1f3f7",
        "border": "#dce1e8",
        "border_mid": "#c8d0dc",
        "text": "#111827",
        "text2": "#374151",
        "text3": "#6b7280",
        "accent": "#2563eb",
        "accent_strong": "#1d4ed8",
        "accent_dim": "#2563eb1a",
        "card_bg": "#ffffff",
        "surface": "#f8fafc",
        "warn_bg": "#fff7ed",
        "warn_border": "#fed7aa",
        "warn": "#ea580c",
        "warn_soft": "#ea580c33",
        "danger": "#dc2626",
        "success": "#16a34a",
        "input_start": "#ffffff",
        "input_end": "#f8fafc",
        "input_border": "#9ca3af",
        "input_border_hover": "#6b7280",
        "chat_field_bg": "#ffffff",
    },
    "Sombre": {
        "white": "#111a2b",
        "bg": "#0b1220",
        "bg_subtle": "#121c2e",
        "border": "#2b3f5f",
        "border_mid": "#3a5478",
        "text": "#e6edf7",
        "text2": "#c2d1e5",
        "text3": "#8ea3bf",
        "accent": "#60a5fa",
        "accent_strong": "#93c5fd",
        "accent_dim": "#60a5fa2e",
        "card_bg": "#111a2b",
        "surface": "#17263d",
        "warn_bg": "#3a2816",
        "warn_border": "#8b5e34",
        "warn": "#fbbf24",
        "warn_soft": "#fbbf2433",
        "danger": "#f87171",
        "success": "#4ade80",
        "input_start": "#111a2b",
        "input_end": "#1a2b45",
        "input_border": "#3a5478",
        "input_border_hover": "#5f7ea6",
        "chat_field_bg": "#0f1b30",
    },
}
_t = _theme_tokens.get(st.session_state.get("ui_theme", "Clair"), _theme_tokens["Clair"])
st.markdown(
    f"""
<style>
:root {{
    --white: {_t['white']};
    --bg: {_t['bg']};
    --bg-subtle: {_t['bg_subtle']};
    --border: {_t['border']};
    --border-mid: {_t['border_mid']};
    --text: {_t['text']};
    --text-2: {_t['text2']};
    --text-3: {_t['text3']};
    --accent: {_t['accent']};
    --accent-strong: {_t['accent_strong']};
    --accent-dim: {_t['accent_dim']};
    --mc-card-bg: {_t['card_bg']};
    --mc-surface: {_t['surface']};
    --mc-border: {_t['border']};
    --mc-text: {_t['text']};
    --mc-text-muted: {_t['text2']};
    --mc-text-subtle: {_t['text3']};
    --mc-accent: {_t['accent']};
    --mc-accent-strong: {_t['accent_strong']};
    --mc-warn-bg: {_t['warn_bg']};
    --mc-warn-border: {_t['warn_border']};
    --mc-warn: {_t['warn']};
    --mc-warn-soft: {_t['warn_soft']};
    --mc-danger: {_t['danger']};
    --mc-success: {_t['success']};
    --mc-input-bg-start: {_t['input_start']};
    --mc-input-bg-end: {_t['input_end']};
    --mc-input-border: {_t['input_border']};
    --mc-input-border-hover: {_t['input_border_hover']};
    --mc-chat-field-bg: {_t['chat_field_bg']};
}}

html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"] .main .block-container {{
    background: var(--bg) !important;
    color: var(--text) !important;
}}
[data-testid="stMarkdownContainer"],
[data-testid="stText"],
[data-testid="stCaptionContainer"] {{
    color: var(--text) !important;
}}

section[data-testid="stSidebar"] {{
    background: var(--bg-subtle) !important;
}}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {{
    background: var(--mc-card-bg) !important;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.10) !important;
}}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {{
    background: linear-gradient(180deg, var(--mc-card-bg) 0%, var(--mc-surface) 100%) !important;
    box-shadow: 0 3px 10px rgba(0, 0, 0, 0.12) !important;
}}

[data-testid="stChatInput"] {{
    background: linear-gradient(180deg, var(--mc-input-bg-start) 0%, var(--mc-input-bg-end) 100%) !important;
    border-color: var(--mc-input-border) !important;
}}
[data-testid="stChatInput"]:hover {{
    border-color: var(--mc-input-border-hover) !important;
}}
[data-testid="stChatInput"]:focus-within {{
    border-color: var(--mc-accent-strong) !important;
}}
[data-testid="stChatInput"] button {{
    background: var(--mc-accent-strong) !important;
}}
[data-testid="stChatInput"] button:hover {{
    background: var(--mc-accent) !important;
}}
[data-testid="stChatInput"] button:disabled {{
    background: var(--mc-surface) !important;
    border-color: var(--mc-border) !important;
    color: var(--mc-text-subtle) !important;
}}
[data-testid="stMetric"] {{
    background: var(--mc-surface) !important;
}}
[data-baseweb="select"] > div,
[data-testid="stTextInput"] input,
[data-testid="stDateInputField"] input {{
    background: var(--mc-card-bg) !important;
    border-color: var(--mc-border) !important;
    color: var(--mc-text) !important;
}}
[data-testid="stTextInput"] input::placeholder {{
    color: var(--mc-text-subtle) !important;
    opacity: 1 !important;
}}
[data-testid="stExpander"] > details,
[data-testid="stExpander"] summary,
[data-testid="stAlert"] {{
    background: var(--mc-card-bg) !important;
    border-color: var(--mc-border) !important;
}}

/* Harmonise les styles inline générés dans toute l'app */
[style*="background:#ffffff"],
[style*="background: #ffffff"] {{ background: var(--mc-card-bg) !important; }}
[style*="background:#f8fafc"],
[style*="background: #f8fafc"],
[style*="background:#fafafa"],
[style*="background: #fafafa"],
[style*="background:#f7fbff"],
[style*="background: #f7fbff"],
[style*="background:#f0f6ff"],
[style*="background: #f0f6ff"],
[style*="background:#ebf3fe"],
[style*="background: #ebf3fe"],
[style*="background:#eff6ff"] {{ background: var(--mc-surface) !important; }}
[style*="background: #eff6ff"] {{ background: var(--mc-surface) !important; }}
[style*="background:#fff7ed"],
[style*="background: #fff7ed"] {{ background: var(--mc-warn-bg) !important; }}
[style*="border:1px solid #e5e5e5"],
[style*="border: 1px solid #e5e5e5"],
[style*="border:1px solid #e5e7eb"],
[style*="border: 1px solid #e5e7eb"],
[style*="border:1px solid #eceff3"],
[style*="border: 1px solid #eceff3"],
[style*="border:1px solid #d4e2f4"],
[style*="border: 1px solid #d4e2f4"],
[style*="border:1px solid #d4d4d8"],
[style*="border: 1px solid #d4d4d8"] {{ border-color: var(--mc-border) !important; }}
[style*="border:1px solid #fed7aa"],
[style*="border: 1px solid #fed7aa"] {{ border-color: var(--mc-warn-border) !important; }}
[style*="border-left:3px solid #2563eb"] {{ border-left-color: var(--mc-accent) !important; }}
[style*="border-left:3px solid #ea580c"] {{ border-left-color: var(--mc-warn) !important; }}
[style*="color:#111827"],
[style*="color: #111827"],
[style*="color:#404040"],
[style*="color: #404040"],
[style*="color:#374151"],
[style*="color: #374151"],
[style*="color:#334155"],
[style*="color: #334155"],
[style*="color:#0a0a0a"],
[style*="color: #0a0a0a"] {{ color: var(--mc-text) !important; }}
[style*="color:#6b7280"],
[style*="color: #6b7280"],
[style*="color:#9ca3af"],
[style*="color: #9ca3af"],
[style*="color:#a3a3a3"],
[style*="color: #a3a3a3"] {{ color: var(--mc-text-muted) !important; }}
[style*="color:#2563eb"] {{ color: var(--mc-accent) !important; }}
[style*="color:#ea580c"] {{ color: var(--mc-warn) !important; }}
[style*="color:#dc2626"] {{ color: var(--mc-danger) !important; }}
[style*="color:#16a34a"] {{ color: var(--mc-success) !important; }}

</style>
""",
    unsafe_allow_html=True,
)

# Fallback robuste (Streamlit 1.54 Windows): réouverture sidebar si contrôle natif absent.

components.html(
"""
<script>
(function () {
  const BTN_ID = "mc-sidebar-fallback-toggle";

  function nativeToggleExists(doc){
    return !!doc.querySelector(
      '[data-testid="collapsedControl"] button,' +
      '[data-testid="stSidebarCollapsedControl"] button,' +
      '[data-testid="stSidebarCollapseButton"] button'
    );
  }

  function clickNativeToggle(doc){
    const el = doc.querySelector(
      '[data-testid="collapsedControl"] button,' +
      '[data-testid="stSidebarCollapsedControl"] button,' +
      '[data-testid="stSidebarCollapseButton"] button'
    );
    if (el) el.click();
  }

  function ensureButton() {
    const doc = window.parent.document;

    // ✅ Si le bouton natif existe => on supprime/masque le fallback (donc plus de petite flèche)
    if (nativeToggleExists(doc)) {
      const existing = doc.getElementById(BTN_ID);
      if (existing) existing.remove();
      return;
    }

    // ✅ Sinon seulement (cas bug Windows) => on crée/affiche le fallback
    let btn = doc.getElementById(BTN_ID);
    if (!btn) {
      btn = doc.createElement("button");
      btn.id = BTN_ID;
      btn.type = "button";
      btn.setAttribute("aria-label", "Toggle sidebar");
      btn.style.position = "fixed";
      btn.style.top = "10px";
      btn.style.left = "10px";
      btn.style.zIndex = "99999";
      btn.style.width = "34px";
      btn.style.height = "34px";
      btn.style.borderRadius = "10px";
      btn.style.border = "1px solid rgba(180,180,180,0.6)";
      btn.style.background = "rgba(255,255,255,0.92)";
      btn.style.boxShadow = "0 6px 18px rgba(0,0,0,0.18)";
      btn.style.cursor = "pointer";
      btn.style.fontSize = "18px";
      btn.style.display = "flex";
      btn.style.alignItems = "center";
      btn.style.justifyContent = "center";
      btn.addEventListener("click", () => clickNativeToggle(doc));
      doc.body.appendChild(btn);
    }

    btn.textContent = "›";
  }

  ensureButton();
  setInterval(ensureButton, 600);
})();
</script>
""",
height=0
)

# ─── HEADER ───────────────────────────────────────────────────────────────────


_logo = _image_data_uri("logo.svg", "logo.png", "logo.jpg")
_logo_html = f'<img src="{_logo}" style="height:32px; width:auto; border-radius:6px; object-fit:contain;">' if _logo else '<div style="width:32px; height:32px; background:var(--accent); border-radius:8px;"></div>'
_has_chat_messages = bool(st.session_state.get("chat_history", []))
_show_chat_clear = (
    st.session_state.get("current_page") == "Chat analytique"
    and _has_chat_messages
)
_show_top_header = not (
    st.session_state.get("current_page") == "Chat analytique"
    and not _has_chat_messages
)
if _show_top_header:
    header_left, header_right = st.columns([6.6, 2.4], gap="small")
    with header_left:
        st.markdown(
            f"""
            <div class="app-top-header-left" style="display:flex; align-items:center; gap:12px;">
                {_logo_html}
                <div>
                    <span style="font-family:'Geist',sans-serif; font-size:16px; font-weight:600; color:var(--text); letter-spacing:-0.02em;">Mobility Copilot</span>
                    <span style="font-family:'Geist',sans-serif; font-size:13px; color:var(--text-3); margin-left:10px; font-weight:400;">Montréal</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with header_right:
        if _show_chat_clear:
            switch_col, clear_col, date_col = st.columns([0.8, 1.7, 1.6], gap="small")
            with switch_col:
                st.toggle(
                    "Mode sombre",
                    key="theme_dark_toggle",
                    label_visibility="collapsed",
                    help="Basculer mode clair / sombre",
                )
            with clear_col:
                if st.button("Effacer", key="clear_chat_header", use_container_width=True):
                    _reset_chat_state()
                    st.rerun()
            with date_col:
                st.markdown(
                    f"""<div style="text-align:right; font-family:'Geist Mono',monospace; font-size:11px; color:var(--text-3); padding-top:6px;">{datetime.now().strftime("%d %b %Y")}</div>""",
                    unsafe_allow_html=True,
                )
        else:
            switch_col, date_col = st.columns([1.1, 2.0], gap="small")
            with switch_col:
                st.toggle(
                    "Mode sombre",
                    key="theme_dark_toggle",
                    label_visibility="collapsed",
                    help="Basculer mode clair / sombre",
                )
            with date_col:
                st.markdown(
                    f"""<div style="text-align:right; font-family:'Geist Mono',monospace; font-size:11px; color:var(--text-3); padding-top:6px;">{datetime.now().strftime("%d %b %Y")}</div>""",
                    unsafe_allow_html=True,
                )
    st.markdown("<div class='app-top-header-divider' style='border-bottom:1px solid var(--border); margin:0 0 26px 0;'></div>", unsafe_allow_html=True)
else:
    # État initial du chat: pas de header, mais switch de thème disponible.
    _sw_spacer, _sw_col = st.columns([8.6, 1.4], gap="small")
    with _sw_col:
        st.toggle(
            "Mode sombre",
            key="theme_dark_toggle",
            label_visibility="collapsed",
            help="Basculer mode clair / sombre",
        )

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_data():
    return load_all_data()

@st.cache_resource(show_spinner=False)
def get_engines(data, cache_buster: tuple[int, int, int]):
    # `cache_buster` invalide le cache quand app.py/query_engine.py/rag_engine.py changent.
    _ = cache_buster
    rag = RAGEngine()
    qe = QueryEngine(data)
    return rag, qe


PERIOD_TO_DAYS = {
    "7 derniers jours": 7,
    "30 derniers jours": 30,
    "3 derniers mois": 90,
    "12 derniers mois": 365,
}

CUSTOM_PERIOD_RE = re.compile(
    r"Personnalisée\s*:\s*(\d{4}-\d{2}-\d{2})\s*(?:->|→)\s*(\d{4}-\d{2}-\d{2})",
    flags=re.IGNORECASE,
)


def parse_custom_period(periode: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    if not isinstance(periode, str):
        return None
    m = CUSTOM_PERIOD_RE.search(periode)
    if not m:
        return None
    start = pd.to_datetime(m.group(1), errors="coerce")
    end = pd.to_datetime(m.group(2), errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return None
    if start > end:
        start, end = end, start
    return start.normalize(), end.normalize()


def get_global_date_bounds(data: dict) -> tuple[date, date]:
    mins, maxs = [], []
    for key in ("collisions", "req311"):
        df = data.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty and "date" in df.columns:
            d = pd.to_datetime(df["date"], errors="coerce").dropna()
            if not d.empty:
                mins.append(d.min().date())
                maxs.append(d.max().date())
    if not mins or not maxs:
        today = datetime.now().date()
        return today, today
    return min(mins), max(maxs)


def _to_date_safe(value, fallback: date) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (list, tuple, set, np.ndarray, pd.Series, pd.Index)):
        seq = list(value)
        if not seq:
            return fallback
        return _to_date_safe(seq[0], fallback)
    parsed = pd.to_datetime(value, errors="coerce")
    if isinstance(parsed, (pd.DatetimeIndex, pd.Series)):
        if len(parsed) == 0:
            return fallback
        first = parsed[0]
        if pd.isna(first):
            return fallback
        return pd.Timestamp(first).date()
    if pd.isna(parsed):
        return fallback
    return pd.Timestamp(parsed).date()


def _normalize_date_range(value, fallback_start: date, fallback_end: date) -> tuple[date, date]:
    if isinstance(value, (tuple, list)) and len(value) >= 2:
        start_date = _to_date_safe(value[0], fallback_start)
        end_date = _to_date_safe(value[1], fallback_end)
    else:
        single = _to_date_safe(value, fallback_end)
        start_date = single
        end_date = single
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def filter_df_by_period(df: pd.DataFrame, periode: str) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else df

    dates = pd.to_datetime(df["date"], errors="coerce")
    custom = parse_custom_period(periode)
    if custom is not None:
        start, end = custom
        return df.loc[(dates >= start) & (dates <= end)].copy()

    anchor = dates.max()
    if pd.isna(anchor):
        return df.copy()

    days = PERIOD_TO_DAYS.get(periode, 30)
    cutoff = anchor - pd.Timedelta(days=days)
    return df.loc[dates >= cutoff].copy()


def compute_hotspots_df(collisions: pd.DataFrame) -> pd.DataFrame:
    if collisions is None or collisions.empty:
        return pd.DataFrame(columns=["lieu", "collisions", "graves", "heure_moyenne", "tendance"])

    coll = collisions.copy()
    coll["intersection"] = (
        coll.get("intersection", pd.Series("Secteur inconnu", index=coll.index))
        .fillna("Secteur inconnu")
        .astype(str)
    )
    coll = coll[coll["intersection"].str.strip() != ""]
    if coll.empty:
        return pd.DataFrame(columns=["lieu", "collisions", "graves", "heure_moyenne", "tendance"])

    hotspots = (coll.groupby("intersection")
        .agg(
            collisions=("gravite_num", "count"),
            graves=("gravite_num", lambda x: (x >= 3).sum()),
            heure_moyenne=("heure", "mean"),
        )
        .reset_index()
        .sort_values("collisions", ascending=False)
        .head(5))
    hotspots["lieu"] = hotspots["intersection"].astype(str)
    hotspots["tendance"] = ""
    return hotspots


def compute_meteo_corr_df(collisions: pd.DataFrame) -> pd.DataFrame:
    if collisions is None or collisions.empty:
        return pd.DataFrame(columns=["date", "collisions", "temperature", "precipitation"])
    return (collisions.groupby("date")
        .agg(
            collisions=("gravite_num", "count"),
            temperature=("temperature", "mean"),
            precipitation=("precipitation_mm", "mean"),
        )
        .reset_index()
        .tail(120))


def compute_weekly_trend_df(collisions: pd.DataFrame, req311: pd.DataFrame) -> pd.DataFrame:
    coll_dates = pd.to_datetime(collisions["date"], errors="coerce") if "date" in collisions.columns else pd.Series(dtype="datetime64[ns]")
    req_dates = pd.to_datetime(req311["date"], errors="coerce") if "date" in req311.columns else pd.Series(dtype="datetime64[ns]")
    coll_max = coll_dates.max() if len(coll_dates) else pd.NaT
    req_max = req_dates.max() if len(req_dates) else pd.NaT

    if pd.isna(coll_max) and pd.isna(req_max):
        anchor = datetime.now()
    elif pd.isna(coll_max):
        anchor = req_max.to_pydatetime()
    elif pd.isna(req_max):
        anchor = coll_max.to_pydatetime()
    else:
        anchor = max(coll_max, req_max).to_pydatetime()

    rows = []
    for i in range(12):
        end = anchor - timedelta(weeks=i)
        start = end - timedelta(weeks=1)
        s, e = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        c = int(((collisions["date"] >= s) & (collisions["date"] <= e)).sum()) if "date" in collisions.columns else 0
        r = int(((req311["date"] >= s) & (req311["date"] <= e)).sum()) if "date" in req311.columns else 0
        rows.append({"semaine": end.strftime("S%V\n%d %b"), "collisions": c, "req311": r})
    return pd.DataFrame(rows[::-1])


def _build_ambiguity_card(reason: str, options: list[str], question: str) -> str:
    return f"""<div style="background:var(--mc-card-bg); border:1px solid var(--mc-border); border-left:3px solid var(--mc-accent); border-radius:8px; padding:14px;">
<div style="font-family:'Geist Mono',monospace; font-size:10px; color:var(--mc-accent); letter-spacing:0.1em; margin-bottom:10px;">DÉTECTION D'AMBIGUÏTÉ</div>
<p style="color:var(--mc-text-muted); margin-bottom:10px;">{reason}</p>
<p style="color:var(--mc-text); margin-bottom:8px;">Choisissez 1 interprétation dans la liste ci-dessous pour continuer.</p>
<p style="color:var(--mc-text-muted); margin-top:10px; font-size:12px;">Question initiale: {question}</p>
</div>"""


def _clean_choice_label(text: str) -> str:
    t = (text or "").strip()
    # Supprime les icônes/emojis en tête pour un rendu plus sobre.
    t = re.sub(r"^[^\wÀ-ÿ]+", "", t)
    return t.strip()


def _refine_question_with_choice(question: str, choice_text: str) -> str:
    c = choice_text.lower()
    if "requête" in c or "311" in c:
        return f"Analyse orientée requêtes 311: {question}"
    if "stm" in c or "bus" in c or "métro" in c:
        return f"Analyse orientée STM: {question}"
    if "embouteill" in c or "trafic" in c:
        return f"Analyse orientée congestion routière (proxy collisions): {question}"
    if "collision" in c or "sécurité" in c:
        return f"Analyse orientée collisions routières: {question}"
    return f"Analyse orientée: {choice_text}. Question: {question}"


def save_weekly_briefing_snapshots(data: dict):
    now = datetime.now()
    iso_year, iso_week, _ = now.isocalendar()
    out_dir = pathlib.Path(__file__).parent / "outputs" / "briefings"
    out_dir.mkdir(parents=True, exist_ok=True)

    week_data = dict(data)
    week_data["collisions"] = filter_df_by_period(data["collisions"], "7 derniers jours")
    week_data["req311"] = filter_df_by_period(data["req311"], "7 derniers jours")

    outputs = []
    for tone, suffix in [("municipal", "municipalite"), ("public", "grand_public")]:
        out_file = out_dir / f"briefing_{iso_year}_W{iso_week}_{suffix}.html"
        if not out_file.exists():
            html = generate_briefing(
                week_data,
                tone=tone,
                periode="7 derniers jours",
                reference_data=data,
            )
            out_file.write_text(html, encoding="utf-8")
        outputs.append(out_file)
    return outputs

if "boot_splash_done" not in st.session_state:
    st.session_state.boot_splash_done = False

_splash = st.empty() if not st.session_state.boot_splash_done else None
if _splash is not None:
    _dark_mode = st.session_state.get("ui_theme") == "Sombre"
    _splash_bg = "rgba(11,18,32,0.98)" if _dark_mode else "rgba(255,255,255,0.98)"
    _splash_text = "#e6edf7" if _dark_mode else "#0a0a0a"
    _splash_dot_color = "#93c5fd" if _dark_mode else "#2563eb"
    _splash_fallback_box = "#17263d" if _dark_mode else "#0a0a0a"
    _splash_logo = _image_data_uri("logo.svg", "logo_bande.svg", "logo.png", "logo.jpg")
    _splash_img = (
        f"""<img src="{_splash_logo}" style="width:min(360px, 72vw); height:auto; object-fit:contain; margin-bottom:16px;">"""
        if _splash_logo
        else f"""<div style="width:88px; height:88px; border-radius:18px; background:{_splash_fallback_box}; margin:0 auto 18px auto;"></div>"""
    )
    _splash.markdown(
        f"""
        <style>
          @keyframes mc-pulse {{ 0%, 100% {{ opacity: 0.22; transform: translateY(0); }} 50% {{ opacity: 1; transform: translateY(-2px); }} }}
        </style>
        <div style="position:fixed; inset:0; z-index:9999; background:{_splash_bg}; display:flex; align-items:center; justify-content:center;">
          <div style="text-align:center; padding:24px;">
            {_splash_img}
            <div style="font-family:'Geist',sans-serif; font-size:22px; font-weight:600; color:{_splash_text}; letter-spacing:-0.01em;">
              Chargement des données à Montréal ...
            </div>
            <div style="margin-top:12px; display:flex; gap:8px; justify-content:center;">
              <span style="width:8px; height:8px; border-radius:50%; background:{_splash_dot_color}; animation:mc-pulse 1.1s infinite ease-in-out;"></span>
              <span style="width:8px; height:8px; border-radius:50%; background:{_splash_dot_color}; animation:mc-pulse 1.1s infinite ease-in-out 0.18s;"></span>
              <span style="width:8px; height:8px; border-radius:50%; background:{_splash_dot_color}; animation:mc-pulse 1.1s infinite ease-in-out 0.36s;"></span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

data = get_data()
engine_cache_buster = (
    int((pathlib.Path(__file__).parent / "app.py").stat().st_mtime),
    int((pathlib.Path(__file__).parent / "query_engine.py").stat().st_mtime),
    int((pathlib.Path(__file__).parent / "rag_engine.py").stat().st_mtime),
)
rag, query_engine = get_engines(data, engine_cache_buster)
if _splash is not None:
    _splash.empty()
    st.session_state.boot_splash_done = True

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    _side_dark = st.session_state.get("ui_theme") == "Sombre"
    if _side_dark:
        _side_logo = _image_data_uri("logo.svg", "logo.png", "logo.jpg")
        if _side_logo:
            st.markdown(
                f"""
                <div style="display:flex; align-items:center; gap:10px; margin:2px 0 14px 0;">
                    <img src="{_side_logo}" style="width:48px; height:48px; border-radius:8px; object-fit:contain;">
                    <div>
                        <div style="font-family:'Geist',sans-serif; font-size:18px; font-weight:600; color:var(--text); line-height:1.15;">Mobility Copilot</div>
                        <div style="font-family:'Geist Mono',monospace; font-size:10px; color:var(--text-3); letter-spacing:0.08em; margin-top:2px;">MONTRÉAL</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        _side_logo = _image_data_uri("logo_bande.svg", "logo.svg", "logo.png", "logo.jpg")
        if _side_logo:
            st.markdown(
                f"""
                <div style="display:flex; justify-content:flex-start; margin:2px 0 14px 0;">
                    <img src="{_side_logo}" style="width:100%; max-width:220px; height:auto; object-fit:contain;">
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("""
    <p style="font-family:'Geist',sans-serif; font-size:11px; font-weight:500; color:var(--text-3);
              letter-spacing:0.06em; text-transform:uppercase; margin:0 0 10px 0;">
      Navigation
    </p>
    """, unsafe_allow_html=True)

    if "sidebar_page" not in st.session_state or st.session_state["sidebar_page"] not in PAGE_OPTIONS:
        st.session_state["sidebar_page"] = st.session_state.current_page

    page = st.radio(
        "Section",
        options=PAGE_OPTIONS,
        index=PAGE_OPTIONS.index(st.session_state["sidebar_page"]),
        label_visibility="collapsed",
        key="sidebar_page",
    )
    st.session_state.current_page = page
    page = st.session_state.current_page
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    st.markdown("""
    <p style="font-family:'Geist',sans-serif; font-size:11px; font-weight:500; color:var(--text-3);
              letter-spacing:0.06em; text-transform:uppercase; margin:0 0 10px 0;">
      Période
    </p>
    """, unsafe_allow_html=True)

    date_min, date_max = get_global_date_bounds(data)
    default_end = date_max
    default_start = max(date_min, default_end - timedelta(days=29))

    stored_range = st.session_state.get("custom_period_range", (default_start, default_end))
    norm_start, norm_end = _normalize_date_range(stored_range, default_start, default_end)
    norm_start = max(date_min, min(norm_start, date_max))
    norm_end = max(date_min, min(norm_end, date_max))
    if norm_start > norm_end:
        norm_start, norm_end = norm_end, norm_start
    st.session_state.custom_period_range = (norm_start, norm_end)

    if "sidebar_custom_period" in st.session_state:
        ws, we = _normalize_date_range(st.session_state["sidebar_custom_period"], norm_start, norm_end)
        ws = max(date_min, min(ws, date_max))
        we = max(date_min, min(we, date_max))
        if ws > we:
            ws, we = we, ws
        st.session_state["sidebar_custom_period"] = (ws, we)

    period_choice = st.selectbox(
        "Période",
        ["7 derniers jours", "30 derniers jours", "3 derniers mois", "12 derniers mois", "Plage personnalisée"],
        index=1,
        label_visibility="collapsed",
        key="sidebar_period_choice",
    )

    if period_choice == "Plage personnalisée":
        custom_value = st.date_input(
            "Plage personnalisée",
            value=st.session_state.custom_period_range,
            min_value=date_min,
            max_value=date_max,
            format="YYYY-MM-DD",
            key="sidebar_custom_period",
        )
        start_date, end_date = _normalize_date_range(custom_value, norm_start, norm_end)
        start_date = max(date_min, min(start_date, date_max))
        end_date = max(date_min, min(end_date, date_max))
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        st.session_state.custom_period_range = (start_date, end_date)
        periode = f"Personnalisée : {start_date.isoformat()} -> {end_date.isoformat()}"
        st.caption(f"Fenêtre active: {start_date.isoformat()} → {end_date.isoformat()}")
    else:
        periode = period_choice

    llm_line = query_engine.llm_status_line()
    cards = []
    for key, label in [("collisions", "Collisions QC"), ("311", "Requêtes 311"), ("stm", "STM GTFS"), ("meteo", "Météo Canada")]:
        kind, desc = data["status"].get(key, ("demo", ""))
        dot = "#16a34a" if kind == "real" else "#ea580c"
        badge = "Réel" if kind == "real" else "Démo"
        count = desc.split("(")[1].replace(")", "").strip() if "(" in desc else desc
        cards.append(
            f"""<div style="padding:6px 0; border-bottom:1px solid var(--border);">
<div style="display:flex; justify-content:space-between; align-items:center;">
<span style="font-family:'Geist',sans-serif; font-size:12px; font-weight:500; color:var(--text);">{label}</span>
<span style="font-family:'Geist Mono',monospace; font-size:10px; color:{dot}; font-weight:600;">● {badge}</span>
</div>
<div style="font-family:'Geist Mono',monospace; font-size:9px; color:var(--text-3); margin-top:1px;">{count}</div>
</div>"""
        )
    sources_html = "".join(cards)
    st.markdown(
        f"""
            <div class="sidebar-bottom">
            <p style="font-family:'Geist',sans-serif; font-size:11px; font-weight:500; color:var(--text-3);
                      letter-spacing:0.06em; text-transform:uppercase; margin:0 0 6px 0;">
              Sources de données chargées
            </p>
            {sources_html}
            <div style="height:8px;"></div>
            <div style="font-family:'Geist Mono',monospace; font-size:9px; color:var(--text-3); line-height:1.8;">
              RAG · ChromaDB<br>{llm_line}<br>Validator · pandas<br>v1.0 · Hackathon 2026
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Jeu de données filtré selon la période sélectionnée (dashboard + briefing).
data_period = dict(data)
data_period["collisions"] = filter_df_by_period(data["collisions"], periode)
data_period["req311"] = filter_df_by_period(data["req311"], periode)
data_period["hotspots"] = compute_hotspots_df(data_period["collisions"])
data_period["meteo_corr"] = compute_meteo_corr_df(data_period["collisions"])
data_period["weekly_trend"] = compute_weekly_trend_df(data_period["collisions"], data_period["req311"])
weekly_briefing_files = save_weekly_briefing_snapshots(data)

# ─── PAGE RENDER ──────────────────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════════════════
# PAGE — CHAT
# ══════════════════════════════════════════════════════════════════════════════
if page == "Chat analytique":
    examples = [
        "Où ça coince en ce moment ?",
        "Quels quartiers ont le plus d'incidents par temps de pluie ?",
        "Quels types de requêtes 311 explosent quand il neige ?",
        "Autour de quels arrêts STM observe-t-on le plus de collisions ?"
    ]
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "pending_question" not in st.session_state:
        st.session_state.pending_question = None
    if "pending_ambiguity" not in st.session_state:
        st.session_state.pending_ambiguity = None

    def _submit_question(question_text: str):
        question_text = str(question_text or "").strip()
        if not question_text:
            return
        # Anti double-submit (Enter/clic répété pendant un rerun).
        if st.session_state.pending_question == question_text:
            return
        if (
            st.session_state.chat_history
            and st.session_state.chat_history[-1].get("role") == "user"
            and str(st.session_state.chat_history[-1].get("content", "")).strip() == question_text
            and st.session_state.pending_question is not None
        ):
            return
        st.session_state.chat_history.append({"role": "user", "content": question_text})
        st.session_state.pending_question = question_text

    def _submit_hero_input():
        hero_text = str(st.session_state.get("hero_prompt_input", "")).strip()
        if not hero_text:
            return
        _submit_question(hero_text)
        st.session_state["hero_prompt_input"] = ""

    has_messages = bool(st.session_state.chat_history)
    if not has_messages:
        hero_logo = _image_data_uri("logo.svg", "logo.png", "logo.jpg", "logo_bande.svg")
        st.markdown('<div class="chat-hero-spacer"></div>', unsafe_allow_html=True)
        left, center, right = st.columns([1.2, 2.6, 1.2])
        with center:
            logo_html = (
                f"""<img src="{hero_logo}" style="height:56px; width:auto; object-fit:contain; margin-bottom:12px;">"""
                if hero_logo
                else ""
            )
            st.markdown(
                f"""
                <div class="chat-hero-card">
                    {logo_html}
                    <div class="chat-hero-title">Analyse intelligente de la mobilité à Montréal</div>
                    <div class="chat-hero-subtitle">Risques routiers, incidents urbains et données 311 en temps réel.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.text_input(
                "Question",
                key="hero_prompt_input",
                placeholder="Posez des questions sur Montréal...",
                label_visibility="collapsed",
                on_change=_submit_hero_input,
            )
            if st.button("Analyser", key="hero_submit_btn", use_container_width=True):
                _submit_hero_input()

            with st.popover("Exemples de questions", use_container_width=True):
                for i, ex in enumerate(examples):
                    if st.button(ex, key=f"hero_ex_{i}", use_container_width=True):
                        _submit_question(ex)
                        st.rerun()
        st.markdown('<div class="chat-hero-spacer-bottom"></div>', unsafe_allow_html=True)


    def _queue_ambiguity(question_text: str, ambiguity_payload: dict):
        cleaned_options = [_clean_choice_label(x) for x in ambiguity_payload["clarifications"]]
        refined_queries = [
            str(x).strip() for x in ambiguity_payload.get("refined_queries", [])
            if str(x).strip()
        ]
        if len(refined_queries) != len(cleaned_options):
            refined_queries = []
        st.session_state.pending_ambiguity = {
            "question": question_text,
            "reason": ambiguity_payload["reason"],
            "options": cleaned_options,
            "refined_queries": refined_queries,
        }
        st.session_state["amb_choice_idx"] = 0
        # Nettoie les anciennes cartes d'ambiguïté qui alourdissent l'affichage.
        st.session_state.chat_history = [
                m for m in st.session_state.chat_history
                if not (
                    m.get("role") == "assistant"
                    and "DÉTECTION D'AMBIGUÏTÉ" in str(m.get("content", ""))
                )
            ]

    def _render_pending_ambiguity():
        if not st.session_state.pending_ambiguity:
            return
        amb = st.session_state.pending_ambiguity
        with st.chat_message("assistant"):
            st.markdown(
                _build_ambiguity_card(amb["reason"], amb["options"], amb["question"]),
                unsafe_allow_html=True,
            )
            selected_idx = st.radio(
                "Interprétation",
                options=list(range(len(amb["options"]))),
                format_func=lambda i: f"{i+1}. {amb['options'][i]}",
                key="amb_choice_idx",
                label_visibility="collapsed",
            )
            action_cols = st.columns([1, 1, 4])
            with action_cols[0]:
                if st.button("Continuer", key="amb_apply_choice"):
                    opt = amb["options"][selected_idx]
                    if amb.get("refined_queries") and selected_idx < len(amb["refined_queries"]):
                        refined_question = amb["refined_queries"][selected_idx]
                    else:
                        refined_question = _refine_question_with_choice(amb["question"], opt)
                    confirm = f"""<div style="background:var(--mc-card-bg); border:1px solid var(--mc-border); border-radius:8px; padding:10px 12px; margin-bottom:8px;">
<span style="font-family:'Geist Mono',monospace; font-size:10px; color:var(--mc-accent); letter-spacing:0.1em;">INTERPRÉTATION RETENUE</span><br>
<span style="color:var(--mc-text); font-size:12px;">{opt}</span>
</div>"""
                    response = query_engine.answer(refined_question, rag, periode, skip_ambiguity=True)
                    st.session_state.chat_history.append({"role": "assistant", "content": confirm + response})
                    st.session_state.pending_ambiguity = None
                    st.rerun()
            with action_cols[1]:
                if st.button("Annuler", key="amb_cancel_choice"):
                    st.session_state.pending_ambiguity = None
                    st.rerun()

    def _needs_manual_ambiguity(question_text: str, current_period: str):
        """
        Déclenche la carte de désambiguïsation pour:
        - les ambiguïtés métier classiques (RAG),
        - les questions trop vagues (options guidées cliquables).
        """
        guessed = query_engine.route_question(question_text)
        if guessed == "need_clarification":
            return True, query_engine.build_clarification_payload(question_text, current_period)
        if guessed != "hotspots":
            return False, None
        ambiguity = rag.detect_ambiguity(question_text)
        return ambiguity["is_ambiguous"], ambiguity

    # Chat history en ordre chronologique (ancien -> récent).
    history_to_show = st.session_state.chat_history
    if history_to_show:
        for msg in history_to_show:
            role = str(msg.get("role", "assistant"))
            content = str(msg.get("content", ""))
            if role == "user":
                st.markdown(
                    f"""<div class="chat-user-row"><div class="chat-user-bubble">{html_lib.escape(content)}</div></div>""",
                    unsafe_allow_html=True,
                )
            else:
                with st.chat_message("assistant"):
                    st.markdown(content, unsafe_allow_html=True)
        # Affiche la désambiguïsation tout en bas, attachée à la dernière question.
        if st.session_state.pending_ambiguity:
            _render_pending_ambiguity()
    else:
        _render_pending_ambiguity()
    
    # Process pending question from buttons
    if st.session_state.pending_question:
        question = st.session_state.pending_question
        st.session_state.pending_question = None
        with st.spinner("Analyse en cours..."):
            need_ambiguity, ambiguity = _needs_manual_ambiguity(question, periode)
            if need_ambiguity:
                _queue_ambiguity(question, ambiguity)
            else:
                response = query_engine.answer(question, rag, periode)
                st.session_state.chat_history.append({"role": "assistant", "content": response})
        st.rerun()
    
    # Chat input fixe en bas seulement après le premier échange.
    if has_messages:
        if prompt := st.chat_input(
            "Posez des questions sur Montréal...",
            disabled=st.session_state.pending_ambiguity is not None,
        ):
            _submit_question(prompt)
            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Dashboard":
    # Mode compact: exploite mieux la largeur écran et réduit le scroll.
    st.markdown(
        """
        <style>
        .block-container { max-width: 1450px !important; padding-top: 1.2rem !important; padding-bottom: 3rem !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    collisions = data_period["collisions"].copy()
    req311 = data_period["req311"].copy()
    hotspots = data_period["hotspots"].copy()
    weekly_view = data_period["weekly_trend"].copy()
    dark_mode = st.session_state.get("ui_theme") == "Sombre"
    plot_bg = _t["card_bg"]
    plot_grid = "#2b3f5f" if dark_mode else "#eef2f7"
    plot_font = _t["text3"]
    legend_bg = "rgba(17,26,43,0.82)" if dark_mode else "rgba(255,255,255,0.85)"
    map_style = "carto-darkmatter" if dark_mode else "carto-positron"
    dashboard_plot_config = {
        "displayModeBar": False,
        "displaylogo": False,
        "scrollZoom": False,
        "responsive": True,
    }

    if "gravite_num" in collisions.columns:
        grav = pd.to_numeric(collisions["gravite_num"], errors="coerce").fillna(0)
        graves_n = int((grav >= 3).sum())
    else:
        graves_n = 0
    coll_total = int(len(collisions))
    req_total = int(len(req311))
    grave_rate = (graves_n / coll_total * 100) if coll_total > 0 else 0.0

    top_req_label = "n/a"
    if not req311.empty and "type_service" in req311.columns:
        top_req_counts = req311["type_service"].fillna("Non spécifié").astype(str).value_counts()
        if len(top_req_counts):
            top_req_label = str(top_req_counts.index[0])

    top_meteo_label = "n/a"
    if not collisions.empty and "condition_meteo" in collisions.columns:
        top_meteo = collisions["condition_meteo"].fillna("Inconnue").astype(str).value_counts()
        if len(top_meteo):
            top_meteo_label = str(top_meteo.index[0])

    if hotspots.empty:
        insight_zone = "Aucune zone prioritaire détectée sur la période."
    else:
        top_h = hotspots.iloc[0]
        zone_name = str(top_h.get("lieu", "zone principale"))
        zone_coll = int(top_h.get("collisions", 0))
        zone_graves = int(top_h.get("graves", 0))
        insight_zone = f"Zone prioritaire: {zone_name} ({zone_coll} collisions, {zone_graves} graves)."

    trend_line = "Tendance hebdomadaire en cours de calcul."
    if isinstance(weekly_view, pd.DataFrame) and len(weekly_view) >= 2 and "collisions" in weekly_view.columns:
        try:
            c_prev = int(pd.to_numeric(weekly_view.iloc[-2]["collisions"], errors="coerce"))
            c_last = int(pd.to_numeric(weekly_view.iloc[-1]["collisions"], errors="coerce"))
            delta = c_last - c_prev
            pct = (delta / c_prev * 100.0) if c_prev > 0 else 0.0
            trend_word = "hausse" if delta > 0 else "baisse" if delta < 0 else "stabilité"
            trend_line = f"Tendance récente: {trend_word} hebdomadaire ({delta:+d}, {pct:+.1f}%)."
        except Exception:
            trend_line = "Tendance hebdomadaire en cours de calcul."

    st.markdown(
        f"""
        <div style="background:var(--mc-surface); border:1px solid var(--mc-border); border-left:4px solid var(--mc-accent); border-radius:12px; padding:12px 14px; margin-bottom:10px;">
          <div style="font-family:'Geist Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.08em; margin-bottom:6px; text-transform:uppercase;">Insight principal</div>
          <div style="font-size:15px; color:var(--mc-text); line-height:1.6; font-weight:600; margin-bottom:4px;">{insight_zone}</div>
          <div style="font-size:12px; color:var(--mc-text-muted); line-height:1.6;">{trend_line} Contexte dominant observé: <strong>{top_meteo_label}</strong>.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Collisions", f"{coll_total:,}".replace(",", " "))
    k2.metric("Collisions graves", f"{graves_n:,}".replace(",", " "), f"{grave_rate:.1f}% du total")
    k3.metric("Requêtes 311", f"{req_total:,}".replace(",", " "))
    k4.metric("Contexte dominant", top_meteo_label if top_meteo_label else "n/a", top_req_label[:22])

    with st.expander("Contexte et méthode de lecture (secondaire)", expanded=False):
        st.markdown(
            f"""
            <div style="font-size:12px; color:var(--mc-text-muted); line-height:1.65;">
              Données sur <strong>{periode}</strong>. Lecture recommandée: carte de concentration,
              puis zones prioritaires, puis détails 311/météo/tendances.
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    col_left, col_right = st.columns([2.2, 1.3], gap="large")
    with col_left:
        st.markdown("""<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.1em; font-weight:600; text-transform:uppercase; margin-bottom:10px;">Carte des collisions (densité pondérée)</div>""", unsafe_allow_html=True)
        if collisions.empty:
            st.info("Aucune collision disponible sur cette période.")
        else:
            collisions_map = collisions.copy()
            if len(collisions_map) > 25_000:
                collisions_map = collisions_map.sample(25_000, random_state=42)

            fig_map = px.density_mapbox(
                collisions_map,
                lat="latitude",
                lon="longitude",
                z="gravite_num",
                radius=14,
                center={"lat": 45.531, "lon": -73.567},
                zoom=10.4,
                mapbox_style=map_style,
                color_continuous_scale=[(0.0, "#dbeafe"), (0.45, "#2563eb"), (1.0, "#dc2626")],
                labels={"gravite_num": "Intensité pondérée"},
                opacity=0.78,
            )

            if {"intersection", "latitude", "longitude"}.issubset(collisions.columns):
                top_pts = (
                    collisions.groupby("intersection")
                    .agg(
                        total=("gravite_num", "count"),
                        latitude=("latitude", "mean"),
                        longitude=("longitude", "mean"),
                    )
                    .sort_values("total", ascending=False)
                    .head(5)
                    .reset_index()
                )
                fig_map.add_trace(
                    go.Scattermapbox(
                        lat=top_pts["latitude"],
                        lon=top_pts["longitude"],
                        mode="markers+text",
                        text=[f"#{i+1}" for i in range(len(top_pts))],
                        textposition="top center",
                        marker=dict(size=10, color=_t["text"]),
                        hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]} collisions<extra>Hotspot</extra>",
                        customdata=np.column_stack([top_pts["intersection"], top_pts["total"]]),
                        name="Hotspots",
                    )
                )

            fig_map.update_layout(
                height=345,
                margin=dict(l=0, r=0, t=0, b=0),
                paper_bgcolor=plot_bg,
                font=dict(color=plot_font, size=10),
                coloraxis_colorbar=dict(
                    title=dict(text="Intensité", font=dict(size=10)),
                    tickfont=dict(size=9),
                    thickness=12,
                    len=0.52,
                ),
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01,
                    bgcolor=legend_bg,
                    bordercolor=_t["border"],
                    borderwidth=1,
                    font=dict(size=9),
                ),
            )
            st.plotly_chart(fig_map, use_container_width=True, config=dashboard_plot_config)

            st.markdown(
                """
                <div style="display:flex; gap:10px; align-items:center; margin-top:4px; font-size:11px; color:var(--mc-text-subtle);">
                    <span style="display:inline-flex; align-items:center; gap:6px;"><span style="width:10px;height:10px;border-radius:50%;background:#dbeafe;display:inline-block;"></span>faible concentration</span>
                    <span style="display:inline-flex; align-items:center; gap:6px;"><span style="width:10px;height:10px;border-radius:50%;background:#2563eb;display:inline-block;"></span>concentration moyenne</span>
                    <span style="display:inline-flex; align-items:center; gap:6px;"><span style="width:10px;height:10px;border-radius:50%;background:#dc2626;display:inline-block;"></span>concentration élevée</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with col_right:
        st.markdown("""<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.1em; font-weight:600; text-transform:uppercase; margin-bottom:10px;">Top 5 zones à surveiller</div>""", unsafe_allow_html=True)
        if hotspots.empty:
            st.info("Aucun hotspot détecté sur cette période.")
        else:
            for i, row in hotspots.iterrows():
                total = int(row.get("collisions", 0))
                graves = int(row.get("graves", 0))
                h = row.get("heure_moyenne", np.nan)
                if pd.isna(h):
                    h_txt = "heure inconnue"
                else:
                    h_val = int(round(float(h)))
                    h_txt = f"pic vers {h_val}h"
                severity_color = "#dc2626" if total >= 25 else "#d97706" if total >= 15 else "#2563eb"
                place = str(row.get("lieu", f"Zone {i+1}"))
                st.markdown(
                    f"""
                    <div style="background:var(--mc-card-bg); border:1px solid var(--mc-border); border-left:4px solid {severity_color}; border-radius:10px; padding:8px 10px; margin-bottom:6px;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.08em;">ZONE #{i+1}</span>
                            <span style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:{severity_color};">{total} collisions</span>
                        </div>
                        <div style="font-size:12px; font-weight:600; color:var(--mc-text); margin:3px 0 4px 0;">{place}</div>
                        <div style="font-size:11px; color:var(--mc-text-muted);">{graves} graves · {h_txt}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            st.caption("Les repères #1..#5 sont affichés sur la carte pour relier visuellement la zone et son détail.")

    with st.expander("Détails analytiques secondaires (311, météo, tendances)", expanded=False):
        col_a, col_b, col_c = st.columns(3, gap="large")
        with col_a:
            st.markdown("""<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.1em; font-weight:600; text-transform:uppercase; margin-bottom:8px;">Requêtes 311 par type</div>""", unsafe_allow_html=True)
            if req311.empty or "type_service" not in req311.columns:
                st.info("Aucune requête 311 sur cette période.")
            else:
                req_counts = req311["type_service"].fillna("Non spécifié").astype(str).value_counts().head(8)
                req_df = req_counts.rename_axis("type_service").reset_index(name="total")
                req_df["pct"] = (req_df["total"] / max(req_df["total"].sum(), 1) * 100).round(1)
                req_df = req_df.sort_values("total", ascending=True)

                fig_311 = go.Figure(
                    go.Bar(
                        x=req_df["total"],
                        y=req_df["type_service"],
                        orientation="h",
                        marker=dict(color="#2563eb"),
                        text=[f"{v} ({p}%)" for v, p in zip(req_df["total"], req_df["pct"])],
                        textposition="outside",
                        cliponaxis=False,
                        hovertemplate="<b>%{y}</b><br>%{x} requêtes<extra></extra>",
                    )
                )
                fig_311.update_layout(
                    height=250,
                    margin=dict(l=0, r=30, t=0, b=0),
                    paper_bgcolor=plot_bg,
                    plot_bgcolor=plot_bg,
                    font=dict(family="IBM Plex Mono", color=plot_font, size=10),
                    xaxis=dict(title="Nb requêtes", gridcolor=plot_grid),
                    yaxis=dict(title=""),
                )
                st.plotly_chart(fig_311, use_container_width=True, config=dashboard_plot_config)

        with col_b:
            st.markdown("""<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.1em; font-weight:600; text-transform:uppercase; margin-bottom:8px;">Collisions par condition météo</div>""", unsafe_allow_html=True)
            st.markdown(
                """<div style="font-size:11px; color:var(--mc-text-muted); background:var(--mc-warn-bg); border:1px solid var(--mc-warn-border); border-radius:8px; padding:8px 10px; margin-bottom:8px;">
Corrélation observée - causalité non démontrée. Analyse basée sur des volumes non normalisés.
</div>""",
                unsafe_allow_html=True,
            )
            if collisions.empty or "condition_meteo" not in collisions.columns:
                st.info("Pas assez de données météo/collisions sur cette période.")
            else:
                met = collisions.copy()
                met["condition_meteo"] = met["condition_meteo"].fillna("Inconnue").astype(str)
                met["gravite_num"] = pd.to_numeric(met.get("gravite_num"), errors="coerce").fillna(0)
                met_df = (
                    met.groupby("condition_meteo")
                    .agg(
                        collisions=("gravite_num", "count"),
                        taux_graves=("gravite_num", lambda x: round(((x >= 3).sum() / max(len(x), 1)) * 100, 1)),
                    )
                    .sort_values("collisions", ascending=False)
                    .head(8)
                    .sort_values("collisions", ascending=True)
                    .reset_index()
                )

                fig_weather = go.Figure(
                    go.Bar(
                        x=met_df["collisions"],
                        y=met_df["condition_meteo"],
                        orientation="h",
                        marker=dict(
                            color=met_df["taux_graves"],
                            colorscale=[[0, "#dbeafe"], [1, "#dc2626"]],
                            colorbar=dict(title="% graves", thickness=9),
                        ),
                        text=[f"{v} · {t}%" for v, t in zip(met_df["collisions"], met_df["taux_graves"])],
                        textposition="outside",
                        cliponaxis=False,
                        hovertemplate="<b>%{y}</b><br>%{x} collisions<br>%{marker.color}% graves<extra></extra>",
                    )
                )
                fig_weather.update_layout(
                    height=250,
                    margin=dict(l=0, r=30, t=0, b=0),
                    paper_bgcolor=plot_bg,
                    plot_bgcolor=plot_bg,
                    font=dict(family="IBM Plex Mono", color=plot_font, size=10),
                    xaxis=dict(title="Nb collisions", gridcolor=plot_grid),
                    yaxis=dict(title=""),
                )
                st.plotly_chart(fig_weather, use_container_width=True, config=dashboard_plot_config)

        with col_c:
            st.markdown("""<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.1em; font-weight:600; text-transform:uppercase; margin-bottom:8px;">Tendances hebdomadaires</div>""", unsafe_allow_html=True)
            weekly = data_period["weekly_trend"]
            if weekly.empty:
                st.info("Aucune tendance hebdomadaire calculable.")
            else:
                weekly = weekly.copy()
                weekly["semaine_label"] = (
                    weekly["semaine"].astype(str).str.replace("\n", " ", regex=False)
                )
                tick_step = 1 if len(weekly) <= 8 else 2
                tick_vals = weekly["semaine_label"].iloc[::tick_step].tolist()
                coll_series = pd.to_numeric(weekly["collisions"], errors="coerce").fillna(0)
                y1_cfg = dict(
                    title="Collisions",
                    gridcolor=plot_grid,
                    zeroline=False,
                    rangemode="tozero",
                )
                if coll_series.max() <= 0:
                    y1_cfg.update(range=[0, 1], dtick=1)

                fig_trend = go.Figure()
                fig_trend.add_trace(
                    go.Scatter(
                        x=weekly["semaine_label"],
                        y=weekly["collisions"],
                        name="Collisions",
                        line=dict(color="#dc2626", width=2.2),
                        mode="lines+markers",
                        yaxis="y",
                        hovertemplate="Semaine %{x}<br>Collisions: %{y}<extra></extra>",
                    )
                )
                fig_trend.add_trace(
                    go.Scatter(
                        x=weekly["semaine_label"],
                        y=weekly["req311"],
                        name="Requêtes 311",
                        line=dict(color="#2563eb", width=2.2),
                        mode="lines+markers",
                        yaxis="y2",
                        hovertemplate="Semaine %{x}<br>Req. 311: %{y}<extra></extra>",
                    )
                )
                fig_trend.update_layout(
                    height=250,
                    margin=dict(l=0, r=0, t=0, b=0),
                    paper_bgcolor=plot_bg,
                    plot_bgcolor=plot_bg,
                    font=dict(family="IBM Plex Mono", color=plot_font, size=10),
                    hovermode="x unified",
                    xaxis=dict(
                        gridcolor=plot_grid,
                        tickmode="array",
                        tickvals=tick_vals,
                        ticktext=tick_vals,
                        tickangle=-32,
                        tickfont=dict(size=9),
                        automargin=True,
                    ),
                    yaxis=y1_cfg,
                    yaxis2=dict(
                        title="Req. 311",
                        overlaying="y",
                        side="right",
                        showgrid=False,
                        zeroline=False,
                        rangemode="tozero",
                        tickformat=",.0f",
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="left",
                        x=0,
                        bgcolor=legend_bg,
                        bordercolor=_t["border"],
                        borderwidth=1,
                        font=dict(size=9),
                    ),
                )
                st.plotly_chart(fig_trend, use_container_width=True, config=dashboard_plot_config)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE — BRIEFING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Briefing":
    if "briefing_mode_selector" not in st.session_state:
        st.session_state.briefing_mode_selector = "Grand public"
    if st.session_state.briefing_mode_selector not in {"Municipalité", "Grand public"}:
        st.session_state.briefing_mode_selector = "Grand public"

    st.markdown(
        """
<style>
.st-key-briefing_mode_selector [data-testid="stRadio"] > div[role="radiogroup"] {
    display: grid !important;
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: 10px !important;
    max-width: 760px !important;
    margin: 0 0 12px 0 !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label {
    margin: 0 !important;
    padding: 12px 14px !important;
    border-radius: 12px !important;
    border: 1px solid var(--mc-border) !important;
    background: var(--mc-card-bg) !important;
    transition: border-color 180ms ease, box-shadow 180ms ease, transform 180ms ease, background 180ms ease !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label:hover {
    transform: translateY(-1px) !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label:nth-of-type(1) {
    --mode-accent: #2563eb;
    --mode-surface: color-mix(in srgb, #2563eb 14%, var(--mc-card-bg) 86%);
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label:nth-of-type(2) {
    --mode-accent: #16a34a;
    --mode-surface: color-mix(in srgb, #16a34a 14%, var(--mc-card-bg) 86%);
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label:has(input:checked) {
    border-color: var(--mode-accent) !important;
    background: var(--mode-surface) !important;
    box-shadow: 0 8px 22px color-mix(in srgb, var(--mode-accent) 22%, transparent) !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label > div:first-child {
    margin-right: 10px !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label > div:first-child [type="radio"] {
    accent-color: var(--mode-accent) !important;
}
.st-key-briefing_mode_selector [data-testid="stRadio"] label p {
    font-family: var(--font) !important;
    font-size: 15px !important;
    font-weight: 600 !important;
    color: var(--mc-text) !important;
    letter-spacing: -0.01em !important;
}
.briefing-mode-banner {
    border: 1px solid var(--mc-border);
    border-left-width: 4px;
    border-radius: 12px;
    background: var(--mc-surface);
    padding: 10px 12px;
    margin: 6px 0 14px 0;
}
.briefing-mode-banner .kicker {
    font-family: var(--font-mono);
    font-size: 10px;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    color: var(--mc-text-subtle);
    margin-bottom: 4px;
}
.briefing-mode-banner .title {
    font-family: var(--font);
    font-size: 14px;
    font-weight: 600;
    color: var(--mc-text);
    margin-bottom: 2px;
}
.briefing-mode-banner .subtitle {
    font-family: var(--font);
    font-size: 12px;
    color: var(--mc-text-muted);
}
.briefing-mode-banner.is-municipal {
    border-left-color: #2563eb;
    background: color-mix(in srgb, #2563eb 10%, var(--mc-card-bg) 90%);
}
.briefing-mode-banner.is-public {
    border-left-color: #16a34a;
    background: color-mix(in srgb, #16a34a 10%, var(--mc-card-bg) 90%);
}
.mc-briefing-shell {
    border: 1px solid var(--mc-border);
    border-radius: 18px;
    padding: 12px;
}
.mc-briefing-shell.is-municipal {
    background: linear-gradient(180deg, color-mix(in srgb, #2563eb 10%, transparent) 0%, transparent 52%);
}
.mc-briefing-shell.is-public {
    background: linear-gradient(180deg, color-mix(in srgb, #16a34a 10%, transparent) 0%, transparent 52%);
}
body:has(.mc-briefing-shell.is-municipal) [data-testid="stAppViewContainer"] .main .block-container {
    background: linear-gradient(180deg, color-mix(in srgb, #2563eb 5%, var(--bg) 95%) 0%, var(--bg) 46%) !important;
}
body:has(.mc-briefing-shell.is-public) [data-testid="stAppViewContainer"] .main .block-container {
    background: linear-gradient(180deg, color-mix(in srgb, #16a34a 5%, var(--bg) 95%) 0%, var(--bg) 46%) !important;
}
@media (max-width: 820px) {
    .st-key-briefing_mode_selector [data-testid="stRadio"] > div[role="radiogroup"] {
        grid-template-columns: 1fr !important;
        max-width: 100% !important;
    }
}
</style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """<div style="font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--mc-text-subtle); letter-spacing:0.08em; text-transform:uppercase; margin-bottom:10px;">Briefing automatique (période sélectionnée)</div>""",
        unsafe_allow_html=True,
    )

    view_label = st.radio(
        "Mode de lecture briefing",
        options=["Municipalité", "Grand public"],
        key="briefing_mode_selector",
        horizontal=True,
        label_visibility="collapsed",
    )
    tone = "municipal" if view_label == "Municipalité" else "public"
    mode_class = "is-municipal" if tone == "municipal" else "is-public"
    mode_title = (
        "Lecture opérationnelle — Municipalité"
        if tone == "municipal"
        else "Lecture pédagogique — Grand public"
    )
    mode_subtitle = (
        "Indicateurs techniques, niveaux de gravité, priorisation d’actions terrain."
        if tone == "municipal"
        else "Explications claires, zones de vigilance et gestes concrets pour les citoyens."
    )
    st.markdown(
        f"""
        <div class="briefing-mode-banner {mode_class}">
            <div class="kicker">Mode actif</div>
            <div class="title">{mode_title}</div>
            <div class="subtitle">{mode_subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("Génération du briefing en cours..."):
        briefing_content = generate_briefing(
            data_period,
            tone=tone,
            periode=periode,
            reference_data=data,
        )
    st.markdown(f"""<div class="mc-briefing-shell {mode_class}">{briefing_content}</div>""", unsafe_allow_html=True)
    files_txt = " · ".join([str(p.name) for p in weekly_briefing_files])
    st.caption(f"Snapshots hebdo auto générés: {files_txt}")
