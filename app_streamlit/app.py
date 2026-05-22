import streamlit as st
from core.styles import load_css
from state.session import init_state
from ui.layout import render_layout


# ==============================
# CHARGEMENT CSS !!! à insérer avant tout widget streamlit
# ==============================
load_css("styles/style.css")

# -------------------------
# CONFIG PAGE
# -------------------------
st.set_page_config(page_title="Calculateur CO2", page_icon="🌳", layout="centered")

# ----------------------
# INITIALISATION SESSION_STATE
# ----------------------
init_state()

render_layout()