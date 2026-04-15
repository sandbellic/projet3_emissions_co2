import streamlit as st
import pandas as pd

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="KPI - Émissions CO₂", layout="centered")

# -------------------------
# STYLE - (réutilise le même CSS que ta page principale)
# --------------------------
st.markdown("""
<style>
    /* Copie ici le CSS de ta page principale */
    .header {
        background-color: #2E7D32;
        color: white;
        padding: 10px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 20px;
    }
    .block {
        background-color: white;
        padding: 20px;
        border-radius: 15px;
        margin-bottom: 15px;
        box-shadow: 0px 4px 10px rgba(0,0,0,0.05);
    }
    .kpi-title {
        color: #2E7D32;
        font-size: 20px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------
# Titre de la page
# -------------------------
st.markdown('<div class="header"><h1>📊 KPI - Émissions CO₂</h1></div>', unsafe_allow_html=True)

# -------------------------
# Données d'exemple (à remplacer par tes données réelles)
# -------------------------
data = {
    "Transport": ["Voiture 🚗", "Avion ✈️", "Train 🚆", "Bus 🚌"],
    "CO₂/100km (kg)": [20, 25, 1, 12.2],
    "Temps moyen (h)": [1.5, 0.5, 2, 3],
}

df = pd.DataFrame(data)

# -------------------------
#--- Section 1 : CO₂ par trajet/transport ---
# -------------------------
st.markdown('<div class="kpi-title">CO₂ par trajet et par transport</div>', unsafe_allow_html=True)
st.dataframe(df.set_index("Transport"), use_container_width=True)

# -------------------------
#-- Section 2 : Différences entre moyens de transport ---
# -------------------------
st.markdown('<div class="kpi-title">Différences entre les moyens de transport</div>', unsafe_allow_html=True)
st.bar_chart(df.set_index("Transport")["CO₂/100km (kg)"])

# -------------------------
#--- Section 3 : Ratio temps/CO₂ ---
# -------------------------
st.markdown('<div class="kpi-title">Ratio temps / CO₂</div>', unsafe_allow_html=True)
df["Ratio (h/kg)"] = df["Temps moyen (h)"] / df["CO₂/100km (kg)"]
st.line_chart(df.set_index("Transport")["Ratio (h/kg)"])

# -------------------------
# Bouton pour revenir à l'accueil
# -------------------------
if st.button("⬅️ Retour à l'accueil"):
    st.switch_page("streamlit_app.py")