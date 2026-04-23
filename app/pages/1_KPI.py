import streamlit as st
import pandas as pd

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="KPI - Émissions CO₂", layout="centered")

# -------------------------
# STYLE
# -------------------------
st.markdown("""
<style>
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
# TITRE
# -------------------------
st.markdown('<div class="header"><h1>📊 KPI - Émissions CO₂</h1></div>', unsafe_allow_html=True)

# -------------------------
# DONNÉES
# -------------------------
data = {
    "Transport": ["Voiture 🚗", "Avion ✈️", "Train 🚆", "Bus 🚌"],
    "CO₂/100km (kg)": [20, 25, 1, 12.2],
    "Temps moyen (h)": [1.5, 0.5, 2, 3],
}

df = pd.DataFrame(data)

# -------------------------
# SECTION 1
# -------------------------
st.markdown('<div class="kpi-title">CO₂ par transport</div>', unsafe_allow_html=True)
st.dataframe(df.set_index("Transport"), use_container_width=True)

# -------------------------
# SECTION 2
# -------------------------
st.markdown('<div class="kpi-title">Comparaison CO₂</div>', unsafe_allow_html=True)
st.bar_chart(df.set_index("Transport")["CO₂/100km (kg)"])

# -------------------------
# SECTION 3
# -------------------------
st.markdown('<div class="kpi-title">Ratio temps / CO₂</div>', unsafe_allow_html=True)
df["Ratio (h/kg)"] = df["Temps moyen (h)"] / df["CO₂/100km (kg)"]
st.line_chart(df.set_index("Transport")["Ratio (h/kg)"])

# -------------------------
# RETOUR
# -------------------------
if st.button("⬅️ Retour à l'accueil"):
    st.switch_page("streamlit_app.py")