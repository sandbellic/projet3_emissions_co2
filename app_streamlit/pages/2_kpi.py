import streamlit as st
import pandas as pd
from app_streamlit.utils.styles_utils import load_css

# ==============================
# chargement du CSS !!! à insérer avant tout widget streamlit
# ==============================
load_css("styles/style.css")

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="KPI - Émissions CO₂", page_icon= "🌳", layout="centered")


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
        # Affichage résultat sous forme de BARRES proportionnelles
        # -------------------------
        st.markdown('<div class="section-title">📈 Impact visuel</div>', unsafe_allow_html=True)

        maxv = (df["part_transport"] + df["part_fabrication"]).max()

        # Légende des couleurs
        st.markdown("""
        <div style="display:flex; gap:20px; margin-bottom:10px;">
            <div style="display:flex; align-items:center; gap:6px;">
                <div style="width:14px; height:14px; background:#4CAF50; border-radius:3px;"></div>
                <span>Transport</span>
            </div>
            <div style="display:flex; align-items:center; gap:6px;">
                <div style="width:14px; height:14px; background:#FF9800; border-radius:3px;"></div>
                <span>Fabrication</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        #sur une ligne on affiche les 2 barres (transport et fabrication) avec des couleurs différentes, 
        # la longueur de la barre est proportionnelle à l'impact total, 
        # et la part de chaque composant est indiquée par sa couleur propre 
        for t in df.index:
            chemin = df.loc[t, "chemin"]
            chemin_villes = []
            for c in chemin:
                chemin_villes.append(df_villes.loc[df_villes['id_commune'] == c, 'name'].iloc[0])
            
            transport = df.loc[t, "part_transport"]
            fabrication = df.loc[t, "part_fabrication"]
            total = round(transport + fabrication,1)

            width_total = (total / maxv) * 100
            width_transport = (transport / total) * 100 if total != 0 else 0
            width_fabrication = (fabrication / total) * 100 if total != 0 else 0

            st.markdown(f"""
            <div class="bar-container">
                <div class="bar-label">{t} {chemin_villes}</div>
                <div class="custom-bar">
                    <div class="transport-part" style="width:{width_transport}%;">
                       {transport:.1f} </div>
                    <div class="fabrication-part" style="width:{width_fabrication}%;">
                       {fabrication:.1f} </div>
                </div>
                <div class="total-text">Total : {total} kg CO₂</div>

            """, unsafe_allow_html=True)




# -------------------------
# Bouton pour revenir à l'accueil
# -------------------------
if st.button("⬅️ Retour à l'accueil"):
    st.switch_page("streamlit_app.py")