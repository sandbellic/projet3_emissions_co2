import streamlit as st
import math

# -------------------------
# Affichage résultat sous forme de ÉQUIVALENT pour le trajet le + émetteur
# -------------------------


def render_tree(df_affichage):

    CO2_ANNUEL_ARBRE = 21  #nb de kg moyen co2 absorbé par arbre sur une année
    df_co2_max = df_affichage.sort_values(by="co2_global", ascending=False).head(1)
                
    st.markdown(
                f"""<div class="section-title">🌍 Équivalent arbre pour transport plus gros émetteur de CO2 : {df_co2_max["mode_transport"].iloc[0]}</div>""",
                unsafe_allow_html=True)

    nb_arbres = math.ceil(df_co2_max['co2_global'].iloc[0] / CO2_ANNUEL_ARBRE)

    st.info(f"""🌳 {nb_arbres} arbres sont nécessaires pour absorber le co2 émis (sur ~1 an d'absorption moyenne)""")
