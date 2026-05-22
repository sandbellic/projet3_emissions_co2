import streamlit as st


# -----------------
# AFFICHAGE DES CARTES PAR LIGNE DE 3
# -----------------
def affichage_cards(df_affichage_cards):

    NB_COLS = 3
    index_list = list(df_affichage_cards.index)
    for start in range(0, len(index_list), NB_COLS):
        cols = st.columns(NB_COLS)
        for i in range(NB_COLS):
            current_index = start + i
            # évite de dépasser le nombre d'éléments
            if current_index < len(index_list):
                t = index_list[current_index]
                transport = df_affichage_cards.loc[t, 'mode_transport']
                icone = df_affichage_cards.loc[t, 'icone']
                valeur = df_affichage_cards.loc[t, 'co2_global']
                distance = df_affichage_cards.loc[t, 'distance']
                duree = df_affichage_cards.loc[t, 'duree']

                with cols[i]:
                    st.markdown(f"""<div class="transport-card">
                                <div class="transport-title">{transport} {icone}</div>
                                <div class="transport-distance">Distance : {distance} km</div>
                                <div class="transport-duree">Durée : {duree}</div>
                                <div class="transport-co2">{valeur:.1f} kg CO₂</div>
                                </div>
                        """, unsafe_allow_html=True)



def render_cards(df_affichage):
    st.markdown('<div class="section-title">📊 Comparaison</div>', unsafe_allow_html=True)
    df_affichage_cartes = df_affichage.sort_values(by="co2_global")
    affichage_cards(df_affichage_cartes)
