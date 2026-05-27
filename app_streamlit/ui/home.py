import streamlit as st
import pandas as pd


# -------------------------
# UI - AFFCHAGE DES DONNEES DE SELECTION / ITINERAIRE OU KM
# -------------------------
def render_home(df_villes, liste_villes_selection, reset_itineraire, reset_km):

    st.title("🌍 Calculateur CO2 intelligent")
    st.markdown(f"""<h3>Comparez l'impact carbone des moyens de transport par personne</h3>""", unsafe_allow_html=True)

    # ----------------
    # choix mode de calcul : définition d'un itinéraire ou comparaison pure
    # ----------------
    st.radio(
        "Souhaitez-vous effectuer une comparaison : ", ["pour un trajet", "sur une distance"],
        horizontal= True, key="preference")

    # ----------------
    # Mode TRAJET
    # ----------------

    match st.session_state['preference']:
        
        case "pour un trajet":      
            st.session_state.calcul = False
            #liste des villes servant de choix pour départ et destination
            villes_finales = liste_villes_selection(df_villes)

            #présentation choix départ / destination + A/R
            col_depart, col_arrivee = st.columns(2)
            with col_depart:
                depart = st.selectbox("Ville de départ", villes_finales, index=None, placeholder="Choisir ou taper le nom de la ville",key="depart_selectbox")
            with col_arrivee:
                arrivee = st.selectbox("Ville d'arrivée", villes_finales, index=None, placeholder="Choisir ou taper le nom de la ville",key="arrivee_selectbox")
            aller_retour = st.checkbox("Aller/Retour", value=False, key = "ar")

            #boutons Calcul itineraire / Reset
            col_calcul_btn,col_milieu , col_reset_btn = st.columns([5,3,1])
            with col_calcul_btn:
                if st.button("Calculer selon trajet"):
                    if pd.isnull(depart) or pd.isnull(arrivee):
                        st.warning("Vous devez choisir une destination complète (villes de départ et d'arrivée)", icon="⚠️")
                    elif depart == arrivee:
                        st.warning("Choisissez une ville d'arrivée différente de la ville de départ", icon="⚠️")
                    else:
                        st.session_state.calcul = True
            with col_reset_btn:
                st.button("Reset", on_click=reset_itineraire)

    # ----------------
    # Mode DISTANCE KM
    # ----------------
        case "sur une distance":
            distance_input = st.number_input("Votre distance en km : ",min_value=0, key="distance_input")
            #boutons calcul / Reset
            col_calcul_btn,col_milieu , col_reset_btn = st.columns([5,3,1])
            with col_calcul_btn:
                if st.button("Calculer selon distance"):
                    if pd.isnull(distance_input) or distance_input <= 0:
                        st.warning("Saisissez une distance de calcul positive", icon="⚠️")
                    else:
                        st.session_state.calcul = True
            with col_reset_btn:
                st.button("Reset", on_click=reset_km)


        case _:
            st.warning("cas non prévu !!!!!!", icon="⚠️")

