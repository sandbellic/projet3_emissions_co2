import streamlit as st

def init_state():

    #initialisation des session_state
    defaults = {
        # filtres UI
        "motorisation": "Electrique",
        "categorie": "Petite",
        "covoiturage": 1,

        "preference" : "pour un trajet",
        "calcul" : False,
   
        "distance_input" : 0,

        "depart_selectbox" : None,
        "arrivee_selectbox" : None,
        "ar" : False,
    }

    for k, v in defaults.items():
        st.session_state.setdefault(k, v)



