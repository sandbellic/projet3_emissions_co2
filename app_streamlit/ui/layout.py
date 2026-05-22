from core.data import load_all_data
from core.calculations import distance_duree_global
from core.styles import load_css

from ui.home import render_home
from ui.cards import render_cards
from ui.bars import render_bars
from ui.map import render_map
from ui.filters import render_filters
from ui.tree import render_tree
from ui.home import render_home 
from utils.utilitaires import *


# ==============================
# CHARGEMENT CSS !!! à insérer avant tout widget streamlit
# ==============================
load_css("styles/style.css")


def render_layout():

    # 1. LOAD DATA 
    df_villes, df_routes_air, df_routes_train, df_emissions_co2, df_affichage = load_all_data()
    #st.dataframe(df_affichage)

    # 2. ÉCRAN D'ACCUEIL / INPUT
    render_home(df_villes, liste_villes_selection, reset_itineraire, reset_km)
    
    # Lancement calcul et préparation données pour affichage
    if st.session_state['calcul'] == True:
        
        with st.spinner("Calcul en cours..."):
            coords, distance_route_value = distance_duree_global(df_villes, df_routes_air, df_routes_train, df_affichage)

        match st.session_state['preference']:
            
            case "pour un trajet":     
                # 1. données cartes
                render_cards(df_affichage)

                # 2. filtres utilisateur
                #filters = ()

                # 3. barres dynamiques
                render_bars(df_affichage, df_villes)

                # 4. équivalent arbres
                render_tree(df_affichage)

                # 5. carte (peut dépendre partiellement des filtres)
                render_map(coords, distance_route_value)

            case "sur une distance":
                #pass
                # 2. filtres utilisateur
                #filters = render_filters()

                # 3. barres dynamiques
                render_bars(df_affichage, df_villes)      

