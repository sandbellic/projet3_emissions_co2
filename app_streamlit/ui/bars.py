import streamlit as st
from utils.utilitaires import etapes_chemin
from core.styles import load_css

# ==============================
# CHARGEMENT CSS !!! à insérer avant tout widget streamlit
# ==============================
load_css("styles/style.css")


def compute_width_and_label(width, value):
    MIN_VISIBLE = 20  # largeur mini pour afficher le texte dedans
    """
    Si la barre est trop petite :
    - on garde une largeur mini visuelle
    - on affiche le texte dehors
    """
    if width < MIN_VISIBLE:
        return MIN_VISIBLE, f'<span class="outside-label">{value:.1f}</span>'
    else:
        return width, f"{value:.1f}"


# ----------------
# LEGENDE DES COULEURS part transport / fabrication
# ----------------
def legende_barre():

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

def titre_barre(mode_transport, chemin_villes):
    st.markdown(f"""
        <div class="bar_label">{mode_transport}
            <span class="chemin-villes"<{chemin_villes}</span>
        </div>
    """, unsafe_allow_html=True)


def affichage_barre(row, maxv, chemin_villes):
        
    transport = row["total_transport"]
    fabrication = row["total_fabrication"]
    total = row['co2_global']
    width_total = (total / maxv) * 100 if maxv else 0
    if total > 0:
        width_transport = (transport / total) * 100
        display_transport, label_transport = compute_width_and_label(width_transport, transport)
        width_fabrication = (fabrication / total) * 100
        display_fabrication, label_fabrication = compute_width_and_label(width_fabrication, fabrication)
        # évite de dépasser 100%
        total_display = display_transport + display_fabrication
        if total_display > 100:
            ratio = 100 / total_display
            display_transport *= ratio
            display_fabrication *= ratio

    titre_barre(row['mode_transport'], chemin_villes)
    # barre principale
    st.markdown(f"""
        <div class="custom-bar">
            <div class="wrapper-bar" style="width:{min(width_total,100)}%;">
                <div class="transport-bar" style="width:{display_transport}%;"> {label_transport}  </div>
                <div class="fabrication-bar" style="width:{display_fabrication}%;"> {label_fabrication} </div>
            </div>
        </div>
        <small>Total : {total:.1f} kg</small>
    """, unsafe_allow_html=True)



def render_bars(df_affichage, df_villes):

    st.markdown('<div class="section-title">📈 Impact visuel</div>', unsafe_allow_html=True)

    match st.session_state['preference']:
        case "pour un trajet":        
            #Tri barres et calcul longueur max barre
            df_affichage_barres = df_affichage[df_affichage['distance'] != 0]   #on va afficher uniquement les transports valides
            df_affichage_barres = df_affichage_barres.sort_values(["type_transport", "mode_transport"], ascending=[True,False])
        case "sur une distance":
            #pour km
            #MAJ df_affichage totaux avant affichage     
            df_affichage['total_transport'] = st.session_state['distance_input'] * df_affichage['part_transport'] /1000
            df_affichage['total_fabrication'] = st.session_state['distance_input'] * df_affichage['part_fabrication'] /1000
            df_affichage['co2_global'] = st.session_state['distance_input']  * (df_affichage['co2_global'])/1000  
            #Tri barres et calcul longueur max barre
            df_affichage_barres = df_affichage.sort_values(["co2_global","type_transport"], ascending=[False,True])
            #st.dataframe(df_affichage_barres)
        case _:
            st.warning("cas non prévu !!!!!!", icon="⚠️")
    
    maxv = (df_affichage["co2_global"]).max()

    legende_barre()

    # Pour chaque mode transport
    # 1 ligne pour afficher mode de transport, étapes gares, et selectbox(s) pour personnalisation
    # des calculs co2
    # 1 ligne pour affiche barre proportionnelle à l'impact total, avec des couleurs différentes
    # pour la part liée au transport et part fabrication

    #boucle sur les différents mode de transport à afficher

    for idx, row in df_affichage_barres.iterrows():
        chemin_villes = etapes_chemin(row, df_villes)  #va servir à afficher les étapes trajet train
 




        #width_transport, transport, width_fabrication, fabrication, total = personnalisation_barre(preference, row, idx, maxv, df_villes)
        
    
        affichage_barre(row,maxv,chemin_villes)
