        
#A SUPPRIMER AVANT LIVRAISON!!!!!!!!!!!!!!!!!!        

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import math

from utils.data_loaders import *
from utils.helpers import *
from utils.styles_loaders import load_css

# ==============================
# chargement du CSS !!! à insérer avant tout widget streamlit
# ==============================
load_css("styles/style.css")

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="Calculateur CO2", page_icon="🌳", layout="centered")


# -------------------------
# SESSION
# -------------------------
if "calcul" not in st.session_state:
    st.session_state.calcul = False


# ----------------------
# CONNEXION POSTGRE SQL et chargement de nos dataframes
# -------------
conn = get_conn()
df_villes = load_villes(conn)
df_routes_air = load_routes_air(conn)
df_routes_train = load_routes_train(conn)
df_emissions_co2 = load_emissions_co2(conn)

# -------------------------
# variables - va servir à enregistrer les émission co2 - en kg / par personne/ par km
# Pour le train et l'avion, les valeurs sont déjà incluses dans dim_routes 
# A revoir pour la voiture. .....
# -------------------------
#dictionnaire contenant les valeurs affichage libellés + champs motorisation(detail) et catégorie(taille)
df_affichage = load_affichage(df_emissions_co2)
variables = {}


# -------------------------
# UI - AFFCHAGE DES DONNEES DE SELECTION / ITINERAIRE OU KM
# -------------------------
st.title("🌍 Calculateur CO2 intelligent")
st.markdown(f"""<h3>Comparez l'impact carbone des moyens de transport par personne</h3>""", unsafe_allow_html=True)

# choix entre définition d'un itinéraire ou comparaison pure
preference = st.radio(
    "Souhaitez-vous effectuer une comparaison : ", ["pour un trajet", "sur une distance"],
    horizontal= True,)

if preference  == "pour un trajet":
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
        if st.button("Calculer le trajet"):
            if pd.isnull(depart) or pd.isnull(arrivee):
                st.warning("Vous devez choisir une destination complète (villes de départ et d'arrivée)", icon="⚠️")
            elif depart == arrivee:
                st.warning("Choisissez une ville d'arrivée différente de la ville de départ", icon="⚠️")
            else:
                st.session_state.calcul = True
    with col_reset_btn:
        st.button("Reset", on_click=reset_itineraire)

else: #choix KM
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


# -------------------------
# LOGIQUE
# -------------------------
if preference  == "pour un trajet":
    
    if st.session_state.calcul:
         
        with st.spinner("Calcul en cours..."):

            # -----------------
            #récupération des identifiants des communes de départ et d'arrivée pour calculs
            # -----------------
            id_commune_dep = int(df_villes.loc[df_villes["label"] == depart,"id_commune"].iloc[0])
            id_commune_arr = int(df_villes.loc[df_villes["label"] == arrivee,"id_commune"].iloc[0])
            # définition du coef Aller/retour
            coef_ar = 2 if aller_retour else 1
            
            #----------------
            # calcul distance pour les avions 
            #----------------
            variables["air"] = distance_air(df_routes_air, id_commune_dep, id_commune_arr, coef_ar)
                
            #----------------
            # calcul distance pour les trains
            #----------------
            variables["rail"] = distance_rail(df_routes_train, id_commune_dep, id_commune_arr, coef_ar)
            
            #----------------
            # calcul distance pour les voitures
            #----------------
            variables["route"] = distance_route(df_villes, id_commune_dep, id_commune_arr, coef_ar)
            distance_route = variables["route"]['Distance']
            coords = variables["route"]['coords']  #données utilisées plus bas pour affichage carte


            if distance_route:
                
                #convertit variables en format dataframe, renommage index en type_transport, et conversion
                #distance pour être certain pas pb dans les calculs ensuite
                df_variables = pd.DataFrame.from_dict(variables, orient="index")
                df_variables = df_variables.reset_index().rename(columns={"index": "type_transport"})
                df_variables['Distance'] = pd.to_numeric(df_variables['Distance'], errors='coerce')
            

                # utilisation des données récupérées de variables (distance, durée, ...)
                # pour préparer df_affichage en vue de son display  
                for idx, row in df_affichage.iterrows():
                    df_ligne = df_variables[df_variables['type_transport'] == row['type_transport']]
                    if not df_ligne.empty:
                        distance = df_ligne.iloc[0]['Distance']           
                        df_affichage.at[idx, 'total_transport'] = distance * row['part_transport'] /1000
                        df_affichage.at[idx, 'total_fabrication'] = distance * row['part_fabrication'] /1000
                        df_affichage.at[idx, 'co2_global'] = distance * (row['part_transport'] + row['part_fabrication'])/1000
                        df_affichage.at[idx, 'chemin'] = df_ligne.iloc[0]['chemin']
                
                # -------------------------
                # Affichage résultats sous forme de CARTES
                # -------------------------
                st.markdown('<div class="section-title">📊 Comparaison</div>', unsafe_allow_html=True)

                df_affichage_cartes = df_affichage.sort_values(by="co2_global")
                affichage_cartes(df_affichage_cartes)


                # -------------------------
                # Affichage résultat sous forme de BARRES proportionnelles
                # -------------------------
                st.markdown('<div class="section-title">📈 Impact visuel</div>', unsafe_allow_html=True)
            
                #Tri barres et calcul longueur max barre
                df_affichage_barres = df_affichage.sort_values(["type_transport", "mode_transport"], ascending=[True,False])
                maxv = (df_affichage["co2_global"]).max()

                legende_barre()

                # Pour chaque mode transport
                # 1 ligne pour afficher mode de transport, étapes gares, et selectbox(s) pour personnalisation
                # des calculs co2
                # 1 ligne pour affiche barre proportionnelle à l'impact total, avec des couleurs différentes
                # pour la part liée au transport et part fabrication

                #boucle sur les différents mode de transport à afficher
                for idx, row in df_affichage_barres.iterrows():
                    width_transport, transport, width_fabrication, fabrication, total = personnalisation_barre(preference, row, idx, maxv, df_villes)
                    affichage_barre(width_transport, transport, width_fabrication, fabrication, total)


                # -------------------------
                # Affichage résultat sous forme de ÉQUIVALENT pour le trajet le + émetteur
                # -------------------------
                CO2_ANNUEL_ARBRE = 21  #nb de kg moyen co2 absorbé par arbre sur une année
                df_co2_max = df_affichage.sort_values(by="co2_global", ascending=False).head(1)
                st.markdown(
                    f"""<div class="section-title">🌍 Équivalent arbre pour transport plus gros émetteur de CO2 : {df_co2_max["mode_transport"].iloc[0]}</div>""",
                            unsafe_allow_html=True)

                nb_arbres = math.ceil(df_co2_max['co2_global'].iloc[0] / CO2_ANNUEL_ARBRE)

                st.info(f"""🌳 {nb_arbres} arbres sont nécessaires pour absorber le co2 émis (sur ~1 an d'absorption moyenne)""")


                # -------------------------
                # Affichage CARTE géographique du trajet voiture
                # -------------------------

                st.markdown('<div class="section-title">🗺️ Trajet par routes', unsafe_allow_html=True)
                st.success(f"📏 Distance route réelle : {distance_route:.1f} km")
                
                affichage_carte_geo(coords)


else:
    #calcul avec distance
    #affichage uniquement en barres

    if st.session_state.calcul:
        # -------------------------
        # Affichage résultat sous forme de BARRES proportionnelles
        # -------------------------
        st.markdown('<div class="section-title">📈 Impact visuel</div>', unsafe_allow_html=True)

        #MAJ df_affichage totaux avant affichage     
        df_affichage['total_transport'] = distance_input * df_affichage['part_transport'] /1000
        df_affichage['total_fabrication'] = distance_input * df_affichage['part_fabrication'] /1000
        df_affichage['co2_global'] = distance_input * (df_affichage['part_transport'] + df_affichage['part_fabrication'])/1000
        
        #Tri barres et calcul longueur max barre
        df_affichage_barres = df_affichage.sort_values(["co2_global","type_transport"], ascending=[False,True])
        maxv = (df_affichage["co2_global"]).max()

        # Légende des couleurs de la barre
        legende_barre()

        # Pour chaque mode transport
        # 1 ligne pour afficher mode de transport, étapes gares, et selectbox(s) pour personnalisation
        # des calculs co2
        # 1 ligne pour affiche barre proportionnelle à l'impact total, avec des couleurs différentes
        # pour la part liée au transport et part fabrication

        #boucle sur les différents mode de transport à afficher
        for idx, row in df_affichage_barres.iterrows():
            width_transport, transport, width_fabrication, fabrication, total = personnalisation_barre(preference, row, idx, maxv, df_villes)
            affichage_barre(width_transport, transport, width_fabrication, fabrication, total)



# Bouton pour aller aux KPI
#if st.button("📊 Voir les KPI"):
#  0  st.switch_page("pages/2_kpi.py")

st.markdown('<div class="footer">Moyens de Transport - Impact CO₂</div>', unsafe_allow_html=True)



# ------------------
ancien data_loaders
import pandas as pd
import streamlit as st
import psycopg2 # pour chargement base

# ----------------------
# CONNEXION POSTGRE SQL
# -------------

#cache_resource pour les databases, et cache_data pour les dataframes

#ici on ne va pas utiliser .env (back end et script python LOCAUX)
# mais on va mettre le password dans le secrets.toml
# STREAMLIT A SON POPRE SYSTEME DE SECURITE  .streamlit/secrets.toml
def get_conn():
    conn = psycopg2.connect(
        database = st.secrets["POSTGRES_DB"],
        user = st.secrets["POSTGRES_USER"],
        password = st.secrets["POSTGRES_PASSWORD"],
        host = st.secrets["POSTGRES_HOST"],
        port = st.secrets["POSTGRES_PORT"]
        )
    return conn

#chargement des données stockés dans postgre
def load_data(query,conn):
    return pd.read_sql(query,conn)

@st.cache_data
def load_villes(_conn):
    query = "select * from emissions_co2.dim_communes"
    return load_data(query,_conn)

@st.cache_data
def load_routes_air(_conn):
    #chargement des routes air 
    query = "select * from emissions_co2.dim_routes"
    df_routes = load_data(query,_conn)
    return df_routes[df_routes['type_transport']=='Avion trajet court']

@st.cache_data
def load_routes_train(_conn):
    #chargement des routes trains
    query = "select * from emissions_co2.dim_routes"
    df_routes = load_data(query,_conn)
    return df_routes[df_routes['type_transport']!='Avion trajet court']

@st.cache_data
def load_emissions_co2(_conn):
    #chargement des émissions co2 de différents transports
    query = "select * from emissions_co2.dim_emission_co2_par_transport"
    return load_data(query,_conn)


def clean_list(x):
    x = x.dropna()
    x = x[~x.astype(str).isin(["", " ", "None", "nan"])]
    vals = x.unique()
    return list(vals) if len(vals) > 0 else None

@st.cache_data
def load_affichage(df_emissions_co2):
    ICONES = {"avion": "✈️", "train" : "🚆",  "moto":"🏍", "autocar": "🚌", "voiture": "🚘", "thermique": "💥", "electrique": "⚡️", "hybride" : "💥⚡️", "autre": "🚑"}
    #on veut un dataframe qui va nous servir pour affichage des lignes
    df_affichage = (
        df_emissions_co2.groupby("mode_transport")
        .agg({
              "taille": clean_list,
              "detail": clean_list
        })
    )
    #transformation de mode_transport en une colonne
    df_affichage = df_affichage.reset_index()
    #on ajoute à ce dataframe :
    # le(s) icone(s) selon la présence des mots liés aux icones dans la clé de dict_affichage (correspond à mode_transport)
    # les valeurs part_fabrication, part_transport pour chaque mode_transport avec taille et détail vides
    # et si pas taille et détail vides on prend la valeur min sur les 2
    # Création des nouvelles colonnes
    df_affichage["icone"] = ""
    df_affichage["part_transport"] = 0.0
    df_affichage["part_fabrication"] = 0.0
    df_affichage["type_transport"] = None
    #celles-ci seront utilisées plus tard
    df_affichage["chemin"] = None
    df_affichage["total_transport"] = 0.0
    df_affichage["total_fabrication"] = 0.0
    df_affichage["co2_global"] = 0.0
    df_affichage['covoiturage'] = False

    for idx, row in df_affichage.iterrows():
        #alimentation colonne icone
        mode_transport_lower = row['mode_transport'].lower()
        icone = []
        for transport, emoji in ICONES.items():
            if transport in mode_transport_lower:
                icone.append(emoji)
        df_affichage.at[idx, "icone"] = "".join(icone)

        #gestion des colonnes part_transport et part_fabrication
        df_ligne = df_emissions_co2[(df_emissions_co2["mode_transport"]==row["mode_transport"]) & (df_emissions_co2["taille"].isna()) & (df_emissions_co2["taille"].isna())]
        if df_ligne.empty:
            df_ligne = df_emissions_co2[df_emissions_co2["mode_transport"]==row["mode_transport"]]
            df_ligne = df_ligne.sort_values(by="part_transport", ascending=True).iloc[0]
        df_affichage.at[idx,"part_transport"] = df_ligne["part_transport"]
        df_affichage.at[idx,"part_fabrication"] = df_ligne["part_fabrication"]

        #ajout du type_transport
        df_affichage.at[idx,"type_transport"] = df_ligne["type_transport"]
        #ajout covoiturage (uniquement pour les mode_transport contenant voiture)
        if 'voiture' in row['mode_transport'].lower():
            df_affichage.at[idx,"covoiturage"] = True

    #vérification format avant calculs
    df_affichage['part_transport'] = pd.to_numeric(df_affichage['part_transport'], errors='coerce')
    df_affichage['part_fabrication'] = pd.to_numeric(df_affichage['part_fabrication'], errors='coerce')
    return df_affichage







# -------------------------
        # Affichage résultat sous forme d'une jauge thermomètre
        # -------------------------
        st.markdown('<div class="section-title">🌡️ Intensité CO₂ du pire trajet</div>', unsafe_allow_html=True)

        # max du dataset
        max_co2 = (df["part_transport"] + df["part_fabrication"]).max()

        # valeur critique (pire cas)
        co2 = max_co2
        #co2 = df.loc[selected_transport, "part_transport"] + df.loc[selected_transport, "part_fabrication"]

        # % de remplissage
        pct = (co2 / max_co2) * 100

        # couleur dynamique, A voir comment on définit les valeurs 40, 75, ...
        if pct < 40:
            color = "#22c55e"  # vert
        elif pct < 75:
            color = "#f59e0b"  # orange
        else:
            color = "#ef4444"  # rouge

        st.markdown(f"""
            <div class="thermo-container">
                <div class="thermo-title">🔥 Niveau d'émission CO₂</div>
                <div class="thermo-wrapper">
                    <div class="thermo-fill"
                        style="
                            height:{pct}%;
                            background:{color};
                        ">
                    </div>
                </div>
                <div class="thermo-value">{co2:.1f} kg CO₂</div>
                <div class="thermo-label">
                    Référence : trajet le plus émetteur
                </div>
            </div>
            """, unsafe_allow_html=True)



        # -------------------------
        # Affichage CARTE géographique du trajet voiture
        # -------------------------
        st.markdown('<div class="section-title">🗺️ Trajet par routes', unsafe_allow_html=True)

        coords_latlon = [(c[1], c[0]) for c in coords]

        m = folium.Map(location=coords_latlon[0], zoom_start=6)
        folium.PolyLine(coords_latlon, color="blue", weight=5).add_to(m)
        folium.Marker(coords_latlon[0], tooltip="Départ").add_to(m)
        folium.Marker(coords_latlon[-1], tooltip="Arrivée").add_to(m)

        st_folium(m, width=700, height=400)





dans dbt , stg_communes_av.sql
/*on va garder au moins dans un premier temsp uniquement les villes de plus de 10000 habitants => environ 1000 communes
on garde aussi les colonnes nom_standard , population , latitude_centre, longitude_centre ,
et les communes situées en France métropolitaine lat/long*/

with source as (
    select nom_standard, 
        trim(lower(unaccent(replace(lower(replace(nom_standard, '-', ' ')), 'ç', 'c')))) AS commune_clean,
        population, dep_code, latitude_centre, longitude_centre 
    from {{ source('emissions_co2', 'raw_communes') }}
    where population >= {{ var('population_commune') }} 
            and latitude_centre between 41.3 and 51.1
            and longitude_centre between -5.1 and 9.6

),
renamed as (
    select
        ROW_NUMBER() OVER (ORDER BY nom_standard) AS id_commune,
        nom_standard as commune, 
        commune_clean,
        population, 
        dep_code,
        latitude_centre as latitude,
        longitude_centre as longitude
    from source
)
select * from renamed




#représentation en barres
       for t in df_affichage_barres.index:
            chemin = df_affichage_barres.loc[t, "chemin"]    #va servir à afficher les étapes trajet train
            chemin_villes = []
            for c in chemin:
                chemin_villes.append(df_villes.loc[df_villes['id_commune'] == c, 'name'].iloc[0])

            transport = df_affichage_barres.loc[t, "part_transport"]
            fabrication = df_affichage_barres.loc[t, "part_fabrication"]
            total = round(transport + fabrication,1)

            width_total = (total / maxv) * 100
            width_transport = (transport / total) * width_total if total != 0 else 0
            width_fabrication = (fabrication / total) * width_total if total != 0 else 0

            # largeur minimale visibilité
            width_fabrication = max(width_fabrication, 3)

            #affichage barres
            col_titre, col_motorisation, col_taille, col_nb_passagers = st.columns([2,1,1,1])
            with col_titre:
                st.markdown(f"""
                    <div class="bar-label">{t} {chemin_villes if len(chemin_villes) > 0 else ""}</div>
                        """, unsafe_allow_html=True)
            with col_motorisation:
                motorisation = st.selectbox("Motorisation",("Essence", "Diesel"), index=None, placeholder="Motorisation", label_visibility="collapsed", key=f"motorisation_{t}")
            with col_taille:
                taille = st.selectbox("Taille", ("Petite", "Moyenne", "Berline", "SUV"),index = None, placeholder="Catégorie", label_visibility="collapsed", key=f"taille_{t}")      
            with col_nb_passagers:
                nb_passagers = st.selectbox("Nb passagers", ("1","2","3","4","5"), index=0, label_visibility="collapsed",key=f"nb_passagers_{t}")   



def distance_avion(df_routes_air, id_commune_dep, id_commune_arr):  
    #calcul distance pour les avions (on a les AR)
    result = {}
    distance_avion = 0
    duree_avion = 0
    emission_transport_avion = 0
    emission_fabrication_avion = 0
    df = df_routes_air[((df_routes_air['id_commune_departure'] == id_commune_dep) & (df_routes_air['id_commune_arrival'] == id_commune_arr))]
    #st.write(df)
    if df.shape[0] > 0:
        df_dist_min = df.sort_values(by='distance_km').head(1)  #si plusieurs routes on prend la plus courte
        distance_avion = int(df_dist_min['distance_km'].iloc[0])
        duree_avion = int(df_dist_min['duree_min'].iloc[0])
        duree_avion = f"{duree_avion//60:.0f}h{duree_avion%60:.0f}min"
        emission_transport_avion = round(float(df_dist_min['emission_transport'].iloc[0])  * distance_avion / 1000,1)
        emission_fabrication_avion = round(float(df_dist_min['emission_fabrication'].iloc[0])  * distance_avion / 1000,1)
        #emission_totale_avion = round(emission_transport_avion + emission_fabrication_avion,0)
        depart_avion = df_dist_min['name_departure']
        arrival_avion = df_dist_min['name_arrival']
    result = {'Distance':distance_avion, 'Durée':duree_avion, 'part_transport':emission_transport_avion,'part_fabrication':emission_fabrication_avion,'chemin':''}
    return result


def distance_train(df_routes_train, id_commune_dep, id_commune_arr):
    result = {}
    chemin_train, distance_train, duree_train = find_itineraire(df_routes_train, id_commune_dep, id_commune_arr, max_steps=10)        
    duree_train = f"{duree_train//60:.0f}h{duree_train%60:.0f}min"
    result = {'Distance':distance_train, 'Durée':duree_train, 'part_transport':2.3,'part_fabrication':0.63,'chemin':chemin_train}
    return result


def distance_voiture(df_villes, df_emissions_co2, id_commune_dep, id_commune_arr):
    mon_dict={}
    # calcul distance /temps pour les voitures et autres transports routiers
    #récupération des coordonnées géographiquesdes villes de départ et d'arrivée pour calcul itinéraire voiture
    row_dep = df_villes[df_villes["id_commune"] == id_commune_dep].iloc[0]
    coord_dep = (row_dep["longitude_centre"], row_dep["latitude_centre"])
    #
    row_arr = df_villes[df_villes["id_commune"] == id_commune_arr].iloc[0]
    coord_arr = (row_arr["longitude_centre"], row_arr["latitude_centre"])
    #calcul itinéraires voiture
    distance_route, coords, duree = get_route(coord_dep, coord_arr)
        
    #Récupération des émissions de CO2 pour les voitures en fonction de leurs caractéristiques
    # dans un premier temps on va juste utiliser les émissions moyennes par km pour les voitures,
    # de type compacte / moyenne Puissance / Esssence ou électrique, 
    # et on pourra dans un second temps affiner en fonction des caractéristiques de la
    # voiture saisie par l'utilisateur (type, puissance, carburant) 
    emission_transport_thermique = round(float(df_emissions_co2.loc[df_emissions_co2['mode_transport']=='Voiture thermique','part_transport'].iloc[0]) * distance_route / 1000,1)
    emission_fabrication_thermique = round(float(df_emissions_co2.loc[df_emissions_co2['mode_transport']=='Voiture thermique','part_fabrication'].iloc[0]) * distance_route / 1000,1)
    emission_transport_electrique = round(float(df_emissions_co2.loc[df_emissions_co2['mode_transport']=='Voiture électrique','part_transport'].iloc[0]) * distance_route / 1000,1)
    emission_fabrication_electrique = round(float(df_emissions_co2.loc[df_emissions_co2['mode_transport']=='Voiture électrique','part_fabrication'].iloc[0]) * distance_route / 1000,1)
    duree = f"{int(duree)//60:.0f}h{int(duree)%60:.0f}min"

    mon_dict["Voiture thermique 🚗"] = {'Distance':distance_route, 'Durée':duree, 'part_transport': emission_transport_thermique,'part_fabrication': emission_fabrication_thermique,'chemin':''}
    mon_dict["Voiture électrique 🚘⚡️"] = {'Distance':distance_route, 'Durée':duree, 'part_transport':emission_transport_electrique,'part_fabrication':emission_fabrication_electrique,'chemin':''}                      
    return distance_route, coords, mon_dict



.custom-bar {
    display: flex;
    width: 100%;
    height: 30px;   /* hauteur identique */
    border-radius: 12px;
    overflow: hidden;
    background-color: #e5e7eb;
    margin-bottom: 6px;
}

.wrapper-bar {
    display: flex;
    height: 28px;
    border-radius: 14px;
    overflow: hidden;
    background-color: #e0e0e0;
}

.bar-wrapper {
    display: flex;
    height: 100%;
}


/* ===== Partie verte ===== */
.transport-part {
    background-color: #4CAF50;
    color: white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    min-width: 40px; /* évite disparition */
}

/* ===== Partie orange ===== */
.fabrication-part {
    background-color: #F59E0B;
    color: white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    min-width: 40px; /* garde hauteur visible */
}