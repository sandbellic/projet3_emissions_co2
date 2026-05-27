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

@st.cache_data
def load_all_data():
    conn = get_conn()
    df_villes = load_villes(conn)
    df_routes_air = load_routes_air(conn)
    df_routes_train = load_routes_train(conn)
    df_emissions_co2 = load_emissions_co2(conn)
    df_affichage = load_affichage(df_emissions_co2)

    return df_villes, df_routes_air, df_routes_train, df_emissions_co2, df_affichage 

# ---------------------
# PREPARATION DONNEES AFFICHAGE
# ---------------------
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
    df_affichage['distance'] = 0
    df_affichage['duree'] = None

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
