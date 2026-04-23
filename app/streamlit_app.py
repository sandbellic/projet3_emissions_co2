import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium

import psycopg2 # pour chargement base
from collections import deque, defaultdict

# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="Calculateur CO2", layout="centered")

API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6ImI2MzVhNGU0NzAxOTRlNWFhNmJmYzAzODIwYTU0ODMwIiwiaCI6Im11cm11cjY0In0="

# -------------------------
# STYLE
# -------------------------
st.markdown("""
<style>
.block {
    background-color: white;
    padding: 20px;
    border-radius: 15px;
    margin-bottom: 15px;
    box-shadow: 0px 4px 10px rgba(0,0,0,0.05);
}
.bar-bg {
    background: #eee;
    border-radius: 10px;
}
.bar-fill {
    background: #4CAF50;
    padding: 5px;
    border-radius: 10px;
    color: white;
    text-align: right;
}
</style>
""", unsafe_allow_html=True)

# -------------------------
# SESSION
# -------------------------
if "calcul" not in st.session_state:
    st.session_state.calcul = False


# ----------------------
# CONNEXION POSTGRE SQL
# -------------

conn = psycopg2.connect(
    database="emissions_co2",
    user="postgres",
    password="SandPOST6642",
    host="localhost",
    port="5432"
)

#chargement des données stockés dans postgre
@st.cache_data
def load_data(query):
    return pd.read_sql(query, conn)

#chargement des communes
query = "SELECT * FROM emissions_co2.dim_communes"
df_villes = load_data(query)

#chargement des routes air et rails
query = "select * from emissions_co2.dim_routes"
df_routes = load_data(query)
df_routes_air = df_routes[df_routes['type_transport']=='Avion trajet court']
df_routes_train = df_routes[df_routes['type_transport']!='Avion trajet court']
#chargement des émissions voitures
query = "select * from emissions_co2.dim_cars"
df_cars = load_data(query)

# -------------------------
# VILLES. - A SUPPRIMER --- VALABLES DANS UN PREMIER TEMPS POUR ESSAIS
# -------------------------

villes = {
    "Paris": (2.3522, 48.8566),
    "Marseille": (5.3698, 43.2965),
    "Lyon": (4.8357, 45.7640),
    "Toulouse": (1.4442, 43.6047),
    "Nice": (7.2620, 43.7102),
    "Nantes": (-1.5536, 47.2184)
}

# -------------------------
# API itinéraire voiture
# -------------------------
def get_route(coord1, coord2):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"

    params = {
        "api_key": API_KEY,
        "start": f"{coord1[0]},{coord1[1]}",
        "end": f"{coord2[0]},{coord2[1]}"

    }

    headers = {"Accept": "application/geo+json"}

    r = requests.get(url, params=params, headers=headers)

    if r.status_code == 200:
        data = r.json()
        dist = round(data["features"][0]["properties"]["segments"][0]["distance"] / 1000,0)  # en km
        duree = data["features"][0]["properties"]["segments"][0]["duration"] / 60 # en secondes
        coords = data["features"][0]["geometry"]["coordinates"]
        return dist, coords, duree
    else:
        st.error("Erreur API")
        st.write(r.text)
        return None, None, None


# -----------------------
# fonction graphe pour calcul itinéraire train
# -----------------------

def build_graph(df):    # algo BFS ou parcours en largeur
    graph = defaultdict(list)
    for row in df.itertuples(index=False):
        graph[row.id_commune_departure].append((row.id_commune_arrival, row.distance_km, row.duree_min))
    return graph

def find_itineraire(df, start, end, max_steps=10):
    graph = build_graph(df)
    queue = deque([(start, 0, 0, [start], 0)])
    #queue : file d’attente (BFS), avec des tuples : (city, distance_totale, duree_totale, chemin, nombre_d_etapes)
    
    # on mémorise (ville, étape) pour éviter explosion combinatoire
    visited = set()

    while queue:
        city, dist, duree, path, steps = queue.popleft()

        if (city, steps) in visited:
            continue     # on evite de revisiter la même ville au même niveau
        visited.add((city, steps))

        if steps > max_steps:   #pour ne pas boucler indéfiniment on limite la longueur des chemins
            continue

        if city == end:             #on a trouvé le bon chemin
            return path, dist, duree

        for neighbor, d, min in graph.get(city, []):
            if neighbor not in path:  # évite cycles
                queue.append((
                    neighbor,
                    dist + d,
                    duree + min,
                    path + [neighbor],
                    steps + 1
                ))

    return None, None, None


# -------------------------
# FACTEURS - Constantes émission co2 - en kg / par personne/ par km
# Pour le train et l'avion, les valeurs sont déjà incluses dans dim_routes 
# A revoir pour la voiture. .....
# -------------------------
FACTEURS = {
#    "Voiture 🚗": 0.2,
#    "Avion ✈️": 0.25,
#    "Train 🚆": 0.01
}

# -------------------------
# UI - AFFCHAGE
# -------------------------
st.title("🌍 Calculateur CO2 intelligent")
st.markdown("Comparez vos trajets avec des données réelles 🚀")

# ajout des dpératements dans les labels des villes pour les différencier en cas de doublons de noms de villes
df_villes["label"] = df_villes["name"] + " (" + df_villes["dep_code"].astype(str) + ")"

villes = df_villes["label"].tolist()

#on va mettre quelques unes des villes les plus demandées en haut de la liste, 
# et les autres villes seront par ordre alphabétique en dessous
# Liste des villes prioritaires
prioritaires = ["Paris (75)", "Lyon (69)", "Marseille (13)", "Toulouse (31)", "Nice (06)","Nantes (44)", "Bordeaux (33)", "Lille (59)"]

# on enlève les villes prioritaires de la liste complète pour éviter de les avoior en double dans la liste finale,
# et on classe la liste des autres villes par ordre alphabétique
villes_prioritaires = [v for v in prioritaires if v in villes]
autres_villes = sorted([v for v in villes if v not in prioritaires])

# on combine les deux listes pour avoir les villes prioritaires en haut et les autres ensuite
villes_finales = villes_prioritaires + autres_villes

depart = st.selectbox("Ville de départ", villes_finales, index=None, placeholder="Choisir ou taper le nom de la ville",key="depart_selectbox")
arrivee = st.selectbox("Ville d'arrivée", villes_finales, index=None, placeholder="Choisir ou taper le nom de la ville",key="arrivee_selectbox")

#depart = st.selectbox("Ville de départ", df_villes.iloc[:,0])
#arrivee = st.selectbox("Ville d'arrivée", df_villes.iloc[:,0])

col1, col2 = st.columns(2)

with col1:
    if st.button("Calculer le trajet"):
        st.session_state.calcul = True

with col2:
    if st.button("Reset"):
        st.session_state.calcul = False

# -------------------------
# LOGIQUE
# -------------------------
if depart != arrivee and st.session_state.calcul:

    with st.spinner("Calcul en cours..."):

        #récupération des identifiants des communes de départ et d'arrivée pour calculs
        id_commune_dep = int(df_villes.loc[df_villes["label"] == depart,"id_commune"].iloc[0])
        id_commune_arr = int(df_villes.loc[df_villes["label"] == arrivee,"id_commune"].iloc[0])
        #id_commune_arr = int(df_villes.loc[df_villes["label"] == arrivee, "id_commune"].iloc[0])

        #----------------
        # calcul distance /temps pour les voitures et autres transports routiers
        #----------------
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
        emission_transport_essence = round(float(df_cars[(df_cars['categorie_masse']=='Compacte') & (df_cars['type_puissance']=="Moyenne Puissance") & (df_cars['type_energie']=='Essence')]['emission_transport'].values[0]) * distance_route / 1000,1)
        emission_fabrication_essence = round(float(df_cars[(df_cars['categorie_masse']=='Compacte') & (df_cars['type_puissance']=="Moyenne Puissance") & (df_cars['type_energie']=='Essence')]['emission_fabrication'].values[0]) * distance_route / 1000,1)
        emission_transport_electrique = round(float(df_cars[(df_cars['categorie_masse']=='Compacte') & (df_cars['type_puissance']=="Moyenne Puissance") & (df_cars['type_energie']=='Electrique')]['emission_transport'].values[0]) * distance_route / 1000,1)
        emission_fabrication_electrique = round(float(df_cars[(df_cars['categorie_masse']=='Compacte') & (df_cars['type_puissance']=="Moyenne Puissance") & (df_cars['type_energie']=='Electrique')]['emission_fabrication'].values[0]) * distance_route / 1000,1)
        duree = f"{int(duree)//60:.0f}h{int(duree)%60:.0f}min"

        FACTEURS["Voiture Compacte essence (Moyenne Puissance) 🚗"] = {'Distance':distance_route, 'Durée':duree, 'part_transport': emission_transport_essence,'part_fabrication': emission_fabrication_essence,'chemin':''}
       ###### FACTEURS["Voiture électrique 🚘⚡️"] = {'Distance':distance_route, 'Durée':duree, 'part_transport':emission_transport_electrique,'part_fabrication':emission_fabrication_electrique}                         
        # fin voiture

 
        #calcul distance pour les avions (on a les AR)
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
        FACTEURS["Avion - trajet court✈️"] = {'Distance':distance_avion, 'Durée':duree_avion, 'part_transport':emission_transport_avion,'part_fabrication':emission_fabrication_avion,'chemin':''}
        # avions

      #calcul pour les trains
        chemin_train, distance_train, duree_train = find_itineraire(df_routes_train, id_commune_dep, id_commune_arr, max_steps=10)        
        duree_train = f"{duree_train//60:.0f}h{duree_train%60:.0f}min"
        FACTEURS["Train 🚆"] = {'Distance':distance_train, 'Durée':duree_train, 'part_transport':2.3,'part_fabrication':0.63,'chemin':chemin_train}



    if distance_route:

        st.success(f"📏 Distance route réelle : {distance_route:.1f} km")

        #resultats = {
        #    transport: distance * facteur
        #    for transport, facteur in FACTEURS.items()
        #}

        df = pd.DataFrame.from_dict(FACTEURS, orient="index")
        df['Distance'] = pd.to_numeric(df['Distance'], errors='coerce')
     
        #les colonnes CO2 transport et fabrication sont soit des scalaires, soit des dictionnaires
        # les valeurs de cà2 sont données pour 1000km d'Où la division par 1000 pour les calculs
        for idx, row in df.iterrows():

            df['part_transport'] = pd.to_numeric(df['part_transport'], errors='coerce')
            df.at[idx, 'part_transport'] = row['Distance'] * row['part_transport'] /1000

            df['part_fabrication'] = pd.to_numeric(df['part_fabrication'], errors='coerce')
            df.at[idx, 'part_fabrication'] = row['Distance'] * row['part_fabrication'] /1000
        st.write(df)



        # -------------------------
        # CARTES
        # -------------------------
        st.subheader("📊 Comparaison")

        cols = st.columns(3)

        for i, t in enumerate(df.index):
            with cols[i]:
                st.markdown(f"""
                <div class="block">
                    <strong>{t}</strong><br><br>
                    <h2>{df.loc[t,'part_transport']+df.loc[t,'part_fabrication']:.1f} kg CO₂</h2>
                </div>
                """, unsafe_allow_html=True)

        # -------------------------
        # BARRES
        # -------------------------
        st.subheader("📊 Impact visuel")

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
            #chemin = df.loc[t, "chemin"]
            #chemin_villes = []
            #for c in chemin:
            #    chemin_villes.append(df_villes[df_villes])

            transport = df.loc[t, "part_transport"]
            fabrication = df.loc[t, "part_fabrication"]
            total = transport + fabrication

            width_total = (total / maxv) * 100
            width_transport = (transport / total) * 100 if total != 0 else 0
            width_fabrication = (fabrication / total) * 100 if total != 0 else 0

            st.markdown(f"""
            <div class="block">
                {t}<br>
                <div class="bar-bg" style="width:{width_total}%;">
                    <div style="display:flex; height:100%;">
                        <div style="
                            width:{width_transport}%;
                            background-color:#4CAF50;
                            text-align:center;
                            color:white;">
                            {transport:.1f}
                        </div>
                        <div style="
                            width:{width_fabrication}%;
                            background-color:#FF9800;
                            text-align:center;
                            color:white;">
                            {fabrication:.1f}
                        </div>
                    </div>
                </div>
                <small>Total : {total:.1f} kg</small>
            </div>
            """, unsafe_allow_html=True)



        # -------------------------
        # ÉQUIVALENT
        # -------------------------
        st.subheader("🌍 Équivalent pour transport émetteur CO2 min")

        co2 = (df["part_transport"] + df["part_fabrication"]).min()

        arbres = co2 / 25
        km = co2 * 5

        st.info(f"""
🌳 {arbres:.1f} arbres nécessaires pour compenser  
🚗 équivaut à {km:.0f} km en voiture  
""")

        # -------------------------
        # CARTE
        # -------------------------
        st.subheader("🗺️ Trajet")

        coords_latlon = [(c[1], c[0]) for c in coords]

        m = folium.Map(location=coords_latlon[0], zoom_start=6)
        folium.PolyLine(coords_latlon, color="blue", weight=5).add_to(m)
        folium.Marker(coords_latlon[0], tooltip="Départ").add_to(m)
        folium.Marker(coords_latlon[-1], tooltip="Arrivée").add_to(m)

        st_folium(m, width=700, height=400)

else:
    if depart == arrivee:
        st.warning("Choisis deux villes différentes")


# Bouton pour aller aux KPI
if st.button("📊 Voir les KPI"):
    st.switch_page("pages/2_kpi.py")

st.markdown('<div class="footer">Impact CO₂ - Calculateur de transport</div>', unsafe_allow_html=True)