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
        dist = data["features"][0]["properties"]["segments"][0]["distance"] / 1000
        coords = data["features"][0]["geometry"]["coordinates"]
        return dist, coords
    else:
        st.error("Erreur API")
        st.write(r.text)
        return None, None


# -----------------------
# fonction graphe pour calcul itinéraire train
# -----------------------

def build_graph(df):
    graph = defaultdict(list)
    for row in df.itertuples(index=False):
        graph[row.id_commune_departure].append((row.id_commune_arrival, row.distance_km, row.duree_min))
    return graph
# va retourner qq chose de l'ordre de :
#{
#    "Paris": [("Orleans", 115)],
#    "Orleans": [("Limoges", 200)]
#}


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
    "Voiture 🚗": 0.2,
    "Avion ✈️": 0.25,
    "Train 🚆": 0.01
}

# -------------------------
# UI - AFFCHAGE
# -------------------------
st.title("🌍 Calculateur CO2 intelligent")
st.markdown("Comparez vos trajets avec des données réelles 🚀")

#depart = st.selectbox("Ville de départ", list(villes.keys()))
#arrivee = st.selectbox("Ville d’arrivée", list(villes.keys()))
depart = st.selectbox("Ville de départ", df_villes.iloc[:,0])
arrivee = st.selectbox("Ville d'arrivée", df_villes.iloc[:,0])

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

        #calcul distance pour les voitures
        row_dep = df_villes[df_villes["name"] == depart].iloc[0]
        coord_dep = (row_dep["longitude_centre"], row_dep["latitude_centre"])
        row_arr = df_villes[df_villes["name"] == arrivee].iloc[0]
        coord_arr = (row_arr["longitude_centre"], row_arr["latitude_centre"])
        distance, coords = get_route(coord_dep, coord_arr)
        # voiture

        # Récupération identifiants communes pour calculs avion et train
        id_commune_dep = row_dep["id_commune"]
        id_commune_arr = row_arr["id_commune"]
        # ----------
 
        #calcul distance pour les avions (on a les AR)
        distance_avion = 0
        duree_avion = 0
        emission_transport_avion = 0
        emission_fabrication_avion = 0
        df = df_routes_air[((df_routes_air['id_commune_departure'] == id_commune_dep) & (df_routes_air['id_commune_arrival'] == id_commune_arr))]
        st.write(df)
        if df.shape[0] > 0:
            df_dist_min = df.sort_values(by='distance_km').head(1)  #si plusieurs routes on prend la plus courte
            distance_avion = int(df_dist_min['distance_km'])
            duree_avion = int(df_dist_min['duree_min'])
            emission_transport_avion = round(float(df_dist_min['emission_transport']) * distance_avion,0)
            emission_fabrication_avion = round(float(df_dist_min['emission_fabrication'])  * distance_avion,0)
            emission_totale_avion = round(emission_transport_avion + emission_fabrication_avion,0)
            depart_avion = df_dist_min['name_departure']
            arrival_avion = df_dist_min['name_arrival']
        #st.write(f"distance avion {distance_avion}")
        # avions

        #calcul pour les trains
        distance_train, duree_train, chemin_train = find_itineraire(df_routes_train, id_commune_dep, id_commune_arr, max_steps=10)
        st.write(f"distance en train {distance_train}")


    if distance:

        st.success(f"📏 Distance réelle : {distance:.1f} km")

        resultats = {
            transport: distance * facteur
            for transport, facteur in FACTEURS.items()
        }

        df = pd.DataFrame.from_dict(resultats, orient="index", columns=["CO2"])

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
                    <h2>{df.loc[t][0]:.1f} kg CO₂</h2>
                </div>
                """, unsafe_allow_html=True)

        # -------------------------
        # BARRES
        # -------------------------
        st.subheader("📊 Impact visuel")

        maxv = df["CO2"].max()

        for t in df.index:
            val = df.loc[t][0]
            width = (val / maxv) * 100

            st.markdown(f"""
            <div class="block">
                {t}<br>
                <div class="bar-bg">
                    <div class="bar-fill" style="width:{width}%;">
                        {val:.1f} kg
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # -------------------------
        # ÉQUIVALENT
        # -------------------------
        st.subheader("🌍 Équivalent")

        co2 = df["CO2"].min()

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