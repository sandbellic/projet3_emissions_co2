import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
import psycopg2
from collections import deque, defaultdict
import time
import heapq
from rich import print

# 🔍 DEBUG (temporaire)
st.write("API KEY:", st.secrets.get("MISTRAL_API_KEY"))
#--------------------------
# STYLE (NOUVEAU : Ajoutez ceci ici)
# -------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap');
html, body, [class*="css"] {
    font-family: 'Roboto', sans-serif;
}
</style>
""", unsafe_allow_html=True)
# -------------------------
# CONFIG
# -------------------------
st.set_page_config(page_title="CO2 Dashboard")
st.title("Dashboard CO2 🚀")
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
if "map_id" not in st.session_state:
    st.session_state.map_id = 0

# ----------------------
# CONNEXION POSTGRE SQL
# ----------------------
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        database="postgres",
        user="postgres",
        password="warkyr",
        host="localhost",
        port="5432"
    )

conn = init_connection()

@st.cache_data
def load_data(query):
    return pd.read_sql(query, conn)

# Chargement des données
df_villes = load_data("SELECT * FROM emissions_co2.dim_communes")
df_routes = load_data("SELECT * FROM emissions_co2.dim_routes")
df_routes_air = df_routes[df_routes['type_transport'] == 'Avion trajet court']
df_routes_train = df_routes[df_routes['type_transport'] != 'Avion trajet court']
df_cars = load_data("SELECT * FROM emissions_co2.dim_cars")

# -------------------------
# API ITINÉRAIRE VOITURE
# -------------------------
if "route_cache" not in st.session_state:
    st.session_state.route_cache = {}

def get_route(coord1, coord2):
    cache_key = f"{coord1[0]},{coord1[1]}_{coord2[0]},{coord2[1]}"
    if cache_key in st.session_state.route_cache:
        return st.session_state.route_cache[cache_key]

    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    params = {
        "api_key": API_KEY,
        "start": f"{coord1[0]},{coord1[1]}",
        "end": f"{coord2[0]},{coord2[1]}"
    }
    headers = {"Accept": "application/geo+json"}

    st.write("Appel API avec URL :", url)  # Débogage
    st.write("Paramètres :", params)       # Débogage

    r = requests.get(url, params=params, headers=headers)
    st.write("Statut HTTP :", r.status_code)  # Débogage
    st.write("Réponse brute :", r.text)      # Débogage (à utiliser temporairement)

    if r.status_code == 200:
        data = r.json()
        dist = data["features"][0]["properties"]["segments"][0]["distance"] / 1000
        coords = data["features"][0]["geometry"]["coordinates"]
        st.session_state.route_cache[cache_key] = (dist, coords)
        return dist, coords
    else:
        st.error(f"Erreur API: {r.status_code} - {r.text}")
        return None, None

# -----------------------
# GRAPHE POUR CALCUL ITINÉRAIRE TRAIN
# -----------------------
def build_graph(df):
    graph = defaultdict(list)
    for row in df.itertuples(index=False):
        graph[row.id_commune_departure].append((row.id_commune_arrival, row.distance_km, row.duree_min))
    return graph

import heapq

def find_itineraire(df, start, end, max_iterations=1000):
    graph = defaultdict(list)

    for row in df.itertuples(index=False):
        graph[row.id_commune_departure].append(
            (row.id_commune_arrival, row.distance_km, row.duree_min)
        )

    heap = [(0, 0, start, [start])]  # distance, durée, ville, chemin
    visited = set()
    iterations = 0

    while heap and iterations < max_iterations:
        dist, duree, city, path = heapq.heappop(heap)
        iterations += 1

        if city in visited:
            continue
        visited.add(city)

        if city == end:
            return path, dist, duree

        for neighbor, d, t in graph.get(city, []):
            if neighbor not in visited:
                heapq.heappush(heap, (dist + d, duree + t, neighbor, path + [neighbor]))

    return None, None, None

# -------------------------
# FACTEURS D'ÉMISSION CO2
# -------------------------
FACTEURS = {
    "Voiture 🚗": 0.2,
    "Avion ✈️": 0.25,
    "Train 🚆": 0.01
}

# -------------------------
# UI - AFFICHAGE
# -------------------------
st.title("🌍 Calculateur CO2 intelligent")
st.markdown("Comparez vos trajets avec des données réelles 🚀")

depart = st.selectbox("Ville de départ", df_villes["name"])
arrivee = st.selectbox("Ville d'arrivée", df_villes["name"])

# --- NOUVEAU : Sélecteur de transport préféré ---
transport_prefere = st.selectbox(
    "Votre transport préféré 🚀",
    ["Voiture 🚗", "Avion ✈️", "Train 🚆", "Bus 🚌"]
)
st.session_state.transport_prefere = transport_prefere

col1, col2 = st.columns(2)
with col1:
    if st.button("Calculer le trajet"):
        st.session_state.calcul = True
with col2:
    if st.button("Reset"):
        st.session_state.calcul = False
        
# --- Sidebar (toujours visible) ---
with st.sidebar:
    st.markdown("### Menu")
    if st.sidebar.button("🏠 Accueil"):
        st.switch_page("streamlit_app.py")
    if st.sidebar.button("📊 KPI"):
        st.switch_page("pages/1_KPI.py")
# -------------------------
# LOGIQUE
# -------------------------
if depart != arrivee and st.session_state.calcul:
    with st.spinner("Calcul en cours..."):
        # --- Coordonnées des villes ---
        row_dep = df_villes[df_villes["name"] == depart].iloc[0]
        coord_dep = (row_dep["longitude_centre"], row_dep["latitude_centre"])
        row_arr = df_villes[df_villes["name"] == arrivee].iloc[0]
        coord_arr = (row_arr["longitude_centre"], row_arr["latitude_centre"])

        # --- Débogage : Affichage des coordonnées ---
        st.write("**Coordonnées départ :**", coord_dep)
        st.write("**Coordonnées arrivée :**", coord_arr)

        # Calcul de la distance pour la voiture
        distance, coords = get_route(coord_dep, coord_arr)
        st.write("**Distance calculée :**", distance)  # Débogage

        # Identifiants des communes pour avion et train
        id_commune_dep = row_dep["id_commune"]
        id_commune_arr = row_arr["id_commune"]

        # Calcul pour l'avion
        df_avion = df_routes_air[
            ((df_routes_air['id_commune_departure'] == id_commune_dep) &
             (df_routes_air['id_commune_arrival'] == id_commune_arr))
        ]
        distance_avion = 0
        duree_avion = 0
        emission_transport_avion = 0
        emission_fabrication_avion = 0

        if not df_avion.empty:
            df_dist_min = df_avion.sort_values(by='distance_km').head(1)
            distance_avion = int(df_dist_min['distance_km'])
            duree_avion = int(df_dist_min['duree_min'])
            emission_transport_avion = round(float(df_dist_min['emission_transport']) * distance_avion, 0)
            emission_fabrication_avion = round(float(df_dist_min['emission_fabrication']) * distance_avion, 0)

        # Calcul pour le train
        distance_train, duree_train, chemin_train = find_itineraire(df_routes_train, id_commune_dep, id_commune_arr)

    # --- Affichage des résultats si distance est calculée ---
    if distance:  # Vérifie que distance n'est pas None ou 0
        st.success(f"📏 Distance réelle : {distance:.1f} km")

        # Calcul des émissions CO2
        resultats = {transport: distance * facteur for transport, facteur in FACTEURS.items()}
        df_emissions = pd.DataFrame.from_dict(resultats, orient="index", columns=["CO2"])
        df_emissions["Type"] = df_emissions.index

        # --- NOUVEAU : Coûts estimés ---
        couts = {
            "Voiture 🚗": distance * 0.15,
            "Avion ✈️": distance * 0.25,
            "Train 🚆": distance * 0.08,
        }
        df_emissions["Coût estimé (€)"] = df_emissions["Type"].map(couts)

        # --- NOUVEAU : Recommandations ---
        df_emissions["Recommandation"] = df_emissions["Type"].apply(
            lambda x: "✅ Meilleur choix" if x == st.session_state.transport_prefere and df_emissions.loc[df_emissions["Type"] == x, "CO2"].values[0] == df_emissions["CO2"].min()
            else ("⚠️ Moins écologique" if x == st.session_state.transport_prefere and df_emissions.loc[df_emissions["Type"] == x, "CO2"].values[0] != df_emissions["CO2"].min()
                  else "")
        )
     # Exemple de données

# ✅ STYLE LIGNE ENTIÈRE
        def highlight_row(row):
            if row["Recommandation"] != "":
                return ["font-weight: bold; font-size: 16px"] * len(row)
            return [""] * len(row)

# 👉 Affichage stylé
        df_emissions.style.apply(highlight_row, axis=1)   

        # --- NOUVEAU : Affichage du tableau ---
        st.dataframe(df_emissions[["Type", "CO2", "Coût estimé (€)", "Recommandation"]], hide_index=True)

        # --- NOUVEAU : Graphique comparatif ---
        #import plotly.express as px
        #fig = px.bar(df_emissions, x="Type", y="CO2", title="Comparaison des émissions CO₂", color="Type")
        #st.plotly_chart(fig, use_container_width=True)

        # -------------------------
        # CARTES DES ÉMISSIONS
        # -------------------------
        cols = st.columns(len(df_emissions))
        for i, transport in enumerate(df_emissions.index):
            with cols[i]:
                st.markdown(f"""
                <div class="block">
                    <strong>{transport}</strong><br><br>
                    <h2>{df_emissions.loc[transport, 'CO2']:.1f} kg CO₂</h2>
                </div>
                """, unsafe_allow_html=True)

        # -------------------------
        # BARRES D'IMPACT VISUEL
        # -------------------------
        st.subheader("📊 Impact visuel")
        maxv = df_emissions["CO2"].max()
        for transport in df_emissions.index:
            val = df_emissions.loc[transport, "CO2"]
            width = (val / maxv) * 100
            st.markdown(f"""
            <div class="block">
                {transport}<br>
                <div class="bar-bg">
                    <div class="bar-fill" style="width:{width}%;">
                        {val:.1f} kg
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # -------------------------
        # ÉQUIVALENT ENVIRONNEMENTAL
        # -------------------------
        st.subheader("🌍 Équivalent")
        co2 = df_emissions["CO2"].min()
        arbres = co2 / 25
        km_voiture = co2 * 5
        st.info(f"🌳 {arbres:.1f} arbres nécessaires pour compenser\n🚗 Équivaut à {km_voiture:.0f} km en voiture")

        # -------------------------
        # CARTE DU TRAJET
        # -------------------------
        st.subheader("🗺️ Trajet")
        coords_latlon = [(c[1], c[0]) for c in coords]
        m = folium.Map(location=coords_latlon[0], zoom_start=6)
        folium.PolyLine(coords_latlon, color="blue", weight=5).add_to(m)
        folium.Marker(coords_latlon[0], tooltip="Départ").add_to(m)
        folium.Marker(coords_latlon[-1], tooltip="Arrivée").add_to(m)

        # Clé unique pour la carte
        map_key = f"map_{depart}_{arrivee}"
        st_folium(m, width=700, height=400, key=map_key)

    else:
        st.error("Impossible de calculer le trajet. Vérifiez les coordonnées et la connexion à l'API.")
  #----------------------------------      
    # --- CHATBOT AVEC MISTRAL ---

st.title("🤖 Chatbot CO₂")

# Mémoire du chat
if "messages" not in st.session_state:
    st.session_state.messages = []

# Affichage des messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Input utilisateur
prompt = st.chat_input("Pose ta question...")

if prompt:
    # Affiche message utilisateur
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 🔐 API key (à mettre dans secrets.toml)
    API_KEY = st.secrets["MISTRAL_API_KEY"]

    response = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "mistral-small",
            "messages": st.session_state.messages
        }
    )

    if response.status_code == 200:
        answer = response.json()["choices"][0]["message"]["content"]
    else:
        answer = f"Erreur API : {response.status_code}"

    # Affiche réponse bot
    st.session_state.messages.append({"role": "assistant", "content": answer})
    with st.chat_message("assistant"):
        st.markdown(answer)

