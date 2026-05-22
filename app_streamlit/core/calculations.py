from collections import deque, defaultdict
import requests
import streamlit as st
import pandas as pd



def distance_air(df_routes_air, id_commune_dep, id_commune_arr, coef_ar):  
    #calcul distance et durée pour les avions 
    result = {}
    distance_avion = 0
    duree_avion = 0
    df = df_routes_air[((df_routes_air['id_commune_departure'] == id_commune_dep) & (df_routes_air['id_commune_arrival'] == id_commune_arr))]
    if df.shape[0] > 0:
        df_dist_min = df.sort_values(by='distance_km').head(1)  #si plusieurs routes on prend la plus courte
        distance_avion = int(df_dist_min['distance_km'].iloc[0]) * coef_ar
        duree_avion = int(df_dist_min['duree_min'].iloc[0])
        duree_avion = duree_avion * coef_ar
        duree_avion_hmm = f"{duree_avion//60:.0f}h{duree_avion%60:.0f}min"
        chemin = f"({df_dist_min['name_departure']} ➞ {df_dist_min['name_arrival']} )"
    result = {'Distance':distance_avion, 'Duree':duree_avion_hmm, 'chemin': chemin, 'coords':''}
    return result


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

def distance_rail(df_routes_train, id_commune_dep, id_commune_arr, coef_ar):
    result = {}
    chemin_train, distance_train, duree_train = find_itineraire(df_routes_train, id_commune_dep, id_commune_arr, max_steps=10)        
    distance_train = distance_train * coef_ar
    duree_train = duree_train * coef_ar
    duree_train_hmm = f"{duree_train//60:.0f}h{duree_train%60:.0f}min"
    result = {'Distance':distance_train, 'Duree':duree_train_hmm, 'chemin':chemin_train, 'coords':''}
    return result


# -----------------------
# fonction pour calcul itinéraire route avec API
# -----------------------

def get_route(coord1, coord2):

    API_KEY = st.secrets["API_KEY"]
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


def distance_route(df_villes, id_commune_dep, id_commune_arr, coef_ar):
    # calcul distance /temps pour les voitures et autres transports routiers
    #récupération des coordonnées géographiquesdes villes de départ et d'arrivée pour calcul itinéraire voiture
    row_dep = df_villes[df_villes["id_commune"] == id_commune_dep].iloc[0]
    coord_dep = (row_dep["longitude_centre"], row_dep["latitude_centre"])
    #
    row_arr = df_villes[df_villes["id_commune"] == id_commune_arr].iloc[0]
    coord_arr = (row_arr["longitude_centre"], row_arr["latitude_centre"])
    #calcul itinéraires voiture
    distance_route, coords, duree = get_route(coord_dep, coord_arr)
    distance_route = distance_route * coef_ar
    duree = duree * coef_ar
    duree_hmm = f"{int(duree)//60:.0f}h{int(duree)%60:.0f}min"
    result = {'Distance':distance_route, 'Duree':duree_hmm, 'chemin':'', 'coords': coords}
    return result



# -----------------
# CALCUL DES DISTANCES ET DUREES POUR CHAQUE TYPE TRANSPORT
# -----------------

def distance_duree_global(df_villes, df_routes_air, df_routes_train, df_affichage):
            
    match st.session_state['preference']:
        
        case "pour un trajet":      
            # ----------------           
            # Récupération des identifiants des communes de départ et d'arrivée
            # -----------------
            id_commune_dep = int(df_villes.loc[df_villes["label"] == st.session_state['depart_selectbox'],"id_commune"].iloc[0])
            id_commune_arr = int(df_villes.loc[df_villes["label"] == st.session_state['arrivee_selectbox'],"id_commune"].iloc[0])
            # définition du coef Aller/retour
            coef_ar = 2 if st.session_state['ar'] else 1

            variables = {}
                    
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
            distance_route_value = variables["route"]['Distance']
            coords = variables["route"]['coords']  #données utilisées plus bas pour affichage carte


            if distance_route_value:
                #convertit variables en format dataframe, renommage index en type_transport, et conversion
                #distance pour être certain pas pb dans les calculs ensuite
                df_variables = pd.DataFrame.from_dict(variables, orient="index")
                df_variables = df_variables.reset_index().rename(columns={"index": "type_transport"})
                df_variables['Distance'] = pd.to_numeric(df_variables['Distance'], errors='coerce')
                df_affichage['duree'] = df_affichage['duree'].astype(str)    

                # utilisation des données récupérées de variables (distance, durée, ...)
                # pour préparer df_affichage en vue de son display  
                for idx, row in df_affichage.iterrows():
                    df_ligne = df_variables[df_variables['type_transport'] == row['type_transport']]
                    if not df_ligne.empty:
                        distance = df_ligne.iloc[0]['Distance'] 
                        df_affichage.at[idx, 'distance'] = distance 
                        df_affichage.at[idx, 'duree'] = df_ligne.iloc[0]['Duree']                                  
                        df_affichage.at[idx, 'total_transport'] = distance * row['part_transport'] /1000
                        df_affichage.at[idx, 'total_fabrication'] = distance * row['part_fabrication'] /1000
                        df_affichage.at[idx, 'co2_global'] = distance * (row['part_transport'] + row['part_fabrication'])/1000
                        df_affichage.at[idx, 'chemin'] = df_ligne.iloc[0]['chemin']
                #st.dataframe(df_affichage)
            
            return coords, distance_route_value
        

        case "sur une distance":
            # ---------------
            # MAJ df_affichage valeurs co2 totales en fonction km saisis   
            df_affichage['total_transport'] = st.session_state['distance_input'] * df_affichage['part_transport'] /1000
            df_affichage['total_fabrication'] = st.session_state['distance_input'] * df_affichage['part_fabrication'] /1000
            df_affichage['co2_global'] = st.session_state['distance_input'] * (df_affichage['part_transport']+ df_affichage['part_fabrication'])/1000
            return None, st.session_state['distance_input']
