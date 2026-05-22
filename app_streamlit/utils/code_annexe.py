        
#A SUPPRIMER AVANT LIVRAISON!!!!!!!!!!!!!!!!!!        


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