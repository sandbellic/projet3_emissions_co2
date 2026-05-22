import pandas as pd
import requests
import zipfile
from io import StringIO, BytesIO

def load_url():
    #----------------
    #liste des url des données à charger
    #-----------------
    url_communes = "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"

    url_cars = "https://www.data.gouv.fr/api/1/datasets/r/bc42c2e3-d24c-4499-a966-d35656c6cfc1"
    url_new_cars = "https://carlabelling.ademe.fr/recherche/export?searchString=&co2=&brand=&model=&category=&range=&transmission=&price=0%2C500000&maxconso=&energy=0%2C7&RechercherL=Rechercher&limit=50&offset=50&limit=3731&offset=0"

    #url_trains = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/emission-co2-perimetre-complet/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    url_routes_train = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"

    url_airports_fr = "https://ourairports.com/countries/FR/airports.csv"
    url_routes_air = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

    url_departements = "https://static.data.gouv.fr/resources/departements-de-france/20200425-135513/departements-france.csv"


    #----------------------

    #a partir des url de chaque éléments on va récupérer les données (format csv) sous-jacentes
    #on va stocker les résultats dans un dictionnaire liste_df:
    #  la clé va représenter le nom de la donnée qu'on va stocker 'cars', 'airports'
    #  et la valeur associée sera le dataframe équivalent du csv
    #Attention: cas particulier des voitures et des routes_trains: on récupère un zip
    liste_df={}

    #load de la liste des aéroports
    liste_df['airports'] = load_airports(url_airports_fr)

    #load de la liste des communes
    liste_df['communes'] = load_communes(url_communes)

    #load routes.txt : liste de toutes les routes SNCF (selon les différents moyen TGV, TER, intercités, ...)
    #dans un fichiers ZIP : on récupère uniquement routes.txt
    liste_df["routes_train"], liste_df["stops"], liste_df["stop_times"], liste_df["trips"] = load_routes_train(url_routes_train)

     #load new_cars
    liste_df['new_cars'] = load_new_cars(url_new_cars)

    #routes aériennes
    liste_df["routes_air"] = load_routes_air(url_routes_air)

   #departements
    liste_df['departements'] = load_departements(url_departements)

    return liste_df


#load de la liste des aéroports
def load_airports(url_airports_fr):
    response = requests.get(url_airports_fr)
    if response.status_code == 200:
        liste_df = pd.read_csv(StringIO(response.text))
    else:
        print("Erreur téléchargement liste aéroport:", response.status_code)
        liste_df = pd.DataFrame()
    return liste_df

   
#load de la liste des communes
def load_communes(url_communes):
    response = requests.get(url_communes)
    if response.status_code == 200:
        liste_df = pd.read_csv(BytesIO(response.content),encoding='utf-8', low_memory=False)
    else:
        print("Erreur téléchargement liste communes:", response.status_code)
        liste_df = pd.DataFrame()
    return liste_df


#load routes.txt : liste de toutes les routes SNCF (selon les différents moyen TGV, TER, intercités, ...)
#dans un fichiers ZIP : on récupère uniquement routes.txt
def load_routes_train(url_routes_train):
    response = requests.get(url_routes_train)
    if response.status_code == 200:
         #ouvrir le zip, le fichier qui nous intéresse est dans le fichier de nom routes.txt
        z = zipfile.ZipFile(BytesIO(response.content))   #z représente l'archive zip
        # ouvrir le bon fichier
        with z.open("routes.txt") as f:
            liste_df1 = pd.read_csv(f, sep=",")  
        with z.open("stops.txt") as f:
            liste_df2 = pd.read_csv(f, sep=",")   
        with z.open("stop_times.txt") as f:
            liste_df3 = pd.read_csv(f, sep=",")    
        with z.open("trips.txt") as f:
            liste_df4 = pd.read_csv(f, sep=",")        
    else:
        print("erreur téléchargement routes SNCF txt")
        liste_df1 = pd.DataFrame()
        liste_df2 = pd.DataFrame()
        liste_df3 = pd.DataFrame()
        liste_df4 = pd.DataFrame()
    return liste_df1, liste_df2, liste_df3, liste_df4


#load new_cars
def load_new_cars(url_new_cars):
    response = requests.get(url_new_cars)
    if response.status_code == 200:
        df_brut = pd.read_excel(BytesIO(response.content))
        # le fichier excel contient un certain nb de lignes inintéressantes pour notre dataframe
        # => Recherche de la dernière ligne contenant "Carrosserie", ensuite on a la liste des véhicules
        ligne_header = df_brut[df_brut.apply(
            lambda row: row.astype(str).str.contains("Carrosserie", case=False, na=False).any(),
            axis=1)].index[-1]       
        # Lecture des données véhicules
        liste_df = df_brut.iloc[ligne_header+3:,0:11]
        liste_df.columns = ["carrosserie", "modele", "energie",
            "bv", "conso_min", "conso_max", "unite", "co2_min",
            "co2_classe_min", "co2_max", "co2_classe_max"]
    else:
        print("Erreur téléchargement new cars:", response.status_code)
        liste_df= pd.DataFrame()
    return liste_df


#routes aériennes
def load_routes_air(url_routes_air):
    response = requests.get(url_routes_air)
    if response.status_code == 200:
        liste_df = pd.read_csv(StringIO(response.text), header=None)
        liste_df.columns = ['Airline', 'Airline_ID', 'Source_airport', 'Source_airport_ID',
                'Destination_airport', 'Destination_airport_ID','Codeshare', 'Stops', 'Equipment']
    else:
        print("Erreur téléchargement routes aériennes:", response.status_code)
        liste_df= pd.DataFrame()
    return liste_df


#departements
def load_departements(url_departements):
    response = requests.get(url_departements)
    if response.status_code == 200:
        liste_df = pd.read_csv(BytesIO(response.content), encoding='utf-8')
    else:
        print("Erreur téléchargement departements :", response.status_code)
        liste_df= pd.DataFrame()
    return liste_df



#-------------------
# chargement des éléments API
#-------------------
def boucle_API(url):
    # API utilisée pour le site open data de la SNCF, pas plus de 100 items récupérés à la fois
    # nécessité de boucler
    all_data = []
    offset = 0
    limit = 100   #on met 'limit' à la plus grande valeur autorisée par SNCF, soit 100
    while True:
        params = {"limit": limit,"offset": offset}
        response = requests.get(url, params=params)
        if response.status_code != 200:
            #print(f"Erreur API  {response.status_code}"). #se déclenche quand on a atteint la fin
            break
        data = response.json()
        valeurs = data.get("results", [])
        all_data.extend(valeurs)
        offset += limit

    return all_data

def fetch_api_data():
    #on a 1 API sur le site impactco2 à récupérer, on va l'appeler 2 fois pour des paramètres différents

    liste_df = {}

    #API impact CO2 de Ademe : permettant de charger les émissions co2 selon différents moyens de transport
    url_base = "https://impactco2.fr/api/v1"
    km = 1000
    include_construction = 0
    # estimation faite pour 1000 km, sans intégration des émissions co2 liées à la construction => part transport
    url_personnalise = f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction={include_construction}&language=fr"
    response = requests.get(url_base + url_personnalise)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2_usage'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (avec), source ImpactCO2:", response.status_code)
        liste_df['valeur_emissions_co2_usage'] = pd.DataFrame()

    # même estimation faite pour 1000 km,mais ici avec intégration des émissions co2 liées à la construction => total = transport + construction
    include_construction = 1
    url_personnalise = f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction={include_construction}&language=fr"
    response = requests.get(url_base + url_personnalise)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2_global'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (sans), source ImpactCO2:", response.status_code)
        liste_df['valeur_emissions_co2_global'] = pd.DataFrame()

    #liste des gares SNCF
    url = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/records?select=code_uic%2C%20libelle%2C%20commune%2C%20departemen%2C%20c_geo&where=voyageurs%3D'O'"
    df = pd.json_normalize(boucle_API(url))
    liste_df['gares'] = df

    return liste_df