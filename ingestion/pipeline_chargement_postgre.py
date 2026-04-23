import pandas as pd
import requests
from io import StringIO, BytesIO
import zipfile
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import psycopg2


#------------------------------
#  initialisation base de données
#------------------------------
# Paramètres de connexion à base de données PostgreSQL
database = "emissions_co2"
username = "postgres"
password = "SandPOST6642"
host="localhost"
#database = "airflow"
#username = "postgres"
#password = "postgres"
#host = "postgres"  #!!!!!!host="localhost" devient host="postgres".  A MODIFIER POUR FONCTIONNER AVEC DOCKER
port = 5432

# On créée la connexion vers la base de données.  
# postgresql s'utilise avec le driver psycopg2 (et mysql avec le driver pymyslq)
DATABASE_URI = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(DATABASE_URI)
# !!!! case à créer manuellement avant  sinon fonctionne pas !!! à revoir

# On créée la base de données si elle n'existe pas.
if not database_exists(engine.url):
    create_database(engine.url)
#on crée le schéma associé dans lequel on va enregistrer nos tables et vues    
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS emissions_co2"))


    #-------------
    #fin initialisation base
    #---------------


def load_url():
    #----------------
    #liste des url des données à charger
    #-----------------
    url_communes = "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"

    url_cars = "https://www.data.gouv.fr/api/1/datasets/r/bc42c2e3-d24c-4499-a966-d35656c6cfc1"

    url_trains = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/emission-co2-perimetre-complet/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
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
    response = requests.get(url_airports_fr)
    if response.status_code == 200:
        liste_df['airports'] = pd.read_csv(StringIO(response.text))
    else:
        print("Erreur téléchargement liste aéroport:", response.status_code)

    #load de la liste des communes
    response = requests.get(url_communes)
    if response.status_code == 200:
        liste_df['communes'] = pd.read_csv(BytesIO(response.content),encoding='utf-8', low_memory=False)
    else:
        print("Erreur téléchargement liste communes:", response.status_code)

    #load routes.txt : liste de toutes les routes SNCF (selon les différents moyen TGV, TER, intercités, ...)
    #dans un fichiers ZIP : on récupère uniquement routes.txt
    response = requests.get(url_routes_train)
    if response.status_code == 200:
         #ouvrir le zip, le fichier qui nous intéresse est dans le fichier de nom routes.txt
        z = zipfile.ZipFile(BytesIO(response.content))   #z représente l'archive zip
        # ouvrir le bon fichier
        with z.open("routes.txt") as f:
            liste_df['routes_train'] = pd.read_csv(f, sep=",")  
        with z.open("stops.txt") as f:
            liste_df['stops'] = pd.read_csv(f, sep=",")   
        with z.open("stop_times.txt") as f:
            liste_df['stop_times'] = pd.read_csv(f, sep=",")    
        with z.open("trips.txt") as f:
            liste_df['trips'] = pd.read_csv(f, sep=",")        
    else:
        print("erreur téléchargement routes SNCF txt")

    #load cars
    response = requests.get(url_cars)
    if response.status_code == 200:
        #ouvrir le zip, le fichier qui nous intéresse est dans le 1er fichier
        z = zipfile.ZipFile(BytesIO(response.content))   #z représente l'archive zip
        with z.open(z.namelist()[-1]) as f:
            liste_df['cars'] = pd.read_csv(f, sep=';', encoding='latin-1')
        #z.namelist()[0] => retourne dans la liste des fichiers dans le ZIP le 1er fichier
        #z.open() => ouvre ce fichier (sans l’extraire sur disque) et retourne un flux de données
    else:
        print("Erreur téléchargement cars:", response.status_code)

    #routes aériennes
    response = requests.get(url_routes_air)
    if response.status_code == 200:
        liste_df['routes_air'] = pd.read_csv(StringIO(response.text), header=None)
        liste_df["routes_air"].columns = ['Airline', 'Airline_ID', 'Source_airport', 'Source_airport_ID',
                'Destination_airport', 'Destination_airport_ID','Codeshare', 'Stops', 'Equipment']
    else:
        print("Erreur téléchargement routes aériennes:", response.status_code)

   #departements
    response = requests.get(url_departements)
    if response.status_code == 200:
        liste_df['departements'] = pd.read_csv(BytesIO(response.content), encoding='utf-8')
    else:
        print("Erreur téléchargement departements :", response.status_code)


    return liste_df


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
    # estimation faite pour 1000 km, avec intégration des émissions co2 liées à la construction
    url_personnalise = f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction={include_construction}&language=fr"
    response = requests.get(url_base + url_personnalise)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (avec), source ImpactCO2:", response.status_code)

    # même estimation faite pour 1000 km,mais ici SANS intégration des émissions co2 liées à la construction
    include_construction = 1
    url_personnalise = f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1&includeConstruction={include_construction}&language=fr"
    response = requests.get(url_base + url_personnalise)
    if response.status_code == 200:
        data = response.json()["data"]
        liste_df['valeur_emissions_co2_sans'] = pd.DataFrame(data)
    else:
        print("Erreur téléchargement émissions CO2 (sans), source ImpactCO2:", response.status_code)

    #liste des gares SNCF
    url = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/records?select=code_uic%2C%20libelle%2C%20commune%2C%20departemen%2C%20c_geo&where=voyageurs%3D'O'"
    df = pd.json_normalize(boucle_API(url))
    liste_df['gares'] = df

    return liste_df



def load_to_postgre(dico):
    #on part d'un dictionnaire contenant pour la clé un nom qui deviendra le nom de table et en valeur un dataframe
    for key, value in dico.items():
        # on récupère de df du dico et on crée une table de nom key dans PostgreSQL avec pour contenu df
        df = value
        df.to_sql(f"raw_{key}", engine, if_exists="replace", index=False, schema="emissions_co2")


def run_pipeline():
     #le pipeline va charger les fichiers .csv présents aux url définies dans des dataframes,  les API
    #puis création des tables correspondantes sous PostgreSQL
    dico_table = {}
    dico_table = load_url()
    dico_table.update(fetch_api_data())
    load_to_postgre(dico_table)



#-----------------
#prog general
#-----------------

if __name__ == "__main__":

   run_pipeline()



