import logging
from io import BytesIO, StringIO
import zipfile

import pandas as pd
import requests
from requests.exceptions import RequestException


# Ce module contient des fonctions pour charger des données à partir de différentes sources : url 
# (fichiers CSV, Excel, ZIP) et API (fichiers JSON).
# Les données sont transformées en DataFrames pandas, qui seront ensuite chargés dans la base de données PostgreSQL.

# Définition de constantes utilisées dans la partie chargement des données pour les URLs 
# et la définion des colonnes des DataFrames
DEFAULT_TIMEOUT = 10
ROUTE_TRAIN_FILES = ["routes.txt", "stops.txt", "stop_times.txt", "trips.txt"]
ROUTES_AIR_COLUMNS = [
    "Airline",
    "Airline_ID",
    "Source_airport",
    "Source_airport_ID",
    "Destination_airport",
    "Destination_airport_ID",
    "Codeshare",
    "Stops",
    "Equipment",
]

NEW_CARS_COLUMNS = [
        "carrosserie",
        "modele",
        "energie",
        "bv",
        "conso_min",
        "conso_max",
        "unite",
        "co2_min",
        "co2_classe_min",
        "co2_max",
        "co2_classe_max",
    ]

# Configuration du logger pour le module, permettant de suivre les erreurs et 
# les informations lors du chargement des données
logger = logging.getLogger(__name__)


# le _ permet d'identifier les fonctions comme privées, c'est à dire qu'elles ne sont pas destinées
# à être utilisées en dehors de ce module.

# ---------------
# Fonctions de chargement des données à partir d'URLs 
# ---------------
# requetage d'une url avec gestion des erreurs (retourne None + message en cas d'erreur)
def _safe_request(url, params=None, timeout=DEFAULT_TIMEOUT):
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response
    except RequestException as exc:
        logger.error("Unable to fetch %s: %s", url, exc)
        return None

# Définitions de fonctions pour charger les données à partir des différentes sources (CSV, Excel, ZIP) 
# et les transformer en DataFrames pandas.
def _read_csv_from_text(response, **kwargs):
    return pd.read_csv(StringIO(response.text), **kwargs)


def _read_csv_from_bytes(response, **kwargs):
    return pd.read_csv(BytesIO(response.content), **kwargs)


def _read_excel_from_bytes(response, **kwargs):
    return pd.read_excel(BytesIO(response.content), **kwargs)


# Fonction pour charger un DataFrame à partir d'une URL, avec gestion des erreurs et support pour différents formats de données.
def _load_dataframe(url, use_bytes=False, **kwargs):
    response = _safe_request(url)
    if response is None:
        return pd.DataFrame()
    return _read_csv_from_bytes(response, **kwargs) if use_bytes else _read_csv_from_text(response, **kwargs)

# Fonction pour charger plusieurs tables à partir d'une archive ZIP, avec gestion des erreurs et 
# création de DataFrames.
def _load_zip_tables(url, filenames):
    response = _safe_request(url)
    if response is None:
        return tuple(pd.DataFrame() for _ in filenames)

    try:
        with zipfile.ZipFile(BytesIO(response.content)) as archive:
            tables = [pd.read_csv(archive.open(filename)) for filename in filenames]
    except (KeyError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        logger.error("Unable to read zip archive from %s: %s", url, exc)
        return tuple(pd.DataFrame() for _ in filenames)

    return tuple(tables)

# Fonction permettant de trouver une ligne d'en-tête dans un DataFrame en recherchant un
# texte spécifique.
# Retourne l'index de cette ligne, ou None si elle n'est pas trouvée.
def _find_header_row(dataframe, search_text):
    matches = dataframe.apply(
        lambda row: row.astype(str).str.contains(search_text, case=False, na=False).any(),
        axis=1,
    )
    return matches.idxmax() if matches.any() else None


def load_url():

    # Dictionnaire des urls à charger, avec pour clé le nom qui deviendra le nom de table (avec prefixe raw_)
    # et en valeur l'url correspondante
    urls = {
        "communes": "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325",
        "new_cars": "https://carlabelling.ademe.fr/recherche/export?searchString=&co2=&brand=&model=&category=&range=&transmission=&price=0%2C500000&maxconso=&energy=0%2C7&RechercherL=Rechercher&limit=50&offset=50&limit=3731&offset=0",
        "routes_train": "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
        "airports": "https://ourairports.com/countries/FR/airports.csv",
        "routes_air": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat",
        "departements": "https://static.data.gouv.fr/resources/departements-de-france/20200425-135513/departements-france.csv",
    }

    # Chargement des données à partir des URLs, avec gestion des erreurs et création de DataFrames
    data_frames = {
        "airports": load_airports(urls["airports"]),
        "communes": load_communes(urls["communes"]),
        "routes_train": None,
        "stops": None,
        "stop_times": None,
        "trips": None,
        "new_cars": load_new_cars(urls["new_cars"]),
        "routes_air": load_routes_air(urls["routes_air"]),
        "departements": load_departements(urls["departements"]),
    }

    data_frames["routes_train"], data_frames["stops"], data_frames["stop_times"], data_frames["trips"] = (
        load_routes_train(urls["routes_train"])
    )

    return data_frames


# Définition des différentes fonctions de chargement pour chaque type de données, 
# appelées par la fonction load_url()
def load_airports(url_airports_fr):
    return _load_dataframe(url_airports_fr)


def load_communes(url_communes):
    return _load_dataframe(url_communes, use_bytes=True, encoding="utf-8", low_memory=False)


def load_routes_train(url_routes_train):
    return _load_zip_tables(url_routes_train, ROUTE_TRAIN_FILES)


def load_new_cars(url_new_cars):
    response = _safe_request(url_new_cars)
    if response is None:
        return pd.DataFrame()

    df_brut = _read_excel_from_bytes(response)
    header_row = _find_header_row(df_brut, "Carrosserie")
    if header_row is None:
        logger.warning("Header row not found in new cars export from %s", url_new_cars)
        return pd.DataFrame()

    voitures = df_brut.iloc[header_row + 3 :, 0:11].copy()
    voitures.columns = NEW_CARS_COLUMNS

    return voitures.reset_index(drop=True)


def load_routes_air(url_routes_air):
    data_frame = _load_dataframe(url_routes_air, header=None)
    if data_frame.empty:
        return data_frame

    data_frame.columns = ROUTES_AIR_COLUMNS
    return data_frame


def load_departements(url_departements):
    return _load_dataframe(url_departements, use_bytes=True, encoding="utf-8")


# ------------------
# Fonctions pour charger les données à partir d'APIs
# ------------------

# Fonction permettant des gérer la pagination d'une API 
# en effectuant des requêtes successives jusqu'à ce que toutes les données soient récupérées.
def boucle_API(url):
    all_data = []
    offset = 0
    limit = 100

    while True:
        response = _safe_request(url, params={"limit": limit, "offset": offset})
        if response is None:
            break
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        all_data.extend(results)
        offset += limit
    return all_data


def fetch_api_data():
    liste_df = {}
    # Premier appel d'API pour récupérer les données d'émissions de CO2 pour les trajets en transport,
    url_base = "https://impactco2.fr/api/v1"
    km = 1000
    # Boucle pour récupérer les données d'émissions de CO2 pour les trajets en transport,
    # en incluant ou non la part fabrication du véhicule (0 = uniquement usage, 1 = usage + fabrication)
    for include_construction, key in ((0, "valeur_emissions_co2_usage"), (1, "valeur_emissions_co2_global")):
        endpoint = (
            f"/transport?km={km}&displayAll=0&ignoreRadiativeForcing=0&occupencyRate=1"
            f"&includeConstruction={include_construction}&language=fr"
        )
        response = _safe_request(url_base + endpoint)
        if response is None:
            liste_df[key] = pd.DataFrame()
            continue

        liste_df[key] = pd.DataFrame(response.json().get("data", []))

    # Second appel d'API pour récupérer les données de la totalité des gares françaises à destination des voyageurs
    gare_url = (
        "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/"
        "liste-des-gares/records?select=code_uic%2C%20libelle%2C%20commune%2C%20departemen%2C%20c_geo&where=voyageurs%3D'O'"
    )
    liste_df["gares"] = pd.json_normalize(boucle_API(gare_url))

    return liste_df
