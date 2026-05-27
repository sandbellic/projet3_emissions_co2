from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import os
from dotenv import load_dotenv


#------------------------------
#  initialisation base de données
#------------------------------
def init_bdd():
    # Création d'un .env pour stocker mon MDP (!!! A ajouter .env dans .gitignore)
    # .env est bien pour le back end et les scripts python LOCAUX
    # chargement des variables de connexion à base de données PostgreSQL, définies dans .env
    load_dotenv()   #fonction prédéfinie pour charger depuis .env
    database = os.getenv("POSTGRES_DB")
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    #!!!!! PENSER A METTRE LE PROFILES.YML A JOUR EGALEMENT
    #-----cas utilisation airflow / docker
    #database = "airflow"
    #username = "postgres"
    #password = "postgres"
    #host = "postgres"  #!!!!!!host="localhost" devient host="postgres".  A MODIFIER POUR FONCTIONNER AVEC DOCKER


    # On créée une connexion temporaire vers la base postgres
    # si on se connecte directement à database et qu'elle n'existe pas => erreur 
    # postgresql s'utilise avec le driver psycopg2 (et mysql avec le driver pymyslq)
    TEMP_DATABASE_URI = (f"postgresql+psycopg2://{username}:{password}@{host}:{port}/postgres")
    temp_engine = create_engine(TEMP_DATABASE_URI)

    #vraie database
    DATABASE_URI = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    # On créée la base de données si elle n'existe pas.
    if not database_exists(DATABASE_URI):
        print(f"Création database {database}")
        create_database(DATABASE_URI)
        print(f"Database créée")

    #connexion à la database
    engine = create_engine(DATABASE_URI)

    #on crée le schéma associé dans lequel on va enregistrer nos tables et vues    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS emissions_co2"))
        engine = create_engine(DATABASE_URI)

    return engine
#-------------
#fin initialisation base
#---------------


#---------------
# chargement des données dans postgres
#---------------
def load_to_postgre(dico, engine):
    # on part d'un dictionnaire contenant pour clé un nom qui deviendra un nom de table et en valeur
    # un dataframe
    for key, value in dico.items():
        # on récupère les valeurs de dico qui sont des dataframes et pour chacunes d'elles on crée 
        # une table dans postgres avec pour nom le préfixe 'raw_' auquel on ajoute key 
        # si la table existe déjà, on la remplace, et on n'ajoute pas d'index, et on précise le schéma 
        # dans lequel enregistrer la table
        df = value
        df.to_sql(f"raw_{key}", engine, if_exists="replace", index=False, schema="emissions_co2")

