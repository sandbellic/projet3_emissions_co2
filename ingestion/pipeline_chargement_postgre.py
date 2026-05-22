from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
from ingestion.utils.data_loaders import *
from ingestion.utils.utils_bdd import *


def run_pipeline(engine):
     #le pipeline va charger les fichiers .csv présents aux url définies dans des dataframes,  les API
    #puis création des tables correspondantes sous PostgreSQL
    dico_table = {}
    dico_table = load_url()
    dico_table.update(fetch_api_data())
    load_to_postgre(dico_table, engine)


#-----------------
#prog general
#-----------------

if __name__ == "__main__":
   engine = init_bdd()
   run_pipeline(engine)



