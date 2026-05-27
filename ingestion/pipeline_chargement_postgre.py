from ingestion.utils.data_loaders import load_url, fetch_api_data
from ingestion.utils.utils_bdd import *


def run_pipeline(engine):
    # Le pipeline va charger nos différentes données à partir des urls définies ou d'APIs dans 
    # des dataframes, 
    dico_table = {}
    dico_table = load_url()
    dico_table.update(fetch_api_data())
 
    # puis charger ces dataframes dans des tables de notre base de données PostgreSQL
    load_to_postgre(dico_table, engine)


#-----------------
#prog general
#-----------------

if __name__ == "__main__":
   engine = init_bdd()
   run_pipeline(engine)



