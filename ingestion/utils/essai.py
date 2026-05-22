import requests
import pandas as pd
from io import StringIO, BytesIO



def test_new_cars():
    url_new_cars = "https://carlabelling.ademe.fr/recherche/export?searchString=&co2=&brand=&model=&category=&range=&transmission=&price=0%2C500000&maxconso=&energy=0%2C7&RechercherL=Rechercher&limit=50&offset=50&limit=3731&offset=0"

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
            liste_df.columns = ["Carrosserie", "Modele", "Energie",
                "BV", "Conso_Min", "Conso_Max", "Unite", "CO2_Min",
                "CO2_Classe_Min", "CO2_Max", "CO2_Classe_Max"]
    else:
            print("Erreur téléchargement new cars:", response.status_code)
            liste_df= pd.DataFrame()
    return liste_df

liste_df = test_new_cars()
print(liste_df.sample(20))