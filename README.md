Le projet a pour objectif de fournir un application web permettant le calcul et la comparaison des émissions CO2, 
pour différents trajets intérieurs France, et selon différents moyens de transports utilisables.

L'utilisateur devra renseigner une ville de départ et d'arrivée. 
Le calculateur lui proposera automatiquement, selon les moyens de transport disponibles entre les 2 lieux, les émissions de co2 respectives.

Les sources utilisées pour nos calculs :
  Fichiers (.csv, .dat)
  * liste des communes FR (Insee)
  * liste des aéroports FR (ourairports.com)
  * liste des routes aériennes (openflights - données de 2015)
*
  API :
  * liste des gares FR (SNCF open Data)
  * Valeurs émissions CO2 par type de transport (Ademe impactCO2)


La stack data utilisée:
  * Ingestion des données : Python => PostgreSQL
  * Nettoyage des données, et préparation (pour aboutir à un table de fait) : dbt => PostgreSQL
  * App web : streamlit (affichage des données)
  * orchestrateur : Prefect


