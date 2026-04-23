Le projet a pour objectif de fournir une application web permettant le calcul et la comparaison des émissions CO2, 
pour différents trajets intérieurs France, et selon différents moyens de transports utilisables.

L'utilisateur devra renseigner une ville de départ et une ville d'arrivée. 
Le calculateur lui proposera automatiquement, et selon les moyens de transport disponibles entre les 2 lieux, les émissions de co2 respectives.

Les sources utilisées pour nos calculs :
  Fichiers (.csv, .dat)
  * liste des communes FR (Insee)
  * liste des départements (Data Gouv)
  * liste des aéroports FR (ourairports.com)
  * liste des routes aériennes (openflights - données de 2015)
  * lstes des trajets SNCF (trajets / Voyages / Arrêts / gares d'Arrêt)

  API :
  * liste des gares FR (SNCF open Data)
  * Valeurs émissions CO2 par type de transport (Ademe impactCO2) avec et sans part émissions liées à construction/entretien


La stack data utilisée:
  * Ingestion des données : Python => PostgreSQL
  * Nettoyage des données, et préparation (pour aboutir à un table de fait) : dbt => PostgreSQL
  * App web : streamlit (affichage des données)
  * orchestrateur : Airflow (ingestion et nettoyage)
  * container : Docker pour l'ensemble

Architecture générale :

Projet3_emissions_co2
-- airflow
|   --dags / pipeline_data.py
-- app / streamlit_app.py
-- dbt_p3
|   -- macros : create_extensions.sql, duree_avion.sql, haversine.sql
|   -- models : 
|      -- mart : dim_routes (table de fait), dim_communes, dim_cars
|      -- staging : stg_air_distances.sql, stg_air_routes.sql, stg_airports.sql, stg_cars.sql, stg_communes_airports.sql,
|          stg_communes_gares.sql, stg_communes.sql, stg_emission_co2_par_transport.sql, stg_gares.sql, stg_train_routes.sql
|          stg_train_stop_times.sql, stg_train_stops
|      -- seeds : coordonnees_villes_manquantes.csv, energie_mapping.csv
|   -- tests
|  --  dbt_project.yml
-- ingestion / pipeline_chargement_postgre 
-- .gitignore
-- readme.md
-- requirements.txt
-- venv
-- db / init_db.sql 
-- docker-compose.yaml

.dbt/profiles.yml

**** INGESTION DES DONNEES *****
A partir des différentes sources de données, chargement des URL et API dans un dictionnaire de dataframes.
Pour chaque dataframe, création dans postgres d'une table "raw_" + clé dico, contenant les lignes de dataframe.

***** dbt - transformation des données *****
A partir des tables postgres, création de vues (sélections, jointures, numérotation de lignes, ...) dans le staging, pour arriver à 3 vues métiers dans le dossier mart:
    * dim_routes : contient les routes air et train (communes départ, arrivée, distance, temps, émissions_co2) - stockée entable avec des index
      sur les communes pour les améliorer les temps de recherche > équivalent table de fait
    * dim_communes : liste des communes (nom, coordonnées géographiques) de france métropolitaine
    * dim_cars : valeurs moyennes d'émission de co2 calculées à partir de données réelles par motorisation et catégorie

***** app streamlit ******
A partir des 3 vues/table stockées dans postgres, recherche et affichage des valeurs calculées (distances, temps et émissions co2) pour les trajets sélectionnés, par moyen de transport, quand ils existent, dans une application web.
page 1 . Comparaison chiffrée et graphique pour bien se rendre compte des différences.
Représentation du trajet routier sur une map.
page 2 . KPIs

***** airflow ***** et Docker
Si docker isole notre projet dans un container pour des raisons de portabilité, ...
airflow va nous permettre d'enchainer et planifier de façon automatique, la chaine suivante :
ingestion des données >> dbt seed (chargement des données figées) >> dbt run (création des vues)
elle n'inclut pas l'application web streamlit qui est lancée de faon indépendante.





