# Projet CO2 Emissions

Application pour calculer et comparer les émissions individuelles de CO2 pour des trajets intérieurs en France.

## 🎯 Objectif

Permettre à un utilisateur de comparer les émissions CO2 (train, avion, route, etc.) entre deux communes françaises et d'afficher distances, durées, valeurs d'émission et visualisations cartographiques.
Possibilité également de saisir un kilométrage pour visualiser les valeurs d'émission.

## 🧭 Stack technique

- Python, Pandas, Requests
- PostgreSQL
- SQLAlchemy
- dbt (dbt-core, dbt-postgres)
- Streamlit (+ streamlit-folium)
- Prefect

## 📁 Structure clé

- `app_streamlit/` — application Streamlit et UI
- `ingestion/` — scripts d'ingestion et utilitaires pour charger les sources (CSV, API) vers PostgreSQL
- `dbt_p3/` — projet dbt (staging, mart, macros, seeds)
- `orchestration/` — flows Prefect pour séquencer ingestion et dbt
- `requirements.txt` — dépendances Python

## ✅ Prérequis

- Python 3.10+
- PostgreSQL accessible
- dbt installé et configuré (profil `dbt_p3`)

## ⚙️ Configuration

Créer un fichier `.env` à la racine et définir :

```
POSTGRES_DB=<nom_de_la_base>
POSTGRES_USER=<utilisateur>
POSTGRES_PASSWORD=<mot_de_passe>
POSTGRES_HOST=<host>
POSTGRES_PORT=<port>
```

Ajoutez `.env` à `.gitignore` pour ne pas commiter les secrets.

## 📦 Installation

```bash
git clone <url-du-projet>
cd projet3_emissions_co2
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
```

## ▶️ Exécution des étapes principales

1. Ingestion des données (création/chargement des tables `raw_*` dans le schéma `emissions_co2`) :

```bash
python ingestion/pipeline_chargement_postgre.py
```

2. dbt — seed et run :

```bash
cd dbt_p3
dbt seed
dbt run
```

3. Orchestration complète (Prefect) :

```bash
python orchestration/prefect_flow.py
```

4. Lancer l'application Streamlit :

```bash
streamlit run app_streamlit/app.py
```

## 🔎 Sources de données

- listes communes (INSEE)
- départements (data.gouv)
- aéroports (OurAirports)
- routes aériennes (OpenFlights)
- données SNCF (gares, trajets)
- valeurs émissions (ADEME ImpactCO2)

## 🛠 Bonnes pratiques

- Ne commitez jamais les secrets (.env).
- Synchronisez `profiles.yml` dbt avec les mêmes paramètres PostgreSQL.
- Préparer un `docker-compose` pour reproductibilité en production.

## ✅ Améliorations suggérées

- Ajouter des tests dbt et un pipeline CI
- Conteneuriser l'ensemble (Docker Compose)
- Ajouter une page Streamlit pour export / API

## Licence

À définir.
