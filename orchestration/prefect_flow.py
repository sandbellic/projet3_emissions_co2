from prefect import flow, task
import subprocess
import os


# =========================
# 1 - INGESTION
# =========================

@task(log_prints=True)
def ingestion_postgres():

    print("Lancement ingestion données dans PostgreSQL...")

    result = subprocess.run(
        ["python", "ingestion/pipeline_chargement_postgre.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(result.stderr)

    print("Ingestion terminée.")


# =========================
# 2 - DBT SEED
# =========================

@task(log_prints=True)
def dbt_seed():

    print("Lancement dbt seed...")

    result = subprocess.run(
        ["dbt", "seed"],
        cwd="dbt_p3",
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(result.stderr)

    print("dbt seed terminé.")


# =========================
# 3 - DBT RUN (models)
# =========================

@task(log_prints=True)
def dbt_run():

    print("Lancement dbt run...")

    result = subprocess.run(
        ["dbt", "run"],
        cwd="dbt_p3",
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(result.stderr)

    print("dbt run terminé.")


# =========================
# 4 - DBT TEST - Pas de tests réalisés ici 
# =========================


# =========================
# FLOW PRINCIPAL
# =========================

@flow(name="pipeline_projet_co2")
def pipeline_complet():
    ingestion_postgres()
    dbt_seed()
    dbt_run()

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    pipeline_complet()