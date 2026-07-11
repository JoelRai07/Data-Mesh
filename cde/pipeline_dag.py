"""
Airflow-DAG fuer Cloudera Data Engineering (CDE): orchestriert die fuenf
Pipeline-Stufen als CDE-Jobs - dieselben src/-Module wie beim lokalen
Docker-Lauf, nur der Taktgeber ist ein anderer (Airflow-Schedule statt
"docker compose run pipeline" / scheduler.py).

Jeder Task triggert per CDEJobRunOperator einen zuvor angelegten CDE-Job
(Anlage der Jobs: s. cde/README.md). Die Reihenfolge entspricht exakt
src/run_pipeline.py:

    Stufe 0 Datenmodell -> 1 Staging -> 2 Audit -> 3 Target (Spark)
    -> 4 Data-Contract-Gate

Ein Fehlschlag stoppt die Kette (Downstream-Tasks laufen nicht), und der
Lauf kann in der Airflow-UI ab der gescheiterten Stufe wiederaufgesetzt
werden - dank Idempotenz aller Stufen (Fingerprint-Skip, atomare
Iceberg-Commits) ist auch ein kompletter Re-Run jederzeit gefahrlos.
"""
from datetime import datetime, timedelta

from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

default_args = {
    "owner": "gruppe3",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Stufen bauen aufeinander auf - bei einem geplanten Nachholen (catchup
    # ist unten aus, aber manuelle Re-Runs gibt es) darf keine Stufe an
    # ihrem Vorgaenger vorbeilaufen.
    "depends_on_past": False,
}

with DAG(
    dag_id="gruppe3_data_mesh_pipeline",
    description="Standortprofil-Datenprodukt: source -> staging -> audit -> star schema -> contract gate",
    schedule_interval="0 5 * * *",  # taeglich 05:00 UTC; dank Fingerprint-Skip sind Leerlaeufe billig
    start_date=datetime(2026, 7, 1),
    catchup=False,
    # Airflow legt neue DAGs standardmaessig pausiert an; in der CDE-Airflow-UI
    # laesst sich der Toggle nicht immer bedienen - deshalb aktiv erzeugen.
    is_paused_upon_creation=False,
    max_active_runs=1,  # Laeufe nie ueberlappen lassen (eine Pipeline, ein Zustand)
    default_args=default_args,
    tags=["gruppe3", "data-mesh", "iceberg"],
) as dag:

    stufe0_datenmodell = CDEJobRunOperator(
        task_id="stufe0_datenmodell",
        job_name="gruppe3-stufe0-datenmodell",
    )

    stufe1_staging = CDEJobRunOperator(
        task_id="stufe1_staging",
        job_name="gruppe3-stufe1-staging",
    )

    stufe2_audit = CDEJobRunOperator(
        task_id="stufe2_audit",
        job_name="gruppe3-stufe2-audit",
    )

    stufe3_target = CDEJobRunOperator(
        task_id="stufe3_target",
        job_name="gruppe3-stufe3-target",
    )

    stufe4_contract_gate = CDEJobRunOperator(
        task_id="stufe4_contract_gate",
        job_name="gruppe3-stufe4-contract",
    )

    stufe0_datenmodell >> stufe1_staging >> stufe2_audit >> stufe3_target >> stufe4_contract_gate
