# Pipeline in Cloudera Data Engineering (CDE) betreiben

Dieses Verzeichnis enthält alles, um die Pipeline zusätzlich zum lokalen
Docker-Weg in CDE laufen zu lassen — **derselbe Code unter `src/`, nur die
Orchestrierung und die Spark-I/O-Schicht unterscheiden sich**:

| | Lokal (Docker) | CDE |
|---|---|---|
| Taktgeber | `docker compose run pipeline` bzw. `scheduler.py` | Airflow-DAG ([pipeline_dag.py](pipeline_dag.py)) |
| Stufen 0/1/2/4 | impyla gegen Impala-Endpoint | **identisch** (impyla gegen Impala-Endpoint) |
| Stufe 3 Spark-I/O | `SPARK_IO_MODE=jdbc` (Default): JDBC-Jar + collect/VALUES/Shadow | `SPARK_IO_MODE=catalog`: native Iceberg-Reads/-Writes, kein Treiber-Jar |
| Zugangsdaten | `.env` | Env-Vars in der Job-Config (s.u.) |
| ETL-State / Fingerprint-Skip | `gruppe3_etl_state` | **dieselbe Tabelle** — beide Welten teilen sich den Zustand |

Wichtig: Nicht beide Scheduler gleichzeitig aktiv haben. Läuft der DAG in
CDE, den lokalen `scheduler`-Container nicht starten (manuelle lokale Läufe
per `docker compose run pipeline` sind dank Idempotenz trotzdem jederzeit ok).

## Einmalige Einrichtung (CDE CLI, gegen euren Virtual Cluster konfiguriert)

### 1. Code als Files-Resource hochladen

Die Verzeichnisstruktur muss erhalten bleiben (`contract_check.py` erwartet
`../data/data_contract.yaml` relativ zu `src/`):

```bash
cde resource create --name gruppe3-pipeline-code --type files
# Aus dem Repo-Root (Data-Mesh/) ausfuehren:
cde resource upload --name gruppe3-pipeline-code \
  --local-path src --resource-path src
cde resource upload --name gruppe3-pipeline-code \
  --local-path data/data_contract.yaml --resource-path data/data_contract.yaml
```

Das JDBC-Jar (`src/utils/ImpalaJDBC42.jar`) wird **nicht** gebraucht und
nicht hochgeladen — im Katalog-Modus liest Spark den Metastore direkt.

### 2. Python-Environment anlegen

```bash
cde resource create --name gruppe3-python-env --type python-env
cde resource upload --name gruppe3-python-env \
  --local-path cde/requirements-cde.txt --resource-path requirements.txt
```

(Danach den Build-Status abwarten: `cde resource describe --name gruppe3-python-env`.)

### 3. Die fünf Stufen-Jobs anlegen

Alle Stufen laufen als Spark-Jobs mit den vorhandenen Modulen als
Application-File — die Stufen 0/1/2/4 nutzen Spark dabei gar nicht (reine
Driver-Python-Skripte mit impyla), deshalb bekommen sie minimale Ressourcen.
Die Zugangsdaten gehen als Driver-Env-Vars in die Job-Config
(`spark.kubernetes.driverEnv.*` — CDE führt den Driver als Kubernetes-Pod aus):

```bash
# Gemeinsame Env-Vars fuer alle Jobs (Werte aus eurer .env):
ENVS="--conf spark.kubernetes.driverEnv.IMPALA_HOST=<host> \
 --conf spark.kubernetes.driverEnv.IMPALA_PORT=443 \
 --conf spark.kubernetes.driverEnv.IMPALA_HTTP_PATH=<http-path> \
 --conf spark.kubernetes.driverEnv.IMPALA_USER=<workload-user> \
 --conf spark.kubernetes.driverEnv.IMPALA_PASSWORD=<workload-passwort> \
 --conf spark.kubernetes.driverEnv.DATABASE=gruppe3 \
 --conf spark.kubernetes.driverEnv.PREFIX=gruppe3_"

cde job create --name gruppe3-stufe0-datenmodell --type spark \
  --application-file src/create_datamodel.py \
  --mount-1-resource gruppe3-pipeline-code \
  --python-env-resource-name gruppe3-python-env \
  --driver-cores 1 --driver-memory 1g --executor-cores 1 --executor-memory 1g --num-executors 1 \
  $ENVS

cde job create --name gruppe3-stufe1-staging --type spark \
  --application-file src/pipeline_default_to_staging.py \
  --mount-1-resource gruppe3-pipeline-code \
  --python-env-resource-name gruppe3-python-env \
  --driver-cores 1 --driver-memory 1g --executor-cores 1 --executor-memory 1g --num-executors 1 \
  $ENVS

cde job create --name gruppe3-stufe2-audit --type spark \
  --application-file src/pipeline_staging_to_audit.py \
  --mount-1-resource gruppe3-pipeline-code \
  --python-env-resource-name gruppe3-python-env \
  --driver-cores 1 --driver-memory 1g --executor-cores 1 --executor-memory 1g --num-executors 1 \
  $ENVS

# Stufe 3: der einzige echte Spark-Job. SPARK_IO_MODE=catalog schaltet die
# native Iceberg-I/O-Schicht ein (s. src/pipeline_audit_to_target.py).
cde job create --name gruppe3-stufe3-target --type spark \
  --application-file src/pipeline_audit_to_target.py \
  --mount-1-resource gruppe3-pipeline-code \
  --python-env-resource-name gruppe3-python-env \
  --driver-cores 2 --driver-memory 4g --executor-cores 2 --executor-memory 4g --num-executors 2 \
  --conf spark.kubernetes.driverEnv.SPARK_IO_MODE=catalog \
  $ENVS

cde job create --name gruppe3-stufe4-contract --type spark \
  --application-file src/contract_check.py \
  --mount-1-resource gruppe3-pipeline-code \
  --python-env-resource-name gruppe3-python-env \
  --driver-cores 1 --driver-memory 1g --executor-cores 1 --executor-memory 1g --num-executors 1 \
  $ENVS
```

**Iceberg-Konfiguration Stufe 3:** In CDE-Virtual-Clustern mit
Iceberg-Support (Spark 3) sind die nötigen Katalog-Einstellungen
(`spark.sql.extensions`, `spark.sql.catalog.spark_catalog`) bereits
vorkonfiguriert. Falls euer VC das nicht mitbringt (erkennbar an
"Table ... is not an Iceberg table" / fehlgeschlagenem `INSERT OVERWRITE`),
dem Stufe-3-Job zusätzlich mitgeben:

```
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
```

### 4. DAG deployen

```bash
cde resource create --name gruppe3-dag --type files
cde resource upload --name gruppe3-dag \
  --local-path cde/pipeline_dag.py --resource-path pipeline_dag.py

cde job create --name gruppe3-pipeline-orchestrierung --type airflow \
  --dag-file pipeline_dag.py \
  --mount-1-resource gruppe3-dag
```

Der DAG läuft dann täglich 05:00 UTC (änderbar in
[pipeline_dag.py](pipeline_dag.py), `schedule_interval`) und ist in der
Airflow-UI des Virtual Clusters sichtbar (Monitoring, Retries, manuelles
Triggern, Wiederaufsetzen ab einer gescheiterten Stufe).

## Verifikation nach dem Deploy

1. Jeden Stufen-Job einmal einzeln manuell starten
   (`cde job run --name gruppe3-stufe1-staging` usw., Reihenfolge 0→4) und
   die Logs prüfen — bei unveränderten Quellen müssen die Stufen 1–3
   "unveraendert - Lauf uebersprungen" bzw. "Snapshot-Fingerprints
   identisch" melden (der State kommt aus `gruppe3_etl_state`, die auch die
   lokalen Läufe nutzen).
2. Einmal Stufe 3 mit erzwungenem Build laufen lassen
   (`--conf spark.kubernetes.driverEnv.FORCE_TARGET_BUILD=1` temporär in die
   Job-Config): Zeilenzahlen müssen dem letzten lokalen Rebuild entsprechen
   (dim_kreis 472, dim_jahr 30, dim_klimastadt 81, dim_gemeinde 10947,
   fact_bevoelkerung 14110, fact_bauland 4720, fact_klima 1539,
   fact_gemeinde_stamm 10947, fact_standortprofil_kpi 4099).
3. Danach den DAG einmal komplett manuell triggern — Stufe 4 muss
   "32 Checks OK" melden.

## Hinweise

- **Secrets:** Die Beispiele oben legen das Workload-Passwort in die
  Job-Config. Wer das vermeiden will, nutzt Airflow-Connections plus einen
  kleinen Wrapper oder CDE-Credentials — für die Abgabe reicht die
  Job-Config, das Passwort ist dort nur für Projektmitglieder sichtbar.
- **CLI-Versionen:** Die exakten Flag-Namen (`--python-env-resource-name`,
  `--mount-N-resource`) variieren leicht zwischen CDE-Versionen — bei
  Abweichungen `cde job create --help` konsultieren; das Setup selbst
  (Resources → Jobs → DAG) bleibt gleich.
- Der lokale Docker-Weg bleibt vollständig funktionsfähig und unverändert
  (Default `SPARK_IO_MODE=jdbc`) — s. Haupt-README, Abschnitt
  "Zwei Betriebsarten".
