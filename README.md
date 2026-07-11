# Data Mesh & Data Engineering – Portfolioprüfung (Gruppe 3)

Datenprodukt **„Standortprofil deutscher Kreise"** auf Basis von vier öffentlichen
Datensätzen (Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die
Rohdaten liegen fertig in einer **Cloudera CDP / Impala**-Datenbank der DHBW
Stuttgart (`default.project_*`, nur lesend); unser Datenprodukt entsteht in der
Gruppen-Datenbank **`gruppe3`**.

**Use Case:** Landkreise vergleichbar machen (Wohnraumdruck, Baulandpreise,
Klimarisiko), indem Bevölkerung + Bauland + Klima + Gemeinden zu **einem**
analysefertigen Datenprodukt verbunden werden — Zielbild ist ein
Standortprofil-Dashboard.

Die Pipeline folgt dem **Write-Audit-Publish-Pattern** (WAP) in drei Stufen und
lädt **inkrementell** (Wasserzeichen, zeilengenauer Key-Merge via Apache
Iceberg, Inhalts-Prüfsummen — unveränderte Quellen werden übersprungen). Die
eigentlichen Transformationen (Unpivot, Pivot, Window-Funktionen) laufen in
**Apache Spark** (PySpark, DataFrame-API).

**Doku-Struktur (genau 3 Markdown-Dateien):**

| Datei | Frage, die sie beantwortet |
|---|---|
| **README.md** (diese Datei) | Was ist das Projekt, wie funktioniert es, wie führe ich es aus, warum sieht das Datenmodell so aus? |
| [docs/ADR.md](docs/ADR.md) | Warum ist alles so gebaut — alle Architektur-Entscheidungen (ADR-1..16) + abgelöste Entscheidungen |
| [docs/Probleme.md](docs/Probleme.md) | Alle Probleme: gelöst (mit Fallstudien), nicht behebbar (dokumentiert), offen |

Die vier abgegebenen Arbeitsergebnisse:

| Deliverable | Wo |
|---|---|
| 1. DDLs + Begründung des Datenmodells | [src/create_datamodel.py](src/create_datamodel.py) + [Datenmodell & Begründung](#datenmodell--begründung) (unten) |
| 2. Pipeline-Code (3 Stufen + Orchestrator + Incremental-State) + Scheduler | [src/run_pipeline.py](src/run_pipeline.py), [src/pipeline_default_to_staging.py](src/pipeline_default_to_staging.py), [src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py), [src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py), [src/etl_state.py](src/etl_state.py), [src/scheduler.py](src/scheduler.py) |
| 3. Data Contract + technische Durchsetzung | [data/data_contract.yaml](data/data_contract.yaml), [data/output_port_ddl.sql](data/output_port_ddl.sql), [src/contract_check.py](src/contract_check.py) |
| 4. README (dieses Dokument) | – |

## Architektur: der Datenfluss

```
default.project_*          (Rohdaten, 4 Tabellen, nur lesend; Klima: 8,6 Mio. Zeilen)
      │
      │  (1) pipeline_default_to_staging.py      WRITE   – impyla / Impala-SQL
      ▼       Klimadaten: Wasserzeichen-Append (nur neue Tage) · übrige Tabellen:
              Prüfsummen-Check, Full Refresh nur bei echter Änderung
gruppe3_staging_*          (unveränderte Rohkopie in unserer Datenbank)
      │
      │  (2) pipeline_staging_to_audit.py        AUDIT   – impyla / Impala-SQL
      ▼       Bereinigung (Encoding-Korrekturen, Transliteration, Koordinaten,
              Filter Germany); inkrementell: Wasserzeichen (Klima) · zeilengenauer
              Business-Key-Merge via Iceberg MERGE/DELETE (Bauland/Bevölkerung)
              · Prüfsumme (Gemeinden)
gruppe3_audit_*            (bereinigte, fachlich saubere Basis)
      │
      │  (3) pipeline_audit_to_target.py         PUBLISH – Apache Spark (PySpark)
      ▼       Unpivot (Bevölkerung), Pivot (Bauland), Jahresmittel (Klima),
              Namens-Matching, Window-Funktionen/z-Score → Star-Schema;
              der komplette Spark-Lauf wird übersprungen, wenn sich seit dem
              letzten Ziel-Build keine Audit-Tabelle geändert hat
gruppe3_dim_* / gruppe3_fact_*   (das Datenprodukt, s. Data Contract)
      │
      │  (4) contract_check.py                   GATE    – impyla / Data Contract
      ▼       Schema, Pflichtfelder, Eindeutigkeit und ausführbare Quality-SQLs;
              bricht den Lauf mit Exit-Code 1 ab, wenn der Contract verletzt ist.
```

**[src/run_pipeline.py](src/run_pipeline.py)** führt alles in fester Reihenfolge
aus (Datenmodell-DDLs + Stufen 1–3 + Data-Contract-Gate) und ist der eine
Einstiegspunkt — auch im Docker-Image.

**Warum Stufe 1+2 in Impala-SQL und nur Stufe 3 in Spark?** Kopieren und
zeilenweises Bereinigen sind reine `INSERT OVERWRITE/INTO … SELECT`-Operationen —
die laufen serverseitig in Impala, ohne 8,6 Mio. Zeilen durch Python/Spark zu
schleusen. Spark wird dort eingesetzt, wo es echten Mehrwert hat: Unpivot per
`explode(array(struct(…)))`, echtes `DataFrame.pivot()` und Window-Aggregate
(`STDDEV() OVER` für den z-Score — in Impala-SQL nicht möglich).

**Wie funktioniert das Incremental Loading?** Den zuletzt verarbeiteten Stand
merkt sich die Pipeline in `gruppe3_etl_state` (Iceberg, ein Eintrag je
Stufe+Tabelle, per `MERGE INTO` geupsertet; die Verlaufs-Historie liegt in den
Iceberg-Snapshots — s. [src/etl_state.py](src/etl_state.py)).

**Ebene 1 — Ganztabellen-Fingerprint (ohne Zeilen-Scan):** Vor jeder Stufe
wird zuerst EIN gespeicherter Fingerprint je Tabelle verglichen — für
Iceberg-Tabellen die aktuelle **Snapshot-ID** (`DESCRIBE HISTORY`, reine
Metadaten-Abfrage). Unverändert → die Stufe wird komplett übersprungen, ohne
eine einzige Datenzeile zu lesen. Erst bei einem Fingerprint-Wechsel greift
Ebene 2, der eigentliche Incremental-Mechanismus. Drei Strategien, je nach
Tabellenart:

- **Wasserzeichen** (Klimadaten, echte Zeitreihe über `dt`): nur neuere Zeilen
  werden angehängt — kein täglicher Full-Rewrite von 8,6 Mio. Zeilen. Die
  Iceberg-Partitionierung `TRUNCATE(4, dt)` (= Jahr) macht den
  Wasserzeichen-Filter zusätzlich zum Partition-Prune.
- **Zeilengenauer Merge** (Bauland, Bevölkerung — Tabellen mit Business-Key):
  billiger Prüfsummen-Vor-Check; bei Änderung direktes Iceberg
  `DELETE` + `MERGE INTO` gegen die Audit-Tabelle mit NULL-sicherem
  Spaltenvergleich (`t.col <=> src.col`) — nur tatsächlich neue/geänderte/
  gelöschte Zeilen werden angefasst, die Audit-Tabelle selbst ist der
  Vergleichsstand (keine separate Zeilen-Hash-Historie nötig).
- **Inhalts-Prüfsumme** (Gemeinden — kein verlässlicher Key): die ganze
  Tabelle wird serverseitig gehasht (`FNV_HASH` je Zeile + `SUM`) und mit dem
  gespeicherten Hash verglichen. Unverändert → Schritt wird übersprungen;
  geändert → Full Refresh per `INSERT OVERWRITE` (auf Iceberg ein einzelner
  atomarer Snapshot-Commit).

Der Skip-Mechanismus zieht sich durch die **ganze** Pipeline: Stufe 3
überspringt den kompletten Spark-Lauf, wenn jede Audit-Tabelle noch exakt die
Iceberg-Snapshot-ID hat, die der letzte Ziel-Build gelesen hat (Vergleich des
Datenstands statt eines Zeitstempels), und der Publish selbst ist ein atomarer
Shadow-Swap (s. `overwrite_table` in
[src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)). Einzige
Stelle, an der im Unverändert-Fall noch Zeilen gescannt werden: die fremden
Quelltabellen in `default.*` (kein Iceberg → kein verlässlicher
Metadaten-Fingerprint → Prüfsummen-Scan als Fallback).

Die fachlichen Begründungen stehen ausführlich in den Modul-Docstrings der
jeweiligen Skripte; die Iceberg-Entscheidung inkl. aller live verifizierten
Iterationen in [ADR.md](ADR.md).

## Projektstruktur

```
Data-Mesh/
├── README.md                        # Diese Datei
├── ADR.md                           # Apache-Iceberg-Entscheidung (alle Iterationen, live verifiziert)
├── requirements.txt                 # Python-Abhängigkeiten (impyla, pyspark, APScheduler, dotenv, PyYAML)
├── .env.example                     # Vorlage für Zugangsdaten → kopieren nach .env
├── .env                             # Echte Zugangsdaten (NICHT eingecheckt)
├── .gitignore / .dockerignore
├── Dockerfile                       # Container-Image (Python 3.11 + JDK 17)
├── docker-compose.yml               # Dienste: pipeline (Komplett-Lauf) + scheduler (dauerhaft)
│
├── src/
│   ├── db.py                        # Zentraler Impala-Verbindungs-Helfer (impyla + .env)
│   ├── create_datamodel.py          # DELIVERABLE 1: DDLs Star-Schema (4 Dim + 5 Fakten), idempotent
│   ├── run_pipeline.py              # DELIVERABLE 2: Orchestrator – Datenmodell + alle 3 Stufen + Contract-Gate
│   ├── pipeline_default_to_staging.py   # Stufe 1: Rohdaten → Staging (inkrementell)
│   ├── pipeline_staging_to_audit.py     # Stufe 2: Staging → Audit (Bereinigung, inkrementell)
│   ├── pipeline_audit_to_target.py      # Stufe 3: Audit → Datenmodell (Spark, mit Skip-Check)
│   ├── contract_check.py            # DELIVERABLE 3b: Data Contract als technisches Publish-Gate
│   ├── etl_state.py                 # Incremental-Loading-Zustand (Iceberg-Upsert) + Iceberg-Helfer
│   ├── scheduler.py                 # Täglicher Batch-Lauf um 00:00 (APScheduler)
│   └── utils/
│       ├── inspect_tables.py        # Zeigt Schema + Zeilenzahl der Rohtabellen
│       ├── reset_database.py        # Löscht ALLE gruppe3-Tabellen (Reset für End-to-End-Tests, fragt nach)
│       ├── german_cities.txt        # Referenzliste Städte/Orte (für Encoding-Auflösung in Stufe 2)
│       ├── german_regions.txt       # Referenzliste Landkreise + kreisfreie Städte
│       ├── german_states.txt        # Referenzliste der 16 Bundesländer
│       └── ImpalaJDBC42.jar         # JDBC-Treiber für Spark (lokal bereitzustellen, s. Einrichtung)
│
├── cde/
│   ├── pipeline_dag.py              # Airflow-DAG für Cloudera Data Engineering (5 Stufen als Tasks)
│   ├── requirements-cde.txt         # Python-Env für CDE-Jobs (ohne pyspark/APScheduler)
│   └── README.md                    # Deploy-Anleitung CDE (Resources, Jobs, DAG, Verifikation)
│
├── data/
│   ├── data_contract.yaml           # DELIVERABLE 3: Data Contract (Schema, Nutzung, Qualität)
│   └── output_port_ddl.sql          # SQL-Basis für datacontract import sql
│
└── docs/
    ├── ADR.md                       # ALLE Architektur-Entscheidungen (ADR-1..16) + abgelöste
    ├── Probleme.md                  # ALLE Probleme: gelöst / nicht behebbar / offen
    └── Portfolioprüfung.pdf         # Aufgabenstellung
```

## Wie liest man den Code?

Empfohlene Lese-Reihenfolge (jede Datei erklärt ihre Entscheidungen selbst im
Modul-Docstring am Dateianfang):

1. **[src/db.py](src/db.py)** – wie die Verbindung zu Impala aufgebaut wird (alle
   Skripte nutzen diesen einen Helfer; Zugangsdaten kommen aus `.env`).
2. **[src/run_pipeline.py](src/run_pipeline.py)** – der rote Faden: ruft
   Datenmodell + die drei Stufen + Contract-Gate in der richtigen Reihenfolge auf.
3. **[src/create_datamodel.py](src/create_datamodel.py)** – die Zieltabellen mit
   `COMMENT` an jeder Spalte. Warum das Modell so aussieht: s. unten
   [Datenmodell & Begründung](#datenmodell--begründung).
4. **[src/pipeline_default_to_staging.py](src/pipeline_default_to_staging.py)** –
   Stufe 1: exakte Kopie (Schema per Iceberg-CTAS `WHERE 1=0`), inkl. der
   Entscheidung, welche Tabelle Wasserzeichen bekommt und welche Prüfsumme.
5. **[src/etl_state.py](src/etl_state.py)** – das „Gedächtnis" der Pipeline:
   der Iceberg-Upsert-State (ein Eintrag je Stufe+Tabelle, Historie über
   Iceberg-Snapshots) und die gemeinsamen Iceberg-Helfer aller Stufen.
6. **[src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py)** –
   Stufe 2, die Datenbereinigung. Hier steckt die Lösung des Encoding-Problems
   (Laufzeit-Erkennung kaputter Kreisnamen + automatische Auflösung über die
   Referenzlisten in `src/utils/`, Transliteration für intakte Umlaute,
   Koordinaten-Normalisierung) und der zeilengenaue Key-Merge per Iceberg
   `MERGE INTO`/`DELETE` (Hintergrund: [ADR-13](docs/ADR.md)).
7. **[src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)** –
   Stufe 3 (Spark): pro Zieltabelle eine `build_…()`-Funktion, `main()` führt
   sie in Abhängigkeits-Reihenfolge aus (erst Dimensionen, dann Basis-Fakten,
   zuletzt der aggregierte KPI-Fakt).
8. **[data/data_contract.yaml](data/data_contract.yaml)** – der eigentliche
   Data Contract für den Output Port, kompatibel mit der Data Contract CLI.
   **[data/output_port_ddl.sql](data/output_port_ddl.sql)** ist die SQL-Basis,
   aus der der Contract per CLI generiert bzw. abgeglichen werden kann.
9. **[src/contract_check.py](src/contract_check.py)** – zusätzliches lokales
   Publish-Gate: prüft denselben Contract live gegen Impala und bricht bei
   Verstoß mit Exit-Code 1 ab.
10. **[src/scheduler.py](src/scheduler.py)** – täglicher Batch-Trigger um 00:00.

## Datenmodell & Begründung

### Ausgangslage: vier Quelltabellen

| Quelle (`default.*`) | Ebene | Verknüpfungs-Schlüssel | Format / Besonderheit |
|---|---|---|---|
| `project_bevoelkerungzahlen` | Kreis | `id` (Regionalschlüssel) | **breit** (92 Spalten: 3 je Jahr 1995–2024); kaputte Umlaute (`�`) |
| `project_bauland` | Kreis | `kreis_id` (Regionalschlüssel) | **lang** (1 Zeile je Merkmal, 21.600 Zeilen); kaputte Umlaute; 4. Merkmal ab Quelle zerstört |
| `project_gemeinden` | Gemeinde | nur Name (kein Schlüssel), `latitude`/`longitude` | CSV-Parsing-Schäden, 10.950 Zeilen |
| `project_klimadaten` | Stadt (weltweit) | nur Stadtname, `latitude`/`longitude` | 8,6 Mio. Zeilen, endet 2013; Kompass-Koordinaten („53.84N") |

**Zentrale Erkenntnis 1:** `bevoelkerung.id` und `bauland.kreis_id` sind derselbe
**amtliche Regionalschlüssel** (z. B. `01001` = Flensburg). Sie matchen exakt
(472 Kreise, live geprüft) und sind hierarchisch: die ersten 2 Stellen kodieren
das Bundesland (`01` = Schleswig-Holstein). → Der natürliche
Integrationsschlüssel auf Kreis-Ebene.

**Zentrale Erkenntnis 2:** `project_gemeinden` und `project_klimadaten` lassen
sich über die Gemeinde-Ebene verbinden: Nach der Bereinigung in Stufe 2
(Transliteration ä→ae/…, englische Exonyme „Munich"→„Muenchen" gemappt) liegen
Gemeindenamen und Klimastadt-Namen im **selben Format** vor → **exakter
Namens-Match** (deterministisch, trifft 74 von 81 deutschen Klimastädten).
`dim_gemeinde` wird damit zur **Brücken-Dimension**, die Kreis-Ebene
(Bevölkerung/Bauland) und Stadt-Ebene (Klima) verbindet — alle vier Quellen
sind in **einem** Modell nutzbar, nicht nur isoliert nebeneinander.

### Gewähltes Modell: Star-/Galaxy-Schema (4 Dimensionen + 5 Fakten)

```
dim_kreis ──< fact_bevoelkerung   >── dim_jahr
dim_kreis ──< fact_bauland        >── dim_jahr
dim_kreis ──< dim_gemeinde ──< fact_gemeinde_stamm
dim_gemeinde ──(Namens-Match, transliteriert)──> dim_klimastadt ──< fact_klima >── dim_jahr
dim_kreis ──< fact_standortprofil_kpi >── dim_jahr   (verdichtet alle o.g. Fakten)
```

Genau genommen ein **Galaxy-Schema**: mehrere Faktentabellen teilen sich die
Dimensionen `dim_kreis` und `dim_jahr` (conformed dimensions); jede
Faktentabelle + ihre Dimensionen = ein Stern.

### Warum so? (die Begründung)

1. **Star-Schema statt normalisiert (3. NF).** Analytische (OLAP-)Systeme lesen
   Aggregate über viele Zeilen; normalisierte Modelle erzwingen viele Joins →
   komplexe, langsame, in der Cloud teure Abfragen. Die Dimensionen sind
   **denormalisiert** (z. B. Bundesland direkt in `dim_kreis`).
2. **Regionalschlüssel als conformed dimension.** `dim_kreis` verbindet
   `fact_bevoelkerung` und `fact_bauland` exakt — Data-Mesh-Gedanke: Mehrwert
   durch *Interconnecting*.
3. **`dim_gemeinde` als zweite conformed dimension (Brücke).** Ohne sie bliebe
   das Klima isoliert (kein Regionalschlüssel): Namens-Match gegen
   `dim_kreis.kreis_name` (Kreis-Anbindung) und gegen
   `dim_klimastadt.stadt_name` (Klima-Anbindung).
4. **Unpivot der Bevölkerungsdaten (breit → lang).** Aus 1 Spalte pro Jahr wird
   1 Zeile je Kreis+Jahr — `jahr` wird echte Dimension, Zeitreihen-Analysen
   trivial. In Spark per `explode(array(struct(…)))`.
5. **Pivot der Baulanddaten (lang → breit).** Aus 1 Zeile je Merkmal wird
   1 Spalte je Merkmal (alle 4: Fälle, Fläche, Kaufsumme, amtl. Kaufwert) —
   genau **eine** Faktenzeile je Kreis+Jahr. In Spark per `DataFrame.pivot()`.
6. **KPI-Spalten direkt in den Basisfakten**, wo sie aus derselben Zeile
   berechenbar sind (z. B. `preis_pro_qm_eur`) — statt bei jeder Abfrage neu.
7. **`fact_standortprofil_kpi` als aggregierter Cross-Table-Fakt.** Kennzahlen,
   die den Join mehrerer Fakten erfordern (`wohnraumdruck_index`,
   `standortattraktivitaets_score`), werden einmal in der Pipeline vorberechnet
   und dashboard-fertig auf Kreis × Jahr gespeichert.
8. **Parquet** (spaltenorientiert) für alle Zieltabellen → ideal für OLAP
   („data skipping", nur benötigte Spalten werden gelesen).
9. **Bewusste Abgrenzung:** Die Namens-Matches sind fehlerbehaftet und **ehrlich
   im Data Contract dokumentiert** (97,5 % Kreis-Coverage, 74/81 Klimastädte) —
   Data-Mesh-Prinzip „Federated Governance": Qualität beschreiben statt verschweigen.

### Idempotenz

Alle DDLs nutzen `CREATE TABLE IF NOT EXISTS`; die Befüllung ist inkrementell
(Wasserzeichen-Append, Key-Merge bzw. `INSERT OVERWRITE` nur bei echter
Änderung). Mehrfaches Ausführen erzeugt keine Duplikate — ein zweiter Lauf
direkt nach dem ersten meldet überall „übersprungen" (End-to-End verifiziert,
s. [Stand](#stand-verifiziert-09072026)).

## Einrichtung (einmalig)

```bash
# 1. Virtuelle Umgebung + Abhängigkeiten
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt

# 2. Zugangsdaten: .env.example nach .env kopieren und ausfüllen
#    (Workload-Username & -Passwort aus dem Cloudera-Portal)
```

**Optionale Konfiguration** (Standard reicht für die Abgabe): Die Ziel-Datenbank,
das Tabellen-Präfix und die Quell-Datenbank sind über die `.env` überschreibbar
(`DATABASE`=`gruppe3`, `PREFIX`=`gruppe3_`, `SOURCE_DATABASE`=`default`, s.
[ADR-14](docs/ADR.md)). Ausnahme: `utils/reset_database.py` ist bewusst fest auf
`gruppe3` verdrahtet (Schutz gegen versehentliches `DROP TABLE` in einer
fremden DB).

**Zusätzlich für Stufe 3 / `run_pipeline.py` / Scheduler (Spark):**

- **JDK 17** installieren (genau Version 17 — neuere JDKs ≥ 23/24 entfernen
  Security-APIs, die Sparks Hadoop noch braucht, s. [Probleme.md](docs/Probleme.md));
  Pfad in `.env` als `JAVA_HOME_JDK17` eintragen. Die Skripte setzen
  `JAVA_HOME` daraus selbst ([ADR-16](docs/ADR.md)).
- **`src/utils/ImpalaJDBC42.jar`**: proprietärer Cloudera-Treiber, deshalb
  **nicht** eingecheckt (`.gitignore`) — die Datei muss lokal unter
  `src/utils/` liegen (aus dem Cloudera-Portal laden bzw. im Team weitergeben).

Die Stufen 1+2 einzeln und alle `utils/`-Skripte brauchen **kein** Java —
nur Python + `.env`.

**Einmalig bei bestehenden Tabellen aus einem älteren (Parquet-)Stand:**
`.venv/Scripts/python.exe src/utils/migrate_to_iceberg.py` migriert alle
`gruppe3`-Tabellen verlustfrei zu Iceberg (Zeilenzahl-Verifikation vor jedem
Tausch, idempotent — bereits am 10.07.2026 gegen die echte Gruppen-Datenbank
ausgeführt). Die Pipeline bricht mit einer klaren Fehlermeldung ab, falls sie
auf eine noch nicht migrierte Tabelle trifft. Bei einer frisch zurückgesetzten
Datenbank ist keine Migration nötig — alle Tabellen werden direkt als Iceberg
angelegt.

## Zwei Betriebsarten: lokal (Docker) und Cloudera Data Engineering

Die Pipeline läuft mit **demselben Code unter `src/`** in zwei Umgebungen:

- **Lokal / Docker** (dieser README-Rest): `docker compose run --rm pipeline`
  bzw. die `.venv`-Aufrufe unten. Spark läuft außerhalb des Clusters und
  erreicht die Daten über den Impala-JDBC-Endpoint
  (`SPARK_IO_MODE=jdbc`, Default — braucht das JDBC-Jar, s. Einrichtung).
- **CDE / Airflow** (produktiver Scheduler): Ein Airflow-DAG orchestriert die
  fünf Stufen als CDE-Jobs; Stufe 3 liest/schreibt Iceberg dort **nativ**
  über den Katalog (`SPARK_IO_MODE=catalog` — kein JDBC-Jar, kein
  collect()-Umweg). Setup und Verifikation: [cde/README.md](cde/README.md).

Beide Welten teilen sich `gruppe3_etl_state` (Fingerprint-Skip funktioniert
über die Grenze hinweg); nur nicht beide **Scheduler** gleichzeitig betreiben.

## Benutzung

```bash
# KOMPLETTER LAUF (empfohlen): Datenmodell + Stufe 1 → 2 → 3 + Contract-Gate
.venv/Scripts/python.exe src/run_pipeline.py
```

Alle Schritte sind **idempotent**: beliebig oft ausführbar, keine Duplikate.
Unveränderte Quellen werden erkannt und übersprungen — ein zweiter Lauf direkt
nach dem ersten tut fast nichts (verifiziert, s. [Stand](#stand-verifiziert-09072026)).

Die Stufen lassen sich auch einzeln ausführen (gleiche Reihenfolge):

```bash
.venv/Scripts/python.exe src/create_datamodel.py                # Zieltabellen anlegen
.venv/Scripts/python.exe src/pipeline_default_to_staging.py     # Stufe 1
.venv/Scripts/python.exe src/pipeline_staging_to_audit.py       # Stufe 2
.venv/Scripts/python.exe src/pipeline_audit_to_target.py        # Stufe 3 (braucht JDK 17 + Treiber)
.venv/Scripts/python.exe src/contract_check.py                  # Data-Contract-Gate gegen das Datenprodukt

# Target-Rebuild erzwingen, wenn sich nur Code/Contract geändert hat, aber keine Audit-Daten
$env:FORCE_TARGET_BUILD="1"; .venv/Scripts/python.exe src/pipeline_audit_to_target.py; Remove-Item Env:FORCE_TARGET_BUILD

# Täglicher Lauf um 00:00 – läuft dauerhaft
.venv/Scripts/python.exe src/scheduler.py

# Kompletter Reset der gruppe3-Tabellen (z.B. um Full Load vs. Skip zu testen)
.venv/Scripts/python.exe src/utils/reset_database.py
```

Danach ist das Datenprodukt abfragbar. Beispiel-Queries, Spaltenbedeutungen
und NULL-Semantik stehen im **[Data Contract](data/data_contract.yaml)**.
Der letzte Schritt des Komplettlaufs erzwingt diesen Contract technisch:
`src/contract_check.py` liest das YAML als Quelle der Wahrheit und prüft das
veröffentlichte Datenprodukt live in Impala (aktuell 32 Checks).

### Data Contract CLI

Der benotete Data Contract ist `data/data_contract.yaml`. Die zusätzliche Datei
`data/output_port_ddl.sql` dokumentiert das physische Output-Port-Schema und
kann mit der Data Contract CLI als Generierungsbasis genutzt werden:

```bash
python -m pip install "datacontract-cli[impala]" packaging

# Syntax/Schema des Contracts prüfen
$env:PYTHONIOENCODING="utf-8"; datacontract lint data/data_contract.yaml

# Contract aus den SQL-DDLs neu generieren/gegenprüfen
datacontract import sql --source data/output_port_ddl.sql --dialect spark --output data/data_contract.generated.yaml

# Contract gegen den Impala-Output-Port testen
# Vorher in .env pflegen: DATACONTRACT_IMPALA_USERNAME, DATACONTRACT_IMPALA_PASSWORD,
# DATACONTRACT_IMPALA_USE_SSL sowie im Contract host/port/database.
$env:PYTHONIOENCODING="utf-8"; datacontract test --server production data/data_contract.yaml
```

Falls `datacontract test` in der DHBW-Umgebung mit `TSocket read 0 bytes`
abbricht, liegt das am Impala-Transport der CLI: die Pipeline nutzt
`IMPALA_HTTP_PATH`/HTTP-Transport über `impyla`, der CLI-Adapter dagegen nur
`host`, `port`, `database`, Username und Passwort. In diesem Fall bleiben
`datacontract lint` und `datacontract import sql` gültig; das Live-Gate gegen
den konkreten DHBW-Output-Port läuft über `src/contract_check.py` — kein
Ersatz für die CLI, sondern ein zusätzliches Pipeline-Gate, weil die Pipeline
bereits über `impyla` gegen dieselbe Impala-Umgebung läuft.

### Handgeschriebener Contract vs. CLI-generiertes Gerüst

Zur Einordnung, was die CLI beiträgt (und was nicht), wurde der Contract am
09.07.2026 testweise per `datacontract import sql` (CLI 1.0.10) aus
`data/output_port_ddl.sql` neu generiert. Ergebnis: **das Schema stimmt 1:1
mit unserem Contract überein** (9 Tabellen, alle Spalten und Typen) — mehr
kann der Import aber prinzipbedingt nicht liefern, denn mehr steht nicht in
einer DDL. Dieselbe Spalte im direkten Vergleich:

```yaml
# CLI-generiert (nur, was aus der DDL ableitbar ist):
- name: kreis_id
  physicalType: STRING
  logicalType: string
```

```yaml
# data/data_contract.yaml (fachlich angereichert):
kreis_id:
  type: string
  required: true      # nie NULL
  unique: true        # keine Duplikate
  primaryKey: true
  description: Amtlicher Regionalschlüssel, 5-stellig, z.B. '01001' (Flensburg).
```

| Bestandteil | CLI-Import | Manuell ergänzt |
|---|---|---|
| Tabellen, Spalten, Typen | ✓ automatisch, tippfehlerfrei aus der DDL | – |
| Constraints (`required`/`primaryKey`/`unique`) | – | ✓ |
| Spalten-Semantik inkl. Warnungen (z. B. `kaufwert_je_qm_eur`: „NICHT VERWENDEN") | – | ✓ |
| `terms` (Join-Regeln, Transliteration, Surrogat-Warnung) | – | ✓ |
| `servers` (erst damit ist `datacontract test` möglich) | – | ✓ |
| `quality` (ausführbare SQLs + live gemessene Zahlen) | – | ✓ |
| `examples`, `servicelevels`, Owner/Kontakt | – | ✓ |

**Was der CLI-Workflow bringt:** (1) das mechanische Gerüst entsteht in
Sekunden und ist per Konstruktion fehlerfrei, weil die DDL die Quelle der
Wahrheit ist; (2) **Drift-Erkennung** — nach einer Schema-Änderung neu
generieren und gegen den gepflegten Contract diffen, statt still zu
veralten; (3) das Gerüst ist ab der ersten Zeile spezifikationskonform.
Die Arbeitsteilung ist genau der empfohlene Weg „automatisiert erstellen,
manuell ergänzen": die CLI liefert das *Was* (Schema), das Team den
fachlichen Gehalt (*Wie nutzt man es korrekt, was ist garantiert, was ist
kaputt*) — unser Contract entspricht dem Endzustand dieses Workflows.

Hinweis: Die CLI exportiert beim Import standardmäßig das **ODCS**-Format
(Open Data Contract Standard, `kind: DataContract`/`schema:`) — der zweite
große Contract-Standard neben der hier genutzten **Data Contract
Specification 1.1.0** (`models:`/`fields:`). Beide Standards konvergieren;
`datacontract lint`/`test` verstehen beide.

## Docker

Der Container enthält Python, PySpark und JDK 17; die Impala-Datenbank bleibt
extern (DHBW), Zugangsdaten kommen zur Laufzeit aus `.env` (nicht im Image).

```bash
docker compose build

# Kompletter Pipeline-Lauf (run_pipeline.py, alle Stufen):
docker compose run --rm pipeline

# Dauerhafter Scheduler:
docker compose up scheduler
```

**Wichtig — immer nur *einen* Dienst starten:** kein nacktes `docker compose up`
(würde `pipeline` und `scheduler` gleichzeitig starten; beide schreiben in
dieselben Tabellen und kämen sich ins Gehege).

## Datenbank `gruppe3` auf Impala

**Quellen (Datenbank `default`, nur lesend):**

| Tabelle | Inhalt | Besonderheit |
|---|---|---|
| `default.project_gemeinden` | 10.950 Gemeinden: Name, Kreis, Fläche, Einwohner, Koordinaten | CSV-Parsing-Schäden, kein amtlicher Schlüssel |
| `default.project_bauland` | Baulandverkäufe je Kreis/Jahr/Merkmal (21.600 Zeilen, Langformat) | kaputte Umlaute (�), 4. Merkmal beim Import zerstört |
| `default.project_klimadaten` | Temperaturen je Stadt/Monat, weltweit (8,6 Mio. Zeilen) | endet 2013; Kompass-Koordinaten („53.84N") |
| `default.project_bevoelkerungzahlen` | Einwohner je Kreis, Breitformat (92 Spalten: 3 je Jahr 1995–2024) | kaputte Umlaute (�) |

**In `gruppe3` erzeugt die Pipeline vier Schichten:**

| Schicht | Tabellen | Zweck |
|---|---|---|
| Staging | `gruppe3_staging_{gemeinden,bauland,klimadaten,bevoelkerungzahlen}` | unveränderte Rohkopie |
| Audit | `gruppe3_audit_{gemeinden,bauland,klimadaten,bevoelkerungzahlen}` | bereinigte Basis |
| Datenprodukt | 4 × `gruppe3_dim_*`, 5 × `gruppe3_fact_*` | Star-Schema für Konsumenten |
| ETL-Metadaten | `gruppe3_etl_state`, 9 × `*_wap_incoming` (leere Publish-Shadow-Tabellen) | Incremental-Loading-Zustand + atomarer Publish (kein Konsumenten-Interface) |

Alle Tabellen sind **Apache-Iceberg-Tabellen** (`format-version` 2); die
Klimadaten-Tabellen sind per Iceberg-Transform `TRUNCATE(4, dt)` nach Jahr
partitioniert. Jeder Schreibvorgang ist ein Iceberg-Snapshot — `DESCRIBE
HISTORY <tabelle>` zeigt die Historie, `SELECT … FOR SYSTEM_TIME AS OF …`
liest einen früheren Stand (Time Travel).

**Das Datenprodukt** (live verifiziert 09.07.2026):

| Dimensionen | Fakten |
|---|---|
| `gruppe3_dim_kreis` (472) | `gruppe3_fact_bevoelkerung` (14.110, 1995–2024) |
| `gruppe3_dim_jahr` (30, lückenlos 1995–2024) | `gruppe3_fact_bauland` (4.720, 2015–2024) |
| `gruppe3_dim_gemeinde` (10.947, Brücke Kreis↔Stadt) | `gruppe3_fact_klima` (1.539, 1995–2013) |
| `gruppe3_dim_klimastadt` (81) | `gruppe3_fact_gemeinde_stamm` (10.947) |
|  | `gruppe3_fact_standortprofil_kpi` (4.099, 2015–2024) — die dashboard-fertigen Cross-Table-KPIs |

Gefüllte Werte in `fact_standortprofil_kpi` (von 4.099): `wohnraumdruck_index`
3.914 · `baulandpreis_pro_kopf_eur` 3.929 · `freiflaeche_pro_einwohner_qm`
3.929 · `klima_angepasstes_wohnraumrisiko` 3.914 · `verstaedterung_index`
3.430 · `standortattraktivitaets_score` 3.911. NULL bedeutet immer: mindestens
eine Eingangsgröße fehlt (amtlich unterdrückt / kein Vorjahr / kein
Klima-Match) — dokumentierte Aussage, kein Fehler.

Schema, Nutzungsregeln, gemessene Qualität und Beispiel-Queries:
**[data/data_contract.yaml](data/data_contract.yaml)**.

## Datenprodukt konsumieren (Output Port)

Der **Output Port** sind genau die 9 `gruppe3_dim_*`/`gruppe3_fact_*`-Tabellen
(nicht staging/audit) — beschrieben durch den [Data Contract](data/data_contract.yaml).
Konsumenten greifen darauf über denselben Impala-Endpoint zu wie die Pipeline
(LDAP + HTTP-Transport + SSL, Port 443).

- **SQL / Notebook:** direkt per `impyla`/JDBC gegen `gruppe3.*` (Verbindung wie
  in [src/db.py](src/db.py)).
- **BI-Tools (z. B. Power BI):** über den **Cloudera ODBC Driver for Impala**
  (der native Power-BI-Impala-Connector kann den HTTP-Transport/HTTP-Path des
  Knox-Gateways meist nicht). ODBC-DSN-Werte 1:1 aus `.env`: Host, Port 443,
  Auth = „User Name and Password" (LDAP), Transport = HTTP, HTTP Path =
  `IMPALA_HTTP_PATH`, SSL = an. Import-Modus (nicht DirectQuery, da die
  Zielfakten klein sind und der Cluster aus dem Ruhezustand „aufwacht"). Das
  Star-Schema mappt direkt: Fakten (*:1) an `dim_kreis`/`dim_jahr`, Klima an
  `dim_klimastadt`, Gemeinde-Stamm an `dim_gemeinde`.

> Reines **Power BI Online** (Browser) kann private Impala-Quellen nicht direkt
> anbinden — dafür wird **Power BI Desktop** (kostenlos) zum Bauen des Modells
> gebraucht, danach „Veröffentlichen"; automatische Aktualisierung im Dienst
> erfordert zusätzlich ein **On-premises Data Gateway**.

**Wichtigste Nutzungsregeln** (vollständig im Contract):

- Joins/Filter auf Kreis-Ebene **nur über `kreis_id`** — `kreis_name` ist
  mehrdeutig (36 Namen mehrfach, z. B. „Leipzig" 3× für Stadt/Landkreis/Alt-Kreis).
- Alle Textwerte sind **ASCII-transliteriert**: mit `'Muenchen'` filtern, nicht `'München'`.
- `gemeinde_id` ist ein Surrogat und wird je Pipeline-Lauf **neu vergeben** —
  nie extern persistieren.
- `kaufwert_je_qm_eur` **nicht verwenden** (ab Quelle zerstört, s.
  [Probleme.md](docs/Probleme.md)); `preis_pro_qm_eur` ist der berechnete Ersatz.
- `standortattraktivitaets_score` ist ein z-Score: nur **innerhalb** eines
  Jahres vergleichbar, negative Werte sind normal.

## Datenqualität: was bereinigt wurde (Kurzfassung)

- **Zerstörte Umlaute** (`L�beck`) in Bauland/Bevölkerung: das Originalzeichen
  ist als U+FFFD unwiederbringlich verloren → Stufe 2 **entdeckt** die
  betroffenen Kreisnamen zur Laufzeit und löst die korrekte Schreibweise
  automatisch über drei Referenzlisten auf; nur 8 Sonderfälle bleiben manuell
  gepflegt. Nach dem Lauf verifiziert: **0 verbleibende kaputte Zeichen** in
  allen Audit-Tabellen (zuletzt 09.07.2026).
- **Intakte Umlaute** (Gemeindenamen): echte Transliteration ä→ae/ö→oe/ü→ue,
  damit alle Namens-Joins dieselbe ASCII-Schreibweise verwenden.
- **Englische Städtenamen** der Klimadaten (Munich→Muenchen …) gemappt,
  **Kompass-Koordinaten** („5.63S" → „-5,63") normalisiert, Filter auf
  `country = 'Germany'`.
- **Nicht behebbar, daher im Data Contract dokumentiert:** amtlich unterdrückte
  Werte, Flächen-Rundung auf 0 (748 Zeilen), Klimadaten nur bis 2013, das beim
  Quellimport zerstörte Merkmal „Durchschnittlicher Kaufwert je qm". Alle
  Details: [Probleme.md](docs/Probleme.md).

## Prüfungswissen kompakt

**Data Mesh (Theorie):** kein Werkzeug, sondern ein Organisations-/
Architektur-Konzept mit **4 Prinzipien**: Domain Ownership · Data as a Product ·
Self-Serve Data Platform · Federated Governance. Im Projekt konkret:
`fact_standortprofil_kpi` + Data Contract = „Data as a Product"; ehrlich
gemessene, dokumentierte Qualität = „Federated Governance"; die
Cloudera-Plattform mit Gruppen-Datenbanken = „Self-Serve Platform".

**OLTP vs. OLAP:** OLTP = operativ (schreiben, normalisiert/3. NF); OLAP =
analytisch (lesen/aggregieren, denormalisiert). Die Pipeline kopiert vom
operativen ins analytische System; Star-Schema + Parquet sind die
OLAP-Konsequenz.

**Die 3 kniffligen Transformationen:**
- **Unpivot** (Bevölkerung breit→lang): `explode(array(struct(…)))` — 1 Spalte
  pro Jahr → 1 Zeile pro Jahr.
- **Pivot** (Bauland lang→breit): `DataFrame.pivot()` — 1 Zeile pro Merkmal →
  1 Spalte pro Merkmal.
- **z-Score** (Standort-Score): `(Wert − AVG über Jahr) / STDDEV über Jahr` als
  Window-Funktion — macht %, €/m² und °C vergleichbar, bevor sie zu einem
  Score verrechnet werden. `STDDEV() OVER` kann Impala-SQL nicht → Spark.

**Werkzeug-Rollen:** Impala = die Datenbank (führt SQL aus, hält die Tabellen);
impyla = Python-Draht zu Impala (DDL + Schreiben); Spark/PySpark = die
Verarbeitungs-Engine für die Transformationen (DataFrame-API statt SQL-Strings —
gleichwertig, besser komponierbar). Ablauf: Spark liest per JDBC → rechnet →
impyla schreibt zurück (warum nicht `df.write.jdbc`: [Probleme.md](docs/Probleme.md)).

**Batch, nicht Streaming:** ein täglicher Lauf (APScheduler, 00:00
Europe/Berlin). Ehrliche Grenze: produktiv gehörte der Scheduler auf die
Plattform (Cloudera Data Engineering / Airflow) — Ausblick in der Präsentation.

**Kür umgesetzt:** Apache **Iceberg** als Open Table Format für die beiden
Audit-Tabellen mit echtem row-level `MERGE INTO`/`DELETE` ([ADR-13](docs/ADR.md)).

## Quellen der Referenzlisten

Die Referenzlisten für die Encoding-Auflösung in Stufe 2 (`src/utils/`):

- Deutsche Kreise und Bundesländer: <https://gist.github.com/leonbeckert/8332153a233a89156ecdbb3905579904>
- Deutsche Städtenamen: <https://www.datenbörse.net/item/Liste_von_deutschen_Staedtenamen_.csv>

## Stand (verifiziert 09.07.2026)

- [x] Datenmodell (DDLs) + Begründung
- [x] Pipeline (3 Stufen, WAP, inkrementell, idempotent) + Orchestrator + Scheduler
- [x] Data Contract + technische Durchsetzung als Publish-Gate
- [ ] Restpunkte unten

**Bereits live verifiziert (10.07.2026, gegen die echte DHBW-Datenbank):**
die einmalige Iceberg-Migration aller 20 Tabellen (Zeilenzahlen identisch),
Stufe 1+2 mit Skip-Verhalten (unveränderte Quellen → alle Schritte
übersprungen), der erzwungene Änderungspfad (DELETE+MERGE für
Bauland/Bevölkerung, Full Refresh für Gemeinden — Inhalts-Prüfsummen vor/nach
identisch, zweiter Lauf skippt wieder) sowie `contract_check.py` mit 32/32
Checks OK.

**Offene Arbeiten:**

1. **End-to-End-Abnahmetest inkl. Stufe 3:** auf einem Rechner mit JDK 17 +
   `ImpalaJDBC42.jar`: `run_pipeline.py` komplett (inkl. Spark-Stufe gegen die
   jetzt Iceberg-basierten Audit-Tabellen; laut ADR.md liest der JDBC-Pfad
   Iceberg wie Parquet, nach dem Shadow-Swap-Umbau von `overwrite_table`
   aber erneut zu bestätigen). Danach optional: `utils/reset_database.py` →
   `run_pipeline.py` → zweiter Lauf muss überall „übersprungen" melden.
2. **Abgabe-Hygiene klären (Team):** bleiben `docs/coursematerial/` (13 MB
   Prof-Folien), `reference/` und `TODO.md` im Abgabe-Repo?
3. **Data-Contract-Härtung (optional):** weitere ausführbare `quality`-SQLs
   ergänzen, z.B. Row-Count-Minima, FK-Integrität und Composite-Key-Checks für
   `fact_bevoelkerung` und `fact_klima`. Der aktuelle Pflichtstand läuft und
   prüft bereits Schema, Required-Felder, einfache Eindeutigkeit und die
   vorhandenen SQL-Regeln.
4. **`dim_kreis.kreis_name` mehrdeutig** (Stadt/Landkreis-Zusatz wurde bei der
   Bereinigung entfernt, z. B. dreimal „Leipzig") → Zusatz-Spalte `kreis_typ`
   oder Originalname ergänzen; bis dahin gilt: Joins nur über `kreis_id`
   (im Data Contract dokumentiert).
5. Kosmetik: `WindowExec`-Warnung (Window ohne `PARTITION BY`, bei unserer
   Datenmenge unkritisch), log4j-`ClassCastException` des JDBC-Treibers
   (harmlos, s. [docs/spark_stolpersteine.md](docs/spark_stolpersteine.md)).

> **Warum ist alles so gebaut (und was galt früher)?** → **[ADR.md](docs/ADR.md)**
> **Was ist schiefgegangen und wie wurde es gelöst?** → **[Probleme.md](docs/Probleme.md)**
