# Data Mesh & Data Engineering – Portfolioprüfung (Gruppe 3)

Datenprodukt **„Standortprofil deutscher Kreise"** auf Basis von vier öffentlichen
Datensätzen (Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die
Rohdaten liegen fertig in einer **Cloudera CDP / Impala**-Datenbank der DHBW
Stuttgart (`default.project_*`, nur lesend); unser Datenprodukt entsteht in der
Gruppen-Datenbank **`gruppe3`**.

Die Pipeline folgt dem **Write-Audit-Publish-Pattern** (WAP, s. Vorlesung 3) in
drei Stufen und lädt **inkrementell** (Incremental-Loader-Pattern: Wasserzeichen,
zeilengenauer Key-Merge bzw. Inhalts-Prüfsummen — unveränderte Quellen werden
übersprungen). **Alle Tabellen aller Schichten (Staging, Audit, Datenprodukt,
ETL-State) sind Apache-Iceberg-Tabellen** — das liefert zeilengenaues
`MERGE INTO`/`DELETE`, atomare `INSERT OVERWRITE`-Snapshots (Konsumenten sehen
nie einen halb geschriebenen Stand), Time Travel und Jahres-Partitionierung
der Klimadaten (Details: [ADR.md](ADR.md)). Die eigentlichen Transformationen
(Unpivot, Pivot, Window-Funktionen) laufen in **Apache Spark** (PySpark,
DataFrame-API).

Die vier abgegebenen Arbeitsergebnisse:

| Deliverable | Wo |
|---|---|
| 1. DDLs + Begründung des Datenmodells | [src/create_datamodel.py](src/create_datamodel.py) + [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md) |
| 2. Pipeline-Code (3 Stufen + Orchestrator + Incremental-State) + Scheduler | [src/run_pipeline.py](src/run_pipeline.py), [src/pipeline_default_to_staging.py](src/pipeline_default_to_staging.py), [src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py), [src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py), [src/etl_state.py](src/etl_state.py), [src/scheduler.py](src/scheduler.py) |
| 3. Data Contract + technische Durchsetzung | [docs/data_contract.yaml](docs/data_contract.yaml), [docs/output_port_ddl.sql](docs/output_port_ddl.sql), [src/contract_check.py](src/contract_check.py) |
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
              Business-Key-Merge (Bauland/Bevölkerung) · Prüfsumme (Gemeinden)
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
              Der benotete Contract selbst ist CLI-kompatibel:
              docs/data_contract.yaml + docs/output_port_ddl.sql.
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
Iceberg-Snapshots — s. [src/etl_state.py](src/etl_state.py)). Drei Strategien,
je nach Tabellenart:

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
überspringt den kompletten Spark-Lauf, wenn sich seit dem letzten Ziel-Build
keine Audit-Tabelle geändert hat, und der Publish selbst ist ein atomarer
Shadow-Swap (s. `overwrite_table` in
[src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)).

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
│       ├── test_connection.py       # Prüft die Impala-Verbindung
│       ├── inspect_tables.py        # Zeigt Schema + Zeilenzahl der Rohtabellen
│       ├── reset_database.py        # Löscht ALLE gruppe3-Tabellen (Reset für End-to-End-Tests, fragt nach)
│       ├── migrate_to_iceberg.py    # Einmalige Migration Parquet → Iceberg (alle Schichten, verlustfrei)
│       └── ImpalaJDBC42.jar         # JDBC-Treiber für Spark (lokal bereitzustellen, s. Einrichtung)
│
├── data/                            # Lokale CSV-Kopie zur Inspektion (nicht Teil der Pipeline)
├── reference/                       # Beispiel-Skripte aus der Vorlesung (nicht Teil der Abgabe)
└── docs/
    ├── Portfolioprüfung.pdf         # Aufgabenstellung
    ├── data_contract.yaml           # DELIVERABLE 3: Data Contract (Schema, Nutzung, Qualität)
    ├── output_port_ddl.sql          # SQL-Basis für datacontract import sql
    ├── datenmodell_begruendung.md   # DELIVERABLE 1: Begründung des Datenmodells
    ├── entscheidungen.md            # Architektur-Entscheidungen (ADRs): aktiv + abgelöst + gelöste Probleme
    ├── spark_stolpersteine.md       # Spark-/JDBC-Probleme + Lösungen (Nachschlagewerk)
    ├── bugfix_score_nullwerte.md    # Fallstudie: warum eine KPI-Spalte komplett NULL war
    ├── scheduler_bug.md             # Fallstudie: APScheduler next_run_time
    ├── projekt_notizen.md           # Verständnis-/Prüfungsnotizen
    └── coursematerial/              # Foliensätze aus der Vorlesung
```

## Wie liest man den Code?

Empfohlene Lese-Reihenfolge (jede Datei erklärt ihre Entscheidungen selbst im
Modul-Docstring am Dateianfang; tiefergehende Analysen liegen in `docs/`):

1. **[src/db.py](src/db.py)** – wie die Verbindung zu Impala aufgebaut wird (alle
   Skripte nutzen diesen einen Helfer; Zugangsdaten kommen aus `.env`).
2. **[src/run_pipeline.py](src/run_pipeline.py)** – der rote Faden: ruft
   Datenmodell + die drei Stufen in der richtigen Reihenfolge auf.
3. **[src/create_datamodel.py](src/create_datamodel.py)** – die Zieltabellen mit
   `COMMENT` an jeder Spalte. Warum das Modell so aussieht:
   [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md).
4. **[src/pipeline_default_to_staging.py](src/pipeline_default_to_staging.py)** –
   Stufe 1: exakte Kopie (Schema per Iceberg-CTAS `WHERE 1=0`), inkl. der
   Entscheidung, welche Tabelle Wasserzeichen bekommt und welche Prüfsumme.
5. **[src/etl_state.py](src/etl_state.py)** – das „Gedächtnis" der Pipeline:
   der Iceberg-Upsert-State (ein Eintrag je Stufe+Tabelle, Historie über
   Iceberg-Snapshots) und die gemeinsamen Iceberg-Helfer aller Stufen.
6. **[src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py)** –
   Stufe 2, die Datenbereinigung. Hier steckt die Lösung des Encoding-Problems
   (Korrektur-Mappings für irreparabel zerstörte Umlaute, Transliteration für
   intakte, Koordinaten-Normalisierung) und der zeilengenaue Key-Merge.
7. **[src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)** –
   Stufe 3 (Spark): pro Zieltabelle eine `build_…()`-Funktion, `main()` führt
   sie in Abhängigkeits-Reihenfolge aus (erst Dimensionen, dann Basis-Fakten,
   zuletzt der aggregierte KPI-Fakt).
8. **[docs/data_contract.yaml](docs/data_contract.yaml)** – der eigentliche
   Data Contract für den Output Port, kompatibel mit der Data Contract CLI.
   **[docs/output_port_ddl.sql](docs/output_port_ddl.sql)** ist die SQL-Basis,
   aus der der Contract per CLI generiert bzw. abgeglichen werden kann.
9. **[src/contract_check.py](src/contract_check.py)** – zusätzliches lokales
   Publish-Gate: prüft denselben Contract live gegen Impala und bricht bei
   Verstoß mit Exit-Code 1 ab.
10. **[src/scheduler.py](src/scheduler.py)** – täglicher Batch-Trigger um 00:00.

## Einrichtung (einmalig)

```bash
# 1. Virtuelle Umgebung + Abhängigkeiten
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt

# 2. Zugangsdaten: .env.example nach .env kopieren und ausfüllen
#    (Workload-Username & -Passwort aus dem Cloudera-Portal)
```

**Zusätzlich für Stufe 3 / `run_pipeline.py` / Scheduler (Spark):**

- **JDK 17** installieren (genau Version 17, s.
  [docs/spark_stolpersteine.md](docs/spark_stolpersteine.md)); Pfad optional in
  `.env` als `JAVA_HOME_JDK17` eintragen.
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

## Benutzung

```bash
# Verbindung testen (optional)
.venv/Scripts/python.exe src/utils/test_connection.py

# KOMPLETTER LAUF (empfohlen): Datenmodell + Stufe 1 → 2 → 3 + Contract-Gate
.venv/Scripts/python.exe src/run_pipeline.py
```

Alle Schritte sind **idempotent**: beliebig oft ausführbar, keine Duplikate
(`CREATE TABLE IF NOT EXISTS`; Befüllung per Wasserzeichen-Append,
Key-Merge bzw. `INSERT OVERWRITE`). Unveränderte Quellen werden erkannt und
übersprungen — ein zweiter Lauf direkt nach dem ersten tut fast nichts.

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
und NULL-Semantik stehen im **[Data Contract](docs/data_contract.yaml)**.
Der letzte Schritt des Komplettlaufs erzwingt diesen Contract technisch:
`src/contract_check.py` liest das YAML als Quelle der Wahrheit und prüft das
veröffentlichte Datenprodukt live in Impala.

### Data Contract CLI

Der benotete Data Contract ist `docs/data_contract.yaml`. Die zusätzliche Datei
`docs/output_port_ddl.sql` dokumentiert das physische Output-Port-Schema und
kann mit der Data Contract CLI als Generierungsbasis genutzt werden:

```bash
python -m pip install "datacontract-cli[impala]" packaging

# Syntax/Schema des Contracts prüfen
$env:PYTHONIOENCODING="utf-8"; datacontract lint docs/data_contract.yaml

# Contract aus den SQL-DDLs neu generieren/gegenprüfen (Spark-Dialekt ist für STRING/INT/BIGINT/DOUBLE passend)
datacontract import sql --source docs/output_port_ddl.sql --dialect spark --output docs/data_contract.generated.yaml

# Contract gegen den Impala-Output-Port testen
# Vorher in .env pflegen: DATACONTRACT_IMPALA_USERNAME, DATACONTRACT_IMPALA_PASSWORD,
# DATACONTRACT_IMPALA_USE_SSL sowie im Contract host/port/database.
$env:PYTHONIOENCODING="utf-8"; datacontract test --server production docs/data_contract.yaml
```

Falls `datacontract test` in der DHBW-Umgebung mit `TSocket read 0 bytes`
abbricht, liegt das am Impala-Transport der CLI: die Pipeline nutzt
`IMPALA_HTTP_PATH`/HTTP-Transport über `impyla`, der getestete CLI-Adapter nutzt
dagegen nur `host`, `port`, `database`, Username und Passwort. In diesem Fall
bleiben `datacontract lint` und `datacontract import sql` gültig; der Live-Gate
gegen den konkreten DHBW-Output-Port läuft über `src/contract_check.py`.

Hinweis: `src/contract_check.py` ist kein Ersatz fuer die Data Contract CLI,
sondern ein zusätzlicher Pipeline-Gate, weil die Pipeline bereits über `impyla`
gegen dieselbe Impala-Umgebung läuft.

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

**Das Datenprodukt** (Zeilenzahlen Stand 06.07.2026):

| Dimensionen | Fakten |
|---|---|
| `gruppe3_dim_kreis` (472) | `gruppe3_fact_bevoelkerung` (14.110, 1995–2024) |
| `gruppe3_dim_jahr` (30) | `gruppe3_fact_bauland` (4.720, 2015–2024) |
| `gruppe3_dim_gemeinde` (10.947, Brücke Kreis↔Stadt) | `gruppe3_fact_klima` (1.539, 1995–2013) |
| `gruppe3_dim_klimastadt` (81) | `gruppe3_fact_gemeinde_stamm` (10.947) |
|  | `gruppe3_fact_standortprofil_kpi` (4.099) — die dashboard-fertigen Cross-Table-KPIs |

Schema, Nutzungsregeln, gemessene Qualität und Beispiel-Queries:
**[docs/data_contract.yaml](docs/data_contract.yaml)**.

## Datenqualität: was bereinigt wurde (Kurzfassung)

- **Zerstörte Umlaute** (`L�beck`) in Bauland/Bevölkerung: das Originalzeichen
  ist als U+FFFD unwiederbringlich verloren → Korrektur über explizite
  Mapping-Listen (~90 Kreise, 2 Merkmalstexte) in Stufe 2. Nach dem Lauf
  verifiziert: **0 verbleibende kaputte Zeichen** in allen Audit-Tabellen.
- **Intakte Umlaute** (Gemeindenamen): echte Transliteration ä→ae/ö→oe/ü→ue,
  damit alle Namens-Joins dieselbe ASCII-Schreibweise verwenden.
- **Englische Städtenamen** der Klimadaten (Munich→Muenchen …) gemappt,
  **Kompass-Koordinaten** („5.63S" → „-5,63") normalisiert.
- **Nicht behebbar, daher im Data Contract dokumentiert:** amtlich unterdrückte
  Werte, Flächen-Rundung auf 0, Klimadaten nur bis 2013, das beim Quellimport
  zerstörte Merkmal „Durchschnittlicher Kaufwert je qm".

## Stand / offene Punkte vor der Abgabe

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

> **Warum ist alles so gebaut (und was galt früher)?** → [docs/entscheidungen.md](docs/entscheidungen.md) (ADRs + Problem-Historie)
> Hintergrund & Prüfungsvorbereitung: [docs/projekt_notizen.md](docs/projekt_notizen.md)
