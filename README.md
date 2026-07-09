# Data Mesh & Data Engineering – Portfolioprüfung (Gruppe 3)

Datenprodukt **„Standortprofil deutscher Kreise"** auf Basis von vier öffentlichen
Datensätzen (Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die
Rohdaten liegen fertig in einer **Cloudera CDP / Impala**-Datenbank der DHBW
Stuttgart (`default.project_*`, nur lesend); unser Datenprodukt entsteht in der
Gruppen-Datenbank **`gruppe3`**.

Die Pipeline folgt dem **Write-Audit-Publish-Pattern** (WAP, s. Vorlesung 3) in
drei Stufen und lädt **inkrementell** (Incremental-Loader-Pattern: Wasserzeichen,
zeilengenauer Key-Merge bzw. Inhalts-Prüfsummen — unveränderte Quellen werden
übersprungen). Die eigentlichen Transformationen (Unpivot, Pivot,
Window-Funktionen) laufen in **Apache Spark** (PySpark, DataFrame-API).

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
merkt sich die Pipeline in `gruppe3_etl_state` bzw. `gruppe3_etl_row_state`
(append-only, weil Impala auf Parquet-Tabellen kein UPDATE/MERGE kennt —
s. [src/etl_state.py](src/etl_state.py)). Drei Strategien, je nach Tabellenart:

- **Wasserzeichen** (Klimadaten, echte Zeitreihe über `dt`): nur neuere Zeilen
  werden angehängt — kein täglicher Full-Rewrite von 8,6 Mio. Zeilen.
- **Zeilengenauer Merge** (Bauland, Bevölkerung — Tabellen mit Business-Key):
  FNV-Hash je Zeile, nur neue/geänderte/gelöschte Keys werden ersetzt.
  Die beiden Audit-Tabellen laufen dafür seit 07.07. als
  **Apache-Iceberg-Tabellen** — echtes `DELETE` + `MERGE INTO` statt des
  früheren `CREATE TABLE … AS SELECT` + Rename-Swap (Parquet in Impala kennt
  kein UPDATE/MERGE). Begründung, Verifikation und Historie: [ADR.md](ADR.md).
- **Inhalts-Prüfsumme** (Gemeinden — kein verlässlicher Key): Full Refresh nur,
  wenn sich die Prüfsumme der Quelle geändert hat.

Die fachlichen Begründungen stehen ausführlich in den Modul-Docstrings der
jeweiligen Skripte.

## Projektstruktur

```
Data-Mesh/
├── README.md                        # Diese Datei (Setup, Architektur, Benutzung)
├── ADR.md                           # ALLE Architektur-Entscheidungen (ADR-1..16) + abgelöste + gelöste Probleme
├── TODO.md                          # Einzige Aufgabenliste (offene Punkte)
├── quellen.txt                      # Herkunft der Referenzlisten (Städte/Kreise)
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
│   ├── etl_state.py                 # Incremental-Loading-Zustand (Wasserzeichen, Zeilen-Hashes)
│   ├── scheduler.py                 # Täglicher Batch-Lauf um 00:00 (APScheduler)
│   └── utils/
│       ├── test_connection.py       # Prüft die Impala-Verbindung
│       ├── inspect_tables.py        # Zeigt Schema + Zeilenzahl der Rohtabellen
│       ├── reset_database.py        # Löscht ALLE gruppe3-Tabellen (Reset für End-to-End-Tests, fragt nach)
│       ├── migrate_audit_tables_to_iceberg.py  # EINMALIG: Audit-Tabellen Parquet → Iceberg (s. Einrichtung)
│       ├── german_cities.txt        # Referenzliste Städte/Orte (für Encoding-Auflösung in Stufe 2)
│       ├── german_regions.txt       # Referenzliste Landkreise + kreisfreie Städte
│       ├── german_states.txt        # Referenzliste der 16 Bundesländer
│       └── ImpalaJDBC42.jar         # JDBC-Treiber für Spark (lokal bereitzustellen, s. Einrichtung)
│
├── data/                            # Lokale CSV-Kopie zur Inspektion (nicht Teil der Pipeline)
├── reference/                       # Beispiel-Skripte aus der Vorlesung (nicht Teil der Abgabe)
└── docs/
    ├── Portfolioprüfung.pdf         # Aufgabenstellung
    ├── data_contract.yaml           # DELIVERABLE 3: Data Contract (Schema, Nutzung, Qualität)
    ├── output_port_ddl.sql          # SQL-Basis für datacontract import sql
    ├── datenmodell_begruendung.md   # DELIVERABLE 1: Begründung des Datenmodells
    ├── entscheidungen.md            # Ausführliche Projekt-Historie (ADR.md ist die kanonische Kurzfassung)
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
   Stufe 1: exakte Kopie (Schema per `CREATE TABLE … LIKE`), inkl. der
   Entscheidung, welche Tabelle Wasserzeichen bekommt und welche Prüfsumme.
5. **[src/etl_state.py](src/etl_state.py)** – das „Gedächtnis" der Pipeline:
   warum die State-Tabellen append-only sind und wie die Zeilen-Hashes
   funktionieren.
6. **[src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py)** –
   Stufe 2, die Datenbereinigung. Hier steckt die Lösung des Encoding-Problems
   (Laufzeit-Erkennung kaputter Kreisnamen + automatische Auflösung über die
   Referenzlisten in `src/utils/`, Transliteration für intakte Umlaute,
   Koordinaten-Normalisierung) und der zeilengenaue Key-Merge per Iceberg
   `MERGE INTO`/`DELETE` (Hintergrund: [ADR.md](ADR.md)).
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

**Optionale Konfiguration** (Standard reicht für die Abgabe): Die Ziel-Datenbank,
das Tabellen-Präfix und die Quell-Datenbank sind über die `.env` überschreibbar
(`DATABASE`=`gruppe3`, `PREFIX`=`gruppe3_`, `SOURCE_DATABASE`=`default`, s.
[ADR-14](ADR.md)). Ohne Eintrag bleibt alles wie beschrieben. Ausnahme:
`utils/reset_database.py` ist bewusst fest auf `gruppe3` verdrahtet (Schutz gegen
versehentliches `DROP TABLE` in einer fremden DB).

**Zusätzlich für Stufe 3 / `run_pipeline.py` / Scheduler (Spark):**

- **JDK 17** installieren (genau Version 17, s.
  [docs/spark_stolpersteine.md](docs/spark_stolpersteine.md)); Pfad optional in
  `.env` als `JAVA_HOME_JDK17` eintragen.
- **`src/utils/ImpalaJDBC42.jar`**: proprietärer Cloudera-Treiber, deshalb
  **nicht** eingecheckt (`.gitignore`) — die Datei muss lokal unter
  `src/utils/` liegen (aus dem Cloudera-Portal laden bzw. im Team weitergeben).

Die Stufen 1+2 einzeln und alle `utils/`-Skripte brauchen **kein** Java —
nur Python + `.env`.

**Einmalige Iceberg-Migration (nur falls die Audit-Tabellen noch als Parquet
existieren):** Seit 07.07. erwartet Stufe 2 die Tabellen
`gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen` als
Apache-Iceberg-Tabellen (s. [ADR.md](ADR.md)). Bestehende Parquet-Bestände
müssen einmalig migriert werden — sonst bricht `pipeline_staging_to_audit.py`
mit einer klaren Fehlermeldung ab:

```bash
.venv/Scripts/python.exe src/utils/migrate_audit_tables_to_iceberg.py
```

Das Skript ist idempotent (erkennt „bereits Iceberg" und tut dann nichts) und
verifiziert die Zeilenzahl vor dem Tausch. Die zentralen Gruppen-Tabellen sind
bereits migriert; der Schritt betrifft vor allem frische Test-/Reset-Umgebungen.

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
| Audit | `gruppe3_audit_{gemeinden,bauland,klimadaten,bevoelkerungzahlen}` | bereinigte Basis (`bauland`+`bevoelkerungzahlen` als Iceberg, Rest Parquet) |
| Datenprodukt | 4 × `gruppe3_dim_*`, 5 × `gruppe3_fact_*` | Star-Schema für Konsumenten |
| ETL-Metadaten | `gruppe3_etl_state`, `gruppe3_etl_row_state`, `gruppe3_etl_changed_keys_tmp` | Incremental-Loading-Zustand (kein Konsumenten-Interface) |

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

## Datenprodukt konsumieren (Output Port)

Der **Output Port** sind genau die 9 `gruppe3_dim_*`/`gruppe3_fact_*`-Tabellen
(nicht staging/audit) — beschrieben durch den [Data Contract](docs/data_contract.yaml).
Konsumenten greifen darauf über denselben Impala-Endpoint zu wie die Pipeline
(LDAP + HTTP-Transport + SSL, Port 443).

- **SQL / Notebook:** direkt per `impyla`/JDBC gegen `gruppe3.*` (Verbindung wie in [src/db.py](src/db.py)).
- **BI-Tools (z. B. Power BI):** über den **Cloudera ODBC Driver for Impala** (der
  native Power-BI-Impala-Connector kann den HTTP-Transport/HTTP-Path des Knox-Gateways
  meist nicht). ODBC-DSN-Werte 1:1 aus `.env`: Host, Port 443, Auth = „User Name and
  Password" (LDAP), Transport = HTTP, HTTP Path = `IMPALA_HTTP_PATH`, SSL = an. Import-Modus
  (nicht DirectQuery, da die Zielfakten klein sind und der Cluster aus dem Ruhezustand
  „aufwacht"). Das Star-Schema mappt direkt: Fakten (*:1) an `dim_kreis`/`dim_jahr`,
  Klima an `dim_klimastadt`, Gemeinde-Stamm an `dim_gemeinde`; Kreis-Joins nur über
  `kreis_id` (nicht `kreis_name`, mehrdeutig — s. Contract).

> Reines **Power BI Online** (Browser) kann private Impala-Quellen nicht direkt
> anbinden — dafür wird **Power BI Desktop** (kostenlos) zum Bauen des Modells
> gebraucht, danach „Veröffentlichen"; automatische Aktualisierung im Dienst
> erfordert zusätzlich ein **On-premises Data Gateway**.

## Datenqualität: was bereinigt wurde (Kurzfassung)

- **Zerstörte Umlaute** (`L�beck`) in Bauland/Bevölkerung: das Originalzeichen
  ist als U+FFFD unwiederbringlich verloren → Stufe 2 **entdeckt** die
  betroffenen Kreisnamen zur Laufzeit in den Staging-Tabellen und löst die
  korrekte Schreibweise automatisch über drei Referenzlisten auf
  (`src/utils/german_cities.txt`, `german_regions.txt`, `german_states.txt`,
  Herkunft s. [quellen.txt](quellen.txt)); nur 8 Sonderfälle ohne
  Listen-Eintrag (Berliner Bezirke, aufgelöste Altkreise) bleiben manuell
  gepflegt (`MANUAL_KREIS_CORRECTIONS`), dazu 2 Merkmalstexte. Nach dem Lauf
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

**Offene Arbeiten:**

1. **End-to-End-Abnahmetest:** `utils/reset_database.py` → `run_pipeline.py` →
   zweiter Lauf direkt danach muss überall „übersprungen" melden; dabei muss
   `contract_check.py` am Ende mit 0 Fehlern bestehen.
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

> **Warum ist alles so gebaut (und was galt früher)?** → **[ADR.md](ADR.md)** — alle Architektur-Entscheidungen (ADR-1..16), abgelöste Entscheidungen und gelöste Probleme an einem Ort.
> Ausführlichere Projekt-Historie (kann ggü. ADR.md veralten): [docs/entscheidungen.md](docs/entscheidungen.md)
> Hintergrund & Prüfungsvorbereitung: [docs/projekt_notizen.md](docs/projekt_notizen.md)
