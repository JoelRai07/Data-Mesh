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
| 3. Data Contract | [docs/data_contract.yaml](docs/data_contract.yaml) |
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
```

**[src/run_pipeline.py](src/run_pipeline.py)** führt alles in fester Reihenfolge
aus (Datenmodell-DDLs + Stufen 1–3) und ist der eine Einstiegspunkt — auch im
Docker-Image.

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
  FNV-Hash je Zeile, nur neue/geänderte/gelöschte Keys werden ersetzt
  (per `CREATE TABLE … AS SELECT` + Rename-Swap, da Impala kein MERGE hat).
- **Inhalts-Prüfsumme** (Gemeinden — kein verlässlicher Key): Full Refresh nur,
  wenn sich die Prüfsumme der Quelle geändert hat.

Die fachlichen Begründungen stehen ausführlich in den Modul-Docstrings der
jeweiligen Skripte.

## Projektstruktur

```
Data-Mesh/
├── README.md                        # Diese Datei
├── requirements.txt                 # Python-Abhängigkeiten (impyla, pyspark, APScheduler, dotenv)
├── .env.example                     # Vorlage für Zugangsdaten → kopieren nach .env
├── .env                             # Echte Zugangsdaten (NICHT eingecheckt)
├── .gitignore / .dockerignore
├── Dockerfile                       # Container-Image (Python 3.11 + JDK 17)
├── docker-compose.yml               # Dienste: pipeline (Komplett-Lauf) + scheduler (dauerhaft)
│
├── src/
│   ├── db.py                        # Zentraler Impala-Verbindungs-Helfer (impyla + .env)
│   ├── create_datamodel.py          # DELIVERABLE 1: DDLs Star-Schema (4 Dim + 5 Fakten), idempotent
│   ├── run_pipeline.py              # DELIVERABLE 2: Orchestrator – Datenmodell + alle 3 Stufen in einem Lauf
│   ├── pipeline_default_to_staging.py   # Stufe 1: Rohdaten → Staging (inkrementell)
│   ├── pipeline_staging_to_audit.py     # Stufe 2: Staging → Audit (Bereinigung, inkrementell)
│   ├── pipeline_audit_to_target.py      # Stufe 3: Audit → Datenmodell (Spark, mit Skip-Check)
│   ├── etl_state.py                 # Incremental-Loading-Zustand (Wasserzeichen, Zeilen-Hashes)
│   ├── scheduler.py                 # Täglicher Batch-Lauf um 00:00 (APScheduler)
│   └── utils/
│       ├── test_connection.py       # Prüft die Impala-Verbindung
│       ├── inspect_tables.py        # Zeigt Schema + Zeilenzahl der Rohtabellen
│       ├── reset_database.py        # Löscht ALLE gruppe3-Tabellen (Reset für End-to-End-Tests, fragt nach)
│       └── ImpalaJDBC42.jar         # JDBC-Treiber für Spark (liegt im Repo, s. Einrichtung)
│
├── data/                            # Lokale CSV-Kopie zur Inspektion (nicht Teil der Pipeline)
├── reference/                       # Beispiel-Skripte aus der Vorlesung (nicht Teil der Abgabe)
└── docs/
    ├── Portfolioprüfung.pdf         # Aufgabenstellung
    ├── data_contract.yaml           # DELIVERABLE 3: Data Contract (Schema, Nutzung, Qualität)
    ├── datenmodell_begruendung.md   # DELIVERABLE 1: Begründung des Datenmodells
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
   (Korrektur-Mappings für irreparabel zerstörte Umlaute, Transliteration für
   intakte, Koordinaten-Normalisierung) und der zeilengenaue Key-Merge.
7. **[src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)** –
   Stufe 3 (Spark): pro Zieltabelle eine `build_…()`-Funktion, `main()` führt
   sie in Abhängigkeits-Reihenfolge aus (erst Dimensionen, dann Basis-Fakten,
   zuletzt der aggregierte KPI-Fakt).
8. **[src/scheduler.py](src/scheduler.py)** – täglicher Batch-Trigger um 00:00.

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
- **`src/utils/ImpalaJDBC42.jar`**: liegt aus Bequemlichkeit versioniert im
  Repo, damit alles ohne Extra-Downloads läuft. Es ist der proprietäre
  Cloudera-Treiber — das Repo deshalb nicht öffentlich weiterverteilen.

Die Stufen 1+2 einzeln und alle `utils/`-Skripte brauchen **kein** Java —
nur Python + `.env`.

## Benutzung

```bash
# Verbindung testen (optional)
.venv/Scripts/python.exe src/utils/test_connection.py

# KOMPLETTER LAUF (empfohlen): Datenmodell + Stufe 1 → 2 → 3
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

# Täglicher Lauf um 00:00 – läuft dauerhaft (s. offene Punkte)
.venv/Scripts/python.exe src/scheduler.py

# Kompletter Reset der gruppe3-Tabellen (z.B. um Full Load vs. Skip zu testen)
.venv/Scripts/python.exe src/utils/reset_database.py
```

Danach ist das Datenprodukt abfragbar — Beispiel-Queries und die genaue
Bedeutung aller Spalten (inkl. NULL-Semantik) stehen im
**[Data Contract](docs/data_contract.yaml)**.

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
| ETL-Metadaten | `gruppe3_etl_state`, `gruppe3_etl_row_state`, `gruppe3_etl_changed_keys_tmp` | Incremental-Loading-Zustand (kein Konsumenten-Interface) |

**Das Datenprodukt** (Zeilenzahlen Stand 06.07.2026):

| Dimensionen | Fakten |
|---|---|
| `gruppe3_dim_kreis` (472) | `gruppe3_fact_bevoelkerung` (14.110, 1995–2024) |
| `gruppe3_dim_jahr` (30) | `gruppe3_fact_bauland` (4.720, 2015–2024) |
| `gruppe3_dim_gemeinde` (10.947, Brücke Kreis↔Stadt) | `gruppe3_fact_klima` (1.539, 1995–2013) |
| `gruppe3_dim_klimastadt` (81) | `gruppe3_fact_gemeinde_stamm` (10.950) |
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
- [x] Data Contract
- [ ] Restpunkte unten

**Offene Arbeiten:**

1. **Scheduler:** steht im Testmodus (`CronTrigger(minute="*")`) → vor Abgabe
   auf `hour=0, minute=0`. Außerdem ruft er nur Stufe 3 auf — seit dem
   Skip-Check würde er ohne frische Stufen 1+2 dauerhaft überspringen →
   auf `run_pipeline.main()` umstellen.
2. **Skip-Check greift im Komplettlauf nie:** `stage_table_incremental`/
   `audit_table_incremental` schreiben ihr Wasserzeichen bei **jedem** Lauf
   neu (auch ohne Änderung). Dadurch ist der Audit-Stand immer „jünger" als
   der letzte Ziel-Build und `should_skip_target_build()` liefert nie True →
   `record_state` nur bei tatsächlicher Änderung aufrufen.
3. **[docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md)**
   beschreibt die Klima-Anbindung noch als Koordinaten-Distanz-Join; der Code
   nutzt inzwischen einen Namens-Join (74 von 81 Städten) → Doku angleichen.
4. **`dim_kreis.kreis_name` mehrdeutig** (Stadt/Landkreis-Zusatz wurde bei der
   Bereinigung entfernt, z. B. dreimal „Leipzig") → Zusatz-Spalte `kreis_typ`
   oder Originalname ergänzen; bis dahin gilt: Joins nur über `kreis_id`
   (im Data Contract dokumentiert).
5. **`fact_gemeinde_stamm`:** 3 doppelte `gemeinde_id` durch Quell-Duplikate →
   Dedupe in `build_fact_gemeinde_stamm`.
6. **`overwrite_table`** trunkiert die Zieltabelle, bevor Spark rechnet →
   Reihenfolge tauschen (erst `collect()`, dann `TRUNCATE`), damit das
   Konsistenz-Fenster minimal wird.
7. Kosmetik: `WindowExec`-Warnung (Window ohne `PARTITION BY`, bei unserer
   Datenmenge unkritisch), log4j-`ClassCastException` des JDBC-Treibers
   (harmlos, s. [docs/spark_stolpersteine.md](docs/spark_stolpersteine.md)),
   Docstring-Angabe „83 Spalten" für die Bevölkerungstabelle (real: 92).

> Hintergrund & Prüfungsvorbereitung: [docs/projekt_notizen.md](docs/projekt_notizen.md)
