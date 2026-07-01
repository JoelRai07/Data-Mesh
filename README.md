# Data Mesh & Data Engineering – Portfolioprüfung (Gruppe 3)

Datenprodukt auf Basis von vier öffentlichen Datensätzen zu deutschen Gemeinden
(Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die Daten liegen in
einer **Cloudera CDP / Impala**-Datenbank der DHBW Stuttgart (Datenbank `gruppe3`).
Das Datenmodell wird aus den Rohdaten per **Apache Spark** (PySpark) befüllt.

Use Case, Datenmodell und die Begründung dafür stehen in
[docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md).

## Projektstruktur

```
Data-Mesh/
├── README.md                       # Diese Datei
├── requirements.txt                 # Python-Abhängigkeiten (pip install -r requirements.txt)
├── .env.example                     # Vorlage für Zugangsdaten -> kopieren nach .env
├── .env                              # Echte Zugangsdaten (NICHT eingecheckt, siehe .gitignore)
├── .gitignore
│
├── Dockerfile                        # Container-Image (Python 3.11 + JDK 17 + Abhängigkeiten)
├── docker-compose.yml                # Zwei Dienste: pipeline (einmalig) + scheduler (dauerhaft)
├── .dockerignore                     # hält .venv/.git/.env aus dem Build-Kontext
│
├── src/                              # Unser Code
│   ├── db.py                        # Zentraler Verbindungs-Helfer (get_connection, impyla)
│   ├── create_datamodel.py          # DELIVERABLE 1: DDLs für das Star-Schema (4 Dim. + 5 Fakten)
│   ├── pipeline_spark.py            # DELIVERABLE 2: Befüllt das Datenmodell aus den Rohdaten (PySpark)
│   ├── scheduler.py                 # DELIVERABLE 2b: führt die Pipeline täglich um 00:00 aus (APScheduler)
│   └── utils/                       # Hilfsskripte (nicht Teil der Abgabe-Logik)
│       ├── test_connection.py       # Prüft die Verbindung zu Impala
│       ├── inspect_tables.py        # Zeigt Aufbau + Zeilenzahl von Tabellen
│       └── ImpalaJDBC42.jar         # JDBC-Treiber für Spark (NICHT eingecheckt, lokal vorhanden)
│
├── data/                             # Beispiel-Rohdaten als CSV (lokal, zur Inspektion)
│   └── bevoelkerungzahlen.csv
│
├── reference/                        # Beispiel-Skripte aus der Vorlesung (Vorlagen, nicht Teil der Abgabe)
│   ├── create_nifi_tables.py
│   └── impala_snippet.py
│
└── docs/                             # Aufgabenstellung, Kursmaterial & Doku
    ├── Portfolioprüfung.pdf          # Aufgabenstellung
    ├── datenmodell_begruendung.md    # DELIVERABLE 1b: Begründung des Datenmodells
    ├── Tabellenbeispiel.md           # Schema + Beispielzeilen der vier Rohdaten-Tabellen
    ├── spark_stolpersteine.md        # Gesammelte Spark-/JDBC-Stolpersteine + Lösungen
    ├── scheduler_bug.md              # Analyse eines APScheduler-Bugs (next_run_time)
    ├── bugfix_score_nullwerte.md     # Analyse + Fix: Score-Spalte war komplett NULL
    ├── projekt_notizen.md            # Verständnis & Prüfungsvorbereitung (Modell, Werkzeuge, Entscheidungen)
    └── coursematerial/               # Foliensätze + Übungsmaterial aus der Vorlesung
```

## Einrichtung (einmalig)

```bash
# 1. Virtuelle Umgebung anlegen
python -m venv .venv

# 2. Abhängigkeiten installieren (impyla, python-dotenv, APScheduler, pyspark)
.venv/Scripts/python.exe -m pip install -r requirements.txt

# 3. Zugangsdaten eintragen: .env.example nach .env kopieren und ausfüllen
#    (Workload-Username & Workload-Passwort aus dem Cloudera-Portal)
```

**Zusätzlich nur für Spark (`pipeline_spark.py` / `scheduler.py`):**
- **JDK 17** installiert; Pfad in `.env` als `JAVA_HOME_JDK17` eintragen (siehe `.env.example`).
- Den JDBC-Treiber **`ImpalaJDBC42.jar`** unter `src/utils/` ablegen (ist per `.gitignore`
  ausgeschlossen, muss also lokal besorgt werden).

## Benutzung

```bash
# Verbindung testen
.venv/Scripts/python.exe src/utils/test_connection.py

# Quelltabellen ansehen (Schema + Zeilenzahl)
.venv/Scripts/python.exe src/utils/inspect_tables.py

# 1. Datenmodell (Dimensionen + Fakten) anlegen - idempotent (CREATE TABLE IF NOT EXISTS)
.venv/Scripts/python.exe src/create_datamodel.py

# 2. Datenmodell befüllen mit Spark - idempotent (TRUNCATE + INSERT), muss nach 1. laufen
.venv/Scripts/python.exe src/pipeline_spark.py

# 3. (optional) Pipeline täglich um 00:00 automatisch ausführen - läuft dauerhaft
.venv/Scripts/python.exe src/scheduler.py
```

`pipeline_spark.py` füllt die Tabellen in fester Reihenfolge entlang der Abhängigkeiten
im Star-Schema: zuerst die Dimensionen, dann die Basis-Fakten, zuletzt die aggregierte
KPI-Faktentabelle `gruppe3_fact_standortprofil_kpi`.

## Docker

Das Projekt lässt sich als Container betreiben, aber nicht vollständig isoliert, weil
die Impala-Datenbank weiterhin extern in der DHBW-Umgebung liegt. Der Container
enthält deshalb nur Python, Spark und Java 17; die Zugangsdaten kommen aus `.env`.

Voraussetzungen:

- `src/utils/ImpalaJDBC42.jar` muss lokal vorhanden sein.
- `.env` muss ausgefüllt sein.

Build und Start:

```bash
docker compose build
docker compose run --rm pipeline
```

Für den dauerhaften Scheduler:

```bash
docker compose up scheduler
```

Der Compose-Stack mountet den JDBC-Treiber direkt in den Container und startet
entweder die Spark-Pipeline oder den APScheduler mit demselben Image.

**Wichtig — immer nur *einen* Dienst starten:** Kein nacktes `docker compose up`
(ohne Service-Namen) verwenden. Das würde `pipeline` und `scheduler` gleichzeitig
starten, und beide leeren und befüllen dieselben Tabellen (`TRUNCATE` + `INSERT`) →
die parallelen Läufe geraten sich ins Gehege (inkonsistente Daten). Also entweder
`docker compose run --rm pipeline` **oder** `docker compose up scheduler`.

**Scheduler-Testmodus beachten:** `scheduler.py` läuft aktuell jede Minute
(`CronTrigger(minute="*")`, s. offene Punkte unten). In einem dauerhaft laufenden
Container würde damit die komplette Pipeline im Minutentakt gegen die DHBW-DB laufen.
Vor dem Container-Dauerbetrieb auf `CronTrigger(hour=0, minute=0)` zurückstellen.

Hinweise:

- `.env` wird **nicht** ins Image gebacken (steht in `.dockerignore`), sondern zur
  Laufzeit per `env_file` injiziert — Zugangsdaten bleiben aus dem Image heraus.
- Das im Container gesetzte `JAVA_HOME` zeigt auf das enthaltene JDK 17; der
  Windows-Pfad `JAVA_HOME_JDK17` aus `.env` wird im Linux-Container ignoriert.
- Der Container braucht Netzzugang zur DHBW-Datenbank (ggf. VPN/Hochschulnetz) —
  das übernimmt Docker nicht.

Eigene Skripte importieren die Verbindung zentral:

```python
from db import get_connection      # funktioniert, wenn das Skript in src/ liegt

conn = get_connection()
cur = conn.cursor()
cur.execute("USE gruppe3")
cur.execute("SELECT * FROM gruppe3_dim_kreis LIMIT 10")
for row in cur.fetchall():
    print(row)
```

## Datenbank `gruppe3` auf Impala

**Quell-Tabellen (Rohdaten):**

| Tabelle | Inhalt |
|---|---|
| `gruppe3_project_gemeinden` | Gemeinden: Land, Kreis, Name, Fläche, Einwohner, Koordinaten |
| `gruppe3_project_bauland` | Baulandverkäufe je Jahr/Kreis (4 Merkmale, Langformat) |
| `gruppe3_project_klimadaten` | Temperaturen je Stadt/Datum (weltweit, ~8,6 Mio. Zeilen) |
| `gruppe3_project_bevoelkerungzahlen` | Einwohner je Kreis (Breitformat: 1 Spalte je Jahr) |

**Datenprodukt (Star-Schema, 4 Dimensionen + 5 Fakten):**

| Dimensionen | Fakten |
|---|---|
| `gruppe3_dim_kreis` | `gruppe3_fact_bevoelkerung` |
| `gruppe3_dim_jahr` | `gruppe3_fact_bauland` |
| `gruppe3_dim_gemeinde` (Brücke Kreis ↔ Stadt) | `gruppe3_fact_klima` |
| `gruppe3_dim_klimastadt` | `gruppe3_fact_gemeinde_stamm` |
|  | `gruppe3_fact_standortprofil_kpi` (aggregierte Cross-Table-KPIs) |

Details + Begründung: [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md).

## Stand / To-do für die Abgabe

- [x] Umgebung & Impala-Verbindung eingerichtet
- [x] **Datenmodell (DDLs)** + Begründung
      → [src/create_datamodel.py](src/create_datamodel.py), [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md)
- [x] **Pipeline** zur Befüllung (Spark, idempotent) inkl. **Scheduler**
      → [src/pipeline_spark.py](src/pipeline_spark.py), [src/scheduler.py](src/scheduler.py)
- [ ] **Data Contract** (Theorie/Umsetzung folgt aus dem Unterricht am Do.)
- [ ] README & Abgabe finalisieren (siehe offene Punkte unten)

## Bekannte offene Punkte (vor der Abgabe)

- **Scheduler steht im Testmodus** (`CronTrigger(minute="*")`, läuft jede Minute) →
  vor der Abgabe in [src/scheduler.py](src/scheduler.py) zurück auf
  `CronTrigger(hour=0, minute=0)` (täglich 00:00, wie gefordert) stellen.
- **`WindowExec`-Warnung in `pipeline_spark.py`** (Window-Funktionen ohne `PARTITION BY`,
  z.B. Surrogat-`gemeinde_id`). Bei unseren Datengrößen (~10–15k Zeilen) unkritisch,
  sollte für saubere Skalierung aber behoben werden.
- **log4j-`ClassCastException` beim JDBC-Connect**: kosmetisches Rauschen aus dem
  `ImpalaJDBC42.jar` (geshadetes log4j kollidiert mit Sparks log4j), harmlos —
  s. [docs/spark_stolpersteine.md](docs/spark_stolpersteine.md).
- **Score-Bug behoben (Code), Pipeline-Lauf steht noch aus:** `standortattraktivitaets_score`
  war komplett NULL (Division durch 0 bei `flaeche = 0` → Infinity → vergiftete die
  z-Score-Fensteraggregate). Fix per neuer `safe_div`-Funktion eingebaut; die Tabelle
  wird erst mit dem nächsten `pipeline_spark.py`-Lauf korrekt neu befüllt. Details:
  [docs/bugfix_score_nullwerte.md](docs/bugfix_score_nullwerte.md).
- Weitere KPIs in `fact_standortprofil_kpi` können bei kleinen Nennern extreme Werte
  annehmen — fachliche Plausibilität vor der Präsentation prüfen.
- Datenqualität: kaputte Umlaute (`L�beck`) und führende Leerzeichen in `kreis_name`
  stammen aus den Rohdaten und sind noch nicht bereinigt → im Data Contract dokumentieren.

> Hintergrund & Prüfungsvorbereitung (Modell, Werkzeuge, Entscheidungen) gesammelt in
> [docs/projekt_notizen.md](docs/projekt_notizen.md).
```
