# Data Mesh & Data Engineering – Portfolioprüfung

Datenprodukt auf Basis von vier öffentlichen Datensätzen zu deutschen Gemeinden
(Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die Daten liegen in
einer **Cloudera CDP / Impala**-Datenbank der DHBW Stuttgart (Datenbank `gruppe3`);
der Code hier greift per Python (`impyla`) darauf zu.

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
├── src/                              # Unser Code
│   ├── db.py                        # Zentraler Verbindungs-Helfer (get_connection)
│   ├── test_connection.py           # Prüft die Verbindung zu Impala
│   ├── inspect_tables.py            # Zeigt Aufbau + Zeilenzahl der project_*-Quelltabellen
│   ├── create_datamodel.py          # DELIVERABLE 1: DDLs für das Star-Schema (Dimensionen + Fakten)
│   └── pipeline.py                  # DELIVERABLE 2: Befüllt das Datenmodell aus den Rohdaten
│
├── data/                             # Beispiel-Rohdaten als CSV (lokal, zur Inspektion)
│   └── bevoelkerungzahlen.csv
│
├── reference/                        # Beispiel-Skripte aus der Vorlesung (Vorlagen, nicht Teil der Abgabe)
│   ├── create_nifi_tables.py
│   └── impala_snippet.py
│
├── docs/                             # Aufgabenstellung, Kursmaterial & Modell-Begründung
│   ├── Portfolioprüfung.pdf          # Aufgabenstellung
│   ├── datenmodell_begruendung.md    # DELIVERABLE 1b: Begründung des Datenmodells
│   ├── Tabellenbeispiel.md           # Schema + Beispielzeilen der vier Rohdaten-Tabellen
│   └── coursematerial/               # Foliensätze + Übungsmaterial aus der Vorlesung
│
└── lib/                              # JDBC-Treiber (für Java/Tools, nicht eingecheckt)
    └── ImpalaJDBC42.jar
```

## Einrichtung (einmalig)

```bash
# 1. Virtuelle Umgebung anlegen
python -m venv .venv

# 2. Abhängigkeiten installieren
.venv/Scripts/python.exe -m pip install -r requirements.txt

# 3. Zugangsdaten eintragen
#    .env.example nach .env kopieren und Werte ausfüllen
#    (Workload-Username & Workload-Passwort aus dem Cloudera-Portal)
```

## Benutzung

```bash
# Verbindung testen
.venv/Scripts/python.exe src/test_connection.py

# Vorhandene project_*-Quelltabellen ansehen (Schema + Zeilenzahl)
.venv/Scripts/python.exe src/inspect_tables.py

# 1. Datenmodell (Dimensionen + Fakten) anlegen - idempotent, mehrfach ausführbar
.venv/Scripts/python.exe src/create_datamodel.py

# 2. Datenmodell befüllen - idempotent (INSERT OVERWRITE), muss nach 1. laufen
.venv/Scripts/python.exe src/pipeline.py
```

`pipeline.py` füllt die Tabellen in fester Reihenfolge entlang der Abhängigkeiten
im Star-Schema (Dimensionen zuerst, dann Basis-Fakten, zuletzt die aggregierte
KPI-Faktentabelle `gruppe3_fact_standortprofil_kpi`) — Details dazu stehen im
Modul-Docstring von `pipeline.py`.

Eigene Skripte importieren die Verbindung zentral:

```python
from db import get_connection      # funktioniert, wenn das Skript in src/ liegt

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM gruppe3_dim_kreis LIMIT 10")
for row in cur.fetchall():
    print(row)
```

## Quell-Tabellen auf Impala (Rohdaten, bereits befüllt, Datenbank `gruppe3`)

| Tabelle                          | Zeilen     | Inhalt                                            |
|-----------------------------------|-----------:|---------------------------------------------------|
| `gruppe3_project_gemeinden`       |     10.950 | Gemeinden: Land, Kreis, Name, Fläche, Einwohner   |
| `gruppe3_project_bauland`         |     21.600 | Baulandverkäufe je Jahr/Kreis (4 Merkmale)        |
| `gruppe3_project_klimadaten`      |  8.599.212 | Temperaturen je Stadt/Datum (weltweit)            |
| `gruppe3_project_bevoelkerungzahlen` |       581 | Einwohner je Kreis (breit: 1 Spalte je Jahr 1995-2024) |

Das daraus abgeleitete Star-Schema (4 Dimensionen, 5 Fakten) ist in
[docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md) beschrieben.

## Stand / To-do für die Abgabe

- [x] Umgebung & Impala-Verbindung eingerichtet
- [x] **Datenmodell (DDLs)** für das Datenprodukt + Begründung
      → [src/create_datamodel.py](src/create_datamodel.py), [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md)
- [x] **Pipeline** zur Befüllung (idempotent via `INSERT OVERWRITE`)
      → [src/pipeline.py](src/pipeline.py)
  - [ ] Bekannte Lücke: `project_bauland` hat 4 Merkmale, die Pipeline pivotiert
        aktuell nur 3 (`Durchschnittlicher Kaufwert je qm` wird nicht übernommen,
        siehe TODO in `pipeline.py`)
- [ ] **Scheduler-Code** für die Pipeline (z. B. Cronjob, siehe Vorlesung 3)
- [ ] **Data Contract** zur Beschreibung der Daten und Nutzung des Datenprodukts
- [ ] Entscheidung NiFi vs. Spark vs. reines SQL für die Pipeline (aktuell: reines SQL via `impyla`)
