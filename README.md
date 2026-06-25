# Data Mesh & Data Engineering – Portfolioprüfung

Datenprodukt auf Basis von vier öffentlichen Datensätzen zu deutschen Gemeinden
(Gemeinden, Baulandverkäufe, Klimadaten, Bevölkerungszahlen). Die Daten liegen in
einer **Cloudera CDP / Impala**-Datenbank der DHBW Stuttgart; der Code hier greift
per Python (`impyla`) darauf zu.

## Projektstruktur

```
Data-Mesh/
├── README.md              # Diese Datei
├── requirements.txt       # Python-Abhängigkeiten (pip install -r requirements.txt)
├── .env.example           # Vorlage für Zugangsdaten -> kopieren nach .env
├── .env                   # Echte Zugangsdaten (NICHT eingecheckt, siehe .gitignore)
├── .gitignore
│
├── src/                   # Unser Code
│   ├── db.py              # Zentraler Verbindungs-Helfer (get_connection)
│   ├── test_connection.py # Prüft die Verbindung zu Impala
│   └── inspect_tables.py  # Zeigt Aufbau + Zeilenzahl der project_*-Tabellen
│
├── data/                  # Rohdaten als CSV
│   └── bevoelkerungzahlen.csv
│
├── reference/             # Beispiel-Skripte aus der Vorlesung (Vorlagen)
│   ├── create_nifi_tables.py
│   └── impala_snippet.py
│
├── docs/                  # Aufgabenstellung & Kursmaterial
│   ├── Portfolioprüfung.pdf
│   └── coursematerial/
│
└── lib/                   # JDBC-Treiber (für Java/Tools, nicht eingecheckt)
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

# Vorhandene project_*-Tabellen ansehen (Schema + Zeilenzahl)
.venv/Scripts/python.exe src/inspect_tables.py
```

Eigene Skripte importieren die Verbindung zentral:

```python
from db import get_connection      # funktioniert, wenn das Skript in src/ liegt

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM project_gemeinden LIMIT 10")
for row in cur.fetchall():
    print(row)
```

## Quell-Tabellen auf Impala (Rohdaten, bereits befüllt)

| Tabelle                        | Zeilen     | Inhalt                                            |
|--------------------------------|-----------:|---------------------------------------------------|
| `project_gemeinden`            |     10.950 | Gemeinden: Land, Kreis, Name, Fläche, Einwohner   |
| `project_bauland`              |     21.600 | Baulandverkäufe je Jahr/Kreis                     |
| `project_klimadaten`           |  8.599.212 | Temperaturen je Stadt/Datum                       |
| `project_bevoelkerungzahlen`   |      ~viele| Einwohner je Kreis und Jahr, nach Geschlecht      |

## Stand / To-do für die Abgabe

- [x] Umgebung & Impala-Verbindung eingerichtet
- [x] **Datenmodell (DDLs)** für das Datenprodukt + Begründung
      → [src/create_datamodel.py](src/create_datamodel.py), [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md)
- [ ] **Pipeline** zur Befüllung (idempotent) inkl. **Scheduler**
- [ ] **Data Contract**
- [ ] README für die Abgabe finalisieren
```
