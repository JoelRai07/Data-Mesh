# Projekt-Notizen (Verständnis & Prüfungsvorbereitung)

Zusammenfassung der wichtigsten Entscheidungen und Erklärungen rund um das
Projekt — als Nachschlagewerk und als Futter für die Präsentation/Fragerunde.

## 1. Worum geht es (Use Case)
**Standortprofil-Dashboard**: Landkreise vergleichbar machen (Wohnraumdruck,
Klimarisiko), indem Bevölkerung + Bauland + Klima + Gemeinden zu **einem**
Datenprodukt verbunden werden. Rohdaten liegen fertig auf Impala — es muss
**nichts** von Kaggle geladen werden.

## 2. Organisatorisches
- Wir sind **Gruppe 3** → arbeiten in der Impala-Datenbank **`gruppe3`**
  (jede Gruppe hat eine eigene: gruppe1..gruppe5). Alle Tabellen tragen das
  Präfix `gruppe3_`.
- Quelltabellen: `gruppe3_project_{gemeinden,bauland,klimadaten,bevoelkerungzahlen}`.

## 3. Datenmodell: Star-/Galaxy-Schema (4 Dim + 5 Fakten)
- `dim_` = Beschriftungen (Wo/Wann), `fact_` = Zahlen (Messwerte).
- Genau genommen ein **Galaxy-Schema**: mehrere Faktentabellen teilen sich die
  Dimensionen `dim_kreis` und `dim_jahr`. Jede Faktentabelle + ihre Dimensionen
  = ein Stern.
- **Verbindungs-Schlüssel:**
  - Kreis-Ebene: der **amtliche Regionalschlüssel** (`01001`); erste 2 Stellen
    = Bundesland. Verbindet Bevölkerung + Bauland exakt.
  - Klima hat **keinen** Schlüssel (weltweite Städte). Lösung: `dim_gemeinde`
    als **Brücke** — Gemeinden haben Koordinaten, Klimastädte auch → zu jeder
    Gemeinde die **geografisch nächste Klimastadt** (lat/long-Abstand). So hängt
    Klima doch am Kreis.
- `fact_standortprofil_kpi` = das **fertige Datenprodukt** (alle KPIs pro Kreis ×
  Jahr vorberechnet, damit ein Dashboard nicht 5 Tabellen joinen muss).

## 4. Die 3 kniffligen Transformationen (Prüfungsfragen!)
- **Unpivot** (Bevölkerung breit→lang): 1 Spalte pro Jahr → 1 Zeile pro Jahr.
  In Spark per `explode(array(struct(...)))`.
- **Pivot** (Bauland lang→breit): 1 Zeile pro Merkmal → 1 Spalte pro Merkmal.
  Gegenteil von Unpivot.
- **z-Score** (Standort-Score): verschiedene Einheiten (%, €/m², °C) vergleichbar
  machen: `(Wert − Jahres-Durchschnitt) / Jahres-Streuung`. Erst dann darf man
  sie zu einem Score verrechnen. Geht in Spark (Window-Funktion), in einfachem
  Impala-SQL ging `STDDEV` als Fensterfunktion nicht.

## 5. Technik: warum welches Werkzeug
- **Impala** = die Datenbank (führt das SQL aus; Tabellen liegen hier).
- **impyla** = einfacher Python-Draht zu Impala: Tabellen **anlegen** (DDL) und
  Ergebnisse **reinschreiben**.
- **Spark (PySpark)** = die Verarbeitungs-Engine für die **Umwandlungen**
  (Unpivot/Pivot/z-Score). Prof wollte Spark statt reiner Skripte.
- **Ablauf:** Spark liest Rohdaten aus Impala (JDBC) → rechnet → Ergebnis wird
  über impyla zurückgeschrieben (Sparks direkter JDBC-Writer hatte Probleme mit
  NULL-Typerkennung, s. spark_stolpersteine.md).
- **DataFrame-API statt `spark.sql`:** Der Code nutzt `df.filter().groupBy()...`
  statt SQL-Strings. Beides ist echtes, gleichwertiges Spark; die DataFrame-API
  ist besser kombinierbar. (Falls der Prof `spark.sql` erwartet: Umschreiben ist
  mittelschwer machbar, weil Spark 3.5 `PIVOT`/`UNPIVOT`/Window nativ in SQL kann.)

## 6. Pipeline & Scheduler
- **Lade-Muster: Full Load** — jeder Lauf leert die Zieltabelle (`TRUNCATE`) und
  füllt sie komplett neu (`INSERT`). Dadurch **idempotent**: mehrfach ausführbar,
  keine Duplikate. (Alternative: Incremental Load / CDC — nur benennen können.)
- **Batch, nicht Streaming**: ein täglicher Lauf.
- **Scheduler**: `scheduler.py` (APScheduler, `CronTrigger(hour=0, minute=0)`).
  Läuft **lokal** — für die Code-Abgabe ok. **Ehrliche Grenze für die
  Präsentation:** produktiv gehörte der Scheduler auf die Plattform, z.B.
  **Cloudera Data Engineering / Apache Airflow**, statt auf einen Laptop.

## 7. Warum Star-Schema / Parquet (OLTP vs. OLAP)
- **OLTP** = operativ (schreiben, normalisiert/3. NF). **OLAP** = analytisch
  (lesen/aggregieren, denormalisiert). Man kopiert Daten vom operativen in ein
  analytisches System (= die Pipeline).
- **Star-Schema** = denormalisiert → wenige Joins → schnell → in der Cloud billig.
- **Parquet** = spaltenweise Speicherung → ideal fürs Lesen weniger Spalten.

## 8. Data Mesh (Theorie — kommt in der Fragerunde!)
- Data Mesh ist **keine Technologie**, sondern Organisations-/Architektur-Konzept.
- **4 Prinzipien:** Domain Ownership · Data as a Product · Self-Serve Data
  Platform · Federated Governance.
- Im Projekt: `fact_standortprofil_kpi` = „Data as a Product"; ehrlich
  dokumentierte Datenqualität = „Federated Governance".

## 9. Datenqualität (ehrlich dokumentieren = Federated Governance)
- Kaputte Umlaute (`L�beck`) und führende Leerzeichen in Namen (Rohdaten, noch
  nicht bereinigt).
- `project_gemeinden`: CSV-Parsing-Fehler (Kommas in Namen), kein Schlüssel →
  Kreis-Zuordnung per Namens-Match (fehlerbehaftet).
- Bauland `flaeche = 0` bei echter Kaufsumme (Rundung auf 1000-qm-Einheiten) →
  hat den Score-Bug ausgelöst, s. `bugfix_score_nullwerte.md`.
- Viele NULLs in KPI-Spalten = amtlich unterdrückte Werte / kein Vorjahr → als
  Coverage-Quote in den Data Contract.

## 10. Stand: was aus den Folien umgesetzt ist
**Umgesetzt:** Star-Schema, Denormalisierung, Parquet, Unpivot/Pivot, Full-Load-
Pattern, Batch, Scheduler, „Data as a Product".
**Anders:** Pipeline in Spark statt NiFi (vom Prof erlaubt/bevorzugt).
**Noch offen / Kür:** Data Contract (Pflicht, kommt Do. im Unterricht),
Incremental Load / CDC (nur erwähnen), Open Table Format / **Iceberg** statt nur
Parquet (echte Kür für Extra-Punkte).

## 11. Offene Punkte vor der Abgabe
- Scheduler von Testmodus (`minute="*"`) zurück auf `hour=0, minute=0`.
- Score-Bugfix per Pipeline-Lauf ausrollen und gegenprüfen.
- 4. Bauland-Merkmal („Kaufwert je qm") noch aufnehmen.
- Umlaute/Leerzeichen bereinigen.
- Data Contract erstellen.
