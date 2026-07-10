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
- Quelltabellen: `default.project_{gemeinden,bauland,klimadaten,bevoelkerungzahlen}`
  (nur lesend; unsere Pipeline kopiert sie als `gruppe3_staging_*` in die eigene DB).

## 3. Datenmodell: Star-/Galaxy-Schema (4 Dim + 5 Fakten)
- `dim_` = Beschriftungen (Wo/Wann), `fact_` = Zahlen (Messwerte).
- Genau genommen ein **Galaxy-Schema**: mehrere Faktentabellen teilen sich die
  Dimensionen `dim_kreis` und `dim_jahr`. Jede Faktentabelle + ihre Dimensionen
  = ein Stern.
- **Verbindungs-Schlüssel:**
  - Kreis-Ebene: der **amtliche Regionalschlüssel** (`01001`); erste 2 Stellen
    = Bundesland. Verbindet Bevölkerung + Bauland exakt.
  - Klima hat **keinen** Schlüssel (weltweite Städte). Lösung: `dim_gemeinde`
    als **Brücke** — nach der Bereinigung (Transliteration + „Munich"→„Muenchen")
    haben Gemeinden und Klimastädte dieselbe Namens-Schreibweise → **exakter
    Namens-Match** (74 von 81 Städten). So hängt Klima doch am Kreis.
    (Früher: nächste Klimastadt per lat/long-Abstand — warum gewechselt:
    `entscheidungen.md`, ADR-7.)
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
- **Lade-Muster: Incremental Load**, differenziert nach Tabellenart (State-Tabelle
  `gruppe3_etl_state`, s. `src/etl_state.py`):
  - `klimadaten` (echte Zeitreihe, 8,6 Mio. Zeilen, Spalte `dt`): Wasserzeichen-
    Append (`INSERT INTO` nur neuer Zeilen) in Staging **und** Audit — kein
    Full Rewrite mehr im Regelbetrieb.
  - `bauland`/`bevoelkerungzahlen`/`gemeinden` (amtliche Statistiken/Stammdaten,
    keine verlässliche Änderungsspalte, mögliche nachträgliche Revisionen):
    Change Detection per Inhalts-Prüfsumme — Full Refresh (`INSERT OVERWRITE`,
    auf Iceberg ein atomarer Snapshot) nur, wenn sich der Tabelleninhalt
    tatsächlich geändert hat, sonst wird der Lauf übersprungen. In der
    **Audit-Stufe** gehen `bauland`/`bevoelkerungzahlen` sogar **zeilengenau**:
    Iceberg `DELETE` + `MERGE INTO` mit NULL-sicherem Spaltenvergleich, nur
    neue/geänderte/gelöschte Zeilen werden angefasst (`gemeinden` bewusst
    nicht — kein NULL-freier Schlüssel, s. Kommentar bei `KEY_COLUMNS`).
  - Zielschicht (Star-Schema, `pipeline_audit_to_target.py`): bleibt Full Rebuild
    (Fenster-Funktionen brauchen ganze Kreis+Jahr-Partitionen, Fakten sind klein),
    aber der komplette Spark-Lauf wird übersprungen, wenn sich seit dem letzten
    Ziel-Build keine Audit-Tabelle verändert hat.
  - Nach wie vor **idempotent**: mehrfaches Ausführen erzeugt keine Duplikate.
- **Batch, nicht Streaming**: ein täglicher Lauf.
- **Scheduler**: `scheduler.py` (APScheduler, `CronTrigger(hour=0, minute=0)`).
  Läuft **lokal** — für die Code-Abgabe ok. **Ehrliche Grenze für die
  Präsentation:** produktiv gehörte der Scheduler auf die Plattform, z.B.
  **Cloudera Data Engineering / Apache Airflow**, statt auf einen Laptop.

## 7. Warum Star-Schema / Iceberg-auf-Parquet (OLTP vs. OLAP)
- **OLTP** = operativ (schreiben, normalisiert/3. NF). **OLAP** = analytisch
  (lesen/aggregieren, denormalisiert). Man kopiert Daten vom operativen in ein
  analytisches System (= die Pipeline).
- **Star-Schema** = denormalisiert → wenige Joins → schnell → in der Cloud billig.
- **Parquet** = spaltenweise Speicherung → ideal fürs Lesen weniger Spalten.
- **Apache Iceberg** = offenes Tabellenformat ÜBER den Parquet-Dateien:
  atomare Snapshot-Commits (nie halb geschriebene Tabellen), zeilengenaues
  `MERGE INTO`/`UPDATE`/`DELETE`, Time Travel (`FOR SYSTEM_TIME AS OF`),
  hidden partitioning (`TRUNCATE(4, dt)` = Jahr, ohne dass Abfragen die
  Partition kennen müssen). Prüfungs-Einzeiler: „Parquet ist das Dateiformat,
  Iceberg das Tabellenformat darüber."

## 8. Data Mesh (Theorie — kommt in der Fragerunde!)
- Data Mesh ist **keine Technologie**, sondern Organisations-/Architektur-Konzept.
- **4 Prinzipien:** Domain Ownership · Data as a Product · Self-Serve Data
  Platform · Federated Governance.
- Im Projekt: `fact_standortprofil_kpi` = „Data as a Product"; ehrlich
  dokumentierte Datenqualität = „Federated Governance". Das konkrete Artefakt
  dazu ist unser **Data Contract** (`docs/data_contract.yaml`): Schema +
  Semantik, Nutzungsregeln, SLA und **gemessene** Qualitätszahlen.

## 9. Datenqualität (ehrlich dokumentieren = Federated Governance)
- Kaputte Umlaute (`L�beck`) und führende Leerzeichen: **bereinigt** in Stufe 2
  (Korrektur-Mappings + Transliteration, s. ADR-6 in `entscheidungen.md`);
  verifiziert: 0 kaputte Zeichen in den Audit-Tabellen (06.07.2026).
- `project_gemeinden`: CSV-Parsing-Fehler (Kommas in Namen), kein Schlüssel →
  Kreis-Zuordnung per Namens-Match, 97,5 % Coverage (im Contract dokumentiert).
- Bauland `flaeche = 0` bei echter Kaufsumme (Rundung auf 1000-qm-Einheiten) →
  hat den Score-Bug ausgelöst, s. `bugfix_score_nullwerte.md`.
- `kaufwert_je_qm_eur` ist **ab Quelle zerstört** (amtliche Dezimalwerte in
  BIGINT-Spalte importiert → nur 0/NULL) — im Contract als „nicht verwenden"
  markiert; `preis_pro_qm_eur` ist der berechnete Ersatz.
- Viele NULLs in KPI-Spalten = amtlich unterdrückte Werte / kein Vorjahr →
  NULL-Quoten je Spalte stehen im Data Contract.

## 10. Stand: was aus den Folien umgesetzt ist
**Umgesetzt:** Star-Schema, Denormalisierung, Iceberg-auf-Parquet, Unpivot/Pivot,
Incremental Load (Wasserzeichen + Change Detection + zeilengenauer Merge,
s. Punkt 6), Batch, Scheduler, „Data as a Product".
**Anders:** Pipeline in Spark statt NiFi (vom Prof erlaubt/bevorzugt).
**Erledigt statt offen:** Data Contract ✔ (`docs/data_contract.yaml`),
Encoding-Bereinigung ✔, Score-Fix ausgerollt ✔ (3.911/4.099 gefüllt),
**Kür umgesetzt:** Open Table Format / **Apache Iceberg** über die komplette
Pipeline ✔ — echtes row-level `MERGE INTO`/`DELETE`, atomare Publish-Snapshots,
Time Travel, Jahres-Partitionierung der Klimadaten (s. `ADR.md`).

## 11. Offene Punkte vor der Abgabe
→ zentrale Aufgabenliste: [../TODO.md](../TODO.md) (einzige Quelle, hier nicht dupliziert).
