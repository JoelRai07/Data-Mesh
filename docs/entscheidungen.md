# Architektur-Entscheidungen (ADRs) & Projekt-Historie

Dieses Dokument ist der **rote Faden durch alle Entscheidungen**: welche gerade
gelten (aktive ADRs), welche wir unterwegs verworfen haben (abgelöste ADRs, mit
Begründung) und welche Probleme dabei gelöst wurden. Ein ADR (Architecture
Decision Record) hält je Entscheidung fest: **Kontext → Entscheidung →
Begründung → Trade-off.**

## 0. Doku-Wegweiser: welche Datei wofür

| Datei | Frage, die sie beantwortet |
|---|---|
| [README.md](../README.md) | „Wie ist das Projekt aufgebaut, wie führe ich es aus, wie lese ich den Code?" (Einstiegspunkt) |
| [ADR.md](../ADR.md) | „Warum Apache Iceberg für den Bauland/Bevölkerung-Merge — und was hat das ersetzt?" (Vertiefung zu ADR-8) |
| [data_contract.yaml](data_contract.yaml) | „Was bekomme ich als **Konsument** des Datenprodukts und wie nutze ich es korrekt?" |
| [datenmodell_begruendung.md](datenmodell_begruendung.md) | „Warum sieht das Datenmodell (Star-Schema) so aus?" (Deliverable 1) |
| **dieses Dokument** | „Warum ist die Lösung insgesamt so gebaut — und was galt früher?" |
| [spark_stolpersteine.md](spark_stolpersteine.md) | Tiefe Fallstudien zu Spark/JDBC-Problemen (6 Stück) |
| [bugfix_score_nullwerte.md](bugfix_score_nullwerte.md) | Tiefe Fallstudie: warum eine KPI-Spalte komplett NULL war |
| [scheduler_bug.md](scheduler_bug.md) | Tiefe Fallstudie: APScheduler `next_run_time` |
| [projekt_notizen.md](projekt_notizen.md) | Prüfungsvorbereitung: Begriffe, Vorlesungsbezug, Fragerunden-Futter |
| [../TODO.md](../TODO.md) | „Was ist noch offen?" (einzige Aufgabenliste) |

## 1. Die aktuelle Architektur in 30 Sekunden

```
default.project_* ──(1) Staging──▶ gruppe3_staging_* ──(2) Audit──▶ gruppe3_audit_* ──(3) Publish──▶ dim_*/fact_*
                    impyla, inkrementell        impyla, Bereinigung + inkrementell         Spark, Star-Schema
```

Write-Audit-Publish-Pattern (ADR-3), drei Lade-Strategien je Tabellenart
(ADR-8), Transformationen in Spark (ADR-4), Orchestrierung über
`run_pipeline.py` (ADR-9), täglicher Trigger per APScheduler (ADR-10),
Konsumenten-Schnittstelle beschrieben im Data Contract (ADR-12).

---

## 2. Aktive ADRs (das gilt heute)

### ADR-1 · Star-/Galaxy-Schema, denormalisiert, als Parquet
**Kontext:** 4 Quelltabellen, analytischer Use Case „Standortprofil-Dashboard" (OLAP, lesen/aggregieren).
**Entscheidung:** 4 Dimensionen + 5 Fakten, denormalisiert (z. B. Bundesland direkt in `dim_kreis`), `STORED AS PARQUET`.
**Warum:** OLAP liest Aggregate über viele Zeilen — Normalisierung (3. NF) erzwingt teure Joins; Parquet ist spaltenorientiert (Vorlesung 1, Folien 22–35).
**Trade-off:** Redundanz in den Dimensionen — für ein Lese-System gewollt. Details: [datenmodell_begruendung.md](datenmodell_begruendung.md).

### ADR-2 · Regionalschlüssel + `dim_gemeinde` als verbindende Dimensionen
**Kontext:** Bevölkerung und Bauland teilen den amtlichen Regionalschlüssel; Klima hat nur Städtenamen, Gemeinden haben keinen amtlichen Schlüssel.
**Entscheidung:** `dim_kreis` (Regionalschlüssel) als conformed dimension für Bevölkerung+Bauland; `dim_gemeinde` als Brücke zwischen Kreis-Ebene und Klimastadt-Ebene.
**Warum:** So werden aus 4 isolierten Tabellen **ein** zusammenhängendes Modell — der eigentliche Mehrwert des Datenprodukts.
**Trade-off:** Die Brücke basiert auf Namens-Matching und ist nicht perfekt (97,5 % Kreis-Coverage, dokumentiert im Contract).

### ADR-3 · Write-Audit-Publish in drei getrennten Stufen
**Kontext:** Rohdaten haben massive Qualitätsprobleme (Encoding, Formate); Vorlesung 3 stellt WAP/Medallion als Organisations-Pattern vor.
**Entscheidung:** Drei Stufen mit eigenen Tabellen-Schichten: `staging_*` (Rohkopie) → `audit_*` (bereinigt) → `dim_*/fact_*` (veröffentlicht). Ein Skript pro Stufe.
**Warum:** Klare Verantwortung pro Stufe; Bereinigung ist nachvollziehbar (roh und bereinigt liegen nebeneinander); Stufen sind einzeln ausführbar und testbar.
**Trade-off:** ~2× Speicher für die Rohkopie, drei Skripte statt einem — bewusst bezahlt (Folie 37/39: Aufwand vs. Vertrauen).

### ADR-4 · Engine-Mix: Impala-SQL für Stufe 1+2, Spark für Stufe 3
**Kontext:** Prof-Vorgabe „Pipeline mit NiFi oder Spark, kein reiner Skript-Code"; Klimadaten haben 8,6 Mio. Zeilen.
**Entscheidung:** Kopieren/Bereinigen als serverseitiges `INSERT OVERWRITE/INTO … SELECT` (impyla setzt nur ab); Transformationen (Unpivot, Pivot, Window/z-Score) als PySpark DataFrame-API.
**Warum:** 8,6 Mio. Zeilen durch Python zu schleusen wäre unsinnig langsam; Spark glänzt genau bei den komplexen Transformationen — u. a. `STDDEV() OVER (…)`, das Impala als Window-Funktion nicht kann.
**Trade-off:** Zwei Werkzeuge im Projekt. In der Präsentation aktiv begründen, bevor die Frage kommt.

### ADR-5 · Hybrid-I/O: Spark liest per JDBC, impyla schreibt zurück
**Kontext:** `df.write.jdbc()` gegen Impala scheitert (kein Impala-Dialekt in Spark; Treiber kann NULL-Parameter nicht binden — Stolpersteine 3+5).
**Entscheidung:** Spark nur zum Lesen/Rechnen; Schreiben via `collect()` + impyla-`INSERT INTO … VALUES`-Batches (`overwrite_table()`), `TRUNCATE` separat per impyla.
**Warum:** Impala ist ein Lese-Motor, kein robustes JDBC-Schreibziel — bewusste Architekturentscheidung, kein Notbehelf. Details: [spark_stolpersteine.md](spark_stolpersteine.md).
**Trade-off:** `collect()` holt Ergebnisse zum Treiber — bei unseren Zielgrößen (≤ ~15 k Zeilen) unkritisch, skaliert aber nicht beliebig.

### ADR-6 · Encoding: Korrektur-Mappings für Zerstörtes, Transliteration für Intaktes
**Kontext:** In Bauland/Bevölkerung sind Umlaute als U+FFFD (`�`) gespeichert — die Information ist **weg**, kein Algorithmus kann sie zurückrechnen. In Gemeinden sind Umlaute intakt; Klimadaten haben englische Exonyme und Kompass-Koordinaten.
**Entscheidung:** 1:1-Korrekturen für die bekannten zerstörten Werte — seit 07.07. werden die betroffenen Kreisnamen zur **Laufzeit entdeckt** (`_discover_bad_kreis_values`) und automatisch über drei Referenzlisten aufgelöst (`src/utils/german_cities.txt`/`german_regions.txt`/`german_states.txt`, s. `quellen.txt`); nur 8 Sonderfälle ohne Listen-Eintrag bleiben manuell (`MANUAL_KREIS_CORRECTIONS`), plus `BAULAND_MERKMAL_CORRECTIONS` (2 Merkmalstexte); echte Transliteration ä→ae/ö→oe/ü→ue nur für intakte Spalten; `CITY_NAME_CORRECTIONS` (Munich→Muenchen); `compass_to_signed_decimal` („5.63S"→„-5,63"); alles serverseitig in Stufe 2.
**Warum:** Bei zerstörter Information ist ein gepflegtes Mapping die einzig **korrekte** Lösung; Transliteration überall sonst macht alle Namens-Joins ASCII-einheitlich.
**Ergebnis:** 0 verbleibende `�` in allen Audit-Tabellen (live verifiziert 06.07.2026).
**Trade-off:** Mapping-Pflege bei neuen kaputten Werten; `strip_after_comma` kostet die Stadt/Landkreis-Unterscheidung im Namen (offener Punkt, s. TODO).

### ADR-7 · Klima-Anbindung per Namens-Join (statt Koordinaten-Distanz)
**Kontext:** Klimastädte haben keinen Regionalschlüssel. Ursprünglich: Zuordnung über die geografisch nächste Stadt (lat/long-Distanz).
**Entscheidung:** Join `gemeinde_name = stadt_name` (beide Seiten transliteriert + Exonym-Mapping) — trifft 74 von 81 Klimastädten; Gemeinden ohne Treffer bekommen keinen Klimawert (LEFT JOIN, im Score neutral 0).
**Warum:** Deterministisch und erklärbar; der Distanz-Join scheiterte zunächst an zerstörten Koordinaten (s. Problem P3) und mehrdeutige Kurznamen („Frankfurt", „Oldenburg") wären beim Fuzzy-Match reine Raterei.
**Trade-off:** 7 Städte bleiben unangebunden; über eine Korrekturliste nachschärfbar. **Achtung:** [datenmodell_begruendung.md](datenmodell_begruendung.md) beschreibt noch den alten Distanz-Ansatz → TODO.

### ADR-8 · Incremental Loading mit drei Strategien + append-only State *(neu 06.07., Duc; Punkt 2 aktualisiert 07.07.)*
**Kontext:** Der Scheduler läuft täglich; ein täglicher Full Load von 8,6 Mio. Klimazeilen wäre Verschwendung. Impala auf Parquet kennt **kein** UPDATE/DELETE/MERGE.
**Entscheidung:** Je Tabellenart die passende Strategie (Vorlesung 3, Incremental-Loader-Pattern):
  1. **Wasserzeichen** (`klimadaten`, echte Zeitreihe über `dt`): nur neuere Zeilen anhängen.
  2. **Zeilengenauer Key-Merge** (`bauland`, `bevoelkerungzahlen` — verlässlicher Business-Key): FNV-Hash je Zeile, nur neue/geänderte/gelöschte Keys ersetzen. **Update 07.07.:** `gruppe3_audit_bauland`/`gruppe3_audit_bevoelkerungzahlen` laufen jetzt als **Apache-Iceberg**-Tabellen (statt Parquet) — echtes `DELETE`/`MERGE INTO` statt des früheren `CREATE TABLE … AS SELECT` + Rename-Swap + Drop. Details/Begründung/Verifikation: [ADR.md](../ADR.md).
  3. **Inhalts-Prüfsumme** (`gemeinden` — kein NULL-freier Key): Full Refresh nur bei geänderter Prüfsumme.
  Der Zustand liegt in `gruppe3_etl_state`/`_row_state` — **append-only** (jüngster Eintrag gewinnt), weil Zeilen-Updates in Impala/Parquet nicht existieren (gilt weiterhin für diese State-Tabellen selbst, die bleiben Parquet). Stufe 3 überspringt den ganzen Spark-Lauf, wenn kein Audit-Stand neuer ist als der letzte Ziel-Build.
**Warum gemeinden nicht zeilengenau:** live getestet — NULL-behaftete Keys kollidierten, ein Reset-Folgelauf erkannte fälschlich Änderungen; bei ~11 k Zeilen ist die Prüfsumme robust und schnell genug.
**Trade-off:** Deutlich mehr Komplexität + State-Tabellen. **Bekannter Bug:** das Wasserzeichen wird bei jedem Lauf neu geschrieben → der Skip von Stufe 3 greift im Komplettlauf nie (s. TODO, Fix ist ein Einzeiler).

### ADR-9 · `run_pipeline.py` als einziger Einstiegspunkt, fail-fast *(neu 06.07., Duc)*
**Kontext:** Vorher vier einzelne Skript-Starts mit stiller Reihenfolge-Abhängigkeit (Zieltabellen mussten vor Stufe 3 existieren).
**Entscheidung:** Ein Orchestrator ruft DDLs + Stufe 1→2→3 auf; bewusst **kein** try/except je Stufe; Docker-`CMD` zeigt darauf.
**Warum:** „Idempotent lauffähig" ist mit einem Befehl demonstrierbar; bricht eine Stufe ab, soll der Gesamtlauf mit Fehler-Exit-Code enden statt auf veralteten Daten weiterzurechnen.

### ADR-10 · APScheduler statt cron/Airflow
**Kontext:** Scheduler-Code ist Teil der benoteten Abgabe.
**Entscheidung:** `BlockingScheduler` + `CronTrigger(hour=0, minute=0)`, Fehler-Isolation je Lauf, `misfire_grace_time=3600`, Zeitzone Europe/Berlin.
**Warum:** Der Zeitplan steht als **lesbarer Code im Repo** (statt Crontab außerhalb), läuft auf Windows wie Linux.
**Trade-off:** Kein Backfilling/DAG wie Airflow — produktiv gehörte das auf die Plattform (Cloudera DE); als Ausblick in der Präsentation. **Offen:** steht noch im Testmodus und ruft nur Stufe 3 → TODO.

### ADR-11 · NULL-Semantik: `safe_div` statt Infinity/NaN
**Kontext:** 748 Bauland-Zeilen haben `flaeche = 0` (amtliche Rundung) → `kaufsumme/0 = Infinity` → **ein** Wert vergiftet AVG/STDDEV-Fensteraggregate eines ganzen Jahrgangs → Score-Spalte komplett NULL.
**Entscheidung:** Jede Division mit variablem Nenner läuft über `safe_div()` (NULL bei Nenner 0/NULL); NaN/Infinity werden beim Schreiben zu NULL; fehlendes Klima geht per `coalesce(…, 0)` neutral in den Score ein.
**Warum:** NULL heißt bei uns ehrlich „nicht berechenbar" und wird von Aggregaten übersprungen — Infinity dagegen zerstört sie. Fallstudie: [bugfix_score_nullwerte.md](bugfix_score_nullwerte.md).

### ADR-12 · Data Contract als YAML mit gemessenen Qualitätszahlen
**Kontext:** Pflicht-Deliverable; Data-Mesh-Prinzipien „Data as a Product" + „Federated Governance" (Vorlesung 2).
**Entscheidung:** [data_contract.yaml](data_contract.yaml) nach Data-Contract-Specification-Muster: Owner, Server, Nutzungs-Terms, alle 9 Modelle mit Spalten-Semantik, Servicelevels, **live gemessene** Qualitätszahlen (97,5 % Kreis-Coverage, 74/81 Klima-Match, Klima nur bis 2013, `kaufwert_je_qm_eur` unbrauchbar, NULL-Quoten je KPI) und Beispiel-Queries.
**Warum:** Konsumenten sollen das Produkt nutzen können, **ohne uns zu fragen** — und ehrliche, messbare Qualität schafft Vertrauen (statt Marketing). Maschinenlesbares YAML, damit Checks daraus automatisierbar sind (die `quality.query`-Einträge sind lauffähige SQLs → Kandidaten fürs Audit-Gate).
**Trade-off:** Die Zahlen sind ein datierter Snapshot (Stand 06.07.2026) und müssen bei Datenänderung nachgezogen werden — bewusst, Kür wäre automatische Generierung.

---

## 3. Abgelöste Entscheidungen (die „alten ADRs")

| Alt (galt früher) | Warum verworfen | Ersetzt durch |
|---|---|---|
| **Reine Impala-SQL-Pipeline** (`pipeline.py`, alles per impyla-SQL-Strings) | Prof will Spark/NiFi statt Skript-Code; `STDDEV` als Window-Funktion in Impala nicht möglich (z-Score!) | ADR-4/5 (Spark-Stufe 3) |
| **Alte Rohdaten-Kopien in der Gruppen-DB** | Koordinaten in der Kopie durch CSV-Bug zerstört (`latitude='13735"'`); Original `default.project_*` ist intakt | Stufe 1 liest `default.project_*` (ADR-3) |
| **Koordinaten-Distanz-Join** Gemeinde→nächste Klimastadt | Scheiterte erst an zerstörten Koordinaten (P3); nach deren Fix bewusst beim einfacheren, deterministischen Namens-Join geblieben | ADR-7 |
| **„Kaputte Zeichen einheitlich entfernen"** (`L�beck`→`Lbeck`) | Erzeugt falsche Namen, die nirgends mehr matchen; die Original-Schreibweisen sind ja bekannt | ADR-6 (Korrektur-Mappings) |
| **`spark.jars`** zum Einbinden des JDBC-Treibers | Löst unter Windows den Hadoop-Kopierpfad aus → braucht `winutils.exe` (Crash) | `spark.driver/executor.extraClassPath` (Stolperstein 2) |
| **`df.write.jdbc()`** zum Zurückschreiben | Treiber kann NULL-Parameter nicht typisieren; Spark-Dialekt-Fallback erzeugt für Impala ungültige DDL (hat einmal eine Tabelle gedroppt!) | ADR-5 (impyla-Batches) |
| **`spark.createDataFrame(python_liste)`** für Kleinst-Daten (dim_jahr) | Python↔JVM-Socket wird von VPN gestört („Accept timed out") | `F.explode(F.sequence(…))` rein JVM-seitig (Stolperstein 4) |
| **Full Load in jeder Stufe, jeden Lauf** (`INSERT OVERWRITE` immer) | Täglicher Rewrite von 8,6 Mio. Zeilen ohne Not; Skips unmöglich | ADR-8 (Incremental, 06.07.) |
| **Vier einzelne Skript-Starts**, Docker-CMD = nur Stufe 3 | Stille Reihenfolge-Abhängigkeiten; Scheduler fütterte sich nie mit frischen Quelldaten | ADR-9 (`run_pipeline.py`) |
| **`wohnraumdruck_index` als Verhältnis zweier Wachstumsraten** | Vorzeichen-Falle: −/− = +, das Vorzeichen spiegelte die Vorzeichen-Kombination, nicht den Druck (Ostallgäu-Beispiel) | Verhältnis von Bestandsgrößen (Einwohner je 1000 m² Bauland), nie negativ |
| **3 von 4 Bauland-Merkmalen pivotiert** | Das 4. (amtlicher Kaufwert/qm) sollte als Vergleichswert dazu — stellte sich dann als ab Quelle zerstört heraus (P5) | Alle 4 pivotiert; Spalte im Contract als „nicht verwenden" markiert |

## 4. Gelöste Probleme (Kurzreferenz)

| # | Problem | Lösung | Details |
|---|---|---|---|
| P1 | `standortattraktivitaets_score` komplett NULL (0/4720) | Kette Division-durch-0 → Infinity → vergiftete Window-Aggregate → NaN → NULL; `safe_div` + Klima-`coalesce`; ausgerollt: 3.911/4.099 gefüllt | [bugfix_score_nullwerte.md](bugfix_score_nullwerte.md), ADR-11 |
| P2 | Encoding: `L�beck` & Co. in 2 Quelltabellen | Korrektur-Mappings + Transliteration in Stufe 2; **0 Reste verifiziert** | ADR-6 |
| P3 | Alle Gemeinde-Koordinaten NULL → Klima-Brücke tot | Ursache: zerstörte `gruppe3`-Kopie; Umstieg auf intakte `default.project_gemeinden` + Dezimalkomma-Parsing | [bugfix_score_nullwerte.md](bugfix_score_nullwerte.md) |
| P4 | 6 Spark/JDBC-Stolpersteine (JDK-Version, winutils, Dialekt, VPN, NULL-Binding, explode-Nesting) | je eigener Workaround | [spark_stolpersteine.md](spark_stolpersteine.md) |
| P5 | `kaufwert_je_qm_eur` enthält nur 0/NULL | **Nicht behebbar:** amtliche Dezimalwerte wurden schon beim Quellimport in eine BIGINT-Spalte geladen (auch in `default.project_bauland` so) → im Contract als „NICHT VERWENDEN" markiert, `preis_pro_qm_eur` ist der Ersatz | data_contract.yaml |
| P6 | APScheduler-Crash beim Loggen von `next_run_time` vor Start | `__slots__`-Attribut existiert erst nach Scheduler-Start → Zeit direkt vom Trigger erfragen | [scheduler_bug.md](scheduler_bug.md) |
| P7 | Cloudera-JDBC wirft „Error converting value to double" (`per_km2`, `area_km2`) | Spalten als STRING lesen (`customSchema`) bzw. Kennzahl selbst berechnen | [spark_stolpersteine.md](spark_stolpersteine.md) |
| P8 | Zeilengenauer Merge für `gemeinden` erkannte Phantom-Änderungen | NULL-Keys kollabieren in `CONCAT_WS` → für gemeinden bewusst Tabellen-Prüfsumme statt Key-Merge | ADR-8 |

## 5. Offene Punkte

Bewusst **nicht** hier dupliziert → [TODO.md](../TODO.md) ist die einzige Aufgabenliste.
