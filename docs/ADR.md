# Architektur-Entscheidungen (ADR)

Dieses Dokument ist die **zentrale, vollständige Sammlung aller wichtigen
Architektur-Entscheidungen** des Projekts. Jede Entscheidung hält fest:
**Kontext → Entscheidung → Warum → Trade-off.** Am Ende: abgelöste
Entscheidungen (was früher galt).

> Für Setup/Benutzung/Datenmodell → [README.md](../README.md) · Konsumenten-Sicht →
> [data/data_contract.yaml](../data/data_contract.yaml) · alle Probleme
> (gelöst/nicht behebbar/offen) → [Probleme.md](Probleme.md).

## Architektur in 30 Sekunden

```
default.project_* ──(1) Staging──▶ gruppe3_staging_* ──(2) Audit──▶ gruppe3_audit_* ──(3) Publish──▶ dim_*/fact_* ──(4) Gate
                    impyla, inkrementell        impyla, Bereinigung + inkrementell        Spark, Star-Schema      Data Contract
```

Write-Audit-Publish in drei Stufen (ADR-3), drei Lade-Strategien je
Tabellenart (ADR-8), Bereinigung in Impala-SQL / Transformationen in Spark
(ADR-4/5), Orchestrierung über `run_pipeline.py` (ADR-9), täglicher Trigger
lokal per APScheduler (ADR-10) bzw. produktiv als Airflow-DAG in Cloudera
Data Engineering (ADR-17, live seit 11.07.), Data Contract als Output-Port +
technisches Gate (ADR-12).

---

## Aktive Entscheidungen

### ADR-1 · Star-Schema, denormalisiert, als Iceberg-Tabellen (Parquet-Datendateien)
**Kontext:** 4 Quelltabellen, analytischer Use Case „Standortprofil-Dashboard" (OLAP, lesen/aggregieren).
**Entscheidung:** 4 Dimensionen + 5 Fakten, denormalisiert (z. B. Bundesland direkt in `dim_kreis`), `STORED BY ICEBERG` (`format-version` 2; die Datendateien bleiben Parquet). Ursprünglich reines `STORED AS PARQUET` — seit dem 10.07. laufen alle Schichten als Iceberg (s. ADR-13).
**Warum:** OLAP liest Aggregate über viele Zeilen — Normalisierung (3. NF) erzwingt teure Joins; Parquet ist spaltenorientiert. Iceberg ergänzt atomare Snapshot-Commits (Publish ohne halbfertige Zwischenstände), Time Travel und den Metadaten-Fingerprint für den Skip-Mechanismus (ADR-8).
**Trade-off:** Redundanz in den Dimensionen — für ein Lese-System gewollt. Details: [README → Datenmodell](../README.md).

### ADR-2 · Regionalschlüssel + `dim_gemeinde` als verbindende Dimensionen
**Kontext:** Bevölkerung und Bauland teilen den amtlichen Regionalschlüssel; Klima hat nur Städtenamen, Gemeinden keinen amtlichen Schlüssel.
**Entscheidung:** `dim_kreis` (Regionalschlüssel) als conformed dimension für Bevölkerung+Bauland; `dim_gemeinde` als Brücke zwischen Kreis- und Klimastadt-Ebene.
**Warum:** So werden aus 4 isolierten Tabellen **ein** zusammenhängendes Modell — der eigentliche Mehrwert des Datenprodukts.
**Trade-off:** Die Brücke basiert auf Namens-Matching, nicht perfekt (97,5 % Kreis-Coverage, im Contract dokumentiert).

### ADR-3 · Write-Audit-Publish in drei getrennten Stufen
**Kontext:** Rohdaten haben massive Qualitätsprobleme (Encoding, Formate); WAP/Medallion als Organisations-Pattern (Vorlesung 3).
**Entscheidung:** Drei Stufen mit eigenen Tabellen-Schichten: `staging_*` (Rohkopie) → `audit_*` (bereinigt) → `dim_*/fact_*` (veröffentlicht). Ein Skript pro Stufe.
**Warum:** Klare Verantwortung pro Stufe; roh und bereinigt liegen nebeneinander (nachvollziehbar); Stufen einzeln ausführ- und testbar.
**Trade-off:** ~2× Speicher für die Rohkopie, drei Skripte statt einem — bewusst bezahlt (Aufwand vs. Vertrauen).

### ADR-4 · Engine-Mix: Impala-SQL für Stufe 1+2, Spark für Stufe 3
**Kontext:** Vorgabe „Pipeline mit NiFi oder Spark, kein reiner Skript-Code"; Klimadaten haben 8,6 Mio. Zeilen.
**Entscheidung:** Kopieren/Bereinigen als serverseitiges `INSERT OVERWRITE/INTO … SELECT` (impyla setzt nur ab); Transformationen (Unpivot, Pivot, Window/z-Score) als PySpark DataFrame-API.
**Warum:** 8,6 Mio. Zeilen durch Python zu schleusen wäre unsinnig langsam; Spark glänzt bei den komplexen Transformationen — u. a. `STDDEV() OVER (…)`, das Impala als Window-Funktion nicht kann.
**Trade-off:** Zwei Werkzeuge im Projekt — in der Präsentation aktiv begründen.

### ADR-5 · Hybrid-I/O: Spark liest per JDBC, impyla schreibt zurück
**Kontext:** `df.write.jdbc()` gegen Impala scheitert (kein Impala-Dialekt in Spark; Treiber kann NULL-Parameter nicht binden).
**Entscheidung:** Spark nur zum Lesen/Rechnen; Schreiben via `collect()` + impyla-`INSERT INTO … VALUES`-Batches (`overwrite_table()`) in eine Shadow-Tabelle, dann EIN atomares `INSERT OVERWRITE` als Publish (s. ADR-13).
**Warum:** Impala ist ein Lese-Motor, kein robustes JDBC-Schreibziel — bewusste Architekturentscheidung. Details: [Probleme.md → Fallstudie 3](Probleme.md).
**Trade-off:** `collect()` holt Ergebnisse zum Treiber — bei ≤ ~15 k Zielzeilen unkritisch, skaliert aber nicht beliebig.
*Nachtrag 11.07.:* Gilt nur für die **lokale** Betriebsart (`SPARK_IO_MODE=jdbc`, Spark läuft außerhalb des Clusters). In CDE entfällt dieser gesamte Unterbau — Spark liest/schreibt den Iceberg-Katalog dort nativ (ADR-17).

### ADR-6 · Encoding: Laufzeit-Erkennung + automatische Auflösung über Referenzlisten
**Kontext:** In Bauland/Bevölkerung sind Umlaute als U+FFFD (`�`) gespeichert — die Information ist **weg**, kein Algorithmus kann sie zurückrechnen. Gemeinden haben intakte Umlaute; Klimadaten englische Exonyme + Kompass-Koordinaten.
**Entscheidung:** Die kaputten Kreisnamen werden zur **Laufzeit** in den Staging-Tabellen **entdeckt** (`_discover_bad_kreis_values`) und automatisch über drei Referenzlisten aufgelöst (`src/utils/german_cities.txt`, `german_regions.txt`, `german_states.txt` — Herkunft: [README → Quellen](../README.md)): das Ersatzzeichen steht für genau ein verlorenes Zeichen, der Name wird in den Listen gesucht und danach zu ASCII transliteriert. Nur 8 Sonderfälle ohne Listen-Eintrag (Berliner Bezirke, aufgelöste Altkreise) bleiben manuell (`MANUAL_KREIS_CORRECTIONS`), dazu 2 Merkmalstexte (`BAULAND_MERKMAL_CORRECTIONS`). Intakte Umlaute (Gemeinden) → echte Transliteration ä→ae/ö→oe/ü→ue; `CITY_NAME_CORRECTIONS` (Munich→Muenchen); `compass_to_signed_decimal` („5.63S"→„-5,63"). Alles serverseitig in Stufe 2.
**Warum:** Laufzeit-Erkennung statt hartcodierter ~90-Einträge-Liste — veraltet nicht bei neuen Staging-Daten, und die korrekte Schreibweise wird aus verifizierten Quellen abgeleitet statt geraten. Bricht laut ab (`RuntimeError`), wenn ein kaputter Wert weder auflösbar noch manuell hinterlegt ist.
**Ergebnis:** 0 verbleibende `�` in allen Audit-Tabellen (live verifiziert).
**Trade-off:** `strip_after_comma` kostet die Stadt/Landkreis-Unterscheidung im Namen (offen, s. [Probleme.md](Probleme.md)).

### ADR-7 · Klima-Anbindung per Namens-Join (statt Koordinaten-Distanz)
**Kontext:** Klimastädte haben keinen Regionalschlüssel. Ursprünglich: Zuordnung über die geografisch nächste Stadt (lat/long-Distanz).
**Entscheidung:** Join `gemeinde_name = stadt_name` (beide Seiten transliteriert + Exonym-Mapping); Gemeinden ohne Treffer bekommen keinen Klimawert (LEFT JOIN, im Score neutral 0).
**Warum:** Deterministisch und erklärbar; der Distanz-Join scheiterte an zerstörten Koordinaten (P3), und mehrdeutige Kurznamen („Frankfurt", „Oldenburg") wären beim Fuzzy-Match reine Raterei.
**Trade-off:** Einige Städte bleiben unangebunden (74 von 81 gematcht); über eine Korrekturliste nachschärfbar.

### ADR-8 · Incremental Loading: zweistufige Change-Detection + drei Lade-Strategien
**Kontext:** Der Scheduler läuft täglich; ein täglicher Full Load von 8,6 Mio. Klimazeilen wäre Verschwendung — und auch der „unverändert?"-Check selbst soll im Normalfall keine Zeilen scannen müssen.
**Entscheidung (Stand 11.07.):** Zweistufig:
  **Ebene 1 — Ganztabellen-Fingerprint:** Vor jeder Stufe wird EIN gespeicherter Fingerprint je (Stufe, Tabelle) verglichen — für Iceberg-Tabellen die aktuelle **Snapshot-ID** (`DESCRIBE HISTORY`, reine Metadaten-Abfrage; `table_fingerprint()` in `etl_state.py`). Unverändert → Stufe komplett übersprungen, ohne eine Zeile zu lesen. Stufe 3 vergleicht je Audit-Tabelle die Snapshot-ID, die der letzte Ziel-Build tatsächlich gelesen hat (Datenstand statt Zeitstempel).
  **Ebene 2 — drei Lade-Strategien** je Tabellenart (greift erst bei Fingerprint-Wechsel bzw. für die Nicht-Iceberg-Quellen in `default.*`):
  1. **Wasserzeichen** (`klimadaten`, echte Zeitreihe über `dt`): nur neuere Zeilen anhängen (`INSERT INTO`; die `TRUNCATE(4, dt)`-Partitionierung macht den Filter zum Partition-Prune). S. ADR-15.
  2. **Zeilengenauer Key-Merge** (`bauland`, `bevoelkerungzahlen` — verlässlicher Business-Key): zustandslos direkt gegen die Iceberg-Audit-Tabelle — `MERGE INTO … WHEN MATCHED AND NOT (t.col <=> src.col AND …)` + `DELETE` für verschwundene Keys; die Audit-Tabelle selbst ist der Vergleichsstand (s. ADR-13).
  3. **Inhalts-Prüfsumme** (`gemeinden` — kein NULL-freier Key): Full Refresh nur bei geänderter Prüfsumme (`FNV_HASH`-Aggregat, serverseitig).
  Der Zustand (Wasserzeichen, Prüfsummen, Fingerprints) liegt in `gruppe3_etl_state` — Iceberg mit echtem **UPSERT** (`MERGE INTO`, genau eine Zeile je Stufe+Tabelle); die Verlaufs-Historie liefern die Iceberg-Snapshots der State-Tabelle statt eigener App-Logik. (Die frühere Append-only-Konstruktion samt Zeilen-Hash-Historie `gruppe3_etl_row_state` existierte nur, weil Parquet kein UPDATE/MERGE kann — s. Abgelöste Entscheidungen.)
**Warum gemeinden nicht zeilengenau:** live getestet — NULL-behaftete Keys kollidieren in `CONCAT_WS`, ein Reset-Folgelauf erkannte fälschlich Änderungen; kein stabiler NULL-freier Schlüssel vorhanden (auch `name+PLZ+kreis` bleibt mehrdeutig, s. P8 in [Probleme.md](Probleme.md)). Bei ~11 k Zeilen ist die Prüfsumme robust und schnell genug.
**Trade-off:** Mehr Komplexität + eine State-Tabelle. Einzige Stelle, an der im Unverändert-Fall noch Zeilen gescannt werden: die fremden `default.*`-Quellen (kein Iceberg → kein verlässlicher Metadaten-Fingerprint → Prüfsummen-Scan als Fallback).

### ADR-9 · `run_pipeline.py` als einziger Einstiegspunkt, fail-fast
**Kontext:** Vorher vier einzelne Skript-Starts mit stiller Reihenfolge-Abhängigkeit.
**Entscheidung:** Ein Orchestrator ruft DDLs + Stufe 1→2→3 + Contract-Gate auf; bewusst **kein** try/except je Stufe; Docker-`CMD` zeigt darauf.
**Warum:** „Idempotent lauffähig" mit einem Befehl demonstrierbar; bricht eine Stufe ab, endet der Gesamtlauf mit Fehler-Exit-Code statt auf veralteten Daten weiterzurechnen.

### ADR-10 · APScheduler statt cron/Airflow
**Kontext:** Scheduler-Code ist Teil der benoteten Abgabe.
**Entscheidung:** `BlockingScheduler` + `CronTrigger(hour=0, minute=0)`, Fehler-Isolation je Lauf, `misfire_grace_time=3600`, Zeitzone Europe/Berlin; ruft täglich den kompletten `run_pipeline.main()` auf.
**Warum:** Der Zeitplan steht als **lesbarer Code im Repo** (statt Crontab außerhalb), läuft auf Windows wie Linux; dank Incremental Loading ist der Komplettlauf bei unveränderten Quellen billig.
**Trade-off:** Kein Backfilling/DAG wie Airflow. *Nachtrag 11.07.:* genau das ist inzwischen umgesetzt — produktiv orchestriert ein Airflow-DAG in Cloudera DE dieselben Stufen (s. ADR-17); der APScheduler bleibt die lokale Betriebsart.

### ADR-11 · NULL-Semantik: `safe_div` statt Infinity/NaN
**Kontext:** 748 Bauland-Zeilen haben `fläche = 0` (amtliche Rundung) → `kaufsumme/0 = Infinity` → **ein** Wert vergiftet AVG/STDDEV-Fensteraggregate eines ganzen Jahrgangs → Score-Spalte komplett NULL.
**Entscheidung:** Jede Division mit variablem Nenner läuft über `safe_div()` (NULL bei Nenner 0/NULL); NaN/Infinity werden beim Schreiben zu NULL; fehlendes Klima geht per `coalesce(…, 0)` neutral in den Score ein.
**Warum:** NULL heißt hier ehrlich „nicht berechenbar" und wird von Aggregaten übersprungen — Infinity dagegen zerstört sie. Fallstudie: [Probleme.md → Fallstudie 1](Probleme.md).

### ADR-12 · Data Contract als YAML + technisches Publish-Gate
**Kontext:** Pflicht-Deliverable; Data-Mesh-Prinzipien „Data as a Product" + „Federated Governance".
**Entscheidung:** [data_contract.yaml](../data/data_contract.yaml) nach Data-Contract-Specification: Owner, Server, Nutzungs-Terms, alle 9 Modelle mit Spalten-Semantik, Servicelevels, **live gemessene** Qualitätszahlen und Beispiel-Queries. Zusätzlich [src/contract_check.py](../src/contract_check.py) als **technisches Gate** (Stage 4/4 in `run_pipeline.py`): prüft Schema, `required`-Felder, Eindeutigkeit und ausführbare `quality`-SQLs live gegen Impala, Exit-Code 1 bei Verstoß.
**Warum:** Konsumenten sollen das Produkt nutzen können **ohne uns zu fragen**; ehrliche, messbare Qualität schafft Vertrauen. Das eigene Gate ergänzt die Data-Contract-CLI, weil die Pipeline bereits über `impyla` gegen dieselbe Umgebung läuft (die CLI scheitert lokal am HTTP-Transport, s. README).
**Verifiziert (09.07.):** `datacontract lint` grün (CLI 1.0.10). Ein testweise per `import sql` aus den Output-Port-DDLs generiertes Gerüst enthält exakt dasselbe Schema, aber keinerlei Constraints/Semantik/Terms/Quality — Vergleich und Einordnung: README, Abschnitt „Handgeschriebener Contract vs. CLI-generiertes Gerüst".
**Trade-off:** Die Qualitätszahlen sind ein datierter Snapshot und müssen bei Datenänderung nachgezogen werden.

### ADR-13 · Apache Iceberg: vom zeilengenauen Audit-Merge zur kompletten Iceberg-Pipeline *(07.07., ausgeweitet 10.07.)*
**Kontext:** `bauland`/`bevoelkerungzahlen` brauchen zeilengenaues Ersetzen einzelner Keys (ADR-8, Strategie 2). Parquet auf Impala kennt aber **kein** `UPDATE`/`DELETE`/`MERGE` — nur Full-Table-Operationen. Der bisherige Umweg: die komplette Audit-Tabelle per `CREATE TABLE … AS SELECT` (unveränderte Zeilen + frisch bereinigte) neu bauen und per `RENAME`-Swap tauschen — nötig, weil Impala nicht sicher aus einer Tabelle lesen kann, die es gerade per `INSERT OVERWRITE` überschreibt (Torn Read). Das schrieb bei jedem Lauf die **ganze** Tabelle neu, auch wenn sich nur wenige Zeilen änderten, und hinterließ bei Abbruch Restmüll.
**Entscheidung:** Zuerst (07.07.) liefen nur `gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen` als **Iceberg-Tabellen** (`STORED BY ICEBERG`, `format-version=2`); seit dem 10.07. sind **alle Tabellen aller Schichten** Iceberg — Staging, Audit, Star-Schema und ETL-State (die Klimadaten-Tabellen partitioniert per `PARTITIONED BY SPEC (TRUNCATE(4, dt))` = hidden partitioning nach Jahr). Der Swap-Umweg ist durch zwei native, **zustandslose** Statements direkt gegen die Audit-Tabelle ersetzt:
  - `MERGE INTO … WHEN MATCHED AND NOT (t.col <=> src.col AND …) THEN UPDATE / WHEN NOT MATCHED THEN INSERT` — NULL-sicherer Spaltenvergleich; die Audit-Tabelle **selbst** ist der Vergleichsstand, keine separate Zeilen-Hash-Historie nötig,
  - `DELETE` für Keys, die nicht mehr in Staging existieren (`NOT IN`-Subquery; die `CONCAT_WS`-Keys sind nie NULL).
Ein billiger `content_signature()`-Vor-Check verhindert, dass DELETE+MERGE an Tagen ohne Änderung überhaupt läuft. Weil die Quellen Duplikat-Leerzeilen enthalten (40 in `project_bauland`, 2 in `project_bevoelkerungzahlen`) und Impala-MERGE bei mehreren Quellzeilen je Ziel-Key hart abbricht („Duplicate row found", live reproduziert), wird die MERGE-Quelle per `GROUP BY` + `MIN()` deterministisch auf eine Zeile je Key verdichtet. Der Stufe-3-Publish läuft als atomarer **Shadow-Swap**: Batches in `<ziel>_wap_incoming`, dann EIN `INSERT OVERWRITE` = ein einzelner Iceberg-Snapshot-Commit (Konsumenten sehen nie einen halben Stand); die Shadow-Tabelle wird nach dem Publish gedroppt. `ensure_iceberg_audit_table()` legt fehlende Tabellen frisch als Iceberg an und bricht bei einem Parquet-Altbestand mit klarer Fehlermeldung ab — die einmalige Migration aller 20 Bestandstabellen lief am 10.07. über ein separates Skript (Zeilenzahlen 1:1 verifiziert, u. a. 8.599.212 Klimazeilen) und das Skript wurde danach aus dem Repo entfernt.
**Warum Iceberg das löst:** Iceberg ist ein **offenes Tabellenformat** (Daten bleiben Parquet-Dateien) mit einer Metadaten-/Snapshot-Schicht, die jeden Schreibvorgang als **atomaren Commit** behandelt. Damit gibt es kein Torn Read mehr — der Swap-Umweg war nur ein Workaround für eine Parquet-Einschränkung, nicht für Impala/SQL an sich. Der Anwendungscode wird dadurch einfacher.
**Verifikation (Impala 4.5.0 / Cloudera Runtime 7.3.2):** `MERGE`/`DELETE`/Time-Travel einzeln getestet; kompletter Insert/Update/Delete-Zyklus gegen isolierte Testtabelle; echte Migration der Produktionstabellen (21.600 / 581 Zeilen) mit identischer Prüfsumme vor/nach; kompletter Stufe-3-Spark-Lauf liest die Iceberg-Tabellen problemlos (generischer JDBC-Dialekt, keine Anpassung nötig).
**Vier Iterationen — Lehre:** (1) Umsetzung mit Zeilen-Hash-Historie (`gruppe3_etl_row_state`) als Change-Detection. (2) Selbstkritik: Historie durch direkten SQL-Vergleich gegen die Iceberg-Tabelle ersetzt — plus robustere Iceberg-Erkennung (`table_type`-Spalte statt Substring-Suche) und Migration als separates Skript. (3) **Rückgängig gemacht** wegen der Behauptung, ein „Incremental Scheduler" konsumiere die Historie. (4) Diese Behauptung beim Nachprüfen als **falsch** entlarvt (kein Code außerhalb von Stufe 2 hat die Tabellen je gelesen — Grep über Funktions- UND Tabellennamen plus Live-Blick in die Datenbank) → der Merge ist seit 10.07. endgültig zustandslos, die Hash-Tabellen sind gelöscht. *Lehre: WER der Konsument eines States ist, gehört mit Dateiname/Zeile in die ADR — eine Abhängigkeit, die sich nicht benennen lässt, existiert mit hoher Wahrscheinlichkeit nicht.*
**Trade-offs:** `MERGE`/`DELETE` sind jüngere Impala-Features (aber gegen den zuvor selbstgebauten Workaround kein Rückschritt); **kein Compaction/Snapshot-Management** ergänzt (Iceberg läuft `merge-on-read` → über sehr viele Läufe sammeln sich Delete-Dateien; bei dieser Datenmenge unkritisch, bewusst als Ausblick offen gelassen).

### ADR-14 · Konfiguration über Umgebungsvariablen *(neu 07.07.)*
**Kontext:** Datenbank- und Präfix-Namen (`gruppe3`, `gruppe3_`, Quell-DB `default`) waren in jedem Skript hartcodiert.
**Entscheidung:** `DATABASE` (Default `gruppe3`), `PREFIX` (Default `gruppe3_`) und `SOURCE_DATABASE` (Default `default`) kommen aus der `.env` (`os.getenv(..., default)`, s. `.env.example`). Ohne gesetzte Variablen bleibt alles wie bisher.
**Warum:** Die Pipeline läuft ohne Code-Änderung gegen eine andere Gruppen-DB / ein anderes Präfix (andere Gruppe, Test-DB) — nützlich für Wiederverwendung und isolierte Tests.
**Trade-off / Ausnahme:** `src/utils/reset_database.py` **hartcodiert** bewusst `DATABASE = "gruppe3"` — ein `DROP TABLE`-Skript soll sich nicht per Env-Var auf eine fremde Datenbank umlenken lassen.

### ADR-15 · Klimadaten bleiben Wasserzeichen — kein zeilengenauer Merge *(neu 07.07.)*
**Kontext:** Es wurde geprüft, ob `klimadaten` (wie `bauland`/`bevoelkerungzahlen`) auf einen zeilengenauen Merge umgestellt werden sollte, um nachträgliche Korrekturen historischer Messwerte per `UPDATE` zu ermöglichen. Ein gültiger Schlüssel existiert sogar (`city + dt`, eindeutig, NULL-frei, auch nach der Transliteration kollisionsfrei).
**Entscheidung:** `klimadaten` bleibt bei der **Wasserzeichen**-Strategie (ADR-8, Strategie 1) — kein zeilengenauer Merge. *(Physisch sind auch die Klimadaten-Tabellen seit 10.07. Iceberg — atomare Appends, `TRUNCATE(4, dt)`-Partitionierung als Partition-Prune für den Wasserzeichen-Filter, s. ADR-13; das ändert die Lade-Strategie nicht.)*
**Warum:** Die Quelle `default.project_klimadaten` ist ein **statisches historisches Archiv** (endet 2013, read-only) — historische Werte ändern sich faktisch nie. Damit trifft die Grundannahme des Wasserzeichens („Historie ist unveränderlich") zu, und es ist bei 8,6 Mio. Zeilen die **effizienteste** Wahl. Ein zeilengenauer Merge würde ein Problem lösen, das diese Daten nicht haben — und wäre inkonsistent, weil die Staging-Stufe (aus Performance-Gründen) ohnehin per Wasserzeichen lädt und Quell-Korrekturen gar nicht erst durchreichen würde.
**Trade-off:** Würde die Quelle jemals änderbar (mutabel), müsste man beide Stufen auf Merge umstellen (Staging = 8,6 Mio. Zeilen hashen pro Lauf → unpraktisch). Für die reale Datenlage ist das Wasserzeichen korrekt; die zeilengenaue Merge-Fähigkeit ist am Beispiel `bauland`/`bevoelkerungzahlen` (ADR-13) bereits demonstriert.

### ADR-16 · `JAVA_HOME_JDK17` wird vor dem `pyspark`-Import selbst gesetzt *(neu 07.07.)*
**Kontext:** Spark 3.5.x startet seine JVM beim ersten `SparkSession`-Aufruf mit dem dann aktuellen `JAVA_HOME`. Auf einem System-Default-JDK ≥ 23/24 scheitert es an einer entfernten Hadoop-Security-API (`UnsupportedOperationException: getSubject is not supported`). `scheduler.py` setzte `JAVA_HOME` aus `.env` (`JAVA_HOME_JDK17`) bereits, `pipeline_audit_to_target.py` beim Direktaufruf jedoch nicht.
**Entscheidung:** `pipeline_audit_to_target.py` übernimmt `JAVA_HOME_JDK17` jetzt selbst — **vor** dem `pyspark`-Import — mit derselben Technik wie `scheduler.py`.
**Warum:** Der direkte Aufruf `python src/pipeline_audit_to_target.py` funktioniert damit ohne manuelles Setzen der Umgebungsvariable; `.env` mit `JAVA_HOME_JDK17` genügt. Details: [Probleme.md → Fallstudie 3, Stolperstein 1](Probleme.md).

### ADR-17 · Zweite Betriebsart: Airflow-DAG in Cloudera Data Engineering + `SPARK_IO_MODE` *(neu 11.07., live)*
**Kontext:** Der APScheduler (ADR-10) läuft lokal in einem Docker-Container — produktiv gehört der Taktgeber auf die Plattform (Schedule, Monitoring, Retries, Wiederaufsetzen ab der gescheiterten Stufe). Gleichzeitig existiert der gesamte JDBC-Unterbau von Stufe 3 (Treiber-Jar, `collect()` + `VALUES`-Batches + Shadow-Swap, ADR-5) **nur**, weil Spark lokal außerhalb des Clusters läuft und nichts außer dem Impala-Endpoint sieht.
**Entscheidung:** Die Pipeline bekommt eine zweite, gleichberechtigte Betriebsart in CDE — **ohne die Pipeline-Logik zu duplizieren**: [cde/pipeline_dag.py](../cde/pipeline_dag.py) (Airflow, `CDEJobRunOperator`) ruft DIESELBEN `src/`-Module als CDE-Jobs in derselben Reihenfolge auf (Stufe 0→1→2→3→4; täglich 05:00 UTC, `max_active_runs=1`, Retry mit 5 min Abstand). In Stufe 3 kapselt der Schalter `SPARK_IO_MODE` („jdbc" = Default | „catalog") **ausschließlich die I/O-Schicht**: im Cluster liest/schreibt Spark Iceberg nativ über den Katalog (`spark.read.table()`; ein natives `INSERT OVERWRITE` ist bereits ein einzelner atomarer Snapshot-Commit — WAP-Garantie identisch zum lokalen Shadow-Swap, kein JDBC-Jar, kein `collect()`-Umweg). Alle `build_*`-Funktionen und der Fingerprint-/State-Mechanismus sind in beiden Modi byte-identisch; beide Betriebsarten teilen sich `gruppe3_etl_state` (Fingerprint-Skip wirkt über die Grenze hinweg). Betriebsregel: nie beide **Scheduler** parallel aktiv.
**Warum:** Ein Repo, zwei Einstiegspunkte — ein frisch gezogenes Repo läuft lokal ohne Vorwissen (Default „jdbc"), und produktiv übernimmt die Plattform Orchestrierung und Monitoring statt eines dauerlaufenden Containers. Genau der in ADR-10 dokumentierte „Ausblick", jetzt umgesetzt.
**Status:** **Deployt und aktiv seit 11.07.2026** — Resources, die fünf Stufen-Jobs und der DAG `gruppe3_data_mesh_pipeline` sind im Virtual Cluster angelegt (Anleitung: [README → CDE-Deployment](../README.md)). Deploy-Stolperstein: Airflow legt neue DAGs standardmäßig **pausiert** an, und der Pause-Toggle ließ sich in der CDE-Airflow-UI nicht zuverlässig bedienen → `is_paused_upon_creation=False` direkt im DAG, damit der Schedule ohne UI-Interaktion anläuft (P10 in [Probleme.md](Probleme.md)).
**Trade-off:** Zwei I/O-Pfade in Stufe 3, die gepflegt werden müssen — der Katalog-Modus ist nur im Virtual Cluster testbar (Abnahme-Checkliste im [README → CDE-Abnahme](../README.md): Einzel-Job-Läufe mit Skip-Meldungen, erzwungener Build mit Zeilenzahl-Abgleich gegen den lokalen Referenz-Rebuild, kompletter DAG-Lauf mit 32/32 Contract-Checks). Zugangsdaten liegen als Env-Vars in der Job-Config — für die Abgabe ok und nur für Projektmitglieder sichtbar; sauberer wären Airflow-Connections/CDE-Credentials.

---

## Abgelöste Entscheidungen (was früher galt)

| Alt | Warum verworfen | Ersetzt durch |
|---|---|---|
| **Reine Impala-SQL-Pipeline** (`pipeline.py`, alles per SQL-Strings) | Vorgabe Spark/NiFi; `STDDEV` als Window-Funktion in Impala nicht möglich (z-Score!) | ADR-4/5 (Spark-Stufe 3) |
| **Alte Rohdaten-Kopien in der Gruppen-DB** | Koordinaten in der Kopie durch CSV-Bug zerstört; Original `default.project_*` ist intakt | Stufe 1 liest `default.project_*` (ADR-3) |
| **Koordinaten-Distanz-Join** Gemeinde→nächste Klimastadt | Scheiterte an zerstörten Koordinaten (P3); danach bewusst der einfachere Namens-Join | ADR-7 |
| **„Kaputte Zeichen einheitlich entfernen"** (`L�beck`→`Lbeck`) | Erzeugt falsche Namen, die nirgends mehr matchen | ADR-6 (Auflösung über Referenzlisten) |
| **Hartcodierte `KREIS_CORRECTIONS`** (~90 Einträge im Code) | Veraltet bei neuen Staging-Daten; Pflegeaufwand | ADR-6 (Laufzeit-Erkennung + Referenzlisten, 07.07.) |
| **`CREATE TABLE … AS SELECT` + Rename-Swap** für den Audit-Merge | Umweg nur nötig, weil Parquet kein UPDATE/MERGE kann; schrieb jedes Mal die ganze Tabelle neu | ADR-13 (Iceberg `MERGE`/`DELETE`, 07.07.) |
| **Append-only ETL-State + Zeilen-Hash-Historie** (`gruppe3_etl_row_state`) | Existierte nur, weil Parquet kein UPDATE/MERGE kann; Historie doppelt gepflegt, ein Python-Roundtrip je Lauf | ADR-8/13 (Iceberg-UPSERT + zustandsloser SQL-Merge, 10.07.) |
| **`TRUNCATE` + Direktschreiben in die sichtbaren Zieltabellen** (Stufe 3) | Ein Konsument konnte währenddessen eine leere/halbe Tabelle lesen | ADR-13 (Shadow-Tabelle + ein atomares `INSERT OVERWRITE`, 10.07.) |
| **Voll-Prüfsummen-Scan als „unverändert?"-Check bei jedem Lauf** | Las jede Zeile jeder Tabelle, auch wenn nichts passiert war | ADR-8 (Ebene-1-Fingerprint via Iceberg-Snapshot-ID, 11.07.) |
| **`spark.jars`** zum Einbinden des JDBC-Treibers | Braucht unter Windows `winutils.exe` (Crash) | `spark.driver/executor.extraClassPath` |
| **`df.write.jdbc()`** zum Zurückschreiben | Treiber kann NULL-Parameter nicht typisieren; hat einmal eine Tabelle gedroppt! | ADR-5 (impyla-Batches) |
| **`spark.createDataFrame(python_liste)`** (dim_jahr) | Python↔JVM-Socket wird von VPN gestört („Accept timed out") | `F.explode(F.sequence(…))` rein JVM-seitig |
| **Full Load in jeder Stufe, jeden Lauf** | Täglicher Rewrite von 8,6 Mio. Zeilen ohne Not | ADR-8 (Incremental) |
| **Vier einzelne Skript-Starts**, Docker-CMD = nur Stufe 3 | Stille Reihenfolge-Abhängigkeiten | ADR-9 (`run_pipeline.py`) |
| **`wohnraumdruck_index` als Verhältnis zweier Wachstumsraten** | Vorzeichen-Falle: −/− = +; spiegelte die Vorzeichen-Kombination, nicht den Druck | Verhältnis von Bestandsgrößen (Einwohner je 1000 m² Bauland), nie negativ |
| **3 von 4 Bauland-Merkmalen pivotiert** | Das 4. (amtl. Kaufwert/qm) sollte dazu — stellte sich als ab Quelle zerstört heraus (P5) | Alle 4 pivotiert; Spalte im Contract als „nicht verwenden" markiert |

## Gelöste Probleme & offene Punkte

Bewusst **nicht** hier dupliziert → [Probleme.md](Probleme.md) ist die einzige
Problemliste (P1–P10 mit Fallstudien, nicht behebbare Einschränkungen, offene Punkte).
