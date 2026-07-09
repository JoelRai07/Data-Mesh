# Architektur-Entscheidungen (ADR)

Dieses Dokument ist die **zentrale, vollständige Sammlung aller wichtigen
Architektur-Entscheidungen** des Projekts. Jede Entscheidung hält fest:
**Kontext → Entscheidung → Warum → Trade-off.** Am Ende: abgelöste
Entscheidungen (was früher galt) und gelöste Probleme.

> Für Benutzung/Setup → [README.md](README.md) · Konsumenten-Sicht →
> [docs/data_contract.yaml](docs/data_contract.yaml) · Datenmodell-Begründung →
> [docs/datenmodell_begruendung.md](docs/datenmodell_begruendung.md) · offene
> Punkte → [TODO.md](TODO.md).

## Architektur in 30 Sekunden

```
default.project_* ──(1) Staging──▶ gruppe3_staging_* ──(2) Audit──▶ gruppe3_audit_* ──(3) Publish──▶ dim_*/fact_* ──(4) Gate
                    impyla, inkrementell        impyla, Bereinigung + inkrementell        Spark, Star-Schema      Data Contract
```

Write-Audit-Publish in drei Stufen (ADR-3), drei Lade-Strategien je
Tabellenart (ADR-8), Bereinigung in Impala-SQL / Transformationen in Spark
(ADR-4/5), Orchestrierung über `run_pipeline.py` (ADR-9), täglicher Trigger
(ADR-10), Data Contract als Output-Port + technisches Gate (ADR-12).

---

## Aktive Entscheidungen

### ADR-1 · Star-Schema, denormalisiert, als Parquet
**Kontext:** 4 Quelltabellen, analytischer Use Case „Standortprofil-Dashboard" (OLAP, lesen/aggregieren).
**Entscheidung:** 4 Dimensionen + 5 Fakten, denormalisiert (z. B. Bundesland direkt in `dim_kreis`), `STORED AS PARQUET`.
**Warum:** OLAP liest Aggregate über viele Zeilen — Normalisierung (3. NF) erzwingt teure Joins; Parquet ist spaltenorientiert.
**Trade-off:** Redundanz in den Dimensionen — für ein Lese-System gewollt. Details: [datenmodell_begruendung.md](docs/datenmodell_begruendung.md).

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
**Entscheidung:** Spark nur zum Lesen/Rechnen; Schreiben via `collect()` + impyla-`INSERT INTO … VALUES`-Batches (`overwrite_table()`), `TRUNCATE` separat per impyla.
**Warum:** Impala ist ein Lese-Motor, kein robustes JDBC-Schreibziel — bewusste Architekturentscheidung. Details: [spark_stolpersteine.md](docs/spark_stolpersteine.md).
**Trade-off:** `collect()` holt Ergebnisse zum Treiber — bei ≤ ~15 k Zielzeilen unkritisch, skaliert aber nicht beliebig.

### ADR-6 · Encoding: Laufzeit-Erkennung + automatische Auflösung über Referenzlisten
**Kontext:** In Bauland/Bevölkerung sind Umlaute als U+FFFD (`�`) gespeichert — die Information ist **weg**, kein Algorithmus kann sie zurückrechnen. Gemeinden haben intakte Umlaute; Klimadaten englische Exonyme + Kompass-Koordinaten.
**Entscheidung:** Die kaputten Kreisnamen werden zur **Laufzeit** in den Staging-Tabellen **entdeckt** (`_discover_bad_kreis_values`) und automatisch über drei Referenzlisten aufgelöst (`src/utils/german_cities.txt`, `german_regions.txt`, `german_states.txt` — Herkunft: [quellen.txt](quellen.txt)): das Ersatzzeichen steht für genau ein verlorenes Zeichen, der Name wird in den Listen gesucht und danach zu ASCII transliteriert. Nur 8 Sonderfälle ohne Listen-Eintrag (Berliner Bezirke, aufgelöste Altkreise) bleiben manuell (`MANUAL_KREIS_CORRECTIONS`), dazu 2 Merkmalstexte (`BAULAND_MERKMAL_CORRECTIONS`). Intakte Umlaute (Gemeinden) → echte Transliteration ä→ae/ö→oe/ü→ue; `CITY_NAME_CORRECTIONS` (Munich→Muenchen); `compass_to_signed_decimal` („5.63S"→„-5,63"). Alles serverseitig in Stufe 2.
**Warum:** Laufzeit-Erkennung statt hartcodierter ~90-Einträge-Liste — veraltet nicht bei neuen Staging-Daten, und die korrekte Schreibweise wird aus verifizierten Quellen abgeleitet statt geraten. Bricht laut ab (`RuntimeError`), wenn ein kaputter Wert weder auflösbar noch manuell hinterlegt ist.
**Ergebnis:** 0 verbleibende `�` in allen Audit-Tabellen (live verifiziert).
**Trade-off:** `strip_after_comma` kostet die Stadt/Landkreis-Unterscheidung im Namen (offen, s. TODO).

### ADR-7 · Klima-Anbindung per Namens-Join (statt Koordinaten-Distanz)
**Kontext:** Klimastädte haben keinen Regionalschlüssel. Ursprünglich: Zuordnung über die geografisch nächste Stadt (lat/long-Distanz).
**Entscheidung:** Join `gemeinde_name = stadt_name` (beide Seiten transliteriert + Exonym-Mapping); Gemeinden ohne Treffer bekommen keinen Klimawert (LEFT JOIN, im Score neutral 0).
**Warum:** Deterministisch und erklärbar; der Distanz-Join scheiterte an zerstörten Koordinaten (P3), und mehrdeutige Kurznamen („Frankfurt", „Oldenburg") wären beim Fuzzy-Match reine Raterei.
**Trade-off:** Einige Städte bleiben unangebunden; über eine Korrekturliste nachschärfbar.

### ADR-8 · Incremental Loading mit drei Strategien + append-only State
**Kontext:** Der Scheduler läuft täglich; ein täglicher Full Load von 8,6 Mio. Klimazeilen wäre Verschwendung.
**Entscheidung:** Je Tabellenart die passende Strategie (Incremental-Loader-Pattern):
  1. **Wasserzeichen** (`klimadaten`, echte Zeitreihe über `dt`): nur neuere Zeilen anhängen (`INSERT INTO`). S. ADR-15, warum das hier die *richtige* Wahl ist.
  2. **Zeilengenauer Key-Merge** (`bauland`, `bevoelkerungzahlen` — verlässlicher Business-Key): FNV-Hash je Zeile (`gruppe3_etl_row_state`), nur neue/geänderte/gelöschte Keys ersetzen. Läuft seit 07.07. als **Apache-Iceberg** mit echtem `DELETE`/`MERGE INTO` (s. ADR-13).
  3. **Inhalts-Prüfsumme** (`gemeinden` — kein NULL-freier Key): Full Refresh nur bei geänderter Prüfsumme der Quelle.
  Der Zustand liegt in `gruppe3_etl_state`/`gruppe3_etl_row_state` — **append-only** (jüngster Eintrag gewinnt), weil Zeilen-Updates auf Parquet nicht existieren. Stufe 3 überspringt den ganzen Spark-Lauf, wenn kein Audit-Stand neuer ist als der letzte Ziel-Build.
**Warum gemeinden nicht zeilengenau:** live getestet — NULL-behaftete Keys kollidieren in `CONCAT_WS`, ein Reset-Folgelauf erkannte fälschlich Änderungen; kein stabiler NULL-freier Schlüssel vorhanden (auch `name+PLZ+kreis` bleibt mehrdeutig, s. P8). Bei ~11 k Zeilen ist die Prüfsumme robust und schnell genug.
**Trade-off:** Mehr Komplexität + State-Tabellen. Der frühere Bug „Wasserzeichen wird bei jedem Lauf neu geschrieben → Stufe-3-Skip greift nie" ist gefixt (`record_state()` nur bei tatsächlicher Änderung).

### ADR-9 · `run_pipeline.py` als einziger Einstiegspunkt, fail-fast
**Kontext:** Vorher vier einzelne Skript-Starts mit stiller Reihenfolge-Abhängigkeit.
**Entscheidung:** Ein Orchestrator ruft DDLs + Stufe 1→2→3 + Contract-Gate auf; bewusst **kein** try/except je Stufe; Docker-`CMD` zeigt darauf.
**Warum:** „Idempotent lauffähig" mit einem Befehl demonstrierbar; bricht eine Stufe ab, endet der Gesamtlauf mit Fehler-Exit-Code statt auf veralteten Daten weiterzurechnen.

### ADR-10 · APScheduler statt cron/Airflow
**Kontext:** Scheduler-Code ist Teil der benoteten Abgabe.
**Entscheidung:** `BlockingScheduler` + `CronTrigger(hour=0, minute=0)`, Fehler-Isolation je Lauf, `misfire_grace_time=3600`, Zeitzone Europe/Berlin; ruft täglich den kompletten `run_pipeline.main()` auf.
**Warum:** Der Zeitplan steht als **lesbarer Code im Repo** (statt Crontab außerhalb), läuft auf Windows wie Linux; dank Incremental Loading ist der Komplettlauf bei unveränderten Quellen billig.
**Trade-off:** Kein Backfilling/DAG wie Airflow — produktiv gehörte das auf Cloudera DE (Ausblick in der Präsentation).

### ADR-11 · NULL-Semantik: `safe_div` statt Infinity/NaN
**Kontext:** 748 Bauland-Zeilen haben `flaeche = 0` (amtliche Rundung) → `kaufsumme/0 = Infinity` → **ein** Wert vergiftet AVG/STDDEV-Fensteraggregate eines ganzen Jahrgangs → Score-Spalte komplett NULL.
**Entscheidung:** Jede Division mit variablem Nenner läuft über `safe_div()` (NULL bei Nenner 0/NULL); NaN/Infinity werden beim Schreiben zu NULL; fehlendes Klima geht per `coalesce(…, 0)` neutral in den Score ein.
**Warum:** NULL heißt hier ehrlich „nicht berechenbar" und wird von Aggregaten übersprungen — Infinity dagegen zerstört sie. Fallstudie: [bugfix_score_nullwerte.md](docs/bugfix_score_nullwerte.md).

### ADR-12 · Data Contract als YAML + technisches Publish-Gate
**Kontext:** Pflicht-Deliverable; Data-Mesh-Prinzipien „Data as a Product" + „Federated Governance".
**Entscheidung:** [data_contract.yaml](docs/data_contract.yaml) nach Data-Contract-Specification: Owner, Server, Nutzungs-Terms, alle 9 Modelle mit Spalten-Semantik, Servicelevels, **live gemessene** Qualitätszahlen und Beispiel-Queries. Zusätzlich [src/contract_check.py](src/contract_check.py) als **technisches Gate** (Stage 4/4 in `run_pipeline.py`): prüft Schema, `required`-Felder, Eindeutigkeit und ausführbare `quality`-SQLs live gegen Impala, Exit-Code 1 bei Verstoß.
**Warum:** Konsumenten sollen das Produkt nutzen können **ohne uns zu fragen**; ehrliche, messbare Qualität schafft Vertrauen. Das eigene Gate ergänzt die Data-Contract-CLI, weil die Pipeline bereits über `impyla` gegen dieselbe Umgebung läuft (die CLI scheitert lokal am HTTP-Transport, s. README).
**Trade-off:** Die Qualitätszahlen sind ein datierter Snapshot und müssen bei Datenänderung nachgezogen werden.

### ADR-13 · Apache Iceberg für den zeilengenauen Audit-Merge *(neu 07.07.)*
**Kontext:** `bauland`/`bevoelkerungzahlen` brauchen zeilengenaues Ersetzen einzelner Keys (ADR-8, Strategie 2). Parquet auf Impala kennt aber **kein** `UPDATE`/`DELETE`/`MERGE` — nur Full-Table-Operationen. Der bisherige Umweg: die komplette Audit-Tabelle per `CREATE TABLE … AS SELECT` (unveränderte Zeilen + frisch bereinigte) neu bauen und per `RENAME`-Swap tauschen — nötig, weil Impala nicht sicher aus einer Tabelle lesen kann, die es gerade per `INSERT OVERWRITE` überschreibt (Torn Read). Das schrieb bei jedem Lauf die **ganze** Tabelle neu, auch wenn sich nur wenige Zeilen änderten, und hinterließ bei Abbruch Restmüll.
**Entscheidung:** `gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen` laufen als **Iceberg-Tabellen** (`STORED BY ICEBERG`, `format-version=2`). Der Swap-Umweg wird durch zwei native Statements ersetzt — ausgelöst für genau die Keys, die die Hash-Historie (`gruppe3_etl_row_state`) als neu/geändert/gelöscht meldet:
  - `DELETE` für echt gelöschte Keys (in Changed-Keys, aber nicht mehr in Staging),
  - `MERGE INTO` für betroffene Keys (`WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`).
Unveränderte Zeilen werden gar nicht mehr angefasst. `ensure_iceberg_audit_table()` legt eine fehlende Tabelle frisch als Iceberg an; existiert sie noch als Parquet, bricht sie mit einer klaren Fehlermeldung ab (die einmalige Migration ist ein separates Skript, [src/utils/migrate_audit_tables_to_iceberg.py](src/utils/migrate_audit_tables_to_iceberg.py)).
**Warum Iceberg das löst:** Iceberg ist ein **offenes Tabellenformat** (Daten bleiben Parquet-Dateien) mit einer Metadaten-/Snapshot-Schicht, die jeden Schreibvorgang als **atomaren Commit** behandelt. Damit gibt es kein Torn Read mehr — der Swap-Umweg war nur ein Workaround für eine Parquet-Einschränkung, nicht für Impala/SQL an sich. Der Anwendungscode wird dadurch einfacher.
**Verifikation (Impala 4.5.0 / Cloudera Runtime 7.3.2):** `MERGE`/`DELETE`/Time-Travel einzeln getestet; kompletter Insert/Update/Delete-Zyklus gegen isolierte Testtabelle; echte Migration der Produktionstabellen (21.600 / 581 Zeilen) mit identischer Prüfsumme vor/nach; kompletter Stufe-3-Spark-Lauf liest die Iceberg-Tabellen problemlos (generischer JDBC-Dialekt, keine Anpassung nötig).
**Drei Iterationen — Lehre:** (1) Umsetzung. (2) Selbstkritik: die Row-Hash-Historie testweise entfernt und durch direkten SQL-Vergleich gegen die Iceberg-Tabelle ersetzt — plus robustere Iceberg-Erkennung (`table_type` statt Substring-Suche) und Migration als separates Skript. (3) **Rückgängig gemacht**, weil `gruppe3_etl_row_state` außerhalb dieser Funktion (Incremental Scheduler) gebraucht wird — ein Konsument, den ein Grep im eigenen Code nicht fand. Die anderen zwei Verbesserungen aus (2) blieben. *Lehre: eine Vereinfachung, die nur den gerade bearbeiteten Code liest, kann Konsumenten desselben States übersehen.*
**Trade-offs:** `MERGE`/`DELETE` sind jüngere Impala-Features (aber gegen den zuvor selbstgebauten Workaround kein Rückschritt); Migration betrifft Produktionsdaten und ist ein bewusster, separater Schritt; **kein Compaction/Snapshot-Management** ergänzt (Iceberg läuft `merge-on-read` → über sehr viele Läufe sammeln sich Delete-Dateien; bei dieser Datenmenge unkritisch, bewusst als Ausblick offen gelassen).

### ADR-14 · Konfiguration über Umgebungsvariablen *(neu 07.07.)*
**Kontext:** Datenbank- und Präfix-Namen (`gruppe3`, `gruppe3_`, Quell-DB `default`) waren in jedem Skript hartcodiert.
**Entscheidung:** `DATABASE` (Default `gruppe3`), `PREFIX` (Default `gruppe3_`) und `SOURCE_DATABASE` (Default `default`) kommen aus der `.env` (`os.getenv(..., default)`, s. `.env.example`). Ohne gesetzte Variablen bleibt alles wie bisher.
**Warum:** Die Pipeline läuft ohne Code-Änderung gegen eine andere Gruppen-DB / ein anderes Präfix (andere Gruppe, Test-DB) — nützlich für Wiederverwendung und isolierte Tests.
**Trade-off / Ausnahme:** `src/utils/reset_database.py` **hartcodiert** bewusst `DATABASE = "gruppe3"` — ein `DROP TABLE`-Skript soll sich nicht per Env-Var auf eine fremde Datenbank umlenken lassen.

### ADR-15 · Klimadaten bleiben Wasserzeichen — Iceberg bewusst NICHT *(neu 07.07.)*
**Kontext:** Es wurde geprüft, ob `klimadaten` (wie `bauland`/`bevoelkerungzahlen`) auf Iceberg + zeilengenauen Merge umgestellt werden sollte, um nachträgliche Korrekturen historischer Messwerte per `UPDATE` zu ermöglichen. Ein gültiger Schlüssel existiert sogar (`city + dt`, eindeutig, NULL-frei, auch nach der Transliteration kollisionsfrei).
**Entscheidung:** `klimadaten` bleibt bei der **Wasserzeichen**-Strategie (ADR-8, Strategie 1). Keine Iceberg-Umstellung.
**Warum:** Die Quelle `default.project_klimadaten` ist ein **statisches historisches Archiv** (endet 2013, read-only) — historische Werte ändern sich faktisch nie. Damit trifft die Grundannahme des Wasserzeichens („Historie ist unveränderlich") zu, und es ist bei 8,6 Mio. Zeilen die **effizienteste** Wahl. Ein zeilengenauer Merge würde ein Problem lösen, das diese Daten nicht haben — und wäre inkonsistent, weil die Staging-Stufe (aus Performance-Gründen) ohnehin per Wasserzeichen lädt und Quell-Korrekturen gar nicht erst durchreichen würde.
**Trade-off:** Würde die Quelle jemals änderbar (mutabel), müsste man beide Stufen auf Merge umstellen (Staging = 8,6 Mio. Zeilen hashen pro Lauf → unpraktisch). Für die reale Datenlage ist das Wasserzeichen korrekt; die zeilengenaue Merge-Fähigkeit ist am Beispiel `bauland`/`bevoelkerungzahlen` (ADR-13) bereits demonstriert.

### ADR-16 · `JAVA_HOME_JDK17` wird vor dem `pyspark`-Import selbst gesetzt *(neu 07.07.)*
**Kontext:** Spark 3.5.x startet seine JVM beim ersten `SparkSession`-Aufruf mit dem dann aktuellen `JAVA_HOME`. Auf einem System-Default-JDK ≥ 23/24 scheitert es an einer entfernten Hadoop-Security-API (`UnsupportedOperationException: getSubject is not supported`). `scheduler.py` setzte `JAVA_HOME` aus `.env` (`JAVA_HOME_JDK17`) bereits, `pipeline_audit_to_target.py` beim Direktaufruf jedoch nicht.
**Entscheidung:** `pipeline_audit_to_target.py` übernimmt `JAVA_HOME_JDK17` jetzt selbst — **vor** dem `pyspark`-Import — mit derselben Technik wie `scheduler.py`.
**Warum:** Der direkte Aufruf `python src/pipeline_audit_to_target.py` (so in der eigenen „Ausführen"-Zeile beschrieben) funktioniert damit ohne manuelles Setzen der Umgebungsvariable; `.env` mit `JAVA_HOME_JDK17` genügt. Details: [spark_stolpersteine.md](docs/spark_stolpersteine.md), Stolperstein 1.

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
| **`spark.jars`** zum Einbinden des JDBC-Treibers | Braucht unter Windows `winutils.exe` (Crash) | `spark.driver/executor.extraClassPath` |
| **`df.write.jdbc()`** zum Zurückschreiben | Treiber kann NULL-Parameter nicht typisieren; hat einmal eine Tabelle gedroppt! | ADR-5 (impyla-Batches) |
| **`spark.createDataFrame(python_liste)`** (dim_jahr) | Python↔JVM-Socket wird von VPN gestört („Accept timed out") | `F.explode(F.sequence(…))` rein JVM-seitig |
| **Full Load in jeder Stufe, jeden Lauf** | Täglicher Rewrite von 8,6 Mio. Zeilen ohne Not | ADR-8 (Incremental) |
| **Vier einzelne Skript-Starts**, Docker-CMD = nur Stufe 3 | Stille Reihenfolge-Abhängigkeiten | ADR-9 (`run_pipeline.py`) |
| **`wohnraumdruck_index` als Verhältnis zweier Wachstumsraten** | Vorzeichen-Falle: −/− = +; spiegelte die Vorzeichen-Kombination, nicht den Druck | Verhältnis von Bestandsgrößen (Einwohner je 1000 m² Bauland), nie negativ |
| **3 von 4 Bauland-Merkmalen pivotiert** | Das 4. (amtl. Kaufwert/qm) sollte dazu — stellte sich als ab Quelle zerstört heraus (P5) | Alle 4 pivotiert; Spalte im Contract als „nicht verwenden" markiert |

## Gelöste Probleme (Kurzreferenz)

| # | Problem | Lösung | Details |
|---|---|---|---|
| P1 | `standortattraktivitaets_score` komplett NULL | Division-durch-0 → Infinity → vergiftete Window-Aggregate → NaN → NULL; `safe_div` + Klima-`coalesce` | [bugfix_score_nullwerte.md](docs/bugfix_score_nullwerte.md), ADR-11 |
| P2 | Encoding: `L�beck` & Co. in 2 Quelltabellen | Auflösung über Referenzlisten + Transliteration in Stufe 2; 0 Reste verifiziert | ADR-6 |
| P3 | Alle Gemeinde-Koordinaten NULL → Klima-Brücke tot | Ursache: zerstörte `gruppe3`-Kopie; Umstieg auf intakte `default.project_gemeinden` + Dezimalkomma-Parsing | [bugfix_score_nullwerte.md](docs/bugfix_score_nullwerte.md) |
| P4 | 6 Spark/JDBC-Stolpersteine | je eigener Workaround | [spark_stolpersteine.md](docs/spark_stolpersteine.md) |
| P5 | `kaufwert_je_qm_eur` enthält nur 0/NULL | **Nicht behebbar:** amtliche Dezimalwerte beim Quellimport in BIGINT geladen → im Contract als „NICHT VERWENDEN" markiert, `preis_pro_qm_eur` ist der Ersatz | data_contract.yaml |
| P6 | APScheduler-Crash beim Loggen von `next_run_time` | Zeit direkt vom Trigger erfragen | [scheduler_bug.md](docs/scheduler_bug.md) |
| P7 | Cloudera-JDBC „Error converting value to double" (`per_km2`, `area_km2`) | Spalten als STRING lesen (`customSchema`) bzw. Kennzahl selbst berechnen | [spark_stolpersteine.md](docs/spark_stolpersteine.md) |
| P8 | Zeilengenauer Merge für `gemeinden` erkannte Phantom-Änderungen | NULL-Keys kollabieren in `CONCAT_WS`; kein stabiler NULL-freier Schlüssel (empirisch geprüft: selbst `name+PLZ+kreis` bleibt mehrdeutig) → bewusst Tabellen-Prüfsumme | ADR-8 |
| P9 | Spark scheitert an System-JDK ≥ 23/24 (`getSubject`) | `JAVA_HOME_JDK17` vor `pyspark`-Import selbst setzen | ADR-16, [spark_stolpersteine.md](docs/spark_stolpersteine.md) |

## Offene Punkte

Bewusst **nicht** hier dupliziert → [TODO.md](TODO.md) ist die einzige Aufgabenliste.
