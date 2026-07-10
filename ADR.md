# ADR: Apache Iceberg — vom zeilengenauen Merge in Stufe 2 zur kompletten Iceberg-Pipeline

**Status:** umgesetzt, end-to-end live verifiziert. Aktueller Stand ist
**Iteration 4 (10.07.2026)**: ALLE Tabellen aller Schichten (Staging, Audit,
Star-Schema, ETL-State) laufen als Iceberg; der zeilengenaue Merge ist
zustandslos (die Iceberg-Audit-Tabelle selbst ist der Vergleichsstand), die
Zeilen-Hash-Historie (`gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp`)
ist endgültig entfallen — die Begründung von Iteration 3, ein „Incremental
Scheduler" brauche sie, hat sich beim Nachprüfen als gegenstandslos
herausgestellt (kein Code außerhalb von Stufe 2 hat diese Tabellen je
gelesen; per Grep und Tabellennamen-Suche verifiziert). Die Iterationen 1-3
unten sind als Entwicklungs-Historie erhalten.
**Betrifft:** alle drei Pipeline-Stufen + `create_datamodel.py` + `etl_state.py`;
ursprünglich nur [src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py) — `audit_table_keyed_snapshot()`,
Tabellen `gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen`.
Am Rande mitgefixt: [src/pipeline_audit_to_target.py](src/pipeline_audit_to_target.py) setzte `JAVA_HOME`
beim Direktaufruf nie auf JDK 17 (s. Verifikations-Abschnitt) — unabhängiger Bug, der bei der End-to-End-Prüfung dieser ADR auffiel.
**Ersetzt:** den Rename-Swap-Teil von ADR-8 in [docs/entscheidungen.md](docs/entscheidungen.md) (dort entsprechend aktualisiert)

## Kontext: das Problem

Alle Tabellen dieses Projekts lagen bisher `STORED AS PARQUET` auf normalem
Impala/HDFS-Speicher. Parquet-Tabellen in Impala kennen **kein zeilengenaues
UPDATE, DELETE oder MERGE** — nur `INSERT`, `INSERT OVERWRITE` und `TRUNCATE`
auf der ganzen Tabelle.

Für `bauland` und `bevoelkerungzahlen` (Snapshot-Tabellen **mit** verlässlichem
Business-Key, s. `KEY_COLUMNS`) betreibt Stufe 2 trotzdem einen echten
**zeilengenauen** Incremental Merge: pro Lauf werden per FNV-Hash-Vergleich
genau die neuen/geänderten/gelöschten Keys ermittelt (`audit_table_keyed_snapshot()`).
Ohne UPDATE/DELETE musste das bisher komplett drumherum gebaut werden:

1. Betroffene Keys in eine Hilfstabelle schreiben (`gruppe3_etl_changed_keys_tmp`).
2. Eine **komplett neue** Tabelle per `CREATE TABLE … AS SELECT` zusammensetzen:
   `LEFT ANTI JOIN` (unveränderte alte Zeilen) `UNION ALL` `LEFT SEMI JOIN`
   (frisch bereinigte Zeilen für die betroffenen Keys).
3. Die neue Tabelle per `ALTER TABLE … RENAME TO` gegen die alte tauschen,
   die alte danach löschen.

Der Umweg über „neue Tabelle bauen + tauschen" statt einfach
`INSERT OVERWRITE TABLE audit SELECT … FROM audit …` war nötig, weil Impala
nicht sicher aus einer Tabelle lesen kann, während dieselbe Tabelle per
`INSERT OVERWRITE` geleert wird (Race zwischen laufendem Scan und Truncate —
im schlechtesten Fall gehen dabei alle nicht neu geschriebenen Zeilen verloren).
Das funktionierte, aber:

- ~35 Zeilen reine Infrastruktur-SQL, die nichts mit der eigentlichen
  Bereinigungslogik zu tun haben.
- Bei jedem Lauf wird die **gesamte** unveränderte Restmenge (bei `bauland`
  z. B. tausende Zeilen) einmal komplett neu geschrieben, nur um sie
  unverändert wieder einzusetzen.
- Ein abgebrochener Lauf mitten in Schritt 2/3 hinterlässt Restmüll
  (`_incoming`-/`_old`-Tabellen), den niemand automatisch aufräumt.

## Entscheidung

Die beiden Tabellen `gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen`
laufen jetzt als **Apache-Iceberg-Tabellen** (`STORED BY ICEBERG`,
`format-version=2`) statt als Parquet-Tabellen. `audit_table_keyed_snapshot()`
ersetzt den kompletten CREATE+RENAME+DROP-Umweg durch zwei direkte, native
SQL-Statements, ausgelöst für genau die Keys, die laut der Hash-Historie
(`gruppe3_etl_row_state`, s. `etl_state.py`) neu/geändert/gelöscht sind:

```sql
-- a) echt gelöschte Keys entfernen (in changed_keys, aber nicht mehr in Staging)
DELETE t FROM gruppe3_audit_bauland t WHERE <key(t)> IN (
    SELECT c.row_key FROM gruppe3_etl_changed_keys_tmp c
    LEFT ANTI JOIN (SELECT * FROM gruppe3_staging_bauland) s ON c.row_key = <key(s)>
)

-- b) betroffene, weiterhin vorhandene Keys aktualisieren/einfügen
MERGE INTO gruppe3_audit_bauland t USING (
    SELECT <bereinigte Spalten> FROM gruppe3_staging_bauland s
    LEFT SEMI JOIN gruppe3_etl_changed_keys_tmp c ON <key(s)> = c.row_key
) src ON <key(t)> = <key(src)>
WHEN MATCHED THEN UPDATE SET spalte1 = src.spalte1, ...
WHEN NOT MATCHED THEN INSERT (spalte1, ...) VALUES (src.spalte1, ...)
```

Unveränderte Zeilen (Key nicht in der Changed-Keys-Hilfstabelle) werden von
keinem der beiden Statements angefasst — kein Neuschreiben der ganzen Tabelle
mehr. Eine Hilfsfunktion `ensure_iceberg_audit_table()` sorgt dafür, dass die
Audit-Tabelle als Iceberg vorliegt: legt sie frisch als Iceberg an, wenn sie
noch nicht existiert; existiert sie noch als Parquet, bricht sie mit einer
klaren Fehlermeldung ab (die einmalige Migration ist ein separates Skript,
s. „Iteration 2"). Die Hash-Historie je Zeile
(`gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp`) bleibt bewusst
bestehen, auch wenn sie für das reine DELETE+MERGE gegen Iceberg technisch
nicht mehr zwingend nötig wäre (s. „Iteration 2"/„Iteration 3" unten) —
sie wird zusätzlich vom Incremental Scheduler gebraucht.

## Wie Apache Iceberg das Problem löst

Iceberg ist ein **offenes Tabellenformat** (kein neues Storage-System) — die
Daten liegen weiterhin als Parquet-Dateien, aber Iceberg legt eine
Metadaten-/Snapshot-Schicht darüber, die jeden Schreibvorgang als **atomaren
Commit** behandelt (neuer Metadaten-Zeiger wird erst nach vollständigem
Schreiben der neuen Datendateien atomar umgeschaltet). Dadurch kann Impala auf
Iceberg-Tabellen echtes zeilengenaues `DELETE`, `UPDATE` und `MERGE INTO`
anbieten, ohne dass ein Reader jemals eine halb-geleerte oder halb-geschriebene
Tabelle sieht — genau die Race, die den Rename-Swap-Umweg nötig gemacht hatte,
existiert bei Iceberg schlicht nicht mehr. Der komplette
CREATE-TABLE-AS-SELECT-plus-Rename-Tanz war nur ein Workaround für eine
Einschränkung von **Parquet-auf-HDFS**, nicht von Impala/SQL an sich — Iceberg
behebt die Einschränkung an der Wurzel, der Anwendungscode wird dadurch
einfacher, nicht komplizierter.

## Was bewusst NICHT migriert wurde

- **`gemeinden` und `klimadaten`** (`audit_table_snapshot()` /
  `audit_table_incremental()`) bleiben `STORED AS PARQUET`. Sie werden nie
  zeilengenau verändert, sondern immer komplett per `INSERT OVERWRITE`
  ersetzt oder per `INSERT INTO` reiner Zeitreihen-Append — für beides bietet
  Iceberg gegenüber Parquet keinen Vorteil, der die Umstellung rechtfertigt.
- **Die Ziel-Sternschema-Tabellen** (`dim_*`/`fact_*`,
  [pipeline_audit_to_target.py](src/pipeline_audit_to_target.py)) bleiben
  ebenfalls Parquet. Sie werden von Spark über den **generischen JDBC-Dialekt**
  geschrieben (kein nativer Iceberg-Catalog-Zugriff, s. ADR-5 in
  `entscheidungen.md`) — Iceberg würde dort das dokumentierte
  TRUNCATE/Dialekt-Problem nicht lösen, sondern nur den Storage-Layer
  austauschen. Eine echte Nutzung von Iceberg dort würde bedeuten, den
  gesamten Schreibpfad auf einen nativen Iceberg-Catalog umzustellen — das ist
  eine deutlich größere Rearchitecture und aktuell nicht gerechtfertigt.

## Verifikation (live gegen den echten Cluster, Impala 4.5.0 / Cloudera Runtime 7.3.2)

- `CREATE TABLE … STORED BY ICEBERG`, `INSERT`, `UPDATE`, `DELETE`,
  `MERGE INTO` und Time Travel (`FOR SYSTEM_TIME AS OF`) einzeln gegen
  Test-Tabellen geprüft — alle funktionieren.
- Exaktes Syntax-Detail geprüft und korrigiert: `CREATE TABLE … LIKE …
  STORED BY ICEBERG` schlägt fehl, wenn die Quelltabelle selbst kein Iceberg
  ist ("cannot be cloned into an Iceberg table") → stattdessen
  `CTAS … WHERE 1=0` für die Schema-Übernahme. `DELETE FROM tbl alias WHERE
  …` ist ein Syntaxfehler; die korrekte Impala-Syntax ist
  `DELETE alias FROM tbl alias WHERE …`.
- Der komplette DELETE+MERGE-Pfad wurde end-to-end gegen eine isolierte
  Testtabelle mit demselben zusammengesetzten Schlüssel wie `bauland`
  (`kreis_id, jahr, merkmal`) durchgespielt: initialer Full Load (reines
  Insert über MERGE), Skip bei unveränderten Daten, und gleichzeitiges
  Update+Delete+Insert in einem Lauf — Ergebnis stimmt exakt mit der Erwartung
  überein.
- Die **echte Migration** wurde gegen die Produktions-Tabellen
  `gruppe3_audit_bauland` (21.600 Zeilen) und `gruppe3_audit_bevoelkerungzahlen`
  (581 Zeilen) ausgeführt: Zeilenzahl und Inhalts-Prüfsumme
  (`content_signature()`) vor und nach der Migration sind identisch, kein
  Datenverlust. Beide Tabellen sind jetzt bestätigt Iceberg
  (`DESCRIBE FORMATTED` zeigt `HiveIcebergSerDe`).
- **Der Spark-JDBC-Lesepfad in `pipeline_audit_to_target.py` ist inzwischen
  verifiziert.** Ein erster erzwungener Testlauf (`FORCE_TARGET_BUILD=1`)
  scheiterte zunächst an einem echten, von dieser Iceberg-Umstellung
  unabhängigen Bug: `pipeline_audit_to_target.py` setzte `JAVA_HOME` beim
  direkten Aufruf nie auf JDK 17 (das übernahm bisher nur `scheduler.py` für
  den Fall, dass der Scheduler der Aufrufer ist) — beim Direktaufruf griff
  PySpark auf das System-Default-JDK (hier JDK 26) zu, das die von
  Spark/Hadoop 3.5.x benötigte, in JDK ≥ 23/24 entfernte Security-API
  `Subject.getSubject` nicht mehr kennt
  (`UnsupportedOperationException: getSubject is not supported`, bereits
  dokumentiert in `docs/spark_stolpersteine.md`, Stolperstein 1). Fix:
  dieselbe `JAVA_HOME_JDK17`-Übernahme wie in `scheduler.py`, jetzt auch am
  Anfang von `pipeline_audit_to_target.py` selbst, vor dem `pyspark`-Import.
  Nach dem Fix lief der komplette Ziel-Build (`FORCE_TARGET_BUILD=1`)
  erfolgreich durch und hat alle 9 Ziel-Tabellen aus den jetzt
  Iceberg-basierten Audit-Tabellen `gruppe3_audit_bauland`/
  `gruppe3_audit_bevoelkerungzahlen` per Spark-JDBC neu gebaut (`dim_kreis`
  472, `dim_jahr` 30, `dim_klimastadt` 81, `dim_gemeinde` 10.947,
  `fact_bevoelkerung` 14.110, `fact_bauland` 4.720, `fact_klima` 1.539,
  `fact_gemeinde_stamm` 10.947, `fact_standortprofil_kpi` 4.099) — der
  generische Impala-JDBC-Dialekt liest Iceberg-Tabellen exakt wie
  Parquet-Tabellen, keine Anpassung in Stufe 3 nötig.
- `ALTER TABLE … CREATE TAG/BRANCH` (Iceberg-Branching/Tagging) schlug als
  Impala-DDL fehl (Syntaxfehler) — ob Impala das grundsätzlich nicht anbietet
  oder nur die Syntax falsch geraten war, ist offen; für diese Entscheidung
  ungenutzt und nicht weiter verfolgt.

## Trade-offs

- **Neue, weniger battle-getestete Code-Pfade:** `DELETE`/`MERGE INTO` auf
  Iceberg sind in Impala jüngere Features als das reine `INSERT`/`CREATE`, mit
  dem der Rest des Projekts arbeitet. Dagegen steht, dass der bisherige
  Rename-Swap-Workaround selbst schon eine dokumentierte Krücke mit eigenen
  Randfällen war (s. ADR-8) — kein Wechsel von "bewährt" zu "neu", sondern von
  "Workaround" zu "Kern-Feature des Formats, für genau diesen Zweck gebaut".
- **Migration betrifft Produktionsdaten:** Die einmalige Parquet→Iceberg-
  Migration wurde gegen die echten Gruppen-Tabellen ausgeführt und verifiziert
  (s. oben). Seit Iteration 2 (s. unten) läuft sie **nicht mehr automatisch**
  innerhalb der Pipeline, sondern über ein separates, explizit zu startendes
  Skript (`src/utils/migrate_audit_tables_to_iceberg.py`) — Teammitglieder,
  die den Code neu auschecken, müssen es einmal manuell ausführen, bevor
  `pipeline_staging_to_audit.py` wieder läuft (sonst bricht die Pipeline mit
  einer klaren Fehlermeldung ab, die genau darauf verweist).
- Iceberg-Tabellen tragen zusätzliche Metadaten (Snapshot-Historie,
  Manifest-Dateien) gegenüber reinem Parquet — bei der Größenordnung dieses
  Projekts (zehntausende Zeilen) vernachlässigbar.
- **Kein Compaction-/Snapshot-Management:** Impala-Iceberg-Tabellen laufen auf
  diesem Cluster standardmäßig `merge-on-read` (bestätigt: `write.merge.mode
  merge-on-read` in den TBLPROPERTIES) — jeder `DELETE`/`MERGE` schreibt
  kleine Delete-Dateien statt die Parquet-Basis neu zu schreiben. Über sehr
  viele tägliche Läufe sammelt sich das an (mehr Dateien, langsamere Reads),
  ohne dass hier eine `OPTIMIZE`/`EXPIRE SNAPSHOTS`-Routine ergänzt wurde.
  Bei der Laufzeit/Datenmenge dieses Projekts unkritisch, aber der fehlende
  Baustein für echten Produktivbetrieb — bewusst als Ausblick stehen
  gelassen, nicht umgesetzt.

## Nicht genutzt, aber möglich (Ausblick)

Time Travel (`FOR SYSTEM_TIME AS OF`) funktioniert bereits auf den migrierten
Tabellen, wird aber aktuell nicht im Code verwendet — könnte künftig die
Frage "wie sah `audit_bauland` vor dem letzten WAP-Lauf aus" direkt per Query
beantworten, ohne eigene Historisierung zu bauen. Kein Teil dieser
Entscheidung, nur als Anschlussmöglichkeit festgehalten.

## Iteration 2: Architektur-Selbstkritik und Vereinfachung (07.07.2026)

Nach der ersten Umsetzung (oben) wurde die eigene Implementierung bewusst
kritisch gegengelesen ("was kann man architektonisch besser machen"). Drei
Punkte wurden identifiziert und noch am selben Tag umgesetzt und erneut live
verifiziert:

**1) Redundante Change-Detection entfernt.** Die erste Fassung hatte den
CREATE+RENAME+DROP-Umweg durch DELETE+MERGE ersetzt, aber die dafür
ursprünglich nötige externe Buchführung (`gruppe3_etl_row_state`:
FNV-Hash je Zeile und Business-Key, plus `gruppe3_etl_changed_keys_tmp` als
Join-Hilfstabelle) unverändert weiterbenutzt, um vor dem DELETE/MERGE erst
in Python zu berechnen, welche Keys überhaupt betroffen sind. Das war nicht
mehr nötig: weil MERGE INTO/DELETE jetzt direkt gegen die (Iceberg-)
Audit-Tabelle laufen, IST die Audit-Tabelle selbst der aktuelle
Vergleichsstand. `audit_table_keyed_snapshot()` vergleicht jetzt direkt in
SQL (`WHEN MATCHED AND NOT (t.col <=> src.col AND ...) THEN UPDATE`, `<=>`
= NULL-sicherer Vergleich) und der DELETE-Schritt joint die Audit-Tabelle
direkt gegen die Staging-Tabelle (`LEFT ANTI JOIN`), ohne Umweg über eine
Changed-Keys-Tabelle. Als günstigen Vor-Check, ob sich überhaupt etwas
geändert hat (um bei unveränderten Daten nicht jeden Tag DELETE+MERGE
auszuführen), wird jetzt dieselbe `content_signature()`-Tabellen-Prüfsumme
verwendet, die `audit_table_snapshot()` (für `gemeinden`) bereits nutzt -
ein Aggregat-Scan statt eines GROUP-BY-Hash-je-Zeile plus Python-Diff.
Ergebnis: `ROW_STATE_TABLE`/`CHANGED_KEYS_TABLE` und alle zugehörigen
Funktionen (`ensure_row_state_table`, `get_latest_row_hashes`,
`record_row_hashes`, `ensure_changed_keys_table`, `load_changed_keys`) wurden
komplett aus `etl_state.py` entfernt (verifiziert unbenutzt per Grep vor dem
Löschen) - eine komplette State-Tabelle und ein kompletter
Python-Roundtrip weniger, bei JEDEM Lauf, nicht nur bei tatsächlichen
Änderungen.

**2) `_is_iceberg_table()` robuster gemacht.** Erkannte Iceberg-Tabellen
vorher per Substring-Suche (`"HiveIcebergSerDe" in <kompletter
DESCRIBE-FORMATTED-Text>`) - funktioniert, ist aber an das Freitext-Format
gekoppelt. Impala kennt kein `SHOW TBLPROPERTIES` (Hive-only, live
getestet, Syntaxfehler); stattdessen liest die neue Fassung gezielt die
Table-Parameter-Zeile, bei der Spalte 2 (`row[1]`) gleich `table_type` und
Spalte 3 (`row[2]`) gleich `ICEBERG` ist - ein präziser Spaltenvergleich
statt einer Suche im gesamten Text-Dump.

**3) Migration aus dem täglichen Hot Path entfernt.** `ensure_iceberg_audit_table()`
migrierte in der ersten Fassung automatisch und lautlos von Parquet zu
Iceberg, falls nötig - das hieße, die Prüfung "ist das schon Iceberg?"
läuft für immer bei jedem Lauf mit, obwohl sie nur beim allerersten Mal
je etwas tut, und ein Abbruch mitten in der (für eine 21.600-Zeilen-Tabelle
nicht-trivialen) CTAS-Migration wäre in einem unbeaufsichtigten,
automatischen Lauf schwerer zu diagnostizieren gewesen als in einem
separaten Schritt. Jetzt: `ensure_iceberg_audit_table()` legt eine fehlende
Tabelle weiterhin frisch als Iceberg an, bricht aber mit einer klaren
`RuntimeError` ab, wenn eine bestehende Audit-Tabelle noch Parquet ist. Die
eigentliche Migrationslogik (CTAS + Zeilenzahl-Verifikation + Rename-Swap)
liegt jetzt in einem eigenen, manuell auszuführenden Skript
([src/utils/migrate_audit_tables_to_iceberg.py](src/utils/migrate_audit_tables_to_iceberg.py)),
nach demselben Muster wie `reset_database.py` (strukturell heikle,
einmalige Operationen bekommen ein eigenes Skript statt in der täglichen
Pipeline "nebenbei" zu laufen).

**4) Bewusst NICHT umgesetzt:** Compaction/Snapshot-Expiration (s.
Trade-offs oben) - als "eher Ausblick als akuter Mangel" bei dieser
Datenmenge eingestuft und dokumentiert stehen gelassen statt implementiert.

### Verifikation Iteration 2

- `MERGE ... WHEN MATCHED AND NOT (t.col <=> src.col ...)` einzeln gegen eine
  Testtabelle mit einer echten NULL-Spalte geprüft: eine Zeile mit
  unverändertem NULL-Wert wird korrekt NICHT als geändert erkannt (wäre
  sie es mit `=` statt `<=>`, würde sie bei jedem Lauf unnötig neu
  geschrieben).
- Kompletter End-to-End-Testlauf (Full Load, Skip bei Unverändertem,
  gleichzeitiges Update+Delete+Insert inkl. einer Zeile mit unveränderter
  NULL-Spalte) gegen eine isolierte Testtabelle mit demselben
  zusammengesetzten Schlüssel wie `bauland` wiederholt - Ergebnis stimmt
  exakt mit der Erwartung überein.
- Fail-Fast-Verhalten explizit geprüft: eine als Parquet angelegte
  Test-Audit-Tabelle führt zu einer `RuntimeError` mit Verweis auf das
  Migrationsskript, die Tabelle bleibt dabei unangetastet (keine stille
  Teil-Migration).
- Migrationsskript gegen die echten (bereits migrierten) Produktionstabellen
  ausgeführt: erkennt korrekt "bereits Iceberg" und tut nichts (idempotent).
- Kompletter `pipeline_staging_to_audit.py`-Lauf nach allen Änderungen erneut
  ausgeführt: alle vier Tabellen weiterhin korrekt als "unverändert"
  erkannt; Zeilenzahl und Inhalts-Prüfsumme von `gruppe3_audit_bauland`/
  `gruppe3_audit_bevoelkerungzahlen` exakt identisch zum Stand vor Iteration 2
  (21.600 / 581 Zeilen, unveränderte `content_signature()`-Werte).
- **Zum Zeitpunkt dieser Messung unbenutzt, aber NICHT gelöscht:**
  `gruppe3_etl_row_state` (22.141 Zeilen) und `gruppe3_etl_changed_keys_tmp`
  (3 Zeilen) wurden zu diesem Zeitpunkt von keinem Code mehr referenziert
  (per Grep verifiziert) - sie wurden bewusst NICHT automatisch gelöscht
  (Sicherheitsklassifizierung der Umgebung hat das Löschen bestehender
  Tabellen mit Dateninhalt ohnehin blockiert). Das erwies sich im Nachhinein
  als richtig: s. „Iteration 3" - diese Tabellen werden doch gebraucht.

### Fazit: ist es dadurch besser geworden?

Ja, in den drei konkret angegangenen Punkten spürbar:

- **Weniger Code, weniger Zustand:** eine komplette State-Tabelle
  (`gruppe3_etl_row_state`) plus die kurzlebige Join-Hilfstabelle
  (`gruppe3_etl_changed_keys_tmp`) und fünf Funktionen in `etl_state.py`
  sind ersatzlos entfallen. `audit_table_keyed_snapshot()` ist dadurch kürzer
  UND näher an `audit_table_snapshot()` (gleiche Skip-Technik via
  `content_signature()`) - ein Muster weniger im Kopf zu behalten.
  Jeder Lauf spart einen kompletten Python-Roundtrip (Hashes lesen -> Python
  diffen -> zurückschreiben), nicht nur an Tagen mit echten Änderungen.
- **Robusteres Detection-Verfahren:** die `_is_iceberg_table()`-Prüfung
  ist jetzt an eine konkrete, benannte Metadaten-Zeile gebunden statt an
  freien Text - weniger anfällig für ein zukünftiges Impala-Versions-Update.
- **Klarere Verantwortlichkeiten:** die strukturell heikle
  Parquet→Iceberg-Migration ist jetzt ein bewusster, expliziter Schritt mit
  eigener Verifikation (Zeilenzahl-Check vor dem Tausch) statt unsichtbar
  im täglichen Lauf - im Fehlerfall (z.B. Verbindungsabbruch mitten in der
  Migration einer 21.600-Zeilen-Tabelle) ist jetzt klar, WANN und WARUM etwas
  schiefging, statt es in einem automatischen nächtlichen Lauf zu vermuten.
- **Was NICHT besser geworden ist:** die vierte Beobachtung (fehlendes
  Compaction-/Snapshot-Management) bleibt unverändert offen - bewusst, weil
  bei dieser Datenmenge kein akuter Schmerzpunkt, aber für echten
  Produktivbetrieb weiterhin eine Lücke.
- **Nachtrag (s. Iteration 3 unten):** Punkt 1 dieser Iteration
  (Row-Hash-Historie entfernt) wurde direkt danach wieder rückgängig
  gemacht - `gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp` werden
  zusätzlich vom Incremental Scheduler gebraucht, was zum Zeitpunkt dieser
  Iteration nicht bekannt war. Punkte 2 (robustere Iceberg-Erkennung) und 3
  (Migration als separates Skript) betreffen dieses Thema nicht und bleiben
  unverändert bestehen.

## Iteration 3: Row-Hash-Historie wiederhergestellt (07.07.2026)

Nach Iteration 2 stellte sich heraus, dass `gruppe3_etl_row_state` und
`gruppe3_etl_changed_keys_tmp` entgegen der Annahme in Iteration 2 NICHT
überflüssig sind: der Incremental Scheduler braucht diese Zeilenhistorie
für eigene Zwecke, unabhängig davon, dass `audit_table_keyed_snapshot()`
selbst technisch auch ohne sie ausgekommen wäre (Iteration 2 hatte
gezeigt, dass ein direkter Vergleich gegen die Iceberg-Audit-Tabelle
funktional ausreicht - das war architektonisch nicht falsch, hat aber einen
Konsumenten dieser Daten übersehen, der außerhalb von
`audit_table_keyed_snapshot()` liegt).

**Rückgängig gemacht:** `ROW_STATE_TABLE`/`CHANGED_KEYS_TABLE` und die
fünf zugehörigen Funktionen (`ensure_row_state_table`,
`get_latest_row_hashes`, `record_row_hashes`, `ensure_changed_keys_table`,
`load_changed_keys`) sind wieder in `etl_state.py`. `audit_table_keyed_snapshot()`
ermittelt Änderungen wieder über den Python-seitigen Hash-Diff gegen
`gruppe3_etl_row_state` (FNV-Hash je Zeile/Business-Key) statt über die in
Iteration 2 eingeführte `content_signature()`-Tabellen-Prüfsumme.

**Bewusst NICHT rückgängig gemacht** (beide Punkte betreffen die
Row-Hash-Frage nicht, bleiben aus Iteration 2 bestehen):
- `_is_iceberg_table()` liest weiterhin gezielt die `table_type`-Spalte statt
  einer Substring-Suche.
- Die Parquet→Iceberg-Migration bleibt ein separates Skript
  ([src/utils/migrate_audit_tables_to_iceberg.py](src/utils/migrate_audit_tables_to_iceberg.py)),
  `ensure_iceberg_audit_table()` bricht weiterhin mit klarer Fehlermeldung
  ab statt lautlos zu migrieren.

Das eigentliche DELETE+MERGE gegen die Iceberg-Audit-Tabelle (der Kern
dieser ADR) bleibt unverändert bestehen - nur WELCHE Keys hineingegeben
werden, wird wieder über die Hash-Historie statt direkt in SQL bestimmt.

### Verifikation Iteration 3

- Kompletter End-to-End-Testlauf (Full Load, Skip bei Unverändertem,
  gleichzeitiges Update+Delete+Insert) mit der wiederhergestellten
  `CHANGED_KEYS_TABLE`-gesteuerten Fassung gegen eine isolierte Testtabelle
  mit demselben zusammengesetzten Schlüssel wie `bauland` wiederholt -
  Ergebnis stimmt weiterhin exakt mit der Erwartung überein.
- Kompletter `pipeline_staging_to_audit.py`-Lauf gegen die echte Datenbank:
  liest die zu diesem Zeitpunkt bereits vorhandenen historischen Einträge in
  `gruppe3_etl_row_state` korrekt ein, erkennt alle vier Tabellen weiterhin
  korrekt als "unverändert". Zeilenzahl und Inhalts-Prüfsumme von
  `gruppe3_audit_bauland`/`gruppe3_audit_bevoelkerungzahlen` weiterhin exakt
  identisch (21.600 / 581 Zeilen, unveränderte `content_signature()`-Werte)
  zum Stand vor allen drei Iterationen - keine der drei Umbauten hat die
  eigentlichen Daten je angefasst.

### Lehre daraus

Eine Vereinfachung, die nur den Code liest, den man selbst gerade bearbeitet,
kann einen Konsumenten übersehen, der an anderer Stelle auf denselben
State zugreift (hier: der Incremental Scheduler auf
`gruppe3_etl_row_state`). Ein Grep auf Funktionsnamen im eigenen Repo (wie in
Iteration 2 gemacht) findet keine Abhängigkeiten, die nur über den
Tabellennamen selbst laufen oder außerhalb des durchsuchten Codes liegen.

## Iteration 4: Komplette Pipeline auf Iceberg, Merge zustandslos (10.07.2026)

**Vorgeschichte:** Am Vormittag des 10.07. war die gesamte Pipeline in einem
(nie committeten) Zwischenstand auf reines Full Load zurückgebaut worden
(Begründung damals: Incremental Loading sei keine Pflichtanforderung).
Diese Entscheidung wurde noch am selben Tag revidiert - das Incremental-
Loader-Pattern ist ausdrücklich gewünscht und wird jetzt konsequent mit
Apache Iceberg über die GANZE Pipeline umgesetzt. Der Full-Load-Zwischenstand
ist auf dem Branch `backup/full-load-rueckbau-2026-07-10` gesichert.

### Was sich gegenüber Iteration 3 ändert

1. **ALLE Tabellen sind Iceberg (format-version 2)** - nicht mehr nur die
   zwei Merge-Audit-Tabellen:
   - Staging (Stufe 1): atomare INSERT-OVERWRITE-Snapshots beim bedingten
     Full Refresh; `gruppe3_staging_klimadaten` per Iceberg-Transform
     `PARTITIONED BY SPEC (TRUNCATE(4, dt))` nach Jahr partitioniert
     (hidden partitioning - der Wasserzeichen-Filter `dt > '...'` wird zum
     Partition-Prune statt zum 8,6-Mio.-Zeilen-Scan).
   - Audit (Stufe 2): `gruppe3_audit_gemeinden`/`_klimadaten` jetzt ebenfalls
     Iceberg (atomarer Full Refresh bzw. partitionierter Append).
   - Star-Schema (Stufe 3): alle 9 `dim_*`/`fact_*`-Tabellen Iceberg; der
     Publish läuft als ATOMARER Shadow-Swap (s. Punkt 3).
   - `gruppe3_etl_state`: Iceberg mit echtem UPSERT (s. Punkt 4).

2. **Zeilengenauer Merge jetzt zustandslos (Iteration-2-Idee, korrigiert
   wieder eingeführt):** `audit_table_keyed_snapshot()` vergleicht direkt
   in SQL - `MERGE INTO ... WHEN MATCHED AND NOT (t.col <=> src.col AND ...)
   THEN UPDATE` (NULL-sicherer Vergleich), gelöschte Keys per
   `DELETE t FROM audit t WHERE key NOT IN (SELECT key FROM staging)`.
   Die Iceberg-Audit-Tabelle selbst ist der Vergleichsstand. Der günstige
   Vor-Check bleibt die `content_signature()`-Tabellen-Prüfsumme. Die
   Zeilen-Hash-Historie ist damit endgültig gelöscht: die Iteration-3-
   Begründung (ein "Incremental Scheduler" als zweiter Konsument) war beim
   Nachprüfen nicht haltbar - `scheduler.py` ruft nur `run_pipeline.main()`
   auf und hat `gruppe3_etl_row_state` nie gelesen (Grep über Funktions-
   UND Tabellennamen, zusätzlich Live-Blick in die Datenbank).

   **Neu gegenüber Iteration 2 - Duplikat-Keys:** `default.project_bauland`
   enthält 40 komplett leere Import-Zeilen (identischer Business-Key ''),
   `project_bevoelkerungzahlen` 2 Leerzeilen mit id='' (live verifiziert;
   sonst sind beide Keys eindeutig). Ein MERGE mit mehreren Quellzeilen je
   Ziel-Key bricht in Impala hart ab ("Duplicate row found ...", live
   reproduziert) - die MERGE-Quelle wird deshalb per GROUP BY (bereinigte
   Key-Ausdrücke) + MIN() je Nicht-Key-Spalte deterministisch auf eine
   Zeile je Key verdichtet. Die naheliegendere ROW_NUMBER()-Variante
   scheitert an einem Impala-Planner-Fehler ("Illegal reference to
   non-materialized tuple", live reproduziert, in beiden Nesting-Formen).

3. **Atomarer Publish (WAP zu Ende gedacht):** `overwrite_table()` in Stufe 3
   schrieb bisher TRUNCATE + INSERT-Batches DIREKT in die sichtbaren
   `dim_*`/`fact_*`-Tabellen - ein Konsument konnte dazwischen eine leere/
   halbe Tabelle lesen. Jetzt: Batches in eine Shadow-Tabelle
   (`<ziel>_wap_incoming`), dann EIN `INSERT OVERWRITE ziel SELECT * FROM
   shadow` = ein einzelner Iceberg-Snapshot-Commit. Konsumenten sehen zu
   jedem Zeitpunkt den kompletten alten ODER den kompletten neuen Stand;
   der alte bleibt per Time Travel abfragbar.

4. **ETL-State als Upsert statt Append-Only:** Die Append-Only-Konstruktion
   existierte NUR, weil Parquet kein UPDATE/MERGE kann. `record_state()`
   macht jetzt ein `MERGE INTO`-UPSERT (genau eine Zeile je
   stage+table_name), `get_latest_state()` ist ein simpler SELECT ohne
   "jüngste Zeile suchen"-Logik. Die Verlaufs-Historie liegt in den
   Iceberg-Snapshots (`DESCRIBE HISTORY gruppe3_etl_state` / Time Travel) -
   Historie als Feature des Tabellenformats statt als App-Logik.

5. **Migration verallgemeinert:** `src/utils/migrate_to_iceberg.py` (ersetzt
   `migrate_audit_tables_to_iceberg.py`) migriert alle Schichten verlustfrei
   (CTAS + Zeilenzahl-Verifikation VOR dem Rename-Swap), verdichtet die
   Append-Only-State-Historie auf den jüngsten Eintrag je Schlüssel und
   löscht die beiden obsoleten Zeilen-Hash-Tabellen.

### Verifikation Iteration 4 (live gegen den echten Cluster, Impala 4.5.0)

- Feature-Probe auf Wegwerf-Tabellen: `MERGE ... WHEN MATCHED AND`,
  `WHEN NOT MATCHED BY SOURCE THEN DELETE`, `UPDATE`, `DELETE` (mit/ohne
  Alias), `INSERT OVERWRITE` (auch auf partitionierte Iceberg-Tabellen),
  `TRUNCATE`, `DESCRIBE HISTORY`, Time Travel, `PARTITIONED BY SPEC
  (TRUNCATE(4, dt))` auf STRING-Spalten, CTAS aus Parquet-Quellen - alles
  funktioniert. Negativ-Befunde (führten zum finalen Design): MERGE-Abbruch
  bei Duplikat-Quellzeilen, ROW_NUMBER()-Planner-Fehler im MERGE-USING,
  `DELETE ... LEFT ANTI JOIN`-AnalysisException ("For deleting every row,
  please use TRUNCATE") -> NOT-IN-Subquery (CONCAT_WS-Keys sind nie NULL,
  daher NULL-sicher).
- Der komplette GROUP-BY-Dedupe-MERGE-Pfad end-to-end auf einer Testtabelle
  mit Duplikat-Keys und NULL-Spalten: initialer Load, Idempotenz-Lauf,
  Update der Duplikat-Zeilen (aktualisiert alle betroffenen Ziel-Zeilen,
  kein Abbruch), Delete-Erkennung - Ergebnis exakt wie erwartet.
- Einmalige Migration ALLER 20 Bestandstabellen ausgeführt: Zeilenzahlen
  1:1 verifiziert (u.a. gruppe3_staging_klimadaten mit 8.599.212 Zeilen),
  `gruppe3_etl_state` auf 9 aktuelle Einträge verdichtet, obsolete
  Hash-Tabellen gelöscht.
- Stufe 1 + Stufe 2 danach komplett gelaufen: alle vier Quellen korrekt als
  "unverändert - Lauf übersprungen" erkannt.
- Änderungspfad erzwungen (State-Prüfsummen invalidiert): DELETE+MERGE
  (bauland, bevoelkerungzahlen) und Full Refresh (gemeinden) gegen die
  echten Daten gelaufen - `content_signature()` und Zeilenzahlen vor/nach
  IDENTISCH, der direkt folgende Lauf skippt wieder überall.
- `contract_check.py` gegen das (migrierte) Datenprodukt: 32/32 Checks OK.
- **Noch offen:** Stufe 3 (Spark) konnte auf dem Umbau-Rechner nicht laufen
  (kein `ImpalaJDBC42.jar`/JDK 17 lokal). Der Spark-JDBC-LESEpfad gegen
  Iceberg-Audit-Tabellen ist seit Iteration 1 verifiziert; der neue
  Shadow-Swap-SCHREIBpfad nutzt dieselben, einzeln live getesteten
  Statements (CTAS WHERE 1=0, TRUNCATE, INSERT INTO ... VALUES, INSERT
  OVERWRITE ... SELECT), muss aber einmal im Ganzen per
  `FORCE_TARGET_BUILD=1` bestätigt werden (s. TODO/README).

### Lehre aus Iteration 3 -> 4

Die Iteration-3-Rückabwicklung beruhte auf einer unbelegten Behauptung
("wird vom Incremental Scheduler gebraucht"), die nie gegen den Code
verifiziert wurde. Konsequenz für künftige Entscheidungen: WER genau der
Konsument ist, gehört mit Dateiname/Zeile in die ADR - eine Abhängigkeit,
die sich nicht benennen lässt, existiert mit hoher Wahrscheinlichkeit nicht.
