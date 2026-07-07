# ADR: Apache Iceberg für den zeilengenauen Merge in Stufe 2 (Audit)

**Status:** umgesetzt und end-to-end live verifiziert, inkl. nachgelagertem Spark-Ziel-Build (07.07.2026)
**Betrifft:** [src/pipeline_staging_to_audit.py](src/pipeline_staging_to_audit.py) — `audit_table_keyed_snapshot()`,
`ensure_iceberg_audit_table()`, Tabellen `gruppe3_audit_bauland` und `gruppe3_audit_bevoelkerungzahlen`.
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
ersetzt den kompletten CREATE+RENAME+DROP-Umweg durch zwei direkte,
native SQL-Statements:

```sql
-- a) echt geloeschte Keys entfernen (in changed_keys, aber nicht mehr in Staging)
DELETE t FROM gruppe3_audit_bauland t WHERE <key> IN (
    SELECT c.row_key FROM gruppe3_etl_changed_keys_tmp c
    LEFT ANTI JOIN (SELECT * FROM gruppe3_staging_bauland) s ON c.row_key = <key(s)>
)

-- b) betroffene, weiterhin vorhandene Keys aktualisieren/einfuegen
MERGE INTO gruppe3_audit_bauland t USING (
    SELECT <bereinigte Spalten> FROM gruppe3_staging_bauland s
    LEFT SEMI JOIN gruppe3_etl_changed_keys_tmp c ON <key(s)> = c.row_key
) src ON <key(t)> = <key(src)>
WHEN MATCHED THEN UPDATE SET spalte1 = src.spalte1, ...
WHEN NOT MATCHED THEN INSERT (spalte1, ...) VALUES (src.spalte1, ...)
```

Unveränderte Zeilen (Key nicht in der Changed-Keys-Hilfstabelle) werden von
keinem der beiden Statements angefasst — kein Neuschreiben der ganzen Tabelle
mehr. Eine neue Hilfsfunktion `ensure_iceberg_audit_table()` sorgt dafür, dass
die Audit-Tabelle immer als Iceberg vorliegt: legt sie frisch als Iceberg an,
wenn sie noch nicht existiert, migriert sie einmalig verlustfrei, falls sie
noch (aus der Zeit vor dieser Umstellung) als Parquet existiert, und tut sonst
nichts (idempotent).

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
  Migration lief automatisch beim ersten Pipeline-Lauf nach diesem Commit
  gegen die echten Gruppen-Tabellen. Bereits ausgeführt und verifiziert
  (s. oben); für Teammitglieder, die den Code neu auschecken, läuft sie beim
  nächsten `pipeline_staging_to_audit.py`-Lauf automatisch und idempotent mit.
- Iceberg-Tabellen tragen zusätzliche Metadaten (Snapshot-Historie,
  Manifest-Dateien) gegenüber reinem Parquet — bei der Größenordnung dieses
  Projekts (zehntausende Zeilen) vernachlässigbar.

## Nicht genutzt, aber möglich (Ausblick)

Time Travel (`FOR SYSTEM_TIME AS OF`) funktioniert bereits auf den migrierten
Tabellen, wird aber aktuell nicht im Code verwendet — könnte künftig die
Frage "wie sah `audit_bauland` vor dem letzten WAP-Lauf aus" direkt per Query
beantworten, ohne eigene Historisierung zu bauen. Kein Teil dieser
Entscheidung, nur als Anschlussmöglichkeit festgehalten.
