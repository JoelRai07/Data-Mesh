# ADR: Apache Iceberg für den zeilengenauen Merge in Stufe 2 (Audit)

**Status:** umgesetzt, end-to-end live verifiziert (07.07.2026). Eine zweite
Iteration hatte die Hash-Historie je Zeile testweise entfernt (s. „Iteration
2" unten) - das wurde danach wieder rueckgaengig gemacht, weil
`gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp` vom Incremental
Scheduler gebraucht werden (s. „Iteration 3").
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
ersetzt den kompletten CREATE+RENAME+DROP-Umweg durch zwei direkte, native
SQL-Statements, ausgeloest fuer genau die Keys, die laut der Hash-Historie
(`gruppe3_etl_row_state`, s. `etl_state.py`) neu/geaendert/geloescht sind:

```sql
-- a) echt geloeschte Keys entfernen (in changed_keys, aber nicht mehr in Staging)
DELETE t FROM gruppe3_audit_bauland t WHERE <key(t)> IN (
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
mehr. Eine Hilfsfunktion `ensure_iceberg_audit_table()` sorgt dafür, dass die
Audit-Tabelle als Iceberg vorliegt: legt sie frisch als Iceberg an, wenn sie
noch nicht existiert; existiert sie noch als Parquet, bricht sie mit einer
klaren Fehlermeldung ab (die einmalige Migration ist ein separates Skript,
s. „Iteration 2"). Die Hash-Historie je Zeile
(`gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp`) bleibt bewusst
bestehen, auch wenn sie fuer das reine DELETE+MERGE gegen Iceberg technisch
nicht mehr zwingend noetig waere (s. „Iteration 2"/„Iteration 3" unten) —
sie wird zusaetzlich vom Incremental Scheduler gebraucht.

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
CREATE+RENAME+DROP-Umweg durch DELETE+MERGE ersetzt, aber die dafuer
urspruenglich noetige externe Buchfuehrung (`gruppe3_etl_row_state`:
FNV-Hash je Zeile und Business-Key, plus `gruppe3_etl_changed_keys_tmp` als
Join-Hilfstabelle) unveraendert weiterbenutzt, um vor dem DELETE/MERGE erst
in Python zu berechnen, welche Keys ueberhaupt betroffen sind. Das war nicht
mehr noetig: weil MERGE INTO/DELETE jetzt direkt gegen die (Iceberg-)
Audit-Tabelle laufen, IST die Audit-Tabelle selbst der aktuelle
Vergleichsstand. `audit_table_keyed_snapshot()` vergleicht jetzt direkt in
SQL (`WHEN MATCHED AND NOT (t.col <=> src.col AND ...) THEN UPDATE`, `<=>`
= NULL-sicherer Vergleich) und der DELETE-Schritt joint die Audit-Tabelle
direkt gegen die Staging-Tabelle (`LEFT ANTI JOIN`), ohne Umweg ueber eine
Changed-Keys-Tabelle. Als guenstigen Vor-Check, ob sich ueberhaupt etwas
geaendert hat (um bei unveraenderten Daten nicht jeden Tag DELETE+MERGE
auszufuehren), wird jetzt dieselbe `content_signature()`-Tabellen-Pruefsumme
verwendet, die `audit_table_snapshot()` (fuer `gemeinden`) bereits nutzt -
ein Aggregat-Scan statt eines GROUP-BY-Hash-je-Zeile plus Python-Diff.
Ergebnis: `ROW_STATE_TABLE`/`CHANGED_KEYS_TABLE` und alle zugehoerigen
Funktionen (`ensure_row_state_table`, `get_latest_row_hashes`,
`record_row_hashes`, `ensure_changed_keys_table`, `load_changed_keys`) wurden
komplett aus `etl_state.py` entfernt (verifiziert unbenutzt per Grep vor dem
Loeschen) - eine komplette State-Tabelle und ein kompletter
Python-Roundtrip weniger, bei JEDEM Lauf, nicht nur bei tatsaechlichen
Aenderungen.

**2) `_is_iceberg_table()` robuster gemacht.** Erkannte Iceberg-Tabellen
vorher per Substring-Suche (`"HiveIcebergSerDe" in <kompletter
DESCRIBE-FORMATTED-Text>`) - funktioniert, ist aber an das Freitext-Format
gekoppelt. Impala kennt kein `SHOW TBLPROPERTIES` (Hive-only, live
getestet, Syntaxfehler); stattdessen liest die neue Fassung gezielt die
Table-Parameter-Zeile, bei der Spalte 2 (`row[1]`) gleich `table_type` und
Spalte 3 (`row[2]`) gleich `ICEBERG` ist - ein praeziser Spaltenvergleich
statt einer Suche im gesamten Text-Dump.

**3) Migration aus dem taeglichen Hot Path entfernt.** `ensure_iceberg_audit_table()`
migrierte in der ersten Fassung automatisch und lautlos von Parquet zu
Iceberg, falls noetig - das hiesse, die Pruefung "ist das schon Iceberg?"
laeuft fuer immer bei jedem Lauf mit, obwohl sie nur beim allerersten Mal
je etwas tut, und ein Abbruch mitten in der (fuer eine 21.600-Zeilen-Tabelle
nicht-trivialen) CTAS-Migration waere in einem unbeaufsichtigten,
automatischen Lauf schwerer zu diagnostizieren gewesen als in einem
separaten Schritt. Jetzt: `ensure_iceberg_audit_table()` legt eine fehlende
Tabelle weiterhin frisch als Iceberg an, bricht aber mit einer klaren
`RuntimeError` ab, wenn eine bestehende Audit-Tabelle noch Parquet ist. Die
eigentliche Migrationslogik (CTAS + Zeilenzahl-Verifikation + Rename-Swap)
liegt jetzt in einem eigenen, manuell auszufuehrenden Skript
([src/utils/migrate_audit_tables_to_iceberg.py](src/utils/migrate_audit_tables_to_iceberg.py)),
nach demselben Muster wie `reset_database.py` (strukturell heikle,
einmalige Operationen bekommen ein eigenes Skript statt in der taeglichen
Pipeline "nebenbei" zu laufen).

**4) Bewusst NICHT umgesetzt:** Compaction/Snapshot-Expiration (s.
Trade-offs oben) - als "eher Ausblick als akuter Mangel" bei dieser
Datenmenge eingestuft und dokumentiert stehen gelassen statt implementiert.

### Verifikation Iteration 2

- `MERGE ... WHEN MATCHED AND NOT (t.col <=> src.col ...)` einzeln gegen eine
  Testtabelle mit einer echten NULL-Spalte geprueft: eine Zeile mit
  unveraendertem NULL-Wert wird korrekt NICHT als geaendert erkannt (waere
  sie es mit `=` statt `<=>`, wuerde sie bei jedem Lauf unnoetig neu
  geschrieben).
- Kompletter End-to-End-Testlauf (Full Load, Skip bei Unveraendertem,
  gleichzeitiges Update+Delete+Insert inkl. einer Zeile mit unveraenderter
  NULL-Spalte) gegen eine isolierte Testtabelle mit demselben
  zusammengesetzten Schluessel wie `bauland` wiederholt - Ergebnis stimmt
  exakt mit der Erwartung ueberein.
- Fail-Fast-Verhalten explizit geprueft: eine als Parquet angelegte
  Test-Audit-Tabelle fuehrt zu einer `RuntimeError` mit Verweis auf das
  Migrationsskript, die Tabelle bleibt dabei unangetastet (keine stille
  Teil-Migration).
- Migrationsskript gegen die echten (bereits migrierten) Produktionstabellen
  ausgefuehrt: erkennt korrekt "bereits Iceberg" und tut nichts (idempotent).
- Kompletter `pipeline_staging_to_audit.py`-Lauf nach allen Aenderungen erneut
  ausgefuehrt: alle vier Tabellen weiterhin korrekt als "unveraendert"
  erkannt; Zeilenzahl und Inhalts-Pruefsumme von `gruppe3_audit_bauland`/
  `gruppe3_audit_bevoelkerungzahlen` exakt identisch zum Stand vor Iteration 2
  (21.600 / 581 Zeilen, unveraenderte `content_signature()`-Werte).
- **Zum Zeitpunkt dieser Messung unbenutzt, aber NICHT geloescht:**
  `gruppe3_etl_row_state` (22.141 Zeilen) und `gruppe3_etl_changed_keys_tmp`
  (3 Zeilen) wurden zu diesem Zeitpunkt von keinem Code mehr referenziert
  (per Grep verifiziert) - sie wurden bewusst NICHT automatisch geloescht
  (Sicherheitsklassifizierung der Umgebung hat das Loeschen bestehender
  Tabellen mit Dateninhalt ohnehin blockiert). Das erwies sich im Nachhinein
  als richtig: s. „Iteration 3" - diese Tabellen werden doch gebraucht.

### Fazit: ist es dadurch besser geworden?

Ja, in den drei konkret angegangenen Punkten spuerbar:

- **Weniger Code, weniger Zustand:** eine komplette State-Tabelle
  (`gruppe3_etl_row_state`) plus die kurzlebige Join-Hilfstabelle
  (`gruppe3_etl_changed_keys_tmp`) und fuenf Funktionen in `etl_state.py`
  sind ersatzlos entfallen. `audit_table_keyed_snapshot()` ist dadurch kuerzer
  UND naeher an `audit_table_snapshot()` (gleiche Skip-Technik via
  `content_signature()`) - ein Muster weniger im Kopf zu behalten.
  Jeder Lauf spart einen kompletten Python-Roundtrip (Hashes lesen -> Python
  diffen -> zurueckschreiben), nicht nur an Tagen mit echten Aenderungen.
- **Robusteres Detection-Verfahren:** die `_is_iceberg_table()`-Pruefung
  ist jetzt an eine konkrete, benannte Metadaten-Zeile gebunden statt an
  freien Text - weniger anfaellig fuer ein zukuenftiges Impala-Versions-Update.
- **Klarere Verantwortlichkeiten:** die strukturell heikle
  Parquet→Iceberg-Migration ist jetzt ein bewusster, expliziter Schritt mit
  eigener Verifikation (Zeilenzahl-Check vor dem Tausch) statt unsichtbar
  im taeglichen Lauf - im Fehlerfall (z.B. Verbindungsabbruch mitten in der
  Migration einer 21.600-Zeilen-Tabelle) ist jetzt klar, WANN und WARUM etwas
  schiefging, statt es in einem automatischen naechtlichen Lauf zu vermuten.
- **Was NICHT besser geworden ist:** die vierte Beobachtung (fehlendes
  Compaction-/Snapshot-Management) bleibt unveraendert offen - bewusst, weil
  bei dieser Datenmenge kein akuter Schmerzpunkt, aber fuer echten
  Produktivbetrieb weiterhin eine Luecke.
- **Nachtrag (s. Iteration 3 unten):** Punkt 1 dieser Iteration
  (Row-Hash-Historie entfernt) wurde direkt danach wieder rueckgaengig
  gemacht - `gruppe3_etl_row_state`/`gruppe3_etl_changed_keys_tmp` werden
  zusaetzlich vom Incremental Scheduler gebraucht, was zum Zeitpunkt dieser
  Iteration nicht bekannt war. Punkte 2 (robustere Iceberg-Erkennung) und 3
  (Migration als separates Skript) betreffen dieses Thema nicht und bleiben
  unveraendert bestehen.

## Iteration 3: Row-Hash-Historie wiederhergestellt (07.07.2026)

Nach Iteration 2 stellte sich heraus, dass `gruppe3_etl_row_state` und
`gruppe3_etl_changed_keys_tmp` entgegen der Annahme in Iteration 2 NICHT
ueberfluessig sind: der Incremental Scheduler braucht diese Zeilenhistorie
fuer eigene Zwecke, unabhaengig davon, dass `audit_table_keyed_snapshot()`
selbst technisch auch ohne sie ausgekommen waere (Iteration 2 hatte
gezeigt, dass ein direkter Vergleich gegen die Iceberg-Audit-Tabelle
funktional ausreicht - das war architektonisch nicht falsch, hat aber einen
Konsumenten dieser Daten uebersehen, der ausserhalb von
`audit_table_keyed_snapshot()` liegt).

**Rueckgaengig gemacht:** `ROW_STATE_TABLE`/`CHANGED_KEYS_TABLE` und die
fuenf zugehoerigen Funktionen (`ensure_row_state_table`,
`get_latest_row_hashes`, `record_row_hashes`, `ensure_changed_keys_table`,
`load_changed_keys`) sind wieder in `etl_state.py`. `audit_table_keyed_snapshot()`
ermittelt Aenderungen wieder ueber den Python-seitigen Hash-Diff gegen
`gruppe3_etl_row_state` (FNV-Hash je Zeile/Business-Key) statt ueber die in
Iteration 2 eingefuehrte `content_signature()`-Tabellen-Pruefsumme.

**Bewusst NICHT rueckgaengig gemacht** (beide Punkte betreffen die
Row-Hash-Frage nicht, bleiben aus Iteration 2 bestehen):
- `_is_iceberg_table()` liest weiterhin gezielt die `table_type`-Spalte statt
  einer Substring-Suche.
- Die Parquet→Iceberg-Migration bleibt ein separates Skript
  ([src/utils/migrate_audit_tables_to_iceberg.py](src/utils/migrate_audit_tables_to_iceberg.py)),
  `ensure_iceberg_audit_table()` bricht weiterhin mit klarer Fehlermeldung
  ab statt lautlos zu migrieren.

Das eigentliche DELETE+MERGE gegen die Iceberg-Audit-Tabelle (der Kern
dieser ADR) bleibt unveraendert bestehen - nur WELCHE Keys hineingegeben
werden, wird wieder ueber die Hash-Historie statt direkt in SQL bestimmt.

### Verifikation Iteration 3

- Kompletter End-to-End-Testlauf (Full Load, Skip bei Unveraendertem,
  gleichzeitiges Update+Delete+Insert) mit der wiederhergestellten
  `CHANGED_KEYS_TABLE`-gesteuerten Fassung gegen eine isolierte Testtabelle
  mit demselben zusammengesetzten Schluessel wie `bauland` wiederholt -
  Ergebnis stimmt weiterhin exakt mit der Erwartung ueberein.
- Kompletter `pipeline_staging_to_audit.py`-Lauf gegen die echte Datenbank:
  liest die zu diesem Zeitpunkt bereits vorhandenen historischen Eintraege in
  `gruppe3_etl_row_state` korrekt ein, erkennt alle vier Tabellen weiterhin
  korrekt als "unveraendert". Zeilenzahl und Inhalts-Pruefsumme von
  `gruppe3_audit_bauland`/`gruppe3_audit_bevoelkerungzahlen` weiterhin exakt
  identisch (21.600 / 581 Zeilen, unveraenderte `content_signature()`-Werte)
  zum Stand vor allen drei Iterationen - keine der drei Umbauten hat die
  eigentlichen Daten je angefasst.

### Lehre daraus

Eine Vereinfachung, die nur den Code liest, den man selbst gerade bearbeitet,
kann einen Konsumenten uebersehen, der an anderer Stelle auf denselben
State zugreift (hier: der Incremental Scheduler auf
`gruppe3_etl_row_state`). Ein Grep auf Funktionsnamen im eigenen Repo (wie in
Iteration 2 gemacht) findet keine Abhaengigkeiten, die nur ueber den
Tabellennamen selbst laufen oder ausserhalb des durchsuchten Codes liegen.
