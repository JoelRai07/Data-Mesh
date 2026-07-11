# Probleme — gelöst, nicht behebbar, offen

Dieses Dokument sammelt **alle** Probleme des Projekts an einem Ort: die
gelösten (mit Ursache und Lösung als Fallstudie), die nicht behebbaren (im
Data Contract dokumentiert) und die noch offenen Punkte.

> Architektur-Entscheidungen → [ADR.md](ADR.md) · Setup/Benutzung/Doku →
> [README.md](../README.md) · Konsumenten-Sicht → [data/data_contract.yaml](../data/data_contract.yaml)

## Übersicht: gelöste Probleme

| # | Problem | Lösung (Kurzfassung) | Details |
|---|---|---|---|
| P1 | `standortattraktivitaets_score` komplett NULL (0 von 4.720 Zeilen) | Division-durch-0 → Infinity vergiftete die Window-Aggregate; `safe_div()` + Klima-`coalesce(…, 0)` | [Fallstudie 1](#fallstudie-1-score-spalte-komplett-null-p1) |
| P2 | Encoding: `L�beck` & Co. (U+FFFD) in Bauland + Bevölkerung | Laufzeit-Erkennung + automatische Auflösung über Referenzlisten; 0 Reste verifiziert | [Fallstudie 2](#fallstudie-2-zerstörte-umlaute-p2), [ADR-6](ADR.md) |
| P3 | Alle Gemeinde-Koordinaten NULL → Klima-Brücke tot | Zerstörte `gruppe3`-Kopie ersetzt durch intakte `default.project_gemeinden` + Dezimalkomma-Parsing | [Fallstudie 1](#zweite-ursache-koordinaten-durch-csv-bug-zerstört-p3) |
| P4 | 6 Spark-/JDBC-Stolpersteine (JDK, winutils, Dialekt, VPN, NULL-Binding, explode) | je ein eigener Workaround | [Fallstudie 3](#fallstudie-3-die-6-sparkjdbc-stolpersteine-p4) |
| P5 | `kaufwert_je_qm_eur` enthält nur 0/NULL | **Nicht behebbar** (ab Quelle zerstört) → im Contract „NICHT VERWENDEN", Ersatz `preis_pro_qm_eur` | [Nicht behebbar](#nicht-behebbare-einschränkungen) |
| P6 | APScheduler-Crash beim Loggen von `next_run_time` | Zeit direkt vom Trigger erfragen statt vom Job | [Fallstudie 4](#fallstudie-4-apscheduler-next_run_time-p6) |
| P7 | Cloudera-JDBC „Error converting value to double" (`per_km2`, `area_km2`) | Spalten als STRING lesen (`customSchema`) bzw. Dichte selbst berechnen | [Fallstudie 3](#stolperstein-7-error-converting-value-to-double-p7) |
| P8 | Zeilengenauer Merge für `gemeinden` erkannte Phantom-Änderungen | NULL-Keys kollabieren in `CONCAT_WS`; kein stabiler NULL-freier Schlüssel → bewusst Tabellen-Prüfsumme | [ADR-8](ADR.md) |
| P9 | Spark scheitert an System-JDK ≥ 23/24 (`getSubject is not supported`) | `JAVA_HOME_JDK17` vor dem `pyspark`-Import selbst setzen | [Fallstudie 3](#stolperstein-1-keinfalsches-java-p9), [ADR-16](ADR.md) |
| P10 | Neuer Airflow-DAG in CDE startete pausiert; der Pause-Toggle in der CDE-Airflow-UI ließ sich nicht zuverlässig bedienen | `is_paused_upon_creation=False` im DAG — er wird direkt aktiv erzeugt, ohne UI-Interaktion | [ADR-17](ADR.md) |

---

## Fallstudie 1: Score-Spalte komplett NULL (P1)

**Symptom:** In `gruppe3_fact_standortprofil_kpi` war `standortattraktivitaets_score`
in **allen** Zeilen NULL (0 von 4.720), obwohl die Eingangsspalten für viele
Zeilen Werte hatten und die anderen KPI-Spalten nur teilweise NULL waren.

**Ursache — eine Kette aus drei Effekten:**

1. **Division durch 0 erzeugt Infinity.** `preis_pro_qm_eur = kaufsumme / fläche`;
   748 Bauland-Zeilen haben `veraeusserte_flaeche_1000qm = 0` (Flächen < 500 m²
   runden amtlich auf 0, die Kaufsumme bleibt echt) → `kaufsumme / 0 = Infinity`.
2. **Infinity/NaN vergiftet Fensteraggregate.** `AVG()`/`STDDEV()` über ein
   Fenster werden **komplett NaN**, sobald auch nur EIN Infinity-Wert im Fenster
   liegt (anders als NULL, das übersprungen wird). Der Score ist ein z-Score
   `(Wert − AVG über Jahr) / STDDEV über Jahr` — ein vergifteter Jahrgang macht
   den Score für **alle** Kreise dieses Jahres NaN.
3. Beim Zurückschreiben wandelt `_sql_literal()` NaN → NULL → gesamte Spalte NULL.

**Lösung:** `safe_div(numerator, denominator)` in
[src/pipeline_audit_to_target.py](../src/pipeline_audit_to_target.py) liefert NULL
statt Infinity, wenn der Nenner 0/NULL ist, und ersetzt **jede** Division mit
variablem Nenner (alle KPI- und z-Score-Terme). NULL heißt damit ehrlich „nicht
berechenbar" und wird von Aggregaten übersprungen (→ [ADR-11](ADR.md)).

### Zweite Ursache: Koordinaten durch CSV-Bug zerstört (P3)

Nach dem `safe_div`-Fix war der Score **immer noch** NULL — der Klima-Term `C`
im Score `A − B − C` war für jede Zeile NULL:

- `dim_gemeinde` hatte **0** gültige `latitude`/`longitude` (von ~10.850 Zeilen).
- Ursache: In der damaligen `gruppe3`-Rohkopie waren die Koordinaten durch einen
  CSV-Bug zerstört — deutsche Dezimal**kommas** (`9,13735`) in einer
  komma-getrennten CSV zerrissen jede Koordinate mittendrin
  (`latitude = '13735"'`, `longitude = '"9'`) → `CAST(… AS DOUBLE)` = NULL überall.
- Ohne Koordinaten fand der (damalige) nächste-Klimastadt-Join nichts →
  Klima-Abweichung überall 0 → STDDEV = 0 → z-Score-Term NULL → Score NULL.

**Fixes:** (1) Der Klima-Term geht per `F.coalesce(…, 0)` **neutral** in den
Score ein, wenn er fehlt (Sicherheitsnetz, bleibt aktiv). (2) Die eigentliche
Reparatur: Stufe 1 liest seither die **intakte** Original-Tabelle
`default.project_gemeinden` (dort stehen die Koordinaten korrekt als `9,43751`)
und parst das deutsche Dezimalkomma per `regexp_replace(col, ",", ".")`.

**Achtung, zwei verschiedene Koordinaten-Formate in den Quellen:**
- Gemeinden: `9,43751` — Dezimal**komma**, keine Himmelsrichtung → Komma→Punkt.
- Klimadaten: `106.55E` / `5.63S` — Dezimal**punkt** + Himmelsrichtung →
  Buchstabe entfernen, bei S/W negieren (`compass_to_signed_decimal`, Stufe 2).

**Ergebnis (verifiziert):** 3.911 von 4.099 KPI-Zeilen haben einen Score
(vorher: 0). Die verbleibenden NULLs sind echte, dokumentierte Lücken (s. unten).

### Folgefehler: `per_km2` bricht den Spark-JDBC-Read ab (P7)

Nach dem Umstieg auf `default.project_gemeinden` warf `build_fact_gemeinde_stamm`
`[Cloudera][JDBC](10140) Error converting value to double`: Die Quellspalte
`per_km2` (double) enthält Werte, die der Cloudera-JDBC-Treiber nicht wandeln
kann (impyla liest sie problemlos — der JDBC-Treiber ist strenger).
**Fix:** `area_km2`/`per_km2` per `customSchema` als STRING lesen; die Dichte
wird selbst berechnet (`einwohner_pro_km2 = population_total / area_km2`,
fachlich identisch) — die problematische Spalte wird gar nicht mehr angefasst
(s. `read_gemeinden()` in [src/pipeline_audit_to_target.py](../src/pipeline_audit_to_target.py)).

---

## Fallstudie 2: Zerstörte Umlaute (P2)

**Symptom:** In `project_bauland` und `project_bevoelkerungzahlen` sind Umlaute
als U+FFFD (`�`) gespeichert: `L�beck`, `Th�ringen`, `Ver�u�erungsf�lle`. Das
Originalzeichen ist **unwiederbringlich verloren** — kein Algorithmus kann es
zurückrechnen. (In `project_gemeinden` sind die Umlaute intakt; die Klimadaten
haben englische Exonyme wie „Munich" und Kompass-Koordinaten.)

**Verworfene Ansätze:**
- *„Kaputte Zeichen einheitlich entfernen"* (`L�beck` → `Lbeck`): erzeugt
  falsche Namen, die nirgends mehr matchen.
- *Hartcodierte Korrekturliste* (~90 Einträge im Code): veraltet, sobald neue
  Staging-Daten neue kaputte Werte enthalten; hoher Pflegeaufwand.

**Lösung (Stufe 2, [ADR-6](ADR.md)):** Die kaputten Kreisnamen werden zur
**Laufzeit entdeckt** (`_discover_bad_kreis_values` liest alle Werte mit `�`
aus den Staging-Tabellen) und automatisch aufgelöst: das Ersatzzeichen steht
für genau ein verlorenes Zeichen → als Regex-Wildcard gegen drei
Referenzlisten gematcht (`src/utils/german_cities.txt`, `german_regions.txt`,
`german_states.txt`; Herkunft s. README „Quellen") → Treffer wird zu ASCII
transliteriert. Nur 8 Sonderfälle ohne Listen-Eintrag (Berliner Bezirke,
längst aufgelöste Altkreise) bleiben manuell gepflegt
(`MANUAL_KREIS_CORRECTIONS`), dazu 2 Merkmalstexte
(`BAULAND_MERKMAL_CORRECTIONS`). Ist ein kaputter Wert weder auflösbar noch
manuell hinterlegt, bricht die Pipeline laut ab (`RuntimeError`) statt still
falsche Daten zu schreiben.

Für **intakte** Umlaute (Gemeindenamen) gilt echte Transliteration
ä→ae/ö→oe/ü→ue/ß→ss, damit alle Namens-Joins dieselbe ASCII-Schreibweise
verwenden; englische Städtenamen der Klimadaten werden gemappt
(`CITY_NAME_CORRECTIONS`: Munich→Muenchen, Cologne→Koeln, …).

**Ergebnis (live verifiziert):** 0 verbleibende `�` in allen Audit-Tabellen.

---

## Fallstudie 3: Die 6 Spark/JDBC-Stolpersteine (P4)

Alle beim Aufsetzen der Spark-Stufe ([src/pipeline_audit_to_target.py](../src/pipeline_audit_to_target.py))
aufgetreten. Die große Lektion: **Spark eignet sich hervorragend zum
Lesen/Transformieren, aber Impala ist kein robustes Schreib-Ziel für
Spark-JDBC-Writes** — daher der Hybrid-Ansatz (Spark liest/rechnet, impyla
schreibt; [ADR-5](ADR.md)).

### Stolperstein 1: Kein/falsches Java (P9)

**Symptom:** `java.lang.UnsupportedOperationException: getSubject is not supported`
beim Spark-Start auf einem neuen JDK (≥ 23/24, z. B. JDK 26).

**Ursache:** Neuere JDKs entfernen alte Security-APIs (`Subject.getSubject`),
die die von Spark 3.5.x mitgebrachte Hadoop-Version noch braucht. Harte
Inkompatibilität, keine Konfigurationsfrage.

**Lösung:** Eclipse Temurin **JDK 17** zusätzlich installieren (parallel zum
System-JDK möglich), Pfad als `JAVA_HOME_JDK17` in `.env`.
`pipeline_audit_to_target.py` und `scheduler.py` setzen `JAVA_HOME`/`PATH`
daraus **selbst, vor dem `pyspark`-Import** ([ADR-16](ADR.md)) — manuelles
Setzen der Umgebungsvariable ist nicht mehr nötig.

### Stolperstein 2: `spark.jars` + Windows = `winutils.exe`-Fehler

**Symptom:** `FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`.

**Ursache:** Wird der JDBC-Treiber per `spark.jars` eingebunden, kopiert Spark
die Datei über Hadoops Dateiwerkzeuge — das verlangt unter Windows das
Hilfsprogramm `winutils.exe`, das normalerweise fehlt.

**Lösung:** Treiber über `spark.driver.extraClassPath` /
`spark.executor.extraClassPath` einbinden — hängt die Jar nur an den
Java-Classpath, ohne Hadoop-Kopiervorgang.

### Stolperstein 3: Spark erkennt Impala nicht als JDBC-Dialekt

**Symptom:** Beim Schreiben (`df.write.jdbc`) erzeugt Spark
`CREATE TABLE … ("kreis_id" TEXT …)` → `ParseException` in Impala.

**Ursache:** Spark hat keinen eingebauten Dialekt für `jdbc:impala://` und
fällt auf einen generischen zurück (Double-Quotes, Typ `TEXT`), den Impala
nicht versteht. Der Existenz-Check der Zieltabelle schlägt fehl, Spark hält
die Tabelle für nicht vorhanden und legt sie selbst an — mit ungültiger Syntax.

**Folgeschaden:** In einem Testlauf hat Spark dabei erst erfolgreich
`DROP TABLE gruppe3_dim_kreis` ausgeführt und ist dann beim `CREATE TABLE`
gescheitert — die Tabelle war komplett weg (per `create_datamodel.py` sofort
wiederherstellbar, `CREATE TABLE IF NOT EXISTS`).

**Lösung:** Kein Spark-natives Schema-Handling: `TRUNCATE` separat per impyla,
Schreiben ebenfalls per impyla (s. Stolperstein 5). Die **Reads** der
`gruppe3_audit_*`-Tabellen über den Cloudera-Treiber funktionieren dagegen
problemlos (auch gegen die Iceberg-Audit-Tabellen, verifiziert bei der
Iceberg-Migration, s. [ADR-13](ADR.md)).

### Stolperstein 4: VPN blockiert Spark-interne Sockets

**Symptom:** `SocketTimeoutException: Accept timed out` /
`Python worker failed to connect back`.

**Ursache:** Operationen wie `spark.createDataFrame(python_liste)` tauschen
Daten zwischen Python-Prozess und JVM über einen lokalen Socket aus; ein
aktiver VPN-Client (OpenVPN/Wintun) störte genau diese Loopback-Verbindungen.

**Lösung (doppelt):** (1) `spark.createDataFrame(python_liste)` komplett
vermieden — Kleinst-Daten wie die Jahresliste entstehen rein JVM-seitig per
`F.explode(F.sequence(F.lit(1995), F.lit(2024)))`. (2) VPN beim Pipeline-Lauf
deaktivieren; seither läuft auch wieder `master("local[*]")` (Mehrkern).

### Stolperstein 5: Impala-JDBC-Treiber kann keine NULL-Parameter binden

**Symptom:** `Error getting the parameter data type: …NON_SUPPORT_DATA_TYPE`
beim `df.write.jdbc()`.

**Ursache:** Der Treiber kann den SQL-Typ eines Parameters nicht bestimmen,
wenn er in der ersten Batch-Zeile NULL ist. Unsere Tabellen haben durchgehend
NULL-fähige Spalten (fehlgeschlagene Namens-Matches, KPI-NULLs) — das ist der
Regelfall, kein Randfall.

**Lösung:** Kompletter Verzicht auf `df.write.jdbc()`. `overwrite_table()`
holt die Zeilen per `df.collect()` und baut daraus normalen SQL-Text
(`INSERT INTO … VALUES (…), (…)`, Batches à 2000), den impyla ausführt — keine
Parameter-Bindung, keine Typ-Probleme. `NaN`/`Infinity` werden dabei zu NULL
(Impala akzeptiert diese Literale nicht).

### Stolperstein 6: `explode()` darf nicht in `CAST(…)` verschachtelt sein

**Symptom:** `AnalysisException: [UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS]`.

**Ursache:** Generator-Funktionen wie `explode()` sind nur als direkter
SELECT-Ausdruck erlaubt, nicht in anderen Funktionen verschachtelt.

**Lösung:** Den `CAST` weglassen — `F.sequence(F.lit(1995), F.lit(2024))`
liefert bereits `array<int>`.

### Stolperstein 7: „Error converting value to double" (P7)

s. oben, [Fallstudie 1 → Folgefehler `per_km2`](#folgefehler-per_km2-bricht-den-spark-jdbc-read-ab-p7).

---

## Fallstudie 4: APScheduler `next_run_time` (P6)

**Symptom:** `AttributeError: 'Job' object has no attribute 'next_run_time'`
beim Start von `scheduler.py`, **vor** `scheduler.start()`.

**Ursache:** `apscheduler.job.Job` definiert `next_run_time` als
`__slots__`-Attribut — es existiert erst, nachdem ihm einmal ein Wert
zugewiesen wurde (kein automatischer `None`-Default). APScheduler berechnet
den Wert aber erst beim **Start** des Schedulers. Der Code wollte ihn für die
Startmeldung schon vorher auslesen.

**Lösung:** Die nächste Fälligkeit direkt vom **Trigger** erfragen, der seine
Logik unabhängig vom Scheduler-Status kennt:
`trigger.get_next_fire_time(None, datetime.now(scheduler.timezone))`.
Verifiziert: korrekte Startmeldung (nächster Tag 00:00 Europe/Berlin), kein
Absturz.

**Hinweis:** Während der Entwicklung stand der Trigger testweise auf
`CronTrigger(minute="*")` (jede Minute); für die Abgabe steht er auf
`CronTrigger(hour=0, minute=0)` (täglich Mitternacht, wie gefordert).

---

## Historische Irrwege (verworfen, aber lehrreich)

Kurzfassung — vollständige Tabelle in [ADR.md → Abgelöste Entscheidungen](ADR.md):

- **Spark-JDBC-Reads direkt gegen `default.project_*`** scheiterten am
  fehlenden Impala-Dialekt (Double-Quote-Quoting: `SELECT "id" …` liefert den
  Spaltennamen als String-Literal statt der Spalte). Die heutige Architektur
  umgeht das strukturell: Stufe 1+2 laufen serverseitig per impyla; Spark
  liest erst die `gruppe3_audit_*`-Tabellen — diese Reads funktionieren mit
  dem Cloudera-Treiber einwandfrei.
- **`wohnraumdruck_index` als Verhältnis zweier Wachstumsraten**:
  Vorzeichen-Falle (−/− = +) — der Wert spiegelte die Vorzeichen-Kombination,
  nicht den Druck. Ersetzt durch ein Verhältnis von Bestandsgrößen
  (Einwohner je 1000 m² Bauland), nie negativ.
- **Koordinaten-Distanz-Join** Gemeinde→nächste Klimastadt: scheiterte erst an
  P3, danach bewusst durch den deterministischen Namens-Join ersetzt ([ADR-7](ADR.md)).
- **Iceberg-Merge: Vereinfachung → Rollback → Rollback des Rollbacks:** Ein
  Refactoring ersetzte die Row-Hash-Historie (`gruppe3_etl_row_state`) durch
  direkten SQL-Vergleich; das wurde zurückgerollt, weil angeblich ein
  „Incremental Scheduler" die Historie konsumiere. Beim Nachprüfen erwies
  sich diese Behauptung als falsch (kein Code außerhalb von Stufe 2 hat die
  Tabellen je gelesen) — seit dem 10.07. ist der Merge endgültig zustandslos,
  die Hash-Tabellen sind gelöscht. Lehre: *Konsumenten eines States mit
  Dateiname/Zeile benennen — was sich nicht benennen lässt, existiert mit
  hoher Wahrscheinlichkeit nicht* ([ADR-13](ADR.md)).

---

## Nicht behebbare Einschränkungen

Diese Punkte sind **keine Bugs**, sondern Eigenschaften der Quelldaten — im
[Data Contract](../data/data_contract.yaml) für Konsumenten dokumentiert:

| Einschränkung | Detail | Umgang |
|---|---|---|
| `kaufwert_je_qm_eur` unbrauchbar (P5) | Der amtliche „Durchschnittliche Kaufwert je qm" wurde **schon beim Quellimport** zerstört: Dezimalwerte in eine BIGINT-Spalte geladen → nur 0/NULL, auch in `default.project_bauland` | Im Contract als „NICHT VERWENDEN" markiert; `preis_pro_qm_eur` (kaufsumme/fläche) ist der berechnete Ersatz |
| Amtliche Geheimhaltung | Baulandwerte kleiner Kreise/Jahre sind unterdrückt → NULL | NULL-Quoten je Spalte im Contract; NULL = „nicht berechenbar", von Aggregaten übersprungen |
| Flächen-Rundung auf 0 | 748 Bauland-Zeilen: Fläche < 500 m² rundet amtlich auf 0, Kaufsumme bleibt echt | `safe_div` → `preis_pro_qm_eur` NULL statt Infinity |
| Klimadaten enden 2013 | Quelle Berkeley Earth ist ein statisches Archiv | Ab 2014 geht der Klima-Term neutral (0) in die KPIs ein; im Contract dokumentiert |
| `wachstum_vorjahr_pct` NULL im ersten Jahr | Kein Vorjahr vorhanden | Fachlich korrekt, dokumentiert |
| `kreis_name` mehrdeutig | Die Bereinigung entfernt Namenszusätze („, kreisfreie Stadt") → z. B. „Leipzig" 3× (Stadt/Landkreis/Alt-Kreis) | Contract-Regel: Joins/Filter **nur** über `kreis_id` |
| Klima-Match unvollständig | 74 von 81 Klimastädten per Namens-Match angebunden; Rest = mehrdeutige Kurznamen („Frankfurt") | LEFT JOIN, fehlendes Klima zählt neutral; über Korrekturliste nachschärfbar |
| Kreis-Coverage 97,5 % | 272 von 10.947 Gemeinden ohne `kreis_id` (Namens-Match fehlgeschlagen) | Im Contract dokumentiert; LEFT JOIN einplanen |

## Harmlose Warnungen (kein Handlungsbedarf)

- **log4j-`ClassCastException`** bei jedem JDBC-Connect
  (`Unable to create Lookup for bundle …`): Der Cloudera-Treiber bringt eine
  geshadete log4j-Kopie mit, die mit Sparks log4j kollidiert; log4j fällt
  intern auf eine Default-Konfiguration zurück, das `connect()` funktioniert.
  Erkennungsmerkmal: direkt danach folgt trotzdem `Baue … -> OK (… Zeilen)`.
- **`WindowExec`-Warnung** (Window ohne `PARTITION BY`, z. B. bei der
  `gemeinde_id`-Vergabe): alle Daten in einer Partition — bei ~11 k Zeilen
  unkritisch.

## Offene Punkte

- **Präsentation erstellen** (Prüfungsleistung; Argumentationshilfen: README
  „Prüfungswissen kompakt" + [ADR.md](ADR.md)).
- **`dim_kreis.kreis_name` mehrdeutig:** optionale Zusatz-Spalte `kreis_typ`
  oder Originalname, um Stadt/Landkreis wieder unterscheidbar zu machen; bis
  dahin gilt die Contract-Regel „Joins nur über `kreis_id`".
- **Data-Contract-Härtung (Kür):** weitere ausführbare `quality`-SQLs
  (Row-Count-Minima, FK-Integrität, Composite-Key-Checks für
  `fact_bevoelkerung`/`fact_klima`).
- **Iceberg-Wartung (Ausblick):** Die Iceberg-Tabellen laufen
  merge-on-read — über sehr viele Läufe sammeln sich Delete-Dateien; bei
  dieser Datenmenge unkritisch, produktiv gehörte Compaction/Snapshot-Expiry
  dazu ([ADR-13](ADR.md)).
- **Airflow/Cloudera DE:** erledigt (11.07.2026) — der produktive Scheduler
  läuft jetzt als Airflow-DAG in Cloudera Data Engineering ([ADR-17](ADR.md),
  Deploy-Anleitung: [README → CDE-Deployment](../README.md)); offen ist nur
  noch die dortige Abnahme-Checkliste (Einzel-Job-Läufe, erzwungener
  Stufe-3-Build mit Zeilenzahl-Abgleich, kompletter DAG-Lauf mit 32 Checks).
- **Kosmetik:** die beiden harmlosen Warnungen oben.
