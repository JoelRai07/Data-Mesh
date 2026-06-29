# Stolpersteine bei der Spark-Pipeline (pipeline_spark.py)

Dieses Dokument haelt fest, welche Probleme beim Aufsetzen der echten
Apache-Spark-Pipeline (`src/pipeline_spark.py`) aufgetreten sind, warum sie
aufgetreten sind und wie sie geloest wurden. Gedacht als Nachschlagewerk,
falls die Pipeline auf einem anderen Rechner (z.B. bei einem anderen
Gruppenmitglied) zum ersten Mal laufen soll.

## Voraussetzungen, die NICHT automatisch da sind

| Voraussetzung | Warum noetig | Wie pruefen |
|---|---|---|
| **JDK 17** (genau diese Major-Version, kein 21/26) | PySpark 3.5.x ist nur bis Java 17 offiziell getestet | `java -version` |
| `src/utils/ImpalaJDBC42.jar` | JDBC-Treiber, mit dem Spark sich mit Impala verbindet (nicht im Git, da gross/lizenzpflichtig) | Datei muss lokal vorhanden sein |
| `.env` mit `IMPALA_HOST`, `IMPALA_PORT`, `IMPALA_HTTP_PATH`, `IMPALA_USER`, `IMPALA_PASSWORD` | Wird fuer den JDBC-Connection-String UND fuer die impyla-Inserts gebraucht | s. `.env.example` |

## Harmlose Warnung, die man ignorieren kann: log4j-`ClassCastException`

Bei **jedem** Connect zur Datenbank erscheinen Zeilen wie:

```
ERROR Unable to create Lookup for bundle
java.lang.ClassCastException: class org.apache.logging.log4j.core.lookup.ResourceBundleLookup
	at com.cloudera.impala.jdbc42.internal.apache.logging.log4j.core.lookup.Interpolator.<init>(...)
	...
	at com.cloudera.impala.jdbc.common.AbstractDriver.connect(Unknown Source)
```

(oft mehrfach hintereinander, fuer `bundle`, `ctx`, `date`, `env`, `event`,
`java`, `log4j`, `upper`, ...).

**Das ist kein Fehler unserer Pipeline.** Der Treiber
`ImpalaJDBC42.jar` bringt seine eigene, umbenannte ("geshadete") Kopie von
log4j mit (`com.cloudera.impala.jdbc42.internal.apache.logging.log4j...`).
Beim Initialisieren dieser internen log4j-Kopie kommt es zu einem
Klassen-Ladekonflikt mit Sparks eigenem log4j auf dem Classpath. log4j faengt
solche Fehler bei der eigenen Konfiguration intern ab und faellt auf eine
Default-Konfiguration zurueck - das DB-`connect()` selbst funktioniert davon
unbeeintraechtigt weiter. **Erkennungsmerkmal, dass alles in Ordnung ist:**
direkt danach folgt trotzdem `Baue <tabelle> ... -> OK (... Zeilen)`. Diese
Warnungen einfach ignorieren.

## Stolperstein 1: Kein/falsches Java installiert

**Symptom:** `java` ist im System nicht bekannt, oder es ist nur ein sehr
neues JDK (in unserem Fall JDK 26) installiert.

**Fehler bei JDK 26:**
```
java.lang.UnsupportedOperationException: getSubject is not supported
	at java.base/javax.security.auth.Subject.getSubject(...)
	at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(...)
```

**Ursache:** Neuere JDKs (ab ca. 23/24) entfernen alte, von Hadoop intern
genutzte Security-APIs (`Subject.getSubject`). Spark 3.5.x bringt eine
Hadoop-Version mit, die das noch braucht. Das ist eine harte
Inkompatibilitaet, keine Konfigurationsfrage.

**Loesung:** Zusaetzlich **Eclipse Temurin JDK 17** installieren (die alte
JDK-Version muss nicht entfernt werden, beide koennen parallel existieren).
`JAVA_HOME`/`PATH` muessen beim Start von Spark auf das JDK-17-Verzeichnis
zeigen, nicht auf das System-Default.

## Stolperstein 2: `spark.jars` + Windows = `winutils.exe`-Fehler

**Symptom:**
```
WARN Shell: Did not find winutils.exe: ... HADOOP_HOME and hadoop.home.dir are unset.
...
RuntimeException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

**Ursache:** Wird der JDBC-Treiber per `.config("spark.jars", pfad)`
eingebunden, kopiert Spark die Datei intern ueber Hadoops Dateiwerkzeuge
(`Utils.fetchFile` → `FileUtil.chmod`) - und das verlangt unter Windows ein
Hilfsprogramm namens `winutils.exe`, das normalerweise nicht installiert ist.

**Loesung:** Den Treiber stattdessen ueber
`spark.driver.extraClassPath` / `spark.executor.extraClassPath` einbinden.
Das haengt die Jar-Datei nur an den Java-Classpath an, ohne den
Hadoop-Kopiervorgang auszuloesen.

## Stolperstein 3: Spark erkennt Impala nicht als JDBC-Dialekt

**Symptom:** Beim Schreiben (`df.write.jdbc(...)`) versucht Spark trotz
`.option("truncate", "true")` die Zieltabelle neu anzulegen:
```
CREATE TABLE gruppe3_dim_kreis ("kreis_id" TEXT , "kreis_name" TEXT , ...)
ParseException: Syntax error in line 1
```

**Ursache:** Spark hat keinen eingebauten SQL-Dialekt fuer
`jdbc:impala://`-URLs. Es faellt auf einen generischen Dialekt zurueck
(doppelte Anfuehrungszeichen, Typ `TEXT`), den Impala nicht versteht. Der
Existenz-Check der Zieltabelle schlaegt dadurch fehl, Spark haelt die Tabelle
fuer nicht vorhanden und versucht, sie selbst anzulegen - mit invalider
Syntax fuer Impala.

**Folgeschaden, der dabei passiert ist:** In einem Testlauf hat Spark dabei
zuerst erfolgreich `DROP TABLE gruppe3_dim_kreis` ausgefuehrt und ist dann
beim `CREATE TABLE` gescheitert - die Tabelle war danach komplett weg.
Behoben durch erneutes Ausfuehren von `create_datamodel.py`
(`CREATE TABLE IF NOT EXISTS` legt sie klaglos neu an).

**Loesung:** Kein Spark-natives Schema-Handling mehr verwenden. Stattdessen:
1. `TRUNCATE TABLE` separat per **impyla** ausfuehren (impyla "spricht"
   natives Impala-SQL, das funktioniert problemlos).
2. Erst danach die neuen Zeilen schreiben (s. Stolperstein 5 - auch das
   passiert am Ende nicht mehr ueber `df.write.jdbc()`).

## Stolperstein 4: VPN blockiert Spark-interne Netzwerk-Sockets

**Symptom:**
```
java.net.SocketTimeoutException: Accept timed out
org.apache.spark.SparkException: Python worker failed to connect back.
```

**Ursache:** Operationen, die Daten zwischen dem Python-Treiberprozess und
der JVM ueber einen eigenen Socket austauschen (z.B.
`spark.createDataFrame(python_liste)`), bauen dafuer eine lokale
Server-Verbindung auf. Ein aktiver VPN-Client (in unserem Fall ein
`OpenVPN`/`Wintun`-Adapter) kann sich in die lokale Netzwerk-/Routing-Schicht
einmischen und genau diese Loopback-Verbindungen stoeren.

**Loesung (doppelt abgesichert):**
1. **Code-seitig:** `spark.createDataFrame(python_liste)` komplett
   vermieden. Stattdessen werden Werte, die man in Spark "von Hand" braucht
   (z.B. die Jahresliste 1995-2024 fuer `dim_jahr`), rein mit
   Spark-SQL-Funktionen erzeugt: `F.explode(F.sequence(F.lit(1995), F.lit(2024)))`
   auf einer JVM-internen `spark.range(1)`-DataFrame - das braucht keinen
   Python-Hilfsprozess.
2. **Betriebs-seitig:** VPN beim Pipeline-Lauf deaktiviert. Das hat das
   Problem zusaetzlich geloest und erlaubt seitdem auch wieder
   `master("local[*]")` (Mehrkern-Betrieb) statt der Notloesung `local[1]`.

## Stolperstein 5: Impala-JDBC-Treiber kann keine NULL-Parameter binden

**Symptom:**
```
com.cloudera.impala.support.exceptions.GeneralException:
[Cloudera][ImpalaJDBCDriver](500352) Error getting the parameter data type:
HIVE_PARAMETER_QUERY_DATA_TYPE_ERR_NON_SUPPORT_DATA_TYPE
```

**Ursache:** `df.write.jdbc(...)` schreibt Zeilen ueber parametrisierte
Batch-Inserts (JDBC `PreparedStatement`). Wenn ein Parameter in der ersten
Zeile eines Batches `NULL` ist, kann der Impala-Treiber dessen SQL-Typ nicht
bestimmen und bricht ab. Unsere Tabellen haben durchgehend NULL-faehige
Spalten (z.B. `kreis_id` in `dim_gemeinde`, wenn die Namens-Zuordnung
fehlschlaegt, oder KPI-Spalten mit Division durch 0 im ersten Jahr einer
Zeitreihe) - das ist also kein Rand-, sondern der Regelfall.

**Loesung:** Kompletter Verzicht auf `df.write.jdbc()` zum Schreiben.
Stattdessen holt `overwrite_table()` in `pipeline_spark.py` die fertigen
Zeilen mit `df.collect()` zum Treiber und baut daraus ganz normalen
SQL-Text (`INSERT INTO tabelle (...) VALUES (...), (...), ...`), den impyla
ausfuehrt - keine Parameter-Bindung mehr, also auch keine
Typ-Erkennungsprobleme. Zusaetzlich werden `NaN`/`Infinity`-Werte (koennen
bei Division durch 0 in Spark entstehen, anders als erwartet NICHT
automatisch `NULL`) vor dem Schreiben in `NULL` umgewandelt, da Impala diese
Literale syntaktisch nicht akzeptiert.

**Wichtig zu wissen:** Spark wird also nur noch zum **Lesen** der Rohdaten
und fuer **alle Berechnungen/Transformationen** genutzt (das war ja der
eigentliche Zweck dieser Variante). Das **Schreiben** laeuft am Ende ueber
denselben impyla-Weg wie in der reinen SQL-Pipeline (`pipeline.py`) - aus
gutem Grund: Impala ist primaer ein Lese-Motor, kein zuverlaessiges
JDBC-Write-Target.

## Stolperstein 6: `explode()` darf nicht in `CAST(...)` verschachtelt sein

**Symptom:**
```
AnalysisException: [UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS]
The generator is not supported: nested in expressions "CAST(explode(...) AS INT)"
```

**Ursache:** Spark-SQL erlaubt "Generator"-Funktionen wie `explode()` nur als
direkten Ausdruck in einem `SELECT`, nicht verschachtelt in einer anderen
Funktion wie `CAST()`.

**Loesung:** `CAST` einfach weglassen, wo er ohnehin unnoetig war
(`F.sequence(F.lit(1995), F.lit(2024))` liefert bereits ein
`array<int>`, `explode()` darauf also direkt `int`).

## Generelle Lektion fuer die Praesentation

Der grosse, fachlich relevante Punkt fuer die Pruefung: **Spark eignet sich
hervorragend zum Lesen/Transformieren von Daten, aber Impala ist kein
robustes Schreib-Ziel fuer Spark-JDBC-Writes.** Die Pipeline nutzt deshalb
einen Hybrid-Ansatz: Spark fuer Unpivot (`F.explode`), Pivot
(`DataFrame.pivot()`) und Window-Funktionen (u.a. echter z-Score per
`STDDEV() OVER (...)`, was in reinem Impala-SQL nicht moeglich war, s.
`pipeline.py`), aber impyla fuer das tatsaechliche Zurueckschreiben in die
Zieltabellen. Das ist eine bewusste Architekturentscheidung, kein
Kompromiss aus Zeitnot.

## Reihenfolge zum ersten Start auf einem neuen Rechner

1. JDK 17 installieren (s. Stolperstein 1).
2. `src/utils/ImpalaJDBC42.jar` an Ort und Stelle bringen.
3. `.env` ausfuellen (s. `.env.example`).
4. `pip install -r requirements.txt` (installiert u.a. `pyspark`).
5. `JAVA_HOME` auf das JDK-17-Verzeichnis setzen, bevor `pipeline_spark.py`
   oder `scheduler.py` gestartet wird (scheduler.py macht das inzwischen
   automatisch, s. Kommentar dort - Pfad ggf. anpassen).
6. `python src/create_datamodel.py` (idempotent, legt fehlende Tabellen an).
7. `python src/pipeline_spark.py` zum manuellen Testen, oder
   `python src/scheduler.py` fuer den dauerhaften 00:00-Uhr-Batch-Job.
