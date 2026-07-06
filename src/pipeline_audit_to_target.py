"""
DELIVERABLE 2 (Spark-Variante): Pipeline zur Befuellung des Datenmodells,
diesmal wirklich mit Apache Spark (PySpark DataFrame-API) statt mit
Impala-SQL-Strings wie in pipeline.py.

WARUM SO?
  - Spark wird hier als eigenstaendige Verarbeitungs-Engine genutzt: die
    bereits bereinigten audit_*-Tabellen (s. pipeline_staging_to_audit.py, STUFE 2 des
    WAP-Patterns) werden per Spark JDBC-Datasource AUS Impala gelesen
    (ueber denselben HiveServer2/Impala-Endpoint, den auch impyla nutzt - kein
    separater Cluster-Zugang noetig), als Spark-DataFrames transformiert und
    per Spark JDBC-Datasource WIEDER nach Impala zurueckgeschrieben.
  - Transformationen nutzen bewusst echte Spark-DataFrame-Idiome statt SQL-
    Strings, weil genau das der Mehrwert von Spark gegenueber der reinen
    Impala-SQL-Pipeline ist:
      * Unpivot von fact_bevoelkerung per F.explode(F.array(struct(...))) statt
        30x UNION ALL (in Impala-SQL mangels UNPIVOT noetig, in Spark unnoetig).
      * Pivot von fact_bauland per echtem DataFrame.pivot() statt manueller
        CASE-WHEN-Aggregation.
      * Window-Funktionen mit AVG()/STDDEV() OVER (PARTITION BY jahr) fuer den
        z-Score in fact_standortprofil_kpi - das hatte in Impala NICHT
        funktioniert (STDDEV ist dort keine Analytic-Function), in Spark
        funktioniert es direkt ueber pyspark.sql.Window.

ACHTUNG - UNGETESTETE ANNAHMEN, DIE IHR VOR DEM ERSTEN LAUF PRUEFEN MUESST:
  - JDBC-Treiberklasse: "com.cloudera.impala.jdbc.Driver" - direkt aus
    src/utils/ImpalaJDBC42.jar ausgelesen (META-INF/services/java.sql.Driver),
    also verifiziert, nicht geraten.
  - JDBC-Connection-String-Parameter (AuthMech, SSL, transportMode, httpPath):
    Standard-Syntax des Cloudera-Treibers fuer LDAP+HTTP+SSL, analog zu den
    Werten, die db.py fuer impyla nutzt. Ggf. anpassen, falls der Treiber
    andere Parameter-Namen erwartet.
  - Schreiben: Spark erkennt "jdbc:impala://" nicht als eigenen SQL-Dialekt
    und faellt auf einen generischen Dialekt zurueck (doppelte Anfuehrungs-
    zeichen, Typ TEXT) - die Option .option("truncate","true") fuehrt dadurch
    NICHT zu einem echten TRUNCATE, sondern Spark versucht beim Existenz-Check
    eine fuer Impala ungueltige Abfrage und faellt auf DROP+CREATE TABLE
    zurueck, was an Impalas Syntax scheitert (getestet, schlaegt fehl).
    Workaround: TRUNCATE TABLE wird separat per impyla ausgefuehrt (das
    Statement, das auch pipeline.py/impyla problemlos versteht), danach
    schreibt Spark nur noch per mode="append" - reines INSERT, ohne dass
    Spark irgendetwas am Tabellenschema anfasst.

Der JDBC-Treiber liegt unter src/utils/ImpalaJDBC42.jar (nicht eingecheckt,
muss lokal vorhanden sein) - Pfad wird unten ueber JDBC_JAR_PATH referenziert.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_audit_to_target.py
"""
import math
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from db import get_connection
from etl_state import ensure_state_table, get_latest_state, record_state

load_dotenv()

DATABASE = "gruppe3"
JDBC_JAR_PATH = os.path.join(os.path.dirname(__file__), "utils", "ImpalaJDBC42.jar")
JDBC_DRIVER_CLASS = "com.cloudera.impala.jdbc.Driver"  # verifiziert aus der Jar, s. Hinweis oben

# Fachliche Namen der vier Audit-Quelltabellen, aus denen sich das komplette
# Star-Schema speist (s. AUDIT_SOURCE_TABLES-Nutzung in should_skip_target_build).
AUDIT_SOURCE_TABLES = ["bauland", "bevoelkerungzahlen", "gemeinden", "klimadaten"]

TRUTHY_VALUES = {"1", "true", "yes", "y", "ja"}


def safe_div(numerator, denominator):
    """
    Division, die NULL liefert, wenn der Nenner 0 oder NULL ist - statt
    Infinity (x/0) oder NaN (0/0).

    WARUM WICHTIG: Ein einzelner Infinity-/NaN-Wert vergiftet spaeter jedes
    Fensteraggregat (AVG()/STDDEV() OVER (...)), das ihn mit einbezieht - das
    Ergebnis wird komplett NaN. Beim Zurueckschreiben wandelt _sql_literal NaN
    dann in NULL um. So wurde z.B. die komplette Spalte
    standortattraktivitaets_score NULL, weil 748 Bauland-Zeilen "Flaeche = 0"
    haben (Kaufsumme / 0 = Infinity). Details: docs/bugfix_score_nullwerte.md.
    """
    return F.when((denominator == 0) | denominator.isNull(), None).otherwise(
        numerator / denominator
    )


def get_spark():
    # extraClassPath statt spark.jars: spark.jars laesst Spark die Datei intern
    # ueber Hadoops Utils.fetchFile kopieren/chmod'en - das braucht unter
    # Windows winutils.exe (nicht vorhanden -> Crash). extraClassPath haengt
    # den Treiber nur an den JVM-Classpath an, ohne diesen Hadoop-Dateischritt.
    return (
        SparkSession.builder
        .appName("gruppe3_pipeline_audit_to_target")
        .master("local[*]")
        .config("spark.driver.extraClassPath", JDBC_JAR_PATH)
        .config("spark.executor.extraClassPath", JDBC_JAR_PATH)
        # Windows-Fix: Treiber/Executor explizit an die lokale Schleife binden,
        # sonst versuchen Python-Worker-Prozesse oft ueber die falsche
        # Netzwerkschnittstelle zum Treiber zurueckzuverbinden und laufen in
        # einen Timeout ("Accept timed out" / "Python worker failed to
        # connect back").
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def jdbc_url():
    host = os.getenv("IMPALA_HOST")
    port = os.getenv("IMPALA_PORT", "443")
    http_path = os.getenv("IMPALA_HTTP_PATH")
    user = os.getenv("IMPALA_USER")
    password = os.getenv("IMPALA_PASSWORD")
    return (
        f"jdbc:impala://{host}:{port}/{DATABASE};"
        f"AuthMech=3;UID={user};PWD={password};SSL=1;"
        f"transportMode=http;httpPath={http_path}"
    )


def read_table(spark, table_name):
    """Liest eine Tabelle (oder Subquery) aus Impala per Spark-JDBC-Datasource."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", table_name)
        .option("driver", JDBC_DRIVER_CLASS)
        .load()
    )


def read_gemeinden(spark):
    """
    Liest die Gemeinden aus gruppe3_audit_gemeinden (bereits bereinigt, s.
    pipeline_staging_to_audit.py: municipality_name transliteriert, district_kreis ab
    dem ersten Komma abgeschnitten + transliteriert). Koordinaten sind dort
    unveraendert im Komma-Dezimalformat der Quelle ("9,43751").

    Zwei JDBC-Fallstricke werden hier umgangen:
    1) Der Cloudera-JDBC-Treiber wirft bei den double-Spalten area_km2/per_km2
       "[Cloudera][JDBC](10140) Error converting value to double" (getDouble).
       -> per `customSchema` als STRING lesen (getString), dann in Spark casten.
    2) Ein Subquery-dbtable ("(SELECT ...) t") liefert mit diesem Treiber teils
       0 Zeilen an Spark. -> Deshalb die volle Tabelle lesen (kein Subquery)
       und die Typen nur via customSchema ueberschreiben.
    """
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", "gruppe3_audit_gemeinden")
        .option("driver", JDBC_DRIVER_CLASS)
        .option("customSchema", "area_km2 STRING, per_km2 STRING")
        .load()
        .withColumn("area_km2", F.trim(F.col("area_km2")).cast("double"))
    )


def truncate_table(table_name):
    """
    Leert die Zieltabelle per impyla (TRUNCATE TABLE), BEVOR Spark schreibt.
    Grund: Spark erkennt den Impala-JDBC-Dialekt nicht und wuerde bei
    .option("truncate","true") versuchen, Existenz-Check/CREATE TABLE mit
    einem generischen (fuer Impala syntaktisch falschen) SQL-Dialekt
    auszufuehren - s. Hinweis im Modul-Docstring. TRUNCATE TABLE selbst
    versteht Impala über impyla aber problemlos (gleiches Pattern wie in
    pipeline.py).
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    cur.execute(f"TRUNCATE TABLE {table_name}")
    cur.close()
    conn.close()


def _sql_literal(value):
    """Wandelt einen Python-/Spark-Wert in ein SQL-Literal fuer INSERT...VALUES um."""
    if value is None:
        return "NULL"
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            # Impala kann NaN/Infinity nicht als DOUBLE-Literal parsen -
            # fachlich ist das sowieso ein "nicht definierter" KPI-Wert (z.B.
            # Division durch 0 bei fehlenden Vorjahresdaten), also NULL.
            return "NULL"
        return repr(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int,)):
        return str(value)
    # String: einfache Anfuehrungszeichen escapen
    return "'" + str(value).replace("'", "''") + "'"


def overwrite_table(df, table_name, batch_size=500):
    """
    Sammelt die Ergebniszeilen ein, leert danach die Zieltabelle
    (truncate_table) und schreibt per impyla als INSERT INTO ... VALUES
    (...)-Batches.

    WARUM NICHT df.write.jdbc(...)?
    Probiert, schlaegt aber zuverlaessig fehl: der Impala-JDBC-Treiber kann
    bei parametrisierten Batch-Inserts (PreparedStatement) den SQL-Typ eines
    Parameters nicht bestimmen, wenn dessen Wert NULL ist
    (HIVE_PARAMETER_QUERY_DATA_TYPE_ERR_NON_SUPPORT_DATA_TYPE) - und unsere
    Tabellen haben durchgehend NULL-faehige Spalten (z.B. nicht zuordenbare
    kreis_id in dim_gemeinde, KPIs mit Division durch 0 im ersten Jahr usw.).
    Deshalb: Zeilen zum Treiber holen (collect() - bei unseren Datengroessen
    von ein paar zehntausend Zeilen unproblematisch) und als reinen SQL-Text
    einfuegen, ganz ohne Parameter-Bindung.
    """
    rows = df.collect()
    truncate_table(table_name)

    if not rows:
        return

    columns = df.columns
    col_list = ", ".join(columns)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")

    for start in range(0, len(rows), batch_size):
        chunk = rows[start:start + batch_size]
        values_sql = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in chunk
        )
        cur.execute(f"INSERT INTO {table_name} ({col_list}) VALUES {values_sql}")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# DIMENSIONEN
# ---------------------------------------------------------------------------

BUNDESLAND_NAMEN = {
    "01": "Schleswig-Holstein", "02": "Hamburg", "03": "Niedersachsen",
    "04": "Bremen", "05": "Nordrhein-Westfalen", "06": "Hessen",
    "07": "Rheinland-Pfalz", "08": "Baden-Wuerttemberg", "09": "Bayern",
    "10": "Saarland", "11": "Berlin", "12": "Brandenburg",
    "13": "Mecklenburg-Vorpommern", "14": "Sachsen", "15": "Sachsen-Anhalt",
    "16": "Thueringen",
}


def build_dim_kreis(spark):
    bev = read_table(spark, "gruppe3_audit_bevoelkerungzahlen")

    bundesland_expr = F.create_map([F.lit(x) for kv in BUNDESLAND_NAMEN.items() for x in kv])

    return (
        bev.filter(F.length("id") == 5)
        .withColumn("bundesland_id", F.substring("id", 1, 2))
        .select(
            F.col("id").alias("kreis_id"),
            F.col("kreis").alias("kreis_name"),
            F.col("bundesland_id"),
            bundesland_expr[F.col("bundesland_id")].alias("bundesland_name"),
        )
    )


def build_dim_jahr(spark):
    """
    Vereinigung der Bauland-Jahre mit dem bekannten Bevoelkerungs-Zeitraum
    1995-2024. Die Jahresliste wird per F.sequence()/F.explode() rein auf der
    JVM-Seite erzeugt (NICHT per spark.createDataFrame(python_liste) - das
    wuerde einen Python-Hilfsprozess fuer die Datenuebergabe brauchen, der
    unter Windows/mit aktivem VPN haeufig an Netzwerk-Timeouts scheitert,
    s. "Accept timed out" beim ersten Testlauf).
    """
    bauland_jahre = (
        read_table(spark, "gruppe3_audit_bauland")
        .select(F.col("jahr").cast(IntegerType()).alias("jahr"))
        .filter(F.col("jahr").isNotNull())
    )
    bevoelkerung_jahre = spark.range(1).select(
        F.explode(F.sequence(F.lit(1995), F.lit(2024))).alias("jahr")
    )
    return (
        bauland_jahre.unionByName(bevoelkerung_jahre)
        .distinct()
        .withColumn("jahrzehnt", (F.floor(F.col("jahr") / 10) * 10).cast(IntegerType()))
    )


def build_dim_klimastadt(spark):
    """
    country = 'Germany' ist bereits in gruppe3_audit_klimadaten gefiltert und
    city bereits transliteriert (s. pipeline_staging_to_audit.py). latitude/longitude
    liegen dort im selben Komma-Dezimalformat wie gruppe3_audit_gemeinden vor
    (Himmelsrichtungs-Buchstabe/Vorzeichen-Umrechnung ist schon im Audit-
    Schritt erledigt) - Parsing hier daher identisch zu build_dim_gemeinde.
    """
    klima = read_table(spark, "gruppe3_audit_klimadaten")
    return klima.select(
        F.col("city").alias("stadt_name"),
        F.regexp_replace(F.col("latitude"), ",", ".").cast("double").alias("latitude"),
        F.regexp_replace(F.col("longitude"), ",", ".").cast("double").alias("longitude"),
    ).distinct()


def build_dim_gemeinde(spark, dim_kreis):
    """
    Bruecken-Dimension. DATENQUALITAET: project_gemeinden hat ein CSV-Parsing-
    Problem (Kommas in Gemeindenamen verschieben alle Folgespalten) - wir
    laden daher nur Zeilen mit area_km2 IS NOT NULL (verlaesslicher Indikator
    fuer "nicht verrutscht"), analog zur Begruendung in pipeline.py.

    Kreis-Zuordnung per Fuzzy-Match (kreis_name enthaelt den bereinigten
    district_kreis-Text) statt Gleichheit, weil die Schreibweisen abweichen
    (z.B. "Flensburg" vs. "Flensburg, kreisfreie Stadt"). Bei mehreren
    Treffern wird per Window/row_number der laengste (= spezifischste)
    Kreisname gewaehlt.
    """
    gem = (
        read_gemeinden(spark)
        .filter(F.col("area_km2").isNotNull())
        .withColumn("district_clean", F.trim(F.regexp_replace(F.col("district_kreis"), '"', "")))
    )

    joined = gem.join(
        dim_kreis,
        F.lower(dim_kreis.kreis_name).contains(F.lower(gem.district_clean)),
        "left",
    )

    rn_window = Window.partitionBy(
        gem.municipality_name, gem.postal_code
    ).orderBy(F.length(dim_kreis.kreis_name).desc_nulls_last())

    deduped = (
        joined.withColumn("rn", F.row_number().over(rn_window))
        .filter(F.col("rn") == 1)
    )

    gemeinde_id_window = Window.orderBy("municipality_name", "postal_code")

    return deduped.select(
        F.row_number().over(gemeinde_id_window).cast("string").alias("gemeinde_id"),
        F.col("municipality_name").alias("gemeinde_name"),
        F.col("kreis_id"),
        F.col("state_land").alias("bundesland_name"),
        F.col("postal_code"),
        # Deutsches Dezimalkomma -> Punkt, dann in double wandeln (9,43751 -> 9.43751).
        # Klimadaten liegen nach dem Audit-Schritt im selben Format vor (s.
        # build_dim_klimastadt), das hier verwendete Parsing ist identisch.
        F.regexp_replace(F.col("latitude"), ",", ".").cast("double").alias("latitude"),
        F.regexp_replace(F.col("longitude"), ",", ".").cast("double").alias("longitude"),
    )


# ---------------------------------------------------------------------------
# BASIS-FAKTEN
# ---------------------------------------------------------------------------

def build_fact_bevoelkerung(spark):
    """
    Unpivot per F.explode(F.array(struct(...))): fuer jedes Jahr 1995-2024
    wird ein struct(jahr, einwohner_*) gebaut, alle structs landen in einem
    array, explode() macht daraus eine Zeile pro Jahr - der idiomatische
    Spark-Ersatz fuer ein generisches UNPIVOT.
    """
    bev = read_table(spark, "gruppe3_audit_bevoelkerungzahlen").filter(F.length("id") == 5)

    jahres_structs = [
        F.struct(
            F.lit(jahr).alias("jahr"),
            F.col(f"insgesamt_{jahr % 100:02d}").alias("einwohner_insgesamt"),
            F.col(f"maennlich_{jahr % 100:02d}").alias("einwohner_maennlich"),
            F.col(f"weiblich_{jahr % 100:02d}").alias("einwohner_weiblich"),
        )
        for jahr in range(1995, 2025)
    ]

    unpivoted = (
        bev.select("id", F.explode(F.array(*jahres_structs)).alias("j"))
        .select("id", "j.*")
        .filter(F.col("einwohner_insgesamt").isNotNull())
        .withColumnRenamed("id", "kreis_id")
    )

    kreis_jahr_window = Window.partitionBy("kreis_id").orderBy("jahr")
    vorjahr = F.lag("einwohner_insgesamt").over(kreis_jahr_window)

    return unpivoted.select(
        "kreis_id",
        "jahr",
        "einwohner_insgesamt",
        "einwohner_maennlich",
        "einwohner_weiblich",
        F.round(safe_div(F.col("einwohner_maennlich"), F.col("einwohner_weiblich")), 4).alias("geschlechterquotient"),
        F.round(
            safe_div(100.0 * (F.col("einwohner_insgesamt") - vorjahr), vorjahr), 3
        ).alias("wachstum_vorjahr_pct"),
    )


def build_fact_bauland(spark):
    """
    Echtes DataFrame.pivot() statt CASE-WHEN-Aggregation (so wie in
    pipeline.py mangels Pivot-Support in einfachem Impala-SQL geloest).
    Die merkmal-Spalte war durch einen Encoding-Fehler beschaedigt
    ('Ver?u?erungsfaelle...' statt 'Veraeusserungsfaelle...'), das ist aber
    bereits im Audit-Schritt auf feste, saubere Werte korrigiert (s.
    BAULAND_MERKMAL_CORRECTIONS in pipeline_staging_to_audit.py) - hier reicht daher ein
    einfacher Gleichheitsvergleich statt der frueheren rlike-Heuristik.

    Alle 4 Merkmale der Quelle werden pivotiert (nicht nur 3 wie zuvor):
    "Durchschnittlicher Kaufwert je qm" ist der amtliche Wert und wird 1:1 als
    kaufwert_je_qm_eur uebernommen - zusaetzlich zur weiterhin selbst
    berechneten KPI preis_pro_qm_eur (kaufsumme_tsd_eur / veraeusserte_flaeche_1000qm),
    damit amtlicher und berechneter Wert vergleichbar bleiben.
    """
    bauland = read_table(spark, "gruppe3_audit_bauland").filter(F.length("kreis_id") == 5)

    kategorisiert = bauland.withColumn(
        "kategorie",
        F.when(F.col("merkmal") == "Veraeusserungsfaelle von Bauland", "faelle")
        .when(F.col("merkmal") == "Veraeusserte Baulandflaeche", "flaeche")
        .when(F.col("merkmal") == "Kaufsumme", "kaufsumme")
        .when(F.col("merkmal") == "Durchschnittlicher Kaufwert je qm", "kaufwert")
        .otherwise(None),
    ).filter(F.col("kategorie").isNotNull())

    pivoted = (
        kategorisiert.groupBy("kreis_id", "jahr")
        .pivot("kategorie", ["faelle", "flaeche", "kaufsumme", "kaufwert"])
        .agg(F.first("insgesamt"))
    )
    baureif_flaeche = (
        kategorisiert.filter(F.col("kategorie") == "flaeche")
        .groupBy("kreis_id", "jahr")
        .agg(F.first("baureifes_land").alias("baureifes_land"))
    )

    result = pivoted.join(baureif_flaeche, ["kreis_id", "jahr"], "left")

    return result.select(
        "kreis_id",
        F.col("jahr").cast(IntegerType()).alias("jahr"),
        F.col("faelle").alias("anzahl_veraeusserungsfaelle"),
        F.col("flaeche").alias("veraeusserte_flaeche_1000qm"),
        F.col("kaufsumme").alias("kaufsumme_tsd_eur"),
        F.col("kaufwert").cast("double").alias("kaufwert_je_qm_eur"),
        F.round(safe_div(F.col("kaufsumme"), F.col("flaeche")), 2).alias("preis_pro_qm_eur"),
        F.round(safe_div(100.0 * F.col("baureifes_land"), F.col("flaeche")), 2).alias("anteil_baureif_pct"),
        F.round(safe_div(1000.0 * F.col("flaeche"), F.col("faelle")), 2).alias("durchschnittsfall_qm"),
    )


def build_fact_klima(spark):
    # country = 'Germany' ist bereits in gruppe3_audit_klimadaten gefiltert,
    # city bereits transliteriert (s. pipeline_staging_to_audit.py) - dieselbe
    # Schreibweise wie dim_klimastadt.stadt_name/dim_gemeinde.gemeinde_name,
    # damit der Namens-Join in build_fact_standortprofil_kpi funktioniert.
    klima = read_table(spark, "gruppe3_audit_klimadaten").filter(
        F.col("averagetemperature").isNotNull()
    ).withColumn("jahr", F.substring("dt", 1, 4).cast(IntegerType()))

    jahresmittel = (
        klima.groupBy(F.col("city").alias("stadt_name"), "jahr")
        .agg(F.avg("averagetemperature").alias("avg_temperatur"))
    )
    referenz = (
        klima.filter((F.col("jahr") >= 1961) & (F.col("jahr") <= 1990))
        .groupBy(F.col("city").alias("stadt_name"))
        .agg(F.avg("averagetemperature").alias("referenz_temp"))
    )

    return (
        jahresmittel.join(referenz, "stadt_name")
        .filter((F.col("jahr") >= 1995) & (F.col("jahr") <= 2024))
        .select(
            "stadt_name",
            "jahr",
            F.round("avg_temperatur", 2).alias("avg_temperatur"),
            F.round(F.col("avg_temperatur") - F.col("referenz_temp"), 2).alias("temperatur_abweichung_grad"),
        )
    )


def build_fact_gemeinde_stamm(spark, dim_gemeinde):
    # Aus gruppe3_audit_gemeinden lesen (s. read_gemeinden); area_km2 kommt
    # als String und wird dort sicher in double gewandelt.
    dedupe_window = Window.partitionBy("municipality_name", "postal_code").orderBy(
        F.col("district_kreis").asc_nulls_last(),
        F.col("latitude").asc_nulls_last(),
        F.col("longitude").asc_nulls_last(),
    )
    gem = (
        read_gemeinden(spark)
        .filter(F.col("area_km2").isNotNull())
        .withColumn("rn", F.row_number().over(dedupe_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    joined = gem.join(
        dim_gemeinde,
        (gem.municipality_name == dim_gemeinde.gemeinde_name) & (gem.postal_code == dim_gemeinde.postal_code),
    )

    return joined.select(
        dim_gemeinde.gemeinde_id,
        F.col("population_total").alias("einwohner_total"),
        F.col("male").alias("einwohner_maennlich"),
        F.col("female").alias("einwohner_weiblich"),
        F.round(safe_div(100.0 * F.col("female"), F.col("population_total")), 2).alias("anteil_weiblich_pct"),
        F.col("area_km2"),
        # per_km2 aus der Quelle NICHT lesen: der Cloudera-JDBC-Treiber wirft dort
        # bei manchen Zeilen "Error converting value to double". Die Dichte
        # berechnen wir stattdessen selbst (population_total / area_km2, fachlich
        # identisch) - so wird die problematische Spalte gar nicht erst gelesen.
        F.round(safe_div(F.col("population_total"), F.col("area_km2")), 2).alias("einwohner_pro_km2"),
    )


# ---------------------------------------------------------------------------
# CROSS-TABLE-KPI-FAKT
# ---------------------------------------------------------------------------

def build_fact_standortprofil_kpi(spark, dim_gemeinde, fact_bevoelkerung, fact_bauland, fact_klima, fact_gemeinde_stamm):
    """
    Verdichtet alle Basis-Fakten zu Kreis x Jahr-KPIs. Klima haengt nur auf
    Stadt-Ebene, daher ueber dim_gemeinde + Klimastadt gleichen Namens
    (gemeinde_name == stadt_name, beide Seiten transliteriert in
    pipeline_staging_to_audit.py) auf Kreis-Ebene hochaggregiert.

    WARUM NAMENS- STATT KOORDINATEN-JOIN?
    Frueher wurde ueber die naechstgelegene Klimastadt per euklidischer lat/
    long-Distanz gematcht. Der Namens-Join ist einfacher, deckt aber nur
    Faelle ab, in denen der Gemeindename nach der Transliteration EXAKT dem
    Klimastadt-Namen entspricht - von den 81 deutschen Staedten in
    gruppe3_audit_klimadaten matchen so ca. 59 (der Rest sind englische
    Namen wie "Munich"/"Cologne" oder mehrdeutige Kurzformen wie "Frankfurt",
    die sowohl zu "Frankfurt am Main" als auch "Frankfurt (Oder)" passen
    wuerden). Bewusst kein Fuzzy-/Contains-Match: bei mehrdeutigen Kurz-
    namen (z.B. "Oldenburg" passt auf 7 Gemeinden) waere die Auswahl reine
    Rateroei. Gemeinden ohne Namens-Treffer bleiben ohne Klimawert (per
    LEFT JOIN + fillna 0.0 unten, wie zuvor bei fehlenden Koordinaten).
    Nachbereinigung: Der Wechsel weg vom Distanz-Match kostet Genauigkeit,
    ist bei Bedarf spaeter ueber eine manuelle Korrekturliste (aehnlich
    KREIS_CORRECTIONS) fuer die bekannten Sonderfaelle nachschaerfbar.

    wohnraumdruck_index (und darauf aufbauend klima_angepasstes_wohnraumrisiko)
    war urspruenglich ein Verhaeltnis zweier Wachstumsraten
    (bevoelkerungswachstum_pct / bauland_angebotswachstum_pct). Das erzeugte
    fachlich falsche Vorzeichen: negativ/negativ ergibt positiv, positiv/negativ
    ergibt negativ - das Vorzeichen spiegelte also die Vorzeichen-KOMBINATION
    beider Wachstumsraten wider, nicht den tatsaechlichen Druck (Beispiel:
    Ostallgaeu 2021 mit wachsender Bevoelkerung + schrumpfendem Bauland-
    Angebot - eigentlich Lehrbuch-Beispiel fuer hohen Druck - bekam den
    negativsten Wert im Datensatz, waehrend Eisenach 2021 mit kollabierender
    Bevoelkerung den hoechsten "Druck"-Wert bekam). Jetzt stattdessen ein
    Verhaeltnis von Bestandsgroessen (Einwohner je 1000 qm NEU veraeusserter
    Baulandflaeche) - beide Groessen sind nie negativ, daher ist auch der
    Index nie negativ und steigt monoton mit dem tatsaechlichen Druck
    (mehr Einwohner pro verfuegbarer Flaeche = mehr Druck).

    standortattraktivitaets_score ist ein echter z-Score per
    AVG()/STDDEV() OVER (PARTITION BY jahr) - in Spark direkt moeglich
    (anders als in Impala, wo STDDEV keine Analytic-Function ist - s.
    Hinweis in pipeline.py). Negative Werte sind hier bewusst und korrekt
    (= unterdurchschnittlich attraktiv ggue. allen Kreisen desselben Jahres),
    anders als beim alten wohnraumdruck_index.
    """
    bev = fact_bevoelkerung.select("kreis_id", "jahr", "einwohner_insgesamt", "wachstum_vorjahr_pct")

    bau = fact_bauland.select(
        "kreis_id", "jahr", "kaufsumme_tsd_eur", "veraeusserte_flaeche_1000qm", "preis_pro_qm_eur"
    )

    gemeinde_mit_kreis = dim_gemeinde.filter(F.col("kreis_id").isNotNull())
    klimastadt = build_dim_klimastadt(spark)

    klimastadt_match = (
        gemeinde_mit_kreis.join(
            klimastadt,
            gemeinde_mit_kreis.gemeinde_name == klimastadt.stadt_name,
        )
        .select("gemeinde_id", "kreis_id", "stadt_name")
    )

    klima_je_kreis = (
        klimastadt_match.join(fact_klima, "stadt_name")
        .groupBy("kreis_id", "jahr")
        .agg(F.avg("temperatur_abweichung_grad").alias("temperatur_abweichung_grad"))
    )

    dichte_je_kreis = (
        fact_gemeinde_stamm.join(dim_gemeinde, "gemeinde_id")
        .filter(F.col("kreis_id").isNotNull())
        .groupBy("kreis_id")
        .agg(
            F.avg("einwohner_pro_km2").alias("kreis_avg_dichte"),
            F.max("einwohner_pro_km2").alias("max_gemeinde_dichte"),
        )
    )

    df = (
        bev.join(bau, ["kreis_id", "jahr"])
        .join(klima_je_kreis, ["kreis_id", "jahr"], "left")
        .join(dichte_je_kreis, "kreis_id", "left")
        .fillna({"temperatur_abweichung_grad": 0.0})
    )

    jahr_window = Window.partitionBy("jahr")

    result = df.select(
        "kreis_id",
        "jahr",
        F.round(safe_div(F.col("einwohner_insgesamt"), F.col("veraeusserte_flaeche_1000qm")), 3).alias("wohnraumdruck_index"),
        F.round(safe_div(1000.0 * F.col("kaufsumme_tsd_eur"), F.col("einwohner_insgesamt")), 2).alias("baulandpreis_pro_kopf_eur"),
        F.round(safe_div(1000.0 * F.col("veraeusserte_flaeche_1000qm"), F.col("einwohner_insgesamt")), 4).alias("freiflaeche_pro_einwohner_qm"),
        F.round(
            safe_div(F.col("einwohner_insgesamt"), F.col("veraeusserte_flaeche_1000qm")) * (1 + F.col("temperatur_abweichung_grad") / 10),
            3,
        ).alias("klima_angepasstes_wohnraumrisiko"),
        F.round(safe_div(F.col("max_gemeinde_dichte"), F.col("kreis_avg_dichte")), 3).alias("verstaedterung_index"),
        F.round(
            safe_div(
                F.col("wachstum_vorjahr_pct") - F.avg("wachstum_vorjahr_pct").over(jahr_window),
                F.stddev("wachstum_vorjahr_pct").over(jahr_window),
            )
            - safe_div(
                F.col("preis_pro_qm_eur") - F.avg("preis_pro_qm_eur").over(jahr_window),
                F.stddev("preis_pro_qm_eur").over(jahr_window),
            )
            # Klima-Term: mit coalesce(..., 0) abgesichert, damit ein fehlender
            # Klimawert (z.B. weil die Gemeinde-Koordinaten in den Rohdaten
            # zerstoert sind, s. docs/bugfix_score_nullwerte.md) den GESAMTEN
            # Score nicht auf NULL zieht. Fehlt Klima, zaehlt es neutral (0).
            - F.coalesce(
                F.abs(
                    safe_div(
                        F.col("temperatur_abweichung_grad") - F.avg("temperatur_abweichung_grad").over(jahr_window),
                        F.stddev("temperatur_abweichung_grad").over(jahr_window),
                    )
                ),
                F.lit(0.0),
            ),
            3,
        ).alias("standortattraktivitaets_score"),
    )

    # Zeilen verwerfen, bei denen ALLE 6 KPI-Spalten NULL sind (komplett
    # uninformative Kreis+Jahr-Kombination, z.B. weil weder Bauland- noch
    # Klima- noch Gemeinde-Dichte-Daten fuer dieses Jahr vorlagen). Bewusst
    # NICHT einfach nach einer einzelnen Spalte (z.B. wohnraumdruck_index)
    # gefiltert - das wuerde auch Zeilen loeschen, die z.B. noch einen
    # gueltigen verstaedterung_index haben (kommt vor, da dieser unabhaengig
    # vom Bauland-Join berechnet wird).
    KPI_SPALTEN = [
        "wohnraumdruck_index", "baulandpreis_pro_kopf_eur", "freiflaeche_pro_einwohner_qm",
        "klima_angepasstes_wohnraumrisiko", "verstaedterung_index", "standortattraktivitaets_score",
    ]
    return result.filter(F.coalesce(*KPI_SPALTEN).isNotNull())


# ---------------------------------------------------------------------------
# INCREMENTAL LOADING - STUFE 3 (audit -> target)
# ---------------------------------------------------------------------------
#
# Das Star-Schema selbst bleibt bewusst Full Reload (s. should_skip_target_build
# und die Erklaerung dort) - Unpivot/Pivot sowie die Fenster-Funktionen
# AVG()/STDDEV() OVER (PARTITION BY jahr) in build_fact_standortprofil_kpi
# brauchen ohnehin JEDE Zeile der betroffenen Kreis+Jahr-Partition, ein
# "nur die neuen Zeilen nachrechnen" wuerde bei bestehenden Jahren falsche
# Durchschnitte/Standardabweichungen liefern. Die Ziel-Faktentabellen sind mit
# ca. 400 Kreisen x 30 Jahren zudem klein (Groessenordnung 10.000-15.000
# Zeilen) - anders als bei gruppe3_staging_klimadaten (8,6 Mio. Zeilen) lohnt
# sich ein inkrementeller Teil-Rebuild hier fachlich nicht.
#
# Der EINE Punkt, an dem trotzdem unnoetige Arbeit vermieden wird: der
# komplette Spark-Lauf (JVM-Start, JDBC-Reads, Neuberechnung, impyla-
# Batch-Writes) wird uebersprungen, wenn sich SEIT DEM LETZTEN Ziel-Build
# keine der vier Audit-Tabellen veraendert hat (s. should_skip_target_build).
# Das ist besonders relevant, weil scheduler.py diese Pipeline taeglich
# (im Testmodus sogar minuetlich) aufruft, waehrend pipeline_default_to_staging.py/
# pipeline_staging_to_audit.py aktuell manuell und deutlich seltener laufen.


def should_skip_target_build(cur):
    """
    Liest den Aufzeichnungszeitpunkt des letzten erfolgreichen Ziel-Builds
    (stage="target") und vergleicht ihn mit dem juengsten Aenderungszeitpunkt
    der vier Audit-Tabellen. record_state() in pipeline_staging_to_audit.py wird
    NUR aufgerufen, wenn eine Audit-Tabelle tatsaechlich neu verarbeitet wurde
    (s. dortige audit_table_incremental/audit_table_snapshot) - der juengste
    recorded_at-Wert je Audit-Tabelle ist daher exakt "wann hat sich dieser
    Audit-Stand zuletzt inhaltlich veraendert".

    True, wenn seit dem letzten Ziel-Build nichts Neues vorliegt (Spark-Lauf
    kann uebersprungen werden). False, wenn noch nie gebaut wurde oder
    mindestens eine Audit-Tabelle neuer ist als der letzte Ziel-Build.
    """
    target_state = get_latest_state(cur, "target", "all")
    if target_state is None:
        return False

    for name in AUDIT_SOURCE_TABLES:
        audit_state = get_latest_state(cur, "audit", name)
        if audit_state is None or audit_state["recorded_at"] > target_state["recorded_at"]:
            return False

    return True


def should_force_target_build():
    """Erzwingt den Target-Rebuild bei Code-/Contract-Aenderungen ohne neue Audit-Daten."""
    return os.getenv("FORCE_TARGET_BUILD", "").strip().lower() in TRUTHY_VALUES


# ---------------------------------------------------------------------------
# AUSFUEHRUNG
# ---------------------------------------------------------------------------

def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    ensure_state_table(cur)
    force = should_force_target_build()
    skip = False if force else should_skip_target_build(cur)
    cur.close()
    conn.close()

    if skip:
        print("Keine Aenderung in den Audit-Tabellen seit dem letzten Ziel-Build - Spark-Lauf wird uebersprungen.")
        return
    if force:
        print("FORCE_TARGET_BUILD=1 gesetzt - Spark-Lauf wird trotz unveraenderter Audit-Tabellen ausgefuehrt.")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Baue dim_kreis ...")
    dim_kreis = build_dim_kreis(spark).cache()
    overwrite_table(dim_kreis, "gruppe3_dim_kreis")
    print(f"  -> OK ({dim_kreis.count()} Zeilen)")

    print("Baue dim_jahr ...")
    dim_jahr = build_dim_jahr(spark)
    overwrite_table(dim_jahr, "gruppe3_dim_jahr")
    print(f"  -> OK ({dim_jahr.count()} Zeilen)")

    print("Baue dim_klimastadt ...")
    dim_klimastadt = build_dim_klimastadt(spark)
    overwrite_table(dim_klimastadt, "gruppe3_dim_klimastadt")
    print(f"  -> OK ({dim_klimastadt.count()} Zeilen)")

    print("Baue dim_gemeinde ...")
    dim_gemeinde = build_dim_gemeinde(spark, dim_kreis).cache()
    overwrite_table(dim_gemeinde, "gruppe3_dim_gemeinde")
    print(f"  -> OK ({dim_gemeinde.count()} Zeilen)")

    print("Baue fact_bevoelkerung ...")
    fact_bevoelkerung = build_fact_bevoelkerung(spark).cache()
    overwrite_table(fact_bevoelkerung, "gruppe3_fact_bevoelkerung")
    print(f"  -> OK ({fact_bevoelkerung.count()} Zeilen)")

    print("Baue fact_bauland ...")
    fact_bauland = build_fact_bauland(spark).cache()
    overwrite_table(fact_bauland, "gruppe3_fact_bauland")
    print(f"  -> OK ({fact_bauland.count()} Zeilen)")

    print("Baue fact_klima ...")
    fact_klima = build_fact_klima(spark).cache()
    overwrite_table(fact_klima, "gruppe3_fact_klima")
    print(f"  -> OK ({fact_klima.count()} Zeilen)")

    print("Baue fact_gemeinde_stamm ...")
    fact_gemeinde_stamm = build_fact_gemeinde_stamm(spark, dim_gemeinde).cache()
    overwrite_table(fact_gemeinde_stamm, "gruppe3_fact_gemeinde_stamm")
    print(f"  -> OK ({fact_gemeinde_stamm.count()} Zeilen)")

    print("Baue fact_standortprofil_kpi ...")
    fact_standortprofil_kpi = build_fact_standortprofil_kpi(
        spark, dim_gemeinde, fact_bevoelkerung, fact_bauland, fact_klima, fact_gemeinde_stamm
    )
    overwrite_table(fact_standortprofil_kpi, "gruppe3_fact_standortprofil_kpi")
    print(f"  -> OK ({fact_standortprofil_kpi.count()} Zeilen)")

    spark.stop()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    record_state(cur, "target", "all")
    cur.close()
    conn.close()

    print("\nFertig.")


if __name__ == "__main__":
    main()
