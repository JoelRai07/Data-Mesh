"""
WAP Stufe 3: audit table -> Star-Schema (Spark).
Input: gruppe3_audit_bauland/audit_bevoelkerungzahlen/audit_gemeinden/audit_klimadaten (JDBC-Read).
Output: gruppe3_dim_kreis/dim_jahr/dim_gemeinde/dim_klimastadt,
gruppe3_fact_bevoelkerung/fact_bauland/fact_klima/fact_gemeinde_stamm/fact_standortprofil_kpi
(impyla-Write, s. overwrite_table).

Braucht JDK 17 (JAVA_HOME_JDK17 in .env) und src/utils/ImpalaJDBC42.jar
(nicht eingecheckt). Details/Stolpersteine: docs/spark_stolpersteine.md.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_audit_to_target.py
"""
import math
import os

from dotenv import load_dotenv

load_dotenv()

# Muss vor dem pyspark-Import gesetzt sein (Spark startet die JVM beim ersten
# SparkSession-Aufruf mit dem dann aktuellen JAVA_HOME). Ohne JDK 17 scheitert
# Spark 3.5.x auf JDK >= 23/24 an einer entfernten Hadoop-API (Stolperstein 1).
JAVA_HOME = os.getenv("JAVA_HOME_JDK17")
if JAVA_HOME and os.path.isdir(JAVA_HOME):
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from db import get_connection
from etl_state import ensure_state_table, get_latest_state, record_state

DATABASE = os.getenv("DATABASE", "gruppe3")
JDBC_JAR_PATH = os.path.join(os.path.dirname(__file__), "utils", "ImpalaJDBC42.jar")
JDBC_DRIVER_CLASS = "com.cloudera.impala.jdbc.Driver"

AUDIT_SOURCE_TABLES = ["bauland", "bevoelkerungzahlen", "gemeinden", "klimadaten"]
TRUTHY_VALUES = {"1", "true", "yes", "y", "ja"}


def safe_div(numerator, denominator):
    """Input: zwei Spark-Columns. Output: Column - NULL statt Infinity/NaN bei Division durch 0."""
    return F.when((denominator == 0) | denominator.isNull(), None).otherwise(
        numerator / denominator
    )


def get_spark():
    """Output: SparkSession (local, JDBC-Treiber via extraClassPath, an 127.0.0.1 gebunden)."""
    return (
        SparkSession.builder
        .appName("gruppe3_pipeline_audit_to_target")
        .master("local[*]")
        .config("spark.driver.extraClassPath", JDBC_JAR_PATH)
        .config("spark.executor.extraClassPath", JDBC_JAR_PATH)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def jdbc_url():
    """Input: IMPALA_* Env-Vars. Output: JDBC-Connection-String."""
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
    """Input: table_name. Output: Spark DataFrame (JDBC-Read aus Impala)."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", table_name)
        .option("driver", JDBC_DRIVER_CLASS)
        .load()
    )


def read_gemeinden(spark):
    """Output: DataFrame aus gruppe3_audit_gemeinden. area_km2/per_km2 werden
    als STRING gelesen und area_km2 zu double gecastet (Treiber wirft sonst
    einen Konvertierungsfehler auf diesen Spalten)."""
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
    """Input: table_name. Output: leert die Tabelle per impyla TRUNCATE
    (Spark erkennt den Impala-JDBC-Dialekt nicht, s. Modul-Docstring)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    cur.execute(f"TRUNCATE TABLE {table_name}")
    cur.close()
    conn.close()


def _sql_literal(value):
    """Input: Python-/Spark-Wert. Output: SQL-Literal fuer INSERT ... VALUES."""
    if value is None:
        return "NULL"
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "NULL"
        return repr(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int,)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def overwrite_table(df, table_name, batch_size=500):
    """Input: DataFrame, table_name. Output: keins - truncated die Zieltabelle
    und schreibt die Zeilen per impyla INSERT-Batches (df.write.jdbc()
    scheitert am Treiber bei NULL-Parametern, s. ADR/Stolpersteine)."""
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


# --- Dimensionen ---

BUNDESLAND_NAMEN = {
    "01": "Schleswig-Holstein", "02": "Hamburg", "03": "Niedersachsen",
    "04": "Bremen", "05": "Nordrhein-Westfalen", "06": "Hessen",
    "07": "Rheinland-Pfalz", "08": "Baden-Wuerttemberg", "09": "Bayern",
    "10": "Saarland", "11": "Berlin", "12": "Brandenburg",
    "13": "Mecklenburg-Vorpommern", "14": "Sachsen", "15": "Sachsen-Anhalt",
    "16": "Thueringen",
}


def build_dim_kreis(spark):
    """Input: gruppe3_audit_bevoelkerungzahlen. Output: DataFrame dim_kreis."""
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
    """Input: gruppe3_audit_bauland (Jahre) + fester Bereich 1995-2024.
    Output: DataFrame dim_jahr."""
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
    """Input: gruppe3_audit_klimadaten. Output: DataFrame dim_klimastadt."""
    klima = read_table(spark, "gruppe3_audit_klimadaten")
    return klima.select(
        F.col("city").alias("stadt_name"),
        F.regexp_replace(F.col("latitude"), ",", ".").cast("double").alias("latitude"),
        F.regexp_replace(F.col("longitude"), ",", ".").cast("double").alias("longitude"),
    ).distinct()


def build_dim_gemeinde(spark, dim_kreis):
    """Input: gruppe3_audit_gemeinden, dim_kreis. Output: DataFrame dim_gemeinde -
    Kreis-Zuordnung per Fuzzy-Match (kreis_name enthaelt district_kreis),
    bei Mehrdeutigkeit gewinnt der laengste/spezifischste Kreisname."""
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
        F.regexp_replace(F.col("latitude"), ",", ".").cast("double").alias("latitude"),
        F.regexp_replace(F.col("longitude"), ",", ".").cast("double").alias("longitude"),
    )


# --- Basis-Fakten ---

def build_fact_bevoelkerung(spark):
    """Input: gruppe3_audit_bevoelkerungzahlen (92 Spalten, 1 Zeile/Kreis).
    Output: DataFrame fact_bevoelkerung (1 Zeile/Kreis+Jahr, per explode/array unpivotiert)."""
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
    """Input: gruppe3_audit_bauland (4 Merkmale je Kreis+Jahr, lange Form).
    Output: DataFrame fact_bauland (pivotiert: je 1 Spalte pro Merkmal)."""
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
    """Input: gruppe3_audit_klimadaten. Output: DataFrame fact_klima
    (Jahresmittel + Abweichung vom Referenzmittel 1961-1990)."""
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
    """Input: gruppe3_audit_gemeinden, dim_gemeinde. Output: DataFrame fact_gemeinde_stamm.
    per_km2 wird nicht aus der Quelle gelesen (Treiber-Konvertierungsfehler),
    sondern selbst berechnet (population_total / area_km2)."""
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
        F.round(safe_div(F.col("population_total"), F.col("area_km2")), 2).alias("einwohner_pro_km2"),
    )


# --- Cross-Table-KPI-Fakt ---

def build_fact_standortprofil_kpi(spark, dim_gemeinde, fact_bevoelkerung, fact_bauland, fact_klima, fact_gemeinde_stamm):
    """Input: dim_gemeinde + alle Basis-Fakten. Output: DataFrame fact_standortprofil_kpi
    (6 KPIs je Kreis+Jahr). Klima wird ueber dim_gemeinde per Namens-Match auf
    Kreis-Ebene aggregiert (Gemeindename == Klimastadtname, beide transliteriert);
    fehlender Klimawert zaehlt neutral (0) statt den ganzen Score zu NULLen.
    standortattraktivitaets_score ist ein z-Score (AVG/STDDEV OVER PARTITION BY jahr).
    Zeilen ohne jeden KPI-Wert werden verworfen."""
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

    KPI_SPALTEN = [
        "wohnraumdruck_index", "baulandpreis_pro_kopf_eur", "freiflaeche_pro_einwohner_qm",
        "klima_angepasstes_wohnraumrisiko", "verstaedterung_index", "standortattraktivitaets_score",
    ]
    return result.filter(F.coalesce(*KPI_SPALTEN).isNotNull())


# --- Incremental Loading Stufe 3 ---
# Star-Schema bleibt Full Reload (Window-Funktionen brauchen ohnehin jede
# Zeile der Partition, Zieltabellen sind klein). Einzige Optimierung: der
# komplette Spark-Lauf wird uebersprungen, wenn seit dem letzten Ziel-Build
# keine Audit-Tabelle neuer ist (s. should_skip_target_build).

def should_skip_target_build(cur):
    """Input: Cursor. Output: bool - True, wenn keine der vier Audit-Tabellen
    neuer ist als der letzte Ziel-Build (record_state stage="target")."""
    target_state = get_latest_state(cur, "target", "all")
    if target_state is None:
        return False

    for name in AUDIT_SOURCE_TABLES:
        audit_state = get_latest_state(cur, "audit", name)
        if audit_state is None or audit_state["recorded_at"] > target_state["recorded_at"]:
            return False

    return True


def should_force_target_build():
    """Input: FORCE_TARGET_BUILD Env-Var. Output: bool."""
    return os.getenv("FORCE_TARGET_BUILD", "").strip().lower() in TRUTHY_VALUES


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
