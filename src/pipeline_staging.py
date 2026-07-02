"""
WAP-PATTERN (Write-Audit-Publish), STUFE 1: source system -> staging table.

Kopiert die 4 Rohtabellen aus dem Source System (Datenbank "default") 1:1,
unbereinigt, in Staging-Tabellen der eigenen Datenbank "gruppe3". Gleiches
Schema, gleiche Daten, keine Transformation - das ist bewusst so, denn Audit/
Bereinigung ist laut WAP-Pattern ein SPAETERER, eigener Schritt (staging ->
audit -> publish), nicht Teil dieser Pipeline.

Quelltabellen (Source System):
  default.project_bauland, default.project_bevoelkerungzahlen,
  default.project_gemeinden, default.project_klimadaten

Staging-Tabellen (Ziel, Datenbank gruppe3):
  gruppe3_staging_bauland, gruppe3_staging_bevoelkerungzahlen,
  gruppe3_staging_gemeinden, gruppe3_staging_klimadaten

WARUM REINES IMPALA-SQL STATT SPARK (anders als pipeline_spark.py)?
  Diese Pipeline transformiert nichts - reines Kopieren. pipeline_spark.py
  schreibt Ergebnisse per collect() + impyla-INSERT-Batches zurueck (noetig,
  weil der Impala-JDBC-Treiber bei Spark-JDBC-Batch-Inserts mit NULL-Werten
  den SQL-Typ nicht bestimmen kann, s. Docstring dort). Das waere hier aber
  unbrauchbar: default.project_klimadaten hat 8.6 Mio. Zeilen, bei 500
  Zeilen/Batch also ~17.000 einzelne Impala-Queries ueber das Netz - viel zu
  langsam. Ein simples "INSERT OVERWRITE TABLE ... SELECT * FROM ..." laeuft
  dagegen komplett serverseitig in Impala, ganz ohne Daten durch Python/Spark
  zu schleusen - fuer einen reinen 1:1-Kopiervorgang der richtige Weg.

WARUM "CREATE TABLE ... LIKE ..." STATT MANUELLER SPALTENLISTE?
  default.project_bevoelkerungzahlen hat 83 Spalten (id, kreis, + 3 Spalten
  x 30 Jahre) - von Hand abzutippen waere fehleranfaellig. LIKE uebernimmt
  Spaltennamen/-typen 1:1 von der Quelltabelle, garantiert also exakt
  dasselbe Schema.

IMMER OVERWRITE, NIE DUPLIZIEREN:
  INSERT OVERWRITE TABLE (nicht INSERT INTO) ersetzt bei jedem Lauf den
  kompletten Tabelleninhalt - mehrfaches Ausfuehren erzeugt keine Duplikate
  (Full-Load-Pattern, analog zu overwrite_table() in pipeline_spark.py).

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_staging.py
"""
from db import get_connection

SOURCE_DATABASE = "default"
DATABASE = "gruppe3"
PREFIX = "gruppe3_"

# source_table -> staging_table
TABLES = {
    "project_bauland": PREFIX + "staging_bauland",
    "project_bevoelkerungzahlen": PREFIX + "staging_bevoelkerungzahlen",
    "project_gemeinden": PREFIX + "staging_gemeinden",
    "project_klimadaten": PREFIX + "staging_klimadaten",
}


def stage_table(cur, source_table, staging_table):
    """
    Fuehrt fuer eine Quelltabelle den kompletten Write-Schritt aus:
    1) Staging-Tabelle anlegen, falls sie noch nicht existiert (Schema 1:1
       von der Quelle uebernommen per LIKE).
    2) Inhalt komplett per INSERT OVERWRITE ersetzen (Full Load, idempotent).
    """
    source_fqn = f"{SOURCE_DATABASE}.{source_table}"

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {staging_table} LIKE {source_fqn} STORED AS PARQUET"
    )
    cur.execute(f"INSERT OVERWRITE TABLE {staging_table} SELECT * FROM {source_fqn}")

    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    return cur.fetchone()[0]


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    print(f"Datenbank: {DATABASE}\n")

    for source_table, staging_table in TABLES.items():
        print(f"Staging {SOURCE_DATABASE}.{source_table} -> {staging_table} ...")
        row_count = stage_table(cur, source_table, staging_table)
        print(f"  -> OK ({row_count} Zeilen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
