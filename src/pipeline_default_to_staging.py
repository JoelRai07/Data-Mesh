"""
WAP-PATTERN (Write-Audit-Publish), STUFE 1: source system -> staging table.

Kopiert die 4 Rohtabellen aus dem Source System (Datenbank "default") in
Staging-Tabellen der eigenen Datenbank "gruppe3". Gleiches Schema, gleiche
Daten, keine Transformation - das ist bewusst so, denn Audit/Bereinigung ist
laut WAP-Pattern ein SPAETERER, eigener Schritt (staging -> audit -> publish),
nicht Teil dieser Pipeline.

Quelltabellen (Source System):
  default.project_bauland, default.project_bevoelkerungzahlen,
  default.project_gemeinden, default.project_klimadaten

Staging-Tabellen (Ziel, Datenbank gruppe3, ALLE als Apache Iceberg):
  gruppe3_staging_bauland, gruppe3_staging_bevoelkerungzahlen,
  gruppe3_staging_gemeinden, gruppe3_staging_klimadaten

INCREMENTAL LOADING - ZWEI TABELLENARTEN, ZWEI STRATEGIEN (s. gruppe3_etl_state,
src/etl_state.py; die Einteilung wurde direkt an den Quelltabellen verifiziert,
s. Schema-/Key-Analyse in ADR.md):

  1) project_klimadaten (WATERMARK_COLUMNS): echte Zeitreihe (8,6 Mio. Zeilen,
     eine Messung je Stadt+Monat, dt von 1743-11-01 bis 2013-09-01), Spalte
     "dt" ist ein ISO-Datumsstring ("YYYY-MM-DD") und waechst nur durch NEUE
     Messtage - historische Wetterwerte werden nicht nachtraeglich korrigiert.
     Dafuer eignet sich ein echtes Wasserzeichen: ab dem zweiten Lauf wird nur
     noch "WHERE dt > letztes_wasserzeichen" per INSERT INTO (APPEND) geladen,
     nie mehr die ganze Tabelle ueberschrieben. Das ist der eigentliche
     Performance-Gewinn dieser Pipeline: kein taeglicher Full Scan +
     Full Rewrite von 8,6 Mio. Zeilen mehr, wenn nur ein paar tausend neue
     dazukommen.

  2) project_bauland, project_bevoelkerungzahlen, project_gemeinden
     (alles, was NICHT in WATERMARK_COLUMNS steht): amtliche Statistiken bzw.
     Stammdaten-Snapshots. project_bauland hat zwar eine "jahr"-Spalte, aber
     amtliche Statistiken werden manchmal nachtraeglich fuer bereits
     gemeldete Jahre revidiert (Destatis-Praxis) - ein reines
     "jahr > Wasserzeichen"-Append wuerde solche Korrekturen an bereits
     geladenen Jahren stillschweigend verpassen. project_bevoelkerungzahlen
     (1 Zeile/Kreis, Jahre als 92 Spalten) und project_gemeinden (Stammdaten,
     kein Zeitbezug) haben ueberhaupt keine Zeilen-Ebene, auf der "neu vs.
     alt" definierbar waere. Fuer alle drei gibt es also KEINEN verlaesslichen
     Aenderungsindikator auf Zeilenebene. Pragmatische, ehrliche Loesung:
     Change Detection auf Tabellenebene per Inhalts-Pruefsumme
     (content_signature(), s. etl_state.py) - hat sich der Tabelleninhalt seit
     dem letzten Lauf NICHT veraendert, wird der Full Load uebersprungen
     (kein unnoetiges INSERT OVERWRITE); hat er sich veraendert, wird -
     mangels granularerer Information in der QUELLE - die ganze Staging-
     Tabelle per INSERT OVERWRITE ersetzt (Full Refresh), aber eben nur dann.
     Das ZEILENGENAUE Incremental Merge fuer bauland/bevoelkerungzahlen
     passiert eine Stufe spaeter (staging -> audit, s.
     pipeline_staging_to_audit.py), wo der Business-Key ausgewertet wird.

WAS APACHE ICEBERG HIER KONKRET BEITRAEGT:
  - INSERT OVERWRITE ist auf Iceberg ein ATOMARER Snapshot-Commit: ein
    Leser (z.B. ein parallel laufender Audit-Schritt) sieht entweder den
    kompletten alten oder den kompletten neuen Stand, nie eine halb
    geleerte Tabelle - genau die Race-Condition, die Parquet-Tabellen auf
    HDFS bei OVERWRITE haben.
  - gruppe3_staging_klimadaten ist per Iceberg-Partition-Transform
    "PARTITIONED BY SPEC (TRUNCATE(4, dt))" nach Jahres-Praefix des
    ISO-Datums partitioniert (hidden partitioning: weder INSERT noch
    SELECT muessen die Partition kennen). Der Wasserzeichen-Filter
    "dt > '...'" liest damit nur die betroffenen Jahres-Partitionen statt
    8,6 Mio. Zeilen zu scannen.
  - Jeder Ladevorgang ist ein Iceberg-Snapshot: "wie sah das Staging vor
    dem letzten Lauf aus" beantwortet Time Travel
    (SELECT ... FOR SYSTEM_TIME AS OF ...) bzw. DESCRIBE HISTORY - ohne
    eigene Historisierungs-Tabellen.

WARUM REINES IMPALA-SQL STATT SPARK (anders als pipeline_audit_to_target.py)?
  Diese Pipeline transformiert nichts - reines Kopieren (bzw. Filtern nach
  Wasserzeichen). "INSERT OVERWRITE"/"INSERT INTO ... SELECT ... WHERE ..."
  laeuft komplett serverseitig in Impala, ganz ohne Daten durch Python/Spark
  zu schleusen - fuer einen reinen Kopiervorgang der richtige Weg (erst recht
  bei 8,6 Mio. Zeilen in project_klimadaten).

WARUM CTAS "WHERE 1=0" STATT MANUELLER SPALTENLISTE ODER "CREATE ... LIKE"?
  default.project_bevoelkerungzahlen hat 92 Spalten (id, kreis, + 3 Spalten
  x 30 Jahre) - von Hand abzutippen waere fehleranfaellig. "CREATE TABLE ...
  LIKE ... STORED BY ICEBERG" schlaegt fehl, wenn die Quelle kein Iceberg ist
  ("cannot be cloned into an Iceberg table") - CTAS mit WHERE 1=0 uebernimmt
  Spaltennamen/-typen 1:1, aber keine Zeilen (s. ensure_iceberg_table_like
  in etl_state.py).

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_default_to_staging.py
"""
import os

from db import get_connection
from etl_state import (
    ensure_state_table,
    ensure_iceberg_table_like,
    get_latest_state,
    record_state,
    get_columns,
    content_signature,
)

SOURCE_DATABASE = os.getenv("SOURCE_DATABASE", "default")
DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

# source_table -> staging_table
TABLES = {
    "project_bauland": PREFIX + "staging_bauland",
    "project_bevoelkerungzahlen": PREFIX + "staging_bevoelkerungzahlen",
    "project_gemeinden": PREFIX + "staging_gemeinden",
    "project_klimadaten": PREFIX + "staging_klimadaten",
}

# Nur echte, verlaesslich anhaengende Zeitreihen bekommen ein Wasserzeichen
# (s. Modul-Docstring, Punkt 1). Alles andere laeuft ueber Change Detection
# per Inhalts-Pruefsumme (Punkt 2).
WATERMARK_COLUMNS = {
    "project_klimadaten": "dt",
}

# Iceberg-Partition-Transforms je Quelltabelle (s. Modul-Docstring, Punkt
# "WAS APACHE ICEBERG HIER KONKRET BEITRAEGT"). TRUNCATE(4, dt) = die ersten
# 4 Zeichen des ISO-Datums-Strings, also das Jahr - passend zum
# Wasserzeichen-Zugriffsmuster "dt > '...'".
ICEBERG_PARTITION_SPECS = {
    "project_klimadaten": "PARTITIONED BY SPEC (TRUNCATE(4, dt))",
}


def stage_table_incremental(cur, source_table, staging_table, watermark_column):
    """
    Echtes Incremental Load per Wasserzeichen: laedt beim ersten Lauf einmalig
    den kompletten Bestand (INSERT OVERWRITE), danach bei jedem weiteren Lauf
    nur noch Zeilen mit watermark_column > letztem Wasserzeichen (INSERT INTO,
    also ANHAENGEN statt ueberschreiben). Gibt nichts an Spark/Python zurueck -
    laeuft komplett serverseitig in Impala; die Jahres-Partitionierung der
    Iceberg-Tabelle sorgt dafuer, dass dabei nur die betroffenen Partitionen
    angefasst werden.
    """
    source_fqn = f"{SOURCE_DATABASE}.{source_table}"
    state = get_latest_state(cur, "staging", source_table)
    watermark = state["watermark_value"] if state else None

    if watermark is None:
        cur.execute(f"INSERT OVERWRITE TABLE {staging_table} SELECT * FROM {source_fqn}")
        changed = True
    else:
        cur.execute(
            f"SELECT MAX({watermark_column}) FROM {source_fqn} "
            f"WHERE {watermark_column} > '{watermark}'"
        )
        pending_max = cur.fetchone()[0]
        if pending_max is None:
            changed = False
        else:
            cur.execute(
                f"INSERT INTO {staging_table} SELECT * FROM {source_fqn} "
                f"WHERE {watermark_column} > '{watermark}'"
            )
            changed = True

    # Wasserzeichen NUR bei tatsaechlicher Verarbeitung fortschreiben:
    # record_state ist ein Upsert mit frischem recorded_at (s. etl_state.py) -
    # ein Update pro unveraendertem Lauf wuerde "zuletzt verarbeitet"
    # verfaelschen (should_skip_target_build in Stufe 3 vergleicht diese
    # Zeitstempel). Bei changed=False bleibt der bisherige Eintrag korrekt
    # der aktuelle Stand.
    if changed:
        cur.execute(f"SELECT MAX({watermark_column}) FROM {source_fqn}")
        new_watermark = cur.fetchone()[0]
        if new_watermark is not None:
            record_state(cur, "staging", source_table, watermark_value=new_watermark)

    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    return cur.fetchone()[0], changed


def stage_table_snapshot(cur, source_table, staging_table):
    """
    Snapshot-Tabelle ohne verlaesslichen Aenderungsindikator (s. Modul-
    Docstring, Punkt 2): Inhalts-Pruefsumme der Quelle bilden, mit dem
    zuletzt aufgezeichneten Stand vergleichen. Unveraendert -> Full Load
    ueberspringen. Veraendert -> Full Refresh per INSERT OVERWRITE - auf
    Iceberg ein einzelner atomarer Snapshot-Commit (Leser sehen nie einen
    halb geladenen Zwischenstand).
    """
    source_fqn = f"{SOURCE_DATABASE}.{source_table}"
    columns = get_columns(cur, source_fqn)
    row_count_source, content_hash = content_signature(cur, source_fqn, columns)

    state = get_latest_state(cur, "staging", source_table)
    if state and state["content_hash"] == content_hash:
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        return cur.fetchone()[0], False

    cur.execute(f"INSERT OVERWRITE TABLE {staging_table} SELECT * FROM {source_fqn}")
    record_state(cur, "staging", source_table, content_hash=content_hash, row_count=row_count_source)

    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    return cur.fetchone()[0], True


def stage_table(cur, source_table, staging_table):
    """
    Fuehrt fuer eine Quelltabelle den kompletten Write-Schritt aus:
    1) Staging-Tabelle als Iceberg anlegen, falls sie noch nicht existiert
       (Schema 1:1 von der Quelle per CTAS WHERE 1=0; klimadaten zusaetzlich
       nach Jahr partitioniert, s. ICEBERG_PARTITION_SPECS).
    2) Inhalt inkrementell aktualisieren - Strategie haengt von der
       Tabellenart ab (s. Modul-Docstring): Wasserzeichen-Append fuer
       project_klimadaten, Change-Detection + bedingter Full Refresh fuer
       den Rest.

    Gibt (row_count, changed) zurueck - changed=False bedeutet "unveraendert,
    Lauf uebersprungen" (fuer die Log-Ausgabe in main()).
    """
    source_fqn = f"{SOURCE_DATABASE}.{source_table}"
    ensure_iceberg_table_like(
        cur, staging_table, source_fqn,
        partition_spec=ICEBERG_PARTITION_SPECS.get(source_table),
    )

    if source_table in WATERMARK_COLUMNS:
        return stage_table_incremental(cur, source_table, staging_table, WATERMARK_COLUMNS[source_table])
    return stage_table_snapshot(cur, source_table, staging_table)


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    ensure_state_table(cur)
    print(f"Datenbank: {DATABASE}\n")

    for source_table, staging_table in TABLES.items():
        print(f"Staging {SOURCE_DATABASE}.{source_table} -> {staging_table} ...")
        row_count, changed = stage_table(cur, source_table, staging_table)
        if changed:
            print(f"  -> OK ({row_count} Zeilen, aktualisiert)")
        else:
            print(f"  -> OK ({row_count} Zeilen, unveraendert - Lauf uebersprungen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
