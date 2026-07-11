"""
WAP Stufe 1: source system -> staging table.

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
    table_fingerprint,
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

    # Ebene 1 (s. etl_state.py): Ganztabellen-Fingerprint der Quelle - reine
    # Metadaten-Abfrage. Unveraendert -> weder MAX()-Scan noch INSERT noetig.
    # Nur fuer Iceberg-Quellen verfuegbar; sonst None -> Wasserzeichen-Logik.
    source_fp = table_fingerprint(cur, source_fqn)
    if (
        source_fp is not None
        and state is not None
        and state["table_fingerprint"] == source_fp
    ):
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        return cur.fetchone()[0], False

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
    # verfaelschen. Bei changed=False bleibt der bisherige Eintrag bestehen;
    # nur ein gewechselter Fingerprint OHNE neue Zeilen (z.B. Compaction der
    # Quelle) wird fortgeschrieben, damit Ebene 1 beim naechsten Lauf wieder
    # greift (Wasserzeichen bleibt dabei erhalten).
    if changed:
        cur.execute(f"SELECT MAX({watermark_column}) FROM {source_fqn}")
        new_watermark = cur.fetchone()[0]
        if new_watermark is not None:
            record_state(
                cur, "staging", source_table,
                watermark_value=new_watermark, table_fingerprint=source_fp,
            )
    elif source_fp is not None and state is not None and state["table_fingerprint"] != source_fp:
        record_state(
            cur, "staging", source_table,
            watermark_value=watermark, table_fingerprint=source_fp,
        )

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
    state = get_latest_state(cur, "staging", source_table)

    # Ebene 1 (s. etl_state.py): Ganztabellen-Fingerprint der Quelle - reine
    # Metadaten-Abfrage statt des vollen Pruefsummen-Scans. Nur bei
    # Fingerprint-Wechsel (oder Nicht-Iceberg-Quelle -> None) geht es
    # runter auf Ebene 2 (content_signature ueber alle Zeilen).
    source_fp = table_fingerprint(cur, source_fqn)
    if (
        source_fp is not None
        and state is not None
        and state["table_fingerprint"] == source_fp
    ):
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        return cur.fetchone()[0], False

    columns = get_columns(cur, source_fqn)
    row_count_source, content_hash = content_signature(cur, source_fqn, columns)

    if state and state["content_hash"] == content_hash:
        # Inhaltlich unveraendert, nur der Fingerprint ist gewechselt (z.B.
        # Compaction der Quelle) -> Fingerprint fortschreiben, damit Ebene 1
        # beim naechsten Lauf wieder ohne Zeilen-Scan greift.
        if source_fp is not None and state["table_fingerprint"] != source_fp:
            record_state(
                cur, "staging", source_table,
                content_hash=content_hash, row_count=state["row_count"],
                table_fingerprint=source_fp,
            )
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        return cur.fetchone()[0], False

    cur.execute(f"INSERT OVERWRITE TABLE {staging_table} SELECT * FROM {source_fqn}")
    record_state(
        cur, "staging", source_table,
        content_hash=content_hash, row_count=row_count_source,
        table_fingerprint=source_fp,
    )

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
