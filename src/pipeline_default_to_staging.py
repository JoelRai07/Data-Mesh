"""
WAP Stufe 1: source system -> staging table.

"""
import os

from db import get_connection
from etl_state import (
    ensure_state_table,
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

WATERMARK_COLUMNS = {
    "project_klimadaten": "dt",
}


def stage_table_incremental(cur, source_table, staging_table, watermark_column):
    """Input: source_table, watermark_column. Output: (row_count, changed) -
    Full Load beim ersten Lauf, danach nur dt > letztes Wasserzeichen (APPEND)."""
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

    if changed:
        cur.execute(f"SELECT MAX({watermark_column}) FROM {source_fqn}")
        new_watermark = cur.fetchone()[0]
        if new_watermark is not None:
            record_state(cur, "staging", source_table, watermark_value=new_watermark)

    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    return cur.fetchone()[0], changed


def stage_table_snapshot(cur, source_table, staging_table):
    """Input: source_table. Output: (row_count, changed) - Inhalts-Pruefsumme
    vergleichen, bei Aenderung Full Refresh (INSERT OVERWRITE)."""
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
    """Input: source_table, staging_table. Output: (row_count, changed) -
    legt staging_table an (falls fehlend) und laedt sie gemaess Strategie."""
    source_fqn = f"{SOURCE_DATABASE}.{source_table}"
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {staging_table} LIKE {source_fqn} STORED AS PARQUET"
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
