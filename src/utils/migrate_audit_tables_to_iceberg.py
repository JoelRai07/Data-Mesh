"""
Einmalige Migration von gruppe3_audit_bauland/audit_bevoelkerungzahlen von
Parquet zu Iceberg (Voraussetzung fuer MERGE INTO/DELETE, s. ADR.md).
Verlustfrei per CTAS + Zeilenzahl-Check + RENAME-Swap; idempotent.

"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

AUDIT_TABLES_TO_MIGRATE = [PREFIX + "audit_bauland", PREFIX + "audit_bevoelkerungzahlen"]


def table_exists(cur, table_name):
    """Input: table_name. Output: bool."""
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    return len(cur.fetchall()) > 0


def is_iceberg_table(cur, table_name):
    """Input: table_name. Output: bool - liest table_type aus DESCRIBE FORMATTED."""
    cur.execute(f"DESCRIBE FORMATTED {table_name}")
    for row in cur.fetchall():
        key = (row[1] or "").strip() if len(row) > 1 else ""
        if key == "table_type":
            value = (row[2] or "").strip() if len(row) > 2 else ""
            return value.upper() == "ICEBERG"
    return False


def row_count(cur, table_name):
    """Input: table_name. Output: Zeilenzahl."""
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def migrate_table(cur, table_name):
    """Input: table_name. Output: keins - migriert Parquet->Iceberg (No-Op,
    falls fehlend oder bereits Iceberg); RuntimeError bei Zeilenzahl-Abweichung."""
    if not table_exists(cur, table_name):
        print(f"  {table_name}: existiert noch nicht - nichts zu migrieren "
              "(naechster Pipeline-Lauf legt sie direkt als Iceberg an).")
        return

    if is_iceberg_table(cur, table_name):
        print(f"  {table_name}: bereits Iceberg - nichts zu tun.")
        return

    before_count = row_count(cur, table_name)
    print(f"  {table_name}: ist noch Parquet ({before_count} Zeilen) - migriere ...")

    migrated_table = f"{table_name}_iceberg"
    old_table = f"{table_name}_pre_iceberg"

    cur.execute(f"DROP TABLE IF EXISTS {migrated_table}")
    cur.execute(
        f"CREATE TABLE {migrated_table} STORED BY ICEBERG TBLPROPERTIES('format-version'='2') "
        f"AS SELECT * FROM {table_name}"
    )

    after_count = row_count(cur, migrated_table)
    if after_count != before_count:
        # Original noch unangetastet: nur die Kopie aufraeumen, dann abbrechen.
        cur.execute(f"DROP TABLE IF EXISTS {migrated_table}")
        raise RuntimeError(
            f"Migration von {table_name} abgebrochen: Kopie hat {after_count} "
            f"statt {before_count} Zeilen. Original wurde NICHT veraendert."
        )

    cur.execute(f"DROP TABLE IF EXISTS {old_table}")
    cur.execute(f"ALTER TABLE {table_name} RENAME TO {old_table}")
    cur.execute(f"ALTER TABLE {migrated_table} RENAME TO {table_name}")
    cur.execute(f"DROP TABLE IF EXISTS {old_table}")

    print(f"  {table_name}: migriert - {after_count} Zeilen verifiziert, jetzt Iceberg.")


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    print(f"Datenbank: {DATABASE}\n")

    for table_name in AUDIT_TABLES_TO_MIGRATE:
        migrate_table(cur, table_name)

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
