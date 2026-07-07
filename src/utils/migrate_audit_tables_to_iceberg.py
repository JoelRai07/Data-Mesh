"""
Einmalige, manuell auszufuehrende Migration der zeilengenau gemergten
Audit-Tabellen (gruppe3_audit_bauland, gruppe3_audit_bevoelkerungzahlen) von
Parquet zu Apache Iceberg - Voraussetzung fuer das MERGE INTO/DELETE in
audit_table_keyed_snapshot() (s. src/pipeline_staging_to_audit.py, ADR.md).

WARUM EIN SEPARATES SKRIPT STATT EINER AUTOMATISCHEN MIGRATION MITTEN IN DER
TAEGLICHEN PIPELINE?
  Eine fruehere Version von pipeline_staging_to_audit.py migrierte lautlos bei
  jedem Lauf mit ("ensure_iceberg_audit_table" hat automatisch erkannt +
  migriert). Das laesst zwei Probleme entstehen: (1) die Pruefung "ist das
  schon Iceberg?" lebt fuer immer im taeglichen Hot Path, obwohl sie nur beim
  allerersten Lauf je etwas tut, und (2) ein Abbruch mitten in der Migration
  (CTAS ueber die komplette Tabelle) waere in einem automatischen,
  unbeaufsichtigten Lauf schwerer zu diagnostizieren als in einem separaten,
  bewusst gestarteten Schritt mit sichtbarer Fortschrittsausgabe - passend
  zum Muster von reset_database.py (strukturell heikle Operationen bekommen
  ein eigenes, explizites Skript statt in der Pipeline "nebenbei" zu laufen).
  pipeline_staging_to_audit.py selbst bricht jetzt mit einer klaren
  Fehlermeldung ab, wenn eine Audit-Tabelle noch Parquet ist, und verweist auf
  dieses Skript.

VORGEHEN JE TABELLE (verlustfrei, mit Verifikation):
  1) Existiert die Tabelle noch nicht -> nichts zu tun, der naechste
     Pipeline-Lauf legt sie direkt als Iceberg an.
  2) Ist sie bereits Iceberg -> nichts zu tun (idempotent, mehrfach
     ausfuehrbar).
  3) Ist sie noch Parquet -> Zeilenzahl vorher merken, per
     CREATE TABLE ... STORED BY ICEBERG AS SELECT in eine neue Tabelle
     kopieren, Zeilenzahl der Kopie mit der Original-Zeilenzahl vergleichen
     (Abbruch bei Abweichung, VOR dem Tausch - das Original bleibt
     unangetastet), danach per RENAME+DROP tauschen (zuerst das alte
     Original umbenennen statt loeschen, erst danach die neue Tabelle
     einsetzen, zuletzt erst die Kopie loeschen - zu keinem Zeitpunkt sind
     die Daten weder in der alten noch in der neuen Tabelle verloren).

Ausfuehren:  .venv/Scripts/python.exe src/utils/migrate_audit_tables_to_iceberg.py
"""
import os
import sys

# db.py liegt eine Ebene hoeher (in src/), s. gleiches Muster in reset_database.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

# Nur die Tabellen, die audit_table_keyed_snapshot() per MERGE INTO/DELETE
# pflegt (s. KEY_COLUMNS in pipeline_staging_to_audit.py) - gemeinden/
# klimadaten bleiben bewusst Parquet (s. ADR.md).
AUDIT_TABLES_TO_MIGRATE = [PREFIX + "audit_bauland", PREFIX + "audit_bevoelkerungzahlen"]


def table_exists(cur, table_name):
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    return len(cur.fetchall()) > 0


def is_iceberg_table(cur, table_name):
    """Gleiche Pruefung wie _is_iceberg_table() in pipeline_staging_to_audit.py:
    gezielt die Table-Parameter-Zeile 'table_type' = 'ICEBERG' aus
    DESCRIBE FORMATTED lesen (Impala kennt kein SHOW TBLPROPERTIES)."""
    cur.execute(f"DESCRIBE FORMATTED {table_name}")
    for row in cur.fetchall():
        key = (row[1] or "").strip() if len(row) > 1 else ""
        if key == "table_type":
            value = (row[2] or "").strip() if len(row) > 2 else ""
            return value.upper() == "ICEBERG"
    return False


def row_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def migrate_table(cur, table_name):
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
        # Original ist zu diesem Zeitpunkt noch unangetastet - kein Tausch,
        # nur die fehlgeschlagene Kopie aufraeumen und laut abbrechen.
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
