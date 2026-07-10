"""
Einmalige, manuell auszufuehrende Migration ALLER Pipeline-Tabellen von
Parquet zu Apache Iceberg - Voraussetzung fuer die komplett auf Iceberg
umgestellte Pipeline (MERGE INTO/DELETE in Stufe 2, atomare INSERT
OVERWRITE-Snapshots in allen Stufen, Upsert-State, Time Travel; s. ADR.md).

Ersetzt das fruehere src/utils/migrate_audit_tables_to_iceberg.py, das nur
gruppe3_audit_bauland/gruppe3_audit_bevoelkerungzahlen migrierte.

WARUM EIN SEPARATES SKRIPT STATT EINER AUTOMATISCHEN MIGRATION MITTEN IN DER
TAEGLICHEN PIPELINE?
  Eine Storage-Format-Migration ist ein einmaliger, strukturell heikler
  Vorgang (CTAS ueber komplette Tabellen + Rename-Swap). Das gehoert NICHT
  in den taeglichen Hot Path: die Pruefung liefe fuer immer mit, obwohl sie
  nur beim allerersten Lauf etwas tut, und ein Abbruch mitten in der
  Migration waere in einem unbeaufsichtigten Lauf schwerer zu diagnostizieren
  als in einem separaten, bewusst gestarteten Schritt mit sichtbarer
  Fortschrittsausgabe (gleiches Muster wie reset_database.py). Die
  Pipeline-Stufen brechen mit einer klaren Fehlermeldung ab, wenn eine
  Tabelle noch Parquet ist, und verweisen auf dieses Skript.

VORGEHEN JE TABELLE (verlustfrei, mit Verifikation):
  1) Existiert die Tabelle noch nicht -> nichts zu tun, der naechste
     Pipeline-Lauf legt sie direkt als Iceberg an.
  2) Ist sie bereits Iceberg -> nichts zu tun (idempotent, mehrfach
     ausfuehrbar).
  3) Ist sie noch Parquet -> Zeilenzahl vorher merken, per
     CREATE TABLE ... STORED BY ICEBERG AS SELECT in eine neue Tabelle
     kopieren (Klimadaten-Tabellen zusaetzlich per Iceberg-Transform
     TRUNCATE(4, dt) nach Jahr partitioniert), Zeilenzahl der Kopie mit der
     Original-Zeilenzahl vergleichen (Abbruch bei Abweichung, VOR dem
     Tausch - das Original bleibt unangetastet), danach per RENAME+DROP
     tauschen (zuerst das alte Original umbenennen statt loeschen, erst
     danach die neue Tabelle einsetzen, zuletzt erst die Kopie loeschen -
     zu keinem Zeitpunkt sind die Daten weder in der alten noch in der
     neuen Tabelle verloren).

SONDERFAELLE:
  - gruppe3_etl_state war frueher APPEND-ONLY (mehrere Zeilen je
    (stage, table_name), die juengste galt). Die neue Iceberg-Fassung ist
    ein UPSERT (genau eine Zeile je Schluessel, s. etl_state.py). Die
    Migration verdichtet die Historie deshalb beim Kopieren auf die jeweils
    juengste Zeile je (stage, table_name) - die Verlaufs-Historie liegt ab
    jetzt in den Iceberg-Snapshots (DESCRIBE HISTORY) statt in eigenen
    Zeilen.
  - gruppe3_etl_row_state / gruppe3_etl_changed_keys_tmp (Zeilen-Hash-
    Historie des frueheren Merge-Verfahrens) werden von der Pipeline nicht
    mehr genutzt (der NULL-sichere Spaltenvergleich passiert jetzt direkt
    im MERGE gegen die Iceberg-Audit-Tabelle, s. ADR.md Iteration 4) und
    hier GELOESCHT.

Ausfuehren:  .venv/Scripts/python.exe src/utils/migrate_to_iceberg.py
"""
import os
import sys

# db.py/etl_state.py liegen eine Ebene hoeher (in src/), s. gleiches Muster
# in reset_database.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection
from etl_state import table_exists, is_iceberg_table, ICEBERG_TBLPROPERTIES

DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

# Alle regulaeren Pipeline-Tabellen: Staging (Stufe 1), Audit (Stufe 2),
# Star-Schema (Stufe 3). table_name -> optionaler Iceberg-Partition-Spec.
THEMEN = ["bauland", "bevoelkerungzahlen", "gemeinden", "klimadaten"]
KLIMA_PARTITION_SPEC = "PARTITIONED BY SPEC (TRUNCATE(4, dt))"

TABLES_TO_MIGRATE = {}
for schicht in ("staging_", "audit_"):
    for thema in THEMEN:
        TABLES_TO_MIGRATE[PREFIX + schicht + thema] = (
            KLIMA_PARTITION_SPEC if thema == "klimadaten" else None
        )
for ziel in (
    "dim_kreis", "dim_jahr", "dim_gemeinde", "dim_klimastadt",
    "fact_bevoelkerung", "fact_bauland", "fact_klima",
    "fact_gemeinde_stamm", "fact_standortprofil_kpi",
):
    TABLES_TO_MIGRATE[PREFIX + ziel] = None

STATE_TABLE = "gruppe3_etl_state"
OBSOLETE_TABLES = ["gruppe3_etl_row_state", "gruppe3_etl_changed_keys_tmp"]


def row_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def _swap(cur, table_name, migrated_table):
    """RENAME+DROP-Tausch: Original erst umbenennen (nicht loeschen), dann
    die Kopie einsetzen, zuletzt das umbenannte Original loeschen."""
    old_table = f"{table_name}_pre_iceberg"
    cur.execute(f"DROP TABLE IF EXISTS {old_table}")
    cur.execute(f"ALTER TABLE {table_name} RENAME TO {old_table}")
    cur.execute(f"ALTER TABLE {migrated_table} RENAME TO {table_name}")
    cur.execute(f"DROP TABLE IF EXISTS {old_table}")


def migrate_table(cur, table_name, partition_spec=None):
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
    spec_sql = f" {partition_spec}" if partition_spec else ""

    cur.execute(f"DROP TABLE IF EXISTS {migrated_table}")
    cur.execute(
        f"CREATE TABLE {migrated_table}{spec_sql} STORED BY ICEBERG {ICEBERG_TBLPROPERTIES} "
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

    _swap(cur, table_name, migrated_table)
    print(f"  {table_name}: migriert - {after_count} Zeilen verifiziert, jetzt Iceberg.")


def migrate_state_table(cur):
    """gruppe3_etl_state: Append-Only-Historie beim Kopieren auf die jeweils
    juengste Zeile je (stage, table_name) verdichten (s. Modul-Docstring)."""
    if not table_exists(cur, STATE_TABLE):
        print(f"  {STATE_TABLE}: existiert noch nicht - nichts zu migrieren.")
        return

    if is_iceberg_table(cur, STATE_TABLE):
        print(f"  {STATE_TABLE}: bereits Iceberg - nichts zu tun.")
        return

    cur.execute(f"SELECT COUNT(DISTINCT CONCAT_WS('|', stage, table_name)) FROM {STATE_TABLE}")
    distinct_keys = cur.fetchone()[0]
    print(f"  {STATE_TABLE}: ist noch Parquet (Append-Only) - verdichte auf "
          f"{distinct_keys} aktuelle Eintraege und migriere ...")

    migrated_table = f"{STATE_TABLE}_iceberg"
    cur.execute(f"DROP TABLE IF EXISTS {migrated_table}")
    cur.execute(
        f"CREATE TABLE {migrated_table} STORED BY ICEBERG {ICEBERG_TBLPROPERTIES} AS "
        f"SELECT stage, table_name, watermark_value, content_hash, row_count, recorded_at "
        f"FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY stage, table_name "
        f"ORDER BY recorded_at DESC) AS rn FROM {STATE_TABLE}) x WHERE rn = 1"
    )

    after_count = row_count(cur, migrated_table)
    if after_count != distinct_keys:
        cur.execute(f"DROP TABLE IF EXISTS {migrated_table}")
        raise RuntimeError(
            f"Migration von {STATE_TABLE} abgebrochen: Kopie hat {after_count} "
            f"statt {distinct_keys} Zeilen. Original wurde NICHT veraendert."
        )

    _swap(cur, STATE_TABLE, migrated_table)
    print(f"  {STATE_TABLE}: migriert - {after_count} aktuelle Eintraege, jetzt Iceberg (Upsert).")


def drop_obsolete_tables(cur):
    for table_name in OBSOLETE_TABLES:
        if not table_exists(cur, table_name):
            print(f"  {table_name}: existiert nicht (mehr) - nichts zu tun.")
            continue
        cur.execute(f"DROP TABLE {table_name}")
        print(f"  {table_name}: geloescht (Zeilen-Hash-Historie wird nicht mehr gebraucht, s. ADR.md).")


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    print(f"Datenbank: {DATABASE}\n")

    print("Pipeline-Tabellen (Staging / Audit / Star-Schema):")
    for table_name, partition_spec in TABLES_TO_MIGRATE.items():
        migrate_table(cur, table_name, partition_spec)

    print("\nETL-State:")
    migrate_state_table(cur)

    print("\nNicht mehr genutzte Tabellen:")
    drop_obsolete_tables(cur)

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
