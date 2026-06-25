import os
from impala.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT"))
IMPALA_HTTP_PATH = os.getenv("IMPALA_HTTP_PATH")
IMPALA_USER = os.getenv("IMPALA_USER")
IMPALA_PASSWORD = os.getenv("IMPALA_PASSWORD")

connection = connect(
    host=IMPALA_HOST,
    port=IMPALA_PORT,
    auth_mechanism="LDAP",
    use_ssl=True,
    use_http_transport=True,
    http_path=IMPALA_HTTP_PATH,
    user=IMPALA_USER,
    password=IMPALA_PASSWORD,
)

cursor = connection.cursor()

SOURCE_TABLE = "nifi_source_orders"
TARGET_TABLE = "nifi_target_orders"

create_source_table = f"""
CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
    incremental_id BIGINT,
    customer_name  STRING,
    city           STRING,
    product_name   STRING,
    quantity       INT,
    amount         DECIMAL(10,2),
    updated_at     TIMESTAMP
)
STORED AS PARQUET
"""

create_target_table = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    incremental_id BIGINT,
    customer_name  STRING,
    city           STRING,
    product_name   STRING,
    quantity       INT,
    amount         DECIMAL(10,2),
    updated_at     TIMESTAMP
)
STORED AS PARQUET
"""

seed_source_table = f"""
INSERT INTO {SOURCE_TABLE}
SELECT 1, 'Mia Weber', 'Stuttgart', 'Sensor Kit', 2, CAST(149.90 AS DECIMAL(10,2)), CAST('2026-06-17 08:15:00' AS TIMESTAMP)
UNION ALL
SELECT 2, 'Leon Becker', 'Karlsruhe', 'Gateway Hub', 1, CAST(249.00 AS DECIMAL(10,2)), CAST('2026-06-17 08:30:00' AS TIMESTAMP)
UNION ALL
SELECT 3, 'Sofia Klein', 'Mannheim', 'Edge Node', 4, CAST(89.50 AS DECIMAL(10,2)), CAST('2026-06-17 09:10:00' AS TIMESTAMP)
UNION ALL
SELECT 4, 'Noah Fischer', 'Heilbronn', 'Telemetry Pack', 3, CAST(59.99 AS DECIMAL(10,2)), CAST('2026-06-17 09:45:00' AS TIMESTAMP)
UNION ALL
SELECT 5, 'Emma Wolf', 'Freiburg', 'IoT Bridge', 1, CAST(319.00 AS DECIMAL(10,2)), CAST('2026-06-17 10:05:00' AS TIMESTAMP)
"""

statements = {
    SOURCE_TABLE: create_source_table,
    TARGET_TABLE: create_target_table,
}

for table_name, statement in statements.items():
    print(f"Erstelle Tabelle: {table_name} ...")
    cursor.execute(statement)
    print("  -> OK")

cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
source_row_count = cursor.fetchone()[0]

if source_row_count == 0:
    print(f"Befuelle Quelltabelle: {SOURCE_TABLE} ...")
    cursor.execute(seed_source_table)
    print("  -> OK")
else:
    print(f"Quelltabelle {SOURCE_TABLE} enthaelt bereits {source_row_count} Zeilen, Seed wird uebersprungen.")

cursor.execute("INVALIDATE METADATA")
print("Metadaten aktualisiert.")

for table_name in (SOURCE_TABLE, TARGET_TABLE):
    print(f"\nSELECT * FROM {table_name} LIMIT 10:")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)
    else:
        print("  (keine Ergebnisse)")

print(
    "\nNiFi-Hinweis: Verwende in QueryDatabaseTableRecord oder GenerateTableFetch "
    f"die Spalte 'incremental_id' oder 'updated_at' fuer inkrementelle Loads von {SOURCE_TABLE} nach {TARGET_TABLE}."
)

cursor.close()
connection.close()

