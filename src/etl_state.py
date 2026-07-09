"""
ETL-Metadaten-Tabellen fuer Incremental Loading. Genutzt von
pipeline_default_to_staging.py, pipeline_staging_to_audit.py,
pipeline_audit_to_target.py.
"""

STATE_TABLE = "gruppe3_etl_state"

CREATE_STATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    stage           STRING COMMENT 'Pipeline-Stufe: staging, audit oder target',
    table_name      STRING COMMENT 'Fachlicher Tabellenname, z.B. klimadaten',
    watermark_value STRING COMMENT 'Zeitreihen-Tabellen: zuletzt verarbeiteter Wasserzeichen-Wert (z.B. MAX(dt))',
    content_hash    STRING COMMENT 'Snapshot-Tabellen: Pruefsumme des Tabelleninhalts, s. content_signature()',
    row_count       BIGINT COMMENT 'Zeilenzahl zum Aufzeichnungszeitpunkt (Diagnose)',
    recorded_at     TIMESTAMP COMMENT 'Wann dieser Stand zuletzt erfolgreich verarbeitet wurde'
)
STORED AS PARQUET
"""


def ensure_state_table(cur):
    """Input: Cursor. Output: legt STATE_TABLE an, falls nicht vorhanden."""
    cur.execute(CREATE_STATE_TABLE)


def get_latest_state(cur, stage, table_name):
    """Input: stage, table_name. Output: juengster State-Eintrag als dict, oder None."""
    cur.execute(
        f"SELECT watermark_value, content_hash, row_count, recorded_at "
        f"FROM {STATE_TABLE} WHERE stage = '{stage}' AND table_name = '{table_name}'"
    )
    rows = cur.fetchall()
    if not rows:
        return None

    watermark_value, content_hash, row_count, recorded_at = max(rows, key=lambda r: r[3])
    return {
        "watermark_value": watermark_value,
        "content_hash": content_hash,
        "row_count": row_count,
        "recorded_at": recorded_at,
    }


def _sql_string_or_null(value):
    """Input: optionaler Wert. Output: SQL-Literal (NULL oder gequoteter String)."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def record_state(cur, stage, table_name, watermark_value=None, content_hash=None, row_count=None):
    """Input: stage, table_name, neuer Stand. Output: haengt neuen State-Eintrag an (append-only)."""
    cur.execute(
        f"INSERT INTO {STATE_TABLE} "
        f"(stage, table_name, watermark_value, content_hash, row_count, recorded_at) "
        f"VALUES ("
        f"'{stage}', '{table_name}', "
        f"{_sql_string_or_null(watermark_value)}, {_sql_string_or_null(content_hash)}, "
        f"{row_count if row_count is not None else 'NULL'}, now())"
    )


def get_columns(cur, table_name):
    """Input: table_name. Output: Liste der Spaltennamen."""
    cur.execute(f"DESCRIBE {table_name}")
    return [row[0] for row in cur.fetchall()]


def content_signature(cur, table_name, columns):
    """Input: table_name, columns. Output: (row_count, content_hash) - Pruefsumme
    ueber alle Zeilen/Spalten, fuer Change-Detection ohne verlaesslichen Key."""
    col_list = ", ".join(f"CAST({c} AS STRING)" for c in columns)
    cur.execute(
        f"SELECT COUNT(*), SUM(fnv_hash(CONCAT_WS('|', {col_list}))) FROM {table_name}"
    )
    row_count, hash_sum = cur.fetchone()
    return row_count, (str(hash_sum) if hash_sum is not None else None)


# Zeilengenaues Incremental Merge fuer bauland/bevoelkerungzahlen
# (s. audit_table_keyed_snapshot in pipeline_staging_to_audit.py).
ROW_STATE_TABLE = "gruppe3_etl_row_state"
CHANGED_KEYS_TABLE = "gruppe3_etl_changed_keys_tmp"

CREATE_ROW_STATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {ROW_STATE_TABLE} (
    table_name  STRING COMMENT 'Fachlicher Tabellenname, z.B. bauland',
    row_key     STRING COMMENT 'Business-Key der Zeile (CONCAT_WS(...) mehrerer Spalten), s. KEY_COLUMNS in pipeline_staging_to_audit.py',
    row_hash    STRING COMMENT 'FNV_HASH-Pruefsumme des Zeileninhalts. NULL = Tombstone (Key wurde zu recorded_at aus der Quelle entfernt, s. get_latest_row_hashes)',
    recorded_at TIMESTAMP COMMENT 'Wann dieser Hash zuletzt aufgezeichnet wurde'
)
STORED AS PARQUET
"""

CREATE_CHANGED_KEYS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CHANGED_KEYS_TABLE} (
    row_key STRING COMMENT 'Business-Key einer neuen/geaenderten/geloeschten Zeile - nur fuer die Dauer EINES Merge-Laufs gueltig, wird pro verarbeiteter Tabelle komplett neu befuellt'
)
STORED AS PARQUET
"""


def ensure_row_state_table(cur):
    cur.execute(CREATE_ROW_STATE_TABLE)


def ensure_changed_keys_table(cur):
    cur.execute(CREATE_CHANGED_KEYS_TABLE)


def get_latest_row_hashes(cur, table_name):
    """Input: table_name. Output: {row_key: row_hash} - juengster Hash je Key,
    Tombstones (row_hash IS NULL = geloescht) werden ausgefiltert."""
    cur.execute(
        f"SELECT row_key, row_hash, recorded_at FROM {ROW_STATE_TABLE} WHERE table_name = '{table_name}'"
    )
    latest_hash = {}
    latest_ts = {}
    for row_key, row_hash, recorded_at in cur.fetchall():
        if row_key not in latest_ts or recorded_at > latest_ts[row_key]:
            latest_hash[row_key] = row_hash
            latest_ts[row_key] = recorded_at
    return {k: v for k, v in latest_hash.items() if v is not None}


def record_row_hashes(cur, table_name, key_hash_pairs, batch_size=500):
    """Input: table_name, [(row_key, row_hash)] (row_hash=None = Tombstone).
    Output: haengt neue Eintraege an ROW_STATE_TABLE an (append-only, Batches)."""
    pairs = list(key_hash_pairs)
    for start in range(0, len(pairs), batch_size):
        chunk = pairs[start:start + batch_size]
        values_sql = ", ".join(
            f"('{table_name}', {_sql_string_or_null(k)}, {_sql_string_or_null(h)}, now())"
            for k, h in chunk
        )
        cur.execute(
            f"INSERT INTO {ROW_STATE_TABLE} (table_name, row_key, row_hash, recorded_at) VALUES {values_sql}"
        )


def load_changed_keys(cur, row_keys, batch_size=500):
    """Input: Liste von row_keys. Output: ersetzt Inhalt von CHANGED_KEYS_TABLE
    komplett (TRUNCATE + Batch-INSERT)."""
    cur.execute(f"TRUNCATE TABLE {CHANGED_KEYS_TABLE}")
    keys = list(row_keys)
    for start in range(0, len(keys), batch_size):
        chunk = keys[start:start + batch_size]
        values_sql = ", ".join(f"({_sql_string_or_null(k)})" for k in chunk)
        cur.execute(f"INSERT INTO {CHANGED_KEYS_TABLE} (row_key) VALUES {values_sql}")
