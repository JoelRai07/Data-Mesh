"""
Persistente ETL-Metadaten-/State-Tabelle fuer das Incremental-Loader-Pattern.

Wird von allen drei Pipeline-Stufen genutzt (pipeline_default_to_staging.py,
pipeline_staging_to_audit.py, pipeline_audit_to_target.py), um pro
(pipeline_stage, table_name) den zuletzt erfolgreich verarbeiteten Stand
zu merken - entweder als Wasserzeichen (Zeitreihen-Tabellen) oder als
Inhalts-Pruefsumme (Snapshot-Tabellen ohne verlaesslichen
Aenderungsindikator, s. Doku in den beiden erstgenannten Modulen).

Zusaetzlich (weiter unten): ROW_STATE_TABLE/CHANGED_KEYS_TABLE fuer echtes
ZEILENGENAUES Incremental Merge bei Snapshot-Tabellen MIT Business-Key
(bauland, bevoelkerungzahlen, gemeinden) - s. audit_table_keyed_snapshot in
pipeline_staging_to_audit.py fuer die Verwendung und die Begruendung, warum
das trotz Impalas fehlendem UPDATE/MERGE moeglich ist (CREATE NEW + SWAP +
DROP statt In-Place-Update).

WARUM EINE APPEND-ONLY-TABELLE STATT UPDATE/UPSERT DES STATE-EINTRAGS?
  Alle Tabellen dieses Projekts liegen STORED AS PARQUET auf normalem
  Impala/HDFS-Speicher, nicht auf Kudu. Damit kennt Impala kein UPDATE,
  DELETE oder MERGE auf einzelnen Zeilen (nur INSERT, INSERT OVERWRITE,
  TRUNCATE) - ein klassisches "ueberschreibe den Watermark-Wert" ist also
  technisch nicht moeglich, ohne die gesamte Tabelle zu ersetzen. Diese
  State-Tabelle ist deshalb bewusst APPEND-ONLY: jeder Lauf haengt per
  INSERT INTO einen neuen Eintrag an, der "aktuelle" Stand einer
  (stage, table_name)-Kombination ist immer die Zeile mit dem juengsten
  recorded_at (s. get_latest_state). Das ist ein Standardmuster fuer
  ETL-Metadaten auf Append-Only-Warehouses ohne Zeilen-Update und kommt
  ohne Kudu/ACID-Tabellen aus, deren Verfuegbarkeit auf dem Kurs-Cluster
  nicht vorausgesetzt werden kann.
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
    """Legt die State-Tabelle an, falls sie noch nicht existiert (idempotent,
    analog zu den CREATE TABLE IF NOT EXISTS-Aufrufen in create_datamodel.py)."""
    cur.execute(CREATE_STATE_TABLE)


def get_latest_state(cur, stage, table_name):
    """
    Liefert den juengsten State-Eintrag (nach recorded_at) fuer
    (stage, table_name) als dict, oder None, falls es noch keinen gibt
    (= erster Lauf fuer diese Tabelle/Stufe -> Full Load noetig).

    Holt bewusst ALLE Zeilen fuer den Schluessel und bestimmt das Maximum in
    Python statt per SQL ORDER BY ... LIMIT 1: die Tabelle waechst nur um
    wenige Zeilen pro Pipeline-Lauf, ein voller Fetch ist hier unproblematisch
    und vermeidet Annahmen ueber Impala-spezifisches ORDER BY/LIMIT-Verhalten
    auf potenziell NULL-haltigen Spalten.
    """
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
    """SQL-Literal fuer einen optionalen String-Wert (NULL, falls nicht gesetzt).
    Einfache Anfuehrungszeichen werden escaped (gleiches Muster wie
    _sql_literal in pipeline_audit_to_target.py)."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def record_state(cur, stage, table_name, watermark_value=None, content_hash=None, row_count=None):
    """Haengt einen neuen State-Eintrag an (APPEND-ONLY, s. Modul-Docstring).
    Nur aufrufen, wenn die Tabelle tatsaechlich (neu) verarbeitet wurde -
    bei einem uebersprungenen, unveraenderten Lauf bleibt der bisherige
    Eintrag bestehen und bleibt damit korrekt der "zuletzt geaenderte" Stand."""
    cur.execute(
        f"INSERT INTO {STATE_TABLE} "
        f"(stage, table_name, watermark_value, content_hash, row_count, recorded_at) "
        f"VALUES ("
        f"'{stage}', '{table_name}', "
        f"{_sql_string_or_null(watermark_value)}, {_sql_string_or_null(content_hash)}, "
        f"{row_count if row_count is not None else 'NULL'}, now())"
    )


def get_columns(cur, table_name):
    """Spaltenliste einer Tabelle per DESCRIBE (gleiche Technik wie in
    pipeline_staging_to_audit.py) - hier wiederverwendet, um die
    Inhalts-Pruefsumme generisch fuer beliebige Snapshot-Tabellen zu bauen."""
    cur.execute(f"DESCRIBE {table_name}")
    return [row[0] for row in cur.fetchall()]


def content_signature(cur, table_name, columns):
    """
    Inhalts-Pruefsumme einer Snapshot-Tabelle OHNE verlaesslichen
    Aenderungsindikator (kein Datum/keine ID, die zuverlaessig waechst,
    und amtliche Statistiken koennen nachtraeglich revidiert werden - s.
    Begruendung in pipeline_default_to_staging.py, warum bauland/
    bevoelkerungzahlen/gemeinden NICHT per Wasserzeichen inkrementell
    geladen werden).

    Rein serverseitig in Impala berechnet (FNV_HASH je Zeile, SUM() als
    Aggregat) - kein Herunterladen der Daten noetig, auch fuer breite
    Tabellen wie bevoelkerungzahlen (83 Spalten) unproblematisch, da die
    Spaltenliste wie in pipeline_staging_to_audit.py per DESCRIBE ermittelt wird.

    KEIN kryptographischer Hash, sondern reine Change-Detection: Ziel ist
    "hat sich der Tabelleninhalt seit dem letzten Lauf ueberhaupt
    veraendert", nicht Manipulationssicherheit. SUM() statt eines
    XOR-Aggregats, weil SUM bei zufaelligen Kollisionen zwischen zwei
    Zeilen (z.B. eine geloescht, eine inhaltsgleiche neu hinzugefuegt)
    seltener zufaellig auf denselben Wert zurueckfaellt als ein
    kommutatives XOR.
    """
    col_list = ", ".join(f"CAST({c} AS STRING)" for c in columns)
    cur.execute(
        f"SELECT COUNT(*), SUM(fnv_hash(CONCAT_WS('|', {col_list}))) FROM {table_name}"
    )
    row_count, hash_sum = cur.fetchone()
    return row_count, (str(hash_sum) if hash_sum is not None else None)


# -----------------------------------------------------------------------------
# ZEILENGENAUES INCREMENTAL MERGE fuer Snapshot-Tabellen MIT Business-Key
# (bauland, bevoelkerungzahlen, gemeinden - s. audit_table_keyed_snapshot in
# pipeline_staging_to_audit.py). content_signature() oben beantwortet nur
# "hat sich IRGENDWO in der Tabelle etwas geaendert" (Tabellenebene). Die
# beiden folgenden Tabellen ermoeglichen es, GENAU DIESE Zeilen zu bestimmen:
# ROW_STATE_TABLE merkt sich einen Inhalts-Hash JE ZEILE (ueber einen fachlichen
# Business-Key), CHANGED_KEYS_TABLE ist ein kurzlebiger Join-Partner, um genau
# die neuen/geaenderten/geloeschten Keys innerhalb EINES Merge-Laufs
# wiederzufinden (Impala kennt keine Arrays/Variablen als Bind-Parameter fuer
# potenziell tausende Keys - eine kleine Hilfstabelle + JOIN ist der uebliche
# Impala/Hive-Weg dafuer).
# -----------------------------------------------------------------------------

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
    """
    Liefert {row_key: row_hash} mit dem JEWEILS juengsten aufgezeichneten Hash
    je Key (ROW_STATE_TABLE ist append-only wie gruppe3_etl_state, s. dortige
    Begruendung - "aktuell" ist immer der juengste recorded_at-Eintrag je Key).
    Holt die komplette Historie fuer table_name und reduziert in Python -
    fuer die Groessenordnung dieses Projekts (max. niedrige zehntausend Keys
    bei bauland) unproblematisch, vermeidet ein Window-Function-Konstrukt in SQL.

    Keys, deren JUENGSTER Eintrag row_hash IS NULL ist (Tombstone, s.
    record_row_hashes), werden NICHT zurueckgegeben - sie gelten als "zu
    diesem Zeitpunkt bereits aus der Quelle entfernt und in der Audit-Tabelle
    bereits geloescht". Ohne diesen Ausschluss wuerde audit_table_keyed_snapshot()
    einen einmal geloeschten Key bei JEDEM weiteren Lauf erneut als "geloescht"
    erkennen (er bliebe ja fuer immer in der Historie stehen) und dadurch
    faelschlich unendlich oft einen Merge-Lauf ausloesen, obwohl er in der
    Audit-Tabelle laengst nicht mehr vorkommt.
    """
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
    """
    Haengt fuer jedes (row_key, row_hash)-Paar einen neuen Eintrag an
    (APPEND-ONLY). NUR fuer tatsaechlich neue/geaenderte/geloeschte Keys
    aufrufen - unveraenderte Keys brauchen keinen neuen Eintrag, ihr letzter
    Hash bleibt weiterhin der gueltige "aktuelle" Stand (s.
    get_latest_row_hashes). Das haelt das Wachstum dieser Tabelle an die
    tatsaechliche Aenderungsrate gekoppelt, nicht an die Tabellengroesse.

    row_hash=None schreibt einen TOMBSTONE (Key wurde aus der Quelle
    entfernt, s. get_latest_row_hashes) - _sql_string_or_null() wandelt das
    korrekt in SQL NULL um.

    Batch-INSERT in Chunks (analog zu overwrite_table() in
    pipeline_audit_to_target.py) statt ein Statement je Zeile.
    """
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
    """
    Ersetzt den kompletten Inhalt von gruppe3_etl_changed_keys_tmp durch die
    uebergebenen Keys (TRUNCATE + Batch-INSERT). Diese Tabelle ist bewusst
    KEIN persistenter State, sondern nur ein kurzlebiger Join-Partner fuer
    GENAU EINEN Merge-Schritt (s. audit_table_keyed_snapshot in
    pipeline_staging_to_audit.py) - wird vor jeder Tabelle neu befuellt.
    """
    cur.execute(f"TRUNCATE TABLE {CHANGED_KEYS_TABLE}")
    keys = list(row_keys)
    for start in range(0, len(keys), batch_size):
        chunk = keys[start:start + batch_size]
        values_sql = ", ".join(f"({_sql_string_or_null(k)})" for k in chunk)
        cur.execute(f"INSERT INTO {CHANGED_KEYS_TABLE} (row_key) VALUES {values_sql}")
