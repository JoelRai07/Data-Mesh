"""
Persistente ETL-Metadaten-/State-Tabelle fuer das Incremental-Loader-Pattern
+ gemeinsame Apache-Iceberg-Helfer fuer alle drei Pipeline-Stufen.

Wird von allen drei Pipeline-Stufen genutzt (pipeline_default_to_staging.py,
pipeline_staging_to_audit.py, pipeline_audit_to_target.py), um pro
(pipeline_stage, table_name) den zuletzt erfolgreich verarbeiteten Stand
zu merken - entweder als Wasserzeichen (Zeitreihen-Tabellen) oder als
Inhalts-Pruefsumme (Snapshot-Tabellen ohne verlaesslichen
Aenderungsindikator, s. Doku in den beiden erstgenannten Modulen).

ZWEISTUFIGE CHANGE-DETECTION (Ebene 1 vor Ebene 2):
  Ebene 1 - table_fingerprint(): EIN gespeicherter Fingerprint je Tabelle
    (Iceberg-Snapshot-ID, reine Metadaten-Abfrage ohne Zeilen-Scan).
    Unveraendert -> Stufe komplett uebersprungen, ohne eine einzige
    Datenzeile anzufassen.
  Ebene 2 - erst bei Fingerprint-Wechsel greift der eigentliche
    Incremental-Mechanismus der jeweiligen Stufe: Wasserzeichen-Append,
    zeilengenaues MERGE oder Pruefsummen-Vergleich + Full Refresh.

WARUM UPSERT (MERGE INTO) STATT DER FRUEHEREN APPEND-ONLY-HISTORIE?
  Die State-Tabelle war frueher append-only (jeder Lauf haengte eine neue
  Zeile an, "aktuell" war die juengste recorded_at-Zeile je Schluessel),
  weil Parquet-Tabellen auf Impala/HDFS kein UPDATE/MERGE kennen. Seit die
  komplette Pipeline auf Apache Iceberg laeuft (s. ADR.md), gilt das nicht
  mehr: gruppe3_etl_state ist selbst eine Iceberg-Tabelle, record_state()
  macht ein echtes UPSERT per MERGE INTO, und get_latest_state() ist ein
  einfacher SELECT auf genau eine Zeile je (stage, table_name) - ohne
  "juengste Zeile suchen"-Logik in Python. Die Historie geht dabei NICHT
  verloren, sie wandert nur die Ebene runter: jeder MERGE ist ein eigener
  Iceberg-Snapshot, d.h. "wie sah der State vor 3 Laeufen aus" beantwortet
  DESCRIBE HISTORY gruppe3_etl_state bzw. Time Travel
  (SELECT ... FOR SYSTEM_TIME AS OF ...), ohne dass die Tabelle selbst
  unbegrenzt waechst.

Die frueher zusaetzlich gepflegte Zeilen-Hash-Historie
(gruppe3_etl_row_state/gruppe3_etl_changed_keys_tmp) ist komplett
entfallen: seit die Audit-Tabellen Iceberg sind, IST die Audit-Tabelle
selbst der Vergleichsstand fuer das zeilengenaue Merge - der NULL-sichere
Spaltenvergleich passiert direkt im MERGE INTO (s.
audit_table_keyed_snapshot in pipeline_staging_to_audit.py und ADR.md,
Iteration 4).
"""

STATE_TABLE = "gruppe3_etl_state"

ICEBERG_TBLPROPERTIES = "TBLPROPERTIES('format-version'='2')"

CREATE_STATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    stage             STRING COMMENT 'Pipeline-Stufe: staging, audit oder target',
    table_name        STRING COMMENT 'Fachlicher Tabellenname, z.B. klimadaten',
    watermark_value   STRING COMMENT 'Zeitreihen-Tabellen: zuletzt verarbeiteter Wasserzeichen-Wert (z.B. MAX(dt))',
    content_hash      STRING COMMENT 'Snapshot-Tabellen: Pruefsumme des Tabelleninhalts, s. content_signature()',
    table_fingerprint STRING COMMENT 'Metadaten-Fingerprint der QUELLE dieser Stufe (Iceberg-Snapshot-ID), s. table_fingerprint()',
    row_count         BIGINT COMMENT 'Zeilenzahl zum Aufzeichnungszeitpunkt (Diagnose)',
    recorded_at       TIMESTAMP COMMENT 'Wann dieser Stand zuletzt erfolgreich verarbeitet wurde'
)
STORED BY ICEBERG {ICEBERG_TBLPROPERTIES}
"""


def table_exists(cur, table_name):
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    return len(cur.fetchall()) > 0


def is_iceberg_table(cur, table_name):
    """Prueft gezielt die Table-Parameter-Zeile 'table_type' = 'ICEBERG' aus
    DESCRIBE FORMATTED (Impala kennt kein SHOW TBLPROPERTIES). Bei diesen
    Zeilen liegt der Property-Name in Spalte 2, der Wert in Spalte 3 (Spalte 1
    ist leer) - gezielter Spaltenvergleich statt einer Substring-Suche ueber
    den gesamten Freitext-Dump, robuster gegenueber Format-Aenderungen."""
    cur.execute(f"DESCRIBE FORMATTED {table_name}")
    for row in cur.fetchall():
        key = (row[1] or "").strip() if len(row) > 1 else ""
        if key == "table_type":
            value = (row[2] or "").strip() if len(row) > 2 else ""
            return value.upper() == "ICEBERG"
    return False


def ensure_iceberg_table_like(cur, table_name, source_fqn, partition_spec=None):
    """
    Stellt sicher, dass table_name als ICEBERG-Tabelle mit dem Schema von
    source_fqn existiert (Iceberg ist Voraussetzung fuer MERGE INTO/DELETE
    und fuer atomare INSERT OVERWRITE-Snapshots, s. ADR.md):

      1) Tabelle existiert noch nicht -> frisch als Iceberg anlegen.
         "CREATE TABLE ... LIKE ... STORED BY ICEBERG" schlaegt fehl, wenn
         die Quelltabelle selbst kein Iceberg ist ("cannot be cloned into
         an Iceberg table") - deshalb stattdessen CTAS mit WHERE 1=0:
         uebernimmt Spalten/Typen 1:1, aber keine Zeilen.
      2) Tabelle existiert bereits als Iceberg -> nichts zu tun (idempotent,
         der Normalfall ab dem zweiten Lauf).
      3) Tabelle existiert noch als Parquet -> FEHLER mit klarer Anleitung,
         statt automatisch und lautlos zu migrieren. Eine Storage-Format-
         Migration ist ein einmaliger, strukturell heikler Vorgang (CTAS
         ueber die komplette Tabelle + Rename-Swap) - das gehoert NICHT in
         den taeglichen Pipeline-Lauf. Migration:
         src/utils/migrate_to_iceberg.py (einmalig, idempotent, mit
         sichtbarer Fortschrittsausgabe).

    partition_spec (optional): Iceberg-Partition-Transform als SQL-Fragment,
    z.B. "PARTITIONED BY SPEC (TRUNCATE(4, dt))" fuer die Klimadaten -
    partitioniert nach Jahres-Praefix des ISO-Datums-Strings (Iceberg
    "hidden partitioning": Abfragen/INSERTs muessen die Partition nie
    explizit nennen, Impala prunt WHERE dt > '...' automatisch).
    """
    if not table_exists(cur, table_name):
        spec_sql = f" {partition_spec}" if partition_spec else ""
        cur.execute(
            f"CREATE TABLE {table_name}{spec_sql} STORED BY ICEBERG "
            f"{ICEBERG_TBLPROPERTIES} AS SELECT * FROM {source_fqn} WHERE 1=0"
        )
        return

    if not is_iceberg_table(cur, table_name):
        raise RuntimeError(
            f"{table_name} existiert noch als Parquet-Tabelle, die Pipeline "
            "erwartet aber Iceberg. Bitte einmalig "
            "'.venv/Scripts/python.exe src/utils/migrate_to_iceberg.py' "
            "ausfuehren und die Pipeline danach erneut starten."
        )


def ensure_state_table(cur):
    """Legt die State-Tabelle an, falls sie noch nicht existiert (idempotent).
    Bricht mit klarer Anleitung ab, falls sie noch als Parquet-Tabelle aus
    einem aelteren Stand existiert (record_state braucht MERGE INTO)."""
    if table_exists(cur, STATE_TABLE):
        if not is_iceberg_table(cur, STATE_TABLE):
            raise RuntimeError(
                f"{STATE_TABLE} existiert noch als Parquet-Tabelle (alter "
                "Append-Only-Stand). Bitte einmalig "
                "'.venv/Scripts/python.exe src/utils/migrate_to_iceberg.py' "
                "ausfuehren und die Pipeline danach erneut starten."
            )
        # Schema-Migration fuer Bestandsinstallationen: die Spalte
        # table_fingerprint kam nachtraeglich dazu (Iceberg-Snapshot-basierte
        # Change-Detection). Iceberg-Schema-Evolution macht das ALTER zu einer
        # reinen Metadaten-Operation; bestehende Zeilen lesen sich als NULL
        # (= "noch kein Fingerprint aufgezeichnet" -> naechster Lauf faellt
        # einmalig auf die Pruefsummen-/Watermark-Pruefung zurueck).
        if "table_fingerprint" not in get_columns(cur, STATE_TABLE):
            cur.execute(
                f"ALTER TABLE {STATE_TABLE} ADD COLUMNS (table_fingerprint STRING)"
            )
        return
    cur.execute(CREATE_STATE_TABLE)


def get_latest_state(cur, stage, table_name):
    """
    Liefert den State-Eintrag fuer (stage, table_name) als dict, oder None,
    falls es noch keinen gibt (= erster Lauf fuer diese Tabelle/Stufe ->
    Full Load noetig). Dank Upsert (s. record_state) existiert je Schluessel
    hoechstens eine Zeile - ein einfacher SELECT genuegt.
    """
    cur.execute(
        f"SELECT watermark_value, content_hash, table_fingerprint, row_count, recorded_at "
        f"FROM {STATE_TABLE} WHERE stage = '{stage}' AND table_name = '{table_name}'"
    )
    rows = cur.fetchall()
    if not rows:
        return None

    watermark_value, content_hash, table_fingerprint, row_count, recorded_at = rows[0]
    return {
        "watermark_value": watermark_value,
        "content_hash": content_hash,
        "table_fingerprint": table_fingerprint,
        "row_count": row_count,
        "recorded_at": recorded_at,
    }


def _sql_string_or_null(value):
    """Input: optionaler Wert. Output: SQL-Literal (NULL oder gequoteter String)."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def record_state(cur, stage, table_name, watermark_value=None, content_hash=None,
                 row_count=None, table_fingerprint=None):
    """
    UPSERT des State-Eintrags per MERGE INTO (Iceberg): existiert schon ein
    Eintrag fuer (stage, table_name), wird er aktualisiert, sonst eingefuegt.
    Jeder Aufruf ist ein eigener Iceberg-Snapshot - die komplette Historie
    bleibt damit per DESCRIBE HISTORY / Time Travel abfragbar (s.
    Modul-Docstring), ohne dass die Tabelle waechst.

    Nur aufrufen, wenn die Tabelle tatsaechlich (neu) verarbeitet wurde -
    bei einem uebersprungenen, unveraenderten Lauf bleibt der bisherige
    Eintrag bestehen und bleibt damit korrekt der "zuletzt geaenderte"
    Stand (wichtig fuer should_skip_target_build in
    pipeline_audit_to_target.py, das recorded_at der Stufen vergleicht).
    """
    row_count_sql = str(row_count) if row_count is not None else "CAST(NULL AS BIGINT)"
    cur.execute(
        f"MERGE INTO {STATE_TABLE} t USING ("
        f"SELECT CAST('{stage}' AS STRING) AS stage, "
        f"CAST('{table_name}' AS STRING) AS table_name, "
        f"CAST({_sql_string_or_null(watermark_value)} AS STRING) AS watermark_value, "
        f"CAST({_sql_string_or_null(content_hash)} AS STRING) AS content_hash, "
        f"CAST({_sql_string_or_null(table_fingerprint)} AS STRING) AS table_fingerprint, "
        f"CAST({row_count_sql} AS BIGINT) AS row_count, "
        f"now() AS recorded_at"
        f") src ON t.stage = src.stage AND t.table_name = src.table_name "
        f"WHEN MATCHED THEN UPDATE SET watermark_value = src.watermark_value, "
        f"content_hash = src.content_hash, table_fingerprint = src.table_fingerprint, "
        f"row_count = src.row_count, recorded_at = src.recorded_at "
        f"WHEN NOT MATCHED THEN INSERT (stage, table_name, watermark_value, content_hash, table_fingerprint, row_count, recorded_at) "
        f"VALUES (src.stage, src.table_name, src.watermark_value, src.content_hash, src.table_fingerprint, src.row_count, src.recorded_at)"
    )


def get_columns(cur, table_name):
    """Input: table_name. Output: Liste der Spaltennamen."""
    cur.execute(f"DESCRIBE {table_name}")
    return [row[0] for row in cur.fetchall()]


def content_signature(cur, table_name, columns):
    """Input: table_name, columns. Output: (row_count, content_hash) - Pruefsumme
    ueber alle Zeilen/Spalten, fuer Change-Detection ohne verlaesslichen Key.
    ACHTUNG: scannt die komplette Tabelle - wo moeglich zuerst den
    metadaten-basierten table_fingerprint() vergleichen (kein Zeilen-Scan)."""
    col_list = ", ".join(f"CAST({c} AS STRING)" for c in columns)
    cur.execute(
        f"SELECT COUNT(*), SUM(fnv_hash(CONCAT_WS('|', {col_list}))) FROM {table_name}"
    )
    row_count, hash_sum = cur.fetchone()
    return row_count, (str(hash_sum) if hash_sum is not None else None)


def iceberg_snapshot_id(cur, table_name):
    """Input: Name einer ICEBERG-Tabelle. Output: aktuelle Snapshot-ID als
    String, oder None (Tabelle hat noch keinen Snapshot, z.B. frisch per
    CTAS WHERE 1=0 angelegt und noch nie beschrieben).

    DESCRIBE HISTORY ist eine reine Metadaten-Abfrage (liest nur die
    Iceberg-Snapshot-Historie, keine einzige Datenzeile). Spalten laut
    Impala-Doku: creation_time, snapshot_id, parent_id, is_current_ancestor.
    Aktuell = juengste creation_time unter den Zeilen mit
    is_current_ancestor=TRUE (nach einem Rollback gehoeren verwaiste
    Snapshots nicht mehr zur aktuellen Linie)."""
    cur.execute(f"DESCRIBE HISTORY {table_name}")
    current_line = [
        row for row in cur.fetchall()
        if len(row) > 3 and str(row[3]).strip().upper() == "TRUE"
    ]
    if not current_line:
        return None
    latest = max(current_line, key=lambda row: row[0])
    return str(latest[1])


def table_fingerprint(cur, table_name):
    """
    GANZTABELLEN-FINGERPRINT OHNE ZEILEN-SCAN (Ebene 1 der Change-Detection):

    Liefert einen String, der sich GENAU DANN aendert, wenn sich der Inhalt
    der Tabelle geaendert haben KANN - fuer Iceberg-Tabellen die aktuelle
    Snapshot-ID ("iceberg-snapshot:<id>"): jeder Schreibvorgang (INSERT,
    INSERT OVERWRITE, DELETE, MERGE, auch Compaction) erzeugt einen neuen
    Snapshot, reine Lesezugriffe nicht. Der Abgleich "Fingerprint jetzt ==
    Fingerprint beim letzten Lauf" ersetzt damit den kompletten
    Zeilen-Scan von content_signature() im Unveraendert-Fall (dem Normalfall
    bei taeglichen Laeufen).

    Fuer Nicht-Iceberg-Tabellen (z.B. die fremden Quelltabellen in
    default.*) gibt es keinen verlaesslichen Metadaten-Indikator -> None;
    der Aufrufer faellt dann auf die naechste Ebene zurueck
    (content_signature()-Pruefsumme bzw. Wasserzeichen).

    Konservativ in die sichere Richtung: ein Fingerprint-Wechsel ohne
    inhaltliche Aenderung (z.B. Compaction) loest nur eine unnoetige
    Ebene-2-Pruefung aus, nie einen Datenverlust; ein inhaltlicher Wechsel
    erzeugt IMMER eine neue Snapshot-ID.
    """
    if not is_iceberg_table(cur, table_name):
        return None
    snapshot_id = iceberg_snapshot_id(cur, table_name)
    if snapshot_id is None:
        return None
    return f"iceberg-snapshot:{snapshot_id}"
