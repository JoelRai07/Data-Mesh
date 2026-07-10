"""
Persistente ETL-Metadaten-/State-Tabelle fuer das Incremental-Loader-Pattern
+ gemeinsame Apache-Iceberg-Helfer fuer alle drei Pipeline-Stufen.

Wird von allen drei Pipeline-Stufen genutzt (pipeline_default_to_staging.py,
pipeline_staging_to_audit.py, pipeline_audit_to_target.py), um pro
(pipeline_stage, table_name) den zuletzt erfolgreich verarbeiteten Stand
zu merken - entweder als Wasserzeichen (Zeitreihen-Tabellen) oder als
Inhalts-Pruefsumme (Snapshot-Tabellen ohne verlaesslichen
Aenderungsindikator, s. Doku in den beiden erstgenannten Modulen).

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
    stage           STRING COMMENT 'Pipeline-Stufe: staging, audit oder target',
    table_name      STRING COMMENT 'Fachlicher Tabellenname, z.B. klimadaten',
    watermark_value STRING COMMENT 'Zeitreihen-Tabellen: zuletzt verarbeiteter Wasserzeichen-Wert (z.B. MAX(dt))',
    content_hash    STRING COMMENT 'Snapshot-Tabellen: Pruefsumme des Tabelleninhalts, s. content_signature()',
    row_count       BIGINT COMMENT 'Zeilenzahl zum Aufzeichnungszeitpunkt (Diagnose)',
    recorded_at     TIMESTAMP COMMENT 'Wann dieser Stand zuletzt erfolgreich verarbeitet wurde'
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
        f"SELECT watermark_value, content_hash, row_count, recorded_at "
        f"FROM {STATE_TABLE} WHERE stage = '{stage}' AND table_name = '{table_name}'"
    )
    rows = cur.fetchall()
    if not rows:
        return None

    watermark_value, content_hash, row_count, recorded_at = rows[0]
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
        f"CAST({row_count_sql} AS BIGINT) AS row_count, "
        f"now() AS recorded_at"
        f") src ON t.stage = src.stage AND t.table_name = src.table_name "
        f"WHEN MATCHED THEN UPDATE SET watermark_value = src.watermark_value, "
        f"content_hash = src.content_hash, row_count = src.row_count, "
        f"recorded_at = src.recorded_at "
        f"WHEN NOT MATCHED THEN INSERT (stage, table_name, watermark_value, content_hash, row_count, recorded_at) "
        f"VALUES (src.stage, src.table_name, src.watermark_value, src.content_hash, src.row_count, src.recorded_at)"
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
    Tabellen wie bevoelkerungzahlen (92 Spalten) unproblematisch, da die
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
