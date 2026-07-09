"""
WAP Stufe 2: staging table -> audit table (Bereinigung).

"""
import os
import re

from db import get_connection
from etl_state import (
    ensure_state_table,
    get_latest_state,
    record_state,
    content_signature,
    ensure_row_state_table,
    get_latest_row_hashes,
    record_row_hashes,
    ensure_changed_keys_table,
    load_changed_keys,
    CHANGED_KEYS_TABLE,
)

DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

STAGING_TABLES = {
    "bauland": PREFIX + "staging_bauland",
    "bevoelkerungzahlen": PREFIX + "staging_bevoelkerungzahlen",
    "gemeinden": PREFIX + "staging_gemeinden",
    "klimadaten": PREFIX + "staging_klimadaten",
}
AUDIT_TABLES = {name: PREFIX + "audit_" + name for name in STAGING_TABLES}

TIME_SERIES_TABLES = {"klimadaten"}
TIME_SERIES_WATERMARK_COLUMN = "dt"
SNAPSHOT_TABLES = set(STAGING_TABLES) - TIME_SERIES_TABLES
KEY_COLUMNS = {
    "bauland": ["kreis_id", "jahr", "merkmal"],
    "bevoelkerungzahlen": ["id"],
}
TABLE_LEVEL_SNAPSHOT_TABLES = SNAPSHOT_TABLES - set(KEY_COLUMNS)


def trim(column):
    """Input: Spaltenname. Output: SQL-Ausdruck TRIM(column)."""
    return f"TRIM({column})"


def strip_after_comma(column):
    """Input: Spaltenname. Output: SQL-Ausdruck - alles ab dem ersten Komma entfernt, getrimmt."""
    return f"TRIM(SPLIT_PART({column}, ',', 1))"


UMLAUT_REPLACEMENTS = [
    ("ä", "ae"), ("ö", "oe"), ("ü", "ue"),
    ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue"),
]


def transliterate_umlauts(column):
    """Input: Spaltenname. Output: SQL-Ausdruck, Umlaute -> ASCII (ae/oe/ue)."""
    expr = column
    for umlaut, replacement in UMLAUT_REPLACEMENTS:
        expr = f"REPLACE({expr}, '{umlaut}', '{replacement}')"
    return expr


def compass_to_signed_decimal(column, negative_letter):
    """Input: Spaltenname (Format "53.84N"), negative_letter ('S' oder 'W').
    Output: SQL-Ausdruck, Format "5,63"/"-5,63" (Komma-Dezimal, Vorzeichen statt Buchstabe)."""
    numeric = f"CAST(REGEXP_REPLACE({column}, '[A-Z]', '') AS DOUBLE)"
    signed = f"CASE WHEN {column} LIKE '%{negative_letter}' THEN -{numeric} ELSE {numeric} END"
    return f"REPLACE(CAST({signed} AS STRING), '.', ',')"


BAULAND_MERKMAL_CORRECTIONS = {
    "Ver�u�erungsf�lle von Bauland": "Veraeusserungsfaelle von Bauland",
    "Ver�u�erte Baulandfl�che": "Veraeusserte Baulandflaeche",
}


def fix_known_values(expr, corrections, else_expr=None):
    """Input: SQL-Ausdruck, {kaputt: korrekt}. Output: CASE-Ausdruck, ersetzt bekannte Werte."""
    if else_expr is None:
        else_expr = expr
    cases = " ".join(
        f"WHEN {expr} = '{bad}' THEN '{good}'" for bad, good in corrections.items()
    )
    return f"CASE {cases} ELSE {else_expr} END"


_UMLAUT_ASCII = [
    ("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"),
    ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue"),
]

_WORD_SPLIT_PATTERN = re.compile(r"([^A-Za-zÀ-ÖØ-öø-ÿ�]+)")


def _transliterate_umlauts_py(name):
    """Input: String. Output: String mit Umlauten -> ASCII."""
    for umlaut, replacement in _UMLAUT_ASCII:
        name = name.replace(umlaut, replacement)
    return name


def _load_name_list(filename):
    """Input: Dateiname in src/utils/. Output: Liste von Namen (eine Zeile je Name)."""
    path = os.path.join(os.path.dirname(__file__), "utils", filename)
    with open(path, encoding="utf-8") as f:
        return [line.strip().strip('"') for line in f if line.strip()]


_REFERENCE_NAMES = list(dict.fromkeys(
    _load_name_list("german_cities.txt")
    + _load_name_list("german_regions.txt")
    + _load_name_list("german_states.txt")
))


def _find_reference_match(pattern_str):
    """Input: Regex-Pattern ('.' statt Ersatzzeichen U+FFFD). Output: eindeutiger
    Treffer im Namenspool, oder None (bei Mehrdeutigkeit: Treffer mit Umlaut bevorzugt)."""
    pattern = re.compile("^" + pattern_str + "$")
    matches = [name for name in _REFERENCE_NAMES if pattern.match(name)]
    if len(matches) > 1:
        matches = [m for m in matches if re.search(r"[äöüßÄÖÜ]", m)]
    return matches[0] if len(matches) == 1 else None


def _resolve_kreis_correction(bad_name):
    """Input: kaputter Kreisname (mit U+FFFD). Output: korrigierter, ASCII-transliterierter
    Name aus den Referenzlisten (erst ganzer Name, dann wortweise), oder None."""
    whole_pattern = re.escape(bad_name).replace(re.escape("�"), ".")
    whole_match = _find_reference_match(whole_pattern)
    if whole_match is not None:
        return _transliterate_umlauts_py(whole_match)

    tokens = _WORD_SPLIT_PATTERN.split(bad_name)
    resolved_any = False
    for i, token in enumerate(tokens):
        if "�" not in token:
            continue
        token_pattern = re.escape(token).replace(re.escape("�"), ".")
        token_match = _find_reference_match(token_pattern)
        if token_match is None:
            return None
        tokens[i] = token_match
        resolved_any = True
    if not resolved_any:
        return None
    return _transliterate_umlauts_py("".join(tokens))


# Manuell gepflegt: Berliner Bezirke + laengst aufgeloeste Landkreise, die in
# keiner Referenzliste mehr vorkommen.
MANUAL_KREIS_CORRECTIONS = {
    "St�dteregion Aachen": "Staedteregion Aachen",
    "Berlin-Treptow-K�penick": "Berlin-Treptow-Koepenick",
    "Berlin-Neuk�lln": "Berlin-Neukoelln",
    "Wei�eritzkreis": "Weisseritzkreis",
    "S�chsische Schweiz": "Saechsische Schweiz",
    "B�rdekreis": "Boerdekreis",
    "R�gen": "Ruegen",
    "Landkreis M�ritz": "Landkreis Mueritz",
}


def _discover_bad_kreis_values(cur):
    """Input: Cursor. Output: sortierte Liste kaputter kreis-Werte (mit U+FFFD),
    live aus bauland/bevoelkerungzahlen gelesen."""
    bad_values = set()
    for table in (STAGING_TABLES["bauland"], STAGING_TABLES["bevoelkerungzahlen"]):
        cur.execute(
            f"SELECT DISTINCT TRIM(SPLIT_PART(kreis, ',', 1)) FROM {table} "
            "WHERE kreis LIKE '%�%'"
        )
        bad_values.update(row[0] for row in cur.fetchall() if row[0])
    return sorted(bad_values)


def _build_kreis_corrections(cur):
    """Input: Cursor. Output: {kaputt: korrekt} - MANUAL_KREIS_CORRECTIONS plus
    automatisch aufgeloeste Werte; RuntimeError, falls ein Wert unaufloesbar ist."""
    corrections = dict(MANUAL_KREIS_CORRECTIONS)
    for bad_name in _discover_bad_kreis_values(cur):
        if bad_name in corrections:
            continue
        good_name = _resolve_kreis_correction(bad_name)
        if good_name is None:
            raise RuntimeError(
                f"KREIS_CORRECTIONS: '{bad_name}' (aus den Staging-Tabellen) "
                "nicht in den Referenzlisten gefunden - ggf. in "
                "MANUAL_KREIS_CORRECTIONS aufnehmen."
            )
        corrections[bad_name] = good_name
    return corrections


CITY_NAME_CORRECTIONS = {
    "Munich": "Muenchen",
    "Cologne": "Koeln",
    "Hanover": "Hannover",
    "Nuremberg": "Nuernberg",
    "Brunswick": "Braunschweig",
    "Ratisbon": "Regensburg",
}


def build_audit_rules(cur):
    """Input: Cursor. Output: {tabelle: {"columns": {...}, "where": ...}} -
    Bereinigungsregeln je Tabelle/Spalte."""
    kreis_corrections = _build_kreis_corrections(cur)
    return {
        "bauland": {
            "columns": {
                "kreis": fix_known_values(strip_after_comma("kreis"), kreis_corrections),
                "merkmal": fix_known_values(trim("merkmal"), BAULAND_MERKMAL_CORRECTIONS),
            },
            "where": None,
        },
        "bevoelkerungzahlen": {
            "columns": {
                "kreis": fix_known_values(strip_after_comma("kreis"), kreis_corrections),
            },
            "where": None,
        },
        "gemeinden": {
            "columns": {
                "municipality_name": trim(transliterate_umlauts("municipality_name")),
                "district_kreis": transliterate_umlauts(strip_after_comma("district_kreis")),
            },
            "where": None,
        },
        "klimadaten": {
            "columns": {
                "city": transliterate_umlauts(fix_known_values(trim("city"), CITY_NAME_CORRECTIONS)),
                "latitude": compass_to_signed_decimal("latitude", "S"),
                "longitude": compass_to_signed_decimal("longitude", "W"),
            },
            "where": "country = 'Germany'",
        },
    }


def get_columns(cur, table_name):
    """Input: table_name. Output: Liste der Spaltennamen."""
    cur.execute(f"DESCRIBE {table_name}")
    return [row[0] for row in cur.fetchall()]


def build_select_list(columns, column_rules):
    """Input: Spaltenliste, {spalte: SQL-Ausdruck}. Output: SELECT-Liste als String."""
    return ", ".join(
        f"{column_rules[col]} AS {col}" if col in column_rules else col
        for col in columns
    )


def audit_table_incremental(cur, name, staging_table, audit_table_name, select_list, base_where):
    """Input: staging_table, select_list, base_where (nur klimadaten). Output:
    (row_count, changed) - Full Load beim ersten Lauf, danach dt > Wasserzeichen (APPEND)."""
    state = get_latest_state(cur, "audit", name)
    watermark = state["watermark_value"] if state else None

    filters = [base_where] if base_where else []
    if watermark is not None:
        filters.append(f"{TIME_SERIES_WATERMARK_COLUMN} > '{watermark}'")
    where_clause = " WHERE " + " AND ".join(filters) if filters else ""

    if watermark is None:
        cur.execute(
            f"INSERT OVERWRITE TABLE {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}"
        )
        changed = True
    else:
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}{where_clause}")
        changed = cur.fetchone()[0] > 0
        if changed:
            cur.execute(
                f"INSERT INTO {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}"
            )

    if changed:
        cur.execute(f"SELECT MAX({TIME_SERIES_WATERMARK_COLUMN}) FROM {staging_table}")
        new_watermark = cur.fetchone()[0]
        if new_watermark is not None:
            record_state(cur, "audit", name, watermark_value=new_watermark)

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], changed


def _key_expr(alias, key_columns):
    """Input: Tabellen-Alias, Key-Spalten. Output: SQL-Ausdruck - Business-Key als EIN String."""
    cols = ", ".join(f"CAST({alias}.{c} AS STRING)" for c in key_columns)
    return f"CONCAT_WS('||', {cols})"


def _table_exists(cur, table_name):
    """Input: table_name. Output: bool."""
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    return len(cur.fetchall()) > 0


def _is_iceberg_table(cur, table_name):
    """Input: table_name. Output: bool - liest table_type aus DESCRIBE FORMATTED."""
    cur.execute(f"DESCRIBE FORMATTED {table_name}")
    for row in cur.fetchall():
        key = (row[1] or "").strip() if len(row) > 1 else ""
        if key == "table_type":
            value = (row[2] or "").strip() if len(row) > 2 else ""
            return value.upper() == "ICEBERG"
    return False


def ensure_iceberg_audit_table(cur, audit_table_name, staging_table):
    """Input: audit_table_name, staging_table. Output: legt audit_table_name frisch
    als Iceberg an, falls nicht vorhanden; RuntimeError, falls sie noch Parquet ist
    (Migration: src/utils/migrate_audit_tables_to_iceberg.py)."""
    if not _table_exists(cur, audit_table_name):
        cur.execute(
            f"CREATE TABLE {audit_table_name} STORED BY ICEBERG "
            f"TBLPROPERTIES('format-version'='2') AS SELECT * FROM {staging_table} WHERE 1=0"
        )
        return

    if not _is_iceberg_table(cur, audit_table_name):
        raise RuntimeError(
            f"{audit_table_name} existiert noch als Parquet-Tabelle, braucht fuer "
            "MERGE INTO/DELETE aber Iceberg. Bitte einmalig "
            "'.venv/Scripts/python.exe src/utils/migrate_audit_tables_to_iceberg.py' "
            "ausfuehren und die Pipeline danach erneut starten."
        )


def audit_table_keyed_snapshot(cur, name, staging_table, audit_table_name, select_list, base_where, key_columns):
    """Input: staging_table, select_list, base_where, key_columns (bauland/bevoelkerungzahlen).
    Output: (row_count, changed) - zeilengenaues Merge gegen die Iceberg-Audit-Tabelle:
    Hash je Business-Key vs. gruppe3_etl_row_state ermittelt betroffene Keys, dann
    DELETE (echt geloescht) + MERGE INTO (update/insert), s. ADR.md."""
    all_columns = get_columns(cur, staging_table)
    key_expr_plain = "CONCAT_WS('||', " + ", ".join(f"CAST({c} AS STRING)" for c in key_columns) + ")"
    hash_expr = "fnv_hash(CONCAT_WS('|', " + ", ".join(f"CAST({c} AS STRING)" for c in all_columns) + "))"

    cur.execute(
        f"SELECT row_key, MIN(row_hash) AS row_hash FROM ("
        f"SELECT {key_expr_plain} AS row_key, {hash_expr} AS row_hash FROM {staging_table}"
        f") t GROUP BY row_key"
    )
    current_hashes = {row_key: str(row_hash) for row_key, row_hash in cur.fetchall()}

    previous_hashes = get_latest_row_hashes(cur, name)

    changed_keys = [key for key, h in current_hashes.items() if previous_hashes.get(key) != h]
    deleted_keys = [key for key in previous_hashes if key not in current_hashes]
    keys_to_replace = changed_keys + deleted_keys

    if not keys_to_replace:
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    ensure_changed_keys_table(cur)
    load_changed_keys(cur, keys_to_replace)

    where_clause = f" WHERE {base_where}" if base_where else ""
    key_expr_audit = _key_expr("t", key_columns)
    key_expr_staging = _key_expr("s", key_columns)
    key_expr_src = _key_expr("src", key_columns)

    # Impala-Iceberg-DELETE braucht "DELETE <alias> FROM <table> <alias> WHERE ...".
    cur.execute(
        f"DELETE t FROM {audit_table_name} t WHERE {key_expr_audit} IN ("
        f"SELECT c.row_key FROM {CHANGED_KEYS_TABLE} c "
        f"LEFT ANTI JOIN (SELECT * FROM {staging_table}{where_clause}) s "
        f"ON c.row_key = {key_expr_staging}"
        f")"
    )

    non_key_columns = [c for c in all_columns if c not in key_columns]
    update_set = ", ".join(f"{c} = src.{c}" for c in non_key_columns)
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join(f"src.{c}" for c in all_columns)
    cur.execute(
        f"MERGE INTO {audit_table_name} t USING ("
        f"SELECT {select_list} FROM {staging_table} s "
        f"LEFT SEMI JOIN {CHANGED_KEYS_TABLE} c ON {key_expr_staging} = c.row_key"
        f"{where_clause}"
        f") src ON {key_expr_audit} = {key_expr_src} "
        f"WHEN MATCHED THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )

    record_row_hashes(
        cur, name,
        [(k, current_hashes[k]) for k in changed_keys] + [(k, None) for k in deleted_keys],
    )

    row_count_staging, content_hash = content_signature(cur, staging_table, all_columns)
    record_state(cur, "audit", name, content_hash=content_hash, row_count=row_count_staging)

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], True


def audit_table_snapshot(cur, name, staging_table, audit_table_name, select_list, base_where):
    """Input: staging_table, select_list, base_where (gemeinden). Output:
    (row_count, changed) - Inhalts-Pruefsumme vergleichen, bei Aenderung Full
    Refresh (INSERT OVERWRITE); kein zeilengenauer Merge (kein verlaesslicher Key)."""
    columns = get_columns(cur, staging_table)
    row_count_staging, content_hash = content_signature(cur, staging_table, columns)

    state = get_latest_state(cur, "audit", name)
    if state and state["content_hash"] == content_hash:
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    where_clause = f" WHERE {base_where}" if base_where else ""
    cur.execute(
        f"INSERT OVERWRITE TABLE {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}"
    )
    record_state(cur, "audit", name, content_hash=content_hash, row_count=row_count_staging)

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], True


def audit_table(cur, name, audit_rules):
    """Input: name, audit_rules. Output: (row_count, changed) - waehlt Strategie
    (Wasserzeichen/Keyed-Merge/Snapshot) und legt Audit-Tabelle ggf. an."""
    staging_table = STAGING_TABLES[name]
    audit_table_name = AUDIT_TABLES[name]
    rule = audit_rules[name]

    if name in KEY_COLUMNS:
        ensure_iceberg_audit_table(cur, audit_table_name, staging_table)
    else:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {audit_table_name} LIKE {staging_table} STORED AS PARQUET")

    columns = get_columns(cur, staging_table)
    select_list = build_select_list(columns, rule["columns"])

    if name in TIME_SERIES_TABLES:
        return audit_table_incremental(cur, name, staging_table, audit_table_name, select_list, rule["where"])
    if name in KEY_COLUMNS:
        return audit_table_keyed_snapshot(
            cur, name, staging_table, audit_table_name, select_list, rule["where"], KEY_COLUMNS[name]
        )
    return audit_table_snapshot(cur, name, staging_table, audit_table_name, select_list, rule["where"])


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    ensure_state_table(cur)
    ensure_row_state_table(cur)
    print(f"Datenbank: {DATABASE}\n")

    audit_rules = build_audit_rules(cur)

    for name in STAGING_TABLES:
        print(f"Audit {STAGING_TABLES[name]} -> {AUDIT_TABLES[name]} ...")
        row_count, changed = audit_table(cur, name, audit_rules)
        if changed:
            print(f"  -> OK ({row_count} Zeilen, aktualisiert)")
        else:
            print(f"  -> OK ({row_count} Zeilen, unveraendert - Lauf uebersprungen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
