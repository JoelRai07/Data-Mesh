"""
WAP-PATTERN (Write-Audit-Publish), STUFE 2: staging table -> audit table.

Uebernimmt die Daten aus den Staging-Tabellen (gruppe3_staging_*, unbereinigte
1:1-Kopie des Source Systems, s. pipeline_default_to_staging.py) und wendet die fachlich
vereinbarten Bereinigungsregeln an. Alle anderen Spalten werden unveraendert
uebernommen.

Bereinigungsregeln je Tabelle:
  - gruppe3_staging_klimadaten:
      nur Zeilen mit country = 'Germany'
      city:      bekannte englische Stadtnamen 1:1 auf die deutsche
                 Schreibweise gemappt (s. CITY_NAME_CORRECTIONS, z.B.
                 "Munich" -> "Muenchen"), danach Umlaute -> ae/oe/ue (z.B.
                 "Düsseldorf" -> "Duesseldorf"), damit Staedtenamen im
                 selben Format vorliegen wie
                 gruppe3_staging_gemeinden.municipality_name (fuer einen
                 spaeteren Join ueber den Namen statt ueber Koordinaten)
      latitude/longitude: vom Himmelsrichtungs-Format ("53.84N", "9.55E")
                         in dasselbe Komma-Dezimalformat wie
                         gruppe3_staging_gemeinden ueberfuehrt (z.B. "53.84N"
                         -> "53,84"), s. compass_to_signed_decimal. S/W wird
                         dabei zu einem negativen Vorzeichen (keine
                         Verzerrung: reines Abschneiden des Buchstabens ohne
                         Vorzeichenwechsel wuerde S/W-Koordinaten faelschlich
                         als positiv darstellen).
  - gruppe3_staging_bauland:
      kreis:   bekannte kaputte Werte 1:1 korrigiert (s. KREIS_CORRECTIONS),
               sonst alles ab dem ersten Komma entfernen (s. strip_after_comma)
      merkmal: TRIM + bekannte kaputte Werte 1:1 auf korrekte Schreibweise
               gemappt (s. BAULAND_MERKMAL_CORRECTIONS) - fuer diese zwei
               Werte ist die urspruengliche Schreibweise bekannt, daher hier
               Korrektur statt Entfernen wie sonst bei kaputten Zeichen
  - gruppe3_staging_bevoelkerungzahlen:
      kreis:   bekannte kaputte Werte 1:1 korrigiert (s. KREIS_CORRECTIONS,
               gemeinsam mit bauland genutzt), sonst alles ab dem ersten
               Komma entfernen (s. strip_after_comma)
  - gruppe3_staging_gemeinden:
      municipality_name: Umlaute -> ae/oe/ue (hier KEINE Beschaedigung, s.u.)
      district_kreis:    alles ab dem ersten Komma entfernen (z.B.
                         "Kiel, Kreisstadt" -> "Kiel"), danach Umlaute ->
                         ae/oe/ue (gleiche Spalte, keine Beschaedigung, s.u.)

WARUM ENTFERNEN STATT TRANSLITERIEREN (ae/oe/ue) BEI BAULAND/BEVOELKERUNGZAHLEN?
  In project_bauland/project_bevoelkerungzahlen ist ein Teil der Umlaute/des ß
  bereits beim urspruenglichen Import als Ersatzzeichen (U+FFFD, "St�dteregion
  Aachen" statt "Staedteregion Aachen") gespeichert - das Originalzeichen ist
  technisch nicht mehr vorhanden (bestaetigt: 101 von ~400 kreis_id-Werten
  betroffen, in JEDER Zeile dieser kreis_id, kein sauberer Wert zum Abgleich
  vorhanden - auch andere Gruppen, die dieselbe Quelle nutzen, haben exakt
  dieselbe Beschaedigung). Eine ae/oe/ue-Transliteration waere fuer die echten
  Umlaute korrekt, koennte aber die kaputten Zeichen nicht sinnvoll uebersetzen
  (welcher Umlaut es urspruenglich war, ist nicht mehr bekannt). Entscheidung:
  beide Faelle (echte Umlaute UND das kaputte Zeichen) werden hier einheitlich
  aus dem Wort entfernt statt geraten/transliteriert.
  project_gemeinden.municipality_name ist NICHT betroffen (Umlaute dort
  intakt) - dort bleibt die Transliteration ae/oe/ue wie bisher.

WARUM REINES IMPALA-SQL STATT SPARK:
  Wie schon bei der Staging-Pipeline: reine Zeilenfilter/String-Transformation
  ohne Aggregation/Join - dafuer braucht es keine eigene Verarbeitungs-Engine.
  "INSERT OVERWRITE TABLE ... SELECT ..." laeuft serverseitig in Impala; bei
  gruppe3_staging_klimadaten (8.6 Mio. Zeilen) waere der Umweg ueber
  Spark-JDBC + collect() + Batch-Inserts unnoetig langsam (s. Begruendung in
  pipeline_default_to_staging.py).

WARUM DIE SPALTENLISTE PER DESCRIBE ERMITTELT WIRD (statt hart codiert):
  gruppe3_staging_bevoelkerungzahlen hat 92 Spalten (id, kreis + 3 Spalten x
  30 Jahre). Alle Spalten von Hand aufzulisten waere fehleranfaellig - stattdessen
  wird die Spaltenliste der Quelltabelle per DESCRIBE gelesen und nur die
  Spalten mit Bereinigungsregel durch einen SQL-Ausdruck ersetzt, alle anderen
  Spalten bleiben ein einfacher Spaltenname.

INCREMENTAL LOADING (spiegelt die Einteilung aus pipeline_default_to_staging.py,
s. dortiger Modul-Docstring fuer die fachliche Begruendung; ALLE
Audit-Tabellen sind Apache-Iceberg-Tabellen, s. ADR.md):
  - gruppe3_staging_klimadaten (TIME_SERIES_TABLES): echte Zeitreihe. Es wird
    nur der bereits in der Staging-Stufe neu angehaengte Bereich (dt > eigenes
    Audit-Wasserzeichen) bereinigt und per INSERT INTO (APPEND) an
    gruppe3_audit_klimadaten angehaengt - nie mehr ein Full Rewrite der
    kompletten (potenziell 8,6 Mio. Zeilen grossen) Audit-Tabelle. Das
    Audit-Wasserzeichen wird bewusst UNABHAENGIG vom Staging-Wasserzeichen in
    einer eigenen State-Zeile (stage="audit") gefuehrt: Staging und Audit sind
    zwei getrennte Skript-Laeufe, die zeitlich auseinanderfallen koennen (z.B.
    Staging laeuft, Audit schlaegt fehl) - jede Stufe muss daher unabhaengig
    wissen, bis wohin SIE SELBST schon verarbeitet hat. Die Audit-Tabelle ist
    wie das Staging per Iceberg-Transform TRUNCATE(4, dt) nach Jahr
    partitioniert.
  - bauland/bevoelkerungzahlen (KEY_COLUMNS): ECHTES zeilengenaues
    Incremental Merge per Business-Key, direkt als Iceberg DELETE + MERGE
    INTO gegen die Audit-Tabelle (audit_table_keyed_snapshot, s. dortige
    Funktions-Dokumentation). Die Iceberg-Audit-Tabelle selbst ist dabei der
    Vergleichsstand: der NULL-sichere Spaltenvergleich (t.col <=> src.col)
    im MERGE erkennt geaenderte Zeilen, ohne eine separate Zeilen-Hash-
    Historie zu pflegen. Als guenstiger Vor-Check, ob ueberhaupt etwas zu
    tun ist, dient dieselbe Tabellen-Pruefsumme (content_signature()), die
    auch gemeinden nutzt - unveraenderte Quellen ueberspringen DELETE+MERGE
    komplett.
  - gemeinden (TABLE_LEVEL_SNAPSHOT_TABLES): bewusst KEIN zeilengenauer
    Merge, sondern die reine Tabellen-Pruefsumme (audit_table_snapshot(),
    content_signature() - "hat sich IRGENDWO etwas geaendert", nicht WELCHE
    Zeilen). Hat sich die Pruefsumme NICHT geaendert, wird der Schritt
    komplett uebersprungen; hat sie sich geaendert, wird die Audit-Tabelle
    per INSERT OVERWRITE voll ersetzt (auf Iceberg ein einzelner ATOMARER
    Snapshot-Commit). Grund: project_gemeinden hat weder einen amtlichen
    Schluessel noch garantiert NULL-freie municipality_name/postal_code-Werte
    (kaputtes CSV-Parsing) - ein zeilengenauer Merge wurde live getestet und
    erkannte selbst nach einem kompletten Datenbank-Reset im direkt folgenden
    Lauf trotz unveraenderter Quelle faelschlich eine Aenderung (vermutlich
    durch NULL-bedingte Kollisionen im Schluessel, s. Kommentar bei
    KEY_COLUMNS unten). Bei nur ca. 11.000 Zeilen ist der Verzicht auf
    Zeilengenauigkeit hier ohne spuerbaren Performance-Nachteil.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_staging_to_audit.py
"""
import os
import re

from db import get_connection
from etl_state import (
    ensure_state_table,
    ensure_iceberg_table_like,
    get_latest_state,
    record_state,
    content_signature,
    table_fingerprint,
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

# Iceberg-Partition-Transforms je Audit-Tabelle (gleiches Muster wie in
# pipeline_default_to_staging.py): klimadaten nach Jahres-Praefix des
# ISO-Datums partitioniert, passend zum Wasserzeichen-Filter "dt > '...'".
ICEBERG_PARTITION_SPECS = {
    "klimadaten": "PARTITIONED BY SPEC (TRUNCATE(4, dt))",
}

# Business-Keys je Snapshot-Tabelle, fuer das zeilengenaue Incremental Merge
# in audit_table_keyed_snapshot(). NUR fuer Tabellen MIT einem hinreichend
# verlaesslichen Schluessel - bevoelkerungzahlen (id = amtlicher
# Regionalschluessel) und bauland (kreis_id+jahr+merkmal, s.
# datenmodell_begruendung.md).
#
# gemeinden ABSICHTLICH NICHT hier drin (mehr dazu unten bei
# TABLE_LEVEL_SNAPSHOT_TABLES): project_gemeinden hat weder einen amtlichen
# Schluessel noch garantiert NULL-freie municipality_name/postal_code-Werte
# (kaputtes CSV-Parsing, s. Modul-Docstring oben). CONCAT_WS() ueberspringt
# NULL-Werte beim Zusammensetzen des Keys - bei NULLs in einer der beiden
# Spalten koennten dadurch mehrere fachlich verschiedene Zeilen auf denselben
# row_key kollabieren (nicht nur die 3 bestaetigten Namens-Duplikate). Live
# beobachtet: selbst nach einem kompletten Datenbank-Reset (also garantiert
# OHNE Altlasten aus einem frueheren, noch nicht deterministischen Hash-Stand)
# wurde gemeinden im direkt folgenden zweiten Lauf trotz unveraenderter Quelle
# erneut als "geaendert" erkannt - ein Hinweis auf genau so ein
# Kollisions-Cluster, das per Definition nicht zuverlaessig ueber einen
# einzelnen Business-Key stabilisierbar ist. Statt hier weiter an einem
# Schluessel herumzudoktern, der nachweislich nicht tragfaehig ist, faellt
# gemeinden bewusst auf die robustere, tabellenweite Pruefsumme zurueck
# (audit_table_snapshot(), dieselbe Technik wie am Anfang dieses Projekts) -
# bei nur ~11.000 Zeilen ist der Vollkosten-Nachteil eines gelegentlichen
# Full Refresh ohnehin vernachlaessigbar.
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

    # Ebene 1 (s. etl_state.py): Snapshot-Fingerprint der Staging-Tabelle
    # (immer Iceberg) - unveraendert seit dem letzten Audit-Lauf heisst:
    # garantiert keine neuen Zeilen, COUNT- und Append-Schritt entfallen.
    staging_fp = table_fingerprint(cur, staging_table)
    if (
        staging_fp is not None
        and state is not None
        and state["table_fingerprint"] == staging_fp
    ):
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

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
            record_state(
                cur, "audit", name,
                watermark_value=new_watermark, table_fingerprint=staging_fp,
            )
    elif state is not None and staging_fp is not None and state["table_fingerprint"] != staging_fp:
        # Fingerprint gewechselt, aber keine Zeilen hinter dem Wasserzeichen
        # (z.B. Compaction des Staging) -> nur Fingerprint fortschreiben,
        # damit Ebene 1 beim naechsten Lauf wieder greift.
        record_state(
            cur, "audit", name,
            watermark_value=watermark, table_fingerprint=staging_fp,
        )

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], changed


def audit_table_keyed_snapshot(cur, name, staging_table, audit_table_name, column_rules, base_where, key_columns):
    """
    Echtes ZEILENGENAUES Incremental Merge fuer Snapshot-Tabellen MIT
    hinreichend verlaesslichem Business-Key (bauland, bevoelkerungzahlen,
    s. KEY_COLUMNS - gemeinden bewusst NICHT, s. dortiger Kommentar).

    Vorgehen (komplett serverseitig, die Iceberg-Audit-Tabelle selbst ist
    der Vergleichsstand - keine separate Zeilen-Hash-Historie mehr noetig):

    1) Guenstiger Vor-Check auf Tabellenebene: content_signature() der
       Staging-Tabelle gegen den zuletzt aufgezeichneten Stand
       (gruppe3_etl_state). Unveraendert -> DELETE+MERGE komplett
       uebersprungen (der Normalfall bei taeglichen Laeufen ohne neue
       Quelldaten).
    2) DELETE: Keys, die in der Audit-Tabelle stehen, aber in der (ggf. per
       base_where gefilterten) Staging-Tabelle nicht mehr vorkommen, werden
       zeilengenau entfernt. NOT IN statt LEFT ANTI JOIN: Impala lehnt die
       Join-Form des Iceberg-DELETE in bestimmten Faellen ab (live
       getestet, "For deleting every row, please use TRUNCATE"); der
       CONCAT_WS-Key ist nie NULL, damit ist NOT IN hier NULL-sicher.
    3) MERGE INTO: bereits bekannte Keys werden per UPDATE SET aktualisiert
       - aber NUR, wenn sich mindestens eine Nicht-Key-Spalte tatsaechlich
       unterscheidet (NULL-sicherer Vergleich "NOT (t.col <=> src.col AND
       ...)"; mit "=" statt "<=>" wuerde eine Zeile mit unveraendertem
       NULL-Wert bei jedem Lauf unnoetig neu geschrieben). Neue Keys werden
       per INSERT ergaenzt. Unveraenderte Zeilen fasst keines der beiden
       Statements an - Iceberg schreibt nur Delta-Dateien fuer die
       tatsaechlich betroffenen Zeilen (merge-on-read).

    DUPLIKAT-KEYS IN DER QUELLE: default.project_bauland enthaelt 40 komplett
    leere Import-Zeilen (kreis_id NULL, merkmal ''), die alle auf denselben
    Business-Key kollabieren, project_bevoelkerungzahlen 2 Leerzeilen mit
    id='' (live verifiziert; ansonsten sind beide Keys eindeutig). Ein MERGE
    mit mehreren Quellzeilen fuer denselben Ziel-Key bricht in Impala hart ab
    ("Duplicate row found") - deshalb wird die MERGE-Quelle deterministisch
    auf EINE Zeile je Key verdichtet: GROUP BY ueber die (bereinigten)
    Key-Ausdruecke, MIN() ueber jede Nicht-Key-Spalte. MIN() ist ein
    deterministisches Aggregat: unabhaengig von der Scan-Reihenfolge liefert
    es fuer dieselbe Menge an Werten IMMER dasselbe Ergebnis (Impala
    garantiert ohne ORDER BY keine stabile Zeilenreihenfolge). Fuer die
    fachlich relevanten (eindeutigen) Keys aendert das Aggregat nichts -
    es gibt je Key genau eine Zeile. Eine analytische Alternative
    (ROW_NUMBER() im USING-Subquery) scheitert an einem Impala-Planner-
    Fehler ("Illegal reference to non-materialized tuple", live getestet).

    WARUM DIREKTES DELETE+MERGE STATT (wie ganz urspruenglich) CREATE NEW
    TABLE + RENAME-SWAP + DROP?
      Der Swap-Umweg existierte nur, weil Parquet/HDFS-Tabellen in Impala
      kein zeilengenaues UPDATE/DELETE/MERGE kennen und ein INSERT OVERWRITE
      waehrend eines laufenden Scans derselben Tabelle Daten haette
      verlieren koennen (Race zwischen Scan und Truncate). audit_table_name
      ist eine ICEBERG-Tabelle (s. ensure_iceberg_table_like) - Iceberg
      fuehrt DELETE/MERGE INTO als eigene, atomare Snapshot-Commits aus
      (kein Truncate, kein Torn Read), macht den kompletten
      CREATE+RENAME+DROP-Tanz damit ueberfluessig. Details/Begruendung: ADR.md.
    """
    all_columns = get_columns(cur, staging_table)
    state = get_latest_state(cur, "audit", name)

    # 1a) Ebene 1 (s. etl_state.py): Snapshot-Fingerprint der Staging-Tabelle
    # (immer Iceberg) - reine Metadaten-Abfrage. Unveraendert -> weder
    # Pruefsummen-Scan noch DELETE+MERGE (der Normalfall bei taeglichen
    # Laeufen ohne neue Quelldaten).
    staging_fp = table_fingerprint(cur, staging_table)
    if (
        staging_fp is not None
        and state is not None
        and state["table_fingerprint"] == staging_fp
    ):
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    # 1b) Ebene 2: Fingerprint hat gewechselt (oder fehlt noch) - erst jetzt
    # der Pruefsummen-Scan ueber die Zeilen. Gleicher Inhalt trotz neuem
    # Snapshot (z.B. Compaction) -> Fingerprint fortschreiben und fertig.
    row_count_staging, content_hash = content_signature(cur, staging_table, all_columns)
    if state and state["content_hash"] == content_hash:
        if staging_fp is not None and state["table_fingerprint"] != staging_fp:
            record_state(
                cur, "audit", name,
                content_hash=content_hash, row_count=state["row_count"],
                table_fingerprint=staging_fp,
            )
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    where_clause = f" WHERE {base_where}" if base_where else ""

    # Bereinigter Ausdruck je Spalte (Spalten ohne Regel bleiben der blosse
    # Spaltenname). Die Audit-Tabelle enthaelt die BEREINIGTEN Werte - Keys
    # muessen daher auf beiden Seiten in bereinigter Form verglichen werden.
    def cleaned(col):
        return column_rules.get(col, col)

    key_expr_audit = "CONCAT_WS('||', " + ", ".join(f"CAST(t.{c} AS STRING)" for c in key_columns) + ")"
    key_expr_src = "CONCAT_WS('||', " + ", ".join(f"CAST(src.{c} AS STRING)" for c in key_columns) + ")"
    key_expr_staging_clean = "CONCAT_WS('||', " + ", ".join(f"CAST({cleaned(c)} AS STRING)" for c in key_columns) + ")"

    # 2) Echt geloeschte Keys zeilengenau entfernen.
    cur.execute(
        f"DELETE t FROM {audit_table_name} t WHERE {key_expr_audit} NOT IN ("
        f"SELECT {key_expr_staging_clean} FROM {staging_table}{where_clause}"
        f")"
    )

    # 3) Neue/geaenderte Keys per MERGE INTO einfuegen/aktualisieren.
    non_key_columns = [c for c in all_columns if c not in key_columns]
    src_select = ", ".join(
        [f"{cleaned(c)} AS {c}" for c in key_columns]
        + [f"MIN({cleaned(c)}) AS {c}" for c in non_key_columns]
    )
    group_by = ", ".join(cleaned(c) for c in key_columns)
    unchanged_check = " AND ".join(f"t.{c} <=> src.{c}" for c in non_key_columns)
    update_set = ", ".join(f"{c} = src.{c}" for c in non_key_columns)
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join(f"src.{c}" for c in all_columns)
    cur.execute(
        f"MERGE INTO {audit_table_name} t USING ("
        f"SELECT {src_select} FROM {staging_table}{where_clause} GROUP BY {group_by}"
        f") src ON {key_expr_audit} = {key_expr_src} "
        f"WHEN MATCHED AND NOT ({unchanged_check}) THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )

    # Pruefsumme + Staging-Fingerprint fortschreiben: Grundlage fuer die
    # Vor-Checks oben (Ebene 1/2). should_skip_target_build() in
    # pipeline_audit_to_target.py prueft seinerseits die Snapshot-IDs der
    # AUDIT-Tabellen und sieht den soeben erzeugten neuen Snapshot.
    record_state(
        cur, "audit", name,
        content_hash=content_hash, row_count=row_count_staging,
        table_fingerprint=staging_fp,
    )

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], True


def audit_table_snapshot(cur, name, staging_table, audit_table_name, select_list, base_where):
    """
    Snapshot-Tabelle OHNE hinreichend verlaesslichen Business-Key (aktuell nur
    gemeinden, s. Kommentar bei TABLE_LEVEL_SNAPSHOT_TABLES): Inhalts-
    Pruefsumme der STAGING-Tabelle (nicht der Audit-Tabelle - der Audit-Schritt
    prueft, ob sich sein EINGANG veraendert hat) bilden und mit dem zuletzt
    aufgezeichneten Stand vergleichen. Unveraendert -> Audit-Lauf ueberspringen.
    Veraendert -> komplette Neuberechnung (INSERT OVERWRITE) mit den
    bestehenden AUDIT_RULES. Bewusst KEIN zeilengenauer Merge - anders als
    bauland/bevoelkerungzahlen fehlt hier ein Schluessel, der Zeilen
    zuverlaessig (NULL-frei, eindeutig) identifiziert. Auf Iceberg ist das
    INSERT OVERWRITE ein einzelner ATOMARER Snapshot-Commit - Leser sehen
    entweder den kompletten alten oder den kompletten neuen Stand, und der
    vorherige Stand bleibt per Time Travel abfragbar.
    """
    state = get_latest_state(cur, "audit", name)

    # Ebene 1 (s. etl_state.py): Snapshot-Fingerprint der Staging-Tabelle -
    # unveraendert -> Audit-Lauf ohne jeden Zeilen-Scan uebersprungen.
    staging_fp = table_fingerprint(cur, staging_table)
    if (
        staging_fp is not None
        and state is not None
        and state["table_fingerprint"] == staging_fp
    ):
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    # Ebene 2: Pruefsummen-Scan nur bei Fingerprint-Wechsel. Gleicher Inhalt
    # trotz neuem Snapshot (z.B. Compaction) -> Fingerprint fortschreiben.
    columns = get_columns(cur, staging_table)
    row_count_staging, content_hash = content_signature(cur, staging_table, columns)

    if state and state["content_hash"] == content_hash:
        if staging_fp is not None and state["table_fingerprint"] != staging_fp:
            record_state(
                cur, "audit", name,
                content_hash=content_hash, row_count=state["row_count"],
                table_fingerprint=staging_fp,
            )
        cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
        return cur.fetchone()[0], False

    where_clause = f" WHERE {base_where}" if base_where else ""
    cur.execute(
        f"INSERT OVERWRITE TABLE {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}"
    )
    record_state(
        cur, "audit", name,
        content_hash=content_hash, row_count=row_count_staging,
        table_fingerprint=staging_fp,
    )

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], True


def audit_table(cur, name, audit_rules):
    """Input: name, audit_rules. Output: (row_count, changed) - waehlt Strategie
    (Wasserzeichen/Keyed-Merge/Snapshot) und legt Audit-Tabelle ggf. an."""
    staging_table = STAGING_TABLES[name]
    audit_table_name = AUDIT_TABLES[name]
    rule = audit_rules[name]

    # ALLE Audit-Tabellen laufen als Iceberg (s. ADR.md): KEY_COLUMNS-Tabellen
    # brauchen es fuer DELETE/MERGE INTO, gemeinden/klimadaten profitieren von
    # atomarem INSERT OVERWRITE/Append, Snapshots/Time Travel und (klimadaten)
    # der Jahres-Partitionierung.
    ensure_iceberg_table_like(
        cur, audit_table_name, staging_table,
        partition_spec=ICEBERG_PARTITION_SPECS.get(name),
    )

    columns = get_columns(cur, staging_table)
    select_list = build_select_list(columns, rule["columns"])

    if name in TIME_SERIES_TABLES:
        return audit_table_incremental(cur, name, staging_table, audit_table_name, select_list, rule["where"])
    if name in KEY_COLUMNS:
        return audit_table_keyed_snapshot(
            cur, name, staging_table, audit_table_name, rule["columns"], rule["where"], KEY_COLUMNS[name]
        )
    return audit_table_snapshot(cur, name, staging_table, audit_table_name, select_list, rule["where"])


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    ensure_state_table(cur)
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
