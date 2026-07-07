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
s. dortiger Modul-Docstring fuer die fachliche Begruendung):
  - gruppe3_staging_klimadaten (TIME_SERIES_TABLES): echte Zeitreihe. Es wird
    nur der bereits in der Staging-Stufe neu angehaengte Bereich (dt > eigenes
    Audit-Wasserzeichen) bereinigt und per INSERT INTO (APPEND) an
    gruppe3_audit_klimadaten angehaengt - nie mehr ein Full Rewrite der
    kompletten (potenziell 8,6 Mio. Zeilen grossen) Audit-Tabelle. Das
    Audit-Wasserzeichen wird bewusst UNABHAENGIG vom Staging-Wasserzeichen in
    einer eigenen State-Zeile (stage="audit") gefuehrt: Staging und Audit sind
    zwei getrennte Skript-Laeufe, die zeitlich auseinanderfallen koennen (z.B.
    Staging laeuft, Audit schlaegt fehl) - jede Stufe muss daher unabhaengig
    wissen, bis wohin SIE SELBST schon verarbeitet hat.
  - bauland/bevoelkerungzahlen (KEY_COLUMNS): ECHTES zeilengenaues
    Incremental Merge per Business-Key (audit_table_keyed_snapshot, s.
    dortige Funktions-Dokumentation fuer das genaue Vorgehen und die
    CREATE-NEW-SWAP-DROP-Technik, die noetig ist, weil Impala weder UPDATE
    noch MERGE kennt). Kurzfassung: Jede Zeile bekommt ueber ihren
    fachlichen Schluessel einen Inhalts-Hash; nur Zeilen mit neuem/
    geaendertem/verschwundenem Hash werden neu bereinigt bzw. entfernt, alle
    unveraenderten Zeilen werden unangetastet aus der bisherigen Audit-Tabelle
    uebernommen.
  - gemeinden (TABLE_LEVEL_SNAPSHOT_TABLES): bewusst KEIN zeilengenauer
    Merge, sondern die reine Tabellen-Pruefsumme (audit_table_snapshot(),
    content_signature() - "hat sich IRGENDWO etwas geaendert", nicht WELCHE
    Zeilen). Grund: project_gemeinden hat weder einen amtlichen Schluessel
    noch garantiert NULL-freie municipality_name/postal_code-Werte (kaputtes
    CSV-Parsing) - ein zeilengenauer Merge wurde live getestet und erkannte
    selbst nach einem kompletten Datenbank-Reset im direkt folgenden Lauf
    trotz unveraenderter Quelle faelschlich eine Aenderung (vermutlich durch
    NULL-bedingte Kollisionen im Schluessel, s. Kommentar bei KEY_COLUMNS
    unten). Bei nur ca. 11.000 Zeilen ist der Verzicht auf Zeilengenauigkeit
    hier ohne spuerbaren Performance-Nachteil.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_staging_to_audit.py
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

# Name des Basistabellen-Themas -> Staging-/Audit-Tabellenname
STAGING_TABLES = {
    "bauland": PREFIX + "staging_bauland",
    "bevoelkerungzahlen": PREFIX + "staging_bevoelkerungzahlen",
    "gemeinden": PREFIX + "staging_gemeinden",
    "klimadaten": PREFIX + "staging_klimadaten",
}
AUDIT_TABLES = {name: PREFIX + "audit_" + name for name in STAGING_TABLES}

# Nur klimadaten ist eine echte, verlaesslich anhaengende Zeitreihe (Spalte
# "dt") -> Wasserzeichen-Append. Der Rest sind Snapshots amtlicher
# Statistiken/Stammdaten ohne verlaesslichen Aenderungsindikator auf
# Zeilenebene -> Change Detection per Inhalts-Pruefsumme (s. Modul-Docstring).
TIME_SERIES_TABLES = {"klimadaten"}
TIME_SERIES_WATERMARK_COLUMN = "dt"
SNAPSHOT_TABLES = set(STAGING_TABLES) - TIME_SERIES_TABLES

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

# Snapshot-Tabellen ohne (hinreichend) verlaesslichen Business-Key -> Change
# Detection nur auf Tabellenebene (audit_table_snapshot()), kein zeilengenauer
# Merge. Aktuell nur gemeinden (s. Kommentar bei KEY_COLUMNS).
TABLE_LEVEL_SNAPSHOT_TABLES = SNAPSHOT_TABLES - set(KEY_COLUMNS)

# Umlaut -> ASCII-Ersatz, inkl. Grossschreibung. Fuer Spalten OHNE Beschaedigung
# (project_gemeinden.municipality_name), wo eine echte Transliteration moeglich ist.

# Fuer project_bauland/project_bevoelkerungzahlen: Umlaute + das kaputte
# Ersatzzeichen aus dem urspruenglichen Encoding-Fehler (U+FFFD) komplett
# entfernen statt transliterieren (s. Modul-Docstring - eine ae/oe/ue-
# Transliteration ist dort nicht sinnvoll, weil das kaputte Zeichen ohnehin
# nicht transliterierbar ist).

def trim(column):
    """SQL-Ausdruck: Leerzeichen vorne/hinten entfernen."""
    return f"TRIM({column})"


def strip_after_comma(column):
    """SQL-Ausdruck: alles ab dem ersten Komma (inkl. Komma) entfernen,
    z.B. "Kiel, Kreisstadt" -> "Kiel". Ergebnis wird zusaetzlich getrimmt."""
    return f"TRIM(SPLIT_PART({column}, ',', 1))"


# Umlaut -> ASCII-Ersatz (Kleinschreibung + Grossschreibung). Nur fuer Spalten
# OHNE Beschaedigung (s. Modul-Docstring), wo eine echte Transliteration
# sinnvoll ist - project_gemeinden.municipality_name.
UMLAUT_REPLACEMENTS = [
    ("ä", "ae"), ("ö", "oe"), ("ü", "ue"),
    ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue"),
]


def transliterate_umlauts(column):
    """SQL-Ausdruck: jeden Umlaut im Wert durch die ASCII-Schreibweise
    ersetzen (ä->ae, ö->oe, ü->ue, jeweils auch grossgeschrieben)."""
    expr = column
    for umlaut, replacement in UMLAUT_REPLACEMENTS:
        expr = f"REPLACE({expr}, '{umlaut}', '{replacement}')"
    return expr


def compass_to_signed_decimal(column, negative_letter):
    """SQL-Ausdruck: Koordinate vom Himmelsrichtungs-Format der Klimadaten
    ("53.84N", "9.55E", Punkt als Dezimaltrennzeichen, Himmelsrichtung als
    Suffix) in dasselbe Format wie gruppe3_staging_gemeinden.latitude/
    longitude ueberfuehren (Komma als Dezimaltrennzeichen, kein Buchstabe).

    negative_letter ist der Buchstabe, der eine negative Koordinate bedeutet
    ('S' fuer latitude, 'W' fuer longitude) - dieser dreht das Vorzeichen um,
    der jeweils andere (N bzw. O) wird nur entfernt. Ohne diese Vorzeichen-
    Behandlung wuerde ein reines Entfernen des Buchstabens S/W-Koordinaten
    verzerren (aus "5.63S" wuerde "5,63" statt korrekt "-5,63")."""
    numeric = f"CAST(REGEXP_REPLACE({column}, '[A-Z]', '') AS DOUBLE)"
    signed = f"CASE WHEN {column} LIKE '%{negative_letter}' THEN -{numeric} ELSE {numeric} END"
    return f"REPLACE(CAST({signed} AS STRING), '.', ',')"


# gruppe3_staging_bauland.merkmal: bekannte kaputte Werte (Ersatzzeichen
# U+FFFD, s. Modul-Docstring), fuer die die urspruengliche Schreibweise
# bekannt ist -> 1:1 auf die korrekte Schreibweise mappen statt die
# kaputten Zeichen nur zu entfernen.
BAULAND_MERKMAL_CORRECTIONS = {
    "Ver�u�erungsf�lle von Bauland": "Veraeusserungsfaelle von Bauland",
    "Ver�u�erte Baulandfl�che": "Veraeusserte Baulandflaeche",
}


def fix_known_values(expr, corrections, else_expr=None):
    """SQL-Ausdruck: wenn expr exakt einem bekannten kaputten Wert entspricht,
    die korrekte Schreibweise einsetzen; sonst else_expr (standardmaessig expr).

    WICHTIG: expr muss bereits die Form haben, in der die kaputten Werte in
    corrections erwartet werden (z.B. inkl. TRIM oder strip_after_comma) -
    sonst greift der Vergleich nie (s. Bug: kreis-Rohwerte wie "  Luebeck,
    kreisfreie Stadt" matchen nicht gegen den blossen Ortsnamen "Luebeck")."""
    if else_expr is None:
        else_expr = expr
    cases = " ".join(
        f"WHEN {expr} = '{bad}' THEN '{good}'" for bad, good in corrections.items()
    )
    return f"CASE {cases} ELSE {else_expr} END"


# kreis (bauland + bevoelkerungzahlen): bekannte kaputte Werte (Ersatzzeichen
# U+FFFD, s. Modul-Docstring) -> 1:1 auf die korrekte Schreibweise mappen
# statt die kaputten Zeichen nur zu entfernen. Beide Staging-Tabellen
# referenzieren dieselben deutschen Kreise/kreisfreien Staedte, daher ein
# gemeinsames Dict.
#
# Die korrekte Schreibweise wird, wo moeglich, automatisch aus drei
# Referenzlisten in src/utils/ ermittelt statt von Hand eingetragen (s.
# _resolve_kreis_correction): german_cities.txt (Staedte/Orte, OSM-Liste),
# german_regions.txt (Landkreise + kreisfreie Staedte) und
# german_states.txt (die 16 Bundeslaender). Das Ersatzzeichen steht fuer
# genau ein verlorenes Zeichen (Umlaut oder ß); der (Teil-)Name wird in
# diesen Listen gesucht und der gefundene echte Name danach zu ASCII
# transliteriert (ä/ö/ü/ß -> ae/oe/ue/ss).
_UMLAUT_ASCII = [
    ("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"),
    ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue"),
]

# Zeichen, die als Wortgrenze gelten, wenn ein zusammengesetzter Kreisname
# (z.B. "Rendsburg-Eckernfoerde") wortweise durchsucht wird, weil er als
# Ganzes keinem Referenzlisten-Eintrag entspricht.
_WORD_SPLIT_PATTERN = re.compile(r"([^A-Za-zÀ-ÖØ-öø-ÿ�]+)")


def _transliterate_umlauts_py(name):
    for umlaut, replacement in _UMLAUT_ASCII:
        name = name.replace(umlaut, replacement)
    return name


def _load_name_list(filename):
    path = os.path.join(os.path.dirname(__file__), "utils", filename)
    with open(path, encoding="utf-8") as f:
        return [line.strip().strip('"') for line in f if line.strip()]


# Gemeinsamer Namenspool aus Staedten, Kreisen/kreisfreien Staedten und
# Bundeslaendern - deckt zusammen alle Ebenen ab, auf denen die kaputten
# kreis-Werte tatsaechlich benannt sind (Stadt, Landkreis oder Bundesland).
# dict.fromkeys() statt set() entfernt Duplikate (z.B. kreisfreie Staedte,
# die sowohl in german_cities.txt als auch in german_regions.txt stehen),
# ohne die Reihenfolge zu veraendern - sonst wuerde z.B. "Luebeck" durch den
# doppelten Eintrag faelschlich als mehrdeutiger Treffer gelten.
_REFERENCE_NAMES = list(dict.fromkeys(
    _load_name_list("german_cities.txt")
    + _load_name_list("german_regions.txt")
    + _load_name_list("german_states.txt")
))


def _find_reference_match(pattern_str):
    """Sucht im Namenspool (Staedte + Kreise + Bundeslaender) genau einen
    Treffer fuer ein Regex-Pattern (mit '.' anstelle des Ersatzzeichens
    U+FFFD). Bei mehreren Treffern wird der mit einem echten deutschen
    Sonderzeichen an der Ersatzstelle bevorzugt (z.B. matcht "M.nster"
    sowohl "Munster" als auch "Muenster" - da das Ersatzzeichen fuer einen
    verlorenen Umlaut steht, ist der Treffer mit Umlaut hier die richtige
    Wahl; bleibt es danach weiterhin mehrdeutig, gilt der Name als nicht
    gefunden)."""
    pattern = re.compile("^" + pattern_str + "$")
    matches = [name for name in _REFERENCE_NAMES if pattern.match(name)]
    if len(matches) > 1:
        matches = [m for m in matches if re.search(r"[äöüßÄÖÜ]", m)]
    return matches[0] if len(matches) == 1 else None


def _resolve_kreis_correction(bad_name):
    """Ermittelt fuer einen kaputten Kreisnamen (mit Ersatzzeichen U+FFFD
    fuer verlorene Umlaute/ß) die korrekte Schreibweise ueber die
    Referenzlisten (Staedte/Kreise/Bundeslaender) und transliteriert sie zu
    ASCII. Erst wird der ganze Name gesucht (z.B. "Rotenburg (Wuemme)"),
    dann - falls kein Treffer - Wort fuer Wort (z.B. bei zusammengesetzten
    Kreisnamen wie "Rendsburg-Eckernfoerde", wo nur "Eckernfoerde" allein
    als eigener Eintrag vorkommt). Gibt None zurueck, wenn sich keine
    Entsprechung finden laesst (z.B. Namen von Berliner Bezirken oder
    laengst aufgeloesten Landkreisen, die in keiner der drei Listen mehr
    auftauchen)."""
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


# Kaputte Namen, fuer die keine der drei Referenzlisten einen Eintrag hat -
# entweder Berliner Bezirke (Berlin steht in german_regions.txt nur als
# Ganzes, ohne Bezirksebene) oder laengst im Zuge von Kreisgebietsreformen
# aufgeloeste Landkreise (Sachsen 2008, Sachsen-Anhalt 2007,
# Mecklenburg-Vorpommern 2011) - hier bleibt die korrekte Schreibweise von
# Hand gepflegt.
MANUAL_KREIS_CORRECTIONS = {
    "St�dteregion Aachen": "Staedteregion Aachen",
    "Berlin-Treptow-K�penick": "Berlin-Treptow-Koepenick",
    "Berlin-Neuk�lln": "Berlin-Neukoelln",
    "Wei�eritzkreis": "Weisseritzkreis",  # aufgeloest 2008 (Sachsen)
    "S�chsische Schweiz": "Saechsische Schweiz",  # aufgeloest 2008 (Sachsen)
    "B�rdekreis": "Boerdekreis",  # aufgeloest 2007 (Sachsen-Anhalt)
    "R�gen": "Ruegen",  # aufgeloest 2011 (Mecklenburg-Vorpommern)
    "Landkreis M�ritz": "Landkreis Mueritz",  # aufgeloest 2011 (Mecklenburg-Vorpommern)
}


def _discover_bad_kreis_values(cur):
    """Ermittelt zur Laufzeit die tatsaechlich in gruppe3_staging_bauland UND
    gruppe3_staging_bevoelkerungzahlen vorkommenden kreis-Werte mit dem
    Ersatzzeichen U+FFFD - statt eine feste Liste bereits bekannter kaputter
    Werte im Code zu pflegen, die bei neuen Staging-Laeufen veralten koennte.
    TRIM + SPLIT_PART(..., ',', 1) entspricht exakt strip_after_comma(): nur
    so liegen die entdeckten Werte in derselben Form vor, in der
    fix_known_values() sie spaeter vergleicht (s. dortige Warnung)."""
    bad_values = set()
    for table in (STAGING_TABLES["bauland"], STAGING_TABLES["bevoelkerungzahlen"]):
        cur.execute(
            f"SELECT DISTINCT TRIM(SPLIT_PART(kreis, ',', 1)) FROM {table} "
            "WHERE kreis LIKE '%�%'"
        )
        bad_values.update(row[0] for row in cur.fetchall() if row[0])
    return sorted(bad_values)


def _build_kreis_corrections(cur):
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


# gruppe3_staging_klimadaten.city: manche deutschen Staedte sind unter ihrem
# englischen Namen erfasst statt unter dem deutschen (z.B. "Munich" statt
# "München") - 1:1 auf die (bereits ASCII-transliterierte) deutsche
# Schreibweise gemappt, damit der Name zu
# gruppe3_staging_gemeinden.municipality_name passt (s. Modul-Docstring,
# Namens-Join in pipeline_audit_to_target.py).
CITY_NAME_CORRECTIONS = {
    "Munich": "Muenchen",
    "Cologne": "Koeln",
    "Hanover": "Hannover",
    "Nuremberg": "Nuernberg",
    "Brunswick": "Braunschweig",
    "Ratisbon": "Regensburg",
}


# Je Thema: SQL-Ausdruck je zu bereinigender Spalte + optionaler WHERE-Filter.
# Wird erst zur Laufzeit gebaut (build_audit_rules(cur), s.u.), weil
# KREIS_CORRECTIONS von den tatsaechlich in den Staging-Tabellen vorkommenden
# kaputten Werten abhaengt (s. _discover_bad_kreis_values) - dafuer wird
# bereits ein offener Cursor gebraucht, den es auf Modulebene noch nicht gibt.
def build_audit_rules(cur):
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
    cur.execute(f"DESCRIBE {table_name}")
    return [row[0] for row in cur.fetchall()]


def build_select_list(columns, column_rules):
    return ", ".join(
        f"{column_rules[col]} AS {col}" if col in column_rules else col
        for col in columns
    )


def audit_table_incremental(cur, name, staging_table, audit_table_name, select_list, base_where):
    """
    Echtes Incremental Load per Wasserzeichen fuer Zeitreihen-Tabellen
    (aktuell nur klimadaten): bereinigt beim ersten Lauf einmalig den
    kompletten Bestand (INSERT OVERWRITE), danach nur noch den Bereich
    dt > eigenem Audit-Wasserzeichen (INSERT INTO, ANHAENGEN statt
    ueberschreiben). Dieselben AUDIT_RULES/Bereinigungsausdruecke wie bisher,
    nur der WHERE-Filter kommt zusaetzlich dazu.
    """
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
        # Dieselben Filter wie der eigentliche INSERT (inkl. base_where, z.B.
        # country = 'Germany') - sonst wuerde "changed" auch bei neuen,
        # aber fachlich irrelevanten Zeilen (z.B. neue Messtage fuer
        # Nicht-Deutschland-Staedte) True liefern und einen nutzlosen
        # Leer-INSERT ausloesen.
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}{where_clause}")
        changed = cur.fetchone()[0] > 0
        if changed:
            cur.execute(
                f"INSERT INTO {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}"
            )

    # Wasserzeichen NUR bei tatsaechlicher Verarbeitung fortschreiben. Hier
    # doppelt wichtig: should_skip_target_build() in pipeline_audit_to_target.py
    # vergleicht das recorded_at der Audit-Eintraege mit dem letzten
    # Ziel-Build - ein Eintrag pro unveraendertem Lauf wuerde den teuren
    # Spark-Rebuild jedes Mal faelschlich ausloesen, der Skip wuerde nie
    # greifen. Bei changed=False bleibt der letzte Eintrag der gueltige
    # Stand; neue, aber fachlich irrelevante Staging-Zeilen (z.B. neue
    # Messtage ausserhalb Deutschlands) werden beim naechsten Lauf einfach
    # erneut guenstig per COUNT geprueft.
    if changed:
        cur.execute(f"SELECT MAX({TIME_SERIES_WATERMARK_COLUMN}) FROM {staging_table}")
        new_watermark = cur.fetchone()[0]
        if new_watermark is not None:
            record_state(cur, "audit", name, watermark_value=new_watermark)

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0], changed


def _key_expr(alias, key_columns):
    """SQL-Ausdruck: fachlicher Business-Key mehrerer Spalten als EIN
    String, fuer Join-Bedingungen zwischen Audit-/Staging-Tabelle."""
    cols = ", ".join(f"CAST({alias}.{c} AS STRING)" for c in key_columns)
    return f"CONCAT_WS('||', {cols})"


def _table_exists(cur, table_name):
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    return len(cur.fetchall()) > 0


def _is_iceberg_table(cur, table_name):
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


def ensure_iceberg_audit_table(cur, audit_table_name, staging_table):
    """
    Stellt sicher, dass audit_table_name als ICEBERG-Tabelle existiert - fuer
    das MERGE INTO/DELETE in audit_table_keyed_snapshot() zwingend
    erforderlich (Parquet/HDFS-Tabellen kennen kein zeilengenaues
    UPDATE/DELETE, s. ADR.md).
      1) Tabelle existiert noch nicht -> frisch als Iceberg anlegen.
      2) Tabelle existiert bereits als Iceberg -> nichts zu tun (idempotent,
         der Normalfall ab dem zweiten Lauf).
      3) Tabelle existiert noch als Parquet -> FEHLER mit klarer Anleitung,
         statt (wie in einer frueheren Version dieser Funktion) automatisch
         und lautlos zu migrieren. Eine Storage-Format-Migration ist ein
         einmaliger, strukturell heikler Vorgang (CTAS ueber die komplette
         Tabelle + Rename-Swap) - das gehoert NICHT in den taeglichen
         Pipeline-Lauf, wo ein Abbruch mitten in der Migration schwerer zu
         diagnostizieren waere als ein expliziter, separat gestarteter
         Migrationsschritt. Migration: s.
         src/utils/migrate_audit_tables_to_iceberg.py (einmalig auszufuehren,
         idempotent, mit sichtbarer Fortschrittsausgabe).
    """
    if not _table_exists(cur, audit_table_name):
        # "CREATE TABLE ... LIKE ... STORED BY ICEBERG" schlaegt fehl, wenn
        # die Quelltabelle selbst kein Iceberg ist ("cannot be cloned into
        # an Iceberg table") - deshalb stattdessen CTAS mit WHERE 1=0: uebernimmt
        # Spalten/Typen 1:1, aber keine Zeilen (die erste MERGE INTO in
        # audit_table_keyed_snapshot() befuellt die Tabelle).
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
    """
    Echtes ZEILENGENAUES Incremental Merge fuer Snapshot-Tabellen MIT
    hinreichend verlaesslichem Business-Key (bauland, bevoelkerungzahlen,
    s. KEY_COLUMNS - gemeinden bewusst NICHT, s. dortiger Kommentar).

    Vorgehen:
    1) Fuer jede Zeile der Staging-Tabelle einen Inhalts-Hash je Business-Key
       berechnen (server-seitig in Impala, FNV_HASH - dieselbe Technik wie
       content_signature(), nur JE ZEILE statt ueber die ganze Tabelle
       aggregiert) und mit dem zuletzt aufgezeichneten Hash je Key
       vergleichen (gruppe3_etl_row_state, s. etl_state.py). Diese
       Zeilenhistorie wird bewusst weiterhin eigenstaendig gepflegt (nicht
       nur aus der Iceberg-Audit-Tabelle abgeleitet) - sie wird auch
       ausserhalb dieser Funktion gebraucht, u.a. vom Incremental Scheduler.
    2) Keys, die neu sind, einen anderen Hash haben ODER komplett aus der
       Quelle verschwunden sind (geloescht) landen in "keys_to_replace".
       Ist diese Menge leer, ist WIRKLICH keine einzige Zeile betroffen ->
       Lauf ueberspringen.
    3) Sonst: die betroffenen Keys in die kurzlebige Hilfstabelle
       CHANGED_KEYS_TABLE schreiben und GENAU diese Zeilen direkt in der
       (jetzt Iceberg-)Audit-Tabelle bereinigen (s. WARUM-Abschnitt unten):
         a) DELETE fuer Keys, die zwar in keys_to_replace stehen, aber in der
            (ggf. per base_where gefilterten) Staging-Tabelle nicht mehr
            vorkommen - das sind die echt geloeschten Zeilen.
         b) MERGE INTO fuer alle noch vorhandenen betroffenen Keys: bereits
            bekannte Keys werden per UPDATE SET aktualisiert, neue Keys per
            INSERT ergaenzt - MERGE unterscheidet das automatisch anhand der
            Join-Bedingung, das ist in Python nicht mehr extra zu trennen.
       Unveraenderte Zeilen (Key nicht in CHANGED_KEYS_TABLE) werden von
       beiden Statements gar nicht erst angefasst.

    WARUM DIREKTES DELETE+MERGE STATT (wie ganz urspruenglich) CREATE NEW
    TABLE + RENAME-SWAP + DROP?
      Der Swap-Umweg existierte nur, weil Parquet/HDFS-Tabellen in Impala
      kein zeilengenaues UPDATE/DELETE/MERGE kennen und ein INSERT OVERWRITE
      waehrend eines laufenden Scans derselben Tabelle Daten haette
      verlieren koennen (Race zwischen Scan und Truncate). audit_table_name
      ist inzwischen eine ICEBERG-Tabelle (s. ensure_iceberg_audit_table) -
      Iceberg fuehrt DELETE/MERGE INTO als eigene, atomare Snapshot-Commits
      aus (kein Truncate, kein Torn Read), macht den kompletten
      CREATE+RENAME+DROP-Tanz damit ueberfluessig. Details/Begruendung: ADR.md.
    """
    all_columns = get_columns(cur, staging_table)
    key_expr_plain = "CONCAT_WS('||', " + ", ".join(f"CAST({c} AS STRING)" for c in key_columns) + ")"
    hash_expr = "fnv_hash(CONCAT_WS('|', " + ", ".join(f"CAST({c} AS STRING)" for c in all_columns) + "))"

    # GROUP BY + MIN() statt eines Python-Dicts ueber cur.fetchall(): selbst bei
    # den hier verwendeten, grundsaetzlich verlaesslichen Schluesseln
    # (bauland/bevoelkerungzahlen) ist eine deterministische Zusammenfuehrung
    # robuster als "die zuletzt gelesene Zeile gewinnt" - Impala garantiert
    # OHNE ORDER BY keine stabile Zeilenreihenfolge zwischen zwei Laeufen,
    # wodurch bei einem etwaigen Duplikat-Key der Hash zufaellig zwischen zwei
    # Werten haette hin- und herspringen koennen (erkannte "Aenderung" ohne
    # echte Aenderung an den Daten - genau dieses Verhalten wurde live bei
    # gemeinden beobachtet, s. Kommentar bei KEY_COLUMNS, weshalb gemeinden
    # aus diesem Merge-Pfad herausgenommen wurde). MIN()
    # ist ein deterministisches Aggregat: unabhaengig von der Scan-Reihenfolge
    # liefert es fuer dieselbe Menge an Hash-Werten IMMER denselben einen Wert.
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

    # a) Echt geloeschte Keys entfernen: in CHANGED_KEYS_TABLE, aber in der
    # (per base_where gefilterten) Staging-Tabelle nicht mehr vorhanden.
    # Impalas Iceberg-DELETE verlangt "DELETE <alias> FROM <table> <alias>
    # WHERE ..." - "DELETE FROM <table> <alias> WHERE ..." (ohne fuehrenden
    # Alias) ist ein Syntaxfehler.
    cur.execute(
        f"DELETE t FROM {audit_table_name} t WHERE {key_expr_audit} IN ("
        f"SELECT c.row_key FROM {CHANGED_KEYS_TABLE} c "
        f"LEFT ANTI JOIN (SELECT * FROM {staging_table}{where_clause}) s "
        f"ON c.row_key = {key_expr_staging}"
        f")"
    )

    # b) Betroffene, weiterhin vorhandene Keys aktualisieren/einfuegen.
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

    # Neue/geaenderte Keys mit ihrem aktuellen Hash aufzeichnen, geloeschte
    # Keys als Tombstone (row_hash=None) - sonst wuerden sie bei jedem
    # weiteren Lauf faelschlich erneut als "geloescht" erkannt (s.
    # get_latest_row_hashes-Doku in etl_state.py).
    record_row_hashes(
        cur, name,
        [(k, current_hashes[k]) for k in changed_keys] + [(k, None) for k in deleted_keys],
    )

    # Tabellen-Ebene weiterhin zusaetzlich pflegen: das ist, was
    # should_skip_target_build() in pipeline_audit_to_target.py liest, um zu
    # entscheiden, ob sich der Spark-Rebuild des Star-Schemas lohnt.
    row_count_staging, content_hash = content_signature(cur, staging_table, all_columns)
    record_state(cur, "audit", name, content_hash=content_hash, row_count=row_count_staging)

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
    zuverlaessig (NULL-frei, eindeutig) identifiziert.
    """
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
    staging_table = STAGING_TABLES[name]
    audit_table_name = AUDIT_TABLES[name]
    rule = audit_rules[name]

    # KEY_COLUMNS-Tabellen (bauland, bevoelkerungzahlen) brauchen DELETE/MERGE
    # INTO in audit_table_keyed_snapshot() -> ICEBERG statt Parquet (s. ADR.md).
    # Alle anderen Tabellen (gemeinden, klimadaten) werden nur per INSERT
    # OVERWRITE/INSERT INTO geschrieben - dafuer reicht weiterhin Parquet.
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
