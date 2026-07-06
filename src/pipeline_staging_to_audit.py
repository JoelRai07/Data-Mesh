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

DATABASE = "gruppe3"
PREFIX = "gruppe3_"

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
# U+FFFD, s. Modul-Docstring), fuer die die urspruengliche Schreibweise
# bekannt ist -> 1:1 auf die korrekte Schreibweise mappen statt die
# kaputten Zeichen nur zu entfernen. Beide Staging-Tabellen referenzieren
# dieselben deutschen Kreise/kreisfreien Staedte, daher ein gemeinsames Dict.
KREIS_CORRECTIONS = {
    "L�beck": "Luebeck",
    "Neum�nster": "Neumuenster",
    "Pl�n": "Ploen",
    "Rendsburg-Eckernf�rde": "Rendsburg-Eckernfoerde",
    "G�ttingen": "Goettingen",
    "Wolfenb�ttel": "Wolfenbuettel",
    "L�neburg": "Lueneburg",
    "L�chow-Dannenberg": "Luechow-Dannenberg",
    "Rotenburg (W�mme)": "Rotenburg (Wuemme)",
    "Osnabr�ck": "Osnabrueck",
    "D�sseldorf": "Duesseldorf",
    "M�nchengladbach": "Moenchengladbach",
    "M�lheim an der Ruhr": "Muelheim an der Ruhr",
    "K�ln": "Koeln",
    "St�dteregion Aachen": "Staedteregion Aachen",
    "D�ren": "Dueren",
    "M�nster": "Muenster",
    "G�tersloh": "Guetersloh",
    "H�xter": "Hoexter",
    "Minden-L�bbecke": "Minden-Luebbecke",
    "M�rkischer Kreis": "Maerkischer Kreis",
    "Bergstra�e": "Bergstrasse",
    "Gro�-Gerau": "Gross-Gerau",
    # zusaetzlich in gruppe3_staging_bevoelkerungzahlen.kreis gefunden:
    "Aschersleben-Sta�furt": "Aschersleben-Stassfurt",
    "Eifelkreis Bitburg-Pr�m": "Eifelkreis Bitburg-Pruem",
    "Berlin-Treptow-K�penick": "Berlin-Treptow-Koepenick",
    "Dessau-Ro�lau": "Dessau-Rosslau",
    "Kyffh�userkreis": "Kyffhaeuserkreis",
    "Wei�enburg-Gunzenhausen": "Weissenburg-Gunzenhausen",
    "M�nchen": "Muenchen",
    "G�ppingen": "Goeppingen",
    "Ha�berge": "Hassberge",
    "Wei�enfels": "Weissenfels",
    "B�blingen": "Boeblingen",
    "Saarbr�cken": "Saarbruecken",
    "Wei�eritzkreis": "Weisseritzkreis",
    "G�nzburg": "Guenzburg",
    "Werra-Mei�ner-Kreis": "Werra-Meissner-Kreis",
    "T�bingen": "Tuebingen",
    "F�rth": "Fuerth",
    "Gie�en": "Giessen",
    "Rh�n-Grabfeld": "Rhoen-Grabfeld",
    "Sch�nebeck": "Schoenebeck",
    "Erlangen-H�chstadt": "Erlangen-Hoechstadt",
    "Mei�en": "Meissen",
    "Unterallg�u": "Unterallgaeu",
    "N�rnberger Land": "Nuernberger Land",
    "Teltow-Fl�ming": "Teltow-Flaeming",
    "L�rrach": "Loerrach",
    "S�chsische Schweiz": "Saechsische Schweiz",
    "S�dliche Weinstra�e": "Suedliche Weinstrasse",
    "N�rnberg": "Nuernberg",
    "Berlin-Neuk�lln": "Berlin-Neukoelln",
    "Berlin-Tempelhof-Sch�neberg": "Berlin-Tempelhof-Schoeneberg",
    "Ostallg�u": "Ostallgaeu",
    "D�beln": "Doebeln",
    "Neustadt an der Weinstra�e": "Neustadt an der Weinstrasse",
    "R�gen": "Ruegen",
    "Vorpommern-R�gen": "Vorpommern-Ruegen",
    "M�hldorf a.Inn": "Muehldorf a.Inn",
    "M�rkisch-Oderland": "Maerkisch-Oderland",
    "Oberallg�u": "Oberallgaeu",
    "Mansfeld-S�dharz": "Mansfeld-Suedharz",
    "Landkreis M�ritz": "Landkreis Mueritz",
    "Kempten (Allg�u)": "Kempten (Allgaeu)",
    "G�rlitz": "Goerlitz",
    "Schw�bisch Hall": "Schwaebisch Hall",
    "G�strow": "Guestrow",
    "Bad D�rkheim": "Bad Duerkheim",
    "L�bau-Zittau": "Loebau-Zittau",
    "S�dwestpfalz": "Suedwestpfalz",
    "B�rdekreis": "Boerdekreis",
    "B�rde": "Boerde",
    "Riesa-Gro�enhain": "Riesa-Grossenhain",
    "W�rzburg": "Wuerzburg",
    "S�mmerda": "Soemmerda",
    "Baden-W�rttemberg": "Baden-Wuerttemberg",
    "Alt�tting": "Altoetting",
    "Rhein-Hunsr�ck-Kreis": "Rhein-Hunsrueck-Kreis",
    "Spree-Nei�e": "Spree-Neisse",
    "Zweibr�cken": "Zweibruecken",
    "Th�ringen": "Thueringen",
    "Bad T�lz-Wolfratshausen": "Bad Toelz-Wolfratshausen",
    "Eichst�tt": "Eichstaett",
    "F�rstenfeldbruck": "Fuerstenfeldbruck",
    "K�then": "Koethen",
    "S�chsische Schweiz-Osterzgebirge": "Saechsische Schweiz-Osterzgebirge",
}


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
AUDIT_RULES = {
    "bauland": {
        "columns": {
            "kreis": fix_known_values(strip_after_comma("kreis"), KREIS_CORRECTIONS),
            "merkmal": fix_known_values(trim("merkmal"), BAULAND_MERKMAL_CORRECTIONS),
        },
        "where": None,
    },
    "bevoelkerungzahlen": {
        "columns": {
            "kreis": fix_known_values(strip_after_comma("kreis"), KREIS_CORRECTIONS),
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
    String, fuer Vergleich/Join gegen CHANGED_KEYS_TABLE.row_key."""
    cols = ", ".join(f"CAST({alias}.{c} AS STRING)" for c in key_columns)
    return f"CONCAT_WS('||', {cols})"


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
       vergleichen (gruppe3_etl_row_state, s. etl_state.py).
    2) Keys, die neu sind, einen anderen Hash haben ODER komplett aus der
       Quelle verschwunden sind (geloescht) landen in "keys_to_replace".
       Ist diese Menge leer, ist WIRKLICH keine einzige Zeile betroffen ->
       Lauf ueberspringen.
    3) Sonst: die betroffenen Keys in die kurzlebige Hilfstabelle
       CHANGED_KEYS_TABLE schreiben und die neue Audit-Tabelle aus zwei
       Teilen zusammensetzen:
         a) alle BISHERIGEN Audit-Zeilen, deren Key NICHT betroffen ist
            (unangetastet uebernommen, kein erneutes Bereinigen noetig)
         b) frisch bereinigte Zeilen (dieselben AUDIT_RULES/select_list wie
            bisher) aus der Staging-Tabelle fuer GENAU die betroffenen Keys
       Geloeschte Keys fallen dabei automatisch weg: sie sind in (a)
       ausgeschlossen (Key ist betroffen) und liefern in (b) keine Treffer
       (Key existiert in der Staging-Tabelle nicht mehr).

    WARUM "CREATE NEW TABLE + RENAME-SWAP + DROP" STATT EINFACH
    INSERT OVERWRITE TABLE audit SELECT ... FROM audit ...?
      Impala kann nicht sicher aus einer Tabelle LESEN, waehrend dieselbe
      Tabelle per INSERT OVERWRITE geleert/ueberschrieben wird (Race
      zwischen dem laufenden Scan und dem Truncate-Schritt von OVERWRITE -
      im schlechtesten Fall werden 0 Zeilen gelesen, das Ergebnis waere dann
      nur noch der neue Teil, der alte Bestand waere verloren). Stattdessen
      wird das Merge-Ergebnis in eine NEUE Tabelle geschrieben
      (CREATE TABLE ... AS SELECT), und die Tabellen werden erst DANACH per
      ALTER TABLE ... RENAME TO getauscht. Reihenfolge bewusst so gewaehlt,
      dass zu keinem Zeitpunkt Daten verloren gehen koennten, falls der
      Prozess mittendrin abbricht: zuerst wird das ALTE Original umbenannt
      (nie geloescht, solange die neue Tabelle nicht sicher an ihrem Platz
      ist), erst zuletzt wird die alte Kopie geloescht.
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
    key_expr_audit = _key_expr("a", key_columns)
    key_expr_staging = _key_expr("s", key_columns)
    incoming_table = f"{audit_table_name}_incoming"
    old_table = f"{audit_table_name}_old"

    cur.execute(f"DROP TABLE IF EXISTS {incoming_table}")
    cur.execute(
        f"CREATE TABLE {incoming_table} STORED AS PARQUET AS "
        f"SELECT * FROM {audit_table_name} a "
        f"LEFT ANTI JOIN {CHANGED_KEYS_TABLE} c ON {key_expr_audit} = c.row_key "
        f"UNION ALL "
        f"SELECT {select_list} FROM {staging_table} s "
        f"LEFT SEMI JOIN {CHANGED_KEYS_TABLE} c ON {key_expr_staging} = c.row_key"
        f"{where_clause}"
    )

    cur.execute(f"DROP TABLE IF EXISTS {old_table}")
    cur.execute(f"ALTER TABLE {audit_table_name} RENAME TO {old_table}")
    cur.execute(f"ALTER TABLE {incoming_table} RENAME TO {audit_table_name}")
    cur.execute(f"DROP TABLE IF EXISTS {old_table}")

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


def audit_table(cur, name):
    staging_table = STAGING_TABLES[name]
    audit_table_name = AUDIT_TABLES[name]
    rule = AUDIT_RULES[name]

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

    for name in STAGING_TABLES:
        print(f"Audit {STAGING_TABLES[name]} -> {AUDIT_TABLES[name]} ...")
        row_count, changed = audit_table(cur, name)
        if changed:
            print(f"  -> OK ({row_count} Zeilen, aktualisiert)")
        else:
            print(f"  -> OK ({row_count} Zeilen, unveraendert - Lauf uebersprungen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
