"""
WAP-PATTERN (Write-Audit-Publish), STUFE 2: staging table -> audit table.

Uebernimmt die Daten aus den Staging-Tabellen (gruppe3_staging_*, unbereinigte
1:1-Kopie des Source Systems, s. pipeline_staging.py) und wendet die fachlich
vereinbarten Bereinigungsregeln an. Alle anderen Spalten werden unveraendert
uebernommen.

Bereinigungsregeln je Tabelle:
  - gruppe3_staging_klimadaten:
      nur Zeilen mit country = 'Germany'
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
  pipeline_staging.py).

WARUM DIE SPALTENLISTE PER DESCRIBE ERMITTELT WIRD (statt hart codiert):
  gruppe3_staging_bevoelkerungzahlen hat 83 Spalten (id, kreis + 3 Spalten x
  27 Jahre). Alle Spalten von Hand aufzulisten waere fehleranfaellig - stattdessen
  wird die Spaltenliste der Quelltabelle per DESCRIBE gelesen und nur die
  Spalten mit Bereinigungsregel durch einen SQL-Ausdruck ersetzt, alle anderen
  Spalten bleiben ein einfacher Spaltenname.

IMMER OVERWRITE, NIE DUPLIZIEREN:
  INSERT OVERWRITE TABLE ersetzt bei jedem Lauf den kompletten Tabelleninhalt
  (Full-Load-Pattern, analog zu pipeline_staging.py/pipeline_spark.py).

Ausfuehren:  .venv/Scripts/python.exe src/pipeline_audit.py
"""
from db import get_connection

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


def audit_table(cur, name):
    staging_table = STAGING_TABLES[name]
    audit_table_name = AUDIT_TABLES[name]
    rule = AUDIT_RULES[name]

    cur.execute(f"CREATE TABLE IF NOT EXISTS {audit_table_name} LIKE {staging_table} STORED AS PARQUET")

    columns = get_columns(cur, staging_table)
    select_list = build_select_list(columns, rule["columns"])
    where_clause = f" WHERE {rule['where']}" if rule["where"] else ""

    cur.execute(f"INSERT OVERWRITE TABLE {audit_table_name} SELECT {select_list} FROM {staging_table}{where_clause}")

    cur.execute(f"SELECT COUNT(*) FROM {audit_table_name}")
    return cur.fetchone()[0]


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    print(f"Datenbank: {DATABASE}\n")

    for name in STAGING_TABLES:
        print(f"Audit {STAGING_TABLES[name]} -> {AUDIT_TABLES[name]} ...")
        row_count = audit_table(cur, name)
        print(f"  -> OK ({row_count} Zeilen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
