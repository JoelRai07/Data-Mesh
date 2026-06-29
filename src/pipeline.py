"""
DELIVERABLE 2: Pipeline - befuellt das Datenmodell.

Liest aus den bereitgestellten Rohdaten (default.project_*) und schreibt in
unser Star-Schema in der Gruppen-Datenbank gruppe3 (gruppe3_dim_*, gruppe3_fact_*).
Die Bereinigung (Aggregat-Zeilen rausfiltern usw.) passiert direkt im SELECT.

Idempotent durch INSERT OVERWRITE: jeder Lauf ersetzt den Inhalt komplett,
es entstehen also keine Duplikate.

Hinweis: Dieselbe SQL-Logik wird spaeter in ein PySpark-Skript uebernommen
(spark.sql("...")). Hier nutzen wir Impala zum Entwickeln und Testen.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline.py
"""
import sys
from db import get_connection

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATABASE = "gruppe3"


# ---------------------------------------------------------------------------
# DIMENSIONEN
# ---------------------------------------------------------------------------

# dim_jahr: distinkte Jahre (2015-2024) aus den Bauland-Rohdaten.
fill_dim_jahr = """
INSERT OVERWRITE gruppe3_dim_jahr
SELECT DISTINCT
    CAST(jahr AS INT)                  AS jahr,
    CAST(FLOOR(jahr / 10) * 10 AS INT) AS jahrzehnt
FROM default.project_bauland
WHERE jahr IS NOT NULL
"""

# dim_kreis: ein Eintrag je Kreis (aus den Bevoelkerungs-Rohdaten).
# Bereinigung: nur echte Kreise (5-stelliger Schluessel), keine Aggregat-Zeilen
#   wie 'DG' (Deutschland) oder '01' (Bundesland).
# bundesland_id = die ersten 2 Stellen des Regionalschluessels (z.B. 01001 -> 01).
fill_dim_kreis = """
INSERT OVERWRITE gruppe3_dim_kreis
SELECT
    id                AS kreis_id,
    kreis             AS kreis_name,
    SUBSTR(id, 1, 2)  AS bundesland_id,
    CASE SUBSTR(id, 1, 2)
        WHEN '01' THEN 'Schleswig-Holstein'
        WHEN '02' THEN 'Hamburg'
        WHEN '03' THEN 'Niedersachsen'
        WHEN '04' THEN 'Bremen'
        WHEN '05' THEN 'Nordrhein-Westfalen'
        WHEN '06' THEN 'Hessen'
        WHEN '07' THEN 'Rheinland-Pfalz'
        WHEN '08' THEN 'Baden-Wuerttemberg'
        WHEN '09' THEN 'Bayern'
        WHEN '10' THEN 'Saarland'
        WHEN '11' THEN 'Berlin'
        WHEN '12' THEN 'Brandenburg'
        WHEN '13' THEN 'Mecklenburg-Vorpommern'
        WHEN '14' THEN 'Sachsen'
        WHEN '15' THEN 'Sachsen-Anhalt'
        WHEN '16' THEN 'Thueringen'
        ELSE 'Unbekannt'
    END               AS bundesland_name
FROM default.project_bevoelkerungzahlen
WHERE LENGTH(id) = 5
"""


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")

    print("Fuelle gruppe3_dim_jahr ...")
    cur.execute(fill_dim_jahr)
    print("  -> OK")

    print("Fuelle gruppe3_dim_kreis ...")
    cur.execute(fill_dim_kreis)
    print("  -> OK")

    # --- Kontrolle: zeigen, was drinsteht ---
    cur.execute("SELECT jahr, jahrzehnt FROM gruppe3_dim_jahr ORDER BY jahr")
    print("\ndim_jahr:")
    for row in cur.fetchall():
        print("  ", row)

    cur.execute("SELECT COUNT(*) FROM gruppe3_dim_kreis")
    print(f"\ndim_kreis: {cur.fetchone()[0]} Kreise. Beispiele:")
    cur.execute("SELECT kreis_id, kreis_name, bundesland_name FROM gruppe3_dim_kreis ORDER BY kreis_id LIMIT 5")
    for row in cur.fetchall():
        print("  ", row)

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
