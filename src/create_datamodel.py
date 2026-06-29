"""
DELIVERABLE 1: DDLs zur Erstellung des Datenmodells (Star-Schema)

Erstellt das Datenprodukt-Datenmodell auf Impala:
  - 2 Dimensionstabellen:  dim_kreis, dim_jahr
  - 2 Faktentabellen:      fact_bevoelkerung, fact_bauland
  - 1 optionale Faktentabelle: fact_klima

Eigenschaften (s. Begruendung in docs/datenmodell_begruendung.md):
  - Star-Schema (denormalisiert) -> wenige Joins, gut fuer OLAP/Analytik
  - STORED AS PARQUET -> spaltenorientiert, ideal fuer analytische Abfragen
  - CREATE TABLE IF NOT EXISTS -> idempotent (mehrfach ausfuehrbar)

Ausfuehren:  .venv/Scripts/python.exe src/create_datamodel.py
"""
from db import get_connection

# Unsere Gruppe arbeitet in der Datenbank "gruppe4" (jede Gruppe hat eine eigene).
DATABASE = "gruppe4"

# Praefix wie vom Team verwendet (z.B. gruppe4_project_bauland_bereinigt).
PREFIX = "gruppe4_"

DIM_KREIS = PREFIX + "dim_kreis"
DIM_JAHR = PREFIX + "dim_jahr"
FACT_BEVOELKERUNG = PREFIX + "fact_bevoelkerung"
FACT_BAULAND = PREFIX + "fact_bauland"
FACT_KLIMA = PREFIX + "fact_klima"


# ---------------------------------------------------------------------------
# DIMENSIONSTABELLEN (qualitative Daten: das "Wer/Wo/Wann")
# ---------------------------------------------------------------------------

# dim_kreis: die geografische Dimension auf Kreis-Ebene.
# Denormalisiert: Bundesland steht direkt mit drin (Star-Schema, kein Join noetig).
create_dim_kreis = f"""
CREATE TABLE IF NOT EXISTS {DIM_KREIS} (
    kreis_id         STRING COMMENT 'Amtlicher Regionalschluessel, 5-stellig (PK), z.B. 01001',
    kreis_name       STRING COMMENT 'Name des Kreises, z.B. Flensburg, kreisfreie Stadt',
    bundesland_id    STRING COMMENT 'Erste 2 Stellen des Regionalschluessels, z.B. 01',
    bundesland_name  STRING COMMENT 'Name des Bundeslandes, z.B. Schleswig-Holstein'
)
STORED AS PARQUET
"""

# dim_jahr: die Zeit-Dimension.
create_dim_jahr = f"""
CREATE TABLE IF NOT EXISTS {DIM_JAHR} (
    jahr       INT COMMENT 'Jahr (PK), z.B. 2024',
    jahrzehnt  INT COMMENT 'Jahrzehnt, z.B. 2020'
)
STORED AS PARQUET
"""


# ---------------------------------------------------------------------------
# FAKTENTABELLEN (quantitative Daten: die Kennzahlen / Messwerte)
# ---------------------------------------------------------------------------

# fact_bevoelkerung: Einwohnerzahlen je Kreis und Jahr.
# Entsteht durch UNPIVOT der breiten Quelltabelle (1 Spalte pro Jahr -> 1 Zeile pro Jahr).
create_fact_bevoelkerung = f"""
CREATE TABLE IF NOT EXISTS {FACT_BEVOELKERUNG} (
    kreis_id              STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                  INT    COMMENT 'FK -> dim_jahr.jahr',
    einwohner_insgesamt   BIGINT COMMENT 'Einwohner gesamt',
    einwohner_maennlich   BIGINT COMMENT 'Einwohner maennlich',
    einwohner_weiblich    BIGINT COMMENT 'Einwohner weiblich'
)
STORED AS PARQUET
"""

# fact_bauland: Baulandverkaeufe je Kreis und Jahr.
# Entsteht durch PIVOT der Quelltabelle (4 Merkmale -> 4 Kennzahl-Spalten).
create_fact_bauland = f"""
CREATE TABLE IF NOT EXISTS {FACT_BAULAND} (
    kreis_id                       STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                           INT    COMMENT 'FK -> dim_jahr.jahr',
    anzahl_veraeusserungsfaelle    BIGINT COMMENT 'Anzahl Veraeusserungsfaelle von Bauland',
    veraeusserte_flaeche_1000qm    BIGINT COMMENT 'Veraeusserte Baulandflaeche in 1000 qm',
    kaufsumme_tsd_eur              BIGINT COMMENT 'Kaufsumme in Tsd. EUR'
)
STORED AS PARQUET
"""

# fact_klima (optional): Durchschnittstemperatur je deutsche Stadt und Jahr.
# Hinweis: Klima hat KEINEN Regionalschluessel -> nur lose ueber Stadtname anbindbar.
create_fact_klima = f"""
CREATE TABLE IF NOT EXISTS {FACT_KLIMA} (
    stadt              STRING COMMENT 'Stadtname (kein Regionalschluessel vorhanden)',
    jahr               INT    COMMENT 'FK -> dim_jahr.jahr',
    avg_temperatur     DOUBLE COMMENT 'Durchschnittstemperatur in Grad Celsius'
)
STORED AS PARQUET
"""


STATEMENTS = {
    DIM_KREIS: create_dim_kreis,
    DIM_JAHR: create_dim_jahr,
    FACT_BEVOELKERUNG: create_fact_bevoelkerung,
    FACT_BAULAND: create_fact_bauland,
    FACT_KLIMA: create_fact_klima,
}


def main():
    conn = get_connection()
    cur = conn.cursor()

    # In die Gruppen-Datenbank wechseln, damit die Tabellen dort angelegt werden.
    cur.execute(f"USE {DATABASE}")
    print(f"Datenbank: {DATABASE}\n")

    for table_name, statement in STATEMENTS.items():
        print(f"Erstelle Tabelle: {table_name} ...")
        cur.execute(statement)
        print("  -> OK")
    print("\nDatenmodell steht.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
