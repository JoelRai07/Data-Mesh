"""
DELIVERABLE 1: DDLs zur Erstellung des Datenmodells (Star-Schema)

Erstellt das Datenprodukt-Datenmodell auf Impala fuer den Use Case
"Standortprofil-Dashboard":
  - 4 Dimensionstabellen:  dim_kreis, dim_jahr, dim_gemeinde, dim_klimastadt
  - 5 Faktentabellen:      fact_bevoelkerung, fact_bauland, fact_klima,
                            fact_gemeinde_stamm, fact_standortprofil_kpi

Eigenschaften (s. Begruendung in docs/datenmodell_begruendung.md):
  - Star-Schema (denormalisiert) -> wenige Joins, gut fuer OLAP/Analytik
  - dim_gemeinde ist die Bruecke zwischen Kreis-Ebene (Bevoelkerung/Bauland)
    und Stadt-Ebene (Klima): Kreis-Zuordnung per Namens-Match, Klimastadt-
    Zuordnung per raeumlicher Naehe (latitude/longitude)
  - fact_standortprofil_kpi ist eine aggregierte Cross-Table-Faktentabelle,
    die Bevoelkerung + Bauland + Klima + Gemeinde-Stammdaten zu KPIs verdichtet
  - STORED AS PARQUET -> spaltenorientiert, ideal fuer analytische Abfragen
  - CREATE TABLE IF NOT EXISTS -> idempotent (mehrfach ausfuehrbar)

Ausfuehren:  .venv/Scripts/python.exe src/create_datamodel.py
"""
from db import get_connection

# Unsere Gruppe arbeitet in der Datenbank "gruppe3" (jede Gruppe hat eine eigene).
DATABASE = "gruppe3"

# Praefix passend zur Gruppen-Datenbank.
PREFIX = "gruppe3_"

DIM_KREIS = PREFIX + "dim_kreis"
DIM_JAHR = PREFIX + "dim_jahr"
DIM_GEMEINDE = PREFIX + "dim_gemeinde"
DIM_KLIMASTADT = PREFIX + "dim_klimastadt"

FACT_BEVOELKERUNG = PREFIX + "fact_bevoelkerung"
FACT_BAULAND = PREFIX + "fact_bauland"
FACT_KLIMA = PREFIX + "fact_klima"
FACT_GEMEINDE_STAMM = PREFIX + "fact_gemeinde_stamm"
FACT_STANDORTPROFIL_KPI = PREFIX + "fact_standortprofil_kpi"


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

# dim_gemeinde: Bruecken-Dimension zwischen Kreis-Ebene und Klimastadt-Ebene.
# kreis_id wird per Namens-Match (district_kreis -> dim_kreis.kreis_name) aufgeloest.
# latitude/longitude dienen als Matching-Schluessel zu dim_klimastadt (naechste Stadt).
create_dim_gemeinde = f"""
CREATE TABLE IF NOT EXISTS {DIM_GEMEINDE} (
    gemeinde_id      STRING COMMENT 'Surrogat-Schluessel (PK), da Quelle keinen Schluessel hat',
    gemeinde_name    STRING COMMENT 'Name der Gemeinde',
    kreis_id         STRING COMMENT 'FK -> dim_kreis.kreis_id, aufgeloest per Namens-Match',
    bundesland_name  STRING COMMENT 'Name des Bundeslandes',
    postal_code      STRING COMMENT 'Postleitzahl',
    latitude         DOUBLE COMMENT 'Breitengrad, fuer Naeherungs-Match zu dim_klimastadt',
    longitude        DOUBLE COMMENT 'Laengengrad, fuer Naeherungs-Match zu dim_klimastadt'
)
STORED AS PARQUET
"""

# dim_klimastadt: deutsche Staedte aus den Klimadaten (distinct city/lat/long).
create_dim_klimastadt = f"""
CREATE TABLE IF NOT EXISTS {DIM_KLIMASTADT} (
    stadt_name  STRING COMMENT 'Stadtname (PK), gefiltert auf country = Germany',
    latitude    DOUBLE COMMENT 'Breitengrad',
    longitude   DOUBLE COMMENT 'Laengengrad'
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
    einwohner_insgesamt    BIGINT COMMENT 'Einwohner gesamt',
    einwohner_maennlich    BIGINT COMMENT 'Einwohner maennlich',
    einwohner_weiblich     BIGINT COMMENT 'Einwohner weiblich',
    geschlechterquotient  DOUBLE COMMENT 'KPI: einwohner_maennlich / einwohner_weiblich',
    wachstum_vorjahr_pct  DOUBLE COMMENT 'KPI: prozentuale Veraenderung ggue. Vorjahr'
)
STORED AS PARQUET
"""

# fact_bauland: Baulandverkaeufe je Kreis und Jahr.
# Entsteht durch PIVOT der Quelltabelle (3 von 4 Merkmalen -> 3 Kennzahl-Spalten;
# das 4. Merkmal "Durchschnittlicher Kaufwert je qm" wird aktuell NICHT
# uebernommen, siehe TODO in pipeline.py).
create_fact_bauland = f"""
CREATE TABLE IF NOT EXISTS {FACT_BAULAND} (
    kreis_id                       STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                           INT    COMMENT 'FK -> dim_jahr.jahr',
    anzahl_veraeusserungsfaelle    BIGINT COMMENT 'Anzahl Veraeusserungsfaelle von Bauland',
    veraeusserte_flaeche_1000qm    BIGINT COMMENT 'Veraeusserte Baulandflaeche in 1000 qm',
    kaufsumme_tsd_eur              BIGINT COMMENT 'Kaufsumme in Tsd. EUR',
    preis_pro_qm_eur               DOUBLE COMMENT 'KPI: kaufsumme_tsd_eur / veraeusserte_flaeche_1000qm',
    anteil_baureif_pct             DOUBLE COMMENT 'KPI: Anteil baureifes Land an Gesamtflaeche',
    durchschnittsfall_qm           DOUBLE COMMENT 'KPI: mittlere Grundstuecksgroesse je Veraeusserungsfall'
)
STORED AS PARQUET
"""

# fact_klima: Durchschnittstemperatur je deutsche Klimastadt und Jahr.
# FK auf dim_klimastadt statt freiem String (s. Begruendung).
create_fact_klima = f"""
CREATE TABLE IF NOT EXISTS {FACT_KLIMA} (
    stadt_name                 STRING COMMENT 'FK -> dim_klimastadt.stadt_name',
    jahr                       INT    COMMENT 'FK -> dim_jahr.jahr',
    avg_temperatur             DOUBLE COMMENT 'Durchschnittstemperatur in Grad Celsius (Jahresmittel)',
    temperatur_abweichung_grad DOUBLE COMMENT 'KPI: Abweichung vom langjaehrigen Referenzmittel (Klimawandel-Indikator)'
)
STORED AS PARQUET
"""

# fact_gemeinde_stamm: Gemeinde-Stammdaten als Snapshot (Quelle hat keine Zeitreihe, daher kein jahr).
create_fact_gemeinde_stamm = f"""
CREATE TABLE IF NOT EXISTS {FACT_GEMEINDE_STAMM} (
    gemeinde_id          STRING COMMENT 'FK -> dim_gemeinde.gemeinde_id',
    einwohner_total       BIGINT COMMENT 'Einwohner gesamt (Snapshot)',
    einwohner_maennlich   BIGINT COMMENT 'Einwohner maennlich (Snapshot)',
    einwohner_weiblich    BIGINT COMMENT 'Einwohner weiblich (Snapshot)',
    anteil_weiblich_pct  DOUBLE COMMENT 'KPI: einwohner_weiblich / einwohner_total * 100',
    area_km2             DOUBLE COMMENT 'Flaeche der Gemeinde in km2',
    einwohner_pro_km2    DOUBLE COMMENT 'KPI: Bevoelkerungsdichte'
)
STORED AS PARQUET
"""

# fact_standortprofil_kpi: aggregierte Cross-Table-Faktentabelle fuer das Dashboard.
# Verdichtet fact_bevoelkerung + fact_bauland + fact_klima (ueber dim_gemeinde) +
# fact_gemeinde_stamm zu Kreis x Jahr-Kennzahlen.
create_fact_standortprofil_kpi = f"""
CREATE TABLE IF NOT EXISTS {FACT_STANDORTPROFIL_KPI} (
    kreis_id                          STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                              INT    COMMENT 'FK -> dim_jahr.jahr',
    wohnraumdruck_index               DOUBLE COMMENT 'bevoelkerungswachstum_pct / bauland_angebotswachstum_pct (fact_bevoelkerung x fact_bauland)',
    baulandpreis_pro_kopf_eur         DOUBLE COMMENT 'kaufsumme_tsd_eur*1000 / einwohner_insgesamt (fact_bauland x fact_bevoelkerung)',
    freiflaeche_pro_einwohner_qm      DOUBLE COMMENT 'veraeusserte_flaeche_1000qm*1000 / einwohner_insgesamt (fact_bauland x fact_bevoelkerung)',
    klima_angepasstes_wohnraumrisiko  DOUBLE COMMENT 'wohnraumdruck_index * (1 + temperatur_abweichung_grad/10) (fact_bevoelkerung x fact_bauland x fact_klima via dim_gemeinde)',
    verstaedterung_index              DOUBLE COMMENT 'Gemeinde-Dichte vs. Kreis-Durchschnittsdichte (fact_gemeinde_stamm x fact_bevoelkerung via dim_gemeinde)',
    standortattraktivitaets_score     DOUBLE COMMENT 'z-standardisierter Score aus Bevoelkerung + Bauland + Klima (alle Basisfakten)'
)
STORED AS PARQUET
"""


STATEMENTS = {
    DIM_KREIS: create_dim_kreis,
    DIM_JAHR: create_dim_jahr,
    DIM_GEMEINDE: create_dim_gemeinde,
    DIM_KLIMASTADT: create_dim_klimastadt,
    FACT_BEVOELKERUNG: create_fact_bevoelkerung,
    FACT_BAULAND: create_fact_bauland,
    FACT_KLIMA: create_fact_klima,
    FACT_GEMEINDE_STAMM: create_fact_gemeinde_stamm,
    FACT_STANDORTPROFIL_KPI: create_fact_standortprofil_kpi,
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
