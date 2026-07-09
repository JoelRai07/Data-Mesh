"""
DDLs fuer das Star-Schema-Datenmodell (Standortprofil-Dashboard).
Input: keine (reine DDL). Output: 4 Dim- + 5 Fakt-Tabellen in Impala.
Details: docs/datenmodell_begruendung.md

Ausfuehren:  .venv/Scripts/python.exe src/create_datamodel.py
"""
import os

from db import get_connection

DATABASE = os.getenv("DATABASE", "gruppe3")
PREFIX = os.getenv("PREFIX", "gruppe3_")

DIM_KREIS = PREFIX + "dim_kreis"
DIM_JAHR = PREFIX + "dim_jahr"
DIM_GEMEINDE = PREFIX + "dim_gemeinde"
DIM_KLIMASTADT = PREFIX + "dim_klimastadt"

FACT_BEVOELKERUNG = PREFIX + "fact_bevoelkerung"
FACT_BAULAND = PREFIX + "fact_bauland"
FACT_KLIMA = PREFIX + "fact_klima"
FACT_GEMEINDE_STAMM = PREFIX + "fact_gemeinde_stamm"
FACT_STANDORTPROFIL_KPI = PREFIX + "fact_standortprofil_kpi"


# --- Dimensionstabellen ---

create_dim_kreis = f"""
CREATE TABLE IF NOT EXISTS {DIM_KREIS} (
    kreis_id         STRING COMMENT 'Amtlicher Regionalschluessel, 5-stellig (PK), z.B. 01001',
    kreis_name       STRING COMMENT 'Name des Kreises, z.B. Flensburg, kreisfreie Stadt',
    bundesland_id    STRING COMMENT 'Erste 2 Stellen des Regionalschluessels, z.B. 01',
    bundesland_name  STRING COMMENT 'Name des Bundeslandes, z.B. Schleswig-Holstein'
)
STORED AS PARQUET
"""

create_dim_jahr = f"""
CREATE TABLE IF NOT EXISTS {DIM_JAHR} (
    jahr       INT COMMENT 'Jahr (PK), z.B. 2024',
    jahrzehnt  INT COMMENT 'Jahrzehnt, z.B. 2020'
)
STORED AS PARQUET
"""

create_dim_gemeinde = f"""
CREATE TABLE IF NOT EXISTS {DIM_GEMEINDE} (
    gemeinde_id      STRING COMMENT 'Surrogat-Schluessel (PK), da Quelle keinen Schluessel hat',
    gemeinde_name    STRING COMMENT 'Name der Gemeinde',
    kreis_id         STRING COMMENT 'FK -> dim_kreis.kreis_id, aufgeloest per Namens-Match',
    bundesland_name  STRING COMMENT 'Name des Bundeslandes',
    postal_code      STRING COMMENT 'Postleitzahl',
    latitude         DOUBLE COMMENT 'Breitengrad',
    longitude        DOUBLE COMMENT 'Laengengrad'
)
STORED AS PARQUET
"""

create_dim_klimastadt = f"""
CREATE TABLE IF NOT EXISTS {DIM_KLIMASTADT} (
    stadt_name  STRING COMMENT 'Stadtname (PK), gefiltert auf country = Germany',
    latitude    DOUBLE COMMENT 'Breitengrad',
    longitude   DOUBLE COMMENT 'Laengengrad'
)
STORED AS PARQUET
"""


# --- Faktentabellen ---

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

create_fact_bauland = f"""
CREATE TABLE IF NOT EXISTS {FACT_BAULAND} (
    kreis_id                       STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                           INT    COMMENT 'FK -> dim_jahr.jahr',
    anzahl_veraeusserungsfaelle    BIGINT COMMENT 'Anzahl Veraeusserungsfaelle von Bauland',
    veraeusserte_flaeche_1000qm    BIGINT COMMENT 'Veraeusserte Baulandflaeche in 1000 qm',
    kaufsumme_tsd_eur              BIGINT COMMENT 'Kaufsumme in Tsd. EUR',
    kaufwert_je_qm_eur             DOUBLE COMMENT 'Amtlicher Durchschnittlicher Kaufwert je qm (Merkmal aus Quelle, nicht berechnet)',
    preis_pro_qm_eur               DOUBLE COMMENT 'KPI: kaufsumme_tsd_eur / veraeusserte_flaeche_1000qm',
    anteil_baureif_pct             DOUBLE COMMENT 'KPI: Anteil baureifes Land an Gesamtflaeche',
    durchschnittsfall_qm           DOUBLE COMMENT 'KPI: mittlere Grundstuecksgroesse je Veraeusserungsfall'
)
STORED AS PARQUET
"""

create_fact_klima = f"""
CREATE TABLE IF NOT EXISTS {FACT_KLIMA} (
    stadt_name                 STRING COMMENT 'FK -> dim_klimastadt.stadt_name',
    jahr                       INT    COMMENT 'FK -> dim_jahr.jahr',
    avg_temperatur             DOUBLE COMMENT 'Durchschnittstemperatur in Grad Celsius (Jahresmittel)',
    temperatur_abweichung_grad DOUBLE COMMENT 'KPI: Abweichung vom langjaehrigen Referenzmittel (Klimawandel-Indikator)'
)
STORED AS PARQUET
"""

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

create_fact_standortprofil_kpi = f"""
CREATE TABLE IF NOT EXISTS {FACT_STANDORTPROFIL_KPI} (
    kreis_id                          STRING COMMENT 'FK -> dim_kreis.kreis_id',
    jahr                              INT    COMMENT 'FK -> dim_jahr.jahr',
    wohnraumdruck_index               DOUBLE COMMENT 'einwohner_insgesamt / veraeusserte_flaeche_1000qm - Einwohner je 1000qm neu veraeusserter Baulandflaeche, nie negativ (fact_bevoelkerung x fact_bauland)',
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
