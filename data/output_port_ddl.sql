-- Physisches Output-Port-Schema des Datenprodukts gruppe3.
-- Zweck: reproduzierbare Basis fuer `datacontract import sql`.
-- Die fachlichen Beschreibungen und Quality-Regeln stehen in data_contract.yaml.

CREATE TABLE IF NOT EXISTS gruppe3_dim_kreis (
    kreis_id STRING,
    kreis_name STRING,
    bundesland_id STRING,
    bundesland_name STRING
);

CREATE TABLE IF NOT EXISTS gruppe3_dim_jahr (
    jahr INT,
    jahrzehnt INT
);

CREATE TABLE IF NOT EXISTS gruppe3_dim_gemeinde (
    gemeinde_id STRING,
    gemeinde_name STRING,
    kreis_id STRING,
    bundesland_name STRING,
    postal_code STRING,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_dim_klimastadt (
    stadt_name STRING,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_fact_bevoelkerung (
    kreis_id STRING,
    jahr INT,
    einwohner_insgesamt BIGINT,
    einwohner_maennlich BIGINT,
    einwohner_weiblich BIGINT,
    geschlechterquotient DOUBLE,
    wachstum_vorjahr_pct DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_fact_bauland (
    kreis_id STRING,
    jahr INT,
    anzahl_veraeusserungsfaelle BIGINT,
    veraeusserte_flaeche_1000qm BIGINT,
    kaufsumme_tsd_eur BIGINT,
    kaufwert_je_qm_eur DOUBLE,
    preis_pro_qm_eur DOUBLE,
    anteil_baureif_pct DOUBLE,
    durchschnittsfall_qm DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_fact_klima (
    stadt_name STRING,
    jahr INT,
    avg_temperatur DOUBLE,
    temperatur_abweichung_grad DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_fact_gemeinde_stamm (
    gemeinde_id STRING,
    einwohner_total BIGINT,
    einwohner_maennlich BIGINT,
    einwohner_weiblich BIGINT,
    anteil_weiblich_pct DOUBLE,
    area_km2 DOUBLE,
    einwohner_pro_km2 DOUBLE
);

CREATE TABLE IF NOT EXISTS gruppe3_fact_standortprofil_kpi (
    kreis_id STRING,
    jahr INT,
    wohnraumdruck_index DOUBLE,
    baulandpreis_pro_kopf_eur DOUBLE,
    freiflaeche_pro_einwohner_qm DOUBLE,
    klima_angepasstes_wohnraumrisiko DOUBLE,
    verstaedterung_index DOUBLE,
    standortattraktivitaets_score DOUBLE
);
