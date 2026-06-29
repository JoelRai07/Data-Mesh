"""
DELIVERABLE 2: Pipeline - befuellt das Datenmodell aus create_datamodel.py.

WICHTIG: Die Rohdaten liegen NICHT mehr in `default`, sondern wurden in unsere
Gruppen-Datenbank `gruppe3` kopiert und tragen dort ebenfalls das Praefix
`gruppe3_` (z.B. `gruppe3_project_bauland` statt `default.project_bauland`).
Alle Quell- UND Zieltabellen befinden sich also in derselben Datenbank.

Reihenfolge der Befuellung folgt den Abhaengigkeiten im Star-Schema:
  1. dim_kreis             (keine Abhaengigkeit)
  2. dim_jahr              (keine Abhaengigkeit)
  3. dim_klimastadt        (keine Abhaengigkeit)
  4. dim_gemeinde          (braucht dim_kreis, fuer die Kreis-Zuordnung)
  5. fact_bevoelkerung     (braucht nur die Rohdaten, liefert spaeter Inputs fuer KPI-Fakt)
  6. fact_bauland          (braucht nur die Rohdaten)
  7. fact_klima            (braucht nur die Rohdaten)
  8. fact_gemeinde_stamm   (braucht dim_gemeinde, fuer die FK)
  9. fact_standortprofil_kpi (braucht ALLE oben genannten Fakten + dim_gemeinde/dim_klimastadt,
                              da hier die Cross-Table-KPIs berechnet werden)

Idempotent durch INSERT OVERWRITE: jeder Lauf ersetzt den Inhalt komplett,
es entstehen also keine Duplikate.

Ausfuehren:  .venv/Scripts/python.exe src/pipeline.py
"""
import sys
from db import get_connection

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATABASE = "gruppe3"

# Alle project_*-Quelltabellen liegen jetzt in gruppe3 und heissen gruppe3_project_*.
SRC_GEMEINDEN = "gruppe3_project_gemeinden"
SRC_BAULAND = "gruppe3_project_bauland"
SRC_KLIMADATEN = "gruppe3_project_klimadaten"
SRC_BEVOELKERUNG = "gruppe3_project_bevoelkerungzahlen"


# ---------------------------------------------------------------------------
# DIMENSIONEN
# ---------------------------------------------------------------------------

# dim_kreis: ein Eintrag je Kreis (aus den Bevoelkerungs-Rohdaten).
# Bereinigung: nur echte Kreise (5-stelliger Schluessel), keine Aggregat-Zeilen
#   wie 'DG' (Deutschland) oder '01' (Bundesland).
# bundesland_id = die ersten 2 Stellen des Regionalschluessels (z.B. 01001 -> 01).
fill_dim_kreis = f"""
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
FROM {SRC_BEVOELKERUNG}
WHERE LENGTH(id) = 5
"""

# dim_jahr: Vereinigung der Jahre aus Bauland (kann ueber 2024 hinausgehen) und dem
# fest bekannten Bevoelkerungs-Zeitraum 1995-2024 (so breit ist die Quelle gepflegt,
# wir hardcoden das hier bewusst statt es aus Spaltennamen zu parsen - lesbarer).
# Impala kennt keinen generischen Tabellenkonstruktor (VALUES (...) AS t(col)),
# daher per Python-Schleife als UNION ALL von Literalen gebaut (selbe Technik wie
# beim Unpivot von fact_bevoelkerung weiter unten).
_bevoelkerung_jahre_sql = "\n    UNION ALL\n".join(
    f"    SELECT {j} AS jahr" for j in range(1995, 2025)
)
fill_dim_jahr = f"""
INSERT OVERWRITE gruppe3_dim_jahr
SELECT DISTINCT
    jahr,
    CAST(FLOOR(jahr / 10) * 10 AS INT) AS jahrzehnt
FROM (
    SELECT CAST(jahr AS INT) AS jahr FROM {SRC_BAULAND} WHERE jahr IS NOT NULL
    UNION ALL
{_bevoelkerung_jahre_sql}
) alle_jahre
"""

# dim_klimastadt: deutsche Staedte aus den Klimadaten, mit lat/long als DOUBLE
# (Quelle speichert z.B. '54.79N' / '9.43E' als String mit Himmelsrichtung).
# Negativ fuer S/W, damit spaeter normale Distanzrechnung (POW/SQRT) funktioniert.
fill_dim_klimastadt = f"""
INSERT OVERWRITE gruppe3_dim_klimastadt
SELECT DISTINCT
    city AS stadt_name,
    CAST(REGEXP_REPLACE(latitude, '[NS]', '') AS DOUBLE)
        * CASE WHEN latitude LIKE '%S' THEN -1 ELSE 1 END AS latitude,
    CAST(REGEXP_REPLACE(longitude, '[EW]', '') AS DOUBLE)
        * CASE WHEN longitude LIKE '%W' THEN -1 ELSE 1 END AS longitude
FROM {SRC_KLIMADATEN}
WHERE country = 'Germany'
"""

# dim_gemeinde: Bruecken-Dimension zu Kreis (per Namens-Match) und spaeter zu
# Klimastadt (per raeumlicher Naehe, das passiert aber erst in fact_standortprofil_kpi).
#
# DATENQUALITAET: project_gemeinden hat ein CSV-Parsing-Problem - Gemeindenamen mit
# Komma (z.B. "Flensburg, Stadt") sprengen das Quoting, wodurch ALLE nachfolgenden
# Spalten (area_km2, population_total, ..., latitude) um eine Spalte verrutschen.
# Wir laden daher NUR Zeilen, bei denen area_km2 erfolgreich als Zahl geparst wurde
# (WHERE area_km2 IS NOT NULL) - das ist ein verlaesslicher Indikator dafuer, dass
# die Zeile NICHT verrutscht ist. Betroffene Zeilen werden bewusst ausgeschlossen,
# nicht reparaturversucht (Begruendung: Federated Governance, s. Begruendungs-MD).
#
# Kreis-Zuordnung per Namens-Match (LIKE statt '='), weil district_kreis und
# dim_kreis.kreis_name unterschiedliche Schreibweisen haben koennen
# (z.B. "Flensburg" vs. "Flensburg, kreisfreie Stadt"). ROW_NUMBER() waehlt bei
# mehreren Treffern den laengsten (= spezifischsten) Kreisnamen.
# gemeinde_id ist ein Surrogat-Schluessel (ROW_NUMBER), da die Quelle keinen hat.
fill_dim_gemeinde = f"""
INSERT OVERWRITE gruppe3_dim_gemeinde
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY g.municipality_name, g.postal_code) AS STRING) AS gemeinde_id,
    g.municipality_name AS gemeinde_name,
    m.kreis_id,
    g.state_land AS bundesland_name,
    g.postal_code,
    CAST(g.latitude AS DOUBLE)  AS latitude,
    CAST(g.longitude AS DOUBLE) AS longitude
FROM {SRC_GEMEINDEN} g
LEFT JOIN (
    SELECT g2.municipality_name, g2.postal_code, k.kreis_id,
        ROW_NUMBER() OVER (
            PARTITION BY g2.municipality_name, g2.postal_code
            ORDER BY LENGTH(k.kreis_name) DESC
        ) AS rn
    FROM {SRC_GEMEINDEN} g2
    JOIN gruppe3_dim_kreis k
        ON LOWER(k.kreis_name) LIKE CONCAT('%', LOWER(TRIM(REGEXP_REPLACE(g2.district_kreis, '"', ''))), '%')
    WHERE g2.area_km2 IS NOT NULL
) m
    ON m.municipality_name = g.municipality_name
    AND m.postal_code = g.postal_code
    AND m.rn = 1
WHERE g.area_km2 IS NOT NULL
"""


# ---------------------------------------------------------------------------
# FAKTEN (Basis-Fakten, je eine Quelltabelle)
# ---------------------------------------------------------------------------

def _build_fill_fact_bevoelkerung():
    """
    fact_bevoelkerung entsteht durch UNPIVOT der breiten Quelltabelle
    (1 Spalte pro Jahr -> 1 Zeile pro Kreis+Jahr). Impala kennt kein generisches
    UNPIVOT, daher bauen wir die 30 fast identischen SELECTs (1995-2024) hier
    per Python-Schleife zusammen statt sie 30x abzuschreiben (DRY, weniger
    Tippfehler-Risiko bei den vielen Spaltennamen).

    Die KPIs (geschlechterquotient, wachstum_vorjahr_pct) brauchen den
    Vorjahreswert -> dafuer eine aeussere Abfrage mit LAG() OVER (PARTITION
    BY kreis_id ORDER BY jahr) auf dem unpivotierten Zwischenergebnis.
    """
    selects = []
    for jahr in range(1995, 2025):
        suffix = f"{jahr % 100:02d}"
        selects.append(f"""
    SELECT
        id AS kreis_id,
        {jahr} AS jahr,
        insgesamt_{suffix} AS einwohner_insgesamt,
        maennlich_{suffix} AS einwohner_maennlich,
        weiblich_{suffix}  AS einwohner_weiblich
    FROM {SRC_BEVOELKERUNG}
    WHERE LENGTH(id) = 5 AND insgesamt_{suffix} IS NOT NULL""")

    unpivot_sql = "\n    UNION ALL\n".join(selects)

    return f"""
INSERT OVERWRITE gruppe3_fact_bevoelkerung
SELECT
    kreis_id,
    jahr,
    einwohner_insgesamt,
    einwohner_maennlich,
    einwohner_weiblich,
    ROUND(einwohner_maennlich / NULLIF(einwohner_weiblich, 0), 4) AS geschlechterquotient,
    ROUND(
        100.0 * (einwohner_insgesamt - LAG(einwohner_insgesamt) OVER (PARTITION BY kreis_id ORDER BY jahr))
        / NULLIF(LAG(einwohner_insgesamt) OVER (PARTITION BY kreis_id ORDER BY jahr), 0)
    , 3) AS wachstum_vorjahr_pct
FROM (
{unpivot_sql}
) unpivotiert
"""


fill_fact_bevoelkerung = _build_fill_fact_bevoelkerung()

# fact_bauland entsteht durch PIVOT (lang -> breit): die Quelle hat eine Zeile je
# Merkmal (Veraeusserungsfaelle / Flaeche / Kaufsumme), wir drehen das in 3 Spalten
# je Kreis+Jahr per CASE-WHEN-Aggregation (klassisches "konditionales Pivot").
#
# DATENQUALITAET: die merkmal-Werte sind durch einen Encoding-Fehler beschaedigt
# (z.B. 'Ver?u?erungsfälle von Bauland' statt 'Veräußerungsfälle von Bauland' -
# 'ä' und 'ß' wurden beim Import als '?' bzw. unbekanntes Zeichen geschrieben).
# Wir matchen daher auf die UNBESCHAEDIGTEN Teilstrings ('erungsf%lle', 'erte
# Bauland', 'Kaufsumme') statt auf den vollen (kaputten) Text.
fill_fact_bauland = f"""
INSERT OVERWRITE gruppe3_fact_bauland
SELECT
    kreis_id,
    CAST(jahr AS INT) AS jahr,
    MAX(CASE WHEN merkmal LIKE '%erungsf%lle%'  THEN insgesamt END) AS anzahl_veraeusserungsfaelle,
    MAX(CASE WHEN merkmal LIKE '%erte Bauland%' THEN insgesamt END) AS veraeusserte_flaeche_1000qm,
    MAX(CASE WHEN merkmal LIKE 'Kaufsumme%'     THEN insgesamt END) AS kaufsumme_tsd_eur,
    ROUND(
        MAX(CASE WHEN merkmal LIKE 'Kaufsumme%' THEN insgesamt END)
        / NULLIF(MAX(CASE WHEN merkmal LIKE '%erte Bauland%' THEN insgesamt END), 0)
    , 2) AS preis_pro_qm_eur,
    ROUND(
        100.0 * MAX(CASE WHEN merkmal LIKE '%erte Bauland%' THEN baureifes_land END)
        / NULLIF(MAX(CASE WHEN merkmal LIKE '%erte Bauland%' THEN insgesamt END), 0)
    , 2) AS anteil_baureif_pct,
    ROUND(
        1000.0 * MAX(CASE WHEN merkmal LIKE '%erte Bauland%' THEN insgesamt END)
        / NULLIF(MAX(CASE WHEN merkmal LIKE '%erungsf%lle%' THEN insgesamt END), 0)
    , 2) AS durchschnittsfall_qm
FROM {SRC_BAULAND}
WHERE LENGTH(kreis_id) = 5  -- nur echte Kreise, keine Aggregatzeilen (DG, Bundeslaender)
GROUP BY kreis_id, jahr
"""

# fact_klima: Jahresmittel je deutscher Stadt + Abweichung vom langjaehrigen
# Referenzmittel 1961-1990 (uebliche Klimawandel-Referenzperiode). Die Referenz
# nutzt bewusst die VOLLE Historie der Quelle (project_klimadaten geht bis 1743
# zurueck), waehrend die gespeicherten Zeilen selbst auf 1995-2024 begrenzt
# werden, damit fact_klima zum Zeitraum von dim_jahr/den anderen Fakten passt.
fill_fact_klima = f"""
INSERT OVERWRITE gruppe3_fact_klima
WITH jahresmittel AS (
    SELECT
        city AS stadt_name,
        CAST(SUBSTR(dt, 1, 4) AS INT) AS jahr,
        AVG(averagetemperature) AS avg_temperatur
    FROM {SRC_KLIMADATEN}
    WHERE country = 'Germany' AND averagetemperature IS NOT NULL
    GROUP BY city, CAST(SUBSTR(dt, 1, 4) AS INT)
),
referenz AS (
    SELECT
        city AS stadt_name,
        AVG(averagetemperature) AS referenz_temp
    FROM {SRC_KLIMADATEN}
    WHERE country = 'Germany'
        AND averagetemperature IS NOT NULL
        AND CAST(SUBSTR(dt, 1, 4) AS INT) BETWEEN 1961 AND 1990
    GROUP BY city
)
SELECT
    j.stadt_name,
    j.jahr,
    ROUND(j.avg_temperatur, 2) AS avg_temperatur,
    ROUND(j.avg_temperatur - r.referenz_temp, 2) AS temperatur_abweichung_grad
FROM jahresmittel j
JOIN referenz r ON r.stadt_name = j.stadt_name
WHERE j.jahr BETWEEN 1995 AND 2024
"""

# fact_gemeinde_stamm: Snapshot-Fakt (keine Jahr-Dimension, da die Quelle keine
# Zeitreihe ist). Join gegen dim_gemeinde ueber (municipality_name, postal_code),
# denselben Schluessel, mit dem dim_gemeinde befuellt wurde - so bleiben beide
# Tabellen exakt auf denselben (erfolgreich geparsten) Zeilen synchron.
fill_fact_gemeinde_stamm = f"""
INSERT OVERWRITE gruppe3_fact_gemeinde_stamm
SELECT
    d.gemeinde_id,
    g.population_total AS einwohner_total,
    g.male              AS einwohner_maennlich,
    g.female             AS einwohner_weiblich,
    ROUND(100.0 * g.female / NULLIF(g.population_total, 0), 2) AS anteil_weiblich_pct,
    g.area_km2,
    g.per_km2 AS einwohner_pro_km2
FROM {SRC_GEMEINDEN} g
JOIN gruppe3_dim_gemeinde d
    ON d.gemeinde_name = g.municipality_name AND d.postal_code = g.postal_code
WHERE g.area_km2 IS NOT NULL
"""


# ---------------------------------------------------------------------------
# CROSS-TABLE-KPI-FAKT (verdichtet alle obigen Fakten zu Dashboard-Kennzahlen)
# ---------------------------------------------------------------------------

# fact_standortprofil_kpi: Kreis x Jahr. Das ist die einzige Tabelle, die wirklich
# alle vier Quellen ueber Joins zusammenbringt:
#   - bev/bau: direkt ueber kreis_id+jahr (gemeinsamer Regionalschluessel)
#   - Klima: nur auf Stadt-Ebene vorhanden -> ueber dim_gemeinde (die je Gemeinde
#     die naechstgelegene Klimastadt per einfacher euklidischer Distanz auf
#     lat/long findet - fuer die geografische Ausdehnung Deutschlands
#     ausreichend genau, vermeidet Haversine-Trigonometrie in SQL) auf
#     Kreis-Ebene hochaggregiert (Mittelwert ueber alle Gemeinden eines Kreises)
#   - Verstaedterung: Gemeinde-Dichte (fact_gemeinde_stamm) vs. Kreis-Durchschnitt
#
# standortattraktivitaets_score ist ein ECHTER z-Score: Bevoelkerungswachstum,
# Baulandpreis und Klimaabweichung werden jeweils per AVG()/STDDEV() OVER
# (PARTITION BY jahr) auf den Mittelwert/Streuung DES JEWEILIGEN JAHRES normiert,
# damit Kreise nur innerhalb desselben Jahres verglichen werden (1996 hat ein
# anderes Preisniveau als 2024 - ein Vergleich ueber alle Jahre hinweg waere
# verzerrt).
fill_fact_standortprofil_kpi = f"""
INSERT OVERWRITE gruppe3_fact_standortprofil_kpi
WITH bev AS (
    SELECT kreis_id, jahr, einwohner_insgesamt, wachstum_vorjahr_pct
    FROM gruppe3_fact_bevoelkerung
),
bau AS (
    SELECT
        kreis_id, jahr, anzahl_veraeusserungsfaelle, veraeusserte_flaeche_1000qm,
        kaufsumme_tsd_eur, preis_pro_qm_eur,
        100.0 * (anzahl_veraeusserungsfaelle - LAG(anzahl_veraeusserungsfaelle) OVER (PARTITION BY kreis_id ORDER BY jahr))
        / NULLIF(LAG(anzahl_veraeusserungsfaelle) OVER (PARTITION BY kreis_id ORDER BY jahr), 0) AS faelle_wachstum_pct
    FROM gruppe3_fact_bauland
),
naechste_stadt AS (
    SELECT gemeinde_id, kreis_id, stadt_name
    FROM (
        SELECT
            g.gemeinde_id, g.kreis_id, s.stadt_name,
            ROW_NUMBER() OVER (
                PARTITION BY g.gemeinde_id
                ORDER BY POW(g.latitude - s.latitude, 2) + POW(g.longitude - s.longitude, 2)
            ) AS rn
        FROM gruppe3_dim_gemeinde g
        CROSS JOIN gruppe3_dim_klimastadt s
        WHERE g.latitude IS NOT NULL AND g.longitude IS NOT NULL AND g.kreis_id IS NOT NULL
    ) ranked
    WHERE rn = 1
),
klima_je_kreis AS (
    SELECT n.kreis_id, k.jahr, AVG(k.temperatur_abweichung_grad) AS temperatur_abweichung_grad
    FROM naechste_stadt n
    JOIN gruppe3_fact_klima k ON k.stadt_name = n.stadt_name
    GROUP BY n.kreis_id, k.jahr
),
dichte_je_kreis AS (
    SELECT g.kreis_id, AVG(f.einwohner_pro_km2) AS kreis_avg_dichte, MAX(f.einwohner_pro_km2) AS max_gemeinde_dichte
    FROM gruppe3_fact_gemeinde_stamm f
    JOIN gruppe3_dim_gemeinde g ON g.gemeinde_id = f.gemeinde_id
    WHERE g.kreis_id IS NOT NULL
    GROUP BY g.kreis_id
),
-- Impala kennt STDDEV() nur als normale Aggregatfunktion, NICHT mit OVER (...).
-- Daher Mittelwert/Standardabweichung je Jahr separat per GROUP BY berechnen
-- und unten zurueckjoinen, statt eines Analytic-Window wie in Postgres/Oracle.
jahres_stats AS (
    SELECT
        bev.jahr,
        AVG(bev.wachstum_vorjahr_pct)                   AS avg_wachstum,
        STDDEV(bev.wachstum_vorjahr_pct)                 AS std_wachstum,
        AVG(bau.preis_pro_qm_eur)                        AS avg_preis,
        STDDEV(bau.preis_pro_qm_eur)                      AS std_preis,
        AVG(COALESCE(kk.temperatur_abweichung_grad, 0))   AS avg_klima,
        STDDEV(COALESCE(kk.temperatur_abweichung_grad, 0)) AS std_klima
    FROM bev
    JOIN bau ON bau.kreis_id = bev.kreis_id AND bau.jahr = bev.jahr
    LEFT JOIN klima_je_kreis kk ON kk.kreis_id = bev.kreis_id AND kk.jahr = bev.jahr
    GROUP BY bev.jahr
)
SELECT
    bev.kreis_id,
    bev.jahr,
    ROUND(bev.wachstum_vorjahr_pct / NULLIF(bau.faelle_wachstum_pct, 0), 3) AS wohnraumdruck_index,
    ROUND(1000.0 * bau.kaufsumme_tsd_eur / NULLIF(bev.einwohner_insgesamt, 0), 2) AS baulandpreis_pro_kopf_eur,
    ROUND(1000.0 * bau.veraeusserte_flaeche_1000qm / NULLIF(bev.einwohner_insgesamt, 0), 4) AS freiflaeche_pro_einwohner_qm,
    ROUND(
        (bev.wachstum_vorjahr_pct / NULLIF(bau.faelle_wachstum_pct, 0))
        * (1 + COALESCE(kk.temperatur_abweichung_grad, 0) / 10)
    , 3) AS klima_angepasstes_wohnraumrisiko,
    ROUND(dk.max_gemeinde_dichte / NULLIF(dk.kreis_avg_dichte, 0), 3) AS verstaedterung_index,
    ROUND(
        (bev.wachstum_vorjahr_pct - js.avg_wachstum) / NULLIF(js.std_wachstum, 0)
        - (bau.preis_pro_qm_eur - js.avg_preis) / NULLIF(js.std_preis, 0)
        - ABS((COALESCE(kk.temperatur_abweichung_grad, 0) - js.avg_klima) / NULLIF(js.std_klima, 0))
    , 3) AS standortattraktivitaets_score
FROM bev
JOIN bau ON bau.kreis_id = bev.kreis_id AND bau.jahr = bev.jahr
LEFT JOIN klima_je_kreis kk ON kk.kreis_id = bev.kreis_id AND kk.jahr = bev.jahr
LEFT JOIN dichte_je_kreis dk ON dk.kreis_id = bev.kreis_id
JOIN jahres_stats js ON js.jahr = bev.jahr
"""


# ---------------------------------------------------------------------------
# AUSFUEHRUNG
# ---------------------------------------------------------------------------

# Reihenfolge = Abhaengigkeitsreihenfolge (s. Modul-Docstring oben).
STEPS = [
    ("gruppe3_dim_kreis", fill_dim_kreis),
    ("gruppe3_dim_jahr", fill_dim_jahr),
    ("gruppe3_dim_klimastadt", fill_dim_klimastadt),
    ("gruppe3_dim_gemeinde", fill_dim_gemeinde),
    ("gruppe3_fact_bevoelkerung", fill_fact_bevoelkerung),
    ("gruppe3_fact_bauland", fill_fact_bauland),
    ("gruppe3_fact_klima", fill_fact_klima),
    ("gruppe3_fact_gemeinde_stamm", fill_fact_gemeinde_stamm),
    ("gruppe3_fact_standortprofil_kpi", fill_fact_standortprofil_kpi),
]


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")

    for table_name, statement in STEPS:
        print(f"Fuelle {table_name} ...")
        cur.execute(statement)
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"  -> OK ({count} Zeilen)")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
