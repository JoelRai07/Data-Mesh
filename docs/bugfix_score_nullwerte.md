# Bugfix: `standortattraktivitaets_score` war komplett NULL

## Symptom
In `gruppe3_fact_standortprofil_kpi` (4720 Zeilen) war die Spalte
`standortattraktivitaets_score` in **allen** Zeilen NULL — obwohl die
Eingangsspalten (Wachstum, Preis, Klima-Abweichung) für viele Zeilen Werte
hatten. Gemessen:

```
standortattraktivitaets_score      0 / 4720 nicht-NULL
```

Zum Vergleich die anderen KPI-Spalten (teilweise NULL, aber NICHT komplett):
```
wohnraumdruck_index             3517 / 4720
baulandpreis_pro_kopf_eur       3929 / 4720
verstaedterung_index            2420 / 4720
```

## Ursache
Der Score ist ein z-Score:
`(Wert − AVG(Wert) OVER (PARTITION BY jahr)) / STDDEV(Wert) OVER (PARTITION BY jahr)`,
summiert über Wachstum, Preis und Klima-Abweichung.

Zwei Effekte greifen ineinander:

1. **Division durch 0 erzeugt Infinity.** `preis_pro_qm_eur = kaufsumme / flaeche`.
   **748** Bauland-Zeilen haben `veraeusserte_flaeche_1000qm = 0` (die Fläche ist
   in *1000 qm* angegeben; kleine Grundstücke < 500 qm runden auf 0, die
   Kaufsumme bleibt aber echt). `kaufsumme / 0 = Infinity`.

2. **Infinity/NaN vergiftet Fensteraggregate.** `AVG()` und `STDDEV()` über ein
   Fenster ergeben **komplett NaN**, sobald auch nur EIN Infinity-/NaN-Wert im
   Fenster liegt (anders als NULL, das übersprungen wird). Da jede Zeile eines
   Jahrgangs denselben (vergifteten) Jahres-Durchschnitt benutzt, wird der Score
   für den **ganzen Jahrgang** NaN.

3. Beim Zurückschreiben wandelt `_sql_literal(...)` NaN → NULL. → gesamte Spalte
   NULL.

„Ein einziger Infinity-Wert pro Jahr genügt, um den ganzen Jahrgang zu kippen"
— deshalb 0 von 4720, obwohl ~3900 Zeilen einen gültigen Preis haben.

## Lösung
Neue Hilfsfunktion `safe_div(numerator, denominator)` in `pipeline_spark.py`:
liefert **NULL statt Infinity/NaN**, wenn der Nenner 0 oder NULL ist.

```python
def safe_div(numerator, denominator):
    return F.when((denominator == 0) | denominator.isNull(), None).otherwise(
        numerator / denominator
    )
```

`safe_div` ersetzt alle Divisionen mit variablem Nenner:
- `build_fact_bevoelkerung`: `geschlechterquotient`, `wachstum_vorjahr_pct`
- `build_fact_bauland`: `preis_pro_qm_eur`, `anteil_baureif_pct`, `durchschnittsfall_qm`
- `build_fact_gemeinde_stamm`: `anteil_weiblich_pct`
- `build_fact_standortprofil_kpi`: `faelle_wachstum_pct`, `wohnraumdruck_index`,
  `baulandpreis_pro_kopf_eur`, `freiflaeche_pro_einwohner_qm`,
  `klima_angepasstes_wohnraumrisiko`, `verstaedterung_index` und alle drei
  z-Score-Terme des `standortattraktivitaets_score`.

Damit entsteht nirgends mehr Infinity → die Jahres-Aggregate bleiben sauber →
der Score wird für Zeilen mit gültigen Eingaben berechnet. Wo eine Eingabe fehlt
(oder die Jahres-Streuung 0 ist), bleibt der Score bewusst NULL (statt eine ganze
Spalte zu kippen).

## Zweite (eigentliche) Ursache: Klima-Brücke ohne Koordinaten
Nach dem `safe_div`-Fix war der Score **immer noch** komplett NULL. Grund: eine
**zweite, unabhängige** Ursache.

Der Score ist `A − B − C` (Wachstum − Preis − |Klima-Abweichung|). Ist auch nur
ein Term NULL, ist die ganze Summe NULL. Der Klima-Term `C` war für **jede** Zeile
NULL, weil das Klima nie auf Kreis-Ebene ankommt:

- `dim_gemeinde` hat **0** gültige `latitude`/`longitude` (von 10.847 Zeilen).
- Ursache: In `project_gemeinden` sind die Koordinaten durch einen CSV-Bug
  zerstört. Sie nutzen **Komma als Dezimaltrennzeichen** (`9,13735`), die CSV ist
  aber ebenfalls komma-getrennt → jede Koordinate wurde mittendrin zerrissen:
  `latitude = '13735"'`, `longitude = '"9'` (mit Anführungszeichen-Resten).
  → `CAST(... AS DOUBLE)` ergibt für **alle** Zeilen NULL.
- Ohne Gemeinde-Koordinaten findet der „nächste-Klimastadt"-Join nichts →
  `temperatur_abweichung_grad` ist überall 0 → dessen Jahres-STDDEV = 0 → der
  z-Score-Term `C` = NULL → Score = `A − B − NULL` = NULL.

**Fix (Robustheit):** Der Klima-Term wird mit `F.coalesce(..., F.lit(0.0))`
abgesichert. Fehlt das Klima, zählt es neutral (0) statt den ganzen Score zu
nullen. Danach ist `Score = A − B`, gefüllt wo Wachstum und Preis vorhanden sind.

### Richtige Lösung: intakte Koordinaten aus `default` lesen
Die Zerstörung ist **nur in der `gruppe3`-Kopie** passiert. Die Original-Tabelle
**`default.project_gemeinden`** ist intakt — dort stehen die Koordinaten im
korrekten Format `9,43751` / `54,78252` (10.949 von 10.950 in Zahlen wandelbar,
live geprüft). Fix in `build_dim_gemeinde` und `build_fact_gemeinde_stamm`:

- Quelle: `read_table(spark, "default.project_gemeinden")` statt der kaputten
  `gruppe3_project_gemeinden`.
- Koordinaten parsen: `regexp_replace(col, ",", ".").cast("double")`
  (deutsches Dezimalkomma → Punkt).

**Achtung, zwei verschiedene Koordinaten-Formate:**
- **Gemeinden:** `9,43751` — Dezimal**komma**, keine Himmelsrichtung → Komma→Punkt.
- **Klimadaten:** `106.55E` / `5.63S` — Dezimal**punkt** + Himmelsrichtung (N/S/E/W)
  → Buchstabe entfernen, bei S/W negieren (passiert in `build_dim_klimastadt`).

Damit bekommt `dim_gemeinde` echte Koordinaten → die nächste-Klimastadt-Brücke
funktioniert → das Klima erreicht die Kreise → der Klima-Term im Score ist echt.

Das `coalesce(..., 0)` aus dem vorigen Schritt bleibt als **Sicherheitsnetz**
(falls ein Kreis doch keine Klimastadt zugeordnet bekommt), ist aber nun nicht
mehr die Hauptlösung.

## Noch auszuführen
Der Fix ist reiner Code. Die Tabelle wird erst mit dem **nächsten Pipeline-Lauf**
neu befüllt (`.venv/Scripts/python.exe src/pipeline_spark.py`, braucht die
Spark-Umgebung: JDK 17 + `src/utils/ImpalaJDBC42.jar`). Danach gegenprüfen:

```sql
SELECT COUNT(standortattraktivitaets_score) FROM gruppe3_fact_standortprofil_kpi;
-- sollte deutlich > 0 sein
```

## Abgrenzung: die „normalen" NULLs
Die restlichen NULLs in den anderen KPI-Spalten sind **kein Bug**, sondern echte
fehlende Daten und gehören in den Data Contract dokumentiert:
- Baulandwerte sind für kleine Kreise amtlich unterdrückt → kein Preis.
- Wachstum fehlt im ersten Jahr (kein Vorjahr).
- `verstaedterung_index` fehlt, wo keine Gemeinde-Dichte zum Kreis passt.

Auch `flaeche = 0` bei vorhandener Kaufsumme ist ein dokumentierenswertes
Datenqualitäts-Detail (Rundung auf 1000-qm-Einheiten).
