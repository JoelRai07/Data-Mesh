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
