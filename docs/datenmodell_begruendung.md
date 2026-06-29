# Begründung des Datenmodells

## Use Case
**Standortprofil-Dashboard**: Bewertung von Kreisen/Gemeinden hinsichtlich
Bevölkerungsentwicklung, Bauland-/Wohnungsmarkt und Klimawandel-Exposition —
z. B. zur Identifikation von Standorten mit Wohnraumdruck oder Klimarisiko.

## Ausgangslage
Vier Quell-Tabellen (Rohdaten) stehen auf Impala bereit:

| Quelle | Ebene | Verknüpfungs-Schlüssel | Format |
|---|---|---|---|
| `project_bevoelkerungzahlen` | Kreis | `id` (Regionalschlüssel) | **breit** (1 Spalte je Jahr) |
| `project_bauland` | Kreis | `kreis_id` (Regionalschlüssel) | **lang** (1 Zeile je Merkmal) |
| `project_gemeinden` | Gemeinde | nur Name (kein Schlüssel), aber `latitude`/`longitude` | flach, mit Parsing-Fehlern |
| `project_klimadaten` | Stadt (weltweit) | nur Stadtname, aber `latitude`/`longitude` | lang, sehr groß (8,6 Mio.) |

**Zentrale Erkenntnis 1:** `bevoelkerung.id` und `bauland.kreis_id` sind derselbe
**amtliche Regionalschlüssel** (z. B. `01001` = Flensburg). Sie matchen exakt
(472 Kreise mit 5-stelligem Schlüssel, live gegen beide Quelltabellen geprüft)
und sind hierarchisch: die ersten 2 Stellen kodieren das Bundesland
(`01` = Schleswig-Holstein). → Das ist unser natürlicher Integrationsschlüssel auf
Kreis-Ebene.

**Zentrale Erkenntnis 2:** `project_gemeinden` und `project_klimadaten` haben
beide `latitude`/`longitude`. Damit lässt sich die sonst fehlende Verknüpfung
zwischen Gemeinde-Ebene und Klimastadt-Ebene über **räumliche Nähe** (nächste
Stadt anhand einfacher euklidischer Distanz auf lat/long, bewusst ohne
Haversine-Trigonometrie — für die geografische Ausdehnung Deutschlands
ausreichend genau) statt über unsicheres Namens-Matching herstellen.
`project_gemeinden` wird damit zur **Brücken-Dimension** (`dim_gemeinde`),
die Kreis-Ebene (Bevölkerung/Bauland) und Stadt-Ebene (Klima) verbindet — alle
vier Quellen sind so in einem Modell nutzbar, nicht nur isoliert nebeneinander.

## Gewähltes Modell: Star-Schema
- **4 Dimensionen:** `dim_kreis` (Geografie), `dim_jahr` (Zeit), `dim_gemeinde`
  (Brücke Kreis ↔ Klimastadt), `dim_klimastadt` (deutsche Städte aus den
  Klimadaten)
- **5 Fakten:** `fact_bevoelkerung`, `fact_bauland`, `fact_klima`,
  `fact_gemeinde_stamm` (Gemeinde-Stammdaten, Snapshot ohne Jahr-Dimension),
  `fact_standortprofil_kpi` (aggregierte Cross-Table-Kennzahlen für das
  Dashboard)

```
dim_kreis ──< fact_bevoelkerung   >── dim_jahr
dim_kreis ──< fact_bauland        >── dim_jahr
dim_kreis ──< dim_gemeinde ──< fact_gemeinde_stamm
dim_gemeinde ──(nächste Stadt, lat/long)──> dim_klimastadt ──< fact_klima >── dim_jahr
dim_kreis ──< fact_standortprofil_kpi >── dim_jahr   (verdichtet alle o.g. Fakten)
```

## Warum so? (die Begründung für die Prüfung)

1. **Star-Schema statt normalisiert (3. NF).**
   Analytische (OLAP-)Systeme lesen Aggregate über viele Zeilen. Normalisierte
   Modelle erzwingen viele Joins → komplexe, langsame, in der Cloud *teure*
   Abfragen. Im Star-Schema sind die Dimensionen **denormalisiert** (z. B. Bundesland
   direkt in `dim_kreis`), das reduziert Joins. (vgl. Vorlesung 1, Folien 33–35)

2. **Regionalschlüssel als „conformed dimension".**
   `dim_kreis` ist die gemeinsame, abgestimmte Dimension, über die sich
   `fact_bevoelkerung` und `fact_bauland` verbinden lassen. Das entspricht dem
   Data-Mesh-Prinzip, durch *Interconnecting* höheren Wert zu schaffen.

3. **`dim_gemeinde` als zweite conformed dimension (Brücke).**
   Ohne `dim_gemeinde` bliebe `project_klimadaten` isoliert, da es keinen
   Regionalschlüssel besitzt. `dim_gemeinde` löst das doppelt: per Namens-Match
   gegen `dim_kreis.kreis_name` (Anbindung an Kreis-Ebene) und per räumlicher
   Nähe gegen `dim_klimastadt` (Anbindung an Klima). So wird aus vier losen
   Tabellen ein zusammenhängendes Modell.

4. **Unpivot der Bevölkerungsdaten (breit → lang).**
   Die Quelle hat eine Spalte pro Jahr (`insgesamt_24`, `insgesamt_23`, …). Für ein
   sauberes Faktenmodell wird daraus eine Zeile je Kreis **und Jahr** — so ist `jahr`
   eine echte Dimension und Zeitreihen-Analysen sind trivial.

5. **Pivot der Baulanddaten (lang → breit).**
   Die Quelle hat eine Zeile je Merkmal. Live geprüft gibt es **4** distinkte
   Merkmale: `Veräußerungsfälle von Bauland`, `Veräußerte Baulandfläche`,
   `Kaufsumme` und `Durchschnittlicher Kaufwert je qm`. Aktuell pivotiert die
   Pipeline nur die ersten 3 in Spalten und berechnet `preis_pro_qm_eur` selbst
   aus Kaufsumme/Fläche, statt den 4. (amtlichen) Wert direkt zu übernehmen —
   **bekannte Lücke, noch zu schließen** (s. README/offene Punkte). Ziel bleibt
   genau **eine** Faktenzeile je Kreis+Jahr, direkt vergleichbar.

6. **KPI-Spalten direkt in den Basisfakten.**
   Wo eine Kennzahl aus Spalten **derselben** Zeile berechenbar ist (z. B.
   `preis_pro_qm_eur` aus `kaufsumme_tsd_eur` und `veraeusserte_flaeche_1000qm`
   in `fact_bauland`), wird sie als zusätzliche Spalte in der Faktentabelle
   gepflegt, statt sie bei jeder Abfrage neu zu berechnen.

7. **`fact_standortprofil_kpi` als aggregierte Cross-Table-Faktentabelle.**
   Manche Kennzahlen ergeben erst durch den **Join mehrerer Fakten** Sinn (z. B.
   `wohnraumdruck_index` = Bevölkerungswachstum vs. Bauland-Angebotswachstum,
   oder `standortattraktivitaets_score` aus Bevölkerung + Bauland + Klima).
   Diese würden bei jeder Dashboard-Abfrage einen teuren Mehrfach-Join über
   `fact_bevoelkerung`, `fact_bauland`, `fact_klima` und `fact_gemeinde_stamm`
   erfordern. Wir berechnen sie daher einmal in der Pipeline vor und speichern
   sie als eigene, dashboard-fertige Faktentabelle auf Kreis × Jahr-Ebene.

8. **Speicherung als Parquet.**
   Spaltenorientiertes Format → ideal für OLAP (es werden nur die benötigten Spalten
   gelesen, "data skipping"). (vgl. Vorlesung 1, Folien 14–24)

9. **Bewusste Abgrenzung (Scope & Datenqualität).**
   - `project_gemeinden` hat kaputtes CSV-Parsing (Kommas in Gemeindenamen
     sprengen das Quoting) und keinen amtlichen Schlüssel. Die Zuordnung zu
     `dim_kreis` erfolgt daher per Namens-Match und ist fehlerbehaftet — das
     wird im Data Contract mit einer Coverage-Quote dokumentiert, nicht
     verschwiegen.
   - `project_klimadaten` hat keinen Regionalschlüssel (weltweite Städte) und
     wird über die nächstgelegene Stadt (lat/long-Distanz) an `dim_gemeinde`
     angebunden — ebenfalls eine Näherung, kein exakter Schlüssel-Join.
   - Beide Einschränkungen entsprechen dem Data-Mesh-Prinzip „Federated
     Governance": Datenqualität wird ehrlich beschrieben statt ignoriert.

## Idempotenz
Alle DDLs nutzen `CREATE TABLE IF NOT EXISTS`; die Befüllung (Pipeline) überschreibt
(`INSERT OVERWRITE`) je Lauf. Mehrfaches Ausführen erzeugt damit keine Duplikate.
