# Begründung des Datenmodells

## Ausgangslage
Vier Quell-Tabellen (Rohdaten) stehen auf Impala bereit:

| Quelle | Ebene | Verknüpfungs-Schlüssel | Format |
|---|---|---|---|
| `project_bevoelkerungzahlen` | Kreis | `id` (Regionalschlüssel) | **breit** (1 Spalte je Jahr) |
| `project_bauland` | Kreis | `kreis_id` (Regionalschlüssel) | **lang** (1 Zeile je Merkmal) |
| `project_gemeinden` | Gemeinde | nur Name (kein Schlüssel) | flach, mit Parsing-Fehlern |
| `project_klimadaten` | Stadt (weltweit) | nur Stadtname | lang, sehr groß (8,6 Mio.) |

**Zentrale Erkenntnis:** `bevoelkerung.id` und `bauland.kreis_id` sind derselbe
**amtliche Regionalschlüssel** (z. B. `01001` = Flensburg). Sie matchen exakt
(539 Kreise) und sind hierarchisch: die ersten 2 Stellen kodieren das Bundesland
(`01` = Schleswig-Holstein). → Das ist unser natürlicher Integrationsschlüssel.

## Gewähltes Modell: Star-Schema
- **2 Dimensionen:** `dim_kreis` (Geografie), `dim_jahr` (Zeit)
- **2 Fakten:** `fact_bevoelkerung`, `fact_bauland` (+ optional `fact_klima`)

```
dim_kreis ──< fact_bevoelkerung >── dim_jahr
dim_kreis ──< fact_bauland      >── dim_jahr
```

## Warum so? (die Begründung für die Prüfung)

1. **Star-Schema statt normalisiert (3. NF).**
   Analytische (OLAP-)Systeme lesen Aggregate über viele Zeilen. Normalisierte
   Modelle erzwingen viele Joins → komplexe, langsame, in der Cloud *teure*
   Abfragen. Im Star-Schema sind die Dimensionen **denormalisiert** (z. B. Bundesland
   direkt in `dim_kreis`), das reduziert Joins. (vgl. Vorlesung 1, Folien 33–35)

2. **Regionalschlüssel als „conformed dimension".**
   `dim_kreis` ist die gemeinsame, abgestimmte Dimension, über die sich alle Fakten
   verbinden lassen. Das entspricht dem Data-Mesh-Prinzip, durch *Interconnecting*
   höheren Wert zu schaffen.

3. **Unpivot der Bevölkerungsdaten (breit → lang).**
   Die Quelle hat eine Spalte pro Jahr (`insgesamt_24`, `insgesamt_23`, …). Für ein
   sauberes Faktenmodell wird daraus eine Zeile je Kreis **und Jahr** — so ist `jahr`
   eine echte Dimension und Zeitreihen-Analysen sind trivial.

4. **Pivot der Baulanddaten (lang → breit).**
   Die Quelle hat eine Zeile je Merkmal (Veräußerungsfälle, Fläche, Kaufsumme …).
   Wir drehen die 4 Merkmale in 4 Kennzahl-Spalten → genau **eine** Faktenzeile je
   Kreis+Jahr, direkt vergleichbar.

5. **Speicherung als Parquet.**
   Spaltenorientiertes Format → ideal für OLAP (es werden nur die benötigten Spalten
   gelesen, "data skipping"). (vgl. Vorlesung 1, Folien 14–24)

6. **Bewusste Abgrenzung (Scope & Datenqualität).**
   - `project_gemeinden` (Gemeinde-Ebene, kaputtes CSV-Parsing, kein Schlüssel) wird
     **nicht** als Fakt aufgenommen, sondern höchstens als optionale Zusatzinfo.
   - `project_klimadaten` hat keinen Regionalschlüssel (weltweite Städte) und lässt
     sich nur **lose über den Stadtnamen** anbinden. Daher optional als `fact_klima`
     auf Stadt-Ebene, mit dokumentierter Einschränkung im Data Contract
     (Data-Mesh-Prinzip „Federated Governance": Qualität ehrlich beschreiben).

## Idempotenz
Alle DDLs nutzen `CREATE TABLE IF NOT EXISTS`; die Befüllung (Pipeline) überschreibt
(`INSERT OVERWRITE`) je Lauf. Mehrfaches Ausführen erzeugt damit keine Duplikate.
