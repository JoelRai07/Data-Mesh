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

**Zentrale Erkenntnis 2:** `project_gemeinden` und `project_klimadaten` lassen
sich über die Gemeinde-Ebene verbinden: Nach der Bereinigung in der Audit-Stufe
(Transliteration ä→ae/…, englische Exonyme wie „Munich"→„Muenchen" gemappt)
liegen Gemeindenamen und Klimastadt-Namen im **selben Format** vor — die
Verknüpfung erfolgt per **exaktem Namens-Match** (deterministisch, trifft 74
von 81 deutschen Klimastädten; der Rest sind mehrdeutige Kurzformen wie
„Frankfurt" und bleibt bewusst ohne Klimawert). `project_gemeinden` wird damit
zur **Brücken-Dimension** (`dim_gemeinde`), die Kreis-Ebene
(Bevölkerung/Bauland) und Stadt-Ebene (Klima) verbindet — alle vier Quellen
sind so in einem Modell nutzbar, nicht nur isoliert nebeneinander.
Beide Tabellen haben zusätzlich `latitude`/`longitude` (normalisiert in den
Dimensionen gespeichert); ein früherer Ansatz nutzte die **räumliche Nähe**
(nächste Klimastadt per lat/long-Distanz) — warum wir auf den Namens-Match
gewechselt sind: s. `entscheidungen.md`, ADR-7.

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
dim_gemeinde ──(Namens-Match, transliteriert)──> dim_klimastadt ──< fact_klima >── dim_jahr
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
   gegen `dim_kreis.kreis_name` (Anbindung an Kreis-Ebene) und per Namens-Match
   gegen `dim_klimastadt.stadt_name` (Anbindung an Klima — beide Seiten in der
   Audit-Stufe auf dieselbe ASCII-Schreibweise gebracht). So wird aus vier
   losen Tabellen ein zusammenhängendes Modell.

4. **Unpivot der Bevölkerungsdaten (breit → lang).**
   Die Quelle hat eine Spalte pro Jahr (`insgesamt_24`, `insgesamt_23`, …). Für ein
   sauberes Faktenmodell wird daraus eine Zeile je Kreis **und Jahr** — so ist `jahr`
   eine echte Dimension und Zeitreihen-Analysen sind trivial.

5. **Pivot der Baulanddaten (lang → breit).**
   Die Quelle hat eine Zeile je Merkmal. Live geprüft gibt es **4** distinkte
   Merkmale: `Veräußerungsfälle von Bauland`, `Veräußerte Baulandfläche`,
   `Kaufsumme` und `Durchschnittlicher Kaufwert je qm`. Die Pipeline pivotiert
   alle 4 in Spalten: `kaufwert_je_qm_eur` übernimmt den 4. (amtlichen) Wert
   direkt, zusätzlich bleibt `preis_pro_qm_eur` als selbst berechnete KPI aus
   Kaufsumme/Fläche erhalten, damit amtlicher und berechneter Wert vergleichbar
   sind. Ziel bleibt genau **eine** Faktenzeile je Kreis+Jahr, direkt vergleichbar.

6. **KPI-Spalten direkt in den Basisfakten.**
   Wo eine Kennzahl aus Spalten **derselben** Zeile berechenbar ist (z. B.
   `preis_pro_qm_eur` aus `kaufsumme_tsd_eur` und `veraeusserte_flaeche_1000qm`
   in `fact_bauland`), wird sie als zusätzliche Spalte in der Faktentabelle
   gepflegt, statt sie bei jeder Abfrage neu zu berechnen.

7. **`fact_standortprofil_kpi` als aggregierte Cross-Table-Faktentabelle.**
   Manche Kennzahlen ergeben erst durch den **Join mehrerer Fakten** Sinn (z. B.
   `wohnraumdruck_index` = Einwohner je 1000qm neu veräußerter Baulandfläche,
   oder `standortattraktivitaets_score` aus Bevölkerung + Bauland + Klima).
   Diese würden bei jeder Dashboard-Abfrage einen teuren Mehrfach-Join über
   `fact_bevoelkerung`, `fact_bauland`, `fact_klima` und `fact_gemeinde_stamm`
   erfordern. Wir berechnen sie daher einmal in der Pipeline vor und speichern
   sie als eigene, dashboard-fertige Faktentabelle auf Kreis × Jahr-Ebene.

8. **Speicherung als Apache Iceberg (Datendateien: Parquet).**
   Spaltenorientiertes Parquet → ideal für OLAP (es werden nur die benötigten
   Spalten gelesen, "data skipping"; vgl. Vorlesung 1, Folien 14–24). Apache
   Iceberg legt darüber eine Snapshot-/Metadaten-Schicht: das Datenprodukt
   wird beim Publish **atomar** ersetzt (Konsumenten sehen nie einen halb
   geschriebenen Stand), frühere Stände bleiben per Time Travel
   (`FOR SYSTEM_TIME AS OF`) abfragbar, und die Pipeline kann zeilengenaues
   `MERGE INTO`/`DELETE` nutzen (s. `entscheidungen.md` ADR-8 und `ADR.md`).

9. **Bewusste Abgrenzung (Scope & Datenqualität).**
   - `project_gemeinden` hat kaputtes CSV-Parsing (Kommas in Gemeindenamen
     sprengen das Quoting) und keinen amtlichen Schlüssel. Die Zuordnung zu
     `dim_kreis` erfolgt daher per Namens-Match und ist fehlerbehaftet — im
     [Data Contract](data_contract.yaml) mit Coverage-Quote dokumentiert
     (97,5 %: 10.675 von 10.947 Gemeinden zugeordnet), nicht verschwiegen.
   - `project_klimadaten` hat keinen Regionalschlüssel (weltweite Städte) und
     wird per Namens-Match an `dim_gemeinde` angebunden — 74 von 81 deutschen
     Klimastädten, kein exakter Schlüssel-Join (ebenfalls im Data Contract
     dokumentiert; zur Historie Distanz- vs. Namens-Join s.
     `entscheidungen.md`, ADR-7).
   - Beide Einschränkungen entsprechen dem Data-Mesh-Prinzip „Federated
     Governance": Datenqualität wird ehrlich beschrieben statt ignoriert.

## Idempotenz
Alle DDLs nutzen `CREATE TABLE IF NOT EXISTS`. Die Befüllung ist inkrementell
(s. `src/etl_state.py` und die Modul-Docstrings der drei Pipeline-Stufen):
Zeitreihen-Tabellen (`klimadaten`) werden per Wasserzeichen nur um neue Zeilen
ergänzt (`INSERT INTO`, Iceberg-partitioniert nach Jahr), `bauland`/
`bevoelkerungzahlen` werden zeilengenau per Iceberg `DELETE`+`MERGE INTO`
nachgeführt, `gemeinden` und das Star-Schema werden nur bei tatsächlicher
Änderung per atomarem `INSERT OVERWRITE`-Snapshot komplett neu geschrieben.
In allen Fällen erzeugt mehrfaches Ausführen keine Duplikate; unveränderte
Quellen werden übersprungen.
