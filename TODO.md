# TODO (Stand: 06.07.2026)

Nur offene Aufgaben. Entscheidungen & Begründungen: [docs/entscheidungen.md](docs/entscheidungen.md) ·
Benutzung/Überblick: [README.md](README.md) · Konsumenten-Sicht: [docs/data_contract.yaml](docs/data_contract.yaml)

## Vor der Abgabe (Pflicht)

- [ ] **End-to-End-Abnahmetest:** `src/utils/reset_database.py` → `src/run_pipeline.py` →
      zweiter Lauf direkt danach muss die unveränderten Stufen überspringen; am Ende muss
      `src/contract_check.py` mit 0 Fehlern bestehen.
- [ ] **Abgabe-Hygiene klären (Team):** bleiben `docs/coursematerial/` (13 MB Prof-Folien),
      `reference/` und diese `TODO.md` im Abgabe-Repo?

## Data Contract

- [x] Data Contract erstellt (`docs/data_contract.yaml`) — 06.07.
- [x] Data Contract technisch enforced: `src/contract_check.py` prüft Schema,
      `required`, einfache Eindeutigkeit und ausführbare `quality`-SQLs live gegen Impala;
      `src/run_pipeline.py` ruft den Check als Stage 4/4 auf.
- [x] Isolierter Contract-Check getestet: 31 Checks OK, 0 fehlgeschlagen.
- [ ] Optional härten: weitere ausführbare `quality`-SQLs ergänzen, z.B. Row-Count-Minima,
      FK-Integrität und Composite-Key-Checks für `fact_bevoelkerung` und `fact_klima`.
- [ ] Optional, falls explizit Data Contract CLI verlangt wird: `datacontract lint` ist
      kompatibel; für `datacontract test` müssten zusätzlich `datacontract-cli[impala]`,
      `DATACONTRACT_IMPALA_*`-Variablen und die Impala-Serverdetails im Contract gepflegt werden.
      Für die Abgabe ist das eigene `contract_check.py`-Gate pragmatischer und bereits lauffähig.

## Fachliche Verbesserungen (wenn Zeit)

- [ ] `dim_kreis.kreis_name` mehrdeutig (3× „Leipzig", ~30 Stadt/Landkreis-Paare) →
      `kreis_typ`-Spalte oder Originalnamen zusätzlich behalten (bis dahin: Joins nur über `kreis_id`).
- [ ] Scheduler produktiv gedacht: auf Cloudera Data Engineering / Airflow heben
      (für die Abgabe nur als Ausblick in der Präsentation erwähnen).
- [ ] Docstring-/Doku-Kosmetik: verwaister „Entfernen statt transliterieren"-Absatz
      in `pipeline_staging_to_audit.py` prüfen.

## Präsentation (15 Min + 10 Min Fragen)

- [ ] Story festlegen: Use Case → Datenqualität der Quellen (Encoding und
      kaufwert-BIGINT-Schaden) → Datenmodell → WAP + Incremental Loading →
      Data Contract + technisches Gate → ehrliche Grenzen.
- [ ] Fragerunde üben: negative Scores (z-Score, gewollt), warum Impala-SQL in
      Stufe 1+2, warum kein UPDATE/MERGE in Impala (append-only State), NULL-Semantik,
      warum eigenes Contract-Gate statt Data Contract CLI.

## Erledigt (Auszug)

- [x] Scheduler finalisiert: täglicher Lauf um 00:00 Uhr und Aufruf von `run_pipeline.main()`.
- [x] Skip-Check-Bug gefixt: `record_state` wird nur bei tatsächlicher Änderung geschrieben.
- [x] `src/utils/ImpalaJDBC42.jar` aus dem Git-Index entfernt; Datei bleibt lokal per `.gitignore`.
- [x] `overwrite_table`: erst `collect()`, dann `TRUNCATE`, damit Tabellen nicht während der Spark-Berechnung leer sind.
- [x] `fact_gemeinde_stamm`: Quell-Duplikate auf `(municipality_name, postal_code)` werden vor dem Join dedupliziert.
- [x] Docstring-/Doku-Kosmetik: alte Spaltenzahl-Angaben auf 92 korrigiert.
- [x] Alle Docs auf aktuellen Stand gebracht (Klima-Join, Quelltabellen, Encoding-Status,
      Score-Nachtrag, `pipeline.py`-Verweise) + `docs/entscheidungen.md` (ADRs) — 06.07.
- [x] README komplett neu (WAP + Incremental + Lese-Reihenfolge) — 06.07.
- [x] Incremental Loader + `run_pipeline.py`-Orchestrator (Duc) — 06.07.
- [x] Encoding-Fix verifiziert: 0 kaputte Zeichen in allen Audit-Tabellen — 06.07.
- [x] Score-NULL-Bug (`safe_div` + Klima-`coalesce`) ausgerollt: 3.911/4.099 gefüllt.
- [x] KPIs auf Plausibilität geprüft (Minuswerte beim z-Score sind gewollt).
- [x] Leere Zeilen im Datenprodukt entfernt, negative Indexe gefixt.
