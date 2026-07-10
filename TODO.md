# TODO (Stand: 10.07.2026)

Nur offene Aufgaben. Entscheidungen & Begründungen: [docs/entscheidungen.md](docs/entscheidungen.md) ·
Benutzung/Überblick: [README.md](README.md) · Konsumenten-Sicht: [docs/data_contract.yaml](docs/data_contract.yaml)

## Vor der Abgabe (Pflicht)

- [ ] **Stufe 3 nach dem Shadow-Swap-Umbau bestätigen** (braucht Rechner mit
      JDK 17 + `ImpalaJDBC42.jar`): einmal `FORCE_TARGET_BUILD=1` +
      `src/pipeline_audit_to_target.py` laufen lassen — der neue atomare
      Publish (`overwrite_table` via `*_wap_incoming`-Shadow) besteht aus
      einzeln live getesteten Statements, ist im Ganzen aber noch nicht
      gelaufen. Stufen 1+2, Migration und Contract-Gate (32/32) sind am
      10.07. live verifiziert (s. README/ADR.md).
- [ ] **End-to-End-Abnahmetest:** `src/utils/reset_database.py` → `src/run_pipeline.py` →
      zweiter Lauf direkt danach muss die unveränderten Stufen überspringen; am Ende muss
      `src/contract_check.py` mit 0 Fehlern bestehen.
- [ ] **Data Contract CLI final testen:** realen Impala-Host in
      `docs/data_contract.yaml` setzen, `DATACONTRACT_IMPALA_*` in `.env`
      pflegen und `datacontract lint` + `datacontract test --server production`
      ausführen. Aktueller Befund: `lint` und `import sql` funktionieren;
      `test` scheitert lokal am CLI-Impala-Transport (`TSocket read 0 bytes`),
      nicht am Contract.
- [ ] **Abgabe-Hygiene klären (Team):** bleiben `docs/coursematerial/` (13 MB Prof-Folien),
      `reference/` und diese `TODO.md` im Abgabe-Repo?

## Data Contract

- [x] Data Contract erstellt (`docs/data_contract.yaml`) — 06.07.
- [x] Data Contract technisch enforced: `src/contract_check.py` prüft Schema,
      `required`, einfache Eindeutigkeit und ausführbare `quality`-SQLs live gegen Impala;
      `src/run_pipeline.py` ruft den Check als Stage 4/4 auf.
- [x] Data Contract CLI berücksichtigt: `docs/output_port_ddl.sql` als SQL-Basis
      für `datacontract import sql`; README enthält `lint`, `import sql` und
      `test`-Befehle.
- [x] Isolierter Contract-Check getestet: 32 Checks OK, 0 fehlgeschlagen.
- [ ] Optional härten: weitere ausführbare `quality`-SQLs ergänzen, z.B. Row-Count-Minima,
      FK-Integrität und Composite-Key-Checks für `fact_bevoelkerung` und `fact_klima`.

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
      Stufe 1+2, Parquet (Dateiformat) vs. Iceberg (Tabellenformat) und was
      Iceberg konkret bringt (MERGE/DELETE, atomare Snapshots, Time Travel,
      hidden partitioning), NULL-Semantik, warum Data Contract CLI als
      Abgabe-Contract plus eigenes Pipeline-Gate.

## Erledigt (Auszug)

- [x] **Komplette Pipeline auf Apache Iceberg + Incremental bestätigt (10.07., Duc):**
      alle 20 Tabellen migriert (`src/utils/migrate_to_iceberg.py`, Zeilenzahlen
      verifiziert), zeilengenauer Merge jetzt zustandslos (Iceberg `DELETE`+`MERGE INTO`
      mit `<=>`-Vergleich, Zeilen-Hash-Historie entfällt), ETL-State als Upsert,
      atomarer Publish per Shadow-Swap, Klimadaten nach Jahr partitioniert;
      Skip- und Änderungspfad live gegen die echte DB getestet (s. `ADR.md` Iteration 4).
      Der kurzzeitige Full-Load-Rückbau vom 10.07. ist revidiert
      (Branch `backup/full-load-rueckbau-2026-07-10`).
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
