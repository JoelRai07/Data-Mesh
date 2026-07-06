# TODO (Stand: 06.07.2026)

Nur offene Aufgaben. Entscheidungen & Begründungen: [docs/entscheidungen.md](docs/entscheidungen.md) ·
Benutzung/Überblick: [README.md](README.md) · Konsumenten-Sicht: [docs/data_contract.yaml](docs/data_contract.yaml)

## Vor der Abgabe (Pflicht)

- [ ] **Scheduler finalisieren:** `CronTrigger(minute="*")` → `CronTrigger(hour=0, minute=0)`
      **und** auf `run_pipeline.main()` umstellen — er ruft aktuell nur Stufe 3 auf;
      mit dem neuen Skip-Check würde er sonst dauerhaft überspringen (`src/scheduler.py`)
- [ ] **Skip-Check-Bug fixen:** `stage_table_incremental`/`audit_table_incremental`
      schreiben ihr Wasserzeichen bei **jedem** Lauf → `should_skip_target_build()`
      greift im Komplettlauf nie. Fix: `record_state` nur bei tatsächlicher Änderung
- [ ] **End-to-End-Abnahmetest:** `utils/reset_database.py` → `run_pipeline.py` →
      zweiter Lauf direkt danach muss überall „übersprungen" melden
- [ ] **Abgabe-Hygiene klären (Team):** bleiben `docs/coursematerial/` (13 MB Prof-Folien),
      `reference/` und diese TODO.md im Abgabe-Repo?

## Fachliche Verbesserungen (wenn Zeit)

- [ ] `dim_kreis.kreis_name` mehrdeutig (3× „Leipzig", ~30 Stadt/Landkreis-Paare) →
      `kreis_typ`-Spalte oder Originalnamen zusätzlich behalten (bis dahin: Joins nur über `kreis_id`)
- [ ] `fact_gemeinde_stamm`: 3 doppelte `gemeinde_id` (Quell-Duplikate auf Name+PLZ) →
      vor dem Join deduplizieren
- [ ] `overwrite_table`: erst `collect()`, dann `TRUNCATE` (aktuell ist die Zieltabelle
      während der ganzen Spark-Berechnung leer)
- [ ] Audit-Stufe um echte **Quality-Checks als Gate** erweitern (WAP vollständig machen);
      die `quality.query`-Einträge im Data Contract sind fertige Kandidaten dafür
- [ ] Scheduler produktiv gedacht: auf Cloudera Data Engineering / Airflow heben
      (für die Abgabe nur als Ausblick in der Präsentation erwähnen)
- [ ] Docstring-Kosmetik: „83 Spalten" → 92 (bevoelkerungzahlen), „ca. 59 Städte
      matchen" → 74, verwaister „Entfernen statt transliterieren"-Absatz in
      `pipeline_staging_to_audit.py`

## Präsentation (15 Min + 10 Min Fragen)

- [ ] Story festlegen: Use Case → Datenqualität der Quellen (Encoding **und**
      kaufwert-BIGINT-Schaden!) → Datenmodell → WAP + Incremental Loading →
      Data Contract → ehrliche Grenzen
- [ ] Fragerunde üben: negative Scores (z-Score, gewollt), warum Impala-SQL in
      Stufe 1+2, warum kein UPDATE/MERGE in Impala (append-only State), NULL-Semantik

## Erledigt (Auszug)

- [x] Data Contract erstellt (`docs/data_contract.yaml`) — 06.07.
- [x] Alle Docs auf aktuellen Stand gebracht (Klima-Join, Quelltabellen, Encoding-Status,
      Score-Nachtrag, `pipeline.py`-Verweise) + `docs/entscheidungen.md` (ADRs) — 06.07.
- [x] README komplett neu (WAP + Incremental + Lese-Reihenfolge) — 06.07.
- [x] Incremental Loader + `run_pipeline.py`-Orchestrator (Duc) — 06.07.
- [x] Encoding-Fix verifiziert: 0 kaputte Zeichen in allen Audit-Tabellen — 06.07.
- [x] Score-NULL-Bug (`safe_div` + Klima-`coalesce`) ausgerollt: 3.911/4.099 gefüllt
- [x] KPIs auf Plausibilität geprüft (Minuswerte beim z-Score sind gewollt)
- [x] Leere Zeilen im Datenprodukt entfernt, negative Indexe gefixt
