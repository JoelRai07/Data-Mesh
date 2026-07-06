"""
Orchestriert den kompletten End-to-End-Lauf: Datenmodell -> source -> staging
-> audit -> target (s. Docstrings der einzelnen Module fuer die fachlichen
Details je Stufe). Ein Aufruf statt vier einzelner Skript-Starts - u.a. der
Docker-Einstiegspunkt (s. Dockerfile/docker-compose.yml, Service "pipeline").

create_datamodel.py wird JEDES Mal mit ausgefuehrt, nicht nur beim ersten
Lauf: alle dortigen Statements sind CREATE TABLE IF NOT EXISTS, also
idempotent und bei bereits vorhandenen Tabellen praktisch kostenlos (nur ein
Metadaten-Check je Tabelle). Das nimmt die bisherige stille Abhaengigkeit
weg, dass jemand create_datamodel.py VOR dem ersten run_pipeline.py-Lauf
manuell ausgefuehrt haben muss - pipeline_audit_to_target.py wuerde sonst
beim TRUNCATE einer nicht existierenden Zieltabelle scheitern.

Jede Stufe entscheidet WEITERHIN SELBST per gruppe3_etl_state (s.
src/etl_state.py), ob sie tatsaechlich etwas zu tun hat oder den Lauf
uebersprungen kann (Incremental Loading) - dieses Skript aendert daran
nichts, es ruft nur alle drei main()-Funktionen in der richtigen
Reihenfolge nacheinander auf.

WARUM KEIN try/except UM DIE EINZELNEN STUFEN?
  Die Reihenfolge ist zwingend: Stufe 2 liest, was Stufe 1 geschrieben hat,
  Stufe 3 liest, was Stufe 2 geschrieben hat. Bricht z.B. Stufe 1 mit einer
  Exception ab, soll der GESAMTE Prozess mit einem Non-Zero-Exit-Code
  abbrechen (wichtig fuer Docker/Compose sowie einen etwaigen Cron/Scheduler-
  Exit-Code-Check) - nicht stillschweigend mit Stufe 2/3 auf veralteten Daten
  weiterlaufen.

Ausfuehren:  .venv/Scripts/python.exe src/run_pipeline.py
"""
import create_datamodel
import pipeline_default_to_staging
import pipeline_staging_to_audit
import pipeline_audit_to_target


def main():
    print("=" * 60)
    print("STUFE 0/3: Datenmodell (CREATE TABLE IF NOT EXISTS)")
    print("=" * 60)
    create_datamodel.main()

    print("\n" + "=" * 60)
    print("STUFE 1/3: source -> staging")
    print("=" * 60)
    pipeline_default_to_staging.main()

    print("\n" + "=" * 60)
    print("STUFE 2/3: staging -> audit")
    print("=" * 60)
    pipeline_staging_to_audit.main()

    print("\n" + "=" * 60)
    print("STUFE 3/3: audit -> target")
    print("=" * 60)
    pipeline_audit_to_target.main()

    print("\n" + "=" * 60)
    print("Gesamter Pipeline-Lauf abgeschlossen.")
    print("=" * 60)


if __name__ == "__main__":
    main()
