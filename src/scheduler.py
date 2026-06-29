"""
DELIVERABLE 2b: Scheduler - fuehrt die Spark-Pipeline (pipeline_spark.py) als
taeglichen Batch-Job um 00:00 Uhr aus.

Warum APScheduler statt z.B. Windows Task Scheduler / cron?
  - Scheduler-Code soll laut Aufgabenstellung Teil der Abgabe sein (im Repo
    lesbar und nachvollziehbar), nicht nur eine externe Konfiguration
    (Crontab-Zeile o.ae.), die ausserhalb des Codes liegt und beim Reviewer
    nicht ohne Weiteres sichtbar ist.
  - APScheduler ist reines Python, braucht keinen separaten Cron-Daemon und
    laeuft genauso auf Windows wie auf Linux - passend zu unserem Setup
    (lokale Windows-Entwicklungsumgebung + Impala in der Cloud).
  - Ein BlockingScheduler mit CronTrigger(hour=0, minute=0) bildet "jeden Tag
    um 00:00 Uhr" 1:1 ab, ohne eine eigene Sleep-Schleife zu programmieren.

WARUM JAVA_HOME HIER GESETZT WIRD:
  pipeline_spark.py startet eine JVM (PySpark) und braucht dafuer JAVA_HOME.
  Wenn man scheduler.py z.B. als Windows-Dienst oder per Autostart laufen
  laesst, ist JAVA_HOME dort NICHT automatisch gesetzt (das war bisher nur in
  der manuellen PowerShell-Session der Fall, in der wir getestet haben).
  Deshalb wird es hier zu Beginn explizit fuer den Python-Prozess gesetzt,
  BEVOR pipeline_spark importiert wird (Spark startet seine JVM erst beim
  ersten SparkSession-Aufruf, also rechtzeitig).

  WICHTIG: Der Pfad ist NICHT hartkodiert, sondern kommt aus der .env-Datei
  (Variable JAVA_HOME_JDK17, s. .env.example) - jeder im Team hat Java an
  einer anderen Stelle installiert, ein fester Pfad wuerde nur auf einem
  einzigen Rechner funktionieren. Ist die Variable nicht gesetzt, lassen wir
  das System-JAVA_HOME unangetastet (s. Stolperstein im
  docs/spark_stolpersteine.md zu JDK-Versionen - es muss JDK 17 sein).

Funktionsweise:
  - Dieses Skript startet einen Dauerlauf-Prozess (blockiert den Thread).
  - Jeden Tag um 00:00 Uhr wird pipeline_spark.main() aufgerufen.
  - Schlaegt ein Lauf fehl (z.B. Impala kurzzeitig nicht erreichbar), wird der
    Fehler geloggt, der Scheduler selbst laeuft aber weiter und versucht es
    am naechsten Tag erneut - ein einzelner Fehlschlag soll nicht den ganzen
    Scheduler-Prozess beenden.

Ausfuehren (laeuft dauerhaft, z.B. in eigenem Terminal/Service):
  .venv/Scripts/python.exe src/scheduler.py
"""
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# JAVA_HOME_JDK17 ist optional und kommt aus der lokalen .env jeder/jedes
# Teammitglieds (s. .env.example) - kein hartkodierter Pfad, da Java bei
# jedem woanders installiert ist. Ist die Variable nicht gesetzt, bleibt das
# bereits im System konfigurierte JAVA_HOME unangetastet.
JAVA_HOME = os.getenv("JAVA_HOME_JDK17")
if JAVA_HOME and os.path.isdir(JAVA_HOME):
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import pipeline_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("scheduler")


def run_pipeline_job():
    """Fuehrt einen kompletten Pipeline-Lauf aus und faengt Fehler ab,
    damit ein fehlgeschlagener Lauf den Scheduler nicht crasht."""
    logger.info("Starte geplanten Pipeline-Lauf ...")
    try:
        pipeline_spark.main()
        logger.info("Pipeline-Lauf erfolgreich abgeschlossen.")
    except Exception:
        # bewusst breites except: der Scheduler-Prozess soll auch nach einem
        # fehlgeschlagenen Lauf weiterleben und es am naechsten Tag erneut
        # versuchen (z.B. wenn Impala gerade nicht erreichbar war).
        logger.exception("Pipeline-Lauf fehlgeschlagen.")


def main():
    scheduler = BlockingScheduler(timezone="Europe/Berlin")
    # TEST-KONFIGURATION: laeuft aktuell jede Minute, um den Job ohne langes
    # Warten beobachten zu koennen. Fuer die Abgabe zurueck auf
    # CronTrigger(hour=0, minute=0) (taeglich um Mitternacht) stellen.
    trigger = CronTrigger(minute="*")
    scheduler.add_job(
        run_pipeline_job,
        trigger=trigger,
        id="daily_pipeline_run",
        name="Pipeline-Lauf (Testmodus: jede Minute)",
        misfire_grace_time=3600,  # bis zu 1h Verspaetung (z.B. nach Rechner-Standby) noch nachholen
    )

    # job.next_run_time ist erst gesetzt, NACHDEM der Scheduler gestartet
    # wurde (vorher wirft der Zugriff einen AttributeError, da next_run_time
    # ein __slots__-Attribut ist, das vor der ersten Berechnung schlicht
    # nicht existiert). Daher die naechste Ausfuehrungszeit direkt aus dem
    # Trigger berechnen, unabhaengig vom Job-internen Status.
    next_run = trigger.get_next_fire_time(None, datetime.now(scheduler.timezone))
    logger.info("Scheduler gestartet. Naechster Lauf: %s", next_run)
    logger.info("Zum Beenden: Strg+C")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler beendet.")


if __name__ == "__main__":
    main()
