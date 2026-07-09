import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Muss vor dem run_pipeline-Import gesetzt sein (JDK 17 fuer PySpark, s.
# pipeline_audit_to_target.py).
JAVA_HOME = os.getenv("JAVA_HOME_JDK17")
if JAVA_HOME and os.path.isdir(JAVA_HOME):
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import run_pipeline

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
        run_pipeline.main()
        logger.info("Pipeline-Lauf erfolgreich abgeschlossen.")
    except Exception:
        logger.exception("Pipeline-Lauf fehlgeschlagen.")


def main():
    scheduler = BlockingScheduler(timezone="Europe/Berlin")
    trigger = CronTrigger(hour=0, minute=0)
    scheduler.add_job(
        run_pipeline_job,
        trigger=trigger,
        id="daily_pipeline_run",
        name="Taeglicher Pipeline-Lauf (00:00 Uhr)",
        misfire_grace_time=3600,  # bis zu 1h Verspaetung (z.B. nach Rechner-Standby) noch nachholen
    )

    next_run = trigger.get_next_fire_time(None, datetime.now(scheduler.timezone))
    logger.info("Scheduler gestartet. Naechster Lauf: %s", next_run)
    logger.info("Zum Beenden: Strg+C")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler beendet.")


if __name__ == "__main__":
    main()
