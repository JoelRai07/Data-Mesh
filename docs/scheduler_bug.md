# Bug: APScheduler `next_run_time` vor Scheduler-Start

## Symptom

Beim ersten Start von `src/scheduler.py` (vor dem eigentlichen
`scheduler.start()`):

```
Traceback (most recent call last):
  File "scheduler.py", line 94, in <module>
    main()
  File "scheduler.py", line 84, in main
    logger.info("Scheduler gestartet. Naechster Lauf: %s", scheduler.get_jobs()[0].next_run_time)
                                                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'Job' object has no attribute 'next_run_time'. Did you mean: '_get_run_times'?
```

## Ursache

`apscheduler.job.Job` definiert `next_run_time` als `__slots__`-Attribut
(s. `apscheduler/job.py`):

```python
__slots__ = (..., 'next_run_time', '__weakref__')
```

Bei Klassen mit `__slots__` existiert ein Attribut erst, **nachdem** ihm
einmal ein Wert zugewiesen wurde - es gibt keinen automatischen `None`-
Default wie bei normalen Objekt-Attributen. `next_run_time` wird von
APScheduler aber erst beim **Start des Schedulers** berechnet (der Scheduler
fragt dafuer den Trigger nach der naechsten Fälligkeit und schreibt das
Ergebnis in den Job). Vor `scheduler.start()` wurde dieser Wert fuer den
gerade per `add_job()` hinzugefuegten Job also schlicht noch nie gesetzt -
der Zugriff darauf wirft deshalb `AttributeError` statt z.B. `None`
zurueckzugeben.

Der urspruengliche Code hat versucht, genau diesen Wert **vor** dem Start
auszulesen, um in der Startmeldung den naechsten Lauf zu loggen:

```python
logger.info("Scheduler gestartet. Naechster Lauf: %s", scheduler.get_jobs()[0].next_run_time)
...
scheduler.start()
```

## Loesung

Die naechste Ausfuehrungszeit nicht vom (noch nicht initialisierten) `Job`
erfragen, sondern direkt vom `Trigger` - der kennt seine eigene Logik
unabhaengig vom Scheduler-Status:

```python
trigger = CronTrigger(hour=0, minute=0)
scheduler.add_job(run_pipeline_job, trigger=trigger, ...)

next_run = trigger.get_next_fire_time(None, datetime.now(scheduler.timezone))
logger.info("Scheduler gestartet. Naechster Lauf: %s", next_run)
```

`BaseTrigger.get_next_fire_time(previous_fire_time, now)` ist die Methode,
die jeder APScheduler-Trigger implementiert, um seine naechste Fälligkeit zu
berechnen - unabhaengig davon, ob er schon einem laufenden Scheduler
zugeordnet ist.

## Wie verifiziert

Scheduler kurz gestartet und Log-Ausgabe geprueft:

```
2026-06-29 15:49:53,790 [INFO] Scheduler gestartet. Naechster Lauf: 2026-06-30 00:00:00+02:00
2026-06-29 15:49:53,792 [INFO] Scheduler started
```

Korrektes Datum (naechster Tag, 00:00 Uhr, Zeitzone Europe/Berlin), kein
Absturz mehr.

## Hinweis zum aktuellen Test-Zustand

`src/scheduler.py` steht aktuell testweise auf `CronTrigger(minute="*")`
(laeuft jede Minute), um den Job-Ablauf ohne langes Warten beobachten zu
koennen. **Vor der Abgabe zurueck auf `CronTrigger(hour=0, minute=0)`
stellen** (taeglich um Mitternacht, wie in der Aufgabenstellung gefordert).
