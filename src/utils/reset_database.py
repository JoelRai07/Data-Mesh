"""
Loescht ALLE Tabellen in der Datenbank "gruppe3" - fuer einen sauberen
Reset vor einem End-to-End-Test der Incremental-Loader-Pipeline (s.
run_pipeline.py), z.B. um Full-Load- von Skip-Verhalten klar zu trennen
(erster Lauf nach dem Reset = alles neu, jeder weitere Lauf ohne Reset =
Incremental/Skip, s. Diskussion zum Testen der Pipeline).

RUEHRT NUR AN DATABASE = "gruppe3" (die eigene Gruppendatenbank), NIEMALS an
"default" (die geteilte Quelldatenbank mit project_bauland/project_bevoelkerungzahlen/
project_gemeinden/project_klimadaten) - DATABASE ist hartkodiert, kein Parameter,
genau um ein versehentliches Loeschen der Quelle auszuschliessen.

Fragt vor dem eigentlichen Loeschen eine explizite Bestaetigung ab (Tabellen-
liste wird vorher angezeigt) - DROP TABLE ist nicht rueckgaengig zu machen.

Ausfuehren:  .venv/Scripts/python.exe src/utils/reset_database.py
"""
import os
import sys

# db.py liegt eine Ebene hoeher (in src/), s. gleiches Muster in inspect_tables.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

DATABASE = "gruppe3"


def list_tables(cur):
    cur.execute(f"SHOW TABLES IN {DATABASE}")
    return [row[0] for row in cur.fetchall()]


def main():
    conn = get_connection()
    cur = conn.cursor()

    tables = list_tables(cur)
    if not tables:
        print(f"Datenbank '{DATABASE}' enthaelt keine Tabellen - nichts zu tun.")
        cur.close()
        conn.close()
        return

    print(f"Folgende {len(tables)} Tabelle(n) in Datenbank '{DATABASE}' werden GELOESCHT:")
    for t in tables:
        print(f"  - {t}")

    confirmation = input(
        f"\nZum Bestaetigen exakt '{DATABASE}' eintippen (alles andere bricht ab): "
    )
    if confirmation != DATABASE:
        print("Abgebrochen - keine Tabelle geloescht.")
        cur.close()
        conn.close()
        return

    for t in tables:
        print(f"Loesche {t} ...")
        cur.execute(f"DROP TABLE IF EXISTS {DATABASE}.{t}")

    cur.close()
    conn.close()
    print(f"\nFertig - {len(tables)} Tabelle(n) geloescht.")


if __name__ == "__main__":
    main()
