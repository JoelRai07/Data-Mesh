"""
Loescht ALLE Tabellen in "gruppe3" nach expliziter Bestaetigung (fuer einen
sauberen Reset vor End-to-End-Tests). DATABASE ist hartkodiert "gruppe3"
(kein Parameter), um ein versehentliches Loeschen der Quelldatenbank "default"
auszuschliessen. DROP TABLE ist nicht rueckgaengig zu machen.

Ausfuehren:  .venv/Scripts/python.exe src/utils/reset_database.py
"""
import os
import sys

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
