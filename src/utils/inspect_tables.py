"""
Schaut sich die vorhandenen project_*-Tabellen an:
Spalten (Schema) + Zeilenanzahl. Nur Lesen, aendert nichts.
"""
from ../db import get_connection

TABLES = [
    "project_gemeinden",
    "project_bauland",
    "project_klimadaten",
    "project_bevoelkerungzahlen",
]


def main():
    conn = get_connection()
    cur = conn.cursor()

    for t in TABLES:
        print("=" * 60)
        print(f"TABELLE: {t}")
        print("=" * 60)
        try:
            cur.execute(f"DESCRIBE {t}")
            cols = cur.fetchall()
            print("Spalten:")
            for row in cols:
                # row = (name, type, comment)
                print(f"  - {row[0]:<30} {row[1]}")

            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"\nZeilen: {count}")

            if count > 0:
                cur.execute(f"SELECT * FROM {t} LIMIT 3")
                print("Beispielzeilen:")
                for r in cur.fetchall():
                    print(f"  {r}")
        except Exception as e:
            print(f"  FEHLER: {e}")
        print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
