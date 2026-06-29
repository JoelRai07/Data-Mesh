import os
import sys

# db.py liegt eine Ebene hoeher (in src/). Diesen Ordner zum Suchpfad
# hinzufuegen, damit "from db import ..." auch aus src/utils/ funktioniert.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

def main():
    print("Verbinde mit Impala ...")
    connection = get_connection()
    cursor = connection.cursor()

    # Ein einfacher Test-Befehl, der auf jedem Impala laeuft.
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"  -> Verbindung OK. Testergebnis: {result[0]}")

    # Zeigt, welche Tabellen es schon gibt (hilfreich fuers Projekt).
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"\nVorhandene Tabellen ({len(tables)}):")
    for (name,) in tables:
        print(f"  - {name}")

    cursor.close()
    connection.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
