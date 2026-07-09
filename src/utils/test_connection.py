"""
Diagnose: prueft die Impala-Verbindung (SELECT 1) und listet vorhandene Tabellen.
Ausfuehren:  .venv/Scripts/python.exe src/utils/test_connection.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

def main():
    print("Verbinde mit Impala ...")
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"  -> Verbindung OK. Testergebnis: {result[0]}")

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
