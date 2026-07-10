"""
Publish-Gate: prueft data/data_contract.yaml live gegen die Impala-Datenbank.
"""
import os
import sys

import yaml

from db import get_connection

DATABASE = os.getenv("DATABASE", "gruppe3")
CONTRACT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "data_contract.yaml"
)

# Contract-Typ -> akzeptierte Impala-Typen (DESCRIBE-Schreibweise).
TYPE_ALIASES = {
    "string": {"string"},
    "text": {"string"},
    "int": {"int"},
    "integer": {"int"},
    "bigint": {"bigint"},
    "long": {"bigint"},
    "double": {"double"},
    "float": {"float", "double"},
    "boolean": {"boolean"},
    "timestamp": {"timestamp"},
    "date": {"date"},
}


def load_contract():
    """Output: geparster Data Contract (dict)."""
    with open(CONTRACT_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


class CheckRunner:
    """Sammelt Check-Ergebnisse; self.failed entscheidet den Exit-Code."""

    def __init__(self, cur):
        self.cur = cur
        self.passed = 0
        self.failed = 0

    def record(self, ok, label, detail=""):
        status = "OK  " if ok else "FEHLER"
        suffix = f" - {detail}" if detail else ""
        print(f"  [{status}] {label}{suffix}")
        if ok:
            self.passed += 1
        else:
            self.failed += 1

    def scalar(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchone()[0]


def check_schema(runner, table, fields):
    """Input: table, Contract-fields. Output: bool (Tabelle erreichbar) -
    prueft Spalten + Typen exakt wie vereinbart, keine Zusatzspalten."""
    try:
        runner.cur.execute(f"DESCRIBE {table}")
        actual = {row[0]: row[1].strip().lower() for row in runner.cur.fetchall()}
    except Exception as e:
        runner.record(False, f"{table}: Tabelle existiert", str(e).splitlines()[0])
        return False

    ok_all = True
    for col, spec in fields.items():
        expected = str(spec.get("type", "")).lower()
        allowed = TYPE_ALIASES.get(expected, {expected})
        if col not in actual:
            runner.record(False, f"{table}.{col}: Spalte vorhanden", "fehlt in der Tabelle")
            ok_all = False
        elif actual[col] not in allowed:
            runner.record(
                False,
                f"{table}.{col}: Typ = {expected}",
                f"Tabelle hat '{actual[col]}'",
            )
            ok_all = False

    extra = set(actual) - set(fields)
    if extra:
        runner.record(
            False,
            f"{table}: keine unvertraglichen Zusatzspalten",
            f"nicht im Contract: {sorted(extra)}",
        )
        ok_all = False

    if ok_all:
        runner.record(True, f"{table}: Schema entspricht dem Contract ({len(fields)} Spalten)")
    return True


def check_required(runner, table, fields):
    """Input: table, fields. Output: keins (Ergebnisse via runner) -
    prueft, dass required-Spalten kein NULL enthalten."""
    for col, spec in fields.items():
        if spec.get("required"):
            nulls = runner.scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            runner.record(nulls == 0, f"{table}.{col}: required (kein NULL)",
                          "" if nulls == 0 else f"{nulls} NULL-Zeilen")


def check_unique(runner, table, fields):
    """Input: table, fields. Output: keins (via runner) -
    prueft, dass primaryKey/unique-Spalten duplikatfrei sind."""
    for col, spec in fields.items():
        if spec.get("primaryKey") or spec.get("unique"):
            dupes = runner.scalar(
                f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {table} WHERE {col} IS NOT NULL"
            )
            runner.record(dupes == 0, f"{table}.{col}: eindeutig",
                          "" if dupes == 0 else f"{dupes} Duplikat-Zeilen")


def check_quality(runner, table, quality_entries):
    """Input: table, quality-Eintraege. Output: keins (via runner) - fuehrt
    sql-Eintraege aus, vergleicht gegen mustBe/mustBeGreaterThan/mustBeLessThan."""
    for entry in quality_entries:
        if entry.get("type") != "sql":
            continue  # "text"-Eintraege sind dokumentarisch
        description = entry.get("description", "Quality-SQL")
        value = runner.scalar(entry["query"])
        if "mustBe" in entry:
            ok = value == entry["mustBe"]
            expected = f"= {entry['mustBe']}"
        elif "mustBeGreaterThan" in entry:
            ok = value is not None and value > entry["mustBeGreaterThan"]
            expected = f"> {entry['mustBeGreaterThan']}"
        elif "mustBeLessThan" in entry:
            ok = value is not None and value < entry["mustBeLessThan"]
            expected = f"< {entry['mustBeLessThan']}"
        else:
            continue  # kein pruefbares Kriterium hinterlegt
        runner.record(ok, f"{table}: {description}",
                      "" if ok else f"Ergebnis {value}, erwartet {expected}")


def main():
    contract = load_contract()
    models = contract.get("models", {})

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"USE {DATABASE}")
    runner = CheckRunner(cur)

    print(f"Data-Contract-Check gegen Datenbank '{DATABASE}' "
          f"({len(models)} Tabellen im Contract)\n")

    for table, model in models.items():
        print(f"{table}:")
        fields = model.get("fields", {})
        table_reachable = check_schema(runner, table, fields)
        if table_reachable:
            check_required(runner, table, fields)
            check_unique(runner, table, fields)
            check_quality(runner, table, model.get("quality", []))
        print()

    cur.close()
    conn.close()

    print(f"Ergebnis: {runner.passed} Checks OK, {runner.failed} fehlgeschlagen.")
    if runner.failed:
        print("Der Datenstand VERLETZT den Data Contract - Publish-Gate schlaegt an.")
        sys.exit(1)
    print("Der Datenstand erfuellt den Data Contract.")


if __name__ == "__main__":
    main()
