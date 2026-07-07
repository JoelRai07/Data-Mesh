"""
DELIVERABLE 3b: Technische Durchsetzung des Data Contracts.

Der Prof hat klargestellt, dass der Data Contract nicht nur Dokumentation
sein soll, sondern TECHNISCH DURCHGESETZT werden muss. Genau das macht
dieses Skript: es liest docs/data_contract.yaml (die eine Quelle der
Wahrheit fuer das Datenprodukt-Interface) und prueft den Vertrag live
gegen die Impala-Datenbank:

  1. SCHEMA:        Jede im Contract beschriebene Tabelle existiert, hat
                    exakt die vereinbarten Spalten mit den vereinbarten
                    Typen - und KEINE zusaetzlichen Spalten (der Contract
                    beschreibt das Interface vollstaendig; eine still
                    hinzugekommene Spalte waere eine unangekuendigte
                    Vertragsaenderung).
  2. PFLICHTFELDER: Spalten mit "required: true" enthalten kein NULL.
  3. EINDEUTIGKEIT: Spalten mit "primaryKey: true" oder "unique: true"
                    enthalten keine Duplikate.
  4. QUALITY-SQLs:  Alle quality-Eintraege vom Typ "sql" werden ausgefuehrt
                    und gegen mustBe / mustBeGreaterThan / mustBeLessThan
                    verglichen (quality-Eintraege vom Typ "text" sind
                    dokumentarisch und werden uebersprungen).

Schlaegt EIN Check fehl, endet das Skript mit Exit-Code 1 - eingebunden als
letzter Schritt in run_pipeline.py wirkt es damit als PUBLISH-GATE des
WAP-Patterns: ein Datenstand, der den Vertrag verletzt, laesst den
Pipeline-Lauf (und damit Docker/Scheduler) sichtbar fehlschlagen, statt
still fehlerhafte Daten am Output-Port liegen zu lassen.

WARUM EIN EIGENES SKRIPT STATT DIREKT "datacontract test"?
  Die Data Contract CLI unterstuetzt Impala inzwischen grundsaetzlich.
  Fuer diese Abgabe ist ein eigenes, kleines Gate trotzdem pragmatischer:
  es nutzt exakt dieselbe .env-/impyla-Verbindung wie die Pipeline, bringt
  keine weitere CLI-Abhaengigkeit in Docker/Scheduler und prueft genau die
  Contract-Teile, die wir im YAML wirklich verwenden. Das YAML bleibt bewusst
  Data-Contract-Specification-kompatibel, sodass z.B. "datacontract lint"
  weiterhin darauf funktioniert.

Ausfuehren (auch standalone, nur Python + .env noetig, kein Spark/Java):
  .venv/Scripts/python.exe src/contract_check.py
"""
import os
import sys

import yaml

from db import get_connection

DATABASE = os.getenv("DATABASE", "gruppe3")
CONTRACT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "docs", "data_contract.yaml"
)

# Contract-Typ -> akzeptierte Impala-Typen (DESCRIBE-Schreibweise).
# Aliase wie integer/long decken die gaengigen Schreibweisen der
# Data-Contract-Specification ab, falls der Contract sie verwendet.
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
    with open(CONTRACT_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


class CheckRunner:
    """Sammelt Ergebnisse aller Checks; failures entscheidet den Exit-Code."""

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
    """Check 1: Spalten + Typen exakt wie im Contract vereinbart."""
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
    """Check 2: required-Spalten enthalten kein NULL."""
    for col, spec in fields.items():
        if spec.get("required"):
            nulls = runner.scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            runner.record(nulls == 0, f"{table}.{col}: required (kein NULL)",
                          "" if nulls == 0 else f"{nulls} NULL-Zeilen")


def check_unique(runner, table, fields):
    """Check 3: primaryKey/unique-Spalten sind duplikatfrei."""
    for col, spec in fields.items():
        if spec.get("primaryKey") or spec.get("unique"):
            dupes = runner.scalar(
                f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {table} WHERE {col} IS NOT NULL"
            )
            runner.record(dupes == 0, f"{table}.{col}: eindeutig",
                          "" if dupes == 0 else f"{dupes} Duplikat-Zeilen")


def check_quality(runner, table, quality_entries):
    """Check 4: quality-Eintraege vom Typ sql ausfuehren und vergleichen."""
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
