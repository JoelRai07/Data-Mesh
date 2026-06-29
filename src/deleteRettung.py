from db import get_connection

# Praefix = WI-User (wie in Uebung 5 gefordert). So gehoeren die Tabellen
# eindeutig zu uns und kollidieren nicht mit denen anderer Gruppen.
PREFIX = "gruppe3"
DATABASE = "gruppe3"
  
DIM_JAHR = f"{PREFIX}_dim_jahr"
DIM_KREIS = f"{PREFIX}_dim_kreis"
DIM_GEMEINDE = f"{PREFIX}_dim_gemeinde"
DIM_KLIMASTADT = f"{PREFIX}_dim_klimastadt"
FACT_KLIMA = f"{PREFIX}_fact_klima"
FACT_BAULAND = f"{PREFIX}_fact_bauland"
FACT_BEVOELKERUNG = f"{PREFIX}_fact_bevoelkerung"
FACT_GEMEINDE_STAMM = f"{PREFIX}_fact_gemeinde_stamm"
FACT_STANDORTPROFIL_KPI = f"{PREFIX}_fact_standortprofil_kpi"


STATEMENTS = {
    FACT_KLIMA: f"DROP TABLE IF EXISTS {DATABASE}.{FACT_KLIMA}",
    FACT_BAULAND: f"DROP TABLE IF EXISTS {DATABASE}.{FACT_BAULAND}",
    FACT_BEVOELKERUNG: f"DROP TABLE IF EXISTS {DATABASE}.{FACT_BEVOELKERUNG}",
    FACT_GEMEINDE_STAMM: f"DROP TABLE IF EXISTS {DATABASE}.{FACT_GEMEINDE_STAMM}",
    FACT_STANDORTPROFIL_KPI: f"DROP TABLE IF EXISTS {DATABASE}.{FACT_STANDORTPROFIL_KPI}",
    DIM_JAHR: f"DROP TABLE IF EXISTS {DATABASE}.{DIM_JAHR}",
    DIM_KREIS: f"DROP TABLE IF EXISTS {DATABASE}.{DIM_KREIS}",
    DIM_GEMEINDE: f"DROP TABLE IF EXISTS {DATABASE}.{DIM_GEMEINDE}",
    DIM_KLIMASTADT: f"DROP TABLE IF EXISTS {DATABASE}.{DIM_KLIMASTADT}",
}


def main():
    conn = get_connection()
    cur = conn.cursor()

    for table_name, statement in STATEMENTS.items():
        print(f"Loesche Tabelle: {table_name} ...")
        cur.execute(statement)
        print("  -> OK")
    print("\nTabellen wurden entfernt.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
