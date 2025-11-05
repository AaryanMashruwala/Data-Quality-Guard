"""
Ingest step: read CSV files from data/raw and load them into DuckDB.
"""

from pathlib import Path
import pandas as pd
import duckdb
from checks.orders_checks import count_negative_order_amounts

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "warehouse.duckdb"  

def main() -> None:
    print("Project root:", PROJECT_ROOT)
    print("Raw data dir:", RAW_DIR)
    print("DuckDB file will be:", DB_PATH)

    # Build the full path to customers.csv using RAW_DIR
    customers_path = RAW_DIR / "customers.csv"
    print("Loading customers from:", customers_path)

    # Read the CSV into a pandas DataFrame (table)
    customers_df = pd.read_csv(customers_path)

    # Show the whole small table for now
    print("\nCustomers table:")
    print(customers_df)

    # --- Write customers into DuckDB ---

    # Connect to a DuckDB database at DB_PATH (file is created if it doesn't exist)
    with duckdb.connect(str(DB_PATH)) as con:
        # Expose the pandas DataFrame to DuckDB as a temporary SQL table
        con.register("customers_tmp", customers_df)

        # Create or replace a persistent table from that DataFrame
        con.execute("""
            CREATE OR REPLACE TABLE customers AS
            SELECT * FROM customers_tmp
        """)

        # Sanity check: count rows we just wrote
        count = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]

    print(f"\nWrote {count} rows to DuckDB table 'customers' at: {DB_PATH}")


    # Build the full path to orders.csv using RAW_DIR
    orders_path = RAW_DIR / "orders.csv"
    print("\nLoading orders from:", orders_path)

    # Read the CSV into a pandas DataFrame
    orders_df = pd.read_csv(orders_path)

    # Show the whole small table for now
    print("\nOrders table:")
    print(orders_df)

    # --- Write orders into DuckDB ---
    with duckdb.connect(str(DB_PATH)) as con:
        # Expose the pandas DataFrame to DuckDB as a temporary table
        con.register("orders_tmp", orders_df)

        # Create or replace a persistent table from that DataFrame
        con.execute("""
            CREATE OR REPLACE TABLE orders AS
            SELECT * FROM orders_tmp
        """)

        # Sanity check: count rows we just wrote
        orders_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

        print(f"\nWrote {orders_count} rows to DuckDB table 'orders' at: {DB_PATH}")

    # --- Run the negative-amounts check from checks.orders_checks ---
        bad_count = count_negative_order_amounts()

        if bad_count == 0:
            print("\nCheck passed: no negative order amounts.")
        else:
            print(f"\nCheck FAILED: {bad_count} orders have negative amounts.")






if __name__ == "__main__":
    main()
