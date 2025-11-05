"""
Transform step: create a joined table that combines orders and customers.

Resulting table: orders_with_customers
Each row contains:
- order info (order_id, customer_id, order_date, amount, status)
- customer info (name, email, signup_date, country)
"""

from pathlib import Path
import duckdb

# Figure out where the DuckDB file lives (same pattern as ingest.py)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"


def main() -> None:
    print("Using DuckDB at:", DB_PATH)

    with duckdb.connect(str(DB_PATH)) as con:
        # Create or replace a joined table combining orders and customers
        con.execute(
            """
            CREATE OR REPLACE TABLE orders_with_customers AS
            SELECT
                o.order_id,
                o.customer_id,
                o.order_date,
                o.amount,
                o.status,
                c.name AS customer_name,
                c.email AS customer_email,
                c.signup_date AS customer_signup_date,
                c.country AS customer_country
            FROM orders o
            JOIN customers c
              ON o.customer_id = c.customer_id
            """
        )

        # Sanity check: how many rows did we create?
        (count,) = con.execute(
            "SELECT COUNT(*) FROM orders_with_customers"
        ).fetchone()

    print(f"Created 'orders_with_customers' with {count} rows.")


if __name__ == "__main__":
    main()
