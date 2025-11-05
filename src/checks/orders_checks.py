"""
Checks related to the orders table.

Right now: one check that verifies there are no negative amounts.
"""

from pathlib import Path
import duckdb
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"
BASELINES_DIR = DATA_DIR / "baselines"


def count_negative_order_amounts() -> int:
    """
    Return how many rows in 'orders' have amount < 0.
    """
    with duckdb.connect(str(DB_PATH)) as con:
        (count,) = con.execute(
            "SELECT COUNT(*) FROM orders WHERE amount < 0"
        ).fetchone()
    return count


def count_duplicate_order_ids() -> int:
    """
    Return how many order_id values are duplicated in the 'orders' table.
    If everything is unique, this should return 0.
    """
    with duckdb.connect(str(DB_PATH)) as con:
        (count,) = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT order_id
                FROM orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
    return count


def count_orders_with_missing_customer() -> int:
    """
    Return how many orders point to a non-existing customer.
    In other words: orders where customer_id is not found in customers.
    """
    with duckdb.connect(str(DB_PATH)) as con:
        (count,) = con.execute(
            """
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN customers c
              ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        ).fetchone()
    return count


def count_orders_with_invalid_status() -> int:
    """
    Return how many orders have a status value outside the allowed set.
    Allowed values: PAID, PENDING, CANCELLED, REFUNDED.
    """
    with duckdb.connect(str(DB_PATH)) as con:
        (count,) = con.execute(
            """
            SELECT COUNT(*)
            FROM orders
            WHERE status NOT IN ('PAID', 'PENDING', 'CANCELLED', 'REFUNDED')
            """
        ).fetchone()
    return count


def count_orders_before_customer_signup() -> int:
    """
    Return how many orders have an order_date earlier than the customer's signup_date.
    These are logically impossible and should be zero.
    """
    with duckdb.connect(str(DB_PATH)) as con:
        (count,) = con.execute(
            """
            SELECT COUNT(*)
            FROM orders_with_customers
            WHERE CAST(order_date AS DATE) < CAST(customer_signup_date AS DATE)
            """
        ).fetchone()
    return count


def get_order_amount_mean_drift_ratio() -> float:
    """
    Compare the current mean(order.amount) to the saved baseline.
    Return the relative change as a ratio (0.0 = no change, 0.5 = 50% change).
    """

    baseline_path = BASELINES_DIR / "orders_amount_baseline.json"
    with baseline_path.open("r", encoding="utf-8") as f:
        baseline = json.load(f)

    baseline_mean = baseline["mean_amount"]

    with duckdb.connect(str(DB_PATH)) as con:
        (_, current_mean) = con.execute(
            "SELECT COUNT(*), AVG(amount) FROM orders"
        ).fetchone()

    # Avoid division by zero if the baseline mean is 0
    if baseline_mean == 0:
        if current_mean == 0:
            return 0.0
        else:
            return 1.0  # treat as 100%+ change

    drift_ratio = abs(current_mean - baseline_mean) / abs(baseline_mean)
    return float(drift_ratio)
