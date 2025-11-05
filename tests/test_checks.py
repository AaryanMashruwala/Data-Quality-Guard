import sys
from pathlib import Path

# Make sure the src/ folder is on sys.path so we can import modules from there
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pytest
from ingest import main as ingest_main
from transform import main as transform_main
from checks.orders_checks import (
    count_negative_order_amounts,
    count_duplicate_order_ids,
    count_orders_with_missing_customer,
    count_orders_with_invalid_status,
    count_orders_before_customer_signup,
)




@pytest.fixture(scope="module", autouse=True)
def load_data():
    """
    This runs once before the tests in this file.
    It calls ingest_main() to load the CSVs into DuckDB.
    """
    ingest_main()
    transform_main()


def test_no_negative_order_amounts():
    """
    Our data-quality rule: no orders should have a negative amount.
    This test will fail if any such rows exist in the DuckDB 'orders' table.
    """
    bad_count = count_negative_order_amounts()
    assert bad_count == 0


def test_order_id_unique():
    """
    Data-quality rule: each order_id should appear at most once.
    This test fails if there are any duplicate order_id values.
    """
    dup_count = count_duplicate_order_ids()
    assert dup_count == 0


def test_orders_have_valid_customers():
    """
    Data-quality rule: every order must reference an existing customer.
    This test fails if any order.customer_id is missing in customers.
    """
    bad_fk_count = count_orders_with_missing_customer()
    assert bad_fk_count == 0


def test_order_status_values_are_valid():
    """
    Data-quality rule: order status must be in a small allowed set.
    This test fails if any order has an unexpected status value.
    """
    bad_status_count = count_orders_with_invalid_status()
    assert bad_status_count == 0


def test_no_orders_before_customer_signup():
    """
    Data-quality rule: customers cannot have orders before their signup date.
    This test fails if any such orders exist.
    """
    bad_time_count = count_orders_before_customer_signup()
    assert bad_time_count == 0

