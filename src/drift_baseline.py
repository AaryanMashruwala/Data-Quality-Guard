"""
Compute a simple baseline for the orders table and save it to JSON.

Right now we track:
- row_count: number of orders
- mean_amount: average order amount

We run this once on known-good data and commit the JSON file.
Later checks compare current stats to this baseline to detect drift.
"""

from pathlib import Path
import json
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"
BASELINES_DIR = DATA_DIR / "baselines"


def main() -> None:
    print("Using DuckDB at:", DB_PATH)

    with duckdb.connect(str(DB_PATH)) as con:
        row_count, mean_amount = con.execute(
            "SELECT COUNT(*), AVG(amount) FROM orders"
        ).fetchone()

    baseline = {
        "row_count": row_count,
        "mean_amount": float(mean_amount),
    }

    BASELINES_DIR.mkdir(parents=True, exist_ok=True)
    baseline_path = BASELINES_DIR / "orders_amount_baseline.json"

    with baseline_path.open("w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2)

    print("Saved baseline to:", baseline_path)
    print("Baseline contents:", baseline)


if __name__ == "__main__":
    main()
