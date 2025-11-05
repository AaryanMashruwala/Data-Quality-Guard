# Data-Quality-Guard

A tiny data pipeline that loads CSV files into a lightweight DuckDB “warehouse”, runs automatic data-quality checks, and fails CI if anything looks wrong.

It’s meant to show how you can treat **data checks like tests**: they run in `pytest`, produce XML/HTML reports, and are enforced in GitHub Actions.

---

## What the pipeline does

1. **Ingest**
   - Reads `data/raw/customers.csv` and `data/raw/orders.csv` with pandas.
   - Writes them into `data/warehouse.duckdb` as tables `customers` and `orders`.

2. **Transform**
   - Creates a joined table `orders_with_customers`:
     - order info: `order_id, customer_id, order_date, amount, status`
     - customer info: `name, email, signup_date, country`.

3. **Checks (pytest tests)**
   Current checks live in `src/checks/orders_checks.py` and `tests/test_checks.py`:
   - No negative order amounts.
   - Each `order_id` is unique.
   - Every order’s `customer_id` exists in `customers`.
   - `status` ∈ {`PAID`, `PENDING`, `CANCELLED`, `REFUNDED`}.
   - No order occurs before the customer’s `signup_date`.
   - **Drift:** average order `amount` cannot change by more than 50% vs a saved baseline.

4. **Drift baseline**
   - `src/drift_baseline.py` computes baseline stats for the orders table and saves:
     - `data/baselines/orders_amount_baseline.json`

5. **Reports**
   - `pytest` is run with:
     - `--junitxml=report.xml`
     - `--html=report.html --self-contained-html`
   - GitHub Actions uploads `report.html` as a CI artifact.

---

## Running locally

From the project root:

```bash
# 1) (Optional) Ingest + transform manually
python src/ingest.py
python src/transform.py

# 2) Run tests + generate reports
pytest --disable-warnings --junitxml=report.xml --html=report.html --self-contained-html
