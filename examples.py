"""
Pipeline Examples
==================
Ready-to-run pipeline configurations for the sample datasets.
Each example shows a realistic ETL use case.

Run with:
    python examples.py consultations
    python examples.py sales
    python examples.py employees
    python examples.py all

Author : Mounir Bekkar
"""

import sys
import json
import pandas as pd
from pathlib import Path

from etl_pipeline import ETLPipeline, Extractor, Transformer, Validator, Loader, ETLResult, setup_logger
import sqlalchemy as sa


# ── EXAMPLE 1: Hospital Consultations ─────────────────────────────────────────

def run_consultations():
    """
    ETL for the hospital consultation dataset.
    Matches the OLAP/BI portfolio project schema.

    Steps:
    - Extract from CSV
    - Remove duplicates and nulls
    - Cast date and numeric types
    - Normalize service and diagnostic strings
    - Validate cost range [0, 1000] and duration [5, 300]
    - Load to SQLite
    """
    print("\n" + "═"*55)
    print("  EXAMPLE 1 — Hospital Consultations ETL")
    print("═"*55)

    pipeline = ETLPipeline(
        source       = "data/consultations.csv",
        target_table = "consultations_clean",
        db_url       = "sqlite:///etl_output.db",

        # Columns that must not be null → rows dropped if null
        not_null_cols = ["id", "patient_id", "date", "service"],

        # Type casting
        type_schema = {
            "id":         "int",
            "patient_id": "int",
            "date":       "datetime",
            "cout":       "float",
            "duree_min":  "int",
            "service":    "str",
            "docteur":    "str",
            "diagnostic": "str",
        },

        # Normalize strings (strip whitespace, lowercase)
        normalize_cols = ["service", "diagnostic", "docteur"],

        # Validation: reject rows outside these ranges
        numeric_ranges = {
            "cout":      (0, 1000),
            "duree_min": (5, 300),
        },

        # Replace existing table on each run
        if_exists = "replace",
    )

    return pipeline.run()


# ── EXAMPLE 2: E-commerce Sales ───────────────────────────────────────────────

def run_sales():
    """
    ETL for the sales dataset.
    Adds a computed 'total' column = quantity × unit_price.
    """
    print("\n" + "═"*55)
    print("  EXAMPLE 2 — E-commerce Sales ETL")
    print("═"*55)

    # We use the lower-level API here to add a custom computation
    logger = setup_logger("etl_sales", log_file="logs/run_sales.log")
    result = ETLResult(run_id="sales_example", source="data/sales.csv", target_table="sales_clean")

    extractor   = Extractor(logger)
    transformer = Transformer(logger, result)
    validator   = Validator(logger, result)
    loader      = Loader(logger, result)

    # Extract
    df = extractor.from_file("data/sales.csv")
    result.rows_extracted = len(df)

    # Transform
    df = transformer.drop_duplicates(df)
    df = transformer.drop_nulls(df, subset=["order_id", "customer_id", "date"])
    df = transformer.cast_types(df, {
        "date":       "datetime",
        "quantity":   "int",
        "unit_price": "float",
    })
    df = transformer.normalize_strings(df, ["category", "status", "city"])

    # Filter: only valid prices
    df = df[df["unit_price"] > 0]

    # Compute total (custom transformation)
    df["total"] = df["quantity"] * df["unit_price"]
    logger.info("[TRANSFORM] Computed column: total = quantity × unit_price")

    df = transformer.add_metadata(df)
    result.rows_after_clean = len(df)

    # Validate
    valid_df, rejected_df = validator.validate(
        df,
        not_null_columns=["order_id", "unit_price", "total"],
        numeric_ranges={"unit_price": (0, 10000), "quantity": (1, 100)},
        allowed_values={"status": ["completed", "pending", "cancelled", "refunded"]},
    )

    # Save rejected
    if not rejected_df.empty:
        loader.save_rejected(rejected_df, "logs/rejected_sales.csv")

    # Load
    loader.to_sql(valid_df, "sales_clean", "sqlite:///etl_output.db", if_exists="replace")

    result.success = True
    logger.info(result.summary())
    return result


# ── EXAMPLE 3: Employees JSON ─────────────────────────────────────────────────

def run_employees():
    """
    ETL for the employees JSON dataset.
    Validates salary > 0 and active status.
    """
    print("\n" + "═"*55)
    print("  EXAMPLE 3 — Employees JSON ETL")
    print("═"*55)

    pipeline = ETLPipeline(
        source       = "data/employees.json",
        target_table = "employees_clean",
        db_url       = "sqlite:///etl_output.db",

        not_null_cols  = ["id", "name", "email", "department"],

        type_schema = {
            "id":        "int",
            "salary":    "float",
            "hire_date": "datetime",
            "active":    "bool",
        },

        normalize_cols = ["department", "role"],

        numeric_ranges = {
            "salary": (15000, 200000),
        },

        if_exists = "replace",
    )

    return pipeline.run()


# ── QUERY OUTPUT ──────────────────────────────────────────────────────────────

def query_results():
    """Display a summary of the loaded data using SQL queries."""
    engine = sa.create_engine("sqlite:///etl_output.db")

    print("\n" + "═"*55)
    print("  LOADED DATA SUMMARY")
    print("═"*55)

    queries = {
        "consultations_clean": """
            SELECT
                service,
                COUNT(*)              AS nb_consultations,
                ROUND(AVG(cout), 2)   AS cout_moyen,
                ROUND(AVG(duree_min)) AS duree_moy_min
            FROM consultations_clean
            GROUP BY service
            ORDER BY nb_consultations DESC
        """,
        "sales_clean": """
            SELECT
                status,
                COUNT(*)              AS nb_orders,
                ROUND(SUM(total), 2)  AS total_revenue,
                ROUND(AVG(total), 2)  AS avg_order_value
            FROM sales_clean
            GROUP BY status
            ORDER BY nb_orders DESC
        """,
        "employees_clean": """
            SELECT
                department,
                COUNT(*)               AS headcount,
                ROUND(AVG(salary), 0)  AS avg_salary,
                SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS active_count
            FROM employees_clean
            GROUP BY department
            ORDER BY headcount DESC
        """,
    }

    with engine.connect() as conn:
        for table, query in queries.items():
            try:
                df = pd.read_sql(sa.text(query), conn)
                print(f"\n📊 {table}")
                print(df.to_string(index=False))
            except Exception as e:
                print(f"  [SKIP] {table} not found: {e}")

    engine.dispose()


# ── MAIN ──────────────────────────────────────────────────────────────────────

EXAMPLES = {
    "consultations": run_consultations,
    "sales":         run_sales,
    "employees":     run_employees,
}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run ETL pipeline examples")
    parser.add_argument(
        "example",
        choices=["consultations", "sales", "employees", "all", "query"],
        help="Which example to run"
    )
    args = parser.parse_args()

    if args.example == "query":
        query_results()
    elif args.example == "all":
        for name, fn in EXAMPLES.items():
            try:
                fn()
            except Exception as e:
                print(f"[ERROR] {name}: {e}")
        query_results()
    else:
        try:
            EXAMPLES[args.example]()
        except FileNotFoundError:
            print(f"\n[ERROR] Data file not found.")
            print("Generate the datasets first:")
            print("  python generate_data.py\n")
            sys.exit(1)
