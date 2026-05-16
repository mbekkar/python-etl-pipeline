"""
ETL CLI — Command-Line Interface
==================================
Run any ETL pipeline from the terminal.

Usage:
    python run_pipeline.py --source data/consultations.csv \\
                           --table  consultations_clean    \\
                           --db     sqlite:///output.db

Author : Mounir Bekkar
"""

import argparse
import sys
import json

from etl_pipeline import ETLPipeline


def parse_json_arg(value: str, name: str) -> dict | list | None:
    """Parse a JSON string argument."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON for --{name}: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline — Extract · Transform · Validate · Load",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Basic CSV → SQLite
  python run_pipeline.py \\
    --source data/sales.csv \\
    --table  sales_clean

  # With type casting and null column enforcement
  python run_pipeline.py \\
    --source data/consultations.csv \\
    --table  consultations_clean \\
    --not-null '["id","date","patient_id"]' \\
    --types '{"date":"datetime","cout":"float","duree":"int"}'

  # With validation ranges
  python run_pipeline.py \\
    --source data/consultations.csv \\
    --table  consultations_clean \\
    --not-null '["id","cout"]' \\
    --ranges '{"cout":[0,1000],"duree":[5,300]}'

  # PostgreSQL target
  python run_pipeline.py \\
    --source data/sales.csv \\
    --table  sales_clean \\
    --db     "postgresql://user:pass@localhost:5432/mydb"

  # Replace existing table
  python run_pipeline.py \\
    --source data/sales.csv \\
    --table  sales_clean \\
    --if-exists replace
        """
    )

    # ── Source / Target ──────────────────────────────────────────────────────
    parser.add_argument(
        "--source", required=True,
        help="Path to source file (CSV, JSON, Excel, Parquet) or SQL query"
    )
    parser.add_argument(
        "--table", required=True,
        help="Target table name in the database"
    )
    parser.add_argument(
        "--db", default="sqlite:///etl_output.db",
        help="SQLAlchemy connection URL (default: sqlite:///etl_output.db)"
    )
    parser.add_argument(
        "--if-exists", choices=["append","replace","fail"], default="append",
        help="What to do if the target table already exists (default: append)"
    )

    # ── Transform options ─────────────────────────────────────────────────────
    parser.add_argument(
        "--not-null",
        help='JSON list of columns that must not be null. E.g.: \'["id","date"]\''
    )
    parser.add_argument(
        "--required",
        help='JSON list of columns that must exist in the source. E.g.: \'["id","name"]\''
    )
    parser.add_argument(
        "--types",
        help='JSON dict of column → type. E.g.: \'{"date":"datetime","price":"float"}\''
    )
    parser.add_argument(
        "--rename",
        help='JSON dict of old_name → new_name. E.g.: \'{"old_col":"new_col"}\''
    )
    parser.add_argument(
        "--drop",
        help='JSON list of columns to drop. E.g.: \'["col1","col2"]\''
    )
    parser.add_argument(
        "--normalize",
        help='JSON list of string columns to strip+lowercase. E.g.: \'["name","city"]\''
    )
    parser.add_argument(
        "--filter",
        help='Pandas query string to filter rows. E.g.: "price > 0 and status == \'active\'"'
    )

    # ── Validation options ────────────────────────────────────────────────────
    parser.add_argument(
        "--ranges",
        help='JSON dict of column → [min, max]. E.g.: \'{"price":[0,9999],"age":[0,120]}\''
    )
    parser.add_argument(
        "--allowed",
        help='JSON dict of column → [val1, val2]. E.g.: \'{"status":["active","inactive"]}\''
    )

    # ── Misc ──────────────────────────────────────────────────────────────────
    parser.add_argument(
        "--log-dir", default="logs",
        help="Directory for log files (default: logs/)"
    )

    args = parser.parse_args()

    # Parse JSON arguments
    not_null_cols  = parse_json_arg(args.not_null,  "not-null")
    required_cols  = parse_json_arg(args.required,  "required")
    type_schema    = parse_json_arg(args.types,     "types")
    rename_map     = parse_json_arg(args.rename,    "rename")
    drop_cols      = parse_json_arg(args.drop,      "drop")
    normalize_cols = parse_json_arg(args.normalize, "normalize")
    filter_query   = args.filter

    numeric_ranges_raw = parse_json_arg(args.ranges,  "ranges")
    allowed_values     = parse_json_arg(args.allowed, "allowed")

    # Convert ranges [min, max] → (min, max)
    numeric_ranges = None
    if numeric_ranges_raw:
        numeric_ranges = {k: tuple(v) for k, v in numeric_ranges_raw.items()}

    # ── Run pipeline ──────────────────────────────────────────────────────────
    try:
        pipeline = ETLPipeline(
            source=args.source,
            target_table=args.table,
            db_url=args.db,
            log_dir=args.log_dir,
            required_cols=required_cols,
            not_null_cols=not_null_cols,
            type_schema=type_schema,
            numeric_ranges=numeric_ranges,
            allowed_values=allowed_values,
            rename_map=rename_map,
            drop_cols=drop_cols,
            normalize_cols=normalize_cols,
            filter_query=filter_query,
            if_exists=args.if_exists,
        )

        result = pipeline.run()
        sys.exit(0 if result.success else 1)

    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
