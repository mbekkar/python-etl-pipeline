# ⚙️ ETL Data Pipeline

> A modular Python ETL pipeline — Extract · Transform · Validate · Load.  
> Supports CSV, JSON, Excel, Parquet sources and SQLite/PostgreSQL/MySQL targets.

**Author:** Mounir Bekkar · Master Informatique · Université Lumière Lyon 2  
**GitHub:** [github.com/mbekkar](https://github.com/mbekkar)

---

## ✨ Features

| Step | What it does |
|------|-------------|
| **Extract** | Reads CSV, JSON, Excel (.xlsx), Parquet, or SQL query |
| **Transform** | Deduplication, null handling, type casting, string normalization, outlier removal, filtering |
| **Validate** | Required columns, not-null checks, numeric range bounds, allowed values — rejects bad rows |
| **Load** | Writes to SQLite / PostgreSQL / MySQL with chunked inserts |
| **Logging** | Structured logs with timestamps, saved to `logs/run_YYYYMMDD_HHMMSS.log` |
| **CLI** | Full command-line interface with JSON-encoded options |
| **Tests** | 30+ unit & integration tests with pytest |

---

## 🗂️ Project Structure

```
etl-pipeline/
├── etl_pipeline.py      # Core engine (Extractor, Transformer, Validator, Loader, ETLPipeline)
├── run_pipeline.py      # CLI interface
├── examples.py          # Ready-to-run examples for all 3 datasets
├── generate_data.py     # Generate realistic messy test data
├── tests.py             # Unit & integration tests
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate sample datasets

```bash
python generate_data.py
# Creates: data/consultations.csv (500 rows)
#          data/sales.csv         (400 rows)
#          data/employees.json    (100 records)
# Data includes: duplicates, nulls, wrong types, out-of-range values
```

### 3. Run an example pipeline

```bash
python examples.py consultations
```

Output:
```
═══════════════════════════════════════════════════════
  ETL Pipeline  —  Run ID: 20240115_090001
═══════════════════════════════════════════════════════
[2024-01-15 09:00:01] INFO   [EXTRACT] Reading .CSV file: consultations.csv
[2024-01-15 09:00:01] INFO   [EXTRACT] Loaded 515 rows × 8 columns
[2024-01-15 09:00:01] WARNING [TRANSFORM] Removed 15 duplicate rows
[2024-01-15 09:00:01] WARNING [TRANSFORM] Dropped 18 rows with nulls in ['id', 'patient_id', 'date', 'service']
[2024-01-15 09:00:01] INFO   [TRANSFORM] Cast 'date' → datetime
[2024-01-15 09:00:01] INFO   [TRANSFORM] Normalized strings: ['service', 'diagnostic', 'docteur']
[2024-01-15 09:00:02] INFO   [VALIDATE] ✓ Valid: 470  |  ✗ Rejected: 12
[2024-01-15 09:00:02] INFO   [LOAD] ✅ 470 rows successfully loaded into 'consultations_clean'

───────────────────────────────────────────────────────
  ✅ SUCCESS  —  Run ID: 20240115_090001
───────────────────────────────────────────────────────
  Source          : data/consultations.csv
  Target table    : consultations_clean
  Rows extracted  : 515
  Rows dropped    : 33
  Rows loaded     : 470
  Rows rejected   : 12
  Duration        : 1.24s
  Warnings        : 3
───────────────────────────────────────────────────────
```

### 4. Query the results

```bash
python examples.py query
```

---

## 📟 CLI Reference

```bash
python run_pipeline.py \
  --source  <file_path>    \   # CSV, JSON, Excel, Parquet
  --table   <table_name>   \   # target SQL table
  --db      <db_url>       \   # SQLAlchemy URL (default: sqlite:///etl_output.db)
  --if-exists append|replace|fail \

  # Transform options
  --not-null  '["col1","col2"]'              \   # drop rows where these are null
  --required  '["col1","col2"]'              \   # these columns must exist
  --types     '{"date":"datetime","val":"float"}' \  # cast types
  --rename    '{"old":"new"}'                \   # rename columns
  --drop      '["col1","col2"]'              \   # drop columns
  --normalize '["name","city"]'              \   # strip + lowercase
  --filter    "price > 0 and status=='active'" \   # pandas query

  # Validation options
  --ranges  '{"price":[0,9999],"age":[0,120]}'    \
  --allowed '{"status":["active","inactive"]}'
```

### Examples

```bash
# Hospital consultations with full validation
python run_pipeline.py \
  --source    data/consultations.csv \
  --table     consultations_clean \
  --not-null  '["id","patient_id","date","service"]' \
  --types     '{"date":"datetime","cout":"float","duree_min":"int"}' \
  --ranges    '{"cout":[0,1000],"duree_min":[5,300]}' \
  --normalize '["service","diagnostic"]' \
  --if-exists replace

# Sales data → PostgreSQL
python run_pipeline.py \
  --source data/sales.csv \
  --table  sales_clean \
  --db     "postgresql://user:pass@localhost:5432/mydb" \
  --types  '{"date":"datetime","unit_price":"float","quantity":"int"}' \
  --ranges '{"unit_price":[0,10000],"quantity":[1,100]}'

# Employees JSON with department filter
python run_pipeline.py \
  --source data/employees.json \
  --table  employees_clean \
  --types  '{"salary":"float","hire_date":"datetime"}' \
  --ranges '{"salary":[15000,200000]}' \
  --filter "salary > 0"
```

---

## 🧠 Architecture

```
Source File / Database
       │
       ▼
┌─────────────┐
│  EXTRACTOR  │  CSV, JSON, Excel, Parquet, SQL
│             │  Multi-file concatenation
└──────┬──────┘
       │ pandas DataFrame
       ▼
┌─────────────────────────────────────────┐
│              TRANSFORMER                │
│  drop_duplicates()  ──  deduplication  │
│  drop_nulls()       ──  null handling  │
│  cast_types()       ──  int/float/date │
│  normalize_strings()──  strip+lower   │
│  rename_columns()   ──  rename        │
│  drop_columns()     ──  remove cols   │
│  remove_outliers_iqr()  IQR method    │
│  filter_rows()      ──  pandas query  │
│  add_metadata()     ──  _loaded_at    │
└──────┬──────────────────────────────────┘
       │ clean DataFrame
       ▼
┌─────────────────────────────────────────┐
│              VALIDATOR                  │
│  required_columns check                │
│  not_null_columns check                │
│  numeric_ranges check                  │
│  allowed_values check                  │
│  → valid_df  + rejected_df             │
└──────┬──────────────────────────────────┘
       │ valid rows
       ▼
┌─────────────┐
│   LOADER    │  SQLite / PostgreSQL / MySQL
│             │  Chunked inserts, rejected.csv
└─────────────┘
       │
       ▼
  logs/run_TIMESTAMP.log
  logs/rejected_TIMESTAMP.csv  (if any)
```

---

## 🧪 Running Tests

```bash
pytest tests.py -v
```

Expected output:
```
tests.py::TestETLResult::test_rows_dropped         PASSED
tests.py::TestExtractor::test_read_csv             PASSED
tests.py::TestExtractor::test_file_not_found       PASSED
tests.py::TestTransformer::test_drop_duplicates    PASSED
tests.py::TestTransformer::test_cast_datetime      PASSED
tests.py::TestValidator::test_numeric_range_rejects PASSED
tests.py::TestLoader::test_to_sql_basic            PASSED
tests.py::TestETLPipelineIntegration::test_full_pipeline_csv PASSED
... (30+ tests)
```

---

## 🗄️ Supported Databases

| Database | Connection URL |
|----------|--------------|
| SQLite (default) | `sqlite:///output.db` |
| PostgreSQL | `postgresql://user:pass@host:5432/dbname` |
| MySQL | `mysql+pymysql://user:pass@host:3306/dbname` |

---

## 🔧 Use the Python API Directly

```python
from etl_pipeline import ETLPipeline

pipeline = ETLPipeline(
    source       = "data/consultations.csv",
    target_table = "consultations_clean",
    db_url       = "sqlite:///hospital.db",
    not_null_cols = ["id", "patient_id", "date"],
    type_schema   = {"date": "datetime", "cout": "float"},
    numeric_ranges = {"cout": (0, 1000)},
    normalize_cols = ["service", "diagnostic"],
    if_exists      = "replace",
)

result = pipeline.run()
print(f"Loaded {result.rows_loaded} rows in {result.duration_s:.2f}s")
print(result.summary())
```

---

## 🚀 Possible Improvements

- [ ] Apache Airflow integration for scheduling
- [ ] Parquet output format
- [ ] Delta/upsert mode (merge instead of append)
- [ ] Email/Slack notifications on failure
- [ ] Web dashboard (Flask) for pipeline monitoring
- [ ] Docker Compose setup with PostgreSQL

---

## 📄 License

MIT License — free to use and modify.
