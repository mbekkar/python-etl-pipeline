"""
ETL Pipeline — Core Engine
============================
Extract → Transform → Validate → Load

Supports multiple source formats: CSV, JSON, Excel, SQL database.
Outputs to SQLite, PostgreSQL, or MySQL.

Author : Mounir Bekkar
"""

import os
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text


# ── LOGGING ───────────────────────────────────────────────────────────────────

def setup_logger(name: str = "etl", log_file: Optional[str] = None) -> logging.Logger:
    """Configure a structured logger with console + optional file output."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-5s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler (optional)
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


# ── RESULT DATACLASS ──────────────────────────────────────────────────────────

@dataclass
class ETLResult:
    """Stores metrics and status for one pipeline run."""
    run_id:           str   = ""
    source:           str   = ""
    target_table:     str   = ""
    rows_extracted:   int   = 0
    rows_after_clean: int   = 0
    rows_loaded:      int   = 0
    rows_rejected:    int   = 0
    duration_s:       float = 0.0
    success:          bool  = False
    warnings:         list  = field(default_factory=list)
    errors:           list  = field(default_factory=list)

    @property
    def rows_dropped(self) -> int:
        return self.rows_extracted - self.rows_after_clean

    def summary(self) -> str:
        status = "✅ SUCCESS" if self.success else "❌ FAILED"
        return (
            f"\n{'─'*55}\n"
            f"  {status}  —  Run ID: {self.run_id}\n"
            f"{'─'*55}\n"
            f"  Source          : {self.source}\n"
            f"  Target table    : {self.target_table}\n"
            f"  Rows extracted  : {self.rows_extracted:,}\n"
            f"  Rows dropped    : {self.rows_dropped:,}\n"
            f"  Rows loaded     : {self.rows_loaded:,}\n"
            f"  Rows rejected   : {self.rows_rejected:,}\n"
            f"  Duration        : {self.duration_s:.2f}s\n"
            f"  Warnings        : {len(self.warnings)}\n"
            f"{'─'*55}\n"
        )


# ── EXTRACTOR ─────────────────────────────────────────────────────────────────

class Extractor:
    """
    Reads data from various sources into a pandas DataFrame.
    Supports: CSV, JSON, Excel (.xlsx), Parquet, SQL database.
    """

    SUPPORTED = {".csv", ".json", ".xlsx", ".xls", ".parquet"}

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def from_file(self, path: str, **kwargs) -> pd.DataFrame:
        """Auto-detect format from file extension and read."""
        p   = Path(path)
        ext = p.suffix.lower()

        if not p.exists():
            raise FileNotFoundError(f"Source file not found: {path}")
        if ext not in self.SUPPORTED:
            raise ValueError(f"Unsupported format: {ext}. Supported: {self.SUPPORTED}")

        self.logger.info(f"[EXTRACT] Reading {ext.upper()} file: {p.name}")

        readers = {
            ".csv":     lambda: pd.read_csv(path, **kwargs),
            ".json":    lambda: pd.read_json(path, **kwargs),
            ".xlsx":    lambda: pd.read_excel(path, **kwargs),
            ".xls":     lambda: pd.read_excel(path, **kwargs),
            ".parquet": lambda: pd.read_parquet(path, **kwargs),
        }

        df = readers[ext]()
        self.logger.info(f"[EXTRACT] Loaded {len(df):,} rows × {len(df.columns)} columns")
        return df

    def from_sql(self, query: str, connection_url: str, **kwargs) -> pd.DataFrame:
        """Read data from a SQL database using a SELECT query."""
        self.logger.info(f"[EXTRACT] Reading from SQL database")
        engine = sa.create_engine(connection_url)
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, **kwargs)
        self.logger.info(f"[EXTRACT] Loaded {len(df):,} rows from SQL")
        engine.dispose()
        return df

    def from_multiple(self, paths: list[str], **kwargs) -> pd.DataFrame:
        """Extract and concatenate multiple files of the same format."""
        self.logger.info(f"[EXTRACT] Reading {len(paths)} files")
        frames = []
        for p in paths:
            df = self.from_file(p, **kwargs)
            df["_source_file"] = Path(p).name
            frames.append(df)
        combined = pd.concat(frames, ignore_index=True)
        self.logger.info(f"[EXTRACT] Combined: {len(combined):,} rows total")
        return combined


# ── TRANSFORMER ───────────────────────────────────────────────────────────────

class Transformer:
    """
    Cleans and transforms a DataFrame.
    All transformations are logged and counted.
    """

    def __init__(self, logger: logging.Logger, result: ETLResult):
        self.logger = logger
        self.result = result

    # ── Deduplication ──────────────────────────────────────────────────────────

    def drop_duplicates(self, df: pd.DataFrame, subset: Optional[list] = None) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=subset)
        dropped = before - len(df)
        if dropped:
            self.result.warnings.append(f"{dropped} duplicate rows removed")
            self.logger.warning(f"[TRANSFORM] Removed {dropped:,} duplicate rows")
        else:
            self.logger.info("[TRANSFORM] No duplicates found")
        return df

    # ── Null handling ──────────────────────────────────────────────────────────

    def drop_nulls(self, df: pd.DataFrame, subset: list) -> pd.DataFrame:
        """Drop rows where ANY of the required columns is null."""
        before = len(df)
        df = df.dropna(subset=subset)
        dropped = before - len(df)
        if dropped:
            self.result.warnings.append(f"{dropped} rows dropped (null in required columns: {subset})")
            self.logger.warning(f"[TRANSFORM] Dropped {dropped:,} rows with nulls in {subset}")
        return df

    def fill_nulls(self, df: pd.DataFrame, fill_values: dict) -> pd.DataFrame:
        """Fill nulls with specified values per column."""
        df = df.fillna(fill_values)
        self.logger.info(f"[TRANSFORM] Filled nulls: {fill_values}")
        return df

    # ── Type casting ───────────────────────────────────────────────────────────

    def cast_types(self, df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """
        Cast columns to target types.
        schema: {"column_name": "int" | "float" | "str" | "datetime" | "bool"}
        """
        for col, dtype in schema.items():
            if col not in df.columns:
                self.logger.warning(f"[TRANSFORM] Column '{col}' not found, skipping cast")
                continue
            try:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                elif dtype == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "bool":
                    df[col] = df[col].astype(bool)
                else:
                    df[col] = df[col].astype(str)
                self.logger.info(f"[TRANSFORM] Cast '{col}' → {dtype}")
            except Exception as e:
                self.logger.warning(f"[TRANSFORM] Cast failed for '{col}': {e}")
        return df

    # ── Value normalization ────────────────────────────────────────────────────

    def normalize_strings(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Strip whitespace and lowercase string columns."""
        for col in columns:
            if col in df.columns and df[col].dtype == object:
                df[col] = df[col].str.strip().str.lower()
        self.logger.info(f"[TRANSFORM] Normalized strings: {columns}")
        return df

    def rename_columns(self, df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        """Rename columns."""
        df = df.rename(columns=mapping)
        self.logger.info(f"[TRANSFORM] Renamed columns: {mapping}")
        return df

    def drop_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Drop columns that exist."""
        existing = [c for c in columns if c in df.columns]
        df = df.drop(columns=existing)
        if existing:
            self.logger.info(f"[TRANSFORM] Dropped columns: {existing}")
        return df

    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add pipeline metadata columns."""
        df["_loaded_at"] = datetime.now()
        df["_run_id"]    = self.result.run_id
        return df

    # ── Outlier removal ────────────────────────────────────────────────────────

    def remove_outliers_iqr(self, df: pd.DataFrame, column: str, factor: float = 1.5) -> pd.DataFrame:
        """Remove outliers using IQR method."""
        if column not in df.columns:
            return df
        Q1, Q3 = df[column].quantile([0.25, 0.75])
        IQR     = Q3 - Q1
        mask    = df[column].between(Q1 - factor * IQR, Q3 + factor * IQR)
        removed = (~mask).sum()
        if removed:
            self.result.warnings.append(f"{removed} outliers removed from '{column}' (IQR method)")
            self.logger.warning(f"[TRANSFORM] Removed {removed:,} outliers from '{column}'")
        return df[mask]

    # ── Filter ────────────────────────────────────────────────────────────────

    def filter_rows(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Filter rows using a pandas query string."""
        before = len(df)
        df     = df.query(condition)
        removed = before - len(df)
        self.logger.info(f"[TRANSFORM] Filter '{condition}': removed {removed:,} rows, kept {len(df):,}")
        return df


# ── VALIDATOR ─────────────────────────────────────────────────────────────────

class Validator:
    """
    Validates a DataFrame against a set of rules before loading.
    Returns (valid_df, rejected_df).
    """

    def __init__(self, logger: logging.Logger, result: ETLResult):
        self.logger = logger
        self.result = result

    def validate(
        self,
        df: pd.DataFrame,
        required_columns: Optional[list] = None,
        not_null_columns: Optional[list] = None,
        numeric_ranges:   Optional[dict] = None,
        allowed_values:   Optional[dict] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply all validation rules.
        Returns (valid_df, rejected_df) — rows failing any rule are rejected.

        Args:
            required_columns: columns that must exist in the DataFrame
            not_null_columns:  columns that must not be null
            numeric_ranges:    {"col": (min, max)} — values must be in range
            allowed_values:    {"col": [v1, v2, ...]} — values must be in set
        """
        self.logger.info("[VALIDATE] Starting data quality checks")

        # Check required columns exist
        if required_columns:
            missing = [c for c in required_columns if c not in df.columns]
            if missing:
                raise ValueError(f"[VALIDATE] Missing required columns: {missing}")

        mask_valid = pd.Series([True] * len(df), index=df.index)

        # Not-null check
        if not_null_columns:
            for col in not_null_columns:
                if col in df.columns:
                    mask = df[col].notna()
                    null_count = (~mask).sum()
                    if null_count:
                        self.logger.warning(f"[VALIDATE] {null_count:,} null values in required column '{col}'")
                    mask_valid &= mask

        # Numeric range check
        if numeric_ranges:
            for col, (lo, hi) in numeric_ranges.items():
                if col in df.columns:
                    mask = df[col].between(lo, hi, inclusive="both")
                    out_count = (~mask & df[col].notna()).sum()
                    if out_count:
                        self.logger.warning(f"[VALIDATE] {out_count:,} values out of range [{lo}, {hi}] in '{col}'")
                    mask_valid &= mask | df[col].isna()

        # Allowed values check
        if allowed_values:
            for col, vals in allowed_values.items():
                if col in df.columns:
                    mask = df[col].isin(vals) | df[col].isna()
                    bad_count = (~mask).sum()
                    if bad_count:
                        self.logger.warning(f"[VALIDATE] {bad_count:,} invalid values in '{col}'")
                    mask_valid &= mask

        valid_df    = df[mask_valid].copy()
        rejected_df = df[~mask_valid].copy()

        self.result.rows_rejected = len(rejected_df)
        self.logger.info(f"[VALIDATE] ✓ Valid: {len(valid_df):,}  |  ✗ Rejected: {len(rejected_df):,}")

        return valid_df, rejected_df


# ── LOADER ────────────────────────────────────────────────────────────────────

class Loader:
    """
    Loads a DataFrame into a SQL database.
    Supports: SQLite (default), PostgreSQL, MySQL.
    """

    def __init__(self, logger: logging.Logger, result: ETLResult):
        self.logger = logger
        self.result = result

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        connection_url: str,
        if_exists: str = "append",
        chunksize: int = 1000,
        index: bool = False,
    ) -> int:
        """
        Load DataFrame into SQL table.

        Args:
            df:             data to load
            table:          target table name
            connection_url: SQLAlchemy connection string
            if_exists:      "append" | "replace" | "fail"
            chunksize:      rows per insert batch
            index:          whether to write the DataFrame index

        Returns:
            Number of rows loaded.
        """
        if df.empty:
            self.logger.warning("[LOAD] DataFrame is empty, nothing to load")
            return 0

        engine = sa.create_engine(connection_url)
        self.logger.info(f"[LOAD] Loading {len(df):,} rows → {table} (if_exists='{if_exists}')")

        try:
            df.to_sql(
                name=table,
                con=engine,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method="multi",
            )
            self.result.rows_loaded = len(df)
            self.logger.info(f"[LOAD] ✅ {len(df):,} rows successfully loaded into '{table}'")
            return len(df)

        except Exception as e:
            self.result.errors.append(str(e))
            self.logger.error(f"[LOAD] ❌ Failed to load into '{table}': {e}")
            raise

        finally:
            engine.dispose()

    def to_csv(self, df: pd.DataFrame, path: str, index: bool = False) -> None:
        """Export DataFrame to CSV."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        df.to_csv(path, index=index)
        self.logger.info(f"[LOAD] Exported {len(df):,} rows → {path}")

    def save_rejected(self, rejected: pd.DataFrame, path: str) -> None:
        """Save rejected rows to CSV for manual review."""
        if rejected.empty:
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        rejected.to_csv(path, index=False)
        self.logger.warning(f"[LOAD] {len(rejected):,} rejected rows saved → {path}")


# ── PIPELINE ──────────────────────────────────────────────────────────────────

class ETLPipeline:
    """
    Orchestrates the full Extract → Transform → Validate → Load pipeline.

    Example usage:
        pipeline = ETLPipeline(
            source="data/consultations.csv",
            target_table="consultations_clean",
            db_url="sqlite:///output.db",
        )
        result = pipeline.run()
        print(result.summary())
    """

    def __init__(
        self,
        source:         str,
        target_table:   str,
        db_url:         str = "sqlite:///etl_output.db",
        log_dir:        str = "logs",
        required_cols:  Optional[list] = None,
        not_null_cols:  Optional[list] = None,
        type_schema:    Optional[dict] = None,
        numeric_ranges: Optional[dict] = None,
        allowed_values: Optional[dict] = None,
        rename_map:     Optional[dict] = None,
        drop_cols:      Optional[list] = None,
        normalize_cols: Optional[list] = None,
        filter_query:   Optional[str]  = None,
        if_exists:      str = "append",
    ):
        self.source         = source
        self.target_table   = target_table
        self.db_url         = db_url
        self.required_cols  = required_cols or []
        self.not_null_cols  = not_null_cols or []
        self.type_schema    = type_schema or {}
        self.numeric_ranges = numeric_ranges or {}
        self.allowed_values = allowed_values or {}
        self.rename_map     = rename_map or {}
        self.drop_cols      = drop_cols or []
        self.normalize_cols = normalize_cols or []
        self.filter_query   = filter_query
        self.if_exists      = if_exists

        # Build run ID
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Build result
        self.result = ETLResult(
            run_id=run_id,
            source=source,
            target_table=target_table,
        )

        # Logger
        log_file = os.path.join(log_dir, f"run_{run_id}.log")
        self.logger = setup_logger("etl", log_file=log_file)

        # Components
        self.extractor   = Extractor(self.logger)
        self.transformer = Transformer(self.logger, self.result)
        self.validator   = Validator(self.logger, self.result)
        self.loader      = Loader(self.logger, self.result)

    def run(self) -> ETLResult:
        """Execute the full pipeline and return an ETLResult."""
        t_start = time.time()
        self.logger.info(f"{'═'*55}")
        self.logger.info(f"  ETL Pipeline  —  Run ID: {self.result.run_id}")
        self.logger.info(f"{'═'*55}")

        try:
            # ── EXTRACT ──────────────────────────────────────────────────────
            df = self.extractor.from_file(self.source)
            self.result.rows_extracted = len(df)

            # ── TRANSFORM ─────────────────────────────────────────────────────
            df = self.transformer.drop_duplicates(df)

            if self.rename_map:
                df = self.transformer.rename_columns(df, self.rename_map)
            if self.drop_cols:
                df = self.transformer.drop_columns(df, self.drop_cols)
            if self.not_null_cols:
                df = self.transformer.drop_nulls(df, self.not_null_cols)
            if self.type_schema:
                df = self.transformer.cast_types(df, self.type_schema)
            if self.normalize_cols:
                df = self.transformer.normalize_strings(df, self.normalize_cols)
            if self.filter_query:
                df = self.transformer.filter_rows(df, self.filter_query)

            df = self.transformer.add_metadata(df)
            self.result.rows_after_clean = len(df)

            # ── VALIDATE ──────────────────────────────────────────────────────
            valid_df, rejected_df = self.validator.validate(
                df,
                required_columns=self.required_cols if self.required_cols else None,
                not_null_columns=self.not_null_cols if self.not_null_cols else None,
                numeric_ranges=self.numeric_ranges if self.numeric_ranges else None,
                allowed_values=self.allowed_values if self.allowed_values else None,
            )

            # Save rejected rows
            if not rejected_df.empty:
                rejected_path = f"logs/rejected_{self.result.run_id}.csv"
                self.loader.save_rejected(rejected_df, rejected_path)

            # ── LOAD ──────────────────────────────────────────────────────────
            self.loader.to_sql(
                valid_df,
                table=self.target_table,
                connection_url=self.db_url,
                if_exists=self.if_exists,
            )

            self.result.success = True

        except Exception as e:
            self.result.errors.append(str(e))
            self.logger.error(f"[PIPELINE] ❌ Pipeline failed: {e}")
            raise

        finally:
            self.result.duration_s = round(time.time() - t_start, 2)
            self.logger.info(self.result.summary())

        return self.result
