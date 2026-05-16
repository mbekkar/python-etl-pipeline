"""
Unit & Integration Tests — ETL Pipeline
=========================================
Run with: pytest tests.py -v

Author : Mounir Bekkar
"""

import os
import json
import pytest
import pandas as pd
import numpy as np
import sqlalchemy as sa
from pathlib import Path
from datetime import datetime

from etl_pipeline import (
    ETLPipeline, ETLResult,
    Extractor, Transformer, Validator, Loader,
    setup_logger,
)


# ── FIXTURES ──────────────────────────────────────────────────────────────────

@pytest.fixture
def logger(tmp_path):
    return setup_logger("test", log_file=str(tmp_path / "test.log"))

@pytest.fixture
def result():
    return ETLResult(run_id="TEST_001", source="test.csv", target_table="test_table")

@pytest.fixture
def sample_df():
    """A clean, well-formed DataFrame."""
    return pd.DataFrame({
        "id":     [1, 2, 3, 4, 5],
        "name":   ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value":  [100.0, 200.5, 300.0, 150.0, 250.0],
        "date":   ["2024-01-01", "2024-02-15", "2024-03-10", "2024-04-20", "2024-05-05"],
        "status": ["active", "inactive", "active", "active", "inactive"],
    })

@pytest.fixture
def messy_df():
    """A DataFrame with duplicates, nulls, and type issues."""
    return pd.DataFrame({
        "id":     [1, 2, 3, 2, None, 4],          # duplicate row 2, null id
        "name":   ["Alice", "Bob", None, "Bob", "Charlie", "Diana"],
        "value":  [100, -50, 200, -50, 300, None],  # negative and null values
        "date":   ["2024-01-01", "invalid_date", "2024-03-10", "invalid_date", "2024-05-05", "2024-06-01"],
        "status": ["active", "active", "unknown", "active", "active", "inactive"],
    })

@pytest.fixture
def csv_file(tmp_path, sample_df):
    path = str(tmp_path / "test.csv")
    sample_df.to_csv(path, index=False)
    return path

@pytest.fixture
def json_file(tmp_path, sample_df):
    path = str(tmp_path / "test.json")
    sample_df.to_json(path, orient="records", indent=2)
    return path

@pytest.fixture
def db_url(tmp_path):
    return f"sqlite:///{tmp_path}/test.db"


# ── ETLResult TESTS ───────────────────────────────────────────────────────────

class TestETLResult:
    def test_rows_dropped(self):
        r = ETLResult(rows_extracted=100, rows_after_clean=88)
        assert r.rows_dropped == 12

    def test_summary_contains_run_id(self):
        r = ETLResult(run_id="TEST_XYZ", success=True)
        assert "TEST_XYZ" in r.summary()

    def test_summary_success(self):
        r = ETLResult(success=True)
        assert "SUCCESS" in r.summary()

    def test_summary_failure(self):
        r = ETLResult(success=False)
        assert "FAILED" in r.summary()

    def test_warnings_list(self):
        r = ETLResult()
        r.warnings.append("test warning")
        assert len(r.warnings) == 1


# ── EXTRACTOR TESTS ───────────────────────────────────────────────────────────

class TestExtractor:
    def test_read_csv(self, logger, csv_file, sample_df):
        ext = Extractor(logger)
        df  = ext.from_file(csv_file)
        assert len(df) == len(sample_df)
        assert list(df.columns) == list(sample_df.columns)

    def test_read_json(self, logger, json_file, sample_df):
        ext = Extractor(logger)
        df  = ext.from_file(json_file)
        assert len(df) == len(sample_df)

    def test_file_not_found(self, logger):
        ext = Extractor(logger)
        with pytest.raises(FileNotFoundError):
            ext.from_file("/nonexistent/path/file.csv")

    def test_unsupported_extension(self, logger, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("hello")
        ext = Extractor(logger)
        with pytest.raises(ValueError, match="Unsupported format"):
            ext.from_file(str(p))

    def test_read_multiple_files(self, logger, tmp_path, sample_df):
        paths = []
        for i in range(3):
            p = str(tmp_path / f"part_{i}.csv")
            sample_df.to_csv(p, index=False)
            paths.append(p)
        ext = Extractor(logger)
        combined = ext.from_multiple(paths)
        assert len(combined) == len(sample_df) * 3
        assert "_source_file" in combined.columns


# ── TRANSFORMER TESTS ─────────────────────────────────────────────────────────

class TestTransformer:
    def test_drop_duplicates(self, logger, result):
        df = pd.DataFrame({"a": [1, 2, 2, 3], "b": ["x", "y", "y", "z"]})
        t  = Transformer(logger, result)
        out = t.drop_duplicates(df)
        assert len(out) == 3
        assert "duplicate" in result.warnings[0].lower()

    def test_drop_nulls(self, logger, result):
        df = pd.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})
        t  = Transformer(logger, result)
        out = t.drop_nulls(df, ["id"])
        assert len(out) == 2
        assert out["id"].notna().all()

    def test_cast_datetime(self, logger, result, sample_df):
        t  = Transformer(logger, result)
        out = t.cast_types(sample_df, {"date": "datetime"})
        assert pd.api.types.is_datetime64_any_dtype(out["date"])

    def test_cast_float(self, logger, result):
        df = pd.DataFrame({"val": ["1.5", "2.0", "abc"]})
        t  = Transformer(logger, result)
        out = t.cast_types(df, {"val": "float"})
        assert pd.api.types.is_float_dtype(out["val"])
        assert pd.isna(out.loc[2, "val"])  # "abc" → NaN

    def test_normalize_strings(self, logger, result):
        df = pd.DataFrame({"city": ["  Paris ", "LYON", "  bordeaux"]})
        t  = Transformer(logger, result)
        out = t.normalize_strings(df, ["city"])
        assert out["city"].tolist() == ["paris", "lyon", "bordeaux"]

    def test_rename_columns(self, logger, result):
        df = pd.DataFrame({"old_name": [1, 2]})
        t  = Transformer(logger, result)
        out = t.rename_columns(df, {"old_name": "new_name"})
        assert "new_name" in out.columns
        assert "old_name" not in out.columns

    def test_drop_columns(self, logger, result, sample_df):
        t   = Transformer(logger, result)
        out = t.drop_columns(sample_df.copy(), ["status", "date"])
        assert "status" not in out.columns
        assert "date" not in out.columns

    def test_add_metadata(self, logger, result):
        df = pd.DataFrame({"a": [1, 2, 3]})
        t  = Transformer(logger, result)
        out = t.add_metadata(df)
        assert "_loaded_at" in out.columns
        assert "_run_id" in out.columns
        assert (out["_run_id"] == "TEST_001").all()

    def test_remove_outliers_iqr(self, logger, result):
        df = pd.DataFrame({"val": [10, 12, 11, 13, 10, 9, 1000, -500]})
        t  = Transformer(logger, result)
        out = t.remove_outliers_iqr(df, "val")
        assert 1000 not in out["val"].values
        assert -500 not in out["val"].values

    def test_filter_rows(self, logger, result):
        df = pd.DataFrame({"val": [10, 20, 30, 5, 50]})
        t  = Transformer(logger, result)
        out = t.filter_rows(df, "val >= 20")
        assert (out["val"] >= 20).all()
        assert len(out) == 3


# ── VALIDATOR TESTS ───────────────────────────────────────────────────────────

class TestValidator:
    def test_all_valid(self, logger, result, sample_df):
        v = Validator(logger, result)
        valid, rejected = v.validate(sample_df)
        assert len(valid) == len(sample_df)
        assert len(rejected) == 0

    def test_not_null_rejects(self, logger, result):
        df = pd.DataFrame({"id": [1, None, 3], "val": [10, 20, 30]})
        v  = Validator(logger, result)
        valid, rejected = v.validate(df, not_null_columns=["id"])
        assert len(valid) == 2
        assert len(rejected) == 1

    def test_numeric_range_rejects(self, logger, result):
        df = pd.DataFrame({"price": [10.0, -5.0, 200.0, 9999.0]})
        v  = Validator(logger, result)
        valid, rejected = v.validate(df, numeric_ranges={"price": (0, 1000)})
        assert len(valid) == 2   # 10 and 200
        assert len(rejected) == 2  # -5 and 9999

    def test_allowed_values_rejects(self, logger, result):
        df = pd.DataFrame({"status": ["active", "inactive", "unknown", "deleted"]})
        v  = Validator(logger, result)
        valid, rejected = v.validate(df, allowed_values={"status": ["active", "inactive"]})
        assert len(valid) == 2
        assert len(rejected) == 2

    def test_missing_required_column_raises(self, logger, result):
        df = pd.DataFrame({"name": ["Alice"]})
        v  = Validator(logger, result)
        with pytest.raises(ValueError, match="Missing required columns"):
            v.validate(df, required_columns=["id", "name"])

    def test_result_rows_rejected_updated(self, logger, result):
        df = pd.DataFrame({"val": [10, -5, 200]})
        v  = Validator(logger, result)
        v.validate(df, numeric_ranges={"val": (0, 100)})
        assert result.rows_rejected == 1


# ── LOADER TESTS ──────────────────────────────────────────────────────────────

class TestLoader:
    def test_to_sql_basic(self, logger, result, sample_df, db_url):
        loader = Loader(logger, result)
        rows   = loader.to_sql(sample_df, "test_table", db_url, if_exists="replace")
        assert rows == len(sample_df)

    def test_to_sql_empty_df(self, logger, result, db_url):
        loader = Loader(logger, result)
        empty  = pd.DataFrame({"a": []})
        rows   = loader.to_sql(empty, "empty_table", db_url, if_exists="replace")
        assert rows == 0

    def test_to_sql_readable(self, logger, result, sample_df, db_url):
        loader = Loader(logger, result)
        loader.to_sql(sample_df, "read_test", db_url, if_exists="replace")

        engine = sa.create_engine(db_url)
        with engine.connect() as conn:
            loaded = pd.read_sql(sa.text("SELECT * FROM read_test"), conn)
        engine.dispose()

        assert len(loaded) == len(sample_df)
        assert "id" in loaded.columns

    def test_to_csv(self, logger, result, sample_df, tmp_path):
        path   = str(tmp_path / "output.csv")
        loader = Loader(logger, result)
        loader.to_csv(sample_df, path)
        assert os.path.exists(path)
        reloaded = pd.read_csv(path)
        assert len(reloaded) == len(sample_df)

    def test_save_rejected(self, logger, result, tmp_path):
        rejected = pd.DataFrame({"id": [99], "reason": ["invalid"]})
        path     = str(tmp_path / "rejected.csv")
        loader   = Loader(logger, result)
        loader.save_rejected(rejected, path)
        assert os.path.exists(path)

    def test_save_rejected_empty(self, logger, result, tmp_path):
        """Empty rejected df should not create a file."""
        empty    = pd.DataFrame()
        path     = str(tmp_path / "rejected_empty.csv")
        loader   = Loader(logger, result)
        loader.save_rejected(empty, path)
        assert not os.path.exists(path)


# ── INTEGRATION TESTS ─────────────────────────────────────────────────────────

class TestETLPipelineIntegration:
    def test_full_pipeline_csv(self, tmp_path, sample_df):
        """End-to-end: CSV → SQLite."""
        # Write source
        csv_path = str(tmp_path / "source.csv")
        sample_df.to_csv(csv_path, index=False)
        db_url   = f"sqlite:///{tmp_path}/output.db"

        pipeline = ETLPipeline(
            source       = csv_path,
            target_table = "test_clean",
            db_url       = db_url,
            log_dir      = str(tmp_path / "logs"),
            not_null_cols = ["id", "name"],
            type_schema   = {"value": "float"},
            if_exists     = "replace",
        )

        result = pipeline.run()
        assert result.success is True
        assert result.rows_extracted == len(sample_df)
        assert result.rows_loaded > 0
        assert result.duration_s > 0

    def test_pipeline_with_nulls_filtered(self, tmp_path):
        """Rows with nulls in required columns should be dropped."""
        df = pd.DataFrame({
            "id":   [1, 2, None, 4, 5],
            "name": ["A", "B", "C", None, "E"],
            "val":  [10, 20, 30, 40, 50],
        })
        csv_path = str(tmp_path / "nulls.csv")
        df.to_csv(csv_path, index=False)
        db_url = f"sqlite:///{tmp_path}/nulls.db"

        pipeline = ETLPipeline(
            source        = csv_path,
            target_table  = "nulls_clean",
            db_url        = db_url,
            log_dir       = str(tmp_path / "logs"),
            not_null_cols = ["id", "name"],
            if_exists     = "replace",
        )
        result = pipeline.run()
        assert result.success is True
        assert result.rows_loaded == 3  # rows 1, 2, 5 (no nulls in id or name)

    def test_pipeline_with_validation_rejects(self, tmp_path):
        """Out-of-range values should be rejected, not loaded."""
        df = pd.DataFrame({
            "id":    [1, 2, 3, 4],
            "price": [100.0, -50.0, 200.0, 9999.0],
        })
        csv_path = str(tmp_path / "prices.csv")
        df.to_csv(csv_path, index=False)
        db_url = f"sqlite:///{tmp_path}/prices.db"

        pipeline = ETLPipeline(
            source         = csv_path,
            target_table   = "prices_clean",
            db_url         = db_url,
            log_dir        = str(tmp_path / "logs"),
            numeric_ranges = {"price": (0, 1000)},
            if_exists      = "replace",
        )
        result = pipeline.run()
        assert result.success is True
        assert result.rows_rejected == 2   # -50 and 9999
        assert result.rows_loaded == 2     # 100 and 200

    def test_pipeline_file_not_found(self, tmp_path):
        """Pipeline should raise FileNotFoundError for missing source."""
        pipeline = ETLPipeline(
            source       = "/nonexistent/data.csv",
            target_table = "test",
            db_url       = f"sqlite:///{tmp_path}/test.db",
            log_dir      = str(tmp_path / "logs"),
        )
        with pytest.raises(FileNotFoundError):
            pipeline.run()

    def test_data_persisted_to_db(self, tmp_path, sample_df):
        """Verify loaded data can be queried from the database."""
        csv_path = str(tmp_path / "source.csv")
        sample_df.to_csv(csv_path, index=False)
        db_url = f"sqlite:///{tmp_path}/verify.db"

        pipeline = ETLPipeline(
            source       = csv_path,
            target_table = "verify_table",
            db_url       = db_url,
            log_dir      = str(tmp_path / "logs"),
            if_exists    = "replace",
        )
        pipeline.run()

        engine = sa.create_engine(db_url)
        with engine.connect() as conn:
            df_loaded = pd.read_sql(sa.text("SELECT * FROM verify_table"), conn)
        engine.dispose()

        assert len(df_loaded) == len(sample_df)
        assert "_loaded_at" in df_loaded.columns
        assert "_run_id" in df_loaded.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
