from __future__ import annotations

import contextlib
import time
from typing import Any, Dict, List

from connectors.source_connector import SourceConnection, prepare_duckdb_for_gx_validation


def _log(msg: str) -> None:
    print(f"[gx_runner] {msg}")


_gx_duckdb_patch_installed = False


def _ensure_gx_duckdb_connection_patch() -> None:
    """
    Great Expectations closes the SQLAlchemy connection between metric queries for most
    dialects; DuckDB returns empty result sets unless one connection is persisted (GX #10738).
    """
    global _gx_duckdb_patch_installed
    if _gx_duckdb_patch_installed:
        return
    try:
        from great_expectations.execution_engine.sqlalchemy_execution_engine import (
            SqlAlchemyExecutionEngine,
        )
    except ImportError:
        return

    _orig = SqlAlchemyExecutionEngine.get_connection

    @contextlib.contextmanager
    def _patched_get_connection(self: Any) -> Any:
        if self.dialect_name == "duckdb":
            try:
                if not self._connection:
                    self._connection = self.engine.connect()
                yield self._connection
            finally:
                pass
        else:
            with _orig(self) as c:
                yield c

    SqlAlchemyExecutionEngine.get_connection = _patched_get_connection  # type: ignore[method-assign]
    _gx_duckdb_patch_installed = True
    _log("installed DuckDB persisted-connection patch for SqlAlchemyExecutionEngine")


def _suite_dict_to_expectation_suite(suite: dict) -> Any:
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

    name = str(suite.get("expectation_suite_name") or "auto_suite")
    configs: List[Any] = []
    for raw in suite.get("expectations") or []:
        if not isinstance(raw, dict):
            continue
        et = str(raw.get("expectation_type") or raw.get("type") or "").strip()
        if not et:
            continue
        kwargs = dict(raw.get("kwargs") or {})
        configs.append(
            ExpectationConfiguration(
                type=et,
                kwargs=kwargs,
                meta=raw.get("meta"),
            )
        )
    return ExpectationSuite(name=name, expectations=configs)


def _extract_failures(validation_result: Any) -> tuple[bool, List[dict], int, int, int]:
    failures: List[dict] = []
    results = getattr(validation_result, "results", None) or []
    total = len(results)
    failed = 0
    for r in results:
        ok = bool(getattr(r, "success", False))
        if ok:
            continue
        failed += 1
        cfg = getattr(r, "expectation_config", None)
        et = str(getattr(cfg, "type", None) or "")
        kwargs = dict(getattr(cfg, "kwargs", None) or {})
        column = kwargs.get("column")
        res = getattr(r, "result", None) or {}
        if isinstance(res, dict):
            unexpected_count = res.get("unexpected_count")
            unexpected_percent = res.get("unexpected_percent")
            partial = res.get("partial_unexpected_list") or res.get("partial_unexpected_counts")
        else:
            unexpected_count = getattr(res, "get", lambda *_: None)("unexpected_count")
            unexpected_percent = getattr(res, "get", lambda *_: None)("unexpected_percent")
            partial = getattr(res, "get", lambda *_: None)("partial_unexpected_list")
        failures.append(
            {
                "expectation_type": et,
                "column": column,
                "unexpected_count": unexpected_count,
                "unexpected_percent": unexpected_percent,
                "partial_unexpected_list": partial if isinstance(partial, list) else [],
            }
        )
    passed = total - failed
    gx_passed = failed == 0 and total > 0
    if total == 0:
        gx_passed = True
    return gx_passed, failures, total, passed, failed


def run_gx_validation(
    source_conn: SourceConnection,
    suite: dict,
    dataset_name: str,
) -> dict:
    """
    Execute a Great Expectations suite against the DuckDB VIEW or Azure SQL table.

    Uses the Fluent ``DataContext`` SQL datasource API with ``SqlAlchemyExecutionEngine`` under the hood.
    """
    t0 = time.time()
    try:
        import great_expectations as gx  # noqa: PLC0415
    except ImportError as e:
        raise ImportError(
            "Great Expectations is required for GX validation. Install with: pip install 'great-expectations>=0.18.0'"
        ) from e

    suite_name = str(suite.get("expectation_suite_name") or f"{dataset_name}_auto_suite")
    try:
        suite_obj = _suite_dict_to_expectation_suite(suite)
    except ImportError as e:
        raise ImportError(
            "Great Expectations is required for GX validation. Install with: pip install 'great-expectations>=0.18.0'"
        ) from e

    ctx = gx.get_context(mode="ephemeral")
    gx_passed = False
    failures: List[dict] = []
    total = passed = failed = 0
    engine_label = "duckdb_sqlalchemy"

    try:
        if source_conn.source_type in ("csv", "json", "parquet", "xml", "excel"):
            _ensure_gx_duckdb_connection_patch()
            prepare_duckdb_for_gx_validation(source_conn)
            db_path = source_conn.duckdb_database_path
            if not db_path:
                raise RuntimeError("Missing duckdb_database_path on SourceConnection")
            from pathlib import Path

            url = "duckdb:///" + Path(db_path).resolve().as_posix()
            ds = ctx.data_sources.add_sql(name=f"dhara_{dataset_name}_{id(source_conn)}", connection_string=url)
            asset = ds.add_table_asset(name=f"asset_{dataset_name}", table_name=source_conn.view_name)
            batch_def = asset.add_batch_definition_whole_table("whole_table")
            batch = batch_def.get_batch()
            val_result = batch.validate(suite_obj)
            gx_passed, failures, total, passed, failed = _extract_failures(val_result)
            engine_label = "duckdb_sqlalchemy"

        elif source_conn.source_type == "azure_sql":
            ds = ctx.data_sources.add_sql(
                name=f"dhara_sql_{dataset_name}_{id(source_conn)}",
                connection_string=source_conn.source_path,
            )
            tbl = source_conn.table_name or source_conn.view_name
            asset = ds.add_table_asset(name=f"sql_asset_{dataset_name}", table_name=tbl)
            batch_def = asset.add_batch_definition_whole_table("whole_table")
            batch = batch_def.get_batch()
            val_result = batch.validate(suite_obj)
            gx_passed, failures, total, passed, failed = _extract_failures(val_result)
            engine_label = "azure_sql_sqlalchemy"
        else:
            raise ValueError(f"Unsupported source_type for GX: {source_conn.source_type!r}")

    except Exception as e:
        _log(f"GX validation error for {dataset_name!r}: {e}")
        return {
            "dataset": dataset_name,
            "gx_passed": False,
            "total_expectations": 0,
            "passed_expectations": 0,
            "failed_expectations": 0,
            "failures": [],
            "error": str(e),
            "gx_meta": {
                "engine": engine_label,
                "suite_name": suite_name,
                "run_time_seconds": round(time.time() - t0, 3),
            },
        }

    elapsed = round(time.time() - t0, 3)
    _log(
        f"GX finished dataset={dataset_name!r} passed={gx_passed} "
        f"expectations_ok={passed}/{total} in {elapsed}s"
    )
    return {
        "dataset": dataset_name,
        "gx_passed": gx_passed,
        "total_expectations": total,
        "passed_expectations": passed,
        "failed_expectations": failed,
        "failures": failures,
        "gx_meta": {
            "engine": engine_label,
            "suite_name": suite_name,
            "run_time_seconds": elapsed,
        },
    }
