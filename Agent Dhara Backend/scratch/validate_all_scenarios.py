"""Generate + validate template ETL for scenarios 2-11, 7-9, 14 engines."""
from __future__ import annotations

import sys
from typing import Any, Dict, List, Tuple

from tests.fixtures.blob_pair_assessment import blob_session_context, make_blob_pair_assessment

from agent.etl_handlers import _template_fallback
from agent.etl_pipeline.business_rules import normalize_business_rules
from agent.etl_pipeline.connector_manifest import build_connector_manifest
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.validate_plan import validate_etl_plan
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.validate_python import validate_etl_python_source
from agent.etl_pipeline.validate_sql import validate_sql_basic


def build(scenario_rules: Dict[str, Any], *, output_base: str = "cleaned/", overwrite: bool = False):
    assess = make_blob_pair_assessment(overlap_count=15)
    ctx = blob_session_context()
    rules = normalize_business_rules(scenario_rules)
    plan = build_etl_plan(assess, rules, source_context=build_source_context(ctx, assess))
    manifest = build_connector_manifest(
        ctx, assess, output_base=output_base, overwrite_in_place=overwrite
    )
    plan["connector_manifest"] = manifest
    plan["business_rules"] = rules
    return assess, plan, rules


SCENARIOS: List[Tuple[str, Dict[str, Any], List[str]]] = [
    ("S2 case+never_drop", {"never_drop_rows": True, "notes": "Normalize name and department to lowercase on both files."}, ["python", "pyspark"]),
    ("S3 phone hash", {"notes": "Hash phone on data_1.xml; lowercase name and department on both."}, ["python", "pyspark"]),
    ("S4 join default", {}, ["python"]),
    ("S5 required cols", {"required_columns": ["id", "name"]}, ["python"]),
    ("S6 valid_values", {"never_drop_rows": True, "valid_values": {"department": ["IT", "HR", "Finance"]}}, ["python"]),
    ("S7 sql", {}, ["sql"]),
    ("S8 pyspark", {"never_drop_rows": True}, ["pyspark"]),
    ("S9 write cleaned", {}, ["python"]),
    ("S10 overwrite", {}, ["python"]),
]


def main() -> int:
    rows: List[str] = []
    failures = 0
    for name, rules, engines in SCENARIOS:
        overwrite = name.startswith("S10")
        out_base = "__overwrite__" if overwrite else "cleaned/"
        assess, plan, norm_rules = build(rules, output_base=out_base, overwrite=overwrite)
        plan_ok, plan_errs = validate_etl_plan(plan, assess, norm_rules)
        rows.append(f"{name:22} plan={'OK' if plan_ok else 'FAIL'} {plan_errs[:2]}")
        if not plan_ok:
            failures += 1
        for eng in engines:
            code, ok, errs = _template_fallback(eng, plan, assess, sql_dialect="tsql")
            if eng == "python" and not ok:
                ok, errs = validate_etl_python_source(code)
            elif eng == "pyspark":
                ok, errs = validate_pyspark_source(code, plan)
            elif eng == "sql":
                ok, errs = validate_sql_basic(code)
            status = "OK" if ok else "FAIL"
            if not ok:
                failures += 1
            err_short = "; ".join(errs[:2]) if errs else ""
            rows.append(f"  {eng:8} codegen+validate={status}  {err_short[:80]}")
    print("ETL generate + validate sweep (TEMPLATE codegen, not live LLM)\n")
    print("\n".join(rows))
    print(f"\nTotal failures: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
