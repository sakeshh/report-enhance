"""
Engine-specific join / load codegen from plan.relationships + connector_manifest.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List


def _safe(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x")
    return (s or "ds").strip("_")


def emit_python_load_and_join(
    plan: Dict[str, Any],
    manifest: Dict[str, Any],
) -> List[str]:
    lines: List[str] = []
    m_ds = (manifest or {}).get("datasets") or {}
    ds_plan = plan.get("datasets") or {}
    rel = plan.get("relationships") or {}
    load_order = rel.get("load_order") or list(ds_plan.keys())
    mn = rel.get("many_to_many") or []

    lines.append("# ── Connector manifest: load all datasets ──")
    lines.append("def load_all_datasets() -> dict:")
    lines.append('    """Load raw data using connector_manifest (set env vars for SQL/blob)."""')
    lines.append("    import os")
    lines.append("    dfs = {}")
    for ds_name in load_order:
        ent = m_ds.get(ds_name) or {}
        snip = ent.get("read_snippet_python") or f'pd.read_csv(r"{ds_name}")'
        lines.append(f"    # {ds_name}: {ent.get('source_type', 'file')} @ {ent.get('location', ds_name)}")
        if ent.get("format") == "sql_table":
            lines.append("    from sqlalchemy import create_engine")
        lines.append(f"    dfs[{repr(ds_name)}] = {snip}")
    lines.append("    return dfs")
    lines.append("")

    lines.append("def transform_all(dfs: dict) -> dict:")
    lines.append("    out = {}")
    for ds_name in ds_plan:
        fn = f"transform_{_safe(ds_name)}"
        lines.append(f"    if {repr(ds_name)} in dfs:")
        lines.append(f"        out[{repr(ds_name)}] = {fn}(dfs[{repr(ds_name)}])")
    lines.append("    return out")
    lines.append("")

    joins = [j for j in (rel.get("joins") or []) if j.get("join_type") != "review"]
    if joins:
        lines.append("def run_joins(dfs: dict) -> dict:")
        for j in joins:
            p, c = j.get("parent_dataset"), j.get("child_dataset")
            pk, ck = j.get("parent_key"), j.get("child_key")
            how = j.get("join_type") or "left"
            lines.append(f"    if {repr(p)} in dfs and {repr(c)} in dfs:")
            lines.append(
                f"        dfs[{repr(c)}] = dfs[{repr(p)}].merge("
                f"dfs[{repr(c)}], left_on={repr(pk)}, right_on={repr(ck)}, "
                f"how={repr(how)}, suffixes=('_parent', '_child'))"
            )
        lines.append("    return dfs")
        lines.append("")

    if mn:
        lines.append("def build_bridge_tables(dfs: dict) -> dict:")
        for b in mn:
            a, bds = b.get("dataset_a"), b.get("dataset_b")
            ka, kb = b.get("column_a"), b.get("column_b")
            bname = b.get("bridge_name") or f"bridge_{_safe(a)}_{_safe(bds)}"
            lines.append(f"    # M:N bridge: {a}.{ka} <-> {bds}.{kb}")
            lines.append(f"    if {repr(a)} in dfs and {repr(bds)} in dfs:")
            lines.append(
                f"        dfs[{repr(bname)}] = dfs[{repr(a)}][[{repr(ka)}]].drop_duplicates()"
                f".merge(dfs[{repr(bds)}][[{repr(kb)}]].drop_duplicates(), "
                f"left_on={repr(ka)}, right_on={repr(kb)}, how='inner')"
            )
        lines.append("    return dfs")
        lines.append("")

    lines.append("def write_outputs(dfs: dict) -> None:")
    lines.append("    import os")
    for ds_name, ent in m_ds.items():
        op = ent.get("output_path") or f"cleaned/{_safe(ds_name)}.parquet"
        wsnip = ent.get("write_snippet_python") or f"dfs[{repr(ds_name)}].to_parquet(r'{op}', index=False)"
        lines.append(f"    if {repr(ds_name)} in dfs:")
        lines.append(f"        os.makedirs(os.path.dirname(r'{op}') or '.', exist_ok=True)")
        lines.append(f"        df = dfs[{repr(ds_name)}]")
        lines.append(f"        {wsnip}")
    lines.append("")

    lines.append("if __name__ == '__main__':")
    lines.append("    _raw = load_all_datasets()")
    lines.append("    _clean = transform_all(_raw)")
    if joins:
        lines.append("    _clean = run_joins(_clean)")
    if mn:
        lines.append("    _clean = build_bridge_tables(_clean)")
    lines.append("    write_outputs(_clean)")
    lines.append("    print('ETL pipeline complete:', list(_clean.keys()))")
    lines.append("")
    return lines


def emit_sql_joins(plan: Dict[str, Any], manifest: Dict[str, Any], *, dialect: str = "tsql") -> List[str]:
    lines: List[str] = []
    rel = plan.get("relationships") or {}
    m_ds = (manifest or {}).get("datasets") or {}
    q = lambda s: f"[{s}]" if dialect == "tsql" else f'"{s}"'

    lines.append("-- ── Staging / load order (connector manifest) ──")
    for ds_name in rel.get("load_order") or list((plan.get("datasets") or {}).keys()):
        ent = m_ds.get(ds_name) or {}
        lines.append(f"-- {ds_name}: {ent.get('read_snippet_sql', '-- file staging required')}")
    lines.append("")

    for j in rel.get("joins") or []:
        if j.get("join_type") == "review":
            continue
        p, c = j.get("parent_dataset"), j.get("child_dataset")
        pk, ck = j.get("parent_key"), j.get("child_key")
        how = (j.get("join_type") or "LEFT").upper()
        if dialect != "tsql":
            how = how if how in ("INNER", "LEFT") else "LEFT"
        lines.append(f"-- Join {p} -> {c} ({j.get('cardinality')})")
        lines.append(f"-- CREATE VIEW vw_{_safe(c)}_enriched AS")
        lines.append(f"SELECT c.*, p.*")
        lines.append(f"FROM {q('stg_' + _safe(c))} c")
        lines.append(f"{how} JOIN {q('stg_' + _safe(p))} p ON c.{q(ck)} = p.{q(pk)};")
        lines.append("")

    for b in rel.get("many_to_many") or []:
        lines.append(f"-- M:N bridge {b.get('bridge_name')}: {b.get('dataset_a')}.{b.get('column_a')}")
        lines.append(f"-- CREATE TABLE {b.get('bridge_name')} AS SELECT DISTINCT ...")
        lines.append("")
    return lines


_PER_DATASET_ONLY_ACTIONS = frozenset(
    {
        "lowercase",
        "uppercase",
        "trim",
        "sanitize_email",
        "normalize_phone",
        "hash_phone",
        "mask_phone",
    }
)


def _skip_joins_for_plan(plan: Dict[str, Any]) -> bool:
    """Skip auto-joins when the plan only normalizes columns per dataset (e.g. case-only scenarios)."""
    steps: List[Dict[str, Any]] = []
    for block in (plan.get("datasets") or {}).values():
        steps.extend((block or {}).get("steps") or [])
    if not steps:
        return False
    for st in steps:
        if str(st.get("action") or "") not in _PER_DATASET_ONLY_ACTIONS:
            return False
    return True


def emit_pyspark_output_contract(plan: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    """Comment block + OUTPUT_PATHS dict for clear production contracts."""
    lines: List[str] = ["# ── OUTPUT CONTRACT ──"]
    m_ds = (manifest or {}).get("datasets") or {}
    rel = plan.get("relationships") or {}
    for ds_name, ent in m_ds.items():
        op = ent.get("output_path") or f"cleaned/{_safe(ds_name)}.parquet"
        fmt = ent.get("format") or "?"
        lines.append(f"#   {ds_name}: {fmt} -> {op}")
    joins = [j for j in (rel.get("joins") or []) if j.get("join_type") != "review"]
    if joins and not _skip_joins_for_plan(plan):
        for j in joins:
            p, c = j.get("parent_dataset"), j.get("child_dataset")
            jname = f"joined_{_safe(p)}_{_safe(c)}"
            lines.append(f"#   {jname}: parquet -> cleaned/{jname}.parquet")
    lines.append("OUTPUT_PATHS: dict = {}")
    lines.append("")
    return lines


def emit_pyspark_write_outputs(plan: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    m_ds = (manifest or {}).get("datasets") or {}
    rel = plan.get("relationships") or {}
    lines.append("# Write cleaned per-dataset outputs (format matches manifest; XML -> parquet)")
    for ds_name, ent in m_ds.items():
        op = ent.get("output_path") or f"cleaned/{_safe(ds_name)}.parquet"
        wsnip = ent.get("write_snippet_pyspark") or (
            f'df.write.mode("overwrite").parquet(r"{op}")'
        )
        if wsnip.startswith("df."):
            wsnip = f'dfs["{ds_name}"].' + wsnip[3:]
        lines.append(f'if "{ds_name}" in dfs:')
        lines.append(f'    _out = _resolve_data_path("{op}")')
        lines.append(f'    OUTPUT_PATHS[{ds_name!r}] = _out')
        lines.append(f"    logging.getLogger('agent_dhara').info('Writing %s -> %s', {ds_name!r}, _out)")
        if ".json(" in wsnip:
            lines.append(f'    dfs["{ds_name}"].write.mode("overwrite").json(_out)')
        elif ".parquet(" in wsnip:
            lines.append(f'    dfs["{ds_name}"].write.mode("overwrite").parquet(_out)')
        elif ".csv(" in wsnip:
            lines.append(
                f'    dfs["{ds_name}"].write.mode("overwrite").option("header", "true").csv(_out)'
            )
        else:
            lines.append(f"    {wsnip}")

    joins = [j for j in (rel.get("joins") or []) if j.get("join_type") != "review"]
    if joins and not _skip_joins_for_plan(plan):
        lines.append("")
        lines.append("# Write joined enrichment outputs")
        for j in joins:
            p, c = j.get("parent_dataset"), j.get("child_dataset")
            jname = f"joined_{_safe(p)}_{_safe(c)}"
            op = f"cleaned/{jname}.parquet"
            lines.append(f'if {jname!r} in dfs:')
            lines.append(f'    _out = _resolve_data_path("{op}")')
            lines.append(f"    OUTPUT_PATHS[{jname!r}] = _out")
            lines.append(
                f"    logging.getLogger('agent_dhara').info('Writing %s -> %s', {jname!r}, _out)"
            )
            lines.append(f'    dfs[{jname!r}].write.mode("overwrite").parquet(_out)')
    lines.append("")
    return lines


def emit_pyspark_load(plan: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    """Load raw datasets only (transform and join happen later in run_pipeline)."""
    lines: List[str] = []
    m_ds = (manifest or {}).get("datasets") or {}
    ds_plan = plan.get("datasets") or {}
    rel = plan.get("relationships") or {}
    load_order = rel.get("load_order") or list(ds_plan.keys())
    rules = plan.get("business_rules") or {}
    required = [str(c) for c in (rules.get("required_columns") or []) if c]

    lines.append("# Load (connector manifest — use _resolve_data_path for blob/SQL)")
    for ds_name in load_order:
        ent = m_ds.get(ds_name) or {}
        snip = ent.get("read_snippet_pyspark") or f'spark.read.json(_resolve_data_path("{ds_name}"))'
        fmt = ent.get("format", "?")
        lines.append(f'# {ds_name}: {ent.get("source_type")} ({fmt}) @ {ent.get("location")}')
        lines.append(f'dfs["{ds_name}"] = {snip}')
        if required:
            cols = [c for c in required if c]
            lines.append(f'    _require_columns(dfs["{ds_name}"], {cols!r}, "{ds_name}")')
    lines.append("")
    return lines


def emit_pyspark_joins(plan: Dict[str, Any]) -> List[str]:
    """Join cleaned datasets; store under joined_* keys and write in emit_pyspark_write_outputs."""
    lines: List[str] = []
    rel = plan.get("relationships") or {}
    joins = [j for j in (rel.get("joins") or []) if j.get("join_type") != "review"]
    if not joins or _skip_joins_for_plan(plan):
        if joins and _skip_joins_for_plan(plan):
            lines.append(
                "# Joins skipped: per-dataset normalization only (no cross-dataset enrichment)."
            )
            lines.append("")
        return lines

    lines.append("# Joins (after per-dataset transforms; prefix right columns; write joined_* output)")
    for j in joins:
        p, c = j.get("parent_dataset"), j.get("child_dataset")
        pk, ck = j.get("parent_key"), j.get("child_key")
        how = j.get("join_type") or "left"
        sfx = _safe(c)
        jname = f"joined_{_safe(p)}_{_safe(c)}"
        lines.append(f'if "{p}" in dfs and "{c}" in dfs:')
        lines.append(f'    _warn_duplicate_keys(dfs["{p}"], {pk!r}, "{p}")')
        lines.append(f'    _warn_duplicate_keys(dfs["{c}"], {ck!r}, "{c}")')
        lines.append(f'    _right = _prefix_columns(dfs["{c}"], "{sfx}", [{ck!r}])')
        if pk == ck:
            lines.append(f'    dfs[{jname!r}] = dfs["{p}"].join(_right, on={pk!r}, how="{how}")')
        else:
            lines.append(
                f'    dfs[{jname!r}] = dfs["{p}"].join(_right, '
                f'F.col("{pk}") == F.col("{ck}"), how="{how}")'
            )
    lines.append("")
    return lines


def emit_pyspark_load_and_join(plan: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    """Backward-compatible: load only (joins emitted separately)."""
    return emit_pyspark_load(plan, manifest)


def emit_adf_join_transformations(
    transformations: List[Dict[str, Any]],
    tid: int,
    rel: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], int, List[str]]:
    """Append Join transforms to ADF mapping flow."""
    script: List[str] = []
    for j in rel.get("joins") or []:
        if j.get("join_type") == "review":
            continue
        tid += 1
        tname = f"join_{tid}"
        p, c = j.get("parent_dataset"), j.get("child_dataset")
        transformations.append(
            {
                "name": tname,
                "description": f"Join {p} to {c} on {j.get('parent_key')}={j.get('child_key')}",
                "type": "join",
                "joinType": j.get("join_type") or "left",
                "leftStream": f"source_{_safe(p)}",
                "rightStream": f"source_{_safe(c)}",
                "leftKey": j.get("parent_key"),
                "rightKey": j.get("child_key"),
                "column": None,
                "action": "join_datasets",
                "upstream": [f"source_{_safe(p)}", f"source_{_safe(c)}"],
            }
        )
        script.append(f"// {tname}: {p} JOIN {c}")
    return transformations, tid, script
