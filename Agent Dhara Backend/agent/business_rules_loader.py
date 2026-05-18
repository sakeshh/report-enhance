"""
Load config/business_rules.yaml and merge with per-run UI or chat overrides.
Supports multi-tenant rule sets via tenants.<id>.defaults / tenants.<id>.datasets.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.business_rules import normalize_business_rules

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


def _config_path() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "config", "business_rules.yaml")


def load_yaml_business_rules() -> Dict[str, Any]:
    path = _config_path()
    if not yaml or not os.path.isfile(path):
        return {"defaults": {}, "datasets": {}, "tenants": {}}
    try:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {"defaults": {}, "datasets": {}, "tenants": {}}
        return {
            "defaults": raw.get("defaults") if isinstance(raw.get("defaults"), dict) else {},
            "datasets": raw.get("datasets") if isinstance(raw.get("datasets"), dict) else {},
            "tenants": raw.get("tenants") if isinstance(raw.get("tenants"), dict) else {},
        }
    except Exception:
        return {"defaults": {}, "datasets": {}, "tenants": {}}


def list_tenant_ids() -> List[str]:
    cfg = load_yaml_business_rules()
    tenants = cfg.get("tenants") or {}
    if isinstance(tenants, dict) and tenants:
        return sorted(str(k) for k in tenants.keys())
    return ["default"]


def _tenant_block(cfg: Dict[str, Any], tenant_id: Optional[str]) -> Dict[str, Any]:
    tid = (tenant_id or "default").strip() or "default"
    tenants = cfg.get("tenants") or {}
    if isinstance(tenants, dict) and tid in tenants and isinstance(tenants[tid], dict):
        return tenants[tid]
    if tid == "default":
        return {
            "defaults": cfg.get("defaults") or {},
            "datasets": cfg.get("datasets") or {},
        }
    return {"defaults": {}, "datasets": {}}


def _merge_lists(a: List[str], b: List[str]) -> List[str]:
    seen: Dict[str, str] = {}
    for x in a + b:
        s = str(x).strip()
        if s:
            seen[s.lower()] = s
    return sorted(seen.values(), key=str.lower)


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if k in ("required_columns", "non_nullable", "exclude_columns") and isinstance(v, list):
            out[k] = _merge_lists(list(out.get(k) or []), v)
        elif k == "valid_values" and isinstance(v, dict):
            vv = dict(out.get("valid_values") or {})
            for ck, vals in v.items():
                if isinstance(vals, list):
                    vv[str(ck)] = _merge_lists(list(vv.get(ck) or []), vals)
                else:
                    vv[str(ck)] = [str(vals)]
            out["valid_values"] = vv
        elif k == "notes" and v:
            prev = str(out.get("notes") or "").strip()
            add = str(v).strip()
            out["notes"] = "\n".join(x for x in (prev, add) if x)
        elif v is not None:
            out[k] = v
    return out


def merge_business_rules_for_datasets(
    ui_or_chat_rules: Any,
    dataset_names: List[str],
    *,
    tenant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Merge tenant YAML + per-dataset YAML + UI/chat payload into one rules dict for the planner.
    """
    cfg = load_yaml_business_rules()
    tblock = _tenant_block(cfg, tenant_id)
    defaults = normalize_business_rules(tblock.get("defaults") or {})
    ui_norm = normalize_business_rules(ui_or_chat_rules or {})
    merged = _merge_dicts(defaults, ui_norm)
    merged["tenant_id"] = (tenant_id or "default").strip() or "default"

    ds_rules = tblock.get("datasets") or {}
    for ds in dataset_names:
        block = ds_rules.get(ds)
        if isinstance(block, dict):
            merged = _merge_dicts(merged, normalize_business_rules(block))

    return merged


def pending_rules_from_session(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pending = ctx.get("pending_business_rules")
    return pending if isinstance(pending, dict) else None


def tenant_id_from_session(ctx: Dict[str, Any]) -> str:
    tid = ctx.get("etl_tenant_id") or ctx.get("tenant_id")
    return (str(tid).strip() if tid else "") or "default"
