"""
LLM-based ETL Code Generator.
Translates an ETL Plan + Natural Language Business Notes into production-ready code.
"""
from __future__ import annotations
import json
import os
import re
from typing import Any, Dict, List, Optional

try:
    from openai import AzureOpenAI, OpenAI
except ImportError:
    AzureOpenAI = None
    OpenAI = None

SYSTEM_PROMPTS: Dict[str, str] = {
    "python": """
You are a Senior Data Engineer.
Generate a complete, production-grade Python Pandas ETL script based on the provided Plan JSON.

CRITICAL REQUIREMENTS:
1. IMPLEMENT BUSINESS NOTES: If the user provides "business_notes" or "notes", you MUST translate them into Python code.
   - Example: "convert X to date" -> df['X'] = pd.to_datetime(df['X'], errors='coerce')
2. PRESERVE COLUMN NAMES: Use the exact casing for columns provided in the plan.
3. TYPE SAFETY: Use 'Int64' for integer columns to support nullable integers.
4. STRUCTURE:
   - Module docstring with summary.
   - Imports (pandas, logging).
   - A function for each dataset (e.g., transform_dataset_name).
   - Error handling for missing required columns.
   - if __name__ == '__main__': entry point with example usage.

Output ONLY valid Python code. No markdown fences.
""",
    "sql": """
You are a Senior Data Engineer.
Generate a production-grade T-SQL script based on the provided Plan JSON.

CRITICAL REQUIREMENTS:
1. IMPLEMENT BUSINESS NOTES: Translate any natural language notes into SQL statements.
2. TRANSACTION SAFETY: Wrap each dataset transform in BEGIN TRY / BEGIN CATCH with ROLLBACK.
3. PERFORMANCE: Use variable-based IQR for outliers (DECLARE @q1, @q3) instead of cross joins.
4. SAFE UPDATES: Use TRY_CAST for type conversions.

Output ONLY valid SQL code. No markdown fences.
"""
}

def _get_llm_client():
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_key      = os.getenv("AZURE_OPENAI_API_KEY")
    if azure_endpoint and azure_key and AzureOpenAI:
        return AzureOpenAI(
            azure_endpoint = azure_endpoint,
            api_key        = azure_key,
            api_version    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key and OpenAI:
        return OpenAI(api_key=openai_key)
    return None

def generate_etl_with_llm(
    plan: Dict[str, Any], 
    assessment: Dict[str, Any], 
    engine: str = "python"
) -> str:
    client = _get_llm_client()
    if not client:
        return "# Error: No LLM credentials (AZURE_OPENAI_API_KEY or OPENAI_API_KEY) found."

    engine = engine.lower()
    if "sql" in engine: engine = "sql"
    elif "spark" in engine: engine = "python" # We'll handle spark as python for now or add prompt
    
    prompt = SYSTEM_PROMPTS.get(engine, SYSTEM_PROMPTS["python"])
    
    # Enrich payload with everything the LLM needs to see
    payload = {
        "plan_id": plan.get("plan_id"),
        "business_rules": plan.get("business_rules"),
        "datasets": plan.get("datasets"),
        "global_steps": plan.get("global_steps"),
        "manual_review": plan.get("manual_review"),
        "source_metadata": {
            ds_name: {
                "row_count": meta.get("row_count"),
                "columns": list((meta.get("columns") or {}).keys())
            }
            for ds_name, meta in (assessment.get("datasets") or {}).items()
        }
    }

    try:
        response = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("OPENAI_MODEL", "gpt-4o"),
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Generate ETL code for the following plan:\n{json.dumps(payload, indent=2)}"}
            ],
            temperature=0.1,
            max_tokens=3000
        )
        code = response.choices[0].message.content or ""
        
        # Clean fences
        code = re.sub(r'^```[a-zA-Z]*\n?', '', code)
        code = re.sub(r'\n?```$', '', code)
        return code.strip()
    except Exception as e:
        return f"# Error generating code with LLM: {str(e)}"
