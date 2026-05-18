#!/usr/bin/env python
"""Test the ETL code generation fix for clip_or_flag and other actions."""

import pandas as pd

def transform_dbo_Orders_Raw(df):
    """Clean transforms for dataset: dbo.Orders_Raw"""
    out = df.copy()

    _q1 = out['CustomerID'].quantile(0.25)
    _q3 = out['CustomerID'].quantile(0.75)
    _iqr = _q3 - _q1
    _lower = _q1 - 1.5 * _iqr
    _upper = _q3 + 1.5 * _iqr
    out['CustomerID_outlier_flagged'] = (((out['CustomerID'] < _lower) | (out['CustomerID'] > _upper)) & out['CustomerID'].notna()).astype(bool)
    out['OrderAmount'] = pd.to_numeric(out['OrderAmount'], errors='coerce')
    return out

# Test with sample data
df = pd.DataFrame({
    'CustomerID': [1, 2, 50, 100, -10],
    'OrderAmount': [100.5, 200.5, 300.5, 400.5, 1500.0]
})

print("Original DataFrame:")
print(df)
print("\nTransformed DataFrame:")
result = transform_dbo_Orders_Raw(df)
print(result)
print("\nColumn dtypes:")
print(result.dtypes)
print("\nOK: Generated code runs successfully!")
print(f"OK: Outlier flag column created: {result['CustomerID_outlier_flagged'].sum()} outliers flagged")
