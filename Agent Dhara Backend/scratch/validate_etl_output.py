"""
Validate the generated ETL code by simulating the exact output Agent Dhara produces.
This script creates mock data matching the assessment profile and runs the transforms.
"""
import pandas as pd
import numpy as np
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ─── Simulate dbo.Orders_Raw (50 rows, with outliers matching assessment) ───
np.random.seed(42)
orders_data = {
    'OrderID': list(range(1, 51)),
    'CustomerID': list(range(1, 47)) + [200, 300, 400, 500],  # 4 outliers
    'OrderDate': pd.date_range('2024-01-01', periods=50, freq='D'),
    'OrderAmount': [round(np.random.uniform(100, 1500), 2) for _ in range(48)] + [5000.0, 8000.0],  # 2 outliers
    'Status': ['Completed'] * 30 + ['Pending'] * 15 + ['Cancelled'] * 5,
}
df_orders = pd.DataFrame(orders_data)
# Make CustomerID float (to simulate "integer_stored_as_float")
df_orders['CustomerID'] = df_orders['CustomerID'].astype(float)

# ─── Simulate dbo.Sales_Raw (50 rows, with outliers matching assessment) ───
sales_data = {
    'SaleID': list(range(1, 51)),
    'OrderID': list(range(1, 50)) + [500],  # 1 outlier
    'ProductName': [f'Product_{i}' for i in range(1, 51)],
    'Quantity': np.random.randint(1, 20, 50).tolist(),
    'UnitPrice': [round(np.random.uniform(10, 200), 2) for _ in range(50)],
    'TotalAmount': [round(np.random.uniform(50, 3000), 2) for _ in range(49)] + [50000.0],  # 1 outlier
}
df_sales = pd.DataFrame(sales_data)
# Make OrderID float (to simulate "integer_stored_as_float")
df_sales['OrderID'] = df_sales['OrderID'].astype(float)

print("=" * 70)
print("VALIDATION: Agent Dhara Generated ETL Code")
print("=" * 70)

# ─── Test 1: transform_dbo_Orders_Raw ───
print("\n--- Test 1: transform_dbo_Orders_Raw ---")
print(f"Input shape: {df_orders.shape}")
print(f"Input CustomerID dtype: {df_orders['CustomerID'].dtype}")

try:
    # === PASTE OF GENERATED CODE (transform_dbo_Orders_Raw) ===
    def transform_dbo_Orders_Raw(df):
        out = df.copy()
        logging.info(f'Applying cast_type to CustomerID in dbo.Orders_Raw')
        out['CustomerID'] = out['CustomerID'].astype('Int64')
        _q1 = out['CustomerID'].quantile(0.25)
        _q3 = out['CustomerID'].quantile(0.75)
        _iqr = _q3 - _q1
        _lower = _q1 - 1.5 * _iqr
        _upper = _q3 + 1.5 * _iqr
        out['CustomerID_outlier_flagged'] = (((out['CustomerID'] < _lower) | (out['CustomerID'] > _upper)) & out['CustomerID'].notna()).astype(bool)
        _q1 = out['OrderAmount'].quantile(0.25)
        _q3 = out['OrderAmount'].quantile(0.75)
        _iqr = _q3 - _q1
        _lower = _q1 - 1.5 * _iqr
        _upper = _q3 + 1.5 * _iqr
        out['OrderAmount_outlier_flagged'] = (((out['OrderAmount'] < _lower) | (out['OrderAmount'] > _upper)) & out['OrderAmount'].notna()).astype(bool)
        return out

    result_orders = transform_dbo_Orders_Raw(df_orders)
    
    print(f"Output shape: {result_orders.shape}")
    print(f"Output CustomerID dtype: {result_orders['CustomerID'].dtype}")
    print(f"New columns added: {[c for c in result_orders.columns if c not in df_orders.columns]}")
    print(f"CustomerID outliers flagged: {result_orders['CustomerID_outlier_flagged'].sum()}")
    print(f"OrderAmount outliers flagged: {result_orders['OrderAmount_outlier_flagged'].sum()}")
    print(f"Row count preserved: {len(result_orders) == len(df_orders)} ({len(result_orders)} == {len(df_orders)})")
    print(f"Original df mutated: {df_orders['CustomerID'].dtype}")  # Should still be float64
    print("[PASS] transform_dbo_Orders_Raw PASSED")
except Exception as e:
    print(f"[FAIL] transform_dbo_Orders_Raw FAILED: {e}")
    import traceback; traceback.print_exc()

# ─── Test 2: transform_dbo_Sales_Raw ───
print("\n--- Test 2: transform_dbo_Sales_Raw ---")
print(f"Input shape: {df_sales.shape}")
print(f"Input OrderID dtype: {df_sales['OrderID'].dtype}")

try:
    def transform_dbo_Sales_Raw(df):
        out = df.copy()
        logging.info(f'Applying cast_type to OrderID in dbo.Sales_Raw')
        out['OrderID'] = out['OrderID'].astype('Int64')
        _q1 = out['OrderID'].quantile(0.25)
        _q3 = out['OrderID'].quantile(0.75)
        _iqr = _q3 - _q1
        _lower = _q1 - 1.5 * _iqr
        _upper = _q3 + 1.5 * _iqr
        out['OrderID_outlier_flagged'] = (((out['OrderID'] < _lower) | (out['OrderID'] > _upper)) & out['OrderID'].notna()).astype(bool)
        _q1 = out['TotalAmount'].quantile(0.25)
        _q3 = out['TotalAmount'].quantile(0.75)
        _iqr = _q3 - _q1
        _lower = _q1 - 1.5 * _iqr
        _upper = _q3 + 1.5 * _iqr
        out['TotalAmount_outlier_flagged'] = (((out['TotalAmount'] < _lower) | (out['TotalAmount'] > _upper)) & out['TotalAmount'].notna()).astype(bool)
        return out

    result_sales = transform_dbo_Sales_Raw(df_sales)
    
    print(f"Output shape: {result_sales.shape}")
    print(f"Output OrderID dtype: {result_sales['OrderID'].dtype}")
    print(f"New columns added: {[c for c in result_sales.columns if c not in df_sales.columns]}")
    print(f"OrderID outliers flagged: {result_sales['OrderID_outlier_flagged'].sum()}")
    print(f"TotalAmount outliers flagged: {result_sales['TotalAmount_outlier_flagged'].sum()}")
    print(f"Row count preserved: {len(result_sales) == len(df_sales)} ({len(result_sales)} == {len(df_sales)})")
    print(f"Original df mutated: {df_sales['OrderID'].dtype}")  # Should still be float64
    print("[PASS] transform_dbo_Sales_Raw PASSED")
except Exception as e:
    print(f"[FAIL] transform_dbo_Sales_Raw FAILED: {e}")
    import traceback; traceback.print_exc()

# ─── Test 3: Edge cases ───
print("\n--- Test 3: Edge cases (NaN handling) ---")
try:
    df_with_nulls = df_orders.copy()
    df_with_nulls.loc[0, 'CustomerID'] = np.nan
    df_with_nulls.loc[1, 'OrderAmount'] = np.nan
    result_nulls = transform_dbo_Orders_Raw(df_with_nulls)
    print(f"NaN in CustomerID preserved after Int64 cast: {pd.isna(result_nulls.loc[0, 'CustomerID'])}")
    print(f"NaN row NOT flagged as outlier: {not result_nulls.loc[0, 'CustomerID_outlier_flagged']}")
    print(f"NaN in OrderAmount NOT flagged: {not result_nulls.loc[1, 'OrderAmount_outlier_flagged']}")
    print("[PASS] NaN handling PASSED")
except Exception as e:
    print(f"❌ NaN handling FAILED: {e}")
    import traceback; traceback.print_exc()

print("\n" + "=" * 70)
print("VALIDATION COMPLETE")
print("=" * 70)
