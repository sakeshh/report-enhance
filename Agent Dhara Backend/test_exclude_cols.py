#!/usr/bin/env python
"""Test exclude_columns implementation."""

from agent.etl_pipeline.python_codegen import generate_python_etl

# Test plan with exclude_columns rule
test_plan = {
    'plan_id': 'test_123',
    'business_rules': {
        'notes': 'Test ETL with exclude',
        'exclude_columns': ['OrderAmount'],
        'required_columns': ['CustomerID']
    },
    'datasets': {
        'dbo.Orders_Raw': {
            'steps': [
                {'order': 1, 'action': 'clip_or_flag', 'column': 'CustomerID'},
            ]
        }
    },
    'manual_review': [
        {'dataset': 'dbo.Orders_Raw', 'column': 'CustomerID', 'issue_type': 'integer_stored_as_float', 'message': 'Should be Int64'}
    ]
}

code = generate_python_etl(test_plan, {})
print('=== GENERATED CODE ===')
print(code)
print()
print('✓ Code generation with exclude_columns successful!')
