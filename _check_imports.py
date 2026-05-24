"""Check that all example scripts can import their dependencies."""
import ast
import importlib
import sys
import os

EXAMPLES = [
    "examples/basic/installation_example.py",
    "examples/basic/functional_testing.py",
    "examples/advanced/walkthrough.py",
    "examples/advanced/sql_examples.py",
    "examples/advanced/dataframe_operations.py",
    "examples/advanced/batch.py",
    "examples/advanced/relationships.py",
    "examples/advanced/fetchxml.py",
    "examples/advanced/alternate_keys_upsert.py",
    "examples/advanced/file_upload.py",
    "examples/advanced/prodev_quick_start.py",
    "examples/advanced/datascience_risk_assessment.py",
]

failures = []
for path in EXAMPLES:
    name = os.path.basename(path)
    try:
        with open(path) as f:
            tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                importlib.import_module(node.module)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    importlib.import_module(alias.name)
        print(f"[OK] {name}")
    except Exception as e:
        print(f"[ERR] {name}: {e}")
        failures.append(name)

print()
if failures:
    print(f"FAILURES: {failures}")
    sys.exit(1)
else:
    print("All 12 examples import successfully.")
