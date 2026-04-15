# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Live Dataverse performance benchmark for picklist label resolution.

Creates a temporary table with a configurable number of picklist columns,
then measures wall-clock time and API call count for label resolution
at various scale points.

Run from repo root:
    $env:PYTHONPATH="src"; .conda/python.exe examples/advanced/perf_benchmark_live.py

Prerequisites:
    - pip install PowerPlatform-Dataverse-Client azure-identity
    - DATAVERSE_URL environment variable set
    - Interactive browser auth (or configure credentials below)

WARNING: This script creates and deletes a temporary table on your
Dataverse environment. It cleans up after itself, but if interrupted
mid-run, you may need to manually delete the table (new_perfbench_*).
"""

import os
import sys
import time
import uuid
from enum import IntEnum

sys.path.insert(0, "src")

from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Scale points: number of picklist columns to create on the test table.
# Dataverse has a max attribute limit based on the SQL 8060-byte row size limit
# (each choice column costs 4 bytes). The practical ceiling varies by table.
# 500 and 1000 are included to probe beyond the observed ~400 limit.
SCALE_POINTS = [1, 10, 100, 250, 400, 450, 480]
OPTIONS_PER_PICKLIST = 4
EXTRA_STRING_FIELDS = 3  # non-picklist string fields added at each scale
REPEAT_CALLS = 3  # warm cache repeat calls


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_picklist_enum(index: int, num_options: int) -> type:
    """Dynamically create an IntEnum for a picklist column."""
    members = {f"Option_{j}": 100000000 + j for j in range(num_options)}
    return IntEnum(f"Picklist{index}", members)


def backoff(op, *, delays=(0, 3, 10, 20)):
    """Retry an operation with exponential backoff."""
    last_err = None
    for delay in delays:
        if delay:
            time.sleep(delay)
        try:
            return op()
        except Exception as e:
            last_err = e
            print(f"  [WARN] Retry after {delay}s: {e}")
    raise last_err


def create_test_table(client, table_name: str, num_picklists: int) -> dict:
    """Create a test table with picklist + string columns."""
    schema = {}

    # Add picklist columns
    for i in range(num_picklists):
        col_name = f"new_Picklist{i}"
        enum_cls = make_picklist_enum(i, OPTIONS_PER_PICKLIST)
        schema[col_name] = enum_cls

    # Add plain string columns
    for i in range(EXTRA_STRING_FIELDS):
        schema[f"new_TextField{i}"] = "string"

    print(f"  [INFO] Creating table {table_name} with {num_picklists} picklists + {EXTRA_STRING_FIELDS} text fields...")
    info = client.tables.create(table_name, schema)
    print(f"  [OK] Table created: {info['entity_set_name']}")
    return info


def build_test_record(num_picklists: int) -> dict:
    """Build a record with label strings for all picklists + plain text."""
    record = {"new_name": f"perf-test-{uuid.uuid4().hex[:8]}"}
    for i in range(num_picklists):
        # Use the first option label
        record[f"new_picklist{i}"] = "Option_0"
    for i in range(EXTRA_STRING_FIELDS):
        record[f"new_textfield{i}"] = f"plain text value {i}"
    return record


def count_api_calls(client, table_name: str, record: dict) -> tuple:
    """Measure a single _convert_labels_to_ints call.

    Returns (elapsed_ms, api_call_count).
    We count calls by patching _request temporarily.
    """
    odata = client._odata
    original_request = odata._request
    call_count = 0

    def counting_request(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return original_request(*args, **kwargs)

    odata._request = counting_request
    try:
        t0 = time.perf_counter()
        odata._convert_labels_to_ints(table_name, record)
        elapsed = time.perf_counter() - t0
    finally:
        odata._request = original_request

    return round(elapsed * 1000, 1), call_count


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATAVERSE_URL")
    if not url:
        url = input("Enter Dataverse URL (e.g. https://org.crm.dynamics.com): ").strip()
    if not url:
        print("[ERR] Dataverse URL required (pass as argument, set DATAVERSE_URL, or enter at prompt).")
        sys.exit(1)

    print("=" * 78)
    print("Picklist Label Resolution - Live Dataverse Benchmark")
    print(f"Environment: {url}")
    print(f"Scale points (picklist columns): {SCALE_POINTS}")
    print(f"Options per picklist: {OPTIONS_PER_PICKLIST}")
    print(f"Extra string fields: {EXTRA_STRING_FIELDS}")
    print(f"Warm cache repeat calls: {REPEAT_CALLS}")
    print("=" * 78)

    cred = InteractiveBrowserCredential()
    client = DataverseClient(base_url=url, credential=cred)

    results = []
    run_id = uuid.uuid4().hex[:6]

    for scale_idx, num_picklists in enumerate(SCALE_POINTS):
        table_name = f"new_PerfBench{run_id}_{num_picklists}p"
        total_fields = num_picklists + EXTRA_STRING_FIELDS
        print(f"\n--- Scale point {scale_idx + 1}/{len(SCALE_POINTS)}: "
              f"{num_picklists} picklists, {total_fields} total fields ---")

        try:
            # Create table
            info = backoff(lambda: create_test_table(client, table_name, num_picklists))

            # Wait for columns to be visible
            print(f"  [INFO] Waiting for columns to become visible...")
            time.sleep(5)

            # Build test record
            record = build_test_record(num_picklists)

            # Flush cache to ensure cold start
            client._odata._picklist_label_cache.clear()

            # Cold cache measurement
            print(f"  [INFO] Cold cache measurement...")
            cold_ms, cold_calls = count_api_calls(client, table_name, record)
            print(f"  [OK] Cold: {cold_calls} API calls, {cold_ms}ms")

            # Warm cache measurements
            warm_times = []
            warm_calls_list = []
            for rep in range(REPEAT_CALLS):
                ms, calls = count_api_calls(client, table_name, record)
                warm_times.append(ms)
                warm_calls_list.append(calls)

            avg_warm_ms = round(sum(warm_times) / len(warm_times), 1)
            avg_warm_calls = round(sum(warm_calls_list) / len(warm_calls_list), 1)
            print(f"  [OK] Warm (avg {REPEAT_CALLS}x): {avg_warm_calls} API calls, {avg_warm_ms}ms")

            results.append({
                "picklists": num_picklists,
                "total_fields": total_fields,
                "cold_calls": cold_calls,
                "cold_ms": cold_ms,
                "warm_calls": avg_warm_calls,
                "warm_ms": avg_warm_ms,
            })

        except Exception as e:
            print(f"  [ERR] Scale point failed: {e}")
            results.append({
                "picklists": num_picklists,
                "total_fields": total_fields,
                "cold_calls": "ERR",
                "cold_ms": "ERR",
                "warm_calls": "ERR",
                "warm_ms": "ERR",
            })

        finally:
            # Cleanup: delete table
            print(f"  [INFO] Cleaning up table {table_name}...")
            try:
                backoff(lambda: client.tables.delete(table_name))
                print(f"  [OK] Table deleted.")
            except Exception as e:
                print(f"  [WARN] Cleanup failed: {e}")

    # Print results
    print("\n" + "=" * 78)
    print("RESULTS")
    print("=" * 78)
    header = (
        f"{'Picklists':>9} | {'Total':>5} | "
        f"{'Cold Calls':>10} | {'Cold ms':>8} | "
        f"{'Warm Calls':>10} | {'Warm ms':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['picklists']:>9} | {r['total_fields']:>5} | "
            f"{str(r['cold_calls']):>10} | {str(r['cold_ms']):>8} | "
            f"{str(r['warm_calls']):>10} | {str(r['warm_ms']):>8}"
        )

    print("\n" + "-" * 78)
    print("Notes:")
    print("  - Cold = first call with empty cache (metadata fetched from Dataverse)")
    print(f"  - Warm = average of {REPEAT_CALLS} repeat calls (cache populated)")
    print("  - Times include real network latency to Dataverse")

    print("\n[OK] Live benchmark complete.")
    client.close()


if __name__ == "__main__":
    main()
