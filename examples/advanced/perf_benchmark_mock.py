# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Mock-based performance benchmark for picklist label resolution.

Compares API call count and wall-clock time across different scale points
(number of string fields / picklists in a record). Uses mocked HTTP
responses with configurable simulated latency to isolate the algorithmic
difference between approaches.

Run from repo root:
    $env:PYTHONPATH="src"; .conda/python.exe examples/advanced/perf_benchmark_mock.py

The script works with whatever picklist resolution approach is currently
checked out (Option B or Option C). Switch branches and re-run to compare.
"""

import sys
import time
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, "src")

from PowerPlatform.Dataverse.data._odata import _ODataClient


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCALE_POINTS = [1, 10, 100, 500, 1000]  # number of picklist columns
EXTRA_STRING_FIELDS = 3  # non-picklist string fields (fixed, same as live)
OPTIONS_PER_PICKLIST = 4  # number of options per picklist attribute (same as live)
SIMULATED_LATENCY_MS = 150  # typical Dataverse metadata response time
REPEAT_CALLS = 3  # number of repeat calls to measure cache effect
CSV_OUTPUT = False  # set True to emit CSV instead of table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_option(value: int, label: str) -> dict:
    """Build a realistic OptionMetadata payload."""
    return {
        "Value": value,
        "Label": {
            "LocalizedLabels": [
                {"Label": label, "LanguageCode": 1033}
            ]
        },
    }


def _build_bulk_response(num_picklists: int, options_per: int) -> dict:
    """Build a PicklistAttributeMetadata bulk response for Option C."""
    items = []
    for i in range(num_picklists):
        attr_name = f"new_picklist_{i}"
        options = [
            _make_option(j, f"Label_{i}_{j}")
            for j in range(options_per)
        ]
        items.append({
            "LogicalName": attr_name,
            "OptionSet": {"Options": options},
        })
    return {"value": items}


def _build_type_check_response(attr_names: list, picklist_names: set) -> dict:
    """Build a CRM.In type-check response for Option B."""
    items = []
    for name in attr_names:
        if name in picklist_names:
            items.append({
                "LogicalName": name,
                "@odata.type": "#Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            })
    return {"value": items}


def _build_single_optionset_response(attr_index: int, options_per: int) -> dict:
    """Build a single attribute OptionSet response for Option B."""
    options = [
        _make_option(j, f"Label_{attr_index}_{j}")
        for j in range(options_per)
    ]
    return {
        "value": [{
            "LogicalName": f"new_picklist_{attr_index}",
            "OptionSet": {"Options": options},
        }]
    }


def _build_record(num_string_fields: int, num_picklists: int) -> dict:
    """Build a test record with string values (labels for picklists, plain for others)."""
    record = {}
    for i in range(num_picklists):
        # Use first label option for each picklist
        record[f"new_picklist_{i}"] = f"Label_{i}_0"
    for i in range(num_string_fields - num_picklists):
        record[f"new_textfield_{i}"] = f"some text value {i}"
    return record


def _create_client() -> _ODataClient:
    """Create an _ODataClient with mocked auth."""
    mock_auth = MagicMock()
    mock_token = MagicMock()
    mock_token.access_token = "fake-token"
    mock_auth._acquire_token.return_value = mock_token

    mock_config = MagicMock()
    mock_config.http_retries = 0
    mock_config.http_backoff = 0
    mock_config.http_timeout = 30
    mock_config.language_code = 1033

    client = _ODataClient(
        auth=mock_auth,
        base_url="https://mock.crm.dynamics.com",
        config=mock_config,
    )
    return client


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------


def run_benchmark(num_picklists: int) -> dict:
    """Run a single scale point and return metrics."""
    num_plain = EXTRA_STRING_FIELDS
    num_fields = num_picklists + num_plain
    record = _build_record(num_fields, num_picklists)
    latency_s = SIMULATED_LATENCY_MS / 1000.0

    # Track API calls
    api_call_count = 0

    def counting_request(method, url, **kwargs):
        """Mock _request that counts calls and adds simulated latency."""
        nonlocal api_call_count
        api_call_count += 1
        time.sleep(latency_s)

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        # Detect which kind of request this is by URL pattern
        url_lower = url.lower()

        # Option C: PicklistAttributeMetadata bulk fetch
        if "picklistattributemetadata" in url_lower:
            mock_resp.json.return_value = _build_bulk_response(num_picklists, OPTIONS_PER_PICKLIST)
            return mock_resp

        # Option B: CRM.In batch type check
        if "crm.in" in url_lower or "microsoft.dynamics.crm.in" in url_lower:
            picklist_names = {f"new_picklist_{i}" for i in range(num_picklists)}
            all_names = list(record.keys())
            mock_resp.json.return_value = _build_type_check_response(all_names, picklist_names)
            return mock_resp

        # Option B: individual optionset fetch (per-attribute)
        if "optionset" in url_lower or "globaloptionsetdefinitions" in url_lower:
            # Extract attr index from URL heuristically
            for i in range(num_picklists):
                if f"new_picklist_{i}" in url_lower:
                    mock_resp.json.return_value = _build_single_optionset_response(i, OPTIONS_PER_PICKLIST)
                    return mock_resp
            mock_resp.json.return_value = {"value": []}
            return mock_resp

        # Option B fallback: per-attribute type check (old approach)
        if "attributetype" in url_lower or "attributes" in url_lower:
            # Check if URL references a picklist attribute
            for i in range(num_picklists):
                if f"new_picklist_{i}" in url_lower:
                    mock_resp.json.return_value = {
                        "value": [{
                            "LogicalName": f"new_picklist_{i}",
                            "@odata.type": "#Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
                            "AttributeType": "Picklist",
                        }]
                    }
                    return mock_resp
            # Plain string attribute
            mock_resp.json.return_value = {
                "value": [{
                    "@odata.type": "#Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "AttributeType": "String",
                }]
            }
            return mock_resp

        # Default fallback
        mock_resp.json.return_value = {"value": []}
        return mock_resp

    # --- Cold cache run ---
    client = _create_client()
    with patch.object(client, "_request", side_effect=counting_request):
        api_call_count = 0
        t0 = time.perf_counter()
        result = client._convert_labels_to_ints("new_perftable", record)
        cold_time = time.perf_counter() - t0
        cold_calls = api_call_count

    # Verify resolution worked (at least some labels should be ints now)
    resolved_count = sum(1 for v in result.values() if isinstance(v, int))

    # --- Warm cache runs ---
    warm_times = []
    warm_calls_list = []
    for _ in range(REPEAT_CALLS):
        with patch.object(client, "_request", side_effect=counting_request):
            api_call_count = 0
            t0 = time.perf_counter()
            client._convert_labels_to_ints("new_perftable", record)
            warm_times.append(time.perf_counter() - t0)
            warm_calls_list.append(api_call_count)

    avg_warm_time = sum(warm_times) / len(warm_times)
    avg_warm_calls = sum(warm_calls_list) / len(warm_calls_list)

    return {
        "fields": num_fields,
        "picklists": num_picklists,
        "plain_strings": num_plain,
        "cold_calls": cold_calls,
        "cold_time_ms": round(cold_time * 1000, 1),
        "warm_calls": round(avg_warm_calls, 1),
        "warm_time_ms": round(avg_warm_time * 1000, 1),
        "resolved": resolved_count,
    }


def main():
    print("=" * 78)
    print("Picklist Label Resolution - Mock Performance Benchmark")
    print(f"Simulated latency: {SIMULATED_LATENCY_MS}ms per API call")
    print(f"Extra string fields: {EXTRA_STRING_FIELDS}")
    print(f"Options per picklist: {OPTIONS_PER_PICKLIST}")
    print(f"Warm cache repeat calls: {REPEAT_CALLS}")
    print("=" * 78)

    results = []
    for n in SCALE_POINTS:
        print(f"\n[INFO] Running scale point: {n} picklists ...", end="", flush=True)
        r = run_benchmark(n)
        results.append(r)
        print(f" done (cold={r['cold_calls']} calls, {r['cold_time_ms']}ms)")

    # Print results table
    print("\n" + "=" * 78)
    print("RESULTS")
    print("=" * 78)
    header = (
        f"{'Fields':>7} | {'Picklists':>9} | {'Plain':>5} | "
        f"{'Cold Calls':>10} | {'Cold ms':>8} | "
        f"{'Warm Calls':>10} | {'Warm ms':>8} | {'Resolved':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['fields']:>7} | {r['picklists']:>9} | {r['plain_strings']:>5} | "
            f"{r['cold_calls']:>10} | {r['cold_time_ms']:>8} | "
            f"{r['warm_calls']:>10} | {r['warm_time_ms']:>8} | {r['resolved']:>8}"
        )

    print("\n" + "-" * 78)
    print("Notes:")
    print(f"  - Cold = first call (cache empty, metadata fetched)")
    print(f"  - Warm = average of {REPEAT_CALLS} repeat calls (cache populated)")
    print(f"  - Resolved = number of label strings converted to ints")
    print(f"  - Times include {SIMULATED_LATENCY_MS}ms simulated latency per API call")

    if CSV_OUTPUT:
        print("\n--- CSV ---")
        print("fields,picklists,plain,cold_calls,cold_ms,warm_calls,warm_ms,resolved")
        for r in results:
            print(
                f"{r['fields']},{r['picklists']},{r['plain_strings']},"
                f"{r['cold_calls']},{r['cold_time_ms']},"
                f"{r['warm_calls']},{r['warm_time_ms']},{r['resolved']}"
            )

    # Summary
    first = results[0]
    last = results[-1]
    print(f"\n[INFO] Scaling: {first['fields']} fields -> {first['cold_calls']} API calls;  "
          f"{last['fields']} fields -> {last['cold_calls']} API calls")
    if last["cold_calls"] <= 2:
        print("[INFO] Approach: constant API calls (Option C - bulk fetch)")
    elif last["cold_calls"] > first["cold_calls"] * 5:
        print("[INFO] Approach: API calls scale with field count (Option B or baseline)")

    print("\n[OK] Benchmark complete.")


if __name__ == "__main__":
    main()
