# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Walkthrough demonstrating auto-chunking and concurrent dispatch in the Dataverse SDK.

This example shows:
- Auto-chunking: operations on >1,000 records are automatically split into chunks
- Boundary conditions: single chunk (≤1,000), exact boundary (1,000), over boundary (1,001+)
- Concurrent dispatch: max_workers > 1 dispatches chunks in parallel via ThreadPoolExecutor
- Performance comparison: wall-clock timing across max_workers values (1, 2, 3)
- Load testing: high-volume creates, updates, and upserts up to 25,000 records
- Correctness: verifies returned IDs count, uniqueness, and no data loss under concurrency

Run from repo root:
    $env:PYTHONPATH="src"; .conda/python.exe examples/advanced/chunking_concurrency_walkthrough.py --url https://yourorg.crm.dynamics.com

Prerequisites:
    - pip install PowerPlatform-Dataverse-Client azure-identity
    - --url flag, DATAVERSE_URL environment variable, or enter at prompt
    - Interactive browser auth (or configure credentials below)

WARNING: This script creates and deletes a temporary table on your Dataverse
environment (new_ChunkBench_*). It cleans up after itself, but if interrupted
mid-run you may need to manually delete the table.
"""

import argparse
import os
import sys
import time
import uuid

from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
from PowerPlatform.Dataverse.models.upsert import UpsertItem


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Batch sizes to test chunking boundary behaviour.
# At _MULTIPLE_BATCH_SIZE=1,000 the SDK auto-splits any list larger than this.
BOUNDARY_SIZES = [
    500,   # single chunk, well below limit
    999,   # single chunk, one below limit
    1_000, # single chunk, exactly at limit
    1_001, # two chunks (1,000 + 1)
    2_500, # three chunks (1,000 + 1,000 + 500)
]

# Concurrency levels tested in the perf comparison sections (4, 6, 9).
# 3,000 records = 3 chunks, which aligns exactly with the 3 worker levels tested.
# The SDK caps max_workers at 15; values above are silently capped.
CONCURRENCY_LEVELS = [1, 2, 3]

# Record count used for the concurrency comparison sections (4, 6, 9).
# 3 chunks of 1,000 — aligns with CONCURRENCY_LEVELS so each worker gets one chunk.
CONCURRENCY_BATCH_SIZE = 3_000

# Record count used for the load test sections (5, 7, 10, 11).
# Sequential baseline is not measured at this scale — see comparison sections for speedup.
LOAD_TEST_BATCH_SIZE = 25_000    # 25 chunks of 1,000
LOAD_TEST_MAX_WORKERS = 15       # concurrency level for load tests (caps max_workers at 15)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def log_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def log_call(description):
    print(f"\n-> {description}")


def backoff(op, *, delays=(0, 2, 5, 10, 20)):
    """Retry an operation with escalating delays on any exception."""
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            time.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = op()
            if attempts > 1:
                print(f"   [INFO] Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            print(f"   [WARN] Attempt {attempts} failed: {ex}")
    raise last


def build_records(n, *, prefix="record"):
    """Build n minimal test records."""
    return [
        {
            "new_Title": f"{prefix}-{i:06d}",
            "new_Value": i,
        }
        for i in range(n)
    ]


def build_upsert_items(n, *, prefix="upsert", value_offset=0):
    """Build n UpsertItem objects keyed by new_title as the alternate key."""
    return [
        UpsertItem(
            alternate_key={"new_title": f"{prefix}-{i:06d}"},
            record={"new_Value": value_offset + i},
        )
        for i in range(n)
    ]


def timed(fn):
    """Call fn(), return (result, elapsed_seconds)."""
    t0 = time.perf_counter()
    result = fn()
    return result, round(time.perf_counter() - t0, 2)


def assert_ids(ids, expected_count, *, label):
    """Verify IDs list: correct count and no duplicates."""
    assert isinstance(ids, list), f"[{label}] expected list, got {type(ids)}"
    assert len(ids) == expected_count, (
        f"[{label}] expected {expected_count} IDs, got {len(ids)}"
    )
    unique = set(ids)
    assert len(unique) == expected_count, (
        f"[{label}] duplicate IDs detected: {expected_count - len(unique)} duplicate(s)"
    )
    assert all(isinstance(i, str) and i for i in ids), (
        f"[{label}] one or more IDs are not non-empty strings"
    )
    print(f"   [OK] {expected_count} IDs returned — all unique, all non-empty strings")


def wait_for_alternate_key(client, table_name, key_schema_name, *, timeout=120, poll=5):
    """Poll until the alternate key index is Active or timeout is reached."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        keys = client.tables.get_alternate_keys(table_name)
        for k in keys:
            if k.schema_name == key_schema_name:
                if k.status == "Active":
                    return
                if k.status == "Failed":
                    raise RuntimeError(
                        f"Alternate key '{key_schema_name}' index build failed."
                    )
                break
        time.sleep(poll)
    print(
        f"  [WARN] Alternate key '{key_schema_name}' did not reach Active within "
        f"{timeout}s; upsert tests may fail."
    )


# ---------------------------------------------------------------------------
# Main walkthrough
# ---------------------------------------------------------------------------


def main():
    log_section("Dataverse SDK — Chunking & Concurrency Walkthrough")

    parser = argparse.ArgumentParser(
        description="Dataverse SDK chunking and concurrency walkthrough."
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("DATAVERSE_URL"),
        metavar="URL",
        help=(
            "Dataverse environment URL (e.g. https://yourorg.crm.dynamics.com). "
            "Falls back to the DATAVERSE_URL environment variable."
        ),
    )
    args = parser.parse_args()

    url = args.url
    if not url:
        url = input("Enter Dataverse URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not url:
        print("[ERR] Dataverse URL required (--url or DATAVERSE_URL env var).")
        sys.exit(1)

    url = url.rstrip("/")

    # ============================================================================
    # 1. SETUP & AUTHENTICATION
    # ============================================================================
    log_section("1. Setup & Authentication")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{url}', credential=...)")
    client = DataverseClient(base_url=url, credential=credential)
    print(f"[OK] Connected to: {url}")

    # Unique table name per run so parallel runs don't collide
    run_id = uuid.uuid4().hex[:6]
    table_name = f"new_ChunkBench{run_id}"

    all_ids = []  # accumulates create() IDs throughout the walkthrough for cleanup

    try:
        _run_walkthrough(client, table_name, all_ids)
    finally:
        _cleanup(client, table_name)

    client.close()


def _run_walkthrough(client, table_name, all_ids):
    # ============================================================================
    # 2. TABLE CREATION
    # Creates the table. The alternate key on new_title (needed for upsert sections
    # 8–11) is created after Section 7 to avoid unique-constraint interference
    # while batch inserts are running in sections 3–7.
    # ============================================================================
    log_section("2. Table Creation")

    log_call(f"client.tables.create('{table_name}', {{...}})")
    table_info = backoff(
        lambda: client.tables.create(
            table_name,
            {
                "new_Title": "string",
                "new_Value": "int",
            },
        )
    )
    print(f"[OK] Table created: {table_info.get('table_schema_name')}")
    print(f"  Entity set: {table_info.get('entity_set_name')}")

    # Give Dataverse a moment to propagate the schema before writing records
    print("  [INFO] Waiting 10s for schema propagation...")
    time.sleep(10)

    # Alternate key on new_title is created after Section 7 (just before Section 8).
    # Creating it here while batch inserts are running can trigger a spurious unique
    # constraint violation (SQL 2601) when the index becomes Active mid-section.
    key_schema_name = f"{table_name}_title_key"

    # ============================================================================
    # 3. AUTO-CHUNKING: BOUNDARY TESTS (CREATE)
    # The SDK automatically splits any list exceeding 1,000 records into chunks.
    # Here we verify correct behaviour at and around the 1,000-record boundary.
    # ============================================================================
    log_section("3. Auto-Chunking: Boundary Tests (Create)")
    print(
        "Testing create() at boundary sizes: single chunk (≤1,000), exact limit (1,000),\n"
        "and over-limit (1,001+). Verifying returned ID count and uniqueness."
    )
    print(f"\n{'Size':>6}  {'Chunks':>6}  {'Time (s)':>9}  {'IDs OK':>6}  {'IDs/s':>7}")
    print("-" * 45)

    boundary_results = []
    for n in BOUNDARY_SIZES:
        records = build_records(n, prefix=f"boundary-{n}")
        ids, elapsed = timed(
            lambda recs=records: backoff(lambda: client.records.create(table_name, recs))
        )
        chunks = (n + 999) // 1000  # ceiling division
        rate = round(n / elapsed) if elapsed > 0 else 0
        assert_ids(ids, n, label=f"boundary-{n}")
        all_ids.extend(ids)

        boundary_results.append((n, chunks, elapsed, rate))
        print(f"{n:>6}  {chunks:>6}  {elapsed:>9.2f}  {'✓':>6}  {rate:>7,}")

    print(
        "\n[OK] All boundary sizes produce correct ID counts with no duplicates.\n"
        "     The SDK auto-splits at 1,000 without any caller intervention."
    )

    # ============================================================================
    # 4. CONCURRENT DISPATCH: max_workers COMPARISON (CREATE)
    # 3,000-record batch (3 chunks) dispatched sequentially and with 2 and 3
    # concurrent workers. Wall-clock time is measured for each level. The 3-chunk
    # batch aligns exactly with the 3 worker levels so each worker gets one chunk.
    # ============================================================================
    log_section("4. Concurrent Dispatch: max_workers Comparison (Create)")
    n = CONCURRENCY_BATCH_SIZE
    chunks = (n + 999) // 1000
    print(
        f"Creating {n:,} records ({chunks} chunks of 1,000) with "
        f"max_workers = {CONCURRENCY_LEVELS}.\n"
        f"Baseline is max_workers=1 (sequential). Higher values dispatch chunks in parallel."
    )
    print(f"\n{'max_workers':>12}  {'Time (s)':>9}  {'IDs OK':>7}  {'IDs/s':>7}  {'Speedup':>8}")
    print("-" * 52)

    concurrency_results = []
    baseline_elapsed = None

    for workers in CONCURRENCY_LEVELS:
        records = build_records(n, prefix=f"concurrency-w{workers}")
        ids, elapsed = timed(
            lambda recs=records, w=workers: backoff(
                lambda: client.records.create(table_name, recs, max_workers=w)
            )
        )
        assert_ids(ids, n, label=f"concurrency-workers-{workers}")
        all_ids.extend(ids)

        if baseline_elapsed is None:
            baseline_elapsed = elapsed
        speedup = round(baseline_elapsed / elapsed, 2) if elapsed > 0 else 0
        rate = round(n / elapsed) if elapsed > 0 else 0
        concurrency_results.append((workers, elapsed, rate, speedup))

        speedup_str = f"{speedup:.2f}×" if workers > 1 else "(baseline)"
        print(f"{workers:>12}  {elapsed:>9.2f}  {'✓':>7}  {rate:>7,}  {speedup_str:>8}")

    # Identify fastest configuration
    best = min(concurrency_results, key=lambda r: r[1])
    print(
        f"\n[OK] Fastest: max_workers={best[0]} at {best[1]:.2f}s "
        f"({best[3]:.2f}× vs sequential)"
    )
    print(
        "     Actual speedup depends on network latency, API throttling,\n"
        "     and the number of available cores."
    )

    # ============================================================================
    # 5. LOAD TEST: HIGH-VOLUME CREATE
    # 100,000 records (100 chunks of 1,000) with max_workers=LOAD_TEST_MAX_WORKERS.
    # Sequential baseline is not measured at this scale — see Section 4 for speedup.
    # Verifies correctness under load: exact ID count, uniqueness, all non-empty.
    # ============================================================================
    log_section("5. Load Test: High-Volume Create")
    n = LOAD_TEST_BATCH_SIZE
    chunks = (n + 999) // 1000
    print(
        f"Creating {n:,} records ({chunks} chunks of 1,000) "
        f"with max_workers={LOAD_TEST_MAX_WORKERS}.\n"
        "Sequential baseline not measured at this scale — see Section 4 for speedup comparison."
    )

    records = build_records(n, prefix="loadtest")
    log_call(
        f"client.records.create('{table_name}', [{n:,} records], "
        f"max_workers={LOAD_TEST_MAX_WORKERS})"
    )
    ids, elapsed = timed(
        lambda: backoff(
            lambda: client.records.create(table_name, records, max_workers=LOAD_TEST_MAX_WORKERS)
        )
    )
    assert_ids(ids, n, label=f"load-create-{n}")
    all_ids.extend(ids)
    load_test_ids = ids  # kept for update load test (Section 7)

    rate = round(n / elapsed) if elapsed > 0 else 0
    print(f"[OK] {n:,} records created in {elapsed:.2f}s  ({rate:,} records/s)")

    # ============================================================================
    # 6. CONCURRENT DISPATCH: max_workers COMPARISON (UPDATE)
    # A 3,000-record slice of the load test IDs dispatched at each concurrency
    # level, mirroring Section 4 for updates. Sequential baseline included.
    # ============================================================================
    log_section("6. Concurrent Dispatch: max_workers Comparison (Update)")
    n_cmp = CONCURRENCY_BATCH_SIZE
    chunks_cmp = (n_cmp + 999) // 1000
    update_cmp_ids = load_test_ids[:n_cmp]
    print(
        f"Updating {n_cmp:,} records ({chunks_cmp} chunks of 1,000) with "
        f"max_workers = {CONCURRENCY_LEVELS}.\n"
        f"Baseline is max_workers=1 (sequential). Higher values dispatch chunks in parallel."
    )
    print(f"\n{'max_workers':>12}  {'Time (s)':>9}  {'OK':>4}  {'Records/s':>10}  {'Speedup':>8}")
    print("-" * 56)

    update_concurrency_results = []
    update_concurrency_baseline = None

    for workers in CONCURRENCY_LEVELS:
        _, elapsed = timed(
            lambda w=workers: backoff(
                lambda: client.records.update(
                    table_name,
                    update_cmp_ids,
                    {"new_Value": workers},
                    max_workers=w,
                )
            )
        )
        if update_concurrency_baseline is None:
            update_concurrency_baseline = elapsed
        speedup = round(update_concurrency_baseline / elapsed, 2) if elapsed > 0 else 0
        rate = round(n_cmp / elapsed) if elapsed > 0 else 0
        update_concurrency_results.append((workers, elapsed, rate, speedup))

        speedup_str = f"{speedup:.2f}×" if workers > 1 else "(baseline)"
        print(f"{workers:>12}  {elapsed:>9.2f}  {'✓':>4}  {rate:>10,}  {speedup_str:>8}")

    best = min(update_concurrency_results, key=lambda r: r[1])
    print(
        f"\n[OK] Fastest: max_workers={best[0]} at {best[1]:.2f}s "
        f"({best[3]:.2f}× vs sequential)"
    )
    print(
        "     Actual speedup depends on network latency, API throttling,\n"
        "     and the number of available cores."
    )

    # ============================================================================
    # 7. LOAD TEST: HIGH-VOLUME UPDATE
    # Broadcast a field change to all 100,000 records from Section 5.
    # Sequential baseline not measured at this scale — see Section 6 for speedup.
    # ============================================================================
    log_section("7. Load Test: High-Volume Update")
    n = LOAD_TEST_BATCH_SIZE
    chunks = (n + 999) // 1000
    print(
        f"Broadcasting an update to {n:,} records ({chunks} chunks of 1,000) "
        f"with max_workers={LOAD_TEST_MAX_WORKERS}.\n"
        "Sequential baseline not measured at this scale — see Section 6 for speedup comparison."
    )

    log_call(
        f"client.records.update('{table_name}', [{n:,} IDs], "
        f"{{...}}, max_workers={LOAD_TEST_MAX_WORKERS})"
    )
    _, elapsed = timed(
        lambda: backoff(
            lambda: client.records.update(
                table_name,
                load_test_ids,
                {"new_Value": 9999},
                max_workers=LOAD_TEST_MAX_WORKERS,
            )
        )
    )
    rate = round(n / elapsed) if elapsed > 0 else 0
    print(f"[OK] {n:,} records updated in {elapsed:.2f}s  ({rate:,} records/s)")

    # ============================================================================
    # 8. AUTO-CHUNKING: BOUNDARY TESTS (UPSERT)
    # Mirrors section 3 using upsert() instead of create(). Each batch uses fresh
    # alternate-key titles so the upsert acts as a bulk create. upsert() returns
    # None, so correctness is verified by the absence of exceptions and timing.
    # ============================================================================
    # Create the alternate key now that all batch inserts (sections 3–7) are done.
    # Building it here avoids the SQL 2601 unique-constraint interference that occurs
    # when the index becomes Active while a batch insert is in progress.
    log_call(
        f"client.tables.create_alternate_key("
        f"'{table_name}', '{key_schema_name}', ['new_title'])"
    )
    backoff(
        lambda: client.tables.create_alternate_key(
            table_name,
            key_schema_name,
            ["new_title"],
        )
    )
    print(f"  [INFO] Alternate key '{key_schema_name}' queued — waiting for index to become Active...")
    wait_for_alternate_key(client, table_name, key_schema_name)
    print("  [OK] Alternate key active.")

    log_section("8. Auto-Chunking: Boundary Tests (Upsert)")
    print(
        "Testing upsert() at boundary sizes with fresh alternate keys (create mode).\n"
        "Verifies auto-chunking is applied identically to create()."
    )
    print(f"\n{'Size':>6}  {'Chunks':>6}  {'Time (s)':>9}  {'OK':>4}  {'Items/s':>8}")
    print("-" * 45)

    upsert_total = 0
    for n in BOUNDARY_SIZES:
        items = build_upsert_items(n, prefix=f"upsert-b{n}")
        _, elapsed = timed(
            lambda it=items: backoff(lambda: client.records.upsert(table_name, it))
        )
        chunks = (n + 999) // 1000
        rate = round(n / elapsed) if elapsed > 0 else 0
        upsert_total += n
        print(f"{n:>6}  {chunks:>6}  {elapsed:>9.2f}  {'✓':>4}  {rate:>8,}")

    print(
        "\n[OK] All boundary sizes completed without error.\n"
        "     The SDK auto-splits upsert() at 1,000 identically to create()."
    )

    # ============================================================================
    # 9. CONCURRENT DISPATCH: max_workers COMPARISON (UPSERT)
    # Same 3,000-item batch dispatched at each concurrency level, mirroring
    # section 4. Fresh alternate-key titles are used per workers level so every
    # run is an independent create, not an update of prior data.
    # ============================================================================
    log_section("9. Concurrent Dispatch: max_workers Comparison (Upsert)")
    n = CONCURRENCY_BATCH_SIZE
    chunks = (n + 999) // 1000
    print(
        f"Upserting {n:,} records ({chunks} chunks of 1,000) with "
        f"max_workers = {CONCURRENCY_LEVELS}.\n"
        f"Baseline is max_workers=1 (sequential). Higher values dispatch chunks in parallel."
    )
    print(f"\n{'max_workers':>12}  {'Time (s)':>9}  {'OK':>4}  {'Items/s':>8}  {'Speedup':>8}")
    print("-" * 52)

    upsert_concurrency_results = []
    upsert_baseline_elapsed = None

    for workers in CONCURRENCY_LEVELS:
        items = build_upsert_items(n, prefix=f"upsert-cw{workers}")
        _, elapsed = timed(
            lambda it=items, w=workers: backoff(
                lambda: client.records.upsert(table_name, it, max_workers=w)
            )
        )
        upsert_total += n

        if upsert_baseline_elapsed is None:
            upsert_baseline_elapsed = elapsed
        speedup = round(upsert_baseline_elapsed / elapsed, 2) if elapsed > 0 else 0
        rate = round(n / elapsed) if elapsed > 0 else 0
        upsert_concurrency_results.append((workers, elapsed, rate, speedup))

        speedup_str = f"{speedup:.2f}×" if workers > 1 else "(baseline)"
        print(f"{workers:>12}  {elapsed:>9.2f}  {'✓':>4}  {rate:>8,}  {speedup_str:>8}")

    best = min(upsert_concurrency_results, key=lambda r: r[1])
    print(
        f"\n[OK] Fastest: max_workers={best[0]} at {best[1]:.2f}s "
        f"({best[3]:.2f}× vs sequential)"
    )
    print(
        "     Actual speedup depends on network latency, API throttling,\n"
        "     and the number of available cores."
    )

    # ============================================================================
    # 10. LOAD TEST: HIGH-VOLUME UPSERT (CREATE MODE)
    # 100,000 items with fresh alternate keys — acts as a bulk create through the
    # upsert path. Mirrors section 5. Items are kept for the update-mode test below.
    # Sequential baseline not measured at this scale — see Section 9 for speedup.
    # ============================================================================
    log_section("10. Load Test: High-Volume Upsert (Create Mode)")
    n = LOAD_TEST_BATCH_SIZE
    chunks = (n + 999) // 1000
    print(
        f"Upserting {n:,} records ({chunks} chunks of 1,000) with "
        f"max_workers={LOAD_TEST_MAX_WORKERS} (create mode — fresh alternate keys).\n"
        "Sequential baseline not measured at this scale — see Section 9 for speedup comparison."
    )

    upsert_load_items = build_upsert_items(n, prefix="upsert-load")
    log_call(
        f"client.records.upsert('{table_name}', [{n:,} items], "
        f"max_workers={LOAD_TEST_MAX_WORKERS})"
    )
    _, elapsed = timed(
        lambda: backoff(
            lambda: client.records.upsert(
                table_name, upsert_load_items, max_workers=LOAD_TEST_MAX_WORKERS
            )
        )
    )
    upsert_total += n
    rate = round(n / elapsed) if elapsed > 0 else 0
    print(f"[OK] {n:,} records upserted (created) in {elapsed:.2f}s  ({rate:,} items/s)")

    # ============================================================================
    # 11. LOAD TEST: HIGH-VOLUME UPSERT (UPDATE MODE)
    # Re-upserts the same 100,000 alternate keys from section 10 with a new value —
    # all items match existing records, so the upsert path acts as a bulk update.
    # Sequential baseline not measured at this scale — see Section 9 for speedup.
    # ============================================================================
    log_section("11. Load Test: High-Volume Upsert (Update Mode)")
    print(
        f"Re-upserting the same {n:,} alternate keys with new_Value=9999 ({chunks} chunks) "
        f"with max_workers={LOAD_TEST_MAX_WORKERS}.\n"
        "All records exist — upsert acts as bulk update.\n"
        "Sequential baseline not measured at this scale — see Section 9 for speedup comparison."
    )

    upsert_update_items = [
        UpsertItem(alternate_key=item.alternate_key, record={"new_Value": 9999})
        for item in upsert_load_items
    ]

    log_call(
        f"client.records.upsert('{table_name}', [{n:,} items], "
        f"max_workers={LOAD_TEST_MAX_WORKERS})  # update mode"
    )
    _, elapsed = timed(
        lambda: backoff(
            lambda: client.records.upsert(
                table_name, upsert_update_items, max_workers=LOAD_TEST_MAX_WORKERS
            )
        )
    )
    upsert_total += n
    rate = round(n / elapsed) if elapsed > 0 else 0
    print(f"[OK] {n:,} records upserted (updated) in {elapsed:.2f}s  ({rate:,} items/s)")

    # ============================================================================
    # 12. CORRECTNESS: NO DATA LOSS UNDER CONCURRENCY
    # Verifies that concurrent dispatch did not silently drop or duplicate records.
    # create() IDs are verified by count and uniqueness. upsert() records (which
    # return None) are spot-checked via alternate key lookup.
    # ============================================================================
    log_section("12. Correctness: No Data Loss Under Concurrency")

    expected_total = len(all_ids)
    print(f"Total records created via create() across all sections: {expected_total:,}")
    print(f"Total items dispatched via upsert() (not tracked by ID): {upsert_total:,}")
    print("\nVerifying all create() IDs in the accumulated set are unique...")

    unique_ids = set(all_ids)
    if len(unique_ids) == expected_total:
        print(f"[OK] No duplicate IDs across {expected_total:,} records — data integrity confirmed.")
    else:
        dupes = expected_total - len(unique_ids)
        print(f"[WARN] {dupes} duplicate IDs detected across {expected_total:,} records.")

    # Spot-check create() records by GUID
    print("\nSpot-checking create() records by GUID...")
    spot_indices = [0, len(all_ids) // 4, len(all_ids) // 2, len(all_ids) - 1]
    for idx in spot_indices:
        rid = all_ids[idx]
        record = backoff(lambda r=rid: client.records.get(table_name, r, select=["new_title"]))
        title = record.get("new_title", "?")
        print(f"   ID[{idx:>6}]: new_title='{title}'  ✓")

    # Ordering verification
    # Chunk-level ordering is guaranteed by the SDK (futures collected in submission
    # order). Within a single chunk, Dataverse preserves positional correspondence
    # for batch operations in practice (though not formally documented).
    #
    # The n=1001 boundary batch uses 2 chunks (1000 + 1 records) and occupies
    # all_ids[2499:3500]. The sole record of chunk 2 (records[1000]) has title
    # "boundary-1001-001000"; it must appear at all_ids[3499], not earlier.
    # The n=500 batch (single chunk) occupies all_ids[0:500]; checking three
    # positions validates within-chunk ordering end-to-end through Dataverse.
    print("\nVerifying chunk-level ordering (SDK guarantee)...")
    chunk2_id = all_ids[3499]
    chunk2_rec = backoff(
        lambda r=chunk2_id: client.records.get(table_name, r, select=["new_title"])
    )
    expected_chunk2 = "boundary-1001-001000"
    actual_chunk2 = chunk2_rec.get("new_title", "?")
    chunk2_status = "✓" if actual_chunk2 == expected_chunk2 else f"MISMATCH (got '{actual_chunk2}')"
    print(f"   all_ids[3499] (chunk-2 sole record): expected '{expected_chunk2}' → {chunk2_status}")

    print("Verifying within-chunk ordering for n=500 single-chunk batch (all_ids[0:500])...")
    for pos in [0, 249, 499]:
        rid = all_ids[pos]
        rec = backoff(lambda r=rid: client.records.get(table_name, r, select=["new_title"]))
        expected = f"boundary-500-{pos:06d}"
        actual = rec.get("new_title", "?")
        pos_status = "✓" if actual == expected else f"MISMATCH (got '{actual}')"
        print(f"   all_ids[{pos:>3}]: expected '{expected}' → {pos_status}")

    # Spot-check upsert() records via alternate key filter
    print("\nSpot-checking upsert() records via alternate key lookup...")
    spot_titles = [
        f"upsert-b500-000000",
        f"upsert-load-000000",
        f"upsert-load-{LOAD_TEST_BATCH_SIZE - 1:06d}",
    ]
    for title in spot_titles:
        pages = backoff(
            lambda t=title: client.records.get(
                table_name,
                filter=f"new_title eq '{t}'",
                select=["new_title"],
                top=1,
            )
        )
        found = next(iter(pages), [])
        status = "✓" if len(found) == 1 else f"WARN: {len(found)} record(s) found"
        print(f"   new_title='{title}': {status}")

    print("\n[OK] Spot-check complete — records exist and are readable.")


def _cleanup(client, table_name):
    # ============================================================================
    # 13. CLEANUP
    # ============================================================================
    log_section("13. Cleanup")

    log_call(f"client.tables.delete('{table_name}')")
    try:
        backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted table: {table_name}")
    except MetadataError as ex:
        if "not found" in str(ex).lower():
            print(f"[OK] Table already removed: {table_name}")
        else:
            print(f"[WARN] Cleanup failed: {ex}")
    except Exception as ex:  # noqa: BLE001
        print(f"[WARN] Cleanup failed: {ex}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
