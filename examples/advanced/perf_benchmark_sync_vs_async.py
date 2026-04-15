# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Sync vs Async performance benchmark for the Dataverse SDK.

Measures wall-clock time for three strategies across three workload scales:

  Strategy 1 — Sync sequential    : sync client, one request at a time
  Strategy 2 — Async sequential   : async client, one await at a time (no gather)
  Strategy 3 — Async concurrent   : async client, asyncio.gather for maximum concurrency

Workload scenarios (same operations on both sides):
  A. Individual creates : N ``records.create`` calls
  B. Individual reads   : N ``records.get`` calls (by ID, no query)
  C. Batch execute      : One ``batch.execute()`` containing N create operations

Scale points : controlled by SCALE_POINTS (default [10, 50, 100])

The script creates a temporary custom table, runs all scenarios, prints a
summary table, then deletes the table.  If interrupted mid-run the table
may remain and must be deleted manually (name printed at startup).

Run from repo root:
    $env:PYTHONPATH="src"
    .conda/python.exe examples/advanced/perf_benchmark_sync_vs_async.py

Prerequisites:
    pip install PowerPlatform-Dataverse-Client azure-identity
    DATAVERSE_URL environment variable set (or enter at prompt)

WARNING: This script creates and deletes a real table on your Dataverse
environment.  Do not run against production.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from enum import IntEnum
from typing import Dict, List, Optional, Tuple

from azure.identity import InteractiveBrowserCredential as _SyncCred

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.aio import AsyncDataverseClient


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCALE_POINTS: List[int] = [10, 50, 100]

# Async concurrency cap for strategy 3.  asyncio.gather sends all at once;
# set lower to avoid hitting Dataverse rate limits (429).
MAX_CONCURRENT: int = 20  # max in-flight requests per gather call

# Seconds to wait after table creation before reading/writing records.
TABLE_SETTLE_DELAY: float = 5.0


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------


class _AsyncBrowserCredential:
    """Async wrapper around the sync InteractiveBrowserCredential.

    ``azure.identity.aio`` does not ship ``InteractiveBrowserCredential``.
    This wrapper runs ``get_token`` in a thread-pool executor so the browser
    flow works without blocking the event loop.
    """

    def __init__(self, sync_cred: _SyncCred) -> None:
        self._cred = sync_cred

    async def get_token(self, *scopes: str, **kwargs):  # type: ignore[override]
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._cred.get_token, *scopes)

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> "_AsyncBrowserCredential":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


def _make_record(i: int) -> Dict:
    return {
        "new_title": f"Bench-{i:04d}",
        "new_quantity": i,
        "new_amount": float(i) * 1.1,
        "new_completed": False,
        "new_priority": Priority.LOW,
    }


def _chunked(lst: list, size: int):
    """Yield successive chunks of *lst* of at most *size* items."""
    for start in range(0, len(lst), size):
        yield lst[start : start + size]


def _elapsed_ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 1)


def _backoff(op, *, delays=(0, 3, 10, 20)):
    last = None
    for delay in delays:
        if delay:
            time.sleep(delay)
        try:
            return op()
        except Exception as exc:
            last = exc
            print(f"  [WARN] Retry after {delay}s: {exc}")
    raise last  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


def create_table(client: DataverseClient, table_name: str) -> None:
    schema = {
        "new_Title": "string",
        "new_Quantity": "int",
        "new_Amount": "decimal",
        "new_Completed": "bool",
        "new_Priority": Priority,
    }
    print(f"  Creating table {table_name} …")
    _backoff(lambda: client.tables.create(table_name, schema))
    print(f"  Table created; waiting {TABLE_SETTLE_DELAY}s for column propagation …")
    time.sleep(TABLE_SETTLE_DELAY)


def delete_table(client: DataverseClient, table_name: str) -> None:
    try:
        _backoff(lambda: client.tables.delete(table_name))
        print(f"  Table {table_name} deleted.")
    except Exception as exc:
        print(f"  [WARN] Cleanup failed — delete {table_name} manually: {exc}")


# ---------------------------------------------------------------------------
# Strategy 1 — Sync sequential
# ---------------------------------------------------------------------------


def sync_seq_creates(client: DataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Create N records one at a time using the sync client."""
    ids: List[str] = []
    t0 = time.perf_counter()
    for i in range(n):
        rid = client.records.create(table, _make_record(i))
        ids.append(rid)
    elapsed = _elapsed_ms(t0)
    # cleanup
    for rid in ids:
        client.records.delete(table, rid)
    return elapsed, n


def sync_seq_reads(client: DataverseClient, table: str, ids: List[str]) -> float:
    """Read N records by ID one at a time using the sync client."""
    t0 = time.perf_counter()
    for rid in ids:
        client.records.get(table, rid, select=["new_title"])
    return _elapsed_ms(t0)


def sync_batch_creates(client: DataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Create N records via a single batch.execute()."""
    batch = client.batch.new()
    for i in range(n):
        batch.records.create(table, _make_record(i))
    t0 = time.perf_counter()
    result = batch.execute()
    elapsed = _elapsed_ms(t0)
    ids = list(result.entity_ids)
    # cleanup
    cleanup = client.batch.new()
    for rid in ids:
        cleanup.records.delete(table, rid)
    cleanup.execute(continue_on_error=True)
    return elapsed, len(ids)


# ---------------------------------------------------------------------------
# Strategy 2 — Async sequential
# ---------------------------------------------------------------------------


async def async_seq_creates(client: AsyncDataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Create N records one at a time using the async client (no gather)."""
    ids: List[str] = []
    t0 = time.perf_counter()
    for i in range(n):
        rid = await client.records.create(table, _make_record(i))
        ids.append(rid)
    elapsed = _elapsed_ms(t0)
    for rid in ids:
        await client.records.delete(table, rid)
    return elapsed, n


async def async_seq_reads(client: AsyncDataverseClient, table: str, ids: List[str]) -> float:
    """Read N records by ID one at a time using the async client."""
    t0 = time.perf_counter()
    for rid in ids:
        await client.records.get(table, rid, select=["new_title"])
    return _elapsed_ms(t0)


async def async_seq_batch_creates(client: AsyncDataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Create N records via a single async batch.execute()."""
    batch = client.batch.new()
    for i in range(n):
        batch.records.create(table, _make_record(i))
    t0 = time.perf_counter()
    result = await batch.execute()
    elapsed = _elapsed_ms(t0)
    ids = list(result.entity_ids)
    cleanup = client.batch.new()
    for rid in ids:
        cleanup.records.delete(table, rid)
    await cleanup.execute(continue_on_error=True)
    return elapsed, len(ids)


# ---------------------------------------------------------------------------
# Strategy 3 — Async concurrent (asyncio.gather with cap)
# ---------------------------------------------------------------------------


async def async_conc_creates(client: AsyncDataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Create N records concurrently using asyncio.gather (capped at MAX_CONCURRENT)."""

    async def _create(i: int) -> str:
        return await client.records.create(table, _make_record(i))

    all_ids: List[str] = []
    t0 = time.perf_counter()
    for chunk in _chunked(list(range(n)), MAX_CONCURRENT):
        chunk_ids = await asyncio.gather(*[_create(i) for i in chunk])
        all_ids.extend(chunk_ids)
    elapsed = _elapsed_ms(t0)

    # cleanup concurrently too
    async def _delete(rid: str) -> None:
        await client.records.delete(table, rid)

    for chunk_ids in _chunked(all_ids, MAX_CONCURRENT):
        await asyncio.gather(*[_delete(rid) for rid in chunk_ids])

    return elapsed, n


async def async_conc_reads(client: AsyncDataverseClient, table: str, ids: List[str]) -> float:
    """Read N records concurrently using asyncio.gather (capped at MAX_CONCURRENT)."""

    async def _read(rid: str) -> None:
        await client.records.get(table, rid, select=["new_title"])

    t0 = time.perf_counter()
    for chunk in _chunked(ids, MAX_CONCURRENT):
        await asyncio.gather(*[_read(rid) for rid in chunk])
    return _elapsed_ms(t0)


async def async_conc_batch_creates(client: AsyncDataverseClient, table: str, n: int) -> Tuple[float, int]:
    """Same as async_seq_batch_creates — batch is already one request, no gather needed."""
    # Batch execute is inherently one HTTP round-trip; concurrency only matters for
    # individual-operation scenarios.  We reuse the sequential result here so the
    # table column in the report is filled and the comparison is fair.
    return await async_seq_batch_creates(client, table, n)


# ---------------------------------------------------------------------------
# Seed + read-back fixture for read benchmarks
# ---------------------------------------------------------------------------


def seed_sync(client: DataverseClient, table: str, n: int) -> List[str]:
    """Create N records synchronously, return their IDs."""
    batch = client.batch.new()
    for i in range(n):
        batch.records.create(table, _make_record(i))
    result = batch.execute()
    return list(result.entity_ids)


def cleanup_sync(client: DataverseClient, table: str, ids: List[str]) -> None:
    batch = client.batch.new()
    for rid in ids:
        batch.records.delete(table, rid)
    batch.execute(continue_on_error=True)


# ---------------------------------------------------------------------------
# Run all scenarios for one scale point
# ---------------------------------------------------------------------------

ScenarioResult = Dict[str, Optional[float]]


def _run_scale_sync(client: DataverseClient, table: str, n: int) -> ScenarioResult:
    print(f"    [sync-seq] creates …", end="", flush=True)
    ms_create, _ = sync_seq_creates(client, table, n)
    print(f" {ms_create:.0f}ms")

    # Seed records for read benchmark
    ids = seed_sync(client, table, n)

    print(f"    [sync-seq] reads …", end="", flush=True)
    ms_read = sync_seq_reads(client, table, ids)
    print(f" {ms_read:.0f}ms")

    cleanup_sync(client, table, ids)

    print(f"    [sync-seq] batch-create …", end="", flush=True)
    ms_batch, _ = sync_batch_creates(client, table, n)
    print(f" {ms_batch:.0f}ms")

    return {"create": ms_create, "read": ms_read, "batch": ms_batch}


async def _run_scale_async_seq(
    client: AsyncDataverseClient, table: str, n: int
) -> ScenarioResult:
    print(f"    [async-seq] creates …", end="", flush=True)
    ms_create, _ = await async_seq_creates(client, table, n)
    print(f" {ms_create:.0f}ms")

    # Seed N records via async batch (one HTTP round-trip) for the read benchmark
    async_batch = client.batch.new()
    for i in range(n):
        async_batch.records.create(table, _make_record(i))
    seed_result = await async_batch.execute()
    ids = list(seed_result.entity_ids)

    print(f"    [async-seq] reads …", end="", flush=True)
    ms_read = await async_seq_reads(client, table, ids)
    print(f" {ms_read:.0f}ms")

    # cleanup
    del_batch = client.batch.new()
    for rid in ids:
        del_batch.records.delete(table, rid)
    await del_batch.execute(continue_on_error=True)

    print(f"    [async-seq] batch-create …", end="", flush=True)
    ms_batch, _ = await async_seq_batch_creates(client, table, n)
    print(f" {ms_batch:.0f}ms")

    return {"create": ms_create, "read": ms_read, "batch": ms_batch}


async def _run_scale_async_conc(
    client: AsyncDataverseClient, table: str, n: int
) -> ScenarioResult:
    print(f"    [async-conc] creates …", end="", flush=True)
    ms_create, _ = await async_conc_creates(client, table, n)
    print(f" {ms_create:.0f}ms")

    # Seed for reads
    async_batch = client.batch.new()
    for i in range(n):
        async_batch.records.create(table, _make_record(i))
    seed_result = await async_batch.execute()
    ids = list(seed_result.entity_ids)

    print(f"    [async-conc] reads …", end="", flush=True)
    ms_read = await async_conc_reads(client, table, ids)
    print(f" {ms_read:.0f}ms")

    del_batch = client.batch.new()
    for rid in ids:
        del_batch.records.delete(table, rid)
    await del_batch.execute(continue_on_error=True)

    print(f"    [async-conc] batch-create …", end="", flush=True)
    ms_batch, _ = await async_conc_batch_creates(client, table, n)
    print(f" {ms_batch:.0f}ms")

    return {"create": ms_create, "read": ms_read, "batch": ms_batch}


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def _async_main(url: str, cred: _SyncCred) -> None:
    async_cred = _AsyncBrowserCredential(cred)

    results: Dict[int, Dict[str, ScenarioResult]] = {}

    async with AsyncDataverseClient(base_url=url, credential=async_cred) as async_client:
        with DataverseClient(base_url=url, credential=cred) as sync_client:
            table_suffix = uuid.uuid4().hex[:6]
            table_name = f"new_PerfBench{table_suffix}"
            print(f"\nTable name: {table_name}  (delete manually if script is interrupted)")

            try:
                create_table(sync_client, table_name)

                for n in SCALE_POINTS:
                    print(f"\n{'─' * 60}")
                    print(f"  Scale: N={n}")
                    print(f"{'─' * 60}")

                    results[n] = {}

                    results[n]["sync_seq"] = _run_scale_sync(sync_client, table_name, n)
                    results[n]["async_seq"] = await _run_scale_async_seq(
                        async_client, table_name, n
                    )
                    results[n]["async_conc"] = await _run_scale_async_conc(
                        async_client, table_name, n
                    )

            finally:
                delete_table(sync_client, table_name)

    _print_results(results)


def _print_results(results: Dict[int, Dict[str, ScenarioResult]]) -> None:
    strategies = [
        ("sync_seq", "Sync-Seq"),
        ("async_seq", "Async-Seq"),
        ("async_conc", "Async-Conc"),
    ]
    scenarios = [
        ("create", "Individual Creates"),
        ("read", "Individual Reads"),
        ("batch", "Batch Creates (1 req)"),
    ]

    print("\n" + "=" * 80)
    print("RESULTS  (wall-clock ms, lower is better)")
    print("=" * 80)

    for scenario_key, scenario_label in scenarios:
        print(f"\n  Scenario: {scenario_label}")
        header = f"  {'N':>6} | {'Sync-Seq':>12} | {'Async-Seq':>12} | {'Async-Conc':>12} | {'Speedup*':>10}"
        print(header)
        print("  " + "-" * (len(header) - 2))

        for n in sorted(results):
            row = results[n]
            s = row.get("sync_seq", {}).get(scenario_key)
            aseq = row.get("async_seq", {}).get(scenario_key)
            aconc = row.get("async_conc", {}).get(scenario_key)

            def fmt(v: Optional[float]) -> str:
                return f"{v:>10.0f}ms" if v is not None else f"{'N/A':>12}"

            speedup = ""
            if s is not None and aconc is not None and aconc > 0:
                speedup = f"{s / aconc:>8.1f}x"

            print(f"  {n:>6} | {fmt(s)} | {fmt(aseq)} | {fmt(aconc)} | {speedup:>10}")

    print()
    print("  * Speedup = Sync-Seq time / Async-Conc time")
    print(f"  * Max concurrent requests per gather call: {MAX_CONCURRENT}")
    print("  * Batch scenario: Async-Conc = Async-Seq (both are one HTTP request)")
    print()
    print("  Notes:")
    print("    - Times include real network latency to Dataverse.")
    print("    - Sync-Seq: one HTTP request at a time, blocks thread.")
    print("    - Async-Seq: one await at a time, no true parallelism.")
    print("    - Async-Conc: asyncio.gather sends up to MAX_CONCURRENT requests in parallel.")
    print("    - Batch: one HTTP round-trip regardless of N (Dataverse server-side parallel).")
    print()
    print("[OK] Benchmark complete.")


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATAVERSE_URL", "")
    if not url:
        url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not url:
        print("[ERR] DATAVERSE_URL required.")
        sys.exit(1)
    url = url.rstrip("/")

    print("=" * 80)
    print("Dataverse SDK — Sync vs Async Performance Benchmark")
    print(f"Environment : {url}")
    print(f"Scale points: {SCALE_POINTS}")
    print(f"Max concurrent (async-conc): {MAX_CONCURRENT}")
    print("=" * 80)

    cred = _SyncCred()
    asyncio.run(_async_main(url, cred))


if __name__ == "__main__":
    main()
