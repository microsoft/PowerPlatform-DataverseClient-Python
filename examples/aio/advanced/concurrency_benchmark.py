# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async concurrency benchmark and validation for the Dataverse Python SDK.

Measures async-sequential vs async-concurrent performance (not async vs sync).
Speedup = time for N sequential awaits / time for N concurrent gather() calls.

Tests
-----
1. Non-blocking reads (canary)
   A background task ticks every 10 ms while each GET call runs. A blocking call
   would starve the canary and produce a large gap. Covers records.list,
   tables.list, tables.get, query.sql, query.fetchxml, query.builder.

2. Read throughput (sequential vs concurrent)
   Runs N reads sequentially then N reads with asyncio.gather(). An internal
   lock or misplaced await would collapse the speedup to ~1x. Covers
   records.list, query.sql, tables.get.

3. Write concurrency (POST path)
   Same as Test 2 but for records.create(). The POST path uses a different
   timeout branch (120 s vs 10 s for GET), so a separate test ensures writes
   are also truly concurrent. Creates N records in parallel then cleans up.

4. Pagination non-blocking (async generator canary)
   Runs list_pages(), fetchxml().execute_pages(), and builder().execute_pages()
   while the canary ticks. Confirms the async generator yields control back to
   the event loop between page fetches.

5. Mixed fan-out (cross-operation concurrency)
   Fires 6 different operation types simultaneously in one gather(). A shared
   internal resource could accidentally serialize different operation types even
   if same-type parallelism works. This test catches cross-operation serialization.

6. Error resilience
   Fires 5 calls — 3 good, 2 intentionally bad — using gather(return_exceptions=True).
   Confirms the 3 good calls complete despite the 2 failures. Validates that the
   SDK does not suppress exceptions in a way that would break this pattern.

7. Real-world metadata fan-out
   Fetches schema info for 6 tables sequentially then in parallel. The most
   common real-world async use case: an app needs metadata for several tables
   at startup. Demonstrates the pattern end-to-end with real results.

How to interpret results
------------------------
- Speedup: async-sequential vs async-concurrent, not async vs sync.
  Expect 3-15x on WAN. Low speedup (<2x) suggests server throttling
  or accidental serialization in the SDK.
- Max tick gap (canary tests): Windows timer resolution is ~15 ms, so gaps
  up to ~30 ms are normal. Gaps > 200 ms indicate a blocking call.

Tip: run with PYTHONASYNCIODEBUG=1 to log a warning whenever a coroutine
holds the event loop for more than 100 ms.

Requirements:
    pip install PowerPlatform-Dataverse-Client[async] azure-identity
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential
from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.models.record import QueryResult
from PowerPlatform.Dataverse.models.table_info import TableInfo

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

SEPARATOR = "=" * 72
_WARN_GAP_MS = 200.0  # max acceptable canary gap in milliseconds
_WARN_SPEEDUP = 2.0  # min acceptable speedup ratio


def heading(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(title)
    print(SEPARATOR)


def _speedup_line(label: str, seq_s: float, conc_s: float) -> str:
    speedup = seq_s / conc_s if conc_s > 0 else float("inf")
    status = "[OK]  " if speedup >= _WARN_SPEEDUP else "[WARN]"
    return (
        f"  {status} {label}\n"
        f"         sequential={seq_s:.2f}s  concurrent={conc_s:.2f}s  "
        f"speedup={speedup:.1f}x"
    ), speedup


# ---------------------------------------------------------------------------
# Canary infrastructure
# ---------------------------------------------------------------------------


async def _canary(ticks: List[float], stop: asyncio.Event) -> None:
    """Append monotonic timestamps every 10 ms until *stop* is set."""
    while not stop.is_set():
        ticks.append(time.monotonic())
        await asyncio.sleep(0.01)


def _max_gap_ms(ticks: List[float]) -> float:
    if len(ticks) < 2:
        return 0.0
    return max(ticks[i + 1] - ticks[i] for i in range(len(ticks) - 1)) * 1000


async def _with_canary(coro_fn: Callable) -> tuple[Any, float, int, float]:
    """
    Run *coro_fn()* while a canary ticks every 10 ms.

    Returns (result, elapsed_ms, tick_count, max_gap_ms).
    """
    ticks: List[float] = []
    stop = asyncio.Event()
    task = asyncio.create_task(_canary(ticks, stop))
    t0 = time.monotonic()
    result = await coro_fn()
    elapsed_ms = (time.monotonic() - t0) * 1000
    stop.set()
    await task
    return result, elapsed_ms, len(ticks), _max_gap_ms(ticks)


def _canary_line(label: str, elapsed_ms: float, ticks: int, gap_ms: float) -> tuple[str, bool]:
    ok = gap_ms < _WARN_GAP_MS
    status = "[OK]  " if ok else "[WARN]"
    line = f"  {status} {label}\n" f"         call={elapsed_ms:.0f}ms  canary_ticks={ticks}  max_gap={gap_ms:.1f}ms"
    return line, ok


# ---------------------------------------------------------------------------
# Test 1: Non-blocking reads (GET operations)
# ---------------------------------------------------------------------------


async def run_test1_nonblocking_reads(client: AsyncDataverseClient) -> None:
    """
    Verify that read operations (GET) do not block the event loop.

    Covers: records.list, tables.list, query.sql, query.fetchxml,
            query.builder, tables.get.
    """
    heading("Test 1: Non-Blocking Reads (GET canary)")
    print(
        "A canary ticks every 10 ms during each read call.\n"
        "Max gap should stay near 10-30 ms (Windows timer resolution).\n"
        "Gaps > 200 ms indicate a blocking call in the async path.\n"
    )

    fetchxml = """
    <fetch top="5">
      <entity name="account">
        <attribute name="name" />
      </entity>
    </fetch>
    """

    calls = [
        ("records.list(account, top=5)", lambda: client.records.list("account", top=5)),
        (
            "tables.list(filter=IsPrivate...)",
            lambda: client.tables.list(
                filter="IsPrivate eq false",
                select=["LogicalName", "SchemaName"],
            ),
        ),
        ("tables.get(account)", lambda: client.tables.get("account")),
        ("query.sql(SELECT TOP 5 ...)", lambda: client.query.sql("SELECT TOP 5 name FROM account ORDER BY name")),
        ("query.fetchxml(...).execute()", lambda: client.query.fetchxml(fetchxml).execute()),
        (
            "query.builder(account).top(5).execute()",
            lambda: client.query.builder("account").select("name").top(5).execute(),
        ),
    ]

    all_ok = True
    for label, coro_fn in calls:
        _, elapsed_ms, ticks, gap_ms = await _with_canary(coro_fn)
        line, ok = _canary_line(label, elapsed_ms, ticks, gap_ms)
        print(line)
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("[OK] Event loop stayed unblocked across all read operations.")
    else:
        print(
            "[WARN] One or more calls produced a large tick gap.\n"
            "       Check for time.sleep, sync HTTP calls, or blocking file I/O."
        )


# ---------------------------------------------------------------------------
# Test 2: Read throughput — sequential vs concurrent
# ---------------------------------------------------------------------------


async def run_test2_read_throughput(client: AsyncDataverseClient, n: int) -> None:
    """
    Compare sequential vs concurrent execution for read operations.

    Expected speedup: 3-15x depending on network latency and server throttling.
    """
    heading(f"Test 2: Read Throughput — Sequential vs Concurrent  (N={n})")
    print(
        f"Each read operation is called {n} times sequentially, then\n"
        f"all {n} at once via asyncio.gather().\n"
        f"Expected: ≥{_WARN_SPEEDUP:.0f}x speedup (typically 3-15x on WAN).\n"
    )

    ops = [
        ("records.list(account, top=5)", lambda: client.records.list("account", top=5)),
        ("query.sql(SELECT TOP 5 ...)", lambda: client.query.sql("SELECT TOP 5 name FROM account ORDER BY name")),
        ("tables.get(account)", lambda: client.tables.get("account")),
    ]

    overall_seq = overall_conc = 0.0
    for label, coro_fn in ops:
        t0 = time.monotonic()
        for _ in range(n):
            await coro_fn()
        seq_s = time.monotonic() - t0

        t0 = time.monotonic()
        await asyncio.gather(*[coro_fn() for _ in range(n)])
        conc_s = time.monotonic() - t0

        line, speedup = _speedup_line(label, seq_s, conc_s)
        print(line)
        overall_seq += seq_s
        overall_conc += conc_s

    overall_speedup = overall_seq / overall_conc if overall_conc > 0 else float("inf")
    print(f"\n  Overall speedup: {overall_speedup:.1f}x")
    if overall_speedup >= _WARN_SPEEDUP:
        print("[OK] Read concurrency confirmed.")
    else:
        print(
            "[WARN] Low speedup. Possible causes: low latency environment,\n"
            "       server throttling, or accidental serialization."
        )


# ---------------------------------------------------------------------------
# Test 3: Write concurrency — POST path
# ---------------------------------------------------------------------------


async def run_test3_write_concurrency(client: AsyncDataverseClient, n: int) -> None:
    """
    Verify that write operations (POST) also benefit from concurrency.

    Creates N records sequentially then N records in parallel, compares
    elapsed time, and deletes all created records.

    The POST path has a 120 s default timeout (vs 10 s for GET), so it
    exercises a different code branch in _AsyncHttpClient.
    """
    heading(f"Test 3: Write Concurrency — Sequential vs Concurrent  (N={n})")
    print(
        f"Creates {n} contact records sequentially, then {n} concurrently.\n"
        f"All records are deleted at the end of this test.\n"
        f"Tests the POST path (different timeout branch from GET).\n"
    )

    tag = uuid.uuid4().hex[:8]

    def _payload(i: int, suffix: str) -> dict:
        return {
            "firstname": f"Bench{suffix}",
            "lastname": f"{tag}-{i}",
        }

    # Sequential creates
    seq_ids: List[str] = []
    t0 = time.monotonic()
    for i in range(n):
        rid = await client.records.create("contact", _payload(i, "Seq"))
        seq_ids.append(rid)
    seq_s = time.monotonic() - t0

    # Concurrent creates
    t0 = time.monotonic()
    conc_ids = list(await asyncio.gather(*[client.records.create("contact", _payload(i, "Con")) for i in range(n)]))
    conc_s = time.monotonic() - t0

    line, speedup = _speedup_line(f"records.create(contact) x{n}", seq_s, conc_s)
    print(line)

    # Cleanup — delete all created records concurrently
    all_ids = seq_ids + [rid for rid in conc_ids if rid]
    if all_ids:
        await client.records.delete("contact", all_ids)
        print(f"\n  [OK] Cleaned up {len(all_ids)} test records.")

    print()
    if speedup >= _WARN_SPEEDUP:
        print("[OK] Write concurrency (POST path) confirmed.")
    else:
        print("[WARN] Low write speedup — may indicate server throttling on contact creates.")


# ---------------------------------------------------------------------------
# Test 4: Pagination non-blocking
# ---------------------------------------------------------------------------


async def run_test4_pagination_nonblocking(client: AsyncDataverseClient) -> None:
    """
    Verify that async generators (list_pages, execute_pages) yield between
    page fetches, keeping the event loop free.

    The canary runs for the entire multi-page iteration. Between each page
    fetch the event loop is idle and the canary should tick. If pagination
    were implemented with blocking I/O or time.sleep, the canary would
    stop ticking between pages.
    """
    heading("Test 4: Pagination Non-Blocking (async generator canary)")
    print(
        "Runs a multi-page query while the canary ticks.\n"
        "The event loop should stay free between page fetches.\n"
        "Canary ticks between pages confirm yields at page boundaries.\n"
    )

    fetchxml = """
    <fetch count="5">
      <entity name="account">
        <attribute name="name" />
        <order attribute="name" />
      </entity>
    </fetch>
    """

    async def _paginate_records():
        pages = 0
        async for _page in client.records.list_pages("account", top=5, page_size=5):
            pages += 1
            if pages >= 3:
                break

    async def _paginate_fetchxml():
        pages = 0
        async for _page in client.query.fetchxml(fetchxml).execute_pages():
            pages += 1
            if pages >= 3:
                break

    async def _paginate_builder():
        pages = 0
        async for _page in client.query.builder("account").select("name").page_size(5).execute_pages():
            pages += 1
            if pages >= 3:
                break

    paginators = [
        ("records.list_pages(account, page_size=5)", _paginate_records),
        ("query.fetchxml(...).execute_pages()", _paginate_fetchxml),
        ("query.builder(...).execute_pages()", _paginate_builder),
    ]

    all_ok = True
    for label, coro_fn in paginators:
        _, elapsed_ms, ticks, gap_ms = await _with_canary(coro_fn)
        line, ok = _canary_line(label, elapsed_ms, ticks, gap_ms)
        print(line)
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("[OK] Async generators yield between pages — event loop stays free.")
    else:
        print("[WARN] Large gap detected during pagination — possible blocking between pages.")


# ---------------------------------------------------------------------------
# Test 5: Mixed fan-out (different operation types simultaneously)
# ---------------------------------------------------------------------------


async def run_test5_mixed_fanout(client: AsyncDataverseClient) -> None:
    """
    Fire different operation types concurrently in a single gather().

    Real applications mix reads, metadata queries, and SQL in parallel.
    This test confirms there is no cross-operation serialization —
    e.g. an internal lock that would cause records.list to wait for
    tables.get to finish before starting.
    """
    heading("Test 5: Mixed Fan-Out (different operations simultaneously)")
    print(
        "Fires records.list, tables.get, query.sql, query.fetchxml, and\n"
        "query.builder all at once in a single asyncio.gather().\n"
        "Verifies no cross-operation serialization exists in the SDK.\n"
    )

    fetchxml = """
    <fetch top="3">
      <entity name="account">
        <attribute name="name" />
      </entity>
    </fetch>
    """

    ops = {
        "records.list(account, top=3)": lambda: client.records.list("account", top=3),
        "tables.get(account)": lambda: client.tables.get("account"),
        "tables.get(contact)": lambda: client.tables.get("contact"),
        "query.sql(SELECT TOP 3 ...)": lambda: client.query.sql("SELECT TOP 3 name FROM account ORDER BY name"),
        "query.fetchxml(...).execute()": lambda: client.query.fetchxml(fetchxml).execute(),
        "query.builder(account).top(3).execute()": lambda: (
            client.query.builder("account").select("name").top(3).execute()
        ),
    }

    # Sequential baseline
    t0 = time.monotonic()
    for coro_fn in ops.values():
        await coro_fn()
    seq_s = time.monotonic() - t0

    # All at once
    t0 = time.monotonic()
    results = await asyncio.gather(*[fn() for fn in ops.values()])
    conc_s = time.monotonic() - t0

    speedup = seq_s / conc_s if conc_s > 0 else float("inf")
    status = "[OK]  " if speedup >= _WARN_SPEEDUP else "[WARN]"

    print(f"  {status} Mixed {len(ops)}-operation fan-out")
    print(f"         sequential={seq_s:.2f}s  concurrent={conc_s:.2f}s  speedup={speedup:.1f}x\n")

    for label, result in zip(ops.keys(), results):
        if isinstance(result, QueryResult):
            print(f"  [OK] {label}  → {len(result.records)} result(s)")
        elif isinstance(result, TableInfo):
            print(f"  [OK] {label}  → {result.schema_name}")
        elif isinstance(result, list):
            print(f"  [OK] {label}  → {len(result)} result(s)")
        elif result is None:
            print(f"  [OK] {label}  → None")
        else:
            print(f"  [OK] {label}  → {type(result).__name__}")

    print()
    if speedup >= _WARN_SPEEDUP:
        print("[OK] Mixed fan-out confirmed — no cross-operation serialization.")
    else:
        print("[WARN] Low mixed fan-out speedup.")


# ---------------------------------------------------------------------------
# Test 6: Error resilience in gather()
# ---------------------------------------------------------------------------


async def run_test6_error_resilience(client: AsyncDataverseClient) -> None:
    """
    Verify that one failing call in asyncio.gather() does not prevent
    the others from completing when return_exceptions=True is used.

    This is an important usage pattern: in a batch of N concurrent calls,
    a single 404 or throttle error should not discard the N-1 successful
    results. This test demonstrates and validates the correct pattern.
    """
    heading("Test 6: Error Resilience in gather(return_exceptions=True)")
    print(
        "Fires 5 calls: 3 valid records.list, 1 intentionally bad SQL,\n"
        "and 1 list against a nonexistent table.\n"
        "With return_exceptions=True the 3 good calls must complete\n"
        "and return results even though 2 calls fail.\n"
    )

    bad_sql = "SELECT name INVALID SYNTAX FROM account"
    nonexistent_table = "new_TableThatDefinitelyDoesNotExist_xyz987"

    coros = [
        client.records.list("account", top=3),  # good
        client.records.list("contact", top=3),  # good
        client.query.sql("SELECT TOP 3 name FROM account ORDER BY name"),  # good
        client.query.sql(bad_sql),  # bad — invalid SQL
        client.records.list(nonexistent_table, top=1),  # bad — table not found
    ]

    t0 = time.monotonic()
    results = await asyncio.gather(*coros, return_exceptions=True)
    elapsed_ms = (time.monotonic() - t0) * 1000

    succeeded = [r for r in results if not isinstance(r, BaseException)]
    failed = [r for r in results if isinstance(r, BaseException)]

    print(f"  Elapsed: {elapsed_ms:.0f}ms")
    print(f"  Succeeded: {len(succeeded)} / {len(results)}")
    print(f"  Failed:    {len(failed)} / {len(results)}\n")

    for i, r in enumerate(results):
        if isinstance(r, BaseException):
            print(f"  [ERR] Call {i+1}: {type(r).__name__}: {str(r)[:80]}")
        else:
            count = len(r) if hasattr(r, "__len__") else ("None" if r is None else "1")
            print(f"  [OK]  Call {i+1}: {count} result(s)")

    print()
    if len(succeeded) == 3 and len(failed) == 2:
        print("[OK] Error resilience confirmed — good calls completed despite failures.")
    else:
        print(f"[WARN] Expected 3 successes and 2 failures, got {len(succeeded)}/{len(failed)}.")


# ---------------------------------------------------------------------------
# Test 7: Real-world fan-out — metadata for multiple tables
# ---------------------------------------------------------------------------


async def run_test7_metadata_fanout(client: AsyncDataverseClient) -> None:
    """
    Fetch metadata for multiple tables simultaneously.

    The canonical real-world use case: an application needs schema info
    for several tables at startup. Sequential is simple but slow;
    concurrent fan-out is fast and equally readable with gather().
    """
    heading("Test 7: Real-World Metadata Fan-Out")
    print(
        "Fetch metadata for 6 built-in tables: first sequentially,\n"
        "then all at once. This is the canonical real-world async pattern.\n"
    )

    tables = ["account", "contact", "lead", "opportunity", "systemuser", "task"]

    t0 = time.monotonic()
    for t in tables:
        await client.tables.get(t)
    seq_s = time.monotonic() - t0

    t0 = time.monotonic()
    results = await asyncio.gather(*[client.tables.get(t) for t in tables])
    conc_s = time.monotonic() - t0

    speedup = seq_s / conc_s if conc_s > 0 else float("inf")
    status = "[OK]  " if speedup >= _WARN_SPEEDUP else "[WARN]"

    print(f"  {status} tables.get() x{len(tables)}")
    print(f"         sequential={seq_s:.2f}s  concurrent={conc_s:.2f}s  speedup={speedup:.1f}x\n")

    for info in results:
        if info:
            print(f"  [OK] {info.get('schema_name', '?'):20s}  entity_set={info.get('entity_set_name', '?')}")

    print()
    if speedup >= _WARN_SPEEDUP:
        print(f"[OK] Metadata fan-out speedup: {speedup:.1f}x")
    else:
        print(f"[WARN] Low metadata fan-out speedup: {speedup:.1f}x")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    print(SEPARATOR)
    print("Dataverse SDK - Async Concurrency Benchmark & Validation")
    print(SEPARATOR)
    print(
        "\nValidates 7 properties of the async client:\n"
        "  1. Non-blocking reads      — GET calls yield to event loop\n"
        "  2. Read throughput         — concurrent reads beat sequential\n"
        "  3. Write concurrency       — concurrent POSTs beat sequential\n"
        "  4. Pagination non-blocking — async generators yield between pages\n"
        "  5. Mixed fan-out           — different op types run simultaneously\n"
        "  6. Error resilience        — one failure doesn't kill other calls\n"
        "  7. Real-world fan-out      — metadata for multiple tables in parallel\n"
        "\nTip: run with PYTHONASYNCIODEBUG=1 to catch any remaining blocking calls.\n"
    )

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    n_str = input("Calls per operation for throughput tests [default: 8]: ").strip()
    n = int(n_str) if n_str.isdigit() and int(n_str) > 0 else 8

    credential = AsyncInteractiveBrowserCredential()
    try:
        async with AsyncDataverseClient(base_url=base_url, credential=credential) as client:
            print("\nWarming up (first call triggers auth + connection)...")
            await client.records.list("account", top=1)
            print("[OK] Warm-up complete.")

            await run_test1_nonblocking_reads(client)
            await run_test2_read_throughput(client, n=n)
            await run_test3_write_concurrency(client, n=n)
            await run_test4_pagination_nonblocking(client)
            await run_test5_mixed_fanout(client)
            await run_test6_error_resilience(client)
            await run_test7_metadata_fanout(client)

            print(f"\n{SEPARATOR}")
            print("Benchmark complete.")
            print(SEPARATOR)
    finally:
        await credential.close()


if __name__ == "__main__":
    asyncio.run(main())
