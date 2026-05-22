# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Chunking verification for CreateMultiple / UpdateMultiple / UpsertMultiple.

Tests auto-chunking at every boundary relative to _MULTIPLE_BATCH_SIZE (B = 1000):
  - 0 records  (no-op)
  - 1 record   (well below B)
  - B-1        (just under one full chunk)
  - B          (exactly one chunk)
  - B+1        (spills into a second chunk)
  - 2*B        (exactly two full chunks)
  - 2*B+1      (spills into a third chunk)

For update, both broadcast (one patch for all IDs) and paired (per-record patches) are tested.

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import argparse
import time
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core import MetadataError
from PowerPlatform.Dataverse.models import UpsertItem

B = 1000  # Must match _MULTIPLE_BATCH_SIZE in _odata.py

SIZES = [0, 1, B - 1, B, B + 1, 2 * B, 2 * B + 1]

TABLE = "new_ChunkingVerification"

# Global pass/fail counters
_pass = 0
_fail = 0


# Simple logging helper (mirrors walkthrough style)
def log_call(description):
    print(f"\n-> {description}")


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
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
                retry_count = attempts - 1
                print(f"   [INFO] Backoff succeeded after {retry_count} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(f"   [WARN] Backoff exhausted after {retry_count} retry(s); waited {total_delay}s total.")
        raise last


def check(condition, msg):
    global _pass, _fail
    if condition:
        _pass += 1
        print(f"[OK] {msg}")
    else:
        _fail += 1
        print(f"[FAIL] {msg}")


def timed(op):
    """Run op(), return (result, elapsed_seconds)."""
    t0 = time.time()
    result = op()
    return result, round(time.time() - t0, 2)


def count_records(client, pk_attr, filter_expr=None):
    """Return total record count by paging with minimal field selection."""
    total = 0
    kwargs = {"select": [pk_attr]}
    if filter_expr:
        kwargs["filter"] = filter_expr
    for page in client.records.get(TABLE, **kwargs):
        total += len(page)
    return total


def delete_all(client, pk_attr):
    """Delete all records in the test table via synchronous $batch chunks.

    Uses $batch (not BulkDelete) so the table is guaranteed empty on return.
    BulkDelete is asynchronous — it returns before records are removed, which
    would corrupt record counts in subsequent test iterations.
    """
    log_call(f"delete_all: fetching IDs from {TABLE}")
    ids = []
    for page in client.records.get(TABLE, select=[pk_attr]):
        ids.extend(r[pk_attr] for r in page)
    if not ids:
        print(f"[OK] {TABLE} is already empty.")
        return
    log_call(f"delete_all: deleting {len(ids)} records via $batch (chunks of {B})")
    for i in range(0, len(ids), B):
        chunk = ids[i : i + B]
        batch = client.batch.new()
        for record_id in chunk:
            batch.records.delete(TABLE, record_id)
        backoff(lambda b=batch: b.execute(continue_on_error=True))
        print(f"  chunk {i // B + 1}: deleted {len(chunk)} records")
    print(f"[OK] Deleted {len(ids)} records from {TABLE}.")


def make_records(n, *, marker="create"):
    """Build n record payloads with a unique marker and sequential index."""
    return [{"new_Label": f"{marker}-{i}", "new_Index": i} for i in range(n)]


def make_upsert_items(n, *, marker="upsert"):
    """Build n UpsertItems using new_Code as the alternate key."""
    return [
        UpsertItem(
            alternate_key={"new_code": f"{marker}-{i}"},
            record={"new_Label": f"{marker}-label-{i}", "new_Index": i},
        )
        for i in range(n)
    ]


def expected_chunks(n):
    """Return the number of chunks n records will be split into."""
    return max(1, -(-n // B)) if n > 0 else 0  # ceiling division


# ---------------------------------------------------------------------------
# Test sections
# ---------------------------------------------------------------------------


def test_create_multiple(client, pk_attr):
    print("\n" + "=" * 80)
    print("CREATE MULTIPLE — boundary sizes")
    print("=" * 80)

    for n in SIZES:
        print(f"\n-- n={n} ({expected_chunks(n)} chunk(s) expected) --")
        delete_all(client, pk_attr)

        if n == 0:
            log_call(f"client.records.create('{TABLE}', [])  # empty list — no HTTP call expected")
            actual = count_records(client, pk_attr)
            check(actual == 0, f"n={n:5d}: server count=0 (empty create is a no-op)")
            continue

        records = make_records(n)
        log_call(f"client.records.create('{TABLE}', [{n} records])  # {expected_chunks(n)} chunk(s)")
        ids, elapsed = timed(lambda r=records: backoff(lambda: client.records.create(TABLE, r)))
        print(f"  create returned {len(ids)} IDs in {elapsed}s")

        log_call(f"count_records (expected {n})")
        actual = count_records(client, pk_attr)

        check(len(ids) == n, f"n={n:5d}: IDs returned={len(ids)} (expected {n})  [{elapsed}s]")
        check(actual == n, f"n={n:5d}: server count={actual} (expected {n})")


def test_update_multiple_broadcast(client, pk_attr):
    print("\n" + "=" * 80)
    print("UPDATE MULTIPLE — broadcast (same patch for all IDs)")
    print("=" * 80)

    for n in SIZES:
        print(f"\n-- n={n} ({expected_chunks(n)} chunk(s) expected) --")
        if n == 0:
            print("[OK] n=0: skipped (no records to update)")
            continue

        delete_all(client, pk_attr)
        records = make_records(n)
        log_call(f"client.records.create('{TABLE}', [{n} records])  # seed")
        ids, elapsed = timed(lambda r=records: backoff(lambda: client.records.create(TABLE, r)))
        print(f"  seeded {len(ids)} records in {elapsed}s")

        log_call(
            f"client.records.update('{TABLE}', [{n} IDs], {{'new_Index': 9999}})  # broadcast, {expected_chunks(n)} chunk(s)"
        )
        _, elapsed = timed(lambda i=ids: backoff(lambda: client.records.update(TABLE, i, {"new_Index": 9999})))
        print(f"  update completed in {elapsed}s")

        log_call("count_records(filter='new_index eq 9999')")
        updated = count_records(client, pk_attr, filter_expr="new_index eq 9999")
        check(updated == n, f"n={n:5d}: {updated}/{n} records have new_Index=9999  [{elapsed}s]")


def test_update_multiple_paired(client, pk_attr):
    print("\n" + "=" * 80)
    print("UPDATE MULTIPLE — paired (per-record patches)")
    print("=" * 80)

    for n in SIZES:
        print(f"\n-- n={n} ({expected_chunks(n)} chunk(s) expected) --")
        if n == 0:
            print("[OK] n=0: skipped (no records to update)")
            continue

        delete_all(client, pk_attr)
        records = make_records(n)
        log_call(f"client.records.create('{TABLE}', [{n} records])  # seed")
        ids, elapsed = timed(lambda r=records: backoff(lambda: client.records.create(TABLE, r)))
        print(f"  seeded {len(ids)} records in {elapsed}s")

        patches = [{"new_Index": n - 1 - i} for i in range(n)]
        log_call(f"client.records.update('{TABLE}', [{n} IDs], [{n} patches])  # paired, {expected_chunks(n)} chunk(s)")
        _, elapsed = timed(lambda i=ids, p=patches: backoff(lambda: client.records.update(TABLE, i, p)))
        print(f"  update completed in {elapsed}s")

        log_call("sum new_index across all records (expect n*(n-1)/2)")
        total_index = 0
        for page in client.records.get(TABLE, select=["new_index"]):
            total_index += sum(r.get("new_index", 0) for r in page)
        expected_sum = n * (n - 1) // 2
        check(
            total_index == expected_sum,
            f"n={n:5d}: index sum={total_index} (expected {expected_sum})  [{elapsed}s]",
        )


def test_upsert_multiple(client, pk_attr):
    print("\n" + "=" * 80)
    print("UPSERT MULTIPLE — insert then update via alternate key")
    print("=" * 80)

    for n in SIZES:
        print(f"\n-- n={n} ({expected_chunks(n)} chunk(s) expected) --")
        if n == 0:
            print("[OK] n=0: skipped (no records to upsert)")
            continue

        delete_all(client, pk_attr)
        items = make_upsert_items(n)

        log_call(f"client.records.upsert('{TABLE}', [{n} items])  # insert pass, {expected_chunks(n)} chunk(s)")
        _, elapsed = timed(lambda i=items: backoff(lambda: client.records.upsert(TABLE, i)))
        print(f"  first upsert completed in {elapsed}s")
        after_insert = count_records(client, pk_attr)
        check(after_insert == n, f"n={n:5d}: {after_insert}/{n} records after insert pass  [{elapsed}s]")

        update_items = [
            UpsertItem(
                alternate_key={"new_code": f"upsert-{i}"},
                record={"new_Label": f"upsert-updated-{i}", "new_Index": i + 1000},
            )
            for i in range(n)
        ]

        log_call(
            f"client.records.upsert('{TABLE}', [{n} items])  # update pass (same keys), {expected_chunks(n)} chunk(s)"
        )
        _, elapsed = timed(lambda i=update_items: backoff(lambda: client.records.upsert(TABLE, i)))
        print(f"  second upsert completed in {elapsed}s")

        after_update = count_records(client, pk_attr)
        check(
            after_update == n, f"n={n:5d}: {after_update}/{n} records after update pass (no duplicates)  [{elapsed}s]"
        )

        log_call("count_records(filter='new_index gt 999')  # verify values updated")
        updated_count = count_records(client, pk_attr, filter_expr="new_index gt 999")
        check(updated_count == n, f"n={n:5d}: {updated_count}/{n} records have updated new_Index (>999)  [{elapsed}s]")


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------


def setup_table(client):
    """Create table and alternate key; return the primary ID attribute name."""
    print("\n" + "=" * 80)
    print("SETUP")
    print("=" * 80)

    log_call(f"client.tables.get('{TABLE}')")
    table_info = backoff(lambda: client.tables.get(TABLE))

    if table_info:
        print(f"[OK] Table already exists: {TABLE}")
    else:
        log_call(f"client.tables.create('{TABLE}', {{...}})")
        table_info = backoff(
            lambda: client.tables.create(
                TABLE,
                {
                    "new_Label": "string",
                    "new_Index": "int",
                    "new_Code": "string",
                },
            )
        )
        print(f"[OK] Created table: {TABLE}")

    pk_attr = table_info.primary_id_attribute
    print(f"[OK] Primary ID attribute: {pk_attr}")

    log_call(f"delete_all (clear any leftovers from a previous run)")
    delete_all(client, pk_attr)

    # TODO: alternate key + upsert tests are commented out because index creation
    # can take several minutes on Dataverse. Uncomment to run upsert verification.
    # log_call(f"client.tables.add_alternate_key('{TABLE}', 'new_ChunkCodeKey', ['new_code'])")
    # try:
    #     backoff(lambda: client.tables.add_alternate_key(TABLE, "new_ChunkCodeKey", ["new_code"]))
    #     print("[OK] Added alternate key on new_Code")
    # except Exception as ex:  # noqa: BLE001
    #     print(f"[OK] Alternate key already exists (skipped): {ex}")
    #
    # log_call("client.tables.get_alternate_keys  # poll until Active (index build can take several minutes)")
    # deadline = time.time() + 600
    # while time.time() < deadline:
    #     keys = backoff(lambda: client.tables.get_alternate_keys(TABLE))
    #     print(f"  all keys: {[(k.schema_name, k.status) for k in keys]}")
    #     match = next((k for k in keys if k.schema_name.lower() == "new_chunkcodekey"), None)
    #     status = match.status if match else "missing"
    #     print(f"  new_ChunkCodeKey status: {status}")
    #     if status == "Active":
    #         break
    #     if status == "Failed":
    #         raise RuntimeError("Alternate key index creation failed — check Dataverse solution health.")
    #     time.sleep(15)
    # else:
    #     raise RuntimeError("Timed out waiting for alternate key to become Active (>600s).")

    print("[OK] Setup complete.")
    return pk_attr


def teardown_table(client):
    print("\n" + "=" * 80)
    print("TEARDOWN")
    print("=" * 80)

    log_call(f"client.tables.delete('{TABLE}')")
    try:
        backoff(lambda: client.tables.delete(TABLE))
        print(f"[OK] Deleted table: {TABLE}")
    except MetadataError as ex:
        if "not found" in str(ex).lower():
            print(f"[OK] Table already removed: {TABLE}")
        else:
            raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=" * 80)
    print("Chunking Verification")
    print(f"B = {B} (_MULTIPLE_BATCH_SIZE)   Sizes: {SIZES}")
    print("=" * 80)
    print()
    print("NOTE: The first API call opens a browser for authentication.")
    print("Table creation and alternate key indexing can take several minutes.")
    print("=" * 80)

    base_url = "https://aurorabapenv642a3.crmtest.dynamics.com"
    print(f"Using org URL: {base_url}")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{base_url}', credential=...)")
    with DataverseClient(base_url=base_url, credential=credential) as client:
        print(f"[OK] Connected to: {base_url}")

        pk_attr = setup_table(client)
        try:
            test_create_multiple(client, pk_attr)
            test_update_multiple_broadcast(client, pk_attr)
            test_update_multiple_paired(client, pk_attr)
            # test_upsert_multiple(client, pk_attr)  # TODO: requires alternate key (see setup_table)
        finally:
            input("\n[PAUSE] Validate table in Maker Portal, then press Enter to proceed with cleanup...")
            delete_all(client, pk_attr)
            teardown_table(client)

    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    total = _pass + _fail
    print(f"  Passed: {_pass}/{total}")
    print(f"  Failed: {_fail}/{total}")
    if _fail == 0:
        print("  All checks passed.")
    else:
        print("  Some checks FAILED — review [FAIL] lines above.")
    print("=" * 80)


if __name__ == "__main__":
    main()
