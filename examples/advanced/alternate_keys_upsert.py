#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Alternate Keys & Upsert Example

Demonstrates the full workflow of creating alternate keys and using
them for upsert operations:
1. Create a custom table with columns
2. Define an alternate key on a column
3. Wait for the key index to become Active
4. Upsert records using the alternate key
5. Verify records were created/updated correctly
6. Clean up

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity
"""

import sys
import time

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from azure.identity import InteractiveBrowserCredential  # type: ignore

# --- Config ---
TABLE_NAME = "new_AltKeyDemo"
KEY_COLUMN = "new_externalid"
KEY_NAME = "new_ExternalIdKey"
BACKOFF_DELAYS = (0, 3, 10, 20, 35)


# --- Helpers ---
def backoff(op, *, delays=BACKOFF_DELAYS):
    """Retry *op* with exponential-ish backoff on any exception."""
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
                print(f"   [INFO] Backoff succeeded after {retry_count} retry(s); " f"waited {total_delay}s total.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(f"   [WARN] Backoff exhausted after {retry_count} retry(s); " f"waited {total_delay}s total.")
        raise last


def wait_for_key_active(client, table, key_name, max_wait=120):
    """Poll get_alternate_keys until the key status is Active."""
    start = time.time()
    while time.time() - start < max_wait:
        keys = client.tables.get_alternate_keys(table)
        for k in keys:
            if k.schema_name == key_name:
                print(f"  Key status: {k.status}")
                if k.status == "Active":
                    return k
                if k.status == "Failed":
                    raise RuntimeError(f"Alternate key index failed: {k.schema_name}")
        time.sleep(5)
    raise TimeoutError(f"Key {key_name} did not become Active within {max_wait}s")


# --- Main ---
def main():
    """Run the alternate-keys & upsert E2E walkthrough."""
    print("PowerPlatform Dataverse Client - Alternate Keys & Upsert Example")
    print("=" * 70)
    print("This script demonstrates:")
    print("  - Creating a custom table with columns")
    print("  - Defining an alternate key on a column")
    print("  - Waiting for the key index to become Active")
    print("  - Upserting records via alternate key (create + update)")
    print("  - Verifying records and listing keys")
    print("  - Cleaning up (delete key, delete table)")
    print("=" * 70)

    entered = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not entered:
        print("No URL entered; exiting.")
        sys.exit(1)

    base_url = entered.rstrip("/")
    credential = InteractiveBrowserCredential()
    client = DataverseClient(base_url, credential)

    # ------------------------------------------------------------------
    # Step 1: Create table
    # ------------------------------------------------------------------
    print("\n1. Creating table...")
    table_info = backoff(
        lambda: client.tables.create(
            TABLE_NAME,
            columns={
                KEY_COLUMN: "string",
                "new_ProductName": "string",
                "new_Price": "decimal",
            },
        )
    )
    print(f"   Created: {table_info.get('table_schema_name', TABLE_NAME)}")

    time.sleep(10)  # Wait for metadata propagation

    # ------------------------------------------------------------------
    # Step 2: Create alternate key
    # ------------------------------------------------------------------
    print("\n2. Creating alternate key...")
    key_info = backoff(lambda: client.tables.create_alternate_key(TABLE_NAME, KEY_NAME, [KEY_COLUMN.lower()]))
    print(f"   Key created: {key_info.schema_name} (id={key_info.metadata_id})")

    # ------------------------------------------------------------------
    # Step 3: Wait for key to become Active
    # ------------------------------------------------------------------
    print("\n3. Waiting for key index to become Active...")
    active_key = wait_for_key_active(client, TABLE_NAME, KEY_NAME)
    print(f"   Key is Active: {active_key.schema_name}")

    # ------------------------------------------------------------------
    # Step 4: Upsert records (creates new)
    # ------------------------------------------------------------------
    print("\n4a. Upsert single record (PATCH, creates new)...")
    client.records.upsert(
        TABLE_NAME,
        [
            UpsertItem(
                alternate_key={KEY_COLUMN.lower(): "EXT-001"},
                record={"new_productname": "Widget A", "new_price": 9.99},
            ),
        ],
    )
    print("   Upserted EXT-001 (single)")

    print("\n4b. Upsert second record (single PATCH)...")
    client.records.upsert(
        TABLE_NAME,
        [
            UpsertItem(
                alternate_key={KEY_COLUMN.lower(): "EXT-002"},
                record={"new_productname": "Widget B", "new_price": 19.99},
            ),
        ],
    )
    print("   Upserted EXT-002 (single)")

    # ------------------------------------------------------------------
    # Step 5: Upsert again (updates existing via single PATCH)
    # ------------------------------------------------------------------
    print("\n5. Upserting record (update existing via PATCH)...")
    client.records.upsert(
        TABLE_NAME,
        [
            UpsertItem(
                alternate_key={KEY_COLUMN.lower(): "EXT-001"},
                record={"new_productname": "Widget A v2", "new_price": 12.99},
            ),
        ],
    )
    print("   Updated EXT-001")

    # ------------------------------------------------------------------
    # Step 6: Verify
    # ------------------------------------------------------------------
    print("\n6. Verifying records...")
    for page in client.records.get(
        TABLE_NAME,
        select=["new_productname", "new_price", KEY_COLUMN.lower()],
    ):
        for record in page:
            ext_id = record.get(KEY_COLUMN.lower(), "?")
            name = record.get("new_productname", "?")
            price = record.get("new_price", "?")
            print(f"   {ext_id}: {name} @ ${price}")

    # ------------------------------------------------------------------
    # Step 7: List alternate keys
    # ------------------------------------------------------------------
    print("\n7. Listing alternate keys...")
    keys = client.tables.get_alternate_keys(TABLE_NAME)
    for k in keys:
        print(f"   {k.schema_name}: columns={k.key_attributes}, status={k.status}")

    # ------------------------------------------------------------------
    # Step 8: Cleanup
    # ------------------------------------------------------------------
    cleanup = input("\n8. Delete table and cleanup? (Y/n): ").strip() or "y"
    if cleanup.lower() in ("y", "yes"):
        try:
            # Delete alternate key first
            for k in keys:
                client.tables.delete_alternate_key(TABLE_NAME, k.metadata_id)
                print(f"   Deleted key: {k.schema_name}")
            time.sleep(5)
            backoff(lambda: client.tables.delete(TABLE_NAME))
            print(f"   Deleted table: {TABLE_NAME}")
        except Exception as e:  # noqa: BLE001
            print(f"   Cleanup error: {e}")
    else:
        print("   Table kept for inspection.")

    print("\nDone.")


if __name__ == "__main__":
    main()
