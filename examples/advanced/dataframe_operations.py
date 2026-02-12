# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - DataFrame Operations Walkthrough

This example demonstrates how to use the pandas DataFrame extension methods
for CRUD operations with Microsoft Dataverse.

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity
"""

import sys
import uuid

import pandas as pd
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient


def main():
    # ── Setup & Authentication ────────────────────────────────────
    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("[ERR] No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    print("[INFO] Authenticating via browser...")
    credential = InteractiveBrowserCredential()
    client = DataverseClient(base_url, credential)

    table = input("Enter table schema name to use [default: account]: ").strip() or "account"
    print(f"[INFO] Using table: {table}")

    # Unique tag to isolate test records from existing data
    tag = uuid.uuid4().hex[:8]
    test_filter = f"contains(name,'{tag}')"
    print(f"[INFO] Using tag '{tag}' to identify test records")

    select_cols = ["name", "telephone1", "websiteurl", "lastonholdtime"]

    # ── 1. Create records from a DataFrame ────────────────────────
    print("\n" + "-" * 60)
    print("1. Create records from a DataFrame")
    print("-" * 60)

    new_accounts = pd.DataFrame([
        {"name": f"Contoso_{tag}", "telephone1": "555-0100", "websiteurl": "https://contoso.com",
         "lastonholdtime": pd.Timestamp("2024-06-15 10:30:00")},
        {"name": f"Fabrikam_{tag}", "telephone1": "555-0200", "websiteurl": None,
         "lastonholdtime": None},
        {"name": f"Northwind_{tag}", "telephone1": None, "websiteurl": "https://northwind.com",
         "lastonholdtime": pd.Timestamp("2024-12-01 08:00:00")},
    ])
    print(f"  Input DataFrame:\n{new_accounts.to_string(index=False)}\n")

    # create_dataframe returns a Series of GUIDs aligned with the input rows
    new_accounts["accountid"] = client.create_dataframe(table, new_accounts)
    print(f"[OK] Created {len(new_accounts)} records")
    print(f"  IDs: {new_accounts['accountid'].tolist()}")

    # ── 2. Query records as paged DataFrames ──────────────────────
    print("\n" + "-" * 60)
    print("2. Query records as paged DataFrames (lazy generator)")
    print("-" * 60)

    page_count = 0
    for df_page in client.get_dataframe(table, select=select_cols, filter=test_filter, page_size=2):
        page_count += 1
        print(f"  Page {page_count} ({len(df_page)} records):\n{df_page.to_string(index=False)}")

    # ── 3. Collect all pages into one DataFrame ───────────────────
    print("\n" + "-" * 60)
    print("3. Collect all pages into one DataFrame with pd.concat")
    print("-" * 60)

    all_records = pd.concat(
        client.get_dataframe(table, select=select_cols, filter=test_filter, page_size=2),
        ignore_index=True,
    )
    print(f"[OK] Got {len(all_records)} total records in one DataFrame")
    print(f"  Columns: {list(all_records.columns)}")
    print(f"{all_records.to_string(index=False)}")

    # ── 4. Fetch a single record by ID ────────────────────────────
    print("\n" + "-" * 60)
    print("4. Fetch a single record by ID")
    print("-" * 60)

    first_id = new_accounts["accountid"].iloc[0]
    print(f"  Fetching record {first_id}...")
    single = client.get_dataframe(table, record_id=first_id, select=select_cols)
    print(f"[OK] Single record DataFrame:\n{single.to_string(index=False)}")

    # ── 5. Update records from a DataFrame ────────────────────────
    print("\n" + "-" * 60)
    print("5. Update records with different values per row")
    print("-" * 60)

    new_accounts["telephone1"] = ["555-1100", "555-1200", "555-1300"]
    print(f"  New telephone numbers: {new_accounts['telephone1'].tolist()}")
    client.update_dataframe(table, new_accounts[["accountid", "telephone1"]], id_column="accountid")
    print("[OK] Updated 3 records")

    # Verify the updates with a bulk get
    verified = next(client.get_dataframe(table, select=select_cols, filter=test_filter))
    print(f"  Verified:\n{verified.to_string(index=False)}")

    # ── 6. Broadcast update (same value to all records) ───────────
    print("\n" + "-" * 60)
    print("6. Broadcast update (same value to all records)")
    print("-" * 60)

    broadcast_df = new_accounts[["accountid"]].copy()
    broadcast_df["websiteurl"] = "https://updated.example.com"
    print(f"  Setting websiteurl to 'https://updated.example.com' for all {len(broadcast_df)} records")
    client.update_dataframe(table, broadcast_df, id_column="accountid")
    print("[OK] Broadcast update complete")

    # Verify all records have the same websiteurl
    verified = next(client.get_dataframe(table, select=select_cols, filter=test_filter))
    print(f"  Verified:\n{verified.to_string(index=False)}")

    # ── 7. Delete records by passing a Series of GUIDs ────────────
    print("\n" + "-" * 60)
    print("7. Delete records by passing a Series of GUIDs")
    print("-" * 60)

    print(f"  Deleting {len(new_accounts)} records...")
    client.delete_dataframe(table, new_accounts["accountid"], use_bulk_delete=False)
    print(f"[OK] Deleted {len(new_accounts)} records")

    # Verify deletions - filter for our tagged records should return 0
    remaining = list(client.get_dataframe(table, select=select_cols, filter=test_filter))
    count = sum(len(page) for page in remaining)
    print(f"  Verified: {count} test records remaining (expected 0)")

    print("\n" + "=" * 60)
    print("[OK] DataFrame operations walkthrough complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
