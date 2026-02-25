# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Batch operations example for the Dataverse Python SDK.

Demonstrates how to use client.batch to send multiple operations in a single
HTTP request to the Dataverse Web API.

Requirements:
    pip install "PowerPlatform.Dataverse" azure-identity
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Setup — replace with your environment URL and credential
# ---------------------------------------------------------------------------

from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient

credential = InteractiveBrowserCredential()
client = DataverseClient("https://org.crm.dynamics.com", credential)


# ---------------------------------------------------------------------------
# Example 1: Record CRUD in a single batch
# ---------------------------------------------------------------------------

print("\n[INFO] Example 1: Record CRUD in a single batch")

batch = client.batch.new()

# Create a single record
batch.records.create("account", {"name": "Contoso Ltd", "telephone1": "555-0100"})

# Create multiple records via CreateMultiple (one batch item)
batch.records.create(
    "contact",
    [
        {"firstname": "Alice", "lastname": "Smith"},
        {"firstname": "Bob", "lastname": "Jones"},
    ],
)

# Assume we have an existing account_id from a prior operation
# batch.records.update("account", account_id, {"telephone1": "555-9999"})
# batch.records.delete("account", old_id)

result = batch.execute()

print(f"[OK] Total: {len(result.responses)}, Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
for guid in result.created_ids:
    print(f"[OK] Created: {guid}")
for item in result.failed:
    print(f"[ERR] {item.status_code}: {item.error_message}")


# ---------------------------------------------------------------------------
# Example 2: Transactional changeset with content-ID chaining
# ---------------------------------------------------------------------------

print("\n[INFO] Example 2: Transactional changeset")

batch = client.batch.new()

with batch.changeset() as cs:
    # Each create() returns a "$n" reference usable in subsequent operations
    lead_ref = cs.records.create(
        "lead",
        {"firstname": "Ada", "lastname": "Lovelace"},
    )
    contact_ref = cs.records.create("contact", {"firstname": "Ada"})

    # Reference the newly created lead and contact in the account
    cs.records.create(
        "account",
        {
            "name": "Babbage & Co.",
            "originatingleadid@odata.bind": lead_ref,
            "primarycontactid@odata.bind": contact_ref,
        },
    )

    # Update using a content-ID reference as the record_id
    cs.records.update("contact", contact_ref, {"lastname": "Lovelace"})

result = batch.execute()

if result.has_errors:
    print("[ERR] Changeset rolled back")
    for item in result.failed:
        print(f"  {item.status_code}: {item.error_message}")
else:
    print(f"[OK] {len(result.created_ids)} records created atomically")


# ---------------------------------------------------------------------------
# Example 3: Table metadata operations in a batch
# ---------------------------------------------------------------------------

print("\n[INFO] Example 3: Table metadata operations")

batch = client.batch.new()

# Create a new custom table
batch.tables.create(
    "new_Product",
    {"new_Price": "decimal", "new_InStock": "bool"},
    solution="MySolution",
)

# Read table metadata
batch.tables.get("new_Product")

# List all non-private tables
batch.tables.list()

result = batch.execute()
print(f"[OK] Table ops: {[(r.status_code, r.is_success) for r in result.responses]}")


# ---------------------------------------------------------------------------
# Example 4: SQL query in a batch
# ---------------------------------------------------------------------------

print("\n[INFO] Example 4: SQL query in batch")

batch = client.batch.new()
batch.query.sql("SELECT TOP 5 accountid, name FROM account ORDER BY name")

result = batch.execute()
if result.responses and result.responses[0].is_success and result.responses[0].body:
    rows = result.responses[0].body.get("value", [])
    print(f"[OK] Retrieved {len(rows)} accounts")
    for row in rows:
        print(f"  {row.get('name')}")


# ---------------------------------------------------------------------------
# Example 5: Mixed batch — changeset writes + standalone GETs
# ---------------------------------------------------------------------------

print("\n[INFO] Example 5: Mixed batch")

# Assume account_id exists
# batch = client.batch.new()
#
# with batch.changeset() as cs:
#     cs.records.update("account", account_id, {"statecode": 0})
#
# batch.records.get("account", account_id, select=["name", "statecode"])
#
# result = batch.execute()
# update_response = result.responses[0]
# account_data = result.responses[1]
# if account_data.is_success and account_data.body:
#     print(f"Account: {account_data.body.get('name')}")


# ---------------------------------------------------------------------------
# Example 6: Continue on error
# ---------------------------------------------------------------------------

print("\n[INFO] Example 6: Continue on error")

batch = client.batch.new()
batch.records.get("account", "nonexistent-guid-1111-1111-111111111111")
batch.query.sql("SELECT TOP 1 name FROM account")

result = batch.execute(continue_on_error=True)
print(f"[OK] Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
for item in result.failed:
    print(f"[ERR] {item.status_code}: {item.error_message}")
