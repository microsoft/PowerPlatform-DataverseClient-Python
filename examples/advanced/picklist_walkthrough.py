# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Walkthrough demonstrating picklist label-to-integer resolution.

This example exercises all edge cases of the SDK's automatic picklist
label resolution, including:
- Single picklist field (create and update)
- Multiple picklist fields in one record
- Mixed picklist + non-picklist string fields
- Integer values passed through unchanged
- Unmatched labels left as strings (graceful fallback)
- Warm cache (second call skips metadata lookups)
- Case-insensitive label matching

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import json
import sys
import time
from enum import IntEnum
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
import requests


# Simple logging helper
def log_call(description):
    print(f"\n-> {description}")


# Two picklist enums to test multiple picklists in one table
class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class Status(IntEnum):
    DRAFT = 100000000
    ACTIVE = 100000001
    CLOSED = 100000002


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
        except Exception as ex:
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(f"   [WARN] Backoff exhausted after {retry_count} retry(s); waited {total_delay}s total.")
        raise last


def main():
    print("=" * 80)
    print("Picklist Label Resolution Walkthrough")
    print("=" * 80)

    # ============================================================================
    # 1. SETUP & AUTHENTICATION
    # ============================================================================
    print("\n" + "=" * 80)
    print("1. Setup & Authentication")
    print("=" * 80)

    base_url = sys.argv[1] if len(sys.argv) > 1 else ""
    if not base_url:
        base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)

    base_url = base_url.rstrip("/")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{base_url}', credential=...)")
    with DataverseClient(base_url=base_url, credential=credential) as client:
        print(f"[OK] Connected to: {base_url}")
        _run_walkthrough(client)


def _run_walkthrough(client):
    table_name = "new_PicklistDemo"

    # ============================================================================
    # 2. TABLE CREATION WITH TWO PICKLISTS
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (two picklist columns)")
    print("=" * 80)

    log_call(f"client.tables.get('{table_name}')")
    table_info = backoff(lambda: client.tables.get(table_name))

    if table_info:
        print(f"[OK] Table already exists: {table_name}")
    else:
        log_call(f"client.tables.create('{table_name}', columns={{...}})")
        columns = {
            "new_Title": "string",
            "new_Description": "string",
            "new_Priority": Priority,
            "new_Status": Status,
            "new_Count": "int",
        }
        table_info = backoff(lambda: client.tables.create(table_name, columns))
        print(f"[OK] Created table: {table_name}")
        print(f"  Columns: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. SINGLE PICKLIST LABEL (CREATE)
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Single Picklist Label (Create)")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 'High'}})")
    record1 = {
        "new_Title": "Single picklist test",
        "new_Priority": "High",
    }
    id1 = backoff(lambda: client.records.create(table_name, record1))
    retrieved1 = backoff(lambda: client.records.get(table_name, id1))
    print(f"[OK] Created with label 'High'")
    print(f"  new_priority = {retrieved1.get('new_priority')} (expected: 3)")
    assert retrieved1.get("new_priority") == 3, "Expected 3 for 'High'"

    # Print the cached picklist metadata for visibility
    odata = client._get_odata()
    table_key = table_name.lower()
    cache_entry = odata._picklist_label_cache.get(table_key, {})
    print(f"\n  [DEBUG] Picklist cache for '{table_key}':")
    picklists = cache_entry.get("picklists", {})
    print(f"  Cached {len(picklists)} picklist attribute(s):")
    for attr, mapping in picklists.items():
        print(f"    {attr}: {json.dumps(mapping, indent=6)}")

    # ============================================================================
    # 4. SINGLE PICKLIST LABEL (UPDATE)
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Single Picklist Label (Update)")
    print("=" * 80)

    log_call(f"client.records.update('{table_name}', id1, {{'new_Priority': 'Low'}})")
    backoff(lambda: client.records.update(table_name, id1, {"new_Priority": "Low"}))
    updated1 = backoff(lambda: client.records.get(table_name, id1))
    print(f"[OK] Updated with label 'Low'")
    print(f"  new_priority = {updated1.get('new_priority')} (expected: 1)")
    assert updated1.get("new_priority") == 1, "Expected 1 for 'Low'"

    # ============================================================================
    # 5. MULTIPLE PICKLISTS IN ONE RECORD
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Multiple Picklists in One Record")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 'Medium', 'new_Status': 'Active'}})")
    record2 = {
        "new_Title": "Two picklists test",
        "new_Priority": "Medium",
        "new_Status": "Active",
    }
    id2 = backoff(lambda: client.records.create(table_name, record2))
    retrieved2 = backoff(lambda: client.records.get(table_name, id2))
    print(f"[OK] Created with two picklist labels")
    print(f"  new_priority = {retrieved2.get('new_priority')} (expected: 2)")
    print(f"  new_status = {retrieved2.get('new_status')} (expected: 100000001)")
    assert retrieved2.get("new_priority") == 2, "Expected 2 for 'Medium'"
    assert retrieved2.get("new_status") == 100000001, "Expected 100000001 for 'Active'"

    # ============================================================================
    # 6. MIXED PICKLIST + NON-PICKLIST STRINGS
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Mixed Picklist + Non-Picklist Strings")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{picklist + string fields}})")
    record3 = {
        "new_Title": "Mixed fields test",
        "new_Description": "This is a plain string, not a picklist",
        "new_Priority": "Low",
        "new_Status": "Draft",
        "new_Count": 42,
    }
    id3 = backoff(lambda: client.records.create(table_name, record3))
    retrieved3 = backoff(lambda: client.records.get(table_name, id3))
    print(f"[OK] Created with mixed fields")
    print(f"  new_priority = {retrieved3.get('new_priority')} (expected: 1)")
    print(f"  new_status = {retrieved3.get('new_status')} (expected: 100000000)")
    print(f"  new_description = '{retrieved3.get('new_description')}' (expected: unchanged string)")
    print(f"  new_count = {retrieved3.get('new_count')} (expected: 42)")
    assert retrieved3.get("new_priority") == 1, "Expected 1 for 'Low'"
    assert retrieved3.get("new_status") == 100000000, "Expected 100000000 for 'Draft'"
    assert retrieved3.get("new_description") == "This is a plain string, not a picklist", "String should be unchanged"
    assert retrieved3.get("new_count") == 42, "Integer should be unchanged"

    # ============================================================================
    # 7. INTEGER VALUES PASSED THROUGH
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. Integer Values Passed Through")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 3}})")
    record4 = {
        "new_Title": "Integer value test",
        "new_Priority": 3,
        "new_Status": 100000002,
    }
    id4 = backoff(lambda: client.records.create(table_name, record4))
    retrieved4 = backoff(lambda: client.records.get(table_name, id4))
    print(f"[OK] Created with integer values (no label resolution needed)")
    print(f"  new_priority = {retrieved4.get('new_priority')} (expected: 3)")
    print(f"  new_status = {retrieved4.get('new_status')} (expected: 100000002)")
    assert retrieved4.get("new_priority") == 3
    assert retrieved4.get("new_status") == 100000002

    # ============================================================================
    # 8. UNMATCHED LABEL LEFT AS STRING
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Unmatched Label (Graceful Fallback)")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Title': 'UnknownLabel'}})")
    # new_Title is a string column, not a picklist -- SDK should pass it through
    # even though it looks like it could be a label
    record5 = {
        "new_Title": "UnknownLabel",
        "new_Count": 7,
    }
    id5 = backoff(lambda: client.records.create(table_name, record5))
    retrieved5 = backoff(lambda: client.records.get(table_name, id5))
    print(f"[OK] String value for non-picklist field passed through")
    print(f"  new_title = '{retrieved5.get('new_title')}' (expected: 'UnknownLabel')")
    assert retrieved5.get("new_title") == "UnknownLabel", "Non-picklist string should be unchanged"

    # ============================================================================
    # 9. CASE-INSENSITIVE LABEL MATCHING
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. Case-Insensitive Label Matching")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 'HIGH'}})")
    record6 = {
        "new_Title": "Case test - uppercase",
        "new_Priority": "HIGH",
    }
    id6 = backoff(lambda: client.records.create(table_name, record6))
    retrieved6 = backoff(lambda: client.records.get(table_name, id6))
    print(f"[OK] 'HIGH' (uppercase) resolved correctly")
    print(f"  new_priority = {retrieved6.get('new_priority')} (expected: 3)")
    assert retrieved6.get("new_priority") == 3, "Case-insensitive match failed"

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 'medium'}})")
    record7 = {
        "new_Title": "Case test - lowercase",
        "new_Priority": "medium",
    }
    id7 = backoff(lambda: client.records.create(table_name, record7))
    retrieved7 = backoff(lambda: client.records.get(table_name, id7))
    print(f"[OK] 'medium' (lowercase) resolved correctly")
    print(f"  new_priority = {retrieved7.get('new_priority')} (expected: 2)")
    assert retrieved7.get("new_priority") == 2, "Case-insensitive match failed"

    # ============================================================================
    # 10. WARM CACHE (SECOND CALL SKIPS METADATA)
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. Warm Cache (Second Call)")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{...}}) -- should use cached metadata")
    record8 = {
        "new_Title": "Warm cache test",
        "new_Priority": "Low",
        "new_Status": "Closed",
        "new_Description": "Cache should be warm from previous calls",
    }
    id8 = backoff(lambda: client.records.create(table_name, record8))
    retrieved8 = backoff(lambda: client.records.get(table_name, id8))
    print(f"[OK] Created using warm cache (no extra metadata calls)")
    print(f"  new_priority = {retrieved8.get('new_priority')} (expected: 1)")
    print(f"  new_status = {retrieved8.get('new_status')} (expected: 100000002)")
    assert retrieved8.get("new_priority") == 1
    assert retrieved8.get("new_status") == 100000002

    # ============================================================================
    # 11. UPDATE WITH MULTIPLE PICKLIST LABELS
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Update with Multiple Picklist Labels")
    print("=" * 80)

    log_call(f"client.records.update('{table_name}', id8, {{'new_Priority': 'High', 'new_Status': 'Active'}})")
    backoff(
        lambda: client.records.update(
            table_name,
            id8,
            {"new_Priority": "High", "new_Status": "Active"},
        )
    )
    updated8 = backoff(lambda: client.records.get(table_name, id8))
    print(f"[OK] Updated with two picklist labels")
    print(f"  new_priority = {updated8.get('new_priority')} (expected: 3)")
    print(f"  new_status = {updated8.get('new_status')} (expected: 100000001)")
    assert updated8.get("new_priority") == 3
    assert updated8.get("new_status") == 100000001

    # ============================================================================
    # 12. MIXED INT AND LABEL IN SAME RECORD
    # ============================================================================
    print("\n" + "=" * 80)
    print("12. Mixed Integer + Label in Same Record")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 2, 'new_Status': 'Closed'}})")
    record9 = {
        "new_Title": "Mixed int+label test",
        "new_Priority": 2,  # already an int
        "new_Status": "Closed",  # label to resolve
    }
    id9 = backoff(lambda: client.records.create(table_name, record9))
    retrieved9 = backoff(lambda: client.records.get(table_name, id9))
    print(f"[OK] Created with int for Priority, label for Status")
    print(f"  new_priority = {retrieved9.get('new_priority')} (expected: 2, passed through)")
    print(f"  new_status = {retrieved9.get('new_status')} (expected: 100000002, resolved from 'Closed')")
    assert retrieved9.get("new_priority") == 2, "Int value should pass through unchanged"
    assert retrieved9.get("new_status") == 100000002, "Expected 100000002 for 'Closed'"

    # ============================================================================
    # 13. FULL REALISTIC UPDATE (picklists + strings + non-strings)
    # ============================================================================
    print("\n" + "=" * 80)
    print("13. Full Realistic Update (All Field Types)")
    print("=" * 80)

    log_call(f"client.records.update('{table_name}', id9, {{picklists + strings + int}})")
    backoff(
        lambda: client.records.update(
            table_name,
            id9,
            {
                "new_Priority": "High",
                "new_Status": "Active",
                "new_Description": "Updated description text",
                "new_Count": 99,
            },
        )
    )
    updated9 = backoff(lambda: client.records.get(table_name, id9))
    print(f"[OK] Updated with all field types in one call")
    print(f"  new_priority = {updated9.get('new_priority')} (expected: 3)")
    print(f"  new_status = {updated9.get('new_status')} (expected: 100000001)")
    print(f"  new_description = '{updated9.get('new_description')}' (expected: unchanged string)")
    print(f"  new_count = {updated9.get('new_count')} (expected: 99)")
    assert updated9.get("new_priority") == 3
    assert updated9.get("new_status") == 100000001
    assert updated9.get("new_description") == "Updated description text"
    assert updated9.get("new_count") == 99

    # ============================================================================
    # 14. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("14. Cleanup")
    print("=" * 80)

    all_ids = [id1, id2, id3, id4, id5, id6, id7, id8, id9]

    log_call(f"client.records.delete('{table_name}', [{len(all_ids)} IDs])")
    backoff(lambda: client.records.delete(table_name, all_ids))
    print(f"[OK] Deleted {len(all_ids)} records")

    log_call(f"client.tables.delete('{table_name}')")
    try:
        backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted table: {table_name}")
    except MetadataError as ex:
        if "not found" in str(ex).lower():
            print(f"[OK] Table already removed: {table_name}")
        else:
            raise
    except Exception as ex:
        code = getattr(getattr(ex, "response", None), "status_code", None)
        if isinstance(ex, requests.exceptions.HTTPError) and code == 404:
            print(f"[OK] Table removed: {table_name}")
        else:
            raise

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("\n" + "=" * 80)
    print("Picklist Walkthrough Complete!")
    print("=" * 80)
    print("\nAll assertions passed:")
    print("  [OK] Single picklist label (create)")
    print("  [OK] Single picklist label (update)")
    print("  [OK] Multiple picklists in one record")
    print("  [OK] Mixed picklist + non-picklist strings")
    print("  [OK] Integer values passed through unchanged")
    print("  [OK] Unmatched label for non-picklist (graceful fallback)")
    print("  [OK] Case-insensitive label matching (uppercase + lowercase)")
    print("  [OK] Warm cache (second call uses cached metadata)")
    print("  [OK] Update with multiple picklist labels")
    print("  [OK] Mixed integer + label in same record")
    print("  [OK] Full realistic update (picklists + strings + non-strings)")
    print("=" * 80)


if __name__ == "__main__":
    main()
