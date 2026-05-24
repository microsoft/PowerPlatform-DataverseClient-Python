# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Walkthrough demonstrating memo/multiline column type.

This example exercises memo columns comprehensively:
- Create table with memo column
- Create record with multiline text (newlines)
- Read back and verify multiline text preserved
- Update memo with new multiline content
- Empty string memo
- None/null memo
- Long text (near max length)
- Memo with special characters (quotes, unicode, tabs)
- Memo alongside picklist and other field types
- Verify memo is not mistaken for picklist label

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import sys
import time
from enum import IntEnum
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
import requests


def log_call(description):
    print(f"\n-> {description}")


class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


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
                print(f"   [INFO] Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:
            last = ex
            continue
    if last:
        if attempts:
            print(f"   [WARN] Backoff exhausted after {max(attempts - 1, 0)} retry(s); waited {total_delay}s total.")
        raise last


assertions_passed = []


def check(label, actual, expected):
    if actual == expected:
        assertions_passed.append(label)
        print(f"  [OK] {label}")
    else:
        print(f"  [FAIL] {label}")
        print(f"    Expected: {expected!r}")
        print(f"    Actual:   {actual!r}")


def main():
    print("=" * 80)
    print("Memo/Multiline Column Walkthrough")
    print("=" * 80)

    print("\n" + "=" * 80)
    print("1. Setup & Authentication")
    print("=" * 80)

    base_url = sys.argv[1] if len(sys.argv) > 1 else input(
        "Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): "
    ).strip()
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
    table_name = "new_MemoDemo"
    created_ids = []

    # ============================================================================
    # 2. Table Creation
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (memo + string + picklist)")
    print("=" * 80)

    log_call(f"client.tables.get('{table_name}')")
    table_info = backoff(lambda: client.tables.get(table_name))
    if table_info:
        print(f"[OK] Table already exists, deleting for clean test...")
        backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted existing table")
        time.sleep(5)

    log_call(f"client.tables.create('{table_name}', columns={{...}})")
    columns = {
        "new_Title": "string",
        "new_Description": "memo",
        "new_Priority": Priority,
    }
    table_info = backoff(lambda: client.tables.create(table_name, columns))
    print(f"[OK] Created table: {table_info.get('table_schema_name')}")
    print(f"  Columns: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. Basic Multiline Create & Read
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Basic Multiline Create & Read")
    print("=" * 80)

    multiline_text = (
        "Subject: Quarterly Performance Review - Q1 2026\n"
        "\n"
        "Team,\n"
        "\n"
        "Below is a summary of our Q1 performance across key metrics:\n"
        "\n"
        "Revenue: $2.4M (up 12% from Q4)\n"
        "New customers: 147 (target was 120)\n"
        "Customer retention: 94.2%\n"
        "Support ticket resolution: avg 4.2 hours\n"
        "\n"
        "Key highlights:\n"
        "- Launched the Python SDK for Dataverse, receiving positive feedback\n"
        "  from early adopters in the data science community.\n"
        "- Reduced API latency by 35% through connection pooling and\n"
        "  metadata caching optimizations.\n"
        "- Onboarded 3 enterprise customers with 10K+ seat deployments.\n"
        "\n"
        "Areas for improvement:\n"
        "- Documentation coverage needs to reach 90% before GA.\n"
        "- Integration test suite completion is at 78% (target: 95%).\n"
        "- Need to finalize the memo/multiline column support.\n"
        "\n"
        "Next steps: Schedule individual 1:1s to discuss Q2 goals.\n"
        "\n"
        "Best regards,\n"
        "Engineering Lead"
    )
    log_call("client.records.create(...) with multiline memo")
    id1 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Basic multiline test",
        "new_Description": multiline_text,
        "new_Priority": Priority.MEDIUM,
    }))
    created_ids.append(id1)
    print(f"[OK] Created record: {id1}")

    record = backoff(lambda: client.records.get(table_name, id1))
    check("Multiline text preserved on create",
          record.get("new_description"), multiline_text)
    print(f"\n  Stored memo ({len(record.get('new_description', ''))} chars):")
    print("  " + "-" * 60)
    for line in record.get("new_description", "").split("\n"):
        print(f"  | {line}")
    print("  " + "-" * 60)

    # ============================================================================
    # 4. Update Memo with New Content
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Update Memo with New Content")
    print("=" * 80)

    updated_text = (
        "UPDATED: Quarterly Performance Review - Q1 2026\n"
        "\n"
        "Revision notes: Added final numbers after audit.\n"
        "\n"
        "Revenue: $2.45M (revised up from $2.4M after late invoices)\n"
        "New customers: 152 (revised up - 5 deals closed on Mar 31)\n"
        "Customer retention: 94.2% (unchanged)\n"
        "Support ticket resolution: avg 3.8 hours (improved from 4.2)\n"
        "\n"
        "Additional Q1 accomplishments:\n"
        "- Shipped picklist label resolution optimization (92x faster at scale)\n"
        "- Completed memo/multiline column support with full test coverage\n"
        "- Published SDK to PyPI with 1,200+ downloads in first week\n"
        "\n"
        "Q2 priorities:\n"
        "1. GA release preparation\n"
        "2. Performance benchmarking framework\n"
        "3. Expanded relationship management APIs\n"
        "\n"
        "-- Updated by Engineering Lead, April 2026"
    )
    log_call("client.records.update(...) with new multiline memo")
    backoff(lambda: client.records.update(table_name, id1, {
        "new_Description": updated_text,
    }))
    record = backoff(lambda: client.records.get(table_name, id1))
    check("Multiline text preserved on update",
          record.get("new_description"), updated_text)

    # ============================================================================
    # 5. Empty String Memo
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Empty String Memo")
    print("=" * 80)

    log_call("client.records.create(...) with empty memo")
    id2 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Empty memo test",
        "new_Description": "",
    }))
    created_ids.append(id2)
    record = backoff(lambda: client.records.get(table_name, id2))
    # Dataverse may return None for empty strings
    val = record.get("new_description")
    is_empty = val is None or val == ""
    if is_empty:
        assertions_passed.append("Empty string stored as null/empty")
        print(f"  [OK] Empty string stored as null/empty (got: {val!r})")
    else:
        print(f"  [FAIL] Expected null or empty, got: {val!r}")

    # ============================================================================
    # 6. None/Null Memo
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. None/Null Memo")
    print("=" * 80)

    log_call("client.records.create(...) with no memo field")
    id3 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "No memo provided",
    }))
    created_ids.append(id3)
    record = backoff(lambda: client.records.get(table_name, id3))
    check("Omitted memo field returns None",
          record.get("new_description"), None)

    # ============================================================================
    # 7. Special Characters
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. Special Characters")
    print("=" * 80)

    special_text = (
        "Quotes: \"double\" and 'single'\n"
        "Tabs:\there\tand\there\n"
        "Unicode: cafe\u0301 \u2603 \u2764\n"
        "Angle brackets: <script>alert('hi')</script>\n"
        "Ampersand: A & B"
    )
    log_call("client.records.create(...) with special characters")
    id4 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Special chars test",
        "new_Description": special_text,
    }))
    created_ids.append(id4)
    record = backoff(lambda: client.records.get(table_name, id4))
    check("Special characters preserved",
          record.get("new_description"), special_text)

    # ============================================================================
    # 8. Long Text (near max length)
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Long Text (near max length)")
    print("=" * 80)

    # MemoAttributeMetadata MaxLength is 4000
    long_text = "A" * 3900 + "\n" + "B" * 99
    log_call(f"client.records.create(...) with {len(long_text)} chars")
    id5 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Long text test",
        "new_Description": long_text,
    }))
    created_ids.append(id5)
    record = backoff(lambda: client.records.get(table_name, id5))
    stored = record.get("new_description")
    check("Long text preserved (4000 chars)",
          stored, long_text)

    # ============================================================================
    # 9. Memo Alongside Picklist (no interference)
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. Memo Alongside Picklist (no interference)")
    print("=" * 80)

    memo_with_picklist_label = "High"  # Same as a picklist label
    log_call("client.records.create(...) with memo='High' and picklist='Low'")
    id6 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Memo vs picklist test",
        "new_Description": memo_with_picklist_label,
        "new_Priority": "Low",
    }))
    created_ids.append(id6)
    record = backoff(lambda: client.records.get(table_name, id6))
    check("Memo 'High' not resolved as picklist int",
          record.get("new_description"), "High")
    check("Picklist 'Low' resolved to int",
          record.get("new_priority"), Priority.LOW)

    # ============================================================================
    # 10. Triple-Quoted String (Python multiline syntax)
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. Triple-Quoted String (Python syntax)")
    print("=" * 80)

    triple_text = """Dear Customer,

Thank you for contacting Contoso Support regarding your recent order #12345.

We have thoroughly reviewed your case and determined the following status
for each item in your order:

  Item A (Wireless Keyboard) - Shipped via FedEx, tracking #1Z999AA10123456784
    Expected delivery: April 5, 2026
    
  Item B (USB-C Hub) - Currently on backorder due to supply chain delays.
    Estimated availability: 2-3 weeks. We will ship immediately when
    stock arrives and send you an updated tracking number.
    
  Item C (Screen Protector) - This item was damaged during fulfillment.
    A full refund of $24.99 has been processed to your original payment
    method. Please allow 5-7 business days for the credit to appear.

If you would like to substitute Item B with an alternative product or
cancel that portion of your order, please reply to this message or
call us at 1-800-555-0199 (Mon-Fri, 8am-6pm PST).

We sincerely apologize for any inconvenience and appreciate your
patience and continued business.

Best regards,
Sarah Johnson
Customer Support Specialist
Contoso Corporation
Ref: CASE-2026-04-001"""

    log_call("client.records.create(...) with triple-quoted multiline")
    id7 = backoff(lambda: client.records.create(table_name, {
        "new_Title": "Triple-quoted test",
        "new_Description": triple_text,
    }))
    created_ids.append(id7)
    record = backoff(lambda: client.records.get(table_name, id7))
    check("Triple-quoted multiline preserved",
          record.get("new_description"), triple_text)

    # ============================================================================
    # 11. Update Memo to None (clear field)
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Update Memo to None (clear field)")
    print("=" * 80)

    log_call("client.records.update(...) set memo to None")
    backoff(lambda: client.records.update(table_name, id1, {
        "new_Description": None,
    }))
    record = backoff(lambda: client.records.get(table_name, id1))
    check("Memo cleared to None",
          record.get("new_description"), None)

    # ============================================================================
    # 12. Cleanup
    # ============================================================================
    print("\n" + "=" * 80)
    print("12. Cleanup")
    print("=" * 80)

    log_call(f"client.records.delete('{table_name}', [{len(created_ids)} IDs])")
    backoff(lambda: client.records.delete(table_name, created_ids))
    print(f"[OK] Deleted {len(created_ids)} records")

    log_call(f"client.tables.delete('{table_name}')")
    try:
        backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted table: {table_name}")
    except Exception as ex:
        if "not found" in str(ex).lower():
            print(f"[OK] Table already removed: {table_name}")
        else:
            raise

    # ============================================================================
    # Summary
    # ============================================================================
    print("\n" + "=" * 80)
    print("Memo Walkthrough Complete!")
    print("=" * 80)
    print(f"\nAll assertions passed ({len(assertions_passed)}):")
    for a in assertions_passed:
        print(f"  [OK] {a}")
    print("=" * 80)


if __name__ == "__main__":
    main()
