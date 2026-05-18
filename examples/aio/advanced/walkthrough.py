# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async walkthrough demonstrating core Dataverse SDK operations.

Async equivalent of examples/advanced/walkthrough.py.

This example shows:
- Table creation with various column types including enums
- Single and multiple record CRUD operations
- Querying with filtering, paging, AsyncQueryBuilder, and SQL
- Expand (navigation properties) with AsyncQueryBuilder
- Picklist label-to-value conversion
- Column management
- Batch operations (create, read, update, changeset, delete in one HTTP request)
- Cleanup

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import asyncio
import sys
import json
from enum import IntEnum

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential
from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
from PowerPlatform.Dataverse.models.filters import col
from PowerPlatform.Dataverse.models.query_builder import ExpandOption


def log_call(description):
    print(f"\n-> {description}")


class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


async def backoff(coro_fn, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry a coroutine with exponential back-off for metadata propagation delays."""
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            await asyncio.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = await coro_fn()
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


async def main():
    print("=" * 80)
    print("Dataverse SDK Async Walkthrough")
    print("=" * 80)

    # ============================================================================
    # 1. SETUP & AUTHENTICATION
    # ============================================================================
    print("\n" + "=" * 80)
    print("1. Setup & Authentication")
    print("=" * 80)

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)

    base_url = base_url.rstrip("/")

    log_call("AsyncInteractiveBrowserCredential()")
    credential = AsyncInteractiveBrowserCredential()

    log_call(f"AsyncDataverseClient(base_url='{base_url}', credential=...)")
    try:
        async with AsyncDataverseClient(base_url=base_url, credential=credential) as client:
            print(f"[OK] Connected to: {base_url}")
            await _run_walkthrough(client)
    finally:
        await credential.close()


async def _run_walkthrough(client):
    # ============================================================================
    # 2. TABLE CREATION (METADATA)
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (Metadata)")
    print("=" * 80)

    table_name = "new_WalkthroughDemo"

    log_call(f"await client.tables.get('{table_name}')")
    table_info = await backoff(lambda: client.tables.get(table_name))

    if table_info:
        print(f"[OK] Table already exists: {table_info.get('table_schema_name')}")
        print(f"  Logical Name: {table_info.get('table_logical_name')}")
        print(f"  Entity Set: {table_info.get('entity_set_name')}")
    else:
        log_call(f"await client.tables.create('{table_name}', columns={{...}}, display_name='Walkthrough Demo')")
        columns = {
            "new_Title": "string",
            "new_Quantity": "int",
            "new_Amount": "decimal",
            "new_Completed": "bool",
            "new_Notes": "memo",
            "new_Priority": Priority,
        }
        table_info = await backoff(lambda: client.tables.create(table_name, columns, display_name="Walkthrough Demo"))
        print(f"[OK] Created table: {table_info.get('table_schema_name')}")
        print(f"  Columns created: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. CREATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Operations")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', {{...}})")
    single_record = {
        "new_Title": "Complete project documentation",
        "new_Quantity": 5,
        "new_Amount": 1250.50,
        "new_Completed": False,
        "new_Notes": "This is a multiline memo field.\nIt supports longer text content.",
        "new_Priority": Priority.MEDIUM,
    }
    id1 = await backoff(lambda: client.records.create(table_name, single_record))
    print(f"[OK] Created single record: {id1}")

    log_call(f"await client.records.create('{table_name}', [{{...}}, {{...}}, {{...}}])")
    multiple_records = [
        {
            "new_Title": "Review code changes",
            "new_Quantity": 10,
            "new_Amount": 500.00,
            "new_Completed": True,
            "new_Priority": Priority.HIGH,
        },
        {
            "new_Title": "Update test cases",
            "new_Quantity": 8,
            "new_Amount": 750.25,
            "new_Completed": False,
            "new_Priority": Priority.LOW,
        },
        {
            "new_Title": "Deploy to staging",
            "new_Quantity": 3,
            "new_Amount": 2000.00,
            "new_Completed": False,
            "new_Priority": Priority.HIGH,
        },
    ]
    ids = await backoff(lambda: client.records.create(table_name, multiple_records))
    print(f"[OK] Created {len(ids)} records: {ids}")

    # ============================================================================
    # 4. READ OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Read Operations")
    print("=" * 80)

    log_call(f"await client.records.retrieve('{table_name}', '{id1}')")
    record = await backoff(lambda: client.records.retrieve(table_name, id1))
    print("[OK] Retrieved single record:")
    print(
        json.dumps(
            {
                "new_walkthroughdemoid": record.get("new_walkthroughdemoid"),
                "new_title": record.get("new_title"),
                "new_quantity": record.get("new_quantity"),
                "new_amount": record.get("new_amount"),
                "new_completed": record.get("new_completed"),
                "new_notes": record.get("new_notes"),
                "new_priority": record.get("new_priority"),
                "new_priority@FormattedValue": record.get("new_priority@OData.Community.Display.V1.FormattedValue"),
            },
            indent=2,
        )
    )

    log_call(f"await client.records.list('{table_name}', filter='new_quantity gt 5')")
    all_records = await backoff(lambda: client.records.list(table_name, filter="new_quantity gt 5"))
    print(f"[OK] Found {len(all_records)} records with new_quantity > 5")
    for rec in all_records:
        print(f"  - new_Title='{rec.get('new_title')}', new_Quantity={rec.get('new_quantity')}")

    # ============================================================================
    # 5. UPDATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Update Operations")
    print("=" * 80)

    log_call(f"await client.records.update('{table_name}', '{id1}', {{...}})")
    await backoff(
        lambda: client.records.update(
            table_name,
            id1,
            {
                "new_Quantity": 100,
                "new_Notes": "Updated memo field.\nNow with revised content across multiple lines.",
            },
        )
    )
    updated = await backoff(lambda: client.records.retrieve(table_name, id1))
    print(f"[OK] Updated single record new_Quantity: {updated.get('new_quantity')}")
    print(f"  new_Notes: {repr(updated.get('new_notes'))}")

    log_call(f"await client.records.update('{table_name}', [{len(ids)} IDs], {{...}})")
    await backoff(lambda: client.records.update(table_name, ids, {"new_Completed": True}))
    print(f"[OK] Updated {len(ids)} records to new_Completed=True")

    # ============================================================================
    # 6. PAGING DEMO
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Paging Demo")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', [20 records])")
    paging_records = [
        {
            "new_Title": f"Paging test item {i}",
            "new_Quantity": i,
            "new_Amount": i * 10.0,
            "new_Completed": False,
            "new_Priority": Priority.LOW,
        }
        for i in range(1, 21)
    ]
    paging_ids = await backoff(lambda: client.records.create(table_name, paging_records))
    print(f"[OK] Created {len(paging_ids)} records for paging demo")

    log_call(f"async for page in client.query.builder('{table_name}').order_by().page_size(5).execute_pages()")
    print("Fetching records with page_size=5...")
    page_num = 0
    async for page in client.query.builder(table_name).order_by("new_Quantity").page_size(5).execute_pages():
        page_num += 1
        record_ids = [r.get("new_walkthroughdemoid")[:8] + "..." for r in page]
        print(f"  Page {page_num}: {len(page)} records - IDs: {record_ids}")

    # ============================================================================
    # 7. QUERYBUILDER - FLUENT QUERIES
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. AsyncQueryBuilder - Fluent Queries")
    print("=" * 80)

    log_call("await client.query.builder(...).select().where(col(...)==...).order_by().execute()")
    print("Querying incomplete records ordered by amount (fluent builder)...")
    qb_result = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_Title", "new_Amount", "new_Priority")
        .where(col("new_Completed") == False)
        .order_by("new_Amount", descending=True)
        .top(10)
        .execute()
    )
    print(f"[OK] AsyncQueryBuilder found {len(qb_result)} incomplete records:")
    for rec in list(qb_result)[:5]:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')}")

    log_call("await client.query.builder(...).where(col('new_Priority').in_([HIGH, LOW])).execute()")
    print("Querying records with HIGH or LOW priority (col().in_())...")
    priority_result = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_Title", "new_Priority")
        .where(col("new_Priority").in_([Priority.HIGH, Priority.LOW]))
        .execute()
    )
    print(f"[OK] Found {len(priority_result)} records with HIGH or LOW priority")

    log_call("await client.query.builder(...).where(col('new_Amount').between(500, 1500)).execute()")
    range_result = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_Title", "new_Amount")
        .where(col("new_Amount").between(500, 1500))
        .execute()
    )
    print(f"[OK] Found {len(range_result)} records with amount in [500, 1500]")

    log_call("await client.query.builder(...).where((col(...)==...) & (col(...) > ...)).execute()")
    expr_result = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_Title", "new_Amount", "new_Quantity")
        .where((col("new_Completed") == False) & (col("new_Amount") > 100))
        .order_by("new_Amount", descending=True)
        .top(5)
        .execute()
    )
    print(f"[OK] Expression tree query found {len(expr_result)} records:")
    for rec in expr_result:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')} Qty={rec.get('new_quantity')}")

    log_call("async for page in client.query.builder(...).where(...).page_size().execute_pages()")
    print("Querying with combined expression filters and paging...")
    combined_page_count = 0
    combined_record_count = 0
    async for page in (
        client.query.builder(table_name)
        .select("new_Title", "new_Quantity")
        .where(col("new_Completed") == False)
        .where(col("new_Quantity").between(1, 15))
        .order_by("new_Quantity")
        .page_size(3)
        .execute_pages()
    ):
        combined_page_count += 1
        combined_record_count += len(page)
        titles = [r.get("new_title", "?") for r in page]
        print(f"  Page {combined_page_count}: {len(page)} records - {titles}")
    print(f"[OK] Combined query: {combined_record_count} records across {combined_page_count} page(s)")

    log_call(f"(await client.query.builder('{table_name}').select(...).where(...).execute()).to_dataframe()")
    print("Querying completed records as a pandas DataFrame (to_dataframe)...")
    completed_result = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_title", "new_quantity")
        .where(col("new_completed") == True)
        .execute()
    )
    df = completed_result.to_dataframe()
    print(f"[OK] to_dataframe() returned {len(df)} rows, columns: {list(df.columns)}")
    if not df.empty:
        print(f"  First row: new_title='{df.iloc[0].get('new_title')}', new_quantity={df.iloc[0].get('new_quantity')}")
        print(f"  Sum of new_quantity: {df['new_quantity'].sum()}")
    else:
        print("  (empty DataFrame)")

    # ============================================================================
    # 8. EXPAND (NAVIGATION PROPERTIES)
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Expand (Navigation Properties)")
    print("=" * 80)

    log_call("await client.query.builder('account').select('name').expand('primarycontactid').top(3).execute()")
    try:
        expanded_records = await backoff(
            lambda: client.query.builder("account").select("name").expand("primarycontactid").top(3).execute()
        )
        print(f"[OK] Found {len(expanded_records)} accounts with expanded contact:")
        for rec in expanded_records:
            contact = rec.get("primarycontactid")
            contact_name = contact.get("fullname", "(none)") if contact else "(no contact)"
            print(f"  - '{rec.get('name')}' -> Contact: {contact_name}")
    except Exception as e:
        print(f"[SKIP] Expand demo skipped (no accounts in org): {e}")

    log_call("ExpandOption('Account_Tasks').select('subject').order_by('createdon', descending=True).top(3)")
    try:
        tasks_opt = (
            ExpandOption("Account_Tasks").select("subject", "createdon").order_by("createdon", descending=True).top(3)
        )
        nested_records = await backoff(
            lambda: client.query.builder("account").select("name").expand(tasks_opt).top(3).execute()
        )
        print(f"[OK] Found {len(nested_records)} accounts with nested task expansion:")
        for rec in nested_records:
            tasks = rec.get("Account_Tasks", [])
            print(f"  - '{rec.get('name')}' has {len(tasks)} task(s)")
    except Exception as e:
        print(f"[SKIP] Nested expand demo skipped: {e}")

    # ============================================================================
    # 9. SQL QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. SQL Query")
    print("=" * 80)

    sql = "SELECT new_title, new_quantity FROM new_walkthroughdemo WHERE new_completed = 1"
    log_call(f"await client.query.sql('{sql}')")
    try:
        results = await backoff(lambda: client.query.sql(sql))
        print(f"[OK] SQL query returned {len(results)} completed records:")
        for result in results[:5]:
            print(f"  - new_Title='{result.get('new_title')}', new_Quantity={result.get('new_quantity')}")
    except Exception as e:
        print(f"[WARN] SQL query failed: {str(e)}")

    # ============================================================================
    # 10. FETCHXML QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. FetchXML Query")
    print("=" * 80)

    xml = f"""
    <fetch top="5">
      <entity name="new_walkthroughdemo">
        <attribute name="new_title" />
        <attribute name="new_quantity" />
        <filter>
          <condition attribute="new_completed" operator="eq" value="0" />
        </filter>
      </entity>
    </fetch>
    """
    log_call("await client.query.fetchxml(xml).execute()")
    try:
        fx_result = await backoff(lambda: client.query.fetchxml(xml).execute())
        print(f"[OK] FetchXML returned {len(fx_result)} incomplete records:")
        for r in fx_result[:5]:
            print(f"  - '{r.get('new_title')}' Quantity={r.get('new_quantity')}")
    except Exception as e:
        print(f"[WARN] FetchXML query failed: {e}")

    log_call("async for page in client.query.fetchxml(paged_xml).execute_pages()")
    paged_xml = f"""
    <fetch count="3">
      <entity name="new_walkthroughdemo">
        <attribute name="new_title" />
        <order attribute="new_quantity" />
      </entity>
    </fetch>
    """
    try:
        fx_page_num = 0
        fx_total = 0
        async for page in client.query.fetchxml(paged_xml).execute_pages():
            fx_page_num += 1
            fx_total += len(page)
            titles = [r.get("new_title", "?") for r in page]
            print(f"  Page {fx_page_num}: {len(page)} record(s) — {titles}")
        print(f"[OK] FetchXML execute_pages(): {fx_total} total records across {fx_page_num} page(s)")
    except Exception as e:
        print(f"[WARN] FetchXML execute_pages failed: {e}")

    # ============================================================================
    # 11. PICKLIST LABEL CONVERSION
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Picklist Label Conversion")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', {{'new_Priority': 'High'}})")
    label_record = {
        "new_Title": "Test label conversion",
        "new_Quantity": 1,
        "new_Amount": 99.99,
        "new_Completed": False,
        "new_Priority": "High",
    }
    label_id = await backoff(lambda: client.records.create(table_name, label_record))
    retrieved = await backoff(lambda: client.records.retrieve(table_name, label_id))
    print(f"[OK] Created record with string label 'High' for new_Priority")
    print(f"  new_Priority stored as integer: {retrieved.get('new_priority')}")
    print(f"  new_Priority@FormattedValue: {retrieved.get('new_priority@OData.Community.Display.V1.FormattedValue')}")

    log_call(f"await client.records.update('{table_name}', label_id, {{'new_Priority': 'Low'}})")
    await backoff(lambda: client.records.update(table_name, label_id, {"new_Priority": "Low"}))
    updated_label = await backoff(lambda: client.records.retrieve(table_name, label_id))
    print(f"[OK] Updated record with string label 'Low' for new_Priority")
    print(f"  new_Priority stored as integer: {updated_label.get('new_priority')}")

    # ============================================================================
    # 12. COLUMN MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("12. Column Management")
    print("=" * 80)

    log_call(f"await client.tables.add_columns('{table_name}', {{'new_Tags': 'string'}})")
    created_cols = await backoff(lambda: client.tables.add_columns(table_name, {"new_Tags": "string"}))
    print(f"[OK] Added column: {created_cols[0]}")

    log_call(f"await client.tables.remove_columns('{table_name}', ['new_Tags'])")
    await backoff(lambda: client.tables.remove_columns(table_name, ["new_Tags"]))
    print("[OK] Deleted column: new_Tags")

    # ============================================================================
    # 13. DELETE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("13. Delete Operations")
    print("=" * 80)

    log_call(f"await client.records.delete('{table_name}', '{id1}')")
    await backoff(lambda: client.records.delete(table_name, id1))
    print(f"[OK] Deleted single record: {id1}")

    log_call(f"await client.records.delete('{table_name}', [{len(paging_ids)} IDs])")
    job_id = await backoff(lambda: client.records.delete(table_name, paging_ids))
    print(f"[OK] Bulk delete job started: {job_id}")

    # ============================================================================
    # 14. BATCH OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("14. Batch Operations")
    print("=" * 80)

    log_call("client.batch.new() + batch.records.create(...) x2 + await batch.execute()")
    batch = client.batch.new()
    batch.records.create(
        table_name,
        {
            "new_Title": "Batch task alpha",
            "new_Quantity": 1,
            "new_Amount": 25.0,
            "new_Completed": False,
            "new_Priority": Priority.LOW,
        },
    )
    batch.records.create(
        table_name,
        {
            "new_Title": "Batch task beta",
            "new_Quantity": 2,
            "new_Amount": 50.0,
            "new_Completed": False,
            "new_Priority": Priority.MEDIUM,
        },
    )
    result = await batch.execute()
    batch_ids = list(result.entity_ids)
    print(f"[OK] Batch create: {len(result.succeeded)} operations, {len(batch_ids)} records created")

    log_call("client.batch.new() + batch.records.retrieve(...) x2 + await batch.execute()")
    batch = client.batch.new()
    for bid in batch_ids:
        batch.records.retrieve(table_name, bid, select=["new_title", "new_quantity"])
    result = await batch.execute()
    print(f"[OK] Batch get: {len(result.succeeded)} reads in one HTTP request")
    for resp in result.succeeded:
        if resp.data:
            print(f"  new_title='{resp.data.get('new_title')}', new_quantity={resp.data.get('new_quantity')}")

    log_call("async with batch.changeset() as cs: cs.records.create(...); cs.records.update(ref, ...)")
    batch = client.batch.new()
    async with batch.changeset() as cs:
        cs_ref = cs.records.create(
            table_name,
            {
                "new_Title": "Changeset task",
                "new_Quantity": 5,
                "new_Amount": 100.0,
                "new_Completed": False,
                "new_Priority": Priority.HIGH,
            },
        )
        cs.records.update(table_name, cs_ref, {"new_Completed": True})
    result = await batch.execute()
    if not result.has_errors:
        batch_ids.extend(result.entity_ids)
        print(f"[OK] Changeset: {len(result.succeeded)} operations committed atomically")
    else:
        for item in result.failed:
            print(f"[WARN] Changeset error {item.status_code}: {item.error_message}")

    log_call(f"client.batch.new() + batch.records.delete(...) x{len(batch_ids)} + await batch.execute()")
    batch = client.batch.new()
    for bid in batch_ids:
        batch.records.delete(table_name, bid)
    result = await batch.execute(continue_on_error=True)
    print(f"[OK] Batch delete: {len(result.succeeded)} records deleted in one HTTP request")

    # ============================================================================
    # 15. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("15. Cleanup")
    print("=" * 80)

    log_call(f"await client.tables.delete('{table_name}')")
    try:
        await backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted table: {table_name}")
    except MetadataError as ex:
        if "not found" in str(ex).lower():
            print(f"[OK] Table already removed: {table_name}")
        else:
            raise
    except Exception as ex:
        if "404" in str(ex):
            print(f"[OK] Table removed: {table_name}")
        else:
            raise

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("\n" + "=" * 80)
    print("Async Walkthrough Complete!")
    print("=" * 80)
    print("\nDemonstrated operations:")
    print("  [OK] Table creation with multiple column types")
    print("  [OK] Single and multiple record creation")
    print("  [OK] Reading records by ID and with filters")
    print("  [OK] Single and multiple record updates")
    print("  [OK] Paging through large result sets")
    print("  [OK] AsyncQueryBuilder fluent queries (where + col(), col().in_(), col().between(), to_dataframe)")
    print("  [OK] Expand navigation properties (simple + nested ExpandOption)")
    print("  [OK] SQL queries")
    print("  [OK] FetchXML queries (execute + execute_pages)")
    print("  [OK] Picklist label-to-value conversion")
    print("  [OK] Column management")
    print("  [OK] Single and bulk delete operations")
    print("  [OK] Batch operations (create, read, changeset, delete)")
    print("  [OK] Table cleanup")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
