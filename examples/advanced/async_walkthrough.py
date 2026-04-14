# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async walkthrough demonstrating core Dataverse SDK operations.

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
from azure.identity import InteractiveBrowserCredential as _SyncInteractiveBrowserCredential
from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
from PowerPlatform.Dataverse.models.filters import eq, gt, between
from PowerPlatform.Dataverse.models.query_builder import ExpandOption


class AsyncInteractiveBrowserCredential:
    """Async wrapper around the sync InteractiveBrowserCredential.

    ``azure.identity.aio`` does not include ``InteractiveBrowserCredential``.
    This wrapper runs ``get_token`` in a thread-pool executor so the browser
    prompt works without blocking the event loop.
    """

    def __init__(self, **kwargs):
        self._credential = _SyncInteractiveBrowserCredential(**kwargs)

    async def get_token(self, *scopes, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self._credential.get_token(*scopes, **kwargs)
        )

    async def close(self):
        self._credential.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


# Simple logging helper
def log_call(description):
    print(f"\n-> {description}")


# Define enum for priority picklist
class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


async def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry helper that awaits ``op()`` with exponential back-off delays."""
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            await asyncio.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = await op()
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


async def main():
    print("=" * 80)
    print("Async Dataverse SDK Walkthrough")
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
    async with AsyncInteractiveBrowserCredential() as credential:
        log_call(f"AsyncDataverseClient(base_url='{base_url}', credential=...)")
        async with AsyncDataverseClient(base_url=base_url, credential=credential) as client:
            print(f"[OK] Connected to: {base_url}")
            await _run_walkthrough(client)


async def _run_walkthrough(client):
    # ============================================================================
    # 2. TABLE CREATION (METADATA)
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (Metadata)")
    print("=" * 80)

    table_name = "new_AsyncWalkthroughDemo"

    log_call(f"await client.tables.get('{table_name}')")
    table_info = await backoff(lambda: client.tables.get(table_name))

    if table_info:
        print(f"[OK] Table already exists: {table_info.get('table_schema_name')}")
        print(f"  Logical Name: {table_info.get('table_logical_name')}")
        print(f"  Entity Set: {table_info.get('entity_set_name')}")
    else:
        log_call(f"await client.tables.create('{table_name}', columns={{...}})")
        columns = {
            "new_Title": "string",
            "new_Quantity": "int",
            "new_Amount": "decimal",
            "new_Completed": "bool",
            "new_Notes": "memo",
            "new_Priority": Priority,
        }
        table_info = await backoff(lambda: client.tables.create(table_name, columns))
        print(f"[OK] Created table: {table_info.get('table_schema_name')}")
        print(f"  Columns created: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. CREATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Operations")
    print("=" * 80)

    # Single create
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

    # Multiple create
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

    # Single read by ID
    log_call(f"await client.records.get('{table_name}', '{id1}')")
    record = await backoff(lambda: client.records.get(table_name, id1))
    print("[OK] Retrieved single record:")
    print(
        json.dumps(
            {
                "new_asyncwalkthroughdemoid": record.get("new_asyncwalkthroughdemoid"),
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

    # Multiple read with filter — get() returns an AsyncGenerator when record_id is omitted
    log_call(f"await client.records.get('{table_name}', filter='new_quantity gt 5')")
    all_records = []
    pages = await backoff(lambda: client.records.get(table_name, filter="new_quantity gt 5"))
    async for page in pages:
        all_records.extend(page)
    print(f"[OK] Found {len(all_records)} records with new_quantity > 5")
    for rec in all_records:
        print(f"  - new_Title='{rec.get('new_title')}', new_Quantity={rec.get('new_quantity')}")

    # ============================================================================
    # 5. UPDATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Update Operations")
    print("=" * 80)

    # Single update
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
    updated = await backoff(lambda: client.records.get(table_name, id1))
    print(f"[OK] Updated single record new_Quantity: {updated.get('new_quantity')}")
    print(f"  new_Notes: {repr(updated.get('new_notes'))}")

    # Multiple update (broadcast same change)
    log_call(f"await client.records.update('{table_name}', [{len(ids)} IDs], {{...}})")
    await backoff(lambda: client.records.update(table_name, ids, {"new_Completed": True}))
    print(f"[OK] Updated {len(ids)} records to new_Completed=True")

    # ============================================================================
    # 6. PAGING DEMO
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Paging Demo")
    print("=" * 80)

    # Create 20 records for paging
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

    # Query with paging — each iteration of the async for loop is one page
    log_call(f"await client.records.get('{table_name}', page_size=5)")
    print("Fetching records with page_size=5...")
    paging_iterator = await backoff(
        lambda: client.records.get(table_name, orderby=["new_Quantity"], page_size=5)
    )
    async for page_num, page in _aenumerate(paging_iterator, start=1):
        record_ids = [r.get("new_asyncwalkthroughdemoid")[:8] + "..." for r in page]
        print(f"  Page {page_num}: {len(page)} records - IDs: {record_ids}")

    # ============================================================================
    # 7. ASYNCQUERYBUILDER - FLUENT QUERIES
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. AsyncQueryBuilder - Fluent Queries")
    print("=" * 80)

    # Basic fluent query: incomplete records sorted by amount (flat async iteration)
    log_call("await client.query.builder(...).select().filter_eq().order_by().execute()")
    print("Querying incomplete records ordered by amount (fluent builder)...")
    qb_records = [
        r
        async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount", "new_Priority")
            .filter_eq("new_Completed", False)
            .order_by("new_Amount", descending=True)
            .top(10)
            .execute()
        )
    ]
    print(f"[OK] AsyncQueryBuilder found {len(qb_records)} incomplete records:")
    for rec in qb_records[:5]:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')}")

    # filter_in: records with specific priorities
    log_call("await client.query.builder(...).filter_in('new_Priority', [HIGH, LOW]).execute()")
    print("Querying records with HIGH or LOW priority (filter_in)...")
    priority_records = [
        r
        async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Priority")
            .filter_in("new_Priority", [Priority.HIGH, Priority.LOW])
            .execute()
        )
    ]
    print(f"[OK] Found {len(priority_records)} records with HIGH or LOW priority")
    for rec in priority_records[:5]:
        print(f"  - '{rec.get('new_title')}' Priority={rec.get('new_priority')}")

    # filter_between: amount in a range
    log_call("await client.query.builder(...).filter_between('new_Amount', 500, 1500).execute()")
    print("Querying records with amount between 500 and 1500 (filter_between)...")
    range_records = [
        r
        async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount")
            .filter_between("new_Amount", 500, 1500)
            .execute()
        )
    ]
    print(f"[OK] Found {len(range_records)} records with amount in [500, 1500]")
    for rec in range_records:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')}")

    # Composable expression tree with where()
    log_call("await client.query.builder(...).where((eq(...) & gt(...))).execute()")
    print("Querying with composable expression tree (where)...")
    expr_records = [
        r
        async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount", "new_Quantity")
            .where((eq("new_Completed", False) & gt("new_Amount", 100)))
            .order_by("new_Amount", descending=True)
            .top(5)
            .execute()
        )
    ]
    print(f"[OK] Expression tree query found {len(expr_records)} records:")
    for rec in expr_records:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')} Qty={rec.get('new_quantity')}")

    # Combined: fluent filters + expression tree + paging (by_page=True)
    log_call("await client.query.builder(...).filter_eq().where(between()).page_size().execute(by_page=True)")
    print("Querying with combined fluent + expression filters and paging...")
    combined_page_count = 0
    combined_record_count = 0
    async for page in await backoff(
        lambda: client.query.builder(table_name)
        .select("new_Title", "new_Quantity")
        .filter_eq("new_Completed", False)
        .where(between("new_Quantity", 1, 15))
        .order_by("new_Quantity")
        .page_size(3)
        .execute(by_page=True)
    ):
        combined_page_count += 1
        combined_record_count += len(page)
        titles = [r.get("new_title", "?") for r in page]
        print(f"  Page {combined_page_count}: {len(page)} records - {titles}")
    print(f"[OK] Combined query: {combined_record_count} records across {combined_page_count} page(s)")

    # to_dataframe: get results as a pandas DataFrame
    log_call(f"await client.query.builder('{table_name}').select(...).filter_eq(...).to_dataframe()")
    print("Querying completed records as a pandas DataFrame (to_dataframe)...")
    df = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_title", "new_quantity")
        .filter_eq("new_completed", True)
        .to_dataframe()
    )
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
    print("Querying accounts with primary contact expanded...")
    try:
        expanded_records = [
            r
            async for r in await backoff(
                lambda: client.query.builder("account")
                .select("name")
                .expand("primarycontactid")
                .top(3)
                .execute()
            )
        ]
        print(f"[OK] Found {len(expanded_records)} accounts with expanded contact:")
        for rec in expanded_records:
            contact = rec.get("primarycontactid")
            contact_name = contact.get("fullname", "(none)") if contact else "(no contact)"
            print(f"  - '{rec.get('name')}' -> Contact: {contact_name}")
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] Expand demo skipped (no accounts in org): {e}")

    log_call("ExpandOption('Account_Tasks').select('subject').order_by('createdon', descending=True).top(3)")
    print("Querying accounts with nested expand options on tasks...")
    try:
        tasks_opt = (
            ExpandOption("Account_Tasks").select("subject", "createdon").order_by("createdon", descending=True).top(3)
        )
        nested_records = [
            r
            async for r in await backoff(
                lambda: client.query.builder("account").select("name").expand(tasks_opt).top(3).execute()
            )
        ]
        print(f"[OK] Found {len(nested_records)} accounts with nested task expansion:")
        for rec in nested_records:
            tasks = rec.get("Account_Tasks", [])
            print(f"  - '{rec.get('name')}' has {len(tasks)} task(s)")
            for task in tasks:
                print(f"      - {task.get('subject')}")
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] Nested expand demo skipped: {e}")

    # ============================================================================
    # 9. SQL QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. SQL Query")
    print("=" * 80)

    sql = "SELECT new_title, new_quantity FROM new_asyncwalkthroughdemo WHERE new_completed = 1"
    log_call(f"await client.query.sql('{sql}')")
    try:
        results = await backoff(lambda: client.query.sql(sql))
        print(f"[OK] SQL query returned {len(results)} completed records:")
        for result in results[:5]:
            print(f"  - new_Title='{result.get('new_title')}', new_Quantity={result.get('new_quantity')}")
    except Exception as e:
        print(f"[WARN] SQL query failed (known server-side bug): {str(e)}")

    # ============================================================================
    # 10. PICKLIST LABEL CONVERSION
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. Picklist Label Conversion")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', {{'new_Priority': 'High'}})")
    label_record = {
        "new_Title": "Test label conversion",
        "new_Quantity": 1,
        "new_Amount": 99.99,
        "new_Completed": False,
        "new_Priority": "High",  # String label instead of int
    }
    label_id = await backoff(lambda: client.records.create(table_name, label_record))
    retrieved = await backoff(lambda: client.records.get(table_name, label_id))
    print(f"[OK] Created record with string label 'High' for new_Priority")
    print(f"  new_Priority stored as integer: {retrieved.get('new_priority')}")
    print(f"  new_Priority@FormattedValue: {retrieved.get('new_priority@OData.Community.Display.V1.FormattedValue')}")

    log_call(f"await client.records.update('{table_name}', label_id, {{'new_Priority': 'Low'}})")
    await backoff(lambda: client.records.update(table_name, label_id, {"new_Priority": "Low"}))
    updated_label = await backoff(lambda: client.records.get(table_name, label_id))
    print(f"[OK] Updated record with string label 'Low' for new_Priority")
    print(f"  new_Priority stored as integer: {updated_label.get('new_priority')}")
    print(
        f"  new_Priority@FormattedValue: {updated_label.get('new_priority@OData.Community.Display.V1.FormattedValue')}"
    )

    # ============================================================================
    # 11. COLUMN MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Column Management")
    print("=" * 80)

    log_call(f"await client.tables.add_columns('{table_name}', {{'new_Tags': 'string'}})")
    created_cols = await backoff(lambda: client.tables.add_columns(table_name, {"new_Tags": "string"}))
    print(f"[OK] Added column: {created_cols[0]}")

    log_call(f"await client.tables.remove_columns('{table_name}', ['new_Tags'])")
    await backoff(lambda: client.tables.remove_columns(table_name, ["new_Tags"]))
    print(f"[OK] Deleted column: new_Tags")

    # ============================================================================
    # 12. DELETE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("12. Delete Operations")
    print("=" * 80)

    log_call(f"await client.records.delete('{table_name}', '{id1}')")
    await backoff(lambda: client.records.delete(table_name, id1))
    print(f"[OK] Deleted single record: {id1}")

    log_call(f"await client.records.delete('{table_name}', [{len(paging_ids)} IDs])")
    job_id = await backoff(lambda: client.records.delete(table_name, paging_ids))
    print(f"[OK] Bulk delete job started: {job_id}")
    print(f"  (Deleting {len(paging_ids)} paging demo records)")

    # ============================================================================
    # 13. BATCH OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("13. Batch Operations")
    print("=" * 80)

    # Batch create: send 2 creates in a single POST $batch
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
    print(
        f"[OK] Batch create: {len(result.succeeded)} operations in one HTTP request, {len(batch_ids)} records created"
    )

    # Batch get: read both records in a single request
    log_call("client.batch.new() + batch.records.get(...) x2 + await batch.execute()")
    batch = client.batch.new()
    for bid in batch_ids:
        batch.records.get(table_name, bid, select=["new_title", "new_quantity"])
    result = await batch.execute()
    print(f"[OK] Batch get: {len(result.succeeded)} reads in one HTTP request")
    for resp in result.succeeded:
        if resp.data:
            print(f"  new_title='{resp.data.get('new_title')}', new_quantity={resp.data.get('new_quantity')}")

    # Changeset: create + update atomically (all-or-nothing)
    log_call("with batch.changeset() as cs: cs.records.create(...); cs.records.update(...)")
    batch = client.batch.new()
    with batch.changeset() as cs:
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

    # Batch delete: clean up all batch-created records in one request
    log_call(f"client.batch.new() + batch.records.delete(...) x{len(batch_ids)} + await batch.execute()")
    batch = client.batch.new()
    for bid in batch_ids:
        batch.records.delete(table_name, bid)
    result = await batch.execute(continue_on_error=True)
    print(f"[OK] Batch delete: {len(result.succeeded)} records deleted in one HTTP request")

    # ============================================================================
    # 14. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("14. Cleanup")
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
    print("  [OK] AsyncQueryBuilder fluent queries (filter_eq, filter_in, filter_between, where, to_dataframe)")
    print("  [OK] Expand navigation properties (simple + nested ExpandOption)")
    print("  [OK] SQL queries")
    print("  [OK] Picklist label-to-value conversion")
    print("  [OK] Column management")
    print("  [OK] Single and bulk delete operations")
    print("  [OK] Batch operations (create, read, changeset, delete)")
    print("  [OK] Table cleanup")
    print("=" * 80)


async def _aenumerate(ait, start=0):
    """Async equivalent of enumerate() for use in async for loops."""
    i = start
    async for item in ait:
        yield i, item
        i += 1


if __name__ == "__main__":
    asyncio.run(main())
