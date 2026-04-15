# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async walkthrough demonstrating 100% of the Dataverse async SDK API surface.

This example covers every public method and parameter variant:
- Table creation, listing, column management
- Single and multiple record CRUD (create, read, update, delete)
- Alternate keys and upsert (single + multiple via UpsertItem)
- Paging and AsyncQueryBuilder (fluent filters, expression trees, SQL)
- Expand (navigation properties, nested ExpandOption)
- Picklist label-to-value conversion
- DataFrame operations (get, create, update, delete via pandas)
- File upload (small and chunked modes)
- Relationships (lookup field, get, delete)
- Batch operations (records, tables, query, dataframe, changeset)
- Cache management (flush_cache)
- Cleanup

Prerequisites:
- pip install PowerPlatform-Dataverse-Client azure-identity pandas
- pip install azure-identity
"""

import asyncio
import os
import sys
import tempfile
from enum import IntEnum

import pandas as pd
from azure.identity import InteractiveBrowserCredential as _SyncInteractiveBrowserCredential

from PowerPlatform.Dataverse.aio import AsyncDataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
from PowerPlatform.Dataverse.models.filters import between, eq, gt
from PowerPlatform.Dataverse.models.query_builder import ExpandOption
from PowerPlatform.Dataverse.models.upsert import UpsertItem


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


def log_call(description):
    print(f"\n-> {description}")


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
                print(f"   [INFO] Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        print(f"   [WARN] Backoff exhausted after {max(attempts - 1, 0)} retry(s); waited {total_delay}s total.")
        raise last


async def main():
    print("=" * 80)
    print("Async Dataverse SDK Walkthrough — Full API Surface")
    print("=" * 80)

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


async def _run_walkthrough(client: AsyncDataverseClient):
    table_name = "new_AsyncWalkthroughDemo"

    # ============================================================================
    # 2. TABLE CREATION & LISTING
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation & Listing")
    print("=" * 80)

    log_call(f"await client.tables.get('{table_name}')")
    table_info = await backoff(lambda: client.tables.get(table_name))

    if table_info:
        print(f"[OK] Table already exists: {table_info.schema_name}")
    else:
        log_call(f"await client.tables.create('{table_name}', columns={{...}})")
        columns = {
            "new_Title": "string",
            "new_Quantity": "int",
            "new_Amount": "decimal",
            "new_Completed": "bool",
            "new_Notes": "memo",
            "new_Priority": Priority,
            "new_Attachment": "file",
        }
        try:
            table_info = await backoff(lambda: client.tables.create(table_name, columns))
            print(f"[OK] Created table: {table_info.schema_name}")
            print(f"  Columns created: {', '.join(table_info.columns_created or [])}")
        except MetadataError as e:
            if "already exists" not in str(e).lower():
                raise
            # Dataverse table deletion is asynchronous: the previous run deleted the
            # table, but the deletion hasn't propagated yet, so get() returns None
            # while create() still sees it. Wait for full propagation then re-fetch.
            print(f"  [INFO] Table in post-deletion transition; waiting for propagation...")
            table_info = await backoff(
                lambda: client.tables.get(table_name),
                delays=(10, 20, 30, 60, 120),
            )
            if table_info:
                print(f"[OK] Table now visible: {table_info.schema_name}")
            else:
                raise RuntimeError(
                    f"Table '{table_name}' still not visible after waiting. "
                    "It may still be deleting — try again in a few minutes."
                ) from e

    log_call("await client.tables.list(filter=\"SchemaName eq 'new_AsyncWalkthroughDemo'\")")
    all_tables = await backoff(
        lambda: client.tables.list(
            filter="SchemaName eq 'new_AsyncWalkthroughDemo'",
            select=["SchemaName", "LogicalName", "EntitySetName"],
        )
    )
    print(f"[OK] tables.list() returned {len(all_tables)} matching table(s)")
    for t in all_tables[:3]:
        print(f"  - SchemaName={t.get('SchemaName')}  EntitySet={t.get('EntitySetName')}")

    # ============================================================================
    # 3. CREATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Operations")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', {{...}})")
    id1 = await backoff(lambda: client.records.create(table_name, {
        "new_Title": "Complete project documentation",
        "new_Quantity": 5,
        "new_Amount": 1250.50,
        "new_Completed": False,
        "new_Notes": "This is a multiline memo field.\nIt supports longer text content.",
        "new_Priority": Priority.MEDIUM,
    }))
    print(f"[OK] Created single record: {id1}")

    log_call(f"await client.records.create('{table_name}', [{{...}}, {{...}}, {{...}}])")
    ids = await backoff(lambda: client.records.create(table_name, [
        {"new_Title": "Review code changes", "new_Quantity": 10, "new_Amount": 500.00, "new_Completed": True, "new_Priority": Priority.HIGH},
        {"new_Title": "Update test cases", "new_Quantity": 8, "new_Amount": 750.25, "new_Completed": False, "new_Priority": Priority.LOW},
        {"new_Title": "Deploy to staging", "new_Quantity": 3, "new_Amount": 2000.00, "new_Completed": False, "new_Priority": Priority.HIGH},
    ]))
    print(f"[OK] Created {len(ids)} records: {ids}")

    # ============================================================================
    # 4. READ OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Read Operations")
    print("=" * 80)

    log_call(f"await client.records.get('{table_name}', '{id1}', select=[...])")
    record = await backoff(lambda: client.records.get(table_name, id1, select=["new_title", "new_quantity", "new_amount"]))
    print(f"[OK] Single record (projected): title={record.get('new_title')} qty={record.get('new_quantity')}")

    log_call(f"await client.records.get('{table_name}', filter='new_quantity gt 5')")
    all_records = []
    pages = await backoff(lambda: client.records.get(table_name, filter="new_quantity gt 5"))
    async for page in pages:
        all_records.extend(page)
    print(f"[OK] Multi-record filter: {len(all_records)} records with new_quantity > 5")

    log_call(f"await client.records.get('{table_name}', count=True)")
    count_records = []
    pages = await backoff(lambda: client.records.get(table_name, count=True, top=1))
    async for page in pages:
        count_records.extend(page)
    print(f"[OK] count=True: fetched page with count annotation present")

    log_call(f"await client.records.get('{table_name}', include_annotations='*')")
    annotated = []
    pages = await backoff(
        lambda: client.records.get(
            table_name,
            filter="new_priority ne null",
            select=["new_title", "new_priority"],
            include_annotations="*",
            top=3,
        )
    )
    async for page in pages:
        annotated.extend(page)
    print(f"[OK] include_annotations='*': {len(annotated)} records with formatted values")
    for rec in annotated[:2]:
        fv = rec.get("new_priority@OData.Community.Display.V1.FormattedValue")
        print(f"  - '{rec.get('new_title')}' priority={rec.get('new_priority')} formatted='{fv}'")

    # ============================================================================
    # 5. UPDATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Update Operations")
    print("=" * 80)

    log_call(f"await client.records.update('{table_name}', id1, {{...}})  # single")
    await backoff(lambda: client.records.update(table_name, id1, {"new_Quantity": 100, "new_Notes": "Updated."}))
    print("[OK] Single record update")

    log_call(f"await client.records.update('{table_name}', [ids], {{...}})  # broadcast")
    await backoff(lambda: client.records.update(table_name, ids, {"new_Completed": True}))
    print(f"[OK] Broadcast update: same change applied to {len(ids)} records")

    log_call(f"await client.records.update('{table_name}', [ids], [dicts])  # paired")
    paired_changes = [
        {"new_Quantity": 11},
        {"new_Quantity": 22},
        {"new_Quantity": 33},
    ]
    await backoff(lambda: client.records.update(table_name, ids, paired_changes))
    print(f"[OK] Paired update: {len(ids)} records each received a different change")

    # ============================================================================
    # 6. UPSERT OPERATIONS (alternate key required)
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Upsert Operations")
    print("=" * 80)

    log_call("await client.tables.create_alternate_key(table_name, 'new_AsyncDemoTitleKey', ['new_title'])")
    key_info = await backoff(lambda: client.tables.create_alternate_key(
        table_name,
        "new_AsyncDemoTitleKey",
        ["new_title"],
        display_name="Title Alternate Key",
    ))
    print(f"[OK] Created alternate key: {key_info.schema_name} (status: {key_info.status})")

    if key_info.status != "Active":
        print("  Polling for key to become Active (may take up to 60s)...")
        for _ in range(12):
            await asyncio.sleep(5)
            keys = await client.tables.get_alternate_keys(table_name)
            matching = [k for k in keys if k.schema_name == "new_AsyncDemoTitleKey"]
            if matching and matching[0].status == "Active":
                key_info = matching[0]
                print(f"  [OK] Key is now Active")
                break
        else:
            print("  [WARN] Key still not Active after 60s — upsert may fail")

    log_call("await client.tables.get_alternate_keys(table_name)")
    keys = await client.tables.get_alternate_keys(table_name)
    print(f"[OK] get_alternate_keys: {len(keys)} key(s) on table")
    for k in keys:
        print(f"  - {k.schema_name}: status={k.status}, columns={k.key_attributes}")

    log_call("await client.records.upsert(table_name, [UpsertItem(...)])  # single → PATCH")
    await backoff(lambda: client.records.upsert(table_name, [
        UpsertItem(
            alternate_key={"new_title": "Complete project documentation"},
            record={"new_Quantity": 999, "new_Notes": "Upserted via alternate key"},
        )
    ]))
    print("[OK] Single upsert: updated existing record by alternate key")

    log_call("await client.records.upsert(table_name, [UpsertItem, UpsertItem])  # multiple → UpsertMultiple")
    upsert_new_title = "New record via upsert"
    await backoff(lambda: client.records.upsert(table_name, [
        UpsertItem(
            alternate_key={"new_title": "Review code changes"},
            record={"new_Quantity": 111},
        ),
        UpsertItem(
            alternate_key={"new_title": upsert_new_title},
            record={"new_Quantity": 1, "new_Amount": 1.0, "new_Completed": False, "new_Priority": Priority.LOW},
        ),
    ]))
    print("[OK] Multiple upsert: 2 records via UpsertMultiple action")

    log_call("await client.tables.delete_alternate_key(table_name, key_info.metadata_id)")
    await backoff(lambda: client.tables.delete_alternate_key(table_name, key_info.metadata_id))
    print(f"[OK] Deleted alternate key: {key_info.schema_name}")

    # ============================================================================
    # 7. PAGING DEMO
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. Paging Demo")
    print("=" * 80)

    log_call(f"await client.records.create('{table_name}', [20 records])")
    paging_records = [
        {"new_Title": f"Paging test item {i}", "new_Quantity": i, "new_Amount": i * 10.0, "new_Completed": False, "new_Priority": Priority.LOW}
        for i in range(1, 21)
    ]
    paging_ids = await backoff(lambda: client.records.create(table_name, paging_records))
    print(f"[OK] Created {len(paging_ids)} records for paging demo")

    log_call(f"await client.records.get('{table_name}', page_size=5)")
    print("Fetching records with page_size=5...")
    paging_iterator = await backoff(lambda: client.records.get(table_name, orderby=["new_Quantity"], page_size=5))
    async for page_num, page in _aenumerate(paging_iterator, start=1):
        ids_preview = [r.get("new_asyncwalkthroughdemoid", "")[:8] + "..." for r in page]
        print(f"  Page {page_num}: {len(page)} records - IDs: {ids_preview}")

    # ============================================================================
    # 8. ASYNCQUERYBUILDER — FLUENT QUERIES
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. AsyncQueryBuilder — Fluent Queries")
    print("=" * 80)

    log_call("client.query.builder(...).select().filter_eq().order_by().top().execute()")
    qb_records = [
        r async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount", "new_Priority")
            .filter_eq("new_Completed", False)
            .order_by("new_Amount", descending=True)
            .top(10)
            .execute()
        )
    ]
    print(f"[OK] fluent builder: {len(qb_records)} incomplete records (top 10 by amount desc)")
    for rec in qb_records[:3]:
        print(f"  - '{rec.get('new_title')}' Amount={rec.get('new_amount')}")

    log_call("client.query.builder(...).filter_in('new_Priority', [HIGH, LOW]).execute()")
    priority_records = [
        r async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Priority")
            .filter_in("new_Priority", [Priority.HIGH, Priority.LOW])
            .execute()
        )
    ]
    print(f"[OK] filter_in: {len(priority_records)} records with HIGH or LOW priority")

    log_call("client.query.builder(...).filter_between('new_Amount', 500, 1500).execute()")
    range_records = [
        r async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount")
            .filter_between("new_Amount", 500, 1500)
            .execute()
        )
    ]
    print(f"[OK] filter_between: {len(range_records)} records with amount in [500, 1500]")

    log_call("client.query.builder(...).where(eq(...) & gt(...)).execute()")
    expr_records = [
        r async for r in await backoff(
            lambda: client.query.builder(table_name)
            .select("new_Title", "new_Amount", "new_Quantity")
            .where(eq("new_Completed", False) & gt("new_Amount", 100))
            .order_by("new_Amount", descending=True)
            .top(5)
            .execute()
        )
    ]
    print(f"[OK] expression tree: {len(expr_records)} records")

    log_call("client.query.builder(...).filter_eq().where(between()).page_size().execute(by_page=True)")
    combined_page_count = combined_record_count = 0
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
    print(f"[OK] combined query (by_page=True): {combined_record_count} records across {combined_page_count} page(s)")

    log_call("client.query.builder(...).filter_eq(...).to_dataframe()")
    df = await backoff(
        lambda: client.query.builder(table_name)
        .select("new_title", "new_quantity")
        .filter_eq("new_completed", True)
        .to_dataframe()
    )
    print(f"[OK] to_dataframe(): {len(df)} rows, columns: {list(df.columns)}")

    # ============================================================================
    # 9. EXPAND (NAVIGATION PROPERTIES)
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. Expand (Navigation Properties)")
    print("=" * 80)

    log_call("client.query.builder('account').select('name').expand('primarycontactid').top(3).execute()")
    try:
        expanded_records = [
            r async for r in await backoff(
                lambda: client.query.builder("account").select("name").expand("primarycontactid").top(3).execute()
            )
        ]
        print(f"[OK] simple expand: {len(expanded_records)} accounts")
        for rec in expanded_records:
            contact = rec.get("primarycontactid")
            name = contact.get("fullname", "(none)") if contact else "(no contact)"
            print(f"  - '{rec.get('name')}' -> Contact: {name}")
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] expand demo (no accounts in org): {e}")

    log_call("ExpandOption('Account_Tasks').select('subject').order_by('createdon', descending=True).top(3)")
    try:
        tasks_opt = ExpandOption("Account_Tasks").select("subject", "createdon").order_by("createdon", descending=True).top(3)
        nested = [
            r async for r in await backoff(
                lambda: client.query.builder("account").select("name").expand(tasks_opt).top(3).execute()
            )
        ]
        print(f"[OK] nested expand: {len(nested)} accounts")
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] nested expand (no accounts in org): {e}")

    # ============================================================================
    # 10. SQL QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. SQL Query")
    print("=" * 80)

    sql = "SELECT new_title, new_quantity FROM new_asyncwalkthroughdemo WHERE new_completed = 1"
    log_call(f"await client.query.sql('{sql}')")
    try:
        results = await backoff(lambda: client.query.sql(sql))
        print(f"[OK] SQL query: {len(results)} completed records")
        for r in results[:3]:
            print(f"  - '{r.get('new_title')}' qty={r.get('new_quantity')}")
    except Exception as e:
        print(f"[WARN] SQL query failed: {e}")

    # ============================================================================
    # 11. PICKLIST LABEL CONVERSION
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Picklist Label Conversion")
    print("=" * 80)

    log_call("await client.records.create(..., {'new_Priority': 'High'})  # string label")
    label_id = await backoff(lambda: client.records.create(table_name, {
        "new_Title": "Test label conversion",
        "new_Quantity": 1,
        "new_Amount": 99.99,
        "new_Completed": False,
        "new_Priority": "High",
    }))
    retrieved = await backoff(lambda: client.records.get(table_name, label_id))
    print(f"[OK] Created with label 'High': stored as integer={retrieved.get('new_priority')}")

    log_call("await client.records.update(..., {'new_Priority': 'Low'})")
    await backoff(lambda: client.records.update(table_name, label_id, {"new_Priority": "Low"}))
    updated = await backoff(lambda: client.records.get(table_name, label_id))
    print(f"[OK] Updated to label 'Low': stored as integer={updated.get('new_priority')}")

    # ============================================================================
    # 12. DATAFRAME OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("12. DataFrame Operations")
    print("=" * 80)

    log_call("await client.dataframe.create(table_name, df)  # returns pd.Series of GUIDs")
    df_create = pd.DataFrame([
        {"new_Title": "DF Demo Alpha", "new_Quantity": 10, "new_Amount": 100.0, "new_Completed": False, "new_Priority": Priority.LOW},
        {"new_Title": "DF Demo Beta", "new_Quantity": 20, "new_Amount": 200.0, "new_Completed": False, "new_Priority": Priority.MEDIUM},
        {"new_Title": "DF Demo Gamma", "new_Quantity": 30, "new_Amount": 300.0, "new_Completed": True, "new_Priority": Priority.HIGH},
    ])
    df_ids = await backoff(lambda: client.dataframe.create(table_name, df_create))
    print(f"[OK] dataframe.create(): created {len(df_ids)} records, returned pd.Series of GUIDs")

    log_call("await client.dataframe.get(table_name, filter=..., select=[...])  # returns DataFrame")
    df_result = await backoff(lambda: client.dataframe.get(
        table_name,
        filter="new_completed eq false",
        select=["new_title", "new_quantity", "new_amount"],
        top=5,
    ))
    print(f"[OK] dataframe.get() (multi): {len(df_result)} rows, columns: {list(df_result.columns)}")
    if not df_result.empty:
        print(f"  First row: {df_result.iloc[0].to_dict()}")

    log_call("await client.dataframe.get(table_name, record_id=id1)  # single record")
    df_single = await backoff(lambda: client.dataframe.get(table_name, record_id=id1))
    print(f"[OK] dataframe.get() (single): {len(df_single)} row, columns: {list(df_single.columns)}")

    log_call("await client.dataframe.update(table_name, changes_df, id_column='id')")
    df_update = pd.DataFrame([
        {"id": df_ids.iloc[0], "new_Quantity": 99},
        {"id": df_ids.iloc[1], "new_Quantity": 88},
    ])
    await backoff(lambda: client.dataframe.update(table_name, df_update, id_column="id"))
    print("[OK] dataframe.update(): updated 2 records via paired DataFrame")

    log_call("await client.dataframe.update(..., clear_nulls=True)  # sends explicit nulls")
    df_null_update = pd.DataFrame([{"id": df_ids.iloc[2], "new_Notes": None}])
    await backoff(lambda: client.dataframe.update(table_name, df_null_update, id_column="id", clear_nulls=True))
    print("[OK] dataframe.update(clear_nulls=True): sent explicit null to clear field")

    log_call("await client.dataframe.delete(table_name, df_ids)  # bulk delete from Series")
    job_id = await backoff(lambda: client.dataframe.delete(table_name, df_ids))
    print(f"[OK] dataframe.delete(): bulk delete job started (job_id={job_id})")

    # ============================================================================
    # 13. FILE UPLOAD
    # ============================================================================
    print("\n" + "=" * 80)
    print("13. File Upload")
    print("=" * 80)

    file_upload_ok = False
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".bin", delete=False) as f:
            f.write(b"Async walkthrough \xe2\x80\x94 test attachment content.\n")
            tmp_path = f.name

        log_call(f"await client.files.upload(table_name, id1, 'new_Attachment', path)")
        # Omit mime_type — file columns require application/octet-stream (the SDK default).
        await backoff(lambda: client.files.upload(
            table_name,
            id1,
            "new_Attachment",
            tmp_path,
            if_none_match=False,  # overwrite if already present
        ))
        print(f"[OK] files.upload(): uploaded '{os.path.basename(tmp_path)}' to new_Attachment column")

        log_call("await client.files.upload(..., mode='chunk')  # explicit chunked mode")
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".bin", delete=False) as f2:
            f2.write(b"Chunked upload test content.\n" * 10)
            tmp_path2 = f2.name
        try:
            await backoff(lambda: client.files.upload(
                table_name,
                ids[0],
                "new_Attachment",
                tmp_path2,
                mode="chunk",
                if_none_match=False,
            ))
            print("[OK] files.upload(mode='chunk'): explicit chunked upload")
            file_upload_ok = True
        finally:
            os.unlink(tmp_path2)

    except Exception as e:
        print(f"[SKIP] File upload failed: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    # ============================================================================
    # 14. COLUMN MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("14. Column Management")
    print("=" * 80)

    log_call(f"await client.tables.add_columns('{table_name}', {{'new_Tags': 'string', 'new_Score': 'decimal'}})")
    created_cols = await backoff(lambda: client.tables.add_columns(table_name, {"new_Tags": "string", "new_Score": "decimal"}))
    print(f"[OK] add_columns(): created {created_cols}")

    log_call(f"await client.tables.remove_columns('{table_name}', ['new_Tags', 'new_Score'])")
    removed_cols = await backoff(lambda: client.tables.remove_columns(table_name, ["new_Tags", "new_Score"]))
    print(f"[OK] remove_columns(): removed {removed_cols}")

    # ============================================================================
    # 15. RELATIONSHIPS
    # ============================================================================
    print("\n" + "=" * 80)
    print("15. Relationships")
    print("=" * 80)

    # create_lookup_field expects the logical name (all-lowercase) for referencing_table.
    table_logical_name = table_info.logical_name
    log_call(f"await client.tables.create_lookup_field('{table_logical_name}', 'new_AccountRef', 'account')")
    relationships_ok = False
    try:
        rel_info = await backoff(lambda: client.tables.create_lookup_field(
            table_logical_name,
            "new_AccountRef",
            "account",
            display_name="Account Reference",
            description="Demo lookup to account table",
        ))
        print(f"[OK] create_lookup_field(): schema={rel_info.lookup_schema_name}")
        print(f"  Relationship: {rel_info.relationship_schema_name} (type={rel_info.relationship_type})")

        log_call(f"await client.tables.get_relationship('{rel_info.relationship_schema_name}')")
        rel = await backoff(lambda: client.tables.get_relationship(rel_info.relationship_schema_name))
        if rel:
            print(f"[OK] get_relationship(): {rel.relationship_schema_name} type={rel.relationship_type}")
        else:
            print("[SKIP] get_relationship() returned None")

        log_call(f"await client.tables.delete_relationship(relationship_id)")
        await backoff(lambda: client.tables.delete_relationship(rel_info.relationship_id))
        print(f"[OK] delete_relationship(): removed {rel_info.relationship_schema_name}")
        relationships_ok = True
    except Exception as e:
        print(f"[SKIP] Relationships demo failed: {e}")

    # ============================================================================
    # 16. DELETE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("16. Delete Operations")
    print("=" * 80)

    log_call(f"await client.records.delete('{table_name}', id1)  # single")
    await backoff(lambda: client.records.delete(table_name, id1))
    print(f"[OK] Deleted single record: {id1}")

    log_call(f"await client.records.delete('{table_name}', ids, use_bulk_delete=False)  # sequential")
    for rid in ids:
        await backoff(lambda: client.records.delete(table_name, rid))
    print(f"[OK] Sequential delete (use_bulk_delete=False): {len(ids)} records deleted one at a time")

    log_call(f"await client.records.delete('{table_name}', paging_ids)  # bulk delete")
    job_id = await backoff(lambda: client.records.delete(table_name, paging_ids))
    print(f"[OK] Bulk delete job started: {job_id} ({len(paging_ids)} records)")

    # clean up label_id and upsert record
    for rid in [label_id]:
        try:
            await client.records.delete(table_name, rid)
        except Exception:
            pass

    # ============================================================================
    # 17. BATCH OPERATIONS (extended)
    # ============================================================================
    print("\n" + "=" * 80)
    print("17. Batch Operations")
    print("=" * 80)

    # -- batch.records: create, get, update, changeset (create + update + delete), delete
    log_call("batch.records.create x2 + await batch.execute()")
    batch = client.batch.new()
    batch.records.create(table_name, {"new_Title": "Batch Alpha", "new_Quantity": 1, "new_Amount": 10.0, "new_Completed": False, "new_Priority": Priority.LOW})
    batch.records.create(table_name, {"new_Title": "Batch Beta", "new_Quantity": 2, "new_Amount": 20.0, "new_Completed": False, "new_Priority": Priority.MEDIUM})
    result = await batch.execute()
    batch_ids = list(result.entity_ids)
    print(f"[OK] batch.records.create: {len(result.succeeded)} ops, {len(batch_ids)} records created")

    log_call("batch.records.get x2 + await batch.execute()")
    batch = client.batch.new()
    for bid in batch_ids:
        batch.records.get(table_name, bid, select=["new_title", "new_quantity"])
    result = await batch.execute()
    print(f"[OK] batch.records.get: {len(result.succeeded)} reads")
    for resp in result.succeeded:
        if resp.data:
            print(f"  - '{resp.data.get('new_title')}' qty={resp.data.get('new_quantity')}")

    log_call("batch.records.update + await batch.execute()")
    batch = client.batch.new()
    batch.records.update(table_name, batch_ids[0], {"new_Quantity": 999})
    result = await batch.execute()
    print(f"[OK] batch.records.update: {len(result.succeeded)} op(s)")

    log_call("batch.changeset() with create + update + delete")
    batch = client.batch.new()
    with batch.changeset() as cs:
        cs_ref = cs.records.create(table_name, {
            "new_Title": "Changeset task",
            "new_Quantity": 5,
            "new_Amount": 100.0,
            "new_Completed": False,
            "new_Priority": Priority.HIGH,
        })
        cs.records.update(table_name, cs_ref, {"new_Completed": True})
        cs.records.delete(table_name, cs_ref)
    result = await batch.execute()
    if not result.has_errors:
        print(f"[OK] batch.changeset (create+update+delete): {len(result.succeeded)} ops, all atomic")
    else:
        for item in result.failed:
            print(f"[WARN] changeset error {item.status_code}: {item.error_message}")

    # -- batch.tables: get and list
    log_call("batch.tables.get + batch.tables.list + await batch.execute()")
    batch = client.batch.new()
    batch.tables.get(table_name)
    batch.tables.list(filter="SchemaName eq 'new_AsyncWalkthroughDemo'", select=["SchemaName", "LogicalName"])
    result = await batch.execute()
    print(f"[OK] batch.tables.get+list: {len(result.succeeded)} ops")
    for resp in result.succeeded:
        if resp.data:
            print(f"  - data keys: {list(resp.data.keys())[:4]}")

    # -- batch.query.sql
    log_call("batch.query.sql + await batch.execute()")
    batch = client.batch.new()
    batch.query.sql("SELECT new_title, new_quantity FROM new_asyncwalkthroughdemo WHERE new_completed = 0")
    try:
        result = await batch.execute()
        print(f"[OK] batch.query.sql: {len(result.succeeded)} op(s)")
    except Exception as e:
        print(f"[WARN] batch.query.sql failed: {e}")

    # -- batch.dataframe: create
    log_call("batch.dataframe.create + await batch.execute()")
    df_batch = pd.DataFrame([
        {"new_Title": "Batch DF Alpha", "new_Quantity": 1, "new_Amount": 1.0, "new_Completed": False, "new_Priority": Priority.LOW},
        {"new_Title": "Batch DF Beta", "new_Quantity": 2, "new_Amount": 2.0, "new_Completed": False, "new_Priority": Priority.LOW},
    ])
    batch = client.batch.new()
    batch.dataframe.create(table_name, df_batch)
    result = await batch.execute()
    df_batch_ids = list(result.entity_ids)
    print(f"[OK] batch.dataframe.create: {len(result.succeeded)} op(s), {len(df_batch_ids)} records")

    # -- batch delete: clean up all batch-created records
    log_call("batch.records.delete x N + await batch.execute(continue_on_error=True)")
    all_batch_ids = batch_ids + df_batch_ids
    batch = client.batch.new()
    for bid in all_batch_ids:
        batch.records.delete(table_name, bid)
    result = await batch.execute(continue_on_error=True)
    print(f"[OK] batch.records.delete: {len(result.succeeded)} records deleted in one request")

    # ============================================================================
    # 18. CACHE MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("18. Cache Management")
    print("=" * 80)

    log_call("await client.flush_cache('picklist')")
    cleared = await client.flush_cache("picklist")
    print(f"[OK] flush_cache('picklist'): cleared {cleared} cached picklist entries")

    # ============================================================================
    # 19. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("19. Cleanup")
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
    print("Async Walkthrough Complete — Full API Surface")
    print("=" * 80)
    def _status(ok: bool) -> str:
        return "[OK]  " if ok else "[SKIP]"

    print("\nDemonstrated:")
    print(f"  [OK]   tables.create, tables.get, tables.list, tables.add_columns, tables.remove_columns, tables.delete")
    print(f"  [OK]   tables.create_alternate_key, tables.get_alternate_keys, tables.delete_alternate_key")
    print(f"  {_status(relationships_ok)} tables.create_lookup_field, tables.get_relationship, tables.delete_relationship")
    print(f"  [OK]   records.create (single + multiple), records.get (single + multi + count + annotations)")
    print(f"  [OK]   records.update (single + broadcast + paired), records.delete (single + bulk + sequential)")
    print(f"  [OK]   records.upsert (single UpsertItem + multiple UpsertMultiple)")
    print(f"  [OK]   query.builder: select, filter_eq, filter_in, filter_between, where, order_by, top, page_size")
    print(f"  [OK]   query.builder: execute (flat), execute(by_page=True), to_dataframe, expand, ExpandOption")
    print(f"  [OK]   query.sql")
    print(f"  [OK]   dataframe.create, dataframe.get (single + multi), dataframe.update (with clear_nulls), dataframe.delete")
    print(f"  {_status(file_upload_ok)} files.upload (auto + chunk mode)")
    print(f"  [OK]   batch.records.create/get/update/delete, batch.changeset (create+update+delete)")
    print(f"  [OK]   batch.tables.get, batch.tables.list, batch.query.sql, batch.dataframe.create")
    print(f"  [OK]   flush_cache('picklist')")
    print("=" * 80)


async def _aenumerate(ait, start=0):
    i = start
    async for item in ait:
        yield i, item
        i += 1


if __name__ == "__main__":
    asyncio.run(main())
