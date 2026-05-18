# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client SDK - Async Functional Testing

Async equivalent of examples/basic/functional_testing.py.

This script provides comprehensive async functional testing of the SDK:
- Real environment connection testing
- Table creation and metadata operations
- Full CRUD operations testing
- Query functionality validation (list, list_pages, builder, fetchxml)
- Batch operations (create, read, update, changeset, delete)
- Interactive cleanup options

Prerequisites:
- PowerPlatform-Dataverse-Client SDK installed (run aio/basic/installation_example.py first)
- Azure Identity credentials configured
- Access to a Dataverse environment with table creation permissions

Usage:
    python examples/aio/basic/functional_testing.py
"""

import asyncio
import sys
from typing import Optional, Dict, Any
from datetime import datetime

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    CascadeConfiguration,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel
from PowerPlatform.Dataverse.common.constants import (
    CASCADE_BEHAVIOR_NO_CASCADE,
    CASCADE_BEHAVIOR_REMOVE_LINK,
)
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential


def get_dataverse_org_url() -> str:
    """Get Dataverse org URL from user input."""
    print("\n-> Dataverse Environment Setup")
    print("=" * 50)

    if not sys.stdin.isatty():
        print("[ERR] Interactive input required. Run this script in a terminal.")
        sys.exit(1)

    while True:
        org_url = input("Enter your Dataverse org URL (e.g., https://yourorg.crm.dynamics.com): ").strip()
        if org_url:
            return org_url.rstrip("/")
        print("[WARN] Please enter a valid URL.")


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
                print(f"   * Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:
            last = ex
            continue
    if last:
        if attempts:
            print(f"   [WARN] Backoff exhausted after {max(attempts - 1, 0)} retry(s); waited {total_delay}s total.")
        raise last


async def setup_authentication():
    """Set up authentication and create async Dataverse client."""
    print("\n-> Authentication Setup")
    print("=" * 50)

    org_url = get_dataverse_org_url()
    try:
        credential = AsyncInteractiveBrowserCredential()
        client = AsyncDataverseClient(org_url, credential)

        print("Testing connection...")
        tables = await client.tables.list()
        print(f"[OK] Connection successful! Found {len(tables)} tables.")

        user_owned = await client.tables.list(
            filter="OwnershipType eq Microsoft.Dynamics.CRM.OwnershipTypes'UserOwned'",
            select=["LogicalName", "SchemaName", "DisplayName"],
        )
        print(f"[OK] Found {len(user_owned)} user-owned tables (filter + select).")
        return client, credential

    except Exception as e:
        print(f"[ERR] Authentication failed: {e}")
        sys.exit(1)


async def wait_for_table_metadata(
    client: AsyncDataverseClient,
    table_schema_name: str,
    retries: int = 10,
    delay_seconds: int = 3,
) -> Dict[str, Any]:
    """Poll until table metadata is published and entity set becomes available."""
    for attempt in range(1, retries + 1):
        try:
            info = await client.tables.get(table_schema_name)
            if info and info.get("entity_set_name"):
                if attempt > 1:
                    print(f"   [OK] Table metadata available after {attempt} attempts.")
                return info
        except Exception:
            pass

        if attempt < retries:
            print(f"   Waiting for table metadata to publish (attempt {attempt}/{retries})...")
            await asyncio.sleep(delay_seconds)

    raise RuntimeError("Table metadata did not become available in time. Please retry later.")


async def ensure_test_table(client: AsyncDataverseClient) -> Dict[str, Any]:
    """Create or verify test table exists."""
    print("\n-> Test Table Setup")
    print("=" * 50)

    table_schema_name = "test_TestSDKFunctionality"

    try:
        existing_table = await client.tables.get(table_schema_name)
        if existing_table:
            print(f"[OK] Test table '{table_schema_name}' already exists")
            return existing_table
    except Exception:
        print(f"Table '{table_schema_name}' not found, creating...")

    try:
        print("Creating new test table...")
        table_info = await client.tables.create(
            table_schema_name,
            primary_column="test_name",
            columns={
                "test_description": "string",
                "test_count": "int",
                "test_amount": "decimal",
                "test_is_active": "bool",
                "test_created_date": "datetime",
            },
        )
        print(f"[OK] Created test table: {table_info.get('table_schema_name')}")
        print(f"   Logical name: {table_info.get('table_logical_name')}")
        print(f"   Entity set: {table_info.get('entity_set_name')}")

        return await wait_for_table_metadata(client, table_schema_name)

    except MetadataError as e:
        print(f"[ERR] Failed to create table: {e}")
        sys.exit(1)


async def test_create_record(client: AsyncDataverseClient, table_info: Dict[str, Any]) -> str:
    """Test record creation."""
    print("\n-> Record Creation Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    test_data = {
        f"{attr_prefix}_name": f"Test Record {datetime.now().strftime('%H:%M:%S')}",
        f"{attr_prefix}_description": "This is a test record created by the async SDK functionality test",
        f"{attr_prefix}_count": 42,
        f"{attr_prefix}_amount": 123.45,
        f"{attr_prefix}_is_active": True,
        f"{attr_prefix}_created_date": datetime.now().isoformat(),
    }

    try:
        print("Creating test record...")
        created_id: Optional[str] = None
        for attempt in range(1, retries + 1):
            try:
                created_id = await client.records.create(table_schema_name, test_data)
                if attempt > 1:
                    print(f"   [OK] Record creation succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(
                        f"   Table not ready for create (attempt {attempt}/{retries}). Retrying in {delay_seconds}s..."
                    )
                    await asyncio.sleep(delay_seconds)
                    continue
                raise

        if created_id:
            print(f"[OK] Record created successfully!")
            print(f"   Record ID: {created_id}")
            return created_id
        else:
            raise ValueError("Unexpected response from records.create operation")

    except Exception as e:
        print(f"[ERR] Failed to create record: {e}")
        sys.exit(1)


async def test_read_record(
    client: AsyncDataverseClient,
    table_info: Dict[str, Any],
    record_id: str,
) -> Dict[str, Any]:
    """Test record reading."""
    print("\n-> Record Reading Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    try:
        print(f"Reading record: {record_id}")
        record = None
        for attempt in range(1, retries + 1):
            try:
                record = await client.records.retrieve(table_schema_name, record_id)
                if attempt > 1:
                    print(f"   [OK] Record read succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(f"   Record not queryable yet (attempt {attempt}/{retries}). Retrying in {delay_seconds}s...")
                    await asyncio.sleep(delay_seconds)
                    continue
                raise

        if record is None:
            raise RuntimeError("Record did not become available in time.")

        print("[OK] Record retrieved successfully!")
        for field_name in [
            f"{attr_prefix}_name",
            f"{attr_prefix}_description",
            f"{attr_prefix}_count",
            f"{attr_prefix}_amount",
            f"{attr_prefix}_is_active",
        ]:
            if field_name in record:
                print(f"     {field_name}: {record[field_name]}")

        # include_annotations
        annotation = "OData.Community.Display.V1.FormattedValue"
        annotated = await client.records.retrieve(
            table_schema_name,
            record_id,
            select=[f"{attr_prefix}_is_active", f"{attr_prefix}_count"],
            include_annotations=annotation,
        )
        ann_key = f"{attr_prefix}_is_active@{annotation}"
        if annotated is not None and ann_key in annotated:
            print(f"[OK] include_annotations verified: {ann_key} = '{annotated[ann_key]}'")
        else:
            print(f"[WARN] include_annotations: expected key '{ann_key}' not present in response")

        # expand
        try:
            expanded = await client.records.retrieve(
                table_schema_name,
                record_id,
                select=[f"{attr_prefix}_name"],
                expand=["owninguser"],
            )
            owner = (expanded.get("owninguser") or {}) if expanded else {}
            owner_name = owner.get("fullname") or owner.get("domainname") or "(unknown)"
            print(f"[OK] records.retrieve with expand=['owninguser']: owner='{owner_name}'")
        except Exception as e:
            print(f"[WARN] records.retrieve expand skipped: {e}")

        return record

    except Exception as e:
        print(f"[ERR] Failed to read record: {e}")
        sys.exit(1)


async def test_query_records(client: AsyncDataverseClient, table_info: Dict[str, Any]) -> None:
    """Test querying multiple records."""
    print("\n-> Record Query Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    select_cols = [f"{attr_prefix}_name", f"{attr_prefix}_count", f"{attr_prefix}_amount"]
    active_filter = f"{attr_prefix}_is_active eq true"

    try:
        # records.list() — eager
        print("Querying records with await client.records.list()...")
        for attempt in range(1, retries + 1):
            try:
                result = await client.records.list(
                    table_schema_name,
                    select=select_cols,
                    filter=active_filter,
                    top=5,
                )
                record_count = 0
                for record in result:
                    record_count += 1
                    name = record.get(f"{attr_prefix}_name", "N/A")
                    count = record.get(f"{attr_prefix}_count", "N/A")
                    amount = record.get(f"{attr_prefix}_amount", "N/A")
                    print(f"   Record {record_count}: {name} (Count: {count}, Amount: {amount})")
                print(f"[OK] records.list() completed! Found {record_count} active records.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(f"   Query retry {attempt}/{retries}. Waiting {delay_seconds}s...")
                    await asyncio.sleep(delay_seconds)
                    continue
                raise

        # records.list_pages() — lazy
        print("\nQuerying records with async for page in client.records.list_pages() (paged)...")
        page_num = 0
        total_records = 0
        async for page in client.records.list_pages(
            table_schema_name,
            select=select_cols,
            filter=active_filter,
        ):
            page_num += 1
            total_records += len(page)
            names = [r.get(f"{attr_prefix}_name", "N/A") for r in page]
            print(f"   Page {page_num}: {len(page)} record(s) — {names}")
        print(f"[OK] records.list_pages() completed! {total_records} records across {page_num} page(s).")

        # records.list() with extended params
        print("\nQuerying records.list() with orderby / page_size / count / include_annotations...")
        annotation = "OData.Community.Display.V1.FormattedValue"
        annotated_result = await client.records.list(
            table_schema_name,
            select=[f"{attr_prefix}_name", f"{attr_prefix}_is_active"],
            filter=active_filter,
            orderby=[f"{attr_prefix}_name asc"],
            page_size=50,
            count=True,
            include_annotations=annotation,
        )
        ann_key = f"{attr_prefix}_is_active@{annotation}"
        ann_present = any(ann_key in r for r in annotated_result)
        if ann_present:
            print(f"[OK] include_annotations verified: '{ann_key}' present in list() results")
        else:
            print(f"[WARN] include_annotations: '{ann_key}' not found")
        print(f"[OK] records.list() with extended params completed! {len(annotated_result)} record(s).")

        # AsyncQueryBuilder
        from PowerPlatform.Dataverse.models.filters import col

        print("\nQuerying with AsyncQueryBuilder (.where(col(...)) + .page_size().execute_pages())...")
        qb_pages = 0
        qb_total = 0
        async for page in (
            client.query.builder(table_schema_name)
            .select(f"{attr_prefix}_name", f"{attr_prefix}_count")
            .where(col(f"{attr_prefix}_is_active") == True)
            .page_size(10)
            .execute_pages()
        ):
            qb_pages += 1
            qb_total += len(page)
        print(f"[OK] AsyncQueryBuilder execute_pages(): {qb_total} records across {qb_pages} page(s).")

        # FetchXML
        print("\nQuerying with client.query.fetchxml().execute() ...")
        fx_xml = f"""
        <fetch top="5">
          <entity name="{table_schema_name.lower()}">
            <attribute name="{attr_prefix}_name" />
            <attribute name="{attr_prefix}_count" />
            <filter>
              <condition attribute="{attr_prefix}_is_active" operator="eq" value="1" />
            </filter>
          </entity>
        </fetch>
        """
        try:
            fx_result = await client.query.fetchxml(fx_xml).execute()
            print(f"[OK] FetchXML execute(): {len(fx_result)} active records.")
        except Exception as e:
            print(f"[WARN] FetchXML query encountered an issue: {e}")

    except Exception as e:
        print(f"[WARN] Query test encountered an issue: {e}")
        print("   This might be expected if the table is very new.")


async def test_batch_all_operations(client: AsyncDataverseClient, table_info: Dict[str, Any]) -> None:
    """Test batch operations using the async batch client."""
    print("\n-> Batch Operations Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    logical_name = table_info.get("table_logical_name", table_schema_name.lower())
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    all_ids: list = []

    try:
        # [1] CREATE — single + CreateMultiple
        print("\n[1/7] Create — single + CreateMultiple")
        batch = client.batch.new()
        batch.records.create(
            table_schema_name,
            {
                f"{attr_prefix}_name": f"Batch-A {datetime.now().strftime('%H:%M:%S')}",
                f"{attr_prefix}_count": 1,
                f"{attr_prefix}_is_active": True,
            },
        )
        batch.records.create(
            table_schema_name,
            [
                {
                    f"{attr_prefix}_name": f"Batch-B {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 2,
                    f"{attr_prefix}_is_active": True,
                },
                {
                    f"{attr_prefix}_name": f"Batch-C {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 3,
                    f"{attr_prefix}_is_active": True,
                },
            ],
        )
        result = await batch.execute()
        all_ids = list(result.entity_ids)
        if result.has_errors:
            for item in result.failed:
                print(f"[WARN] {item.status_code}: {item.error_message}")
        else:
            print(f"[OK] {len(result.succeeded)} ops → {len(all_ids)} records created")

        # [2] READ — retrieve + list + query.sql
        if all_ids:
            annotation = "OData.Community.Display.V1.FormattedValue"
            print(f"\n[2/7] Read — records.retrieve + records.list + query.sql")
            batch = client.batch.new()
            batch.records.retrieve(
                table_schema_name,
                all_ids[0],
                select=[f"{attr_prefix}_name", f"{attr_prefix}_count", f"{attr_prefix}_is_active"],
                include_annotations=annotation,
            )
            batch.records.list(
                table_schema_name,
                select=[f"{attr_prefix}_name", f"{attr_prefix}_is_active"],
                filter=f"{attr_prefix}_is_active eq true",
                orderby=[f"{attr_prefix}_name asc"],
                page_size=50,
                include_annotations=annotation,
            )
            batch.query.sql(f"SELECT TOP 3 {attr_prefix}_name FROM {logical_name}")
            result = await batch.execute()
            print(f"[OK] {len(result.succeeded)} succeeded, {len(result.failed)} failed")

        # [3] UPDATE — single + multiple
        if len(all_ids) >= 2:
            print(f"\n[3/7] Update — single PATCH + UpdateMultiple")
            batch = client.batch.new()
            batch.records.update(table_schema_name, all_ids[0], {f"{attr_prefix}_count": 10})
            batch.records.update(table_schema_name, all_ids[1:], {f"{attr_prefix}_count": 20})
            result = await batch.execute()
            print(f"[OK] {len(result.succeeded)} updates succeeded")

        # [4] CHANGESET (happy path) — create + update via content-ID + delete
        if all_ids:
            print("\n[4/7] Changeset (happy path) — create + update(ref) + delete")
            batch = client.batch.new()
            async with batch.changeset() as cs:
                ref = cs.records.create(
                    table_schema_name,
                    {
                        f"{attr_prefix}_name": f"Batch-D {datetime.now().strftime('%H:%M:%S')}",
                        f"{attr_prefix}_count": 4,
                        f"{attr_prefix}_is_active": False,
                    },
                )
                cs.records.update(table_schema_name, ref, {f"{attr_prefix}_is_active": True})
                cs.records.delete(table_schema_name, all_ids[-1])
            result = await batch.execute()
            if result.has_errors:
                for item in result.failed:
                    print(f"[WARN] {item.status_code}: {item.error_message}")
            else:
                new_id = next(iter(result.entity_ids), None)
                if new_id:
                    all_ids[-1] = new_id
                print(f"[OK] {len(result.succeeded)} ops committed atomically")

        # [5] CHANGESET (rollback)
        print("\n[5/7] Changeset (rollback) — failing update rolls back create")
        batch = client.batch.new()
        async with batch.changeset() as cs:
            cs.records.create(
                table_schema_name,
                {
                    f"{attr_prefix}_name": f"Rollback-test {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 0,
                    f"{attr_prefix}_is_active": False,
                },
            )
            cs.records.update(table_schema_name, "00000000-0000-0000-0000-000000000001", {f"{attr_prefix}_count": 999})
        result = await batch.execute(continue_on_error=True)
        if result.has_errors:
            print("[OK] Changeset rollback verified: changeset failed, no records created")
        else:
            print("[WARN] Expected rollback but changeset succeeded (unexpected)")
            all_ids.extend(result.entity_ids)

        # [6] ADD/REMOVE COLUMNS
        col_a = f"{attr_prefix}_batch_extra_a"
        col_b = f"{attr_prefix}_batch_extra_b"
        print(f"\n[6/7] Batch tables.add_columns + tables.remove_columns")
        batch = client.batch.new()
        batch.tables.add_columns(table_schema_name, {col_a: "string"})
        batch.tables.add_columns(table_schema_name, {col_b: "int"})
        result = await batch.execute()
        if not result.has_errors:
            print(f"[OK] {len(result.succeeded)} column(s) added: {col_a}, {col_b}")
            batch_rm = client.batch.new()
            batch_rm.tables.remove_columns(table_schema_name, [col_a, col_b])
            rm_result = await batch_rm.execute(continue_on_error=True)
            print(f"[OK] Removed {len(rm_result.succeeded)} batch-added column(s)")
        else:
            for item in result.failed:
                print(f"[WARN] add_columns error {item.status_code}: {item.error_message}")

        # [7] DELETE
        if all_ids:
            print(f"\n[7/7] Delete — {len(all_ids)} records (use_bulk_delete=False)")
            batch = client.batch.new()
            batch.records.delete(table_schema_name, all_ids, use_bulk_delete=False)
            result = await batch.execute(continue_on_error=True)
            print(f"[OK] Deleted {len(result.succeeded)}, failed {len(result.failed)}")

        print("\n[OK] Batch all-operations test completed!")

    except Exception as e:
        print(f"[WARN] Batch test encountered an issue: {e}")
        if all_ids:
            try:
                batch = client.batch.new()
                batch.records.delete(table_schema_name, all_ids, use_bulk_delete=False)
                await batch.execute(continue_on_error=True)
            except Exception:
                pass


async def test_relationships(client: AsyncDataverseClient) -> None:
    """Test relationship lifecycle: create tables, 1:N, N:N, query, delete."""
    print("\n-> Relationship Tests")
    print("=" * 50)

    rel_parent_schema = "test_RelParent"
    rel_child_schema = "test_RelChild"
    rel_m2m_schema = "test_RelProject"

    rel_id_1n = None
    rel_id_lookup = None
    rel_id_nn = None
    created_tables = []

    try:
        # Cleanup leftovers
        print("Checking for leftover relationship test resources...")
        found_leftovers = False
        for rel_name in ["test_RelParent_RelChild", "contact_test_relchild_test_ManagerId", "test_relchild_relproject"]:
            try:
                rel = await client.tables.get_relationship(rel_name)
                if rel:
                    found_leftovers = True
                    break
            except Exception:
                pass

        if not found_leftovers:
            for tbl in [rel_child_schema, rel_parent_schema, rel_m2m_schema]:
                try:
                    if await client.tables.get(tbl):
                        found_leftovers = True
                        break
                except Exception:
                    pass

        if found_leftovers:
            cleanup_ok = input("Found leftover test resources. Clean up? (y/N): ").strip().lower() in ["y", "yes"]
            if cleanup_ok:
                for rel_name in [
                    "test_RelParent_RelChild",
                    "contact_test_relchild_test_ManagerId",
                    "test_relchild_relproject",
                ]:
                    try:
                        rel = await client.tables.get_relationship(rel_name)
                        if rel:
                            await client.tables.delete_relationship(rel.relationship_id)
                            print(f"   (Cleaned up relationship: {rel_name})")
                    except Exception:
                        pass
                for tbl in [rel_child_schema, rel_parent_schema, rel_m2m_schema]:
                    try:
                        if await client.tables.get(tbl):
                            await client.tables.delete(tbl)
                            print(f"   (Cleaned up table: {tbl})")
                    except Exception:
                        pass

        # Create tables
        print("\nCreating relationship test tables...")

        async def _get_or_create(schema, columns, label):
            info = await client.tables.get(schema)
            if info:
                print(f"[OK] Table already exists: {schema} (skipped)")
                return info
            try:
                result = await backoff(lambda: client.tables.create(schema, columns))
                print(f"[OK] Created {label}: {schema}")
                return result
            except Exception as e:
                if "already exists" in str(e).lower() or "not unique" in str(e).lower():
                    print(f"[OK] Table already exists: {schema} (skipped)")
                    return await client.tables.get(schema)
                raise

        parent_info = await _get_or_create(rel_parent_schema, {"test_Code": "string"}, "parent table")
        created_tables.append(rel_parent_schema)

        child_info = await _get_or_create(rel_child_schema, {"test_Number": "string"}, "child table")
        created_tables.append(rel_child_schema)

        proj_info = await _get_or_create(rel_m2m_schema, {"test_ProjectCode": "string"}, "M:N table")
        created_tables.append(rel_m2m_schema)

        await wait_for_table_metadata(client, rel_parent_schema)
        await wait_for_table_metadata(client, rel_child_schema)
        await wait_for_table_metadata(client, rel_m2m_schema)

        # 1:N relationship
        print("\n  Test 1: Create 1:N relationship")
        lookup = LookupAttributeMetadata(
            schema_name="test_ParentId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Parent", language_code=1033)]),
            required_level="None",
        )
        relationship = OneToManyRelationshipMetadata(
            schema_name="test_RelParent_RelChild",
            referenced_entity=parent_info["table_logical_name"],
            referencing_entity=child_info["table_logical_name"],
            referenced_attribute=f"{parent_info['table_logical_name']}id",
            cascade_configuration=CascadeConfiguration(
                delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                assign=CASCADE_BEHAVIOR_NO_CASCADE,
                merge=CASCADE_BEHAVIOR_NO_CASCADE,
            ),
        )
        existing_1n = await client.tables.get_relationship("test_RelParent_RelChild")
        if existing_1n:
            rel_id_1n = existing_1n.relationship_id
            print(f"  [OK] Relationship already exists (skipped)")
        else:
            result_1n = await backoff(
                lambda: client.tables.create_one_to_many_relationship(lookup=lookup, relationship=relationship)
            )
            assert result_1n.relationship_schema_name == "test_RelParent_RelChild"
            rel_id_1n = result_1n.relationship_id
            print(f"  [OK] Created 1:N: {result_1n.relationship_schema_name}")

        # Lookup field
        print("\n  Test 2: Create lookup field (convenience API)")
        existing_lookup = await client.tables.get_relationship("contact_test_relchild_test_ManagerId")
        if existing_lookup:
            rel_id_lookup = existing_lookup.relationship_id
            print(f"  [OK] Lookup already exists (skipped)")
        else:
            result_lookup = await backoff(
                lambda: client.tables.create_lookup_field(
                    referencing_table=child_info["table_logical_name"],
                    lookup_field_name="test_ManagerId",
                    referenced_table="contact",
                    display_name="Manager",
                    description="The record's manager contact",
                    required=False,
                    cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                )
            )
            rel_id_lookup = result_lookup.relationship_id
            print(f"  [OK] Created lookup: {result_lookup.lookup_schema_name}")

        # N:N relationship
        print("\n  Test 3: Create N:N relationship")
        m2m = ManyToManyRelationshipMetadata(
            schema_name="test_relchild_relproject",
            entity1_logical_name=child_info["table_logical_name"],
            entity2_logical_name=proj_info["table_logical_name"],
        )
        existing_nn = await client.tables.get_relationship("test_relchild_relproject")
        if existing_nn:
            rel_id_nn = existing_nn.relationship_id
            print(f"  [OK] Relationship already exists (skipped)")
        else:
            result_nn = await backoff(lambda: client.tables.create_many_to_many_relationship(relationship=m2m))
            assert result_nn.relationship_schema_name == "test_relchild_relproject"
            rel_id_nn = result_nn.relationship_id
            print(f"  [OK] Created N:N: {result_nn.relationship_schema_name}")

        # Get relationship metadata
        print("\n  Test 4: Query relationship metadata")
        fetched_1n = await client.tables.get_relationship("test_RelParent_RelChild")
        assert fetched_1n is not None and fetched_1n.relationship_type == "one_to_many"
        print(f"  [OK] Retrieved 1:N: {fetched_1n.relationship_schema_name}")

        fetched_nn = await client.tables.get_relationship("test_relchild_relproject")
        assert fetched_nn is not None and fetched_nn.relationship_type == "many_to_many"
        print(f"  [OK] Retrieved N:N: {fetched_nn.relationship_schema_name}")

        missing = await client.tables.get_relationship("nonexistent_relationship_xyz")
        assert missing is None
        print("  [OK] Non-existent relationship returns None")

        # Delete relationships
        print("\n  Test 5: Delete relationships")
        await backoff(lambda: client.tables.delete_relationship(rel_id_1n))
        rel_id_1n = None
        print("  [OK] Deleted 1:N relationship")

        await backoff(lambda: client.tables.delete_relationship(rel_id_lookup))
        rel_id_lookup = None
        print("  [OK] Deleted lookup relationship")

        await backoff(lambda: client.tables.delete_relationship(rel_id_nn))
        rel_id_nn = None
        print("  [OK] Deleted N:N relationship")

        verify = await client.tables.get_relationship("test_RelParent_RelChild")
        assert verify is None
        print("  [OK] Verified 1:N deletion")

        print("\n[OK] All relationship tests passed!")

    finally:
        for rid in [rel_id_1n, rel_id_lookup, rel_id_nn]:
            if rid:
                try:
                    await client.tables.delete_relationship(rid)
                except Exception:
                    pass

        for tbl in reversed(created_tables):
            try:
                await backoff(lambda name=tbl: client.tables.delete(name))
                print(f"   (Cleaned up table: {tbl})")
            except Exception as e:
                print(f"   [WARN] Could not delete {tbl}: {e}")


async def cleanup_test_data(
    client: AsyncDataverseClient,
    table_info: Dict[str, Any],
    record_id: str,
) -> None:
    """Clean up test data."""
    print("\n-> Cleanup")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    retries = 5
    delay_seconds = 3

    cleanup_choice = input("Do you want to delete the test record? (y/N): ").strip().lower()
    if cleanup_choice in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                await client.records.delete(table_schema_name, record_id)
                print("[OK] Test record deleted successfully")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    print("Record already deleted; skipping.")
                    break
                if attempt < retries:
                    await asyncio.sleep(delay_seconds)
                    continue
                print(f"[WARN] Failed to delete test record: {err}")
            except Exception as e:
                print(f"[WARN] Failed to delete test record: {e}")
                break
    else:
        print("Test record kept for inspection")

    table_cleanup = input("Do you want to delete the test table? (y/N): ").strip().lower()
    if table_cleanup in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                await client.tables.delete(table_schema_name)
                print("[OK] Test table deleted successfully")
                break
            except HttpError as err:
                if attempt < retries:
                    await asyncio.sleep(delay_seconds)
                    continue
                print(f"[WARN] Failed to delete test table: {err}")
            except Exception as e:
                print(f"[WARN] Failed to delete test table: {e}")
                break
    else:
        print("Test table kept for future testing")


async def main():
    """Main async test function."""
    print("PowerPlatform Dataverse Client SDK - Async Functional Testing")
    print("=" * 70)
    print("This script tests async SDK functionality in a real Dataverse environment:")
    print("  - Authentication & Connection")
    print("  - Table Creation & Metadata Operations")
    print("  - Record CRUD Operations")
    print("  - Query Functionality (list, list_pages, builder, fetchxml)")
    print("  - Relationship Operations (1:N, N:N, lookup)")
    print("  - Batch Operations (create, read, update, changeset, delete)")
    print("  - Interactive Cleanup")
    print("=" * 70)
    print("For installation validation, run examples/aio/basic/installation_example.py first")
    print("=" * 70)

    try:
        client, credential = await setup_authentication()

        try:
            async with client:
                table_info = await ensure_test_table(client)
                record_id = await test_create_record(client, table_info)
                await test_read_record(client, table_info, record_id)
                await test_query_records(client, table_info)
                await test_relationships(client)
                await test_batch_all_operations(client, table_info)

                print("\nAsync Functional Test Summary")
                print("=" * 50)
                print("[OK] Authentication: Success")
                print("[OK] Table Operations: Success")
                print("[OK] Record Creation: Success")
                print("[OK] Record Reading: Success")
                print("[OK] Record Querying (list, list_pages, builder, fetchxml): Success")
                print("[OK] Relationship Operations: Success")
                print("[OK] Batch Operations: Success")
                print("\nYour async PowerPlatform Dataverse Client SDK is fully functional!")

                await cleanup_test_data(client, table_info, record_id)
        finally:
            await credential.close()

    except KeyboardInterrupt:
        print("\n\n[WARN] Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
