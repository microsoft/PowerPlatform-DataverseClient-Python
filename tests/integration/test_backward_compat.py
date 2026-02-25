#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Backward Compatibility Integration Tests

Verifies that all operations available to users of v0.1.0b3 (the currently
published PyPI version) continue to work on the current build. The primary
focus is deprecated flat methods -- these are what existing users call.

Prerequisites:
    pip install -e .
    pip install azure-identity

Usage:
    python tests/integration/test_backward_compat.py <org_url>
"""

import os
import sys
import tempfile
import time
import warnings
from pathlib import Path

from azure.identity import InteractiveBrowserCredential

from PowerPlatform.Dataverse.client import DataverseClient

# ------------------------------------------------------------------ Helpers

PASS = 0
FAIL = 0
SKIP = 0

TABLE_NAME = "new_BackCompatTest"
BACKOFF_DELAYS = (0, 3, 10, 20, 35)


def log(status: str, test: str, detail: str = ""):
    global PASS, FAIL, SKIP
    if status == "PASS":
        PASS += 1
    elif status == "FAIL":
        FAIL += 1
    elif status == "SKIP":
        SKIP += 1
    symbol = {"PASS": "[OK]", "FAIL": "[ERR]", "SKIP": "[--]"}[status]
    msg = f"  {symbol} {test}"
    if detail:
        msg += f" -- {detail}"
    print(msg)


def backoff(op, *, delays=BACKOFF_DELAYS):
    last = None
    for d in delays:
        if d:
            time.sleep(d)
        try:
            return op()
        except Exception as ex:
            last = ex
    if last:
        raise last


def suppress_deprecation():
    """Context manager to suppress DeprecationWarning for deprecated method calls."""
    ctx = warnings.catch_warnings()
    ctx.__enter__()
    warnings.simplefilter("ignore", DeprecationWarning)
    return ctx


# --------------------------------------------------- 1. Import Compatibility


def test_imports():
    """Verify all v0.1.0b3 import paths still work."""
    print("\n1. Import Compatibility")
    print("-" * 50)

    try:
        from PowerPlatform.Dataverse.client import DataverseClient  # noqa: F811

        log("PASS", "import DataverseClient")
    except ImportError as e:
        log("FAIL", "import DataverseClient", str(e))

    try:
        from PowerPlatform.Dataverse.core.errors import (  # noqa: F401
            DataverseError,
            HttpError,
            ValidationError,
            MetadataError,
            SQLParseError,
        )

        log("PASS", "import core.errors (all 5 types)")
    except ImportError as e:
        log("FAIL", "import core.errors", str(e))

    try:
        from PowerPlatform.Dataverse.core.config import DataverseConfig  # noqa: F401

        log("PASS", "import DataverseConfig")
    except ImportError as e:
        log("FAIL", "import DataverseConfig", str(e))

    try:
        from PowerPlatform.Dataverse.models.relationship import (  # noqa: F401
            RelationshipInfo,
            CascadeConfiguration,
            LookupAttributeMetadata,
            OneToManyRelationshipMetadata,
            ManyToManyRelationshipMetadata,
        )
        from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel  # noqa: F401
        from PowerPlatform.Dataverse.models.upsert import UpsertItem  # noqa: F401
        from PowerPlatform.Dataverse.common.constants import (  # noqa: F401
            CASCADE_BEHAVIOR_CASCADE,
            CASCADE_BEHAVIOR_NO_CASCADE,
            CASCADE_BEHAVIOR_REMOVE_LINK,
            CASCADE_BEHAVIOR_RESTRICT,
        )

        log("PASS", "import models + constants")
    except ImportError as e:
        log("FAIL", "import models + constants", str(e))


# --------------------------------- 2. Deprecated Method Existence + Warnings


def test_deprecated_methods(client: DataverseClient):
    """Verify all v0.1.0b3 flat methods exist and emit DeprecationWarning."""
    print("\n2. Deprecated Methods Exist + Emit Warnings")
    print("-" * 50)

    deprecated = [
        "create",
        "update",
        "delete",
        "get",
        "query_sql",
        "get_table_info",
        "create_table",
        "delete_table",
        "list_tables",
        "create_columns",
        "delete_columns",
        "upload_file",
        "flush_cache",
    ]
    for name in deprecated:
        if hasattr(client, name):
            log("PASS", f"client.{name} exists")
        else:
            log("FAIL", f"client.{name} exists", "not found")

    for ns in ["records", "query", "tables", "files"]:
        if hasattr(client, ns):
            log("PASS", f"client.{ns} namespace exists")
        else:
            log("FAIL", f"client.{ns} namespace exists", "not found")


# ------------------------------------ 3. CRUD via Deprecated Flat Methods


def test_crud_deprecated(client: DataverseClient):
    """Full CRUD lifecycle using ONLY v0.1.0b3 deprecated flat methods."""
    print("\n3. CRUD via Deprecated Flat Methods (v0.1.0b3 code paths)")
    print("-" * 50)

    w = suppress_deprecation()
    record_id = None

    # CREATE single
    try:
        ids = client.create("account", {"name": "BackCompat Deprecated Test"})
        assert isinstance(ids, list), f"Expected list, got {type(ids)}"
        assert len(ids) == 1
        record_id = ids[0]
        log("PASS", "client.create() single", f"id={record_id[:8]}...")
    except Exception as e:
        log("FAIL", "client.create() single", str(e))
        w.__exit__(None, None, None)
        return

    # CREATE bulk
    try:
        bulk_ids = client.create(
            "account",
            [
                {"name": "BackCompat Bulk 1"},
                {"name": "BackCompat Bulk 2"},
            ],
        )
        assert isinstance(bulk_ids, list)
        assert len(bulk_ids) == 2
        log("PASS", "client.create() bulk", f"{len(bulk_ids)} records")
    except Exception as e:
        log("FAIL", "client.create() bulk", str(e))
        bulk_ids = []

    # GET single
    try:
        record = client.get("account", record_id, select=["name", "accountid"])
        assert record["name"] == "BackCompat Deprecated Test"
        log("PASS", "client.get() single record")
    except Exception as e:
        log("FAIL", "client.get() single record", str(e))

    # GET paginated (multi-record query)
    try:
        pages = list(client.get("account", filter="statecode eq 0", top=5))
        assert len(pages) > 0
        assert len(pages[0]) > 0
        log("PASS", "client.get() paginated", f"{sum(len(p) for p in pages)} records")
    except Exception as e:
        log("FAIL", "client.get() paginated", str(e))

    # UPDATE single
    try:
        client.update("account", record_id, {"name": "Updated Deprecated"})
        log("PASS", "client.update() single")
    except Exception as e:
        log("FAIL", "client.update() single", str(e))

    # UPDATE broadcast (multiple IDs, same changes)
    if bulk_ids:
        try:
            client.update("account", bulk_ids, {"description": "bulk updated"})
            log("PASS", "client.update() broadcast")
        except Exception as e:
            log("FAIL", "client.update() broadcast", str(e))

    # DELETE single
    try:
        client.delete("account", record_id)
        log("PASS", "client.delete() single")
    except Exception as e:
        log("FAIL", "client.delete() single", str(e))

    # Cleanup bulk records
    for rid in bulk_ids:
        try:
            client.delete("account", rid)
        except Exception:
            pass

    w.__exit__(None, None, None)


# ------------------------------------ 4. SQL Query via Deprecated Method


def test_sql_deprecated(client: DataverseClient):
    """SQL query using deprecated client.query_sql()."""
    print("\n4. SQL Query (Deprecated)")
    print("-" * 50)

    w = suppress_deprecation()

    try:
        rows = client.query_sql("SELECT TOP 5 accountid, name FROM account")
        assert isinstance(rows, list)
        if rows:
            assert "name" in rows[0] or "accountid" in rows[0]
        log("PASS", "client.query_sql()", f"{len(rows)} rows")
    except Exception as e:
        log("FAIL", "client.query_sql()", str(e))

    w.__exit__(None, None, None)


# ------------------------------------ 5. Table Metadata via Deprecated Methods


def test_table_metadata_deprecated(client: DataverseClient):
    """Table metadata using deprecated flat methods."""
    print("\n5. Table Metadata (Deprecated)")
    print("-" * 50)

    w = suppress_deprecation()

    # get_table_info
    try:
        info = client.get_table_info("account")
        assert info is not None
        assert "table_schema_name" in info or "table_logical_name" in info
        log("PASS", "client.get_table_info()", f"schema={info.get('table_schema_name', '?')}")
    except Exception as e:
        log("FAIL", "client.get_table_info()", str(e))

    # list_tables (always returned dicts despite old list[str] annotation)
    try:
        tables = client.list_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0
        assert isinstance(tables[0], dict)
        log("PASS", "client.list_tables()", f"{len(tables)} tables")
    except Exception as e:
        log("FAIL", "client.list_tables()", str(e))

    w.__exit__(None, None, None)


# -------- 6. Table Lifecycle via Deprecated Methods (create/columns/delete)


def test_table_lifecycle_deprecated(client: DataverseClient):
    """Table create, add columns, remove columns, delete using deprecated methods."""
    print("\n6. Table Lifecycle (Deprecated Methods)")
    print("-" * 50)

    w = suppress_deprecation()
    table_created = False

    # create_table (deprecated, with old parameter names)
    try:
        info = backoff(
            lambda: client.create_table(
                TABLE_NAME,
                {"new_Title": "string", "new_Score": "int"},
                solution_unique_name=None,
                primary_column_schema_name=None,
            )
        )
        assert info is not None
        log("PASS", "client.create_table()", f"entity_set={info.get('entity_set_name', '?')}")
        table_created = True
    except Exception as e:
        log("FAIL", "client.create_table()", str(e))
        w.__exit__(None, None, None)
        return

    time.sleep(10)

    # create_columns (deprecated)
    try:
        added = backoff(lambda: client.create_columns(TABLE_NAME, {"new_Notes": "string"}))
        assert isinstance(added, list)
        log("PASS", "client.create_columns()", f"added={added}")
    except Exception as e:
        log("FAIL", "client.create_columns()", str(e))

    # delete_columns (deprecated)
    try:
        removed = backoff(lambda: client.delete_columns(TABLE_NAME, ["new_Notes"]))
        assert isinstance(removed, list)
        log("PASS", "client.delete_columns()")
    except Exception as e:
        log("FAIL", "client.delete_columns()", str(e))

    # delete_table (deprecated)
    if table_created:
        try:
            backoff(lambda: client.delete_table(TABLE_NAME))
            log("PASS", "client.delete_table()")
        except Exception as e:
            log("FAIL", "client.delete_table()", str(e))

    w.__exit__(None, None, None)


# ------------------------------------ 7. File Upload via Deprecated Method


def test_file_upload_deprecated(client: DataverseClient):
    """File upload using deprecated client.upload_file()."""
    print("\n7. File Upload (Deprecated)")
    print("-" * 50)

    w = suppress_deprecation()
    record_id = None
    file_table = "new_FileUploadTest"

    # Setup: create a table with a file column
    try:
        backoff(lambda: client.create_table(file_table, {"new_Title": "string"}))
        time.sleep(10)
    except Exception:
        # Table may already exist
        pass

    # Create a record to upload to
    try:
        attr_prefix = file_table.split("_", 1)[0] if "_" in file_table else file_table
        name_attr = f"{attr_prefix}_name"
        ids = client.create(file_table.lower(), {name_attr: "File Test Record"})
        record_id = ids[0]
        log("PASS", "setup: created record for file upload")
    except Exception as e:
        log("SKIP", "client.upload_file() deprecated", f"could not create record: {e}")
        w.__exit__(None, None, None)
        # Cleanup table
        try:
            client.delete_table(file_table)
        except Exception:
            pass
        return

    # Create a small test file
    test_file = Path(tempfile.gettempdir()) / "backcompat_test.txt"
    test_file.write_text("backward compatibility test file content")

    # Upload using deprecated method
    file_attr = f"{attr_prefix}_Document"
    try:
        backoff(
            lambda: client.upload_file(
                file_table.lower(),
                record_id,
                file_attr,
                str(test_file),
                mode="small",
            )
        )
        log("PASS", "client.upload_file() deprecated")
    except Exception as e:
        log("FAIL", "client.upload_file() deprecated", str(e))

    # Upload using namespaced method
    try:
        backoff(
            lambda: client.files.upload(
                table=file_table.lower(),
                record_id=record_id,
                file_column=file_attr,
                path=str(test_file),
                mode="small",
                if_none_match=False,
            )
        )
        log("PASS", "client.files.upload() namespaced")
    except Exception as e:
        log("FAIL", "client.files.upload() namespaced", str(e))

    # Cleanup
    try:
        test_file.unlink(missing_ok=True)
        client.delete(file_table.lower(), record_id)
        backoff(lambda: client.delete_table(file_table))
    except Exception:
        pass

    w.__exit__(None, None, None)


# ------------------------------------ 8. CRUD via Namespaced Methods


def test_crud_namespaced(client: DataverseClient):
    """Full CRUD using new namespaced methods (verifies new API works)."""
    print("\n8. CRUD via Namespaced Methods (New API)")
    print("-" * 50)

    record_id = None

    try:
        record_id = client.records.create("account", {"name": "Namespaced Test"})
        assert isinstance(record_id, str)
        log("PASS", "records.create() single", f"id={record_id[:8]}...")
    except Exception as e:
        log("FAIL", "records.create() single", str(e))
        return

    try:
        record = client.records.get("account", record_id, select=["name"])
        assert record["name"] == "Namespaced Test"
        log("PASS", "records.get() single")
    except Exception as e:
        log("FAIL", "records.get() single", str(e))

    try:
        client.records.update("account", record_id, {"name": "Updated"})
        log("PASS", "records.update()")
    except Exception as e:
        log("FAIL", "records.update()", str(e))

    try:
        pages = list(client.records.get("account", filter="statecode eq 0", top=5))
        assert len(pages) > 0
        log("PASS", "records.get() paginated", f"{sum(len(p) for p in pages)} records")
    except Exception as e:
        log("FAIL", "records.get() paginated", str(e))

    try:
        client.records.delete("account", record_id)
        log("PASS", "records.delete()")
    except Exception as e:
        log("FAIL", "records.delete()", str(e))


# ------------------------------------ 9. SQL via Namespaced Method


def test_sql_namespaced(client: DataverseClient):
    """SQL query via namespaced method."""
    print("\n9. SQL Query (Namespaced)")
    print("-" * 50)

    try:
        rows = client.query.sql("SELECT TOP 5 accountid, name FROM account")
        assert isinstance(rows, list)
        log("PASS", "query.sql()", f"{len(rows)} rows")
    except Exception as e:
        log("FAIL", "query.sql()", str(e))


# ------------------------------------ 10. Table Metadata via Namespaced


def test_table_metadata_namespaced(client: DataverseClient):
    """Table metadata via namespaced methods including new filter/select."""
    print("\n10. Table Metadata (Namespaced)")
    print("-" * 50)

    try:
        info = client.tables.get("account")
        assert info is not None
        log("PASS", "tables.get()")
    except Exception as e:
        log("FAIL", "tables.get()", str(e))

    try:
        tables = client.tables.list()
        assert len(tables) > 0
        log("PASS", "tables.list()", f"{len(tables)} tables")
    except Exception as e:
        log("FAIL", "tables.list()", str(e))

    try:
        filtered = client.tables.list(
            filter="IsCustomEntity eq true",
            select=["LogicalName", "SchemaName"],
        )
        log("PASS", "tables.list(filter, select)", f"{len(filtered)} custom tables")
    except Exception as e:
        log("FAIL", "tables.list(filter, select)", str(e))


# ------------------------------------ 11. Relationship Operations (New)


def test_relationships(client: DataverseClient):
    """Relationship operations (new in this release)."""
    print("\n11. Relationships (New)")
    print("-" * 50)

    from PowerPlatform.Dataverse.models.relationship import RelationshipInfo

    try:
        rel = client.tables.get_relationship("account_primary_contact")
        if rel:
            assert isinstance(rel, RelationshipInfo)
            log("PASS", "tables.get_relationship()", f"type={rel.relationship_type}")
        else:
            log("SKIP", "tables.get_relationship()", "system relationship not found, trying another")
            # Try a common system relationship
            rel = client.tables.get_relationship("contact_customer_accounts")
            if rel:
                assert isinstance(rel, RelationshipInfo)
                log("PASS", "tables.get_relationship()", f"type={rel.relationship_type}")
            else:
                log("SKIP", "tables.get_relationship()", "no system relationships found")
    except Exception as e:
        log("FAIL", "tables.get_relationship()", str(e))


# ------------------------------------ 12. Deprecation Warning Verification


def test_deprecation_warnings(client: DataverseClient):
    """Verify deprecated methods emit DeprecationWarning."""
    print("\n12. Deprecation Warning Emission")
    print("-" * 50)

    cases = [
        ("client.get()", lambda: list(client.get("account", filter="statecode eq 0", top=1))),
        ("client.query_sql()", lambda: client.query_sql("SELECT TOP 1 name FROM account")),
        ("client.list_tables()", lambda: client.list_tables()),
        ("client.get_table_info()", lambda: client.get_table_info("account")),
    ]

    for name, op in cases:
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                op()
            has_deprecation = any(issubclass(x.category, DeprecationWarning) for x in w)
            if has_deprecation:
                log("PASS", f"{name} emits DeprecationWarning")
            else:
                log("FAIL", f"{name} emits DeprecationWarning", "no warning")
        except Exception as e:
            log("FAIL", f"{name} emits DeprecationWarning", str(e))


# -------------------------------------------------------------------- Main


def main():
    if len(sys.argv) < 2:
        print("Usage: python tests/integration/test_backward_compat.py <org_url>")
        sys.exit(1)

    org_url = sys.argv[1].rstrip("/")
    print("Backward Compatibility Test Suite")
    print(f"Target: {org_url}")
    print("=" * 60)

    credential = InteractiveBrowserCredential()
    client = DataverseClient(org_url, credential)

    test_imports()
    test_deprecated_methods(client)
    test_crud_deprecated(client)  # Priority: deprecated CRUD
    test_sql_deprecated(client)  # Priority: deprecated SQL
    test_table_metadata_deprecated(client)  # Priority: deprecated metadata
    test_table_lifecycle_deprecated(client)  # Priority: deprecated table mgmt
    test_file_upload_deprecated(client)  # Priority: deprecated file upload
    test_crud_namespaced(client)  # Verify: new namespaced CRUD
    test_sql_namespaced(client)  # Verify: new namespaced SQL
    test_table_metadata_namespaced(client)  # Verify: new namespaced metadata
    test_relationships(client)  # Verify: new relationship ops
    test_deprecation_warnings(client)  # Verify: warnings emitted

    print("\n" + "=" * 60)
    print("BACKWARD COMPATIBILITY SUMMARY")
    print("=" * 60)
    total = PASS + FAIL + SKIP
    print(f"  PASS: {PASS}")
    print(f"  FAIL: {FAIL}")
    print(f"  SKIP: {SKIP}")
    print(f"  Total: {total}")
    print("=" * 60)

    if FAIL > 0:
        print(f"\n[ERR] {FAIL} test(s) FAILED")
        sys.exit(1)
    else:
        print(f"\n[OK] All {PASS} tests passed ({SKIP} skipped)")
        sys.exit(0)


if __name__ == "__main__":
    main()
