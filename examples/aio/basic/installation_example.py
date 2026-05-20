# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Async Installation, Validation & Usage Example

Async equivalent of examples/basic/installation_example.py.

This script demonstrates the async client (AsyncDataverseClient) and validates
that all async imports, classes, and methods are correctly installed.

## Installation

### For End Users (Production/Consumption):
1. Install the published SDK from PyPI with the async extra:
   ```bash
   pip install "PowerPlatform-Dataverse-Client[async]"
   ```

2. Install Azure Identity for authentication:
   ```bash
   pip install azure-identity
   ```

### For Developers (Contributing/Local Development):
1. Clone the repository and navigate to the project directory
2. Install in editable/development mode:
   ```bash
   pip install -e ".[async,dev]"
   ```

**Key Differences:**
- `pip install "PowerPlatform-Dataverse-Client[async]"` → Downloads and installs the published package from PyPI with aiohttp
- `pip install -e ".[async,dev]"` → Installs from local source code in "editable" mode

**Editable Mode Benefits:**
- Changes to source code are immediately available (no reinstall needed)
- Perfect for development, testing, and contributing
- Examples and tests can access the local codebase
- Supports debugging and live code modifications

## What This Script Does

- Validates async package imports
- Checks version and package metadata
- Shows async usage patterns
- Offers optional interactive testing with a real Dataverse environment

Prerequisites for Interactive Testing:
- Access to a Microsoft Dataverse environment
- Azure Identity credentials configured
- Interactive browser access for authentication
"""

import asyncio
import sys
import subprocess
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryOperations
from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations


def validate_imports():
    """Validate that all key async imports work correctly."""
    print("Validating Async Package Imports...")
    print("-" * 50)

    try:
        from PowerPlatform.Dataverse import __version__
        from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

        print(f"  [OK] Namespace: PowerPlatform.Dataverse.aio")
        print(f"  [OK] Package version: {__version__}")
        print(f"  [OK] Async client: PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient")

        from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError

        print(f"  [OK] Core errors: HttpError, MetadataError")

        from PowerPlatform.Dataverse.core.config import DataverseConfig

        print(f"  [OK] Core config: DataverseConfig")

        from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient

        print(f"  [OK] Async data layer: _AsyncODataClient")

        from PowerPlatform.Dataverse.aio.models.async_fetchxml_query import AsyncFetchXmlQuery
        from PowerPlatform.Dataverse.aio.models.async_query_builder import AsyncQueryBuilder

        print(f"  [OK] Async models: AsyncFetchXmlQuery, AsyncQueryBuilder")

        from _auth import AsyncInteractiveBrowserCredential

        print(f"  [OK] Azure Identity: AsyncInteractiveBrowserCredential (interactive browser)")

        return True, __version__, AsyncDataverseClient

    except ImportError as e:
        print(f"  [ERR] Import failed: {e}")
        print("\nTroubleshooting:")
        print("  pip install PowerPlatform-Dataverse-Client azure-identity")
        print("  Or for development: pip install -e .")
        return False, None, None


def validate_client_methods(AsyncDataverseClient):
    """Validate that AsyncDataverseClient has expected methods."""
    print("\nValidating Async Client Methods...")
    print("-" * 50)

    expected_namespaces = {
        "records": ["create", "retrieve", "update", "delete", "list", "list_pages", "upsert"],
        "query": ["sql", "builder", "fetchxml", "sql_columns", "odata_expands"],
        "tables": [
            "create",
            "get",
            "list",
            "delete",
            "add_columns",
            "remove_columns",
            "create_one_to_many_relationship",
            "create_many_to_many_relationship",
            "delete_relationship",
            "get_relationship",
            "create_lookup_field",
        ],
        "files": ["upload"],
    }

    ns_classes = {
        "records": AsyncRecordOperations,
        "query": AsyncQueryOperations,
        "tables": AsyncTableOperations,
        "files": AsyncFileOperations,
    }

    missing_methods = []
    for ns, methods in expected_namespaces.items():
        ns_cls = ns_classes.get(ns)
        for method in methods:
            attr_path = f"{ns}.{method}"
            if ns_cls is not None and hasattr(ns_cls, method):
                print(f"  [OK] Method exists: {attr_path}")
            else:
                print(f"  [ERR] Method missing: {attr_path}")
                missing_methods.append(attr_path)

    return len(missing_methods) == 0


def validate_package_metadata():
    """Validate package metadata from pip."""
    print("\nValidating Package Metadata...")
    print("-" * 50)

    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", "PowerPlatform-Dataverse-Client"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if any(line.startswith(p) for p in ["Name:", "Version:", "Summary:", "Location:"]):
                    print(f"  [OK] {line}")
            return True
        else:
            print("  [ERR] Package not found in pip list")
            return False
    except Exception as e:
        print(f"  [ERR] Metadata validation failed: {e}")
        return False


def show_usage_examples():
    """Display async usage examples."""
    print("\nAsync Usage Examples")
    print("=" * 50)

    print("""
Basic Setup:
```python
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential
from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

async def main():
    credential = AsyncInteractiveBrowserCredential()
    try:
        async with AsyncDataverseClient("https://yourorg.crm.dynamics.com", credential) as client:
            ...  # all operations here
    finally:
        await credential.close()

asyncio.run(main())
```

CRUD Operations:
```python
async def main():
    async with AsyncDataverseClient(url, credential) as client:
        # Create a record
        account_id = await client.records.create("account", {"name": "Contoso Ltd"})

        # Read a single record by ID
        account = await client.records.retrieve("account", account_id)
        print(f"Account name: {account['name']}")

        # Update a record
        await client.records.update("account", account_id, {"telephone1": "555-0200"})

        # Delete a record
        await client.records.delete("account", account_id)
```

Querying Data:
```python
async def main():
    async with AsyncDataverseClient(url, credential) as client:
        from PowerPlatform.Dataverse.models.filters import col

        # Fluent query builder
        result = await (
            client.query.builder("account")
            .select("name", "telephone1")
            .where(col("statecode") == 0)
            .top(10)
            .execute()
        )
        for record in result:
            print(record["name"])

        # Lazy paged iteration
        async for page in (
            client.query.builder("account")
            .select("name")
            .page_size(50)
            .execute_pages()
        ):
            for record in page:
                print(record["name"])

        # SQL query
        rows = await client.query.sql("SELECT TOP 5 name FROM account")
        for row in rows:
            print(row["name"])

        # FetchXML
        xml = '<fetch top="5"><entity name="account"><attribute name="name"/></entity></fetch>'
        rows = await client.query.fetchxml(xml).execute()
        for row in rows:
            print(row["name"])
```

Batch Operations:
```python
async def main():
    async with AsyncDataverseClient(url, credential) as client:
        batch = client.batch.new()
        batch.records.create("account", {"name": "Alpha"})
        batch.records.create("account", {"name": "Beta"})
        result = await batch.execute()
        print(f"Created {len(list(result.entity_ids))} records")

        # Atomic changeset
        batch = client.batch.new()
        async with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
        result = await batch.execute()
```
""")


async def interactive_test():
    """Offer optional interactive testing with real Dataverse environment."""
    print("\nInteractive Testing")
    print("=" * 50)

    choice = input("Would you like to test with a real Dataverse environment? (y/N): ").strip().lower()
    if choice not in ["y", "yes"]:
        print("  Skipping interactive test")
        return

    if not sys.stdin.isatty():
        print("  [ERR] Interactive input required for testing")
        return

    org_url = input("Enter your Dataverse org URL (e.g., https://yourorg.crm.dynamics.com): ").strip()
    if not org_url:
        print("  [WARN] No URL provided, skipping test")
        return

    try:
        from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
        from _auth import AsyncInteractiveBrowserCredential

        print("  Setting up authentication...")
        credential = AsyncInteractiveBrowserCredential()

        print("  Creating async client...")
        try:
            async with AsyncDataverseClient(org_url.rstrip("/"), credential) as client:
                print("  Testing connection...")
                tables = await client.tables.list()
                print(f"  [OK] Connection successful!")
                print(f"  Found {len(tables)} tables in environment")

                custom_tables = await client.tables.list(
                    filter="IsCustomEntity eq true",
                    select=["LogicalName", "SchemaName"],
                )
                print(f"  Found {len(custom_tables)} custom tables (filter + select)")
        finally:
            await credential.close()

        print("\n  Your async SDK is ready for use!")

    except Exception as e:
        print(f"  [ERR] Interactive test failed: {e}")
        print("  This might be due to authentication, network, or permissions")
        print("  The SDK imports are still valid for offline development")


async def main():
    """Run async installation validation and demonstration."""
    print("PowerPlatform Dataverse Client SDK - Async Installation & Validation")
    print("=" * 70)
    print(f"Validation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    imports_success, version, AsyncDataverseClient = validate_imports()
    if not imports_success:
        print("\n[ERR] Import validation failed. Please check installation.")
        sys.exit(1)

    methods_success = True
    if AsyncDataverseClient:
        methods_success = validate_client_methods(AsyncDataverseClient)
        if not methods_success:
            print("\n[WARN] Some client methods are missing, but basic functionality should work.")

    metadata_success = validate_package_metadata()

    show_usage_examples()

    await interactive_test()

    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    results = [
        ("Async Package Imports", imports_success),
        ("Async Client Methods", methods_success),
        ("Package Metadata", metadata_success),
    ]

    all_passed = True
    for test_name, success in results:
        status = "[OK] PASS" if success else "[ERR] FAIL"
        print(f"{test_name:<25} {status}")
        if not success:
            all_passed = False

    print("=" * 70)
    if all_passed:
        print("SUCCESS: Async PowerPlatform-Dataverse-Client is properly installed!")
        if version:
            print(f"Package Version: {version}")
        print("\nNext Steps:")
        print("  - Run examples/aio/basic/functional_testing.py for a live test")
        print("  - Run examples/aio/advanced/walkthrough.py for a full feature tour")
    else:
        print("[ERR] Some validation checks failed!")
        print("   pip uninstall PowerPlatform-Dataverse-Client")
        print("   pip install PowerPlatform-Dataverse-Client")
        sys.exit(1)


if __name__ == "__main__":
    print("PowerPlatform-Dataverse-Client SDK - Async Installation Example")
    print("=" * 60)
    asyncio.run(main())
