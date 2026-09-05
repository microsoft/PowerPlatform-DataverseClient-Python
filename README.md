# PowerPlatform Dataverse Client for Python

[![PyPI version](https://img.shields.io/pypi/v/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![Python](https://img.shields.io/pypi/pyversions/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The Dataverse SDK for Python lets Python developers access, manage, and manipulate Microsoft Dataverse business data using familiar Python syntax — no .NET knowledge required. It wraps the Dataverse Web API in a single typed client that works with tables and records as native Python dictionaries and pandas DataFrames.

**[Source code](https://github.com/microsoft/PowerPlatform-DataverseClient-Python)** | **[Package (PyPI)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)** | **[API reference](https://learn.microsoft.com/python/api/dataverse-sdk-docs-python/dataverse-overview?view=dataverse-sdk-python-latest)** | **[Product documentation](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/)** | **[Samples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples)**

## Table of contents

- [Key features](#key-features)
- [Getting started](#getting-started)
- [Usage](#usage)
- [Documentation](#documentation)
- [Samples](#samples)
- [Contributing](#contributing)

## Key features

- **CRUD & bulk** — single records plus native `CreateMultiple` / `UpdateMultiple` / `UpsertMultiple` / `BulkDelete`.
- **Fluent QueryBuilder** — type-safe `col()` filters; plus read-only SQL and FetchXML.
- **Schema & relationships** — create tables, columns, and 1:N / N:N relationships.
- **pandas DataFrames** — read and write records as DataFrames and Series.
- **File uploads** — to file columns, with automatic chunking for large files.
- **Batch** — many operations per HTTP request, with transactional changesets.
- **Azure Identity auth & typed errors** — any `TokenCredential`; structured exception hierarchy with retry guidance.
- **Async** — `AsyncDataverseClient` mirrors the sync API.

## Getting started

### Prerequisites

- Python 3.10 or later
- A Microsoft Dataverse environment with appropriate permissions
- OAuth authentication configured for your application

### Install

```bash
pip install PowerPlatform-Dataverse-Client
```

The `pandas` library is installed automatically and powers the `client.dataframe` namespace. The async client requires an optional extra: `pip install "PowerPlatform-Dataverse-Client[async]"`.

### Authenticate

The client accepts any Azure Identity [`TokenCredential`](https://learn.microsoft.com/python/api/azure-identity/azure.identity?view=azure-python). Use `InteractiveBrowserCredential` for local development, or `ClientSecretCredential` / `CertificateCredential` for unattended production apps. For app registration and all credential types, see [Use OAuth with Dataverse](https://learn.microsoft.com/power-apps/developer/data-platform/authenticate-oauth).

```python
from azure.identity import InteractiveBrowserCredential

# Local development: opens a browser to sign in
credential = InteractiveBrowserCredential()

# Production (unattended) alternatives:
# from azure.identity import ClientSecretCredential, CertificateCredential
# credential = ClientSecretCredential(tenant_id, client_id, client_secret)
# credential = CertificateCredential(tenant_id, client_id, cert_path)
```

The examples below pass this `credential` to `DataverseClient`, opened as a context manager.

## Usage

Every operation hangs off a namespace on the client: `records` for CRUD, `query` for filtered reads, `tables` for schema and metadata, `dataframe` for pandas, `files` for uploads, and `batch` for multi-operation requests. Records are plain Python dictionaries keyed by column schema names — custom tables and columns keep their customization prefix (for example `"new_"`). The sections below cover the capabilities in the order you typically reach for them; each links to the Learn article and runnable sample that go deeper.

### Create, read, update, delete

Use the client as a context manager so connections are pooled and cleaned up for you. `create` returns the new record's GUID as a string, and `retrieve` returns `None` when the record does not exist rather than raising on a 404.

```python
from PowerPlatform.Dataverse.client import DataverseClient

with DataverseClient("https://yourorg.crm.dynamics.com", credential) as client:
    account_id = client.records.create("account", {"name": "Contoso Ltd"})   # -> GUID str

    account = client.records.retrieve("account", account_id, select=["name"])
    if account is not None:                       # None when the record is not found
        print(account["name"])

    client.records.update("account", account_id, {"telephone1": "555-0199"})
    client.records.delete("account", account_id)
```

More on reading and writing records: [Work with Dataverse data](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/work-data) and the [walkthrough.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/walkthrough.py) sample.

### Query records

`client.query.builder()` builds type-safe OData for you — filters use `col()` with standard Python operators, and it escapes values automatically. Results are iterable and can be handed straight to pandas with `.to_dataframe()`.

```python
from PowerPlatform.Dataverse.models import col

results = (client.query.builder("account")
           .select("name", "revenue")
           .where(col("statecode") == 0)
           .where(col("revenue") > 1_000_000)
           .order_by("revenue", descending=True)
           .top(100)
           .execute())

for account in results:
    print(account["name"], account["revenue"])

df = results.to_dataframe()          # same rows as a pandas DataFrame
```

`col()` also supports `.in_([...])` and `.between(low, high)`, and expressions compose with `&`. For large result sets call `.page_size(n).execute_pages()` to stream page by page, or `.expand("primarycontactid")` to pull related rows in one request. For raw SQL and FetchXML, see [Query data](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/query) and the [sql_examples.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/sql_examples.py) and [fetchxml.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/fetchxml.py) samples.

### Define and evolve schema

`client.tables` creates and inspects tables, columns, relationships, and alternate keys. Column types are simple strings (`"string"`, `"int"`, `"decimal"`, `"money"`, `"datetime"`, `"bool"`, `"memo"`); pass an `IntEnum` subclass to create a choice column. `tables.get` returns `None` when the table does not exist, which makes schema setup idempotent. Every table gets a primary name column automatically — `<prefix>_Name` unless you pass `primary_column` — so do not list it in `columns`.

```python
# Create a custom table with typed columns
if client.tables.get("new_Project") is None:
    client.tables.create("new_Project", {
        "new_Budget": "money",
        "new_StartDate": "datetime",
    }, display_name="Project")

if client.tables.get("new_Task") is None:
    client.tables.create("new_Task", {"new_DueDate": "datetime"}, display_name="Task")

# Add a column to an existing table
client.tables.add_columns("new_Project", {"new_Status": "string"})

# Add a lookup column -- a 1:N relationship from Task to Project
client.tables.create_lookup_field(
    referencing_table="new_task",
    lookup_field_name="new_ProjectId",
    referenced_table="new_project",
    display_name="Project",
)
```

Relationship methods take logical names, which are always the schema name in lowercase. For choice columns, many-to-many relationships, and alternate keys, see [Customize tables and columns](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/metadata) and [Manage table relationships](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/relationships).

### Work in bulk

Passing a list to `create()` uses Dataverse's native `CreateMultiple`; a single change dict applied to a list of IDs uses `UpdateMultiple`; and deleting a list uses `BulkDelete`, which returns a job ID and removes the records in the background — pass `use_bulk_delete=False` to delete them one at a time instead. `upsert()` creates or updates each record by an alternate key — ideal for idempotent syncs. The key must already exist on the table and its index must have reached `Active`; create one with `client.tables.create_alternate_key` and poll `client.tables.get_alternate_keys` until it does.

```python
from PowerPlatform.Dataverse.models import UpsertItem

# Create many in one request -- returns a list of GUID strings
ids = client.records.create("account", [{"name": "Company A"}, {"name": "Company B"}])

# Apply the same change to every record, then delete them all
client.records.update("account", ids, {"industrycode": 1})
job_id = client.records.delete("account", ids)

# Upsert by alternate key (create if missing, update if present)
client.records.upsert("account", [
    UpsertItem(alternate_key={"accountnumber": "ACC-001"},
               record={"name": "Contoso Ltd", "telephone1": "555-0100"}),
])
```

For DataFrame-driven loads, `client.dataframe.create(table, df)` writes an entire pandas DataFrame in one call — see the [dataframe_operations.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/dataframe_operations.py) and [alternate_keys_upsert.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/alternate_keys_upsert.py) samples.

### Upload files and group requests

`client.files.upload` writes a local file to a file column, chunking large files automatically. `client.batch` packs many operations into one HTTP request, and `batch.changeset()` groups them so they commit or roll back together.

```python
client.files.upload("account", account_id, "new_Attachment", "report.pdf")

batch = client.batch.new()
batch.records.create("account", {"name": "Company A"})
batch.records.update("account", account_id, {"telephone1": "555-0199"})
result = batch.execute()
```

See the [file_upload.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/file_upload.py) and [batch.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/batch.py) samples.

### Use the async client

`AsyncDataverseClient` (from the `[async]` extra) exposes the same namespaces and methods as the sync client, each awaitable — so independent operations can run concurrently with `asyncio.gather()`.

```python
import asyncio
from azure.identity.aio import DefaultAzureCredential
from PowerPlatform.Dataverse.aio import AsyncDataverseClient

async def main():
    async with DefaultAzureCredential() as credential, \
               AsyncDataverseClient("https://yourorg.crm.dynamics.com", credential) as client:
        # Independent calls run concurrently
        a_id, b_id = await asyncio.gather(
            client.records.create("account", {"name": "Company A"}),
            client.records.create("account", {"name": "Company B"}),
        )
        accounts = await client.query.builder("account").select("name").top(50).execute()

asyncio.run(main())
```

See [Asynchronous client operations](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/async-client) and the [examples/aio](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples/aio) directory.

### Handle errors

The SDK raises a typed exception hierarchy rooted at `DataverseError`. `HttpError` exposes `status_code` and `is_transient`, so you can retry throttled or transient failures and let everything else fall through to the base class.

```python
from PowerPlatform.Dataverse.core.errors import (
    DataverseError,   # base class -- catch last as a fallback
    ValidationError,  # client-side input validation failed, including unsupported SQL
    MetadataError,    # unknown table / column / relationship
    HttpError,        # Dataverse Web API returned a non-2xx status
)

try:
    client.records.create("account", {"name": "Contoso Ltd"})
except ValidationError as e:
    print(f"Invalid input: {e.message}")
except HttpError as e:
    print(f"HTTP {e.status_code}: {e.message}")
    if e.is_transient:                     # 429 / 502 / 503 / 504
        print(f"Retry after {e.details.get('retry_after')}s")
except DataverseError as e:                # catches MetadataError, etc.
    print(f"Dataverse error: {e.message}")
```

For retry patterns, timeouts, and HTTP diagnostics logging, see [Handle errors and enable HTTP diagnostics](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/error-handling).

## Documentation

[Microsoft Learn](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/) is the authoritative reference; the guide for each capability is linked from its section above. New to the SDK? Start here:

- [Overview](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/overview)
- [Quick guide to Dataverse](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/quick-guide-dataverse)
- [Getting started](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/get-started)

## Samples

The [examples/](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples) directory has complete, runnable scripts for every operation, including advanced scenarios — relationships, batch changesets, file uploads, SQL and FetchXML queries, and DataFrames — plus a full [async mirror](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples/aio). Start with the [examples guide](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/README.md) for a suggested learning progression.

## Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### API Design Guidelines

When contributing new features to this SDK, please follow these guidelines:

1. **Public methods in operation namespaces** - New public methods go in the appropriate namespace module under [operations/](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/src/PowerPlatform/Dataverse/operations). Public types and constants live in their own modules (e.g., `models/metadata.py`, `common/constants.py`)
2. **Add README example for public methods** - Add usage examples to this README for public API methods
3. **Document public APIs** - Include Sphinx-style docstrings with parameter descriptions and examples for all public methods
4. **Update documentation** when adding features - Keep README and SKILL files (note that each skill has 2 copies) in sync
5. **Internal vs public naming** - Modules, files, and functions not meant to be part of the public API must use a `_` prefix (e.g., `_odata.py`, `_relationships.py`). Files without the prefix (e.g., `constants.py`, `metadata.py`) are public and importable by SDK consumers

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
