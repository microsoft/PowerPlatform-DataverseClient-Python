# Dataverse SDK for Python

A Python package allowing developers to connect to Dataverse environments for DDL / DML operations.

- Read (SQL) — Execute constrained read-only SQL via the Dataverse Web API `?sql=` parameter. Returns `list[dict]`.
- OData CRUD — Unified methods `create(logical_name, record|records)`, `update(logical_name, id|ids, patch|patches)`, `delete(logical_name, id|ids)` plus `get` with record id or filters.
- Bulk create — Pass a list of records to `create(...)` to invoke the bound `CreateMultiple` action; returns `list[str]` of GUIDs. If any payload omits `@odata.type` the SDK resolves and stamps it (cached).
- Bulk update — Provide a list of IDs with a single patch (broadcast) or a list of per‑record patches to `update(...)`; internally uses the bound `UpdateMultiple` action; returns nothing. Each record must include the primary key attribute when sent to UpdateMultiple.
- Retrieve multiple (paging) — Generator-based `get(...)` that yields pages, supports `$top` and Prefer: `odata.maxpagesize` (`page_size`).
- Upload files — Call `upload_file(logical_name, ...)` and an upload method will be auto picked (you can override the mode). See https://learn.microsoft.com/en-us/power-apps/developer/data-platform/file-column-data?tabs=sdk#upload-files
- Metadata helpers — Create/inspect/delete tables and create/delete columns (EntityDefinitions + Attributes).
- Pandas helpers — Convenience DataFrame oriented wrappers for quick prototyping/notebooks.
- Auth — Azure Identity (`TokenCredential`) injection.

## Features

- Simple `DataverseClient` facade for CRUD, SQL (read-only), and table metadata.
- SQL-over-API: Constrained SQL (single SELECT with limited WHERE/TOP/ORDER BY) via native Web API `?sql=` parameter.
- Table metadata ops: create/delete simple custom tables (supports string/int/decimal/float/datetime/bool/optionset) and create/delete columns.
- Bulk create via `CreateMultiple` (collection-bound) by passing `list[dict]` to `create(logical_name, payloads)`; returns list of created IDs.
- Bulk update via `UpdateMultiple` (invoked internally) by calling unified `update(logical_name, ids, patch|patches)`; returns nothing.
- Retrieve multiple with server-driven paging: `get(...)` yields lists (pages) following `@odata.nextLink`. Control total via `$top` and per-page via `page_size` (Prefer: `odata.maxpagesize`).
- Upload files, using either a single request (supports file size up to 128 MB) or chunk upload under the hood
- Optional pandas integration (`PandasODataClient`) for DataFrame based create / get / query.

Auth:
- Accept only an `azure.core.credentials.TokenCredential` credential. See full supported list at https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential?view=azure-dotnet.
- Token scope used by the SDK: `https://<yourorg>.crm.dynamics.com/.default` (derived from `base_url`).

## API Reference (Summary)

| Method | Signature (simplified) | Returns | Notes |
|--------|------------------------|---------|-------|
| `create` | `create(logical_name, record_dict)` | `list[str]` (len 1) | Single create; GUID from `OData-EntityId`. |
| `create` | `create(logical_name, list[record_dict])` | `list[str]` | Uses `CreateMultiple`; stamps `@odata.type` if missing. |
| `get` | `get(logical_name, id)` | `dict` | One record; supply GUID (with/without parentheses). |
| `get` | `get(logical_name, ..., page_size=None)` | `Iterable[list[dict]]` | Multiple records; Pages yielded (non-empty only). |
| `update` | `update(logical_name, id, patch)` | `None` | Single update; no representation returned. |
| `update` | `update(logical_name, list[id], patch)` | `None` | Broadcast; same patch applied to all IDs (UpdateMultiple). |
| `update` | `update(logical_name, list[id], list[patch])` | `None` | 1:1 patches; lengths must match (UpdateMultiple). |
| `delete` | `delete(logical_name, id)` | `None` | Delete one record. |
| `delete` | `delete(logical_name, list[id])` | `None` | Delete many (sequential). |
| `query_sql` | `query_sql(sql)` | `list[dict]` | Constrained read-only SELECT via `?sql=`. |
| `create_table` | `create_table(tablename, schema, solution_unique_name=None)` | `dict` | Creates custom table + columns. Friendly name (e.g. `SampleItem`) becomes schema `new_SampleItem`; explicit schema name (contains `_`) used as-is. Pass `solution_unique_name` to attach the table to a specific solution instead of the default solution. |
| `create_column` | `create_column(tablename, columns)` | `list[str]` | Adds columns using a `{name: type}` mapping (same shape as `create_table` schema). Returns schema names for the created columns. |
| `get_table_info` | `get_table_info(schema_name)` | `dict | None` | Basic table metadata by schema name (e.g. `new_SampleItem`). Friendly names not auto-converted. |
| `list_tables` | `list_tables()` | `list[dict]` | Lists non-private tables. |
| `delete_table` | `delete_table(tablename)` | `None` | Drops custom table. Accepts friendly or schema name; friendly converted to `new_<PascalCase>`. |
| `delete_column` | `delete_column(tablename, columns)` | `list[str]` | Deletes one or more columns; returns schema names (accepts string or list[str]). |
| `PandasODataClient.create_df` | `create_df(logical_name, series)` | `str` | Create one record (returns GUID). |
| `PandasODataClient.update` | `update(logical_name, id, series)` | `None` | Returns None; ignored if Series empty. |
| `PandasODataClient.get_ids` | `get_ids(logical_name, ids, select=None)` | `DataFrame` | One row per ID (errors inline). |
| `PandasODataClient.query_sql_df` | `query_sql_df(sql)` | `DataFrame` | DataFrame for SQL results. |

Guidelines:
- `create` always returns a list of GUIDs (1 for single, N for bulk).
- `update`/`delete` always return `None` (single and multi forms).
- Bulk update chooses broadcast vs per-record by the type of `changes` (dict vs list).
- Paging and SQL operations never mutate inputs.
- Metadata lookups for logical name stamping cached per entity set (in-memory).

## Install

Create and activate a Python 3.13+ environment, then install dependencies:

```powershell
# from the repo root
python -m pip install -r requirements.txt
```

Direct TDS via ODBC is not used; SQL reads are executed via the Web API using the `?sql=` query parameter.

## Configuration Notes

- For Web API (OData), tokens target your Dataverse org URL scope: https://yourorg.crm.dynamics.com/.default. The SDK requests this scope from the provided TokenCredential.

### Configuration

```python
from azure.identity import InteractiveBrowserCredential
from dataverse_sdk import DataverseClient

base_url = "https://yourorg.crm.dynamics.com"
credential = InteractiveBrowserCredential()  # or DeviceCodeCredential(), ClientSecretCredential(...), etc.
client = DataverseClient(base_url=base_url, credential=credential)
```

## Quickstart

Edit `examples/quickstart.py` and run:

```powershell
python examples/quickstart.py
```

The quickstart demonstrates:
- Creating a simple custom table (metadata APIs)
- Creating, reading, updating, and deleting records (OData)
- Bulk create (CreateMultiple) to insert many records in one call
- Bulk update via unified `update` (multi-ID broadcast & per‑record patches)
- Retrieve multiple with paging (`$top` vs `page_size`)
- Executing a read-only SQL query (Web API `?sql=`)

For upload files functionalities, run quickstart_file_upload.py instead

## Examples

### DataverseClient (recommended)

```python
from azure.identity import InteractiveBrowserCredential
from dataverse_sdk import DataverseClient

base_url = "https://yourorg.crm.dynamics.com"
credential = InteractiveBrowserCredential()  # or DeviceCodeCredential(), ClientSecretCredential(...), etc.
client = DataverseClient(base_url=base_url, credential=credential)

# Create (returns list[str] of new GUIDs)
account_id = client.create("account", {"name": "Acme, Inc.", "telephone1": "555-0100"})[0]

# Read
account = client.get("account", account_id)

# Update (returns None)
client.update("account", account_id, {"telephone1": "555-0199"})

# Bulk update (broadcast) – apply same patch to several IDs
ids = client.create("account", [
	{"name": "Contoso"},
	{"name": "Fabrikam"},
])
client.update("account", ids, {"telephone1": "555-0200"})  # broadcast patch

# Bulk update (1:1) – list of patches matches list of IDs
client.update("account", ids, [
	{"telephone1": "555-1200"},
	{"telephone1": "555-1300"},
])
print({"multi_update": "ok"})

# Delete
client.delete("account", account_id)

# SQL (read-only) via Web API `?sql=`
rows = client.query_sql("SELECT TOP 3 accountid, name FROM account ORDER BY createdon DESC")
for r in rows:
	print(r.get("accountid"), r.get("name"))

## Bulk create (CreateMultiple)

Pass a list of payloads to `create(logical_name, payloads)` to invoke the collection-bound `Microsoft.Dynamics.CRM.CreateMultiple` action. The method returns `list[str]` of created record IDs.

```python
# Bulk create accounts (returns list of GUIDs)
payloads = [
	{"name": "Contoso"},
	{"name": "Fabrikam"},
	{"name": "Northwind"},
]
ids = client.create("account", payloads)
assert isinstance(ids, list) and all(isinstance(x, str) for x in ids)
print({"created_ids": ids})
```

## Bulk update (UpdateMultiple under the hood)

Use the unified `update` method for both single and bulk scenarios:

```python
# Broadcast
client.update("account", ids, {"telephone1": "555-0200"})

# 1:1 patches (length must match)
client.update("account", ids, [
	{"telephone1": "555-1200"},
	{"telephone1": "555-1300"},
])
```

Notes:
- Returns `None` (same as single update) to keep semantics consistent.
- Broadcast vs per-record determined by whether `changes` is a dict or list.
- Primary key attribute is injected automatically when constructing UpdateMultiple targets.
- If any payload omits `@odata.type`, it's stamped automatically (cached logical name lookup).

Bulk create notes:
- Response includes only IDs; the SDK returns those GUID strings.
- Single-record `create` returns a one-element list of GUIDs.
- Metadata lookup for `@odata.type` is performed once per entity set (cached in-memory).

## File upload

```python
client.upload_file('account', record_id, 'sample_filecolumn', 'test.pdf')

client.upload_file('account', record_id, 'sample_filecolumn', 'test.pdf', mode='chunk', if_none_match=True)

```

Notes:
- upload_file picks one of the three methods to use based on file size: if file is less than 128 MB uses upload_file_small, otherwise uses upload_file_chunk
- upload_file_small makes a single Web API call and only supports file size < 128 MB
- upload_file_chunk uses PATCH with Content-Range to upload the file (more aligned with HTTP standard compared to Dataverse messages). It consists of 2 stages 1. PATCH request to get the headers used for actual upload. 2. Actual upload in chunks. It uses x-ms-chunk-size returned in the first stage to determine chunk size (normally 4 MB), and use Content-Range and Content-Length as metadata for the upload. Total number of Web API calls is number of chunks + 1

## Retrieve multiple with paging

Use `get(logical_name, ...)` to stream results page-by-page. You can cap total results with `$top` and hint the per-page size with `page_size` (sets Prefer: `odata.maxpagesize`).

```python
pages = client.get(
	"account",
	select=["accountid", "name", "createdon"],
	orderby=["name asc"],
	top=10,          # stop after 10 total rows (optional)
	page_size=3,     # ask for ~3 per page (optional)
)

total = 0
for page in pages:         # each page is a list[dict]
	print({"page_size": len(page), "sample": page[:2]})
	total += len(page)
print({"total_rows": total})
```

Parameters (all optional except `logical_name`)
- `logical_name`: str — Logical (singular) name, e.g., `"account"`.
- `select`: list[str] | None — Columns -> `$select` (comma joined).
- `filter`: str | None — OData `$filter` expression (e.g., `contains(name,'Acme') and statecode eq 0`).
- `orderby`: list[str] | None — Sort expressions -> `$orderby` (comma joined).
- `top`: int | None — Global cap via `$top` (applied on first request; service enforces across pages).
- `expand`: list[str] | None — Navigation expansions -> `$expand`; pass raw clauses (e.g., `primarycontactid($select=fullname,emailaddress1)`).
- `page_size`: int | None — Per-page hint using Prefer: `odata.maxpagesize=<N>` (not guaranteed; last page may be smaller).

Return value & semantics
- `$select`, `$filter`, `$orderby`, `$expand`, `$top` map directly to corresponding OData query options on the first request.
- `$top` caps total rows; the service may partition those rows across multiple pages.
- `page_size` (Prefer: `odata.maxpagesize`) is a hint; the server decides actual page boundaries.
- Returns a generator yielding non-empty pages (`list[dict]`). Empty pages are skipped.
- Each yielded list corresponds to a `value` page from the Web API.
- Iteration stops when no `@odata.nextLink` remains (or when `$top` satisfied server-side).
- The generator does not materialize all results; pages are fetched lazily.

Example (all parameters + expected response)

```python
pages = client.get(
		"account",
		select=["accountid", "name", "createdon", "primarycontactid"],
		filter="contains(name,'Acme') and statecode eq 0",
		orderby=["name asc", "createdon desc"],
		top=5,
		expand=["primarycontactid($select=fullname,emailaddress1)"],
		page_size=2,
)

for page in pages:  # page is list[dict]
		# Expected page shape (illustrative):
		# [
		#   {
		#     "accountid": "00000000-0000-0000-0000-000000000001",
		#     "name": "Acme West",
		#     "createdon": "2025-08-01T12:34:56Z",
		#     "primarycontactid": {
		#         "contactid": "00000000-0000-0000-0000-0000000000aa",
		#         "fullname": "Jane Doe",
		#         "emailaddress1": "jane@acme.com"
		#     },
		#     "@odata.etag": "W/\"123456\""
		#   },
		#   ...
		# ]
		print({"page_size": len(page)})
```


### Custom table (metadata) example

```python
# Support enums with labels in different languages
class Status(IntEnum):
	Active = 1
	Inactive = 2
	Archived = 5
	__labels__ = {
		1033: {
			"Active": "Active",
			"Inactive": "Inactive",
			"Archived": "Archived",
		},
		1036: {
			"Active": "Actif",
			"Inactive": "Inactif",
			"Archived": "Archivé",
		}
	}

# Create a simple custom table and a few columns
info = client.create_table(
	"SampleItem",  # friendly name; defaults to SchemaName new_SampleItem
	{
		"code": "string",
		"count": "int",
		"amount": "decimal",
		"when": "datetime",
		"active": "bool",
		"status": Status,
	},
	solution_unique_name="my_solution_unique_name",  # optional: associate table with this solution
)

# Create or delete columns
client.create_column("SampleItem", {"category": "string"})  # returns ["new_Category"]
client.delete_column("SampleItem", "category")  # returns ["new_Category"]

logical = info["entity_logical_name"]  # e.g., "new_sampleitem"

# Create a record in the new table
# Set your publisher prefix (used when creating the table). If you used the default, it's "new".
prefix = "new"
name_attr = f"{prefix}_name"
id_attr = f"{logical}id"

rec_id = client.create(logical, {name_attr: "Sample A"})[0]

# Clean up
client.delete(logical, rec_id)          # delete record
client.delete_table("SampleItem")       # delete table (friendly name or explicit schema new_SampleItem)
```

Notes:
- `create` always returns a list of GUIDs (length 1 for single input).
- `update` and `delete` return `None` for both single and multi.
- Passing a list of payloads to `create` triggers bulk create and returns `list[str]` of IDs.
- `get` supports single record retrieval with record id or paging through result sets (prefer `select` to limit columns).
- For CRUD methods that take a record id, pass the GUID string (36-char hyphenated). Parentheses around the GUID are accepted but not required.
* SQL queries are executed directly against entity set endpoints using the `?sql=` parameter. Supported subset only (single SELECT, optional WHERE/TOP/ORDER BY, alias). Unsupported constructs will be rejected by the service.

### Pandas helpers

`PandasODataClient` is a thin wrapper around the low-level client. All methods accept logical (singular) names (e.g. `account`, `new_sampleitem`), not entity set (plural) names. See `examples/quickstart_pandas.py` for a DataFrame workflow.

VS Code Tasks
- Install deps: `Install deps (pip)`
- Run example: `Run Quickstart (Dataverse SDK)`

## Limitations / Future Work
- No general-purpose OData batching, upsert, or association operations yet.
- `DeleteMultiple` not yet exposed.
- Minimal retry policy in library (network-error only); examples include additional backoff for transient Dataverse consistency.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
