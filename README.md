# Dataverse SDK (Python) — Proof of Concept

A minimal Python SDK to use Microsoft Dataverse as a database for Azure AI Foundry–style apps.

- Read (SQL) — Execute constrained read-only SQL via the Dataverse Web API `?sql=` parameter. Returns `list[dict]`.
- OData CRUD — Thin wrappers over Dataverse Web API (create/get/update/delete).
- Bulk create — Pass a list of records to `create(...)` to invoke the bound `CreateMultiple` action; returns `list[str]` of GUIDs. If `@odata.type` is absent the SDK resolves the logical name from metadata (cached).
- Bulk update — Call `update_multiple(entity_set, records)` to invoke the bound `UpdateMultiple` action; returns nothing. Each record must include the real primary key attribute (e.g. `accountid`).
- Retrieve multiple (paging) — Generator-based `get_multiple(...)` that yields pages, supports `$top` and Prefer: `odata.maxpagesize` (`page_size`).
- Metadata helpers — Create/inspect/delete simple custom tables (EntityDefinitions + Attributes).
- Pandas helpers — Convenience DataFrame oriented wrappers for quick prototyping/notebooks.
- Auth — Azure Identity (`TokenCredential`) injection.

## Features

- Simple `DataverseClient` facade for CRUD, SQL (read-only), and table metadata.
- SQL-over-API: Constrained SQL (single SELECT with limited WHERE/TOP/ORDER BY) via native Web API `?sql=` parameter.
- Table metadata ops: create simple custom tables with primitive columns (string/int/decimal/float/datetime/bool) and delete them.
- Bulk create via `CreateMultiple` (collection-bound) by passing `list[dict]` to `create(entity_set, payloads)`; returns list of created IDs.
- Bulk update via `UpdateMultiple` by calling `update_multiple(entity_set, records)` with primary key attribute present in each record; returns nothing.
- Retrieve multiple with server-driven paging: `get_multiple(...)` yields lists (pages) following `@odata.nextLink`. Control total via `$top` and per-page via `page_size` (Prefer: `odata.maxpagesize`).
- Optional pandas integration (`PandasODataClient`) for DataFrame based create / get / query.

Auth:
- Credential is optional; if omitted, the SDK uses `DefaultAzureCredential`.
- You can pass any `azure.core.credentials.TokenCredential` you prefer; examples use `InteractiveBrowserCredential` for local runs.
- Token scope used by the SDK: `https://<yourorg>.crm.dynamics.com/.default` (derived from `base_url`).

## Install

Create and activate a Python 3.13+ environment, then install dependencies:

```powershell
# from the repo root
python -m pip install -r requirements.txt
```

Direct TDS via ODBC is not used; SQL reads are executed via the Web API using the `?sql=` query parameter.

## Configuration Notes

- For Web API (OData), tokens target your Dataverse org URL scope: https://yourorg.crm.dynamics.com/.default. The SDK requests this scope from the provided TokenCredential.
	(Preprod environments may surface newest SQL subset capabilities sooner than production.)

### Configuration (DataverseConfig)

Pass a `DataverseConfig` or rely on sane defaults:

```python
from dataverse_sdk import DataverseClient
from dataverse_sdk.config import DataverseConfig

cfg = DataverseConfig()  # defaults: language_code=1033
client = DataverseClient(base_url="https://yourorg.crm.dynamics.com", config=cfg)

# Optional HTTP tunables (timeouts/retries)
# cfg.http_retries, cfg.http_backoff, cfg.http_timeout
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
- Retrieve multiple with paging (contrasting `$top` vs `page_size`)
- Executing a read-only SQL query (Web API `?sql=`)

## Examples

### DataverseClient (recommended)

Tip: You can omit the credential and the SDK will use `DefaultAzureCredential` automatically:

```python
from dataverse_sdk import DataverseClient

base_url = "https://yourorg.crm.dynamics.com"
client = DataverseClient(base_url=base_url)  # uses DefaultAzureCredential by default
```

```python
from azure.identity import DefaultAzureCredential
from dataverse_sdk import DataverseClient

base_url = "https://yourorg.crm.dynamics.com"
client = DataverseClient(base_url=base_url, credential=DefaultAzureCredential())

# Create (returns created record)
created = client.create("accounts", {"name": "Acme, Inc.", "telephone1": "555-0100"})
account_id = created["accountid"]

# Read
account = client.get("accounts", account_id)

# Update (returns updated record)
updated = client.update("accounts", account_id, {"telephone1": "555-0199"})

# Bulk update (collection-bound UpdateMultiple)
# Each record must include the primary key attribute (accountid). The call returns None.
client.update_multiple("accounts", [
	{"accountid": account_id, "telephone1": "555-0200"},
])
print({"bulk_update": "ok"})

# Delete
client.delete("accounts", account_id)

# SQL (read-only) via Web API `?sql=`
rows = client.query_sql("SELECT TOP 3 accountid, name FROM account ORDER BY createdon DESC")
for r in rows:
	print(r.get("accountid"), r.get("name"))

## Bulk create (CreateMultiple)

Pass a list of payloads to `create(entity_set, payloads)` to invoke the collection-bound `Microsoft.Dynamics.CRM.CreateMultiple` action. The method returns a `list[str]` of created record IDs.

```python
# Bulk create accounts (returns list of GUIDs)
payloads = [
	{"name": "Contoso"},
	{"name": "Fabrikam"},
	{"name": "Northwind"},
]
ids = client.create("accounts", payloads)
assert isinstance(ids, list) and all(isinstance(x, str) for x in ids)
print({"created_ids": ids})
```

## Bulk update (UpdateMultiple)

Use `update_multiple(entity_set, records)` for a transactional batch update. The method returns `None`.

```python
ids = client.create("accounts", [
    {"name": "Fourth Coffee"},
    {"name": "Tailspin"},
])

client.update_multiple("accounts", [
    {"accountid": ids[0], "telephone1": "555-1111"},
    {"accountid": ids[1], "telephone1": "555-2222"},
])
print({"bulk_update": "ok"})
```

Notes:
- Each record must include the primary key attribute (e.g. `accountid`). No `id` alias yet.
- If any payload omits `@odata.type`, the logical name is resolved once and stamped (same as bulk create).
- Entire request fails (HTTP error) if any individual update fails; no partial success list is returned.
- If you need refreshed records post-update, issue individual `get` calls or a `get_multiple` query.

Notes:
- The bulk create response typically includes IDs only; the SDK returns the list of GUID strings.
- Single-record `create` still returns the full entity representation.
- `@odata.type` handling: If any payload in the list omits `@odata.type`, the SDK performs a one-time metadata query (`EntityDefinitions?$filter=EntitySetName eq '<entity_set>'`) to resolve the logical name, caches it, and stamps each missing item with `Microsoft.Dynamics.CRM.<logical>`. If **all** payloads already include `@odata.type`, no metadata call is made.
- The metadata lookup is per entity set and reused across subsequent multi-create calls in the same client instance (in-memory cache only).


## Retrieve multiple with paging

Use `get_multiple(entity_set, ...)` to stream results page-by-page. You can cap total results with `$top` and hint the per-page size with `page_size` (sets Prefer: `odata.maxpagesize`).

```python
# Iterate pages of accounts ordered by name, selecting a few columns
pages = client.get_multiple(
	"accounts",
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

Parameters (all optional except `entity_set`)
- `entity_set`: str — Entity set (plural logical name), e.g., `"accounts"`.
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
pages = client.get_multiple(
		"accounts",
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
# Create a simple custom table and a few primitive columns
info = client.create_table(
	"SampleItem",  # friendly name; defaults to SchemaName new_SampleItem
	{
		"code": "string",
		"count": "int",
		"amount": "decimal",
		"when": "datetime",
		"active": "bool",
	},
)

entity_set = info["entity_set_name"]  # e.g., "new_sampleitems"
logical = info["entity_logical_name"]  # e.g., "new_sampleitem"

# Create a record in the new table
# Set your publisher prefix (used when creating the table). If you used the default, it's "new".
prefix = "new"
name_attr = f"{prefix}_name"
id_attr = f"{logical}id"

rec = client.create(entity_set, {name_attr: "Sample A"})

# Clean up
client.delete(entity_set, rec[id_attr])  # delete record
client.delete_table("SampleItem")        # delete the table
```

Notes:
- `create/update` return the full record using `Prefer: return=representation`.
- Passing a list of payloads to `create` triggers bulk create and returns `list[str]` of IDs.
- Use `get_multiple` for paging through result sets; prefer `select` to limit columns.
- For CRUD methods that take a record id, pass the GUID string (36-char hyphenated). Parentheses around the GUID are accepted but not required.
* SQL queries are executed directly against entity set endpoints using the `?sql=` parameter. Supported subset only (single SELECT, optional WHERE/TOP/ORDER BY, alias). Unsupported constructs will be rejected by the service.

### Pandas helpers

See `examples/quickstart_pandas.py` for a DataFrame workflow via `PandasODataClient`.

VS Code Tasks
- Install deps: `Install deps (pip)`
- Run example: `Run Quickstart (Dataverse SDK)`

## Limitations / Future Work
- No general-purpose OData batching, upsert, or association operations yet.
- `DeleteMultiple` not yet exposed.
- Minimal retry policy in library (network-error only); examples include additional backoff for transient Dataverse consistency.
- Entity naming conventions in Dataverse: for multi-create the SDK resolves logical names from entity set metadata.

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
