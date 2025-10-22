# Dataverse SDK for Python

Lean Dataverse Web API + SQL-over-API client focused on fast CRUD, simple metadata, file upload, and optional pandas helpers. Built on Azure Identity (any `TokenCredential`).

## 1. Install

Python 3.10+ (recommended 3.12/3.13):

```powershell
python -m pip install -r requirements.txt
```

## 2. Quickstart (CRUD + SQL)

```python
from dataverse_sdk import DataverseClient

client = DataverseClient(base_url="https://yourorg.crm.dynamics.com")  # uses DefaultAzureCredential if none passed

# Create (list[str] – length 1 for single)
account_id = client.create("account", {"name": "Acme", "telephone1": "555-0100"})[0]

# Read one
record = client.get("account", account_id)

# Update
client.update("account", account_id, {"telephone1": "555-0199"})

# Bulk create (CreateMultiple)
ids = client.create("account", [
    {"name": "Contoso"},
    {"name": "Fabrikam"},
])

# Bulk update broadcast
client.update("account", ids, {"telephone1": "555-0200"})

# Bulk update 1:1
client.update("account", ids, [
    {"telephone1": "555-1200"},
    {"telephone1": "555-1300"},
])

# SQL (Web API ?sql= subset)
rows = client.query_sql("SELECT TOP 3 accountid, name FROM account ORDER BY createdon DESC")
for r in rows: print(r.get("accountid"), r.get("name"))

# Delete
client.delete("account", account_id)
```

## 3. API Cheat Sheet

All logical names are singular (e.g. "account"). Single create returns `list[str]` length 1; update/delete return `None`.

```python
# create (single)
client.create("contact", {"firstname": "Ada", "lastname": "Lovelace"})  # -> [guid]

# create (bulk)
client.create("contact", [{"firstname": "Alan"}, {"firstname": "Grace"}])  # -> [guid, guid]

# get (single)
client.get("account", some_guid)

# get (multiple pages)
for page in client.get("account", select=["accountid", "name"], top=5, page_size=2):
    for row in page: ...  # page is list[dict]

# update single
client.update("account", some_guid, {"name": "Acme Updated"})

# update broadcast
client.update("account", [id1, id2], {"statecode": 1})

# update 1:1
client.update("account", [id1, id2], [{"name": "A"}, {"name": "B"}])

# delete single
client.delete("account", some_guid)

# delete many
client.delete("account", [id1, id2])

# query SQL
client.query_sql("SELECT TOP 10 accountid, name FROM account ORDER BY name ASC")

# create table (simple schema + IntEnum status)
from enum import IntEnum
class Status(IntEnum):
    Active = 1
    Inactive = 2

info = client.create_table("SampleItem", {
    "code": "string",
    "count": "int",
    "amount": "decimal",
    "when": "datetime",
    "active": "bool",
    "status": Status,
})

# get table info
client.get_table_info("new_SampleItem")

# list tables
client.list_tables()

# delete table
client.delete_table("SampleItem")

# upload file (auto selects mode by size)
client.upload_file("account", some_guid, "sample_filecolumn", "example.pdf")

# flush cache (picklist labels)
client.flush_cache("picklist")
```

## 4. Paging Example (generator)

```python
pages = client.get("account", select=["accountid", "name"], top=6, page_size=3)
for page in pages:
    print(len(page), "records")
```

## 5. Configuration

```python
from dataverse_sdk.config import DataverseConfig
cfg = DataverseConfig()  # uses env + defaults (language_code=1033)
client = DataverseClient(base_url="https://yourorg.crm.dynamics.com", config=cfg)

# Optional HTTP tuning
# cfg.http_retries, cfg.http_backoff, cfg.http_timeout
```

Token scope requested: `https://<org>.crm.dynamics.com/.default`.

## 6. Error Handling (Overview)

Operations raise structured exceptions (e.g. `HttpError`, `ValidationError`, `MetadataError`). Inspect `status_code`, `subcode`, `service_error_code`, `correlation_id`, `retry_after`, `is_transient` to decide on logging vs retry.

## 7. Pandas Helpers

Optional thin wrapper (`PandasODataClient`) for DataFrame-centric create/get/query. See `examples/quickstart_pandas.py`.

## 8. Contributing

We welcome issues and PRs. Most contributions require signing the Microsoft CLA: <https://cla.opensource.microsoft.com>.

Code of Conduct: <https://opensource.microsoft.com/codeofconduct/> – questions: <mailto:opencode@microsoft.com>.

## 9. Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow the [Microsoft Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general). Use in modified versions must not imply Microsoft sponsorship. Third‑party marks remain subject to their policies.

## 10. License

MIT (see `LICENSE`).

---
Fast ramp: Install, instantiate `DataverseClient`, call `create / get / update / query_sql`. Use logical names (singular). Bulk operations determined by passing lists. Paging yields lists of dict per page. File uploads pick method automatically.
