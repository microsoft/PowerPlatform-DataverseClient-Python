# Async Architecture Design

## Design Philosophy

The async layer follows the **inheritance-based async pattern** used by the Azure SDK for Python:
a parallel class hierarchy that inherits all pure-logic from the sync classes and overrides only
the three blocking operations.

### Only 3 blocking operations get async overrides

| Blocking operation | Sync implementation | Async implementation |
|---|---|---|
| Token acquisition | `credential.get_token(scope)` | `await credential.get_token(scope)` |
| HTTP I/O | `requests.request(...)` | `await aiohttp.ClientSession.request(...)` |
| Sleep / backoff | `time.sleep(delay)` | `await asyncio.sleep(delay)` |

Everything else — URL building, OData serialization, key formatting, cache lookups,
payload construction, error parsing — is **pure CPU logic** that runs in microseconds.
These methods are inherited directly from the sync classes with no override needed.

## Class Hierarchy

```
azure.core.credentials.TokenCredential
    _AuthManager._acquire_token(scope)  →  credential.get_token(scope)

azure.core.credentials_async.AsyncTokenCredential
    _AsyncAuthManager._acquire_token(scope)  →  await credential.get_token(scope)

_HttpClient._request(method, url, **kw)  →  requests.request(...)
    _ODataClient._raw_request(...)
        _ODataClient._request(...)
            ... all sync CRUD / metadata methods

_AsyncHttpClient._request(method, url, **kw)  →  await aiohttp.ClientSession.request(...)
    _AsyncODataClient(inherits _ODataClient)
        override: _raw_request, _request, _headers, _merge_headers
        override: _create, _create_multiple, _update, _update_multiple, ...
        override: _entity_set_from_schema_name, _get, _get_multiple, ...
        inherited: _format_key, _build_alternate_key_str, _escape_odata_quotes, ...
        inherited: _attribute_payload, _label, _to_pascal, _normalize_cache_key, ...

DataverseClient
    records → RecordOperations (sync)
    query   → QueryOperations (sync)
    tables  → TableOperations (sync)
    files   → FileOperations (sync)

AsyncDataverseClient
    records → AsyncRecordOperations (async)
    query   → AsyncQueryOperations (async)
    tables  → AsyncTableOperations (async)
    files   → AsyncFileOperations (async)
```

## File Map

| File | Purpose |
|---|---|
| `core/_auth.py` | Sync `_AuthManager` (unchanged) |
| `core/_async_auth.py` | Async `_AsyncAuthManager` |
| `core/_http.py` | Sync `_HttpClient` using `requests` (unchanged) |
| `core/_async_http.py` | Async `_AsyncHttpClient` using `aiohttp` |
| `data/_odata.py` | Sync `_ODataClient` (unchanged) |
| `data/_async_odata.py` | Async `_AsyncODataClient` inheriting `_ODataClient` |
| `client.py` | Sync `DataverseClient` (unchanged) |
| `async_client.py` | Async `AsyncDataverseClient` |
| `operations/records.py` | Sync `RecordOperations` (unchanged) |
| `operations/async_records.py` | Async `AsyncRecordOperations` |
| `operations/query.py` | Sync `QueryOperations` (unchanged) |
| `operations/async_query.py` | Async `AsyncQueryOperations` |
| `operations/tables.py` | Sync `TableOperations` (unchanged) |
| `operations/async_tables.py` | Async `AsyncTableOperations` |
| `operations/files.py` | Sync `FileOperations` (unchanged) |
| `operations/async_files.py` | Async `AsyncFileOperations` |

## Async HTTP Response Wrapper

`_AsyncODataClient` depends on response objects that provide `.status_code`, `.headers`,
`.text`, and `.json()` synchronously (matching the `requests.Response` interface used
throughout `_ODataClient`).

`_AsyncResponse` achieves this by **eagerly reading the entire response body** when the
aiohttp request completes. This is acceptable for Dataverse API responses (which are
typically small JSON payloads). File uploads use streaming writes (not reads), so
eager body reading does not affect upload performance.

## Async Generator for `_get_multiple`

The sync `_get_multiple` is a regular generator (`yield`). The async version is an
**async generator** (`async def` with `yield`), enabling callers to iterate pages with
`async for`:

```python
async for page in od._get_multiple("account", filter="statecode eq 0"):
    for row in page:
        print(row["name"])
```

At the public API level, `AsyncRecordOperations.get()` returns an async generator function:

```python
pages = await client.records.get("account", filter="statecode eq 0")
async for page in pages:
    for record in page:
        print(record["name"])
```

## ContextVar Correlation IDs

`_CALL_SCOPE_CORRELATION_ID` is a `ContextVar`. Python's `ContextVar` is fully compatible
with asyncio — each task gets its own copy of the context. The `_call_scope()` context
manager (sync `@contextmanager`) is used inside `@asynccontextmanager` (`_scoped_odata`)
via a regular `with` statement — this is safe because the ContextVar set/reset is
instantaneous (no I/O).

## Usage Comparison

### Sync (existing code — unchanged)

```python
from azure.identity import ClientSecretCredential
from PowerPlatform.Dataverse.client import DataverseClient

credential = ClientSecretCredential(tenant_id, client_id, client_secret)

with DataverseClient("https://org.crm.dynamics.com", credential) as client:
    guid = client.records.create("account", {"name": "Contoso"})
    record = client.records.get("account", guid)
    client.records.update("account", guid, {"telephone1": "555-0100"})
    client.records.delete("account", guid)
```

### Async (new)

```python
import asyncio
from azure.identity.aio import ClientSecretCredential
from PowerPlatform.Dataverse.async_client import AsyncDataverseClient

credential = ClientSecretCredential(tenant_id, client_id, client_secret)

async def main():
    async with AsyncDataverseClient("https://org.crm.dynamics.com", credential) as client:
        guid = await client.records.create("account", {"name": "Contoso"})
        record = await client.records.get("account", guid)
        await client.records.update("account", guid, {"telephone1": "555-0100"})
        await client.records.delete("account", guid)

asyncio.run(main())
```

## Installation

### Async support (optional dependency)

```bash
pip install "PowerPlatform-Dataverse-Client[async]"
```

This installs `aiohttp>=3.13.3` in addition to the core dependencies. The sync client
continues to work without `aiohttp` installed.

## Migration Guide

Existing sync code does **not** need to change. Async support is purely additive:

1. Change import: `from PowerPlatform.Dataverse.client import DataverseClient`
   → `from PowerPlatform.Dataverse.async_client import AsyncDataverseClient`

2. Use `async with` instead of `with`

3. Use async credentials: `azure.identity.aio.*` instead of `azure.identity.*`

4. Add `await` before every operation call

5. Use `async for` when iterating pages returned by `client.records.get(table, ...)`
