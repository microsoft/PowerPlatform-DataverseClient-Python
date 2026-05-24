# Async SDK Design

| | |
|---|---|
| **Status** | In progress |
| **Date** | April 2026 |
| **Options analysis** | [async-design-options.md](./async-design-options.md) |

---

## Summary

- `AsyncDataverseClient` will be a new standalone client that mirrors the sync `DataverseClient` API — the sync client and all its behavior are untouched
- Pure logic (payload building, URL construction, parsing) will live in shared base classes (`_ODataBase`, `_BatchBase`) inherited by both sync and async clients independently, not through inheritance of one from the other
- All async code will live under a dedicated **`aio/` sub-package** — async dependencies will be fully isolated and will never affect sync-only users
- `aiohttp` will be an **optional dependency**; sync-only users will install nothing new
- Full design options analysis: [async-design-options.md](./async-design-options.md)

**What's unchanged:** Models, error classes, configuration, constants, and utilities require no async changes and will be reused as-is by both clients.

---

## Implementation Pattern — Shared Pure Base + Sibling Clients

Two patterns were considered:

- **Inheritance pattern** — the async client inherits from the sync client and overrides all I/O methods with async equivalents
- **Sibling pattern** — pure logic is extracted into a shared base; the sync and async clients both inherit from that base independently

The sibling pattern is proposed. See [async-design-options.md](./async-design-options.md) for the full comparison.

In the sibling pattern, pure logic will live in `_ODataBase` and `_BatchBase`. The sync and async clients will inherit from the same base and will be siblings.

```
_ODataBase  (pure: URL building, payload construction, parsing, caches)
    ├── _ODataClient          (sync I/O)
    └── _AsyncODataClient     (async I/O)

_BatchBase  (pure: multipart serialisation, response parsing)
    ├── _BatchClient          (sync I/O)
    └── _AsyncBatchClient     (async I/O)
```

**Why the sibling pattern:**

- **Correct relationship** — async is not a subtype of sync. The sibling pattern has both clients inherit from a pure base class, a valid is-a relationship that both satisfy.
- **No silent coupling** — in the inheritance pattern, sync changes (a new I/O method, or an existing method gaining I/O) are silently inherited by the async client, potentially blocking the event loop with no error. In the sibling pattern, a missing async method fails immediately.
- **Type safety** — no `# type: ignore[override]` suppressions needed. Async methods are first-class definitions, not overrides with mismatched return types.

**Tradeoff accepted:** This pattern requires extracting pure logic out of the sync clients.

---

### Folder Structure — Dedicated Async Sub-package

Two patterns were considered:

- **Co-location pattern** — async files placed alongside their sync counterparts in the existing folders, distinguished by a naming prefix
- **Dedicated async sub-package pattern** — all async code grouped under a dedicated sub-package, mirroring the sync layout

The dedicated async sub-package pattern is proposed. See [async-design-options.md](./async-design-options.md) for the full comparison.

All async code will live under `aio/`, mirroring the sync layout.

```
src/PowerPlatform/Dataverse/
├── client.py                        # DataverseClient (sync entry point)
├── data/
│   ├── _odata_base.py               # shared pure base
│   ├── _odata.py                    # sync OData client
│   ├── _batch_base.py               # shared pure base
│   └── _batch.py                    # sync batch client
├── operations/                      # sync operations
│   ├── records.py
│   ├── tables.py
│   ├── query.py
│   ├── batch.py
│   ├── dataframe.py
│   └── files.py
└── aio/                             # ALL async code
    ├── async_client.py              # AsyncDataverseClient (async entry point)
    ├── core/
    │   ├── _async_auth.py           # AsyncTokenCredential implementation
    │   └── _async_http.py           # aiohttp-based HTTP client
    ├── data/
    │   ├── _async_odata.py          # async OData client
    │   └── _async_batch.py          # async batch client
    └── operations/
        ├── async_records.py
        ├── async_tables.py
        ├── async_query.py
        ├── async_batch.py
        ├── async_dataframe.py
        └── async_files.py
```

**Why the dedicated async sub-package:**

- **Dependency isolation** — the `aio/` boundary ensures `aiohttp` is never imported by the sync path. With co-location, `__init__.py` eager imports or accidental cross-imports can pull async deps into the sync path, causing `ImportError` for users who never installed `aiohttp`.
- **Azure SDK convention** — `azure-storage-blob`, `azure-data-tables`, and other Azure SDKs expose async clients under `aio/` sub-package ([Python Guidelines](https://azure.github.io/azure-sdk/python_design.html)), lowering the learning curve for developers who use other Azure SDKs.
- **Discoverability** — the full async surface is visible in one directory tree, not scattered across every folder in the project.

**Tradeoff accepted:** Sync and async counterparts live in different directories.

---

## SDK Components

| Component | Existing files | Async change |
|---|---|---|
| Entry point | `client.py` | New `AsyncDataverseClient` entry point |
| Core | `_auth.py`, `_http.py`, `_http_logger.py`, `config.py`, `errors.py`, `log_config.py` | New async auth (`AsyncTokenCredential`) and async HTTP client (`aiohttp`); rest reused as-is |
| Data layer | `_odata.py`, `_batch.py`, `_relationships.py`, `_upload.py`, `_raw_request.py` | New async OData, batch, relationships, and upload inheriting shared pure bases; `_raw_request.py` reused as-is |
| Operations | `records.py`, `tables.py`, `query.py`, `batch.py`, `dataframe.py`, `files.py` | New async counterpart for each — thin `async def` + `await` delegation wrappers |
| Models | `query_builder.py`, `record.py`, `filters.py`, `batch.py`, `relationship.py`, `table_info.py`, `upsert.py` | All reused as-is; `execute()` and `to_dataframe()` in query builder become coroutines in the async path |
| Common | `constants.py` | Reused as-is |
| Utils | `_pandas.py` | Reused as-is |

---

## Dependencies

| Dependency | Type | Required by |
|---|---|---|
| `aiohttp>=3.9` | Optional runtime | `aio/` only — never imported by the sync path |
| `pytest-asyncio` | test | Async test suite |

`aiohttp` will be listed as an optional extra in `pyproject.toml`. Sync-only users who do not install the extra will never encounter an import error originating from the async path.

---

## Implementation Notes

### `ClientSession` Lifecycle

`aiohttp.ClientSession` requires explicit closure to drain in-flight requests and release connections. Both usage patterns are supported:

```python
# Context manager (preferred — session lifecycle is explicit)
async with AsyncDataverseClient(url, credential) as client:
    await client.records.get(...)

# Standalone (supported — caller is responsible for closing)
client = AsyncDataverseClient(url, credential)
try:
    await client.records.get(...)
finally:
    await client.close()
```

One `ClientSession` will be shared across all requests for the client's lifetime. Creating a new session per request defeats connection pooling and is an antipattern in `aiohttp`. The session is created lazily on the first request for standalone usage, or in `__aenter__` for context manager usage, and passed down to `_AsyncODataClient` → `_AsyncHttpClient`, which uses it but does not own it.

**Timeouts:** Timeouts will be configured per-request via `aiohttp.ClientTimeout`, matching the sync client's per-method defaults (120s for writes, 10s for reads). `aiohttp` automatically discards failed or timed-out connections from the pool — no manual pool recovery is needed.

---

### Concurrency — `asyncio.gather` + `Semaphore`

For bulk record operations (`records.create()`, `records.update()`, `records.upsert()`), the sync SDK uses `ThreadPoolExecutor(max_workers=N)` to dispatch record chunks concurrently. The async equivalent will replace the thread pool with `asyncio.gather()` and an `asyncio.Semaphore` to enforce a concurrency cap (`max_workers` defaults to `10`; async coroutines are cheap so the cap is driven by Dataverse server-side throttling limits, not client resource constraints).

```python
semaphore = asyncio.Semaphore(max_workers)

async def _bounded(chunk):
    async with semaphore:
        return await _execute_with_retry_async(chunk)

await asyncio.gather(*[_bounded(chunk) for chunk in chunks])
```

**429 throttling:** Transient errors (429, 503, 504) will trigger a retry with backoff. No new chunk will be dispatched while one is waiting out its backoff — the concurrency cap is maintained during retries, matching the sync throttling behavior.

---

### QueryBuilder

`QueryBuilder.execute()` and `QueryBuilder.to_dataframe()` are sync methods that call into the sync client. In the async path, `AsyncQueryOperations.builder()` returns an `AsyncQueryBuilder` subclass that overrides both as `async def`, delegating to the async client.

**At GA**, `QueryBuilder` will be replaced by an inert `SelectQuery` builder, with `build()` remaining on `SelectQuery` and execution moving to `QueryOperations.execute()` / `AsyncQueryOperations.execute()`. `SelectQuery` is shared between sync and async — no async variant is needed. The async path adds only `await`:

```python
result = await client.query.execute(
    select("name", "revenue")
    .from_("account")
    .where(eq("statecode", 0))
)
for record in result:
    print(record["name"])
```

---

### Error Handling and Cancellation

**`asyncio.CancelledError`** propagates — the SDK never suppresses it. Since Python 3.8 it is a `BaseException`, so the retry loop's `except aiohttp.ClientError` clause will not catch it. A cancelled request's connection is discarded from the pool; the `ClientSession` remains valid for future requests. Final cleanup is handled by `async with` or `await client.close()`.

**Error types:** No new async-specific error types are introduced.

| Failure type | Sync | Async |
|---|---|---|
| HTTP error (4xx, 5xx) | `HttpError` | `HttpError` — reused as-is |
| Network error (connection, timeout) | `requests.exceptions.RequestException` | `aiohttp.ClientError` — same pattern, different library |
| Cancellation | N/A | `asyncio.CancelledError` — propagates as `BaseException` |

`HttpError`, `ValidationError`, `MetadataError`, and `SQLParseError` are all reused unchanged.

---

### Pagination

OData pagination is sequential — each `@odata.nextLink` URL is only known after receiving the previous page's response. The async implementation will use an async generator, structurally identical to the sync version with `await` on each page fetch.

The SDK has two pagination patterns, each consumed differently:

- **OData (`records.get`)** — async generator; iterated with `async for`, yields one page at a time. Each `@odata.nextLink` is fetched sequentially.

    ```python
    async with AsyncDataverseClient(url, credential) as client:
        async for page in client.records.get(
            "account",
            filter="statecode eq 0",
            select=["name", "telephone1"],
            page_size=50,
        ):
            for record in page:
                print(record["name"])
    ```

- **SQL (`query.sql`)** — coroutine; called with `await`, returns a flat list after all pages are collected internally.

    ```python
    rows = await client.query.sql("SELECT TOP 100 name FROM account")
    for row in rows:
        print(row["name"])
    ```

---

## Testing Strategy

- Unit tests for all async implementations with >= 90% coverage
- Integration tests similar to the existing sync integration tests — same scenarios and coverage, adapted to use `AsyncDataverseClient` with `async`/`await`

---

## Implementation Phases

| Phase | Deliverable |
|---|---|
| 1 — Refactoring | Extract pure logic into `_ODataBase` and `_BatchBase`; sync client inherits from base; all existing sync tests should pass |
| 2 — Async implementation | `aio/` sub-package with async HTTP, auth, data layer, operations, and `AsyncDataverseClient` entry point |

---

## Examples

### Basic usage

```python
from PowerPlatform.Dataverse.aio import AsyncDataverseClient

async with AsyncDataverseClient(url, credential) as client:
    record_id = await client.records.create("account", {"name": "Contoso"})
    record = await client.records.get("account", record_id)
    await client.records.delete("account", record_id)
```

### Batch

```python
async with AsyncDataverseClient(url, credential) as client:
    batch = client.batch.new()
    batch.records.create("account", {"name": "A"})
    batch.records.create("account", {"name": "B"})
    result = await batch.execute()
```
