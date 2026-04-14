# Async SDK Design Plan

**ADO Task:** 6012729  
**Status:** Draft  
**Date:** April 2026

---

## 1. Goals & Constraints

| # | Requirement |
|---|-------------|
| G1 | Provide `async`/`await` API for all existing sync operations |
| G2 | Follow Azure SDK for Python guidelines (separate sync/async clients, `.aio` namespace) |
| G3 | Zero breaking changes to existing sync API |
| G4 | Minimize code duplication between sync and async paths |
| G5 | New async dependency (`aiohttp`) is optional — sync users never import it |

---

## 2. Current Architecture (I/O boundary analysis)

All HTTP I/O flows through exactly **two chokepoints**:

| Layer | Class | Method | I/O type |
|-------|-------|--------|----------|
| HTTP  | `_HttpClient` | `_request(method, url, **kwargs)` | `requests.request()` + `time.sleep()` retry |
| Auth  | `_AuthManager` | `_acquire_token(scope)` | `credential.get_token(scope)` |

`_ODataClient` is a ~2 800-line class with **33 instance methods**.
Every method that calls `self._request()` or `self._raw_request()` is I/O and needs an async twin.

### Method classification

**Pure logic (no I/O) — shareable as-is:**

| Method | Purpose |
|--------|---------|
| `_escape_odata_quotes` | OData string escaping (static) |
| `_normalize_cache_key` | Lowercase cache key (static) |
| `_lowercase_keys` | Dict key normalization (static) |
| `_lowercase_list` | List lowering (static) |
| `_format_key` | GUID → `(guid)` formatting |
| `_build_alternate_key_str` | Alternate key URL segment |
| `_label` / `_to_pascal` | Metadata label/name helpers |
| `_build_localizedlabels_payload` | Translation payload builder |
| `_normalize_picklist_label` | Unicode NFD → NFC + lowercase |
| `_convert_labels_to_ints` | Picklist label → int (cache read only) |
| `_build_delete_multiple` | `_RawRequest` builder |
| `_build_delete_entity` | `_RawRequest` builder |
| `_build_get_entity` | `_RawRequest` builder |
| `_build_delete_relationship` | `_RawRequest` builder |
| `_build_get_relationship` | `_RawRequest` builder |
| `_build_sql` | `_RawRequest` builder |
| All `_build_*` methods | Return `_RawRequest` — no network |

**I/O methods — need async versions (~20 methods):**

| Method | I/O reason |
|--------|-----------|
| `_request` / `_raw_request` / `_execute_raw` | Core HTTP dispatch |
| `_headers` / `_merge_headers` | Calls `_acquire_token` |
| `_create` / `_create_multiple` | POST requests |
| `_update` / `_update_multiple` | PATCH requests |
| `_upsert_multiple` | POST (action) |
| `_delete` | DELETE request |
| `_get` | GET request |
| `_query_sql` | GET with pagination loop |
| `_entity_set_from_schema_name` / `_primary_id_attr` | Metadata GET (cached) |
| `_bulk_fetch_picklists` / `_request_metadata_with_retry` | Metadata GET with retry |
| `_get_table_info` / `_delete_table` | Metadata CRUD |
| `_get_alternate_keys` / `_delete_alternate_key` | Metadata CRUD |
| `_upload_file_small` / `_upload_file_chunk` | File PATCH (mixin) |
| `_delete_relationship` / `_get_relationship` | Metadata (mixin) |
| `close` | Session teardown |

---

## 3. Options Considered

### Option A — Separate async class hierarchy (Azure SDK pattern)

```
src/PowerPlatform/Dataverse/
├── aio/                          # NEW async subpackage
│   ├── __init__.py               # exports AsyncDataverseClient
│   ├── client.py                 # AsyncDataverseClient
│   ├── operations/
│   │   ├── records.py            # AsyncRecordOperations (async def create, ...)
│   │   ├── query.py
│   │   ├── tables.py
│   │   ├── files.py
│   │   ├── dataframe.py
│   │   └── batch.py
│   ├── data/
│   │   ├── _async_odata.py       # _AsyncODataClient
│   │   ├── _async_upload.py      # _AsyncFileUploadMixin
│   │   └── _async_relationships.py
│   └── core/
│       ├── _async_http.py        # _AsyncHttpClient (aiohttp)
│       └── _async_auth.py        # _AsyncAuthManager
├── client.py                     # DataverseClient (unchanged)
├── data/_odata.py                # _ODataClient    (unchanged)
└── ...
```

**How it works:**
- Full copy of every I/O class with `async def` methods
- Pure logic is imported from the sync modules (no duplication of `_build_*`, statics)
- `_AsyncODataClient` inherits static/pure methods via a shared base or by importing them directly, overrides every I/O method with `async def`

**Pros:**
- Follows Azure SDK standard exactly (`.aio` subpackage, same class names)
- No runtime overhead or metaclass tricks
- Sync path completely untouched — zero risk of breaking it
- Each side can evolve independently if needed

**Cons:**
- ~20 I/O methods need async copies (the substantive duplication)
- 6 operations files need async mirrors
- Bug fixes must be applied in two places for I/O methods

**Estimated duplication:** ~600–800 lines of async I/O method bodies (the `_build_*` logic and static helpers are shared, so actual unique logic duplication is smaller — mostly the async wrappers around the same `_build_*` → `_execute_raw` flow)

---

### Option B — Inheritance override (async subclass of sync)

```python
class _AsyncODataClient(_ODataClient):
    """Override only the I/O methods with async versions."""

    async def _request(self, method, url, *, expected=..., **kwargs):
        # aiohttp version
        ...

    async def _create(self, entity_set, table_schema_name, record):
        # Must be async because it calls self._request()
        ...
```

**Problem:** Python can't have a sync base method calling `await self._request()`; the calling method must also be `async`. So every method in the chain needs to be overridden. This ends up ~identical to Option A duplication-wise, but with the fragility of inheritance — any refactor to the base class silently breaks the subclass if the override doesn't match.

**Pros:**
- Slightly less structural boilerplate (no separate directory tree)

**Cons:**
- Same method duplication as Option A
- Fragile: base class changes can silently break async path
- Violates LSP: async methods can't be used where sync methods are expected
- Not the Azure SDK standard
- ❌ **Not recommended**

---

### Option C — Code generation (`unasync`)

Write **async-first** code, then auto-generate the sync version at build time using a tool like [`unasync`](https://github.com/python-trio/unasync).

```python
# _async_odata.py (source of truth)
async def _create(self, entity_set, table_schema_name, record):
    payload = self._build_create_payload(record)
    resp = await self._request("post", url, json=payload)
    ...

# _odata.py (auto-generated from above)
def _create(self, entity_set, table_schema_name, record):
    payload = self._build_create_payload(record)
    resp = self._request("post", url, json=payload)
    ...
```

**Pros:**
- Single source of truth for business logic
- One bug fix → both paths updated automatically

**Cons:**
- `unasync` is a text-transform tool — brittle with context managers, `asyncio.sleep` → `time.sleep`, `aiohttp` → `requests` etc.
- Complex build toolchain (must run unasync before packaging, CI must verify generated code is up to date)
- Harder to debug: the sync code a user steps through is auto-generated
- `unasync` is largely unmaintained (last release 2021, 1.5k GitHub stars)
- Would require rewriting ALL existing sync code as async-first — massive migration risk
- ❌ **Not recommended** given our codebase maturity

---

### Option D — `httpx` dual transport (single codebase)

Replace `requests` with `httpx`, which provides both `httpx.Client` (sync) and `httpx.AsyncClient` (async) with identical APIs.

```python
class _HttpClient:
    def __init__(self, ..., async_mode=False):
        if async_mode:
            self._client = httpx.AsyncClient(...)
        else:
            self._client = httpx.Client(...)
```

**Problem:** Even with a unified HTTP library, every method in the call chain still needs `async def` / `await` in the async path. You'd still need separate `_ODataClient` / `_AsyncODataClient` classes with separate method definitions. `httpx` doesn't magically make one function work both ways.

**Pros:**
- Single HTTP dependency for both modes
- `httpx` is modern, well-maintained, supports HTTP/2

**Cons:**
- **Breaking change**: replacing `requests` (0.1.0b8 is pre-GA, so technically allowed, but still churn)
- Still need full async class hierarchy for the calling code
- `httpx` response objects differ from `requests` — tests need updating
- Adds a net-new dependency that isn't in the Azure SDK "well known" list (requests + aiohttp are)
- ⚠️ **Could consider for v1.0**, but too disruptive for this task

---

## 4. Recommendation: Option A (with shared pure-logic base)

Option A is the best fit because:

1. **Azure SDK alignment** — The official Azure SDK guidelines explicitly require: "DO provide two separate client classes for synchronous and asynchronous operations" using an `.aio` subpackage
2. **Zero sync regression risk** — Existing sync code is completely untouched
3. **Optional dependency** — `aiohttp` only imported in the `aio` subpackage
4. **Clear, debuggable** — Users see exactly the code they're running
5. **Already proven** — Azure Blob Storage, Azure Key Vault, Azure Event Hubs all use this pattern

### Minimizing duplication

The ~20 I/O methods follow a predictable pattern:

```python
# Sync (_odata.py) — current code
def _create(self, entity_set, table_schema_name, record):
    payload = self._build_create_payload(...)  # pure logic
    r = self._request("post", url, json=payload)  # I/O
    return self._extract_guid(r)  # pure logic

# Async (_async_odata.py)
async def _create(self, entity_set, table_schema_name, record):
    payload = self._build_create_payload(...)  # SAME pure logic
    r = await self._request("post", url, json=payload)  # async I/O
    return self._extract_guid(r)  # SAME pure logic
```

The pure-logic parts (`_build_*`, static methods, `_extract_*`) are shared by importing from the sync module or a common base. Only the I/O wiring is duplicated — and it's thin (typically 3–10 lines per method).

---

## 5. Implementation Plan

### Phase 1: Prepare shared foundation

**Goal:** Extract pure logic so it can be imported by both sync/async without circular imports.

1. Move all `@staticmethod` and pure helper methods from `_ODataClient` into a new `_odata_common.py` mixin or keep them accessible via import
2. Move `_build_*` methods (already pure — they return `_RawRequest`) into a shared `_ODataBuilderMixin`
3. Verify sync tests still pass — ensure no behavioral change

### Phase 2: Core async infrastructure

**Create `core/` async modules:**

```python
# core/_async_http.py
class _AsyncHttpClient:
    """aiohttp-based HTTP client with retry + timeout."""

    def __init__(self, retries, backoff, timeout, session=None):
        self._session = session  # aiohttp.ClientSession or None
        ...

    async def _request(self, method, url, **kwargs) -> aiohttp.ClientResponse:
        for attempt in range(self.max_attempts):
            try:
                async with self._session.request(method, url, **kwargs) as resp:
                    body = await resp.read()
                    return _ResponseAdapter(resp.status, resp.headers, body)
            except aiohttp.ClientError:
                if attempt == self.max_attempts - 1:
                    raise
                await asyncio.sleep(self.base_delay * (2 ** attempt))

    async def close(self):
        if self._session:
            await self._session.close()
```

```python
# core/_async_auth.py
class _AsyncAuthManager:
    """Wraps AsyncTokenCredential from azure.identity.aio."""

    def __init__(self, credential: AsyncTokenCredential):
        ...

    async def _acquire_token(self, scope: str) -> _TokenPair:
        token = await self.credential.get_token(scope)
        return _TokenPair(resource=scope, access_token=token.token)
```

### Phase 3: Async OData client

```python
# aio/data/_async_odata.py
from ...data._odata import (
    _ODataClient,             # for shared pure logic
    _MULTIPLE_BATCH_SIZE,
    _DEFAULT_EXPECTED_STATUSES,
    _RequestContext,
    _extract_pagingcookie,
)

class _AsyncODataClient(_AsyncFileUploadMixin, _AsyncRelationshipOperationsMixin):
    """Async mirror of _ODataClient. Shares all pure logic."""

    # Inherit all @staticmethod pure helpers
    _escape_odata_quotes = _ODataClient._escape_odata_quotes
    _normalize_cache_key = _ODataClient._normalize_cache_key
    _lowercase_keys = _ODataClient._lowercase_keys
    _lowercase_list = _ODataClient._lowercase_list

    def __init__(self, auth, base_url, config=None, session=None):
        # Same init structure, but uses _AsyncHttpClient
        self.auth = auth  # _AsyncAuthManager
        self.base_url = (base_url or "").rstrip("/")
        self.api = f"{self.base_url}/api/data/v9.2"
        self.config = config or ...
        self._http = _AsyncHttpClient(...)
        self._logical_to_entityset_cache = {}
        self._logical_primaryid_cache = {}
        self._picklist_label_cache = {}
        self._picklist_cache_ttl_seconds = 3600
        self._picklist_cache_lock = asyncio.Lock()

    async def _request(self, method, url, *, expected=..., **kwargs):
        """Async version — same error handling as sync."""
        request_context = _RequestContext.build(
            method, url, expected=expected,
            merge_headers=await self._merge_headers_async(),
            **kwargs,
        )
        r = await self._http._request(request_context.method, request_context.url, **request_context.kwargs)
        # Same error handling logic (extracted to shared helper)
        ...

    async def _create(self, entity_set, table_schema_name, record):
        """Async create — same payload building, async I/O."""
        ...
```

### Phase 4: Async operations layer

```python
# aio/operations/records.py
class AsyncRecordOperations:
    def __init__(self, client: "AsyncDataverseClient"):
        self._client = client

    async def create(self, table, data, *, max_workers=1):
        async with self._client._scoped_odata() as od:
            if isinstance(data, dict):
                return await od._create(entity_set, table, data)
            return await od._create_multiple(entity_set, table, data, max_workers=max_workers)
```

### Phase 5: Async client

```python
# aio/client.py
from PowerPlatform.Dataverse.aio.operations.records import AsyncRecordOperations
...

class DataverseClient:
    """Async Dataverse client. Same name, .aio namespace."""

    def __init__(self, base_url, credential, config=None):
        self.auth = _AsyncAuthManager(credential)
        ...
        self.records = AsyncRecordOperations(self)
        self.query = AsyncQueryOperations(self)
        ...

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self):
        ...
```

### Phase 6: Concurrent batch dispatch (async)

Replace `ThreadPoolExecutor` with `asyncio.gather` for async chunk dispatch:

```python
async def _async_dispatch_chunks(fn, chunks, max_concurrency):
    semaphore = asyncio.Semaphore(min(max_concurrency, _MAX_WORKERS))

    async def _execute_with_retry(chunk):
        for attempt in range(_CHUNK_RETRY_LIMIT + 1):
            try:
                async with semaphore:
                    return await fn(chunk)
            except HttpError as exc:
                if exc.is_transient and attempt < _CHUNK_RETRY_LIMIT:
                    wait = float(exc.details.get("retry_after") or _CHUNK_RETRY_DEFAULT_WAIT)
                    wait += random.uniform(0, _CHUNK_RETRY_JITTER_MAX)
                    await asyncio.sleep(wait)
                else:
                    raise

    results = await asyncio.gather(*[_execute_with_retry(c) for c in chunks])
    return list(results)
```

### Phase 7: Packaging & dependencies

```toml
# pyproject.toml additions
[project.optional-dependencies]
aio = [
    "aiohttp>=3.9.0",
]
dev = [
    ...existing...
    "aiohttp>=3.9.0",
    "pytest-asyncio>=0.23.0",
]
```

Usage:
```bash
pip install PowerPlatform-Dataverse-Client[aio]
```

---

## 6. User-facing API

### Sync (unchanged)

```python
from PowerPlatform.Dataverse.client import DataverseClient
from azure.identity import DefaultAzureCredential

with DataverseClient("https://org.crm.dynamics.com", DefaultAzureCredential()) as client:
    guid = client.records.create("account", {"name": "Contoso"})
```

### Async (new)

```python
from PowerPlatform.Dataverse.aio.client import DataverseClient
from azure.identity.aio import DefaultAzureCredential

async with DataverseClient("https://org.crm.dynamics.com", DefaultAzureCredential()) as client:
    guid = await client.records.create("account", {"name": "Contoso"})
```

Key differences:
- Import from `PowerPlatform.Dataverse.aio` (not `PowerPlatform.Dataverse`)
- Credential from `azure.identity.aio` (async token provider)
- `async with` context manager
- `await` on all operations

---

## 7. Testing Strategy

| Test type | Approach |
|-----------|----------|
| Unit tests | Mirror sync test structure in `tests/unit/aio/`, using `pytest-asyncio` and `unittest.mock.AsyncMock` |
| Shared fixtures | `conftest.py` provides both sync and async mock clients |
| Coverage | Same 90% threshold applies to `aio/` code |
| CI | `pytest-asyncio` added to dev dependencies |

Example async test:

```python
@pytest.mark.asyncio
async def test_create_single(mock_async_odata):
    mock_async_odata._create = AsyncMock(return_value="abc-123")
    ops = AsyncRecordOperations(mock_client)
    result = await ops.create("account", {"name": "Contoso"})
    assert result == "abc-123"
```

---

## 8. Migration & Rollout

| Phase | Scope | PR |
|-------|-------|----|
| 1 | Extract `_ODataBuilderMixin` with pure logic | Small refactor PR |
| 2 | `core/_async_http.py` + `core/_async_auth.py` | Core async infra |
| 3 | `aio/data/_async_odata.py` (20 async I/O methods) | Main async PR |
| 4 | `aio/operations/*.py` (6 operation files) | Same or follow-up |
| 5 | `aio/client.py` + `__init__.py` exports | Same PR |
| 6 | Tests (`tests/unit/aio/`) | Same PR |
| 7 | `pyproject.toml` optional dep + docs | Same PR |

Phase 1 goes first as an independent PR to keep the main async PR smaller and reviewable.

---

## 9. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Duplication drift: sync and async methods diverge | Linting check: ensure async I/O methods match sync signatures; shared pure logic prevents logic drift |
| `aiohttp` session management | Follow `async with` pattern; provide `__aenter__`/`__aexit__` |
| Picklist cache thread-safe → async-safe | Replace `threading.Lock` with `asyncio.Lock` in async path |
| `_dispatch_chunks` threading → asyncio | Use `asyncio.gather` + `asyncio.Semaphore` (natural async concurrency) |
| Azure Identity async availability | `azure-identity` ships `azure.identity.aio` since v1.5.0 (2021) — well established |

---

## 10. Files to Create (full list)

```
src/PowerPlatform/Dataverse/
├── aio/
│   ├── __init__.py
│   ├── client.py                         # AsyncDataverseClient
│   ├── core/
│   │   ├── __init__.py
│   │   ├── _async_http.py                # _AsyncHttpClient
│   │   └── _async_auth.py                # _AsyncAuthManager
│   ├── data/
│   │   ├── __init__.py
│   │   ├── _async_odata.py               # _AsyncODataClient
│   │   ├── _async_upload.py              # _AsyncFileUploadMixin
│   │   └── _async_relationships.py       # _AsyncRelationshipOperationsMixin
│   └── operations/
│       ├── __init__.py
│       ├── records.py                    # AsyncRecordOperations
│       ├── query.py                      # AsyncQueryOperations
│       ├── tables.py                     # AsyncTableOperations
│       ├── files.py                      # AsyncFileOperations
│       ├── dataframe.py                  # AsyncDataFrameOperations
│       └── batch.py                      # AsyncBatchOperations
tests/unit/aio/
├── __init__.py
├── test_async_client.py
├── test_async_records.py
├── test_async_query.py
├── ...
```

~18 new source files, ~12 new test files.

---

## 11. Package Structure Decision: `.aio` Subpackage vs. Colocated Files

### The question

Two layouts were considered for placing async-specific source files:

**Option 1 — `.aio` subpackage (chosen)**

```
src/PowerPlatform/Dataverse/
├── aio/
│   ├── core/
│   │   ├── _async_http.py
│   │   └── _async_auth.py
│   ├── data/
│   │   └── _async_odata.py
│   └── operations/
│       └── records.py
├── core/
│   └── _http.py          # sync, unchanged
└── data/
    └── _odata.py         # sync, unchanged
```

**Option 2 — Colocated `_async_*` files alongside sync counterparts**

```
src/PowerPlatform/Dataverse/
├── core/
│   ├── _http.py
│   └── _async_http.py    # async twin lives next to sync
├── data/
│   ├── _odata.py
│   └── _async_odata.py
└── aio/
    └── __init__.py       # thin shell: imports + re-exports
```

### Decision: `.aio` subpackage (Option 1)

Three reasons drove this choice:

1. **Azure SDK convention** — The Azure SDK for Python guidelines and every major Azure client library (`azure-storage-blob`, `azure-keyvault-secrets`, `azure-identity`) use a self-contained `.aio` subpackage. Users familiar with the Azure ecosystem will find the pattern immediately recognizable. Colocated `_async_*` files have no established precedent in the ecosystem.

2. **Optional dependency semantics** — `aiohttp` must never be imported when a user only uses the sync client. With Option 1, the entire `aio/` tree is only imported on demand; `aiohttp` can be a hard import at the top of `aio/core/_async_http.py` with no risk of a `ModuleNotFoundError` for sync-only users. With Option 2, care would be needed to guard every `_async_*.py` import, because those files sit inside packages that the sync client already imports.

3. **Cognitive separation** — A single `aio/` boundary makes it visually unambiguous which modules are async. Reviewers and contributors know at a glance that anything outside `aio/` is sync-safe and `requests`-based; anything inside `aio/` is async and `aiohttp`-based. Mixed colocated files blur that boundary and invite accidental cross-contamination.
