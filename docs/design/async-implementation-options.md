# Async SDK — Implementation Design Options

**Status:** Decision pending  
**Date:** April 2026

---

## Background

Adding async support touches every layer of the SDK. The HTTP transport, authentication, data layer, operations, and public client entry point all require async versions. The query builder needs only minimal changes (the fluent chain stays sync; only `execute()` becomes a coroutine). The models layer (`Record`, `TableInfo`, filters, etc.) requires **no changes** — pure dataclasses are shared between sync and async as-is. The existing sync client and all sync behaviour are **untouched**. See Part 3 for a full layer-by-layer breakdown.

Two decisions are open and require team input before implementation begins. Everything else — HTTP transport, auth, operations, query builder — follows directly from these two choices and has no meaningful alternatives.

1. **How should the async data layer relate to the sync data layer?** The data layer contains a mix of pure logic (payload building, parsing, validation) and I/O calls. The question is whether the async client *inherits* from the sync client and overrides I/O methods, or whether pure logic is *extracted into a shared base* that both sync and async inherit from as siblings. See Part 1.

2. **Where should async files live in the package tree?** Either all async files go under a dedicated `aio/` sub-package (the Azure SDK convention), or they are placed alongside their sync counterparts in the existing folders. See Part 2.

---

## Part 1 — Implementation Design

Two options were explored for how the async data layer relates to the sync data layer.

---

### Option A — Async inherits from Sync

The async client is a subclass of the sync client and overrides every I/O method with an `async def`.

```
_ODataClient
    └── _AsyncODataClient   (overrides all I/O methods)

_BatchClient
    └── _AsyncBatchClient   (overrides all I/O methods)
```

**Pros**

- Fewer files — no extra base classes needed.
- DRY without indirection — shared logic lives once in the sync class.
- Sync tests implicitly cover the shared methods.
- Simple inheritance chain, easy to read top-down.

**Cons**

- **LSP violation.** The Liskov Substitution Principle (LSP) states that a subclass must be usable wherever its parent class is expected, without breaking the program. Here, `_AsyncODataClient` is a subclass of `_ODataClient`, yet every I/O method changes from a regular function to a coroutine (`async def`). A caller written against `_ODataClient` expects `client.get(...)` to return a result directly; the async subclass returns a coroutine object instead, which is a different type entirely.
- Every overridden method requires `# type: ignore[override]` to silence the type checker (44 suppressions in this codebase).
- Conceptually misleading — "is a" implies substitutability; async-over-sync does not have it.
- Sync and async surfaces must evolve in lockstep or the inheritance chain silently inherits wrong sync behavior.

---

### Option B — Shared Pure Base + Sibling Sync/Async

Pure (I/O-free) methods are extracted into a base class. The sync and async clients both inherit from that base and are siblings.

```
_ODataBase  (pure methods: validation, payload building, parsing)
    ├── _ODataClient          (sync I/O)
    └── _AsyncODataClient     (async I/O)

_BatchBase  (pure methods: multipart serialisation, response parsing)
    ├── _BatchClient          (sync I/O)
    └── _AsyncBatchClient     (async I/O)
```

**Pros**

- Correct OOP — the relationship is "shares pure logic", not "is a". No LSP violation: because async and sync clients are siblings rather than parent/child, neither can be passed where the other is expected, so the type system enforces the boundary automatically.
- Zero `# type: ignore` suppressions — async methods are first-class definitions, not overrides.
- Type-safe out of the box; type checkers validate rather than suppress.
- Matches the Azure SDK pattern (azure-storage-blob, azure-data-tables, etc.).
- Base classes are independently testable.

**Cons**

- **Requires refactoring the existing sync layer.** Pure methods must be extracted out of `_ODataClient` and `_BatchClient` and moved into the new base classes. This is a non-trivial change to production code that was not touched by Option A, introducing risk and review surface for the sync path even though its behavior is unchanged.
- Two extra files (`_odata_base.py`, `_batch_base.py`).
- Slightly more indirection — reading the full API surface requires checking both the base and the subclass.
- The pure/IO boundary must be actively maintained as new methods are added.

---

## Part 2 — Folder Structure

Two options were explored for where async files live in the package tree.

---

### Option A — Dedicated `aio/` folder

All async code lives under a separate `aio/` sub-package, mirroring the sync layout.

```
src/PowerPlatform/Dataverse/
├── core/                   # sync
│   ├── _auth.py
│   └── _http.py
├── data/                   # sync data layer
│   ├── _odata.py
│   ├── _odata_base.py      # (if using Implementation Option B)
│   ├── _batch.py
│   └── _batch_base.py
├── operations/             # sync public operations (records, tables, …)
└── aio/                    # ALL async code
    ├── core/               # async counterparts to core/
    │   ├── _async_auth.py
    │   └── _async_http.py
    ├── data/               # async data layer
    │   ├── _async_odata.py
    │   └── _async_batch.py
    └── operations/         # async public operations
```

**Pros**

- `aio/` is the well-established Python convention (`aiohttp`, `azure-storage-blob`, `motor`, etc.).
- All async code is discoverable from a single entry point.
- Enforces a hard boundary — async and sync cannot accidentally be mixed at import time.
- Users who only want the sync client never need to open the `aio/` tree.

**Cons**

- Related sync/async files are in different directories — comparing or maintaining parity requires navigating across the tree.
- Slightly deeper nesting.

---

### Option B — Co-located in existing folders

Async files live alongside their sync counterparts, distinguished by a naming prefix/suffix.

```
src/PowerPlatform/Dataverse/
├── core/
│   ├── _auth.py
│   ├── _async_auth.py        # next to _auth.py
│   ├── _http.py
│   └── _async_http.py        # next to _http.py
├── data/
│   ├── _odata.py
│   ├── _async_odata.py       # next to _odata.py
│   ├── _odata_base.py
│   ├── _batch.py
│   ├── _async_batch.py
│   └── _batch_base.py
└── operations/
    ├── records.py
    ├── _async_records.py     # next to records.py
    └── …
```

**Pros**

- Sync and async counterparts sit side-by-side — parity gaps are immediately visible.
- Flatter structure, fewer directories.
- No sub-package boundary to cross when reading related files.

**Cons**

- Does not follow the `aio/` convention expected by most Python developers.
- Each folder mixes sync and async concerns; harder to see the async surface at a glance.
- No package-level boundary to prevent accidental cross-imports.

---

## Part 3 — Scope by Layer

The two design decisions above apply primarily to the data layer. The remaining layers of the SDK have different async implications and are summarised below to give a complete picture of the implementation scope.

| Layer | Files | Async approach |
|---|---|---|
| **HTTP client** | `core/_http.py` | Requires a new async HTTP library (`aiohttp`). Not a wrapper — a parallel implementation using a different transport. |
| **Auth** | `core/` credentials | Must implement `AsyncTokenCredential` from `azure-core`, a different Protocol from the sync `TokenCredential`. Not a simple `async def` wrapper. |
| **Data layer** | `data/_odata.py`, `_batch.py` | Subject of Parts 1 and 2 above. |
| **Operations** | `operations/records.py`, `tables.py`, `query.py`, `batch.py`, `dataframe.py`, `files.py` | Thin delegation wrappers. Each public method becomes `async def` + `await`. Straightforward but must be written per method. |
| **Query builder** | `operations/query.py` | Fluent chain with lazy execution. The chain itself is sync; only `execute()` and `to_dataframe()` are coroutines. |
| **Models** | `models/` | Pure dataclasses and typed dicts. Shared between sync and async unchanged. No async work required. |
| **Public client** | `DataverseClient` | A new `AsyncDataverseClient` entry point that wires the async operations together. Mirrors the sync client structurally. |

---

## Appendix — Full SDK Structure Under Each Folder Option

The trees below show the complete package layout. Async files are marked with `*`.  
The sync-only layers (`models/`, `common/`, `core/errors`, `utils/`) are identical in both options and included for completeness.

### Folder Option A — Dedicated `aio/` sub-package

```
src/PowerPlatform/Dataverse/
├── client.py                        # DataverseClient (sync entry point)
├── common/
│   └── constants.py
├── core/
│   ├── _auth.py
│   ├── _http.py
│   ├── _http_logger.py
│   ├── config.py
│   ├── errors.py
│   └── log_config.py
├── data/
│   ├── _odata.py
│   ├── _odata_base.py               # shared pure base (if Implementation Option B)
│   ├── _batch.py
│   ├── _batch_base.py               # shared pure base (if Implementation Option B)
│   ├── _relationships.py
│   ├── _upload.py
│   └── _raw_request.py
├── models/
│   ├── batch.py
│   ├── filters.py
│   ├── labels.py
│   ├── query_builder.py
│   ├── record.py
│   ├── relationship.py
│   ├── table_info.py
│   └── upsert.py
├── operations/
│   ├── batch.py
│   ├── dataframe.py
│   ├── files.py
│   ├── query.py
│   ├── records.py
│   └── tables.py
├── utils/
│   └── _pandas.py
└── aio/                             # * all async code lives here
    ├── async_client.py              # * AsyncDataverseClient (async entry point)
    ├── core/
    │   ├── _async_auth.py           # * AsyncTokenCredential impl
    │   └── _async_http.py           # * aiohttp-based HTTP client
    ├── data/
    │   ├── _async_odata.py          # * async OData client
    │   ├── _async_batch.py          # * async batch client
    │   ├── _async_relationships.py  # * async relationships mixin
    │   └── _async_upload.py         # * async file upload mixin
    └── operations/
        ├── async_batch.py           # * async batch operations
        ├── async_dataframe.py       # * async dataframe operations
        ├── async_files.py           # * async file operations
        ├── async_query.py           # * async query builder
        ├── async_records.py         # * async record operations
        └── async_tables.py          # * async table operations
```

---

### Folder Option B — Co-located async files

```
src/PowerPlatform/Dataverse/
├── client.py                        # DataverseClient (sync entry point)
├── async_client.py                  # * AsyncDataverseClient (async entry point)
├── common/
│   └── constants.py
├── core/
│   ├── _auth.py
│   ├── _async_auth.py               # * AsyncTokenCredential impl
│   ├── _http.py
│   ├── _async_http.py               # * aiohttp-based HTTP client
│   ├── _http_logger.py
│   ├── config.py
│   ├── errors.py
│   └── log_config.py
├── data/
│   ├── _odata.py
│   ├── _async_odata.py              # * async OData client
│   ├── _odata_base.py               # shared pure base (if Implementation Option B)
│   ├── _batch.py
│   ├── _async_batch.py              # * async batch client
│   ├── _batch_base.py               # shared pure base (if Implementation Option B)
│   ├── _relationships.py
│   ├── _async_relationships.py      # * async relationships mixin
│   ├── _upload.py
│   ├── _async_upload.py             # * async file upload mixin
│   └── _raw_request.py
├── models/
│   ├── batch.py
│   ├── filters.py
│   ├── labels.py
│   ├── query_builder.py
│   ├── record.py
│   ├── relationship.py
│   ├── table_info.py
│   └── upsert.py
├── operations/
│   ├── batch.py
│   ├── async_batch.py               # * async batch operations
│   ├── dataframe.py
│   ├── async_dataframe.py           # * async dataframe operations
│   ├── files.py
│   ├── async_files.py               # * async file operations
│   ├── query.py
│   ├── async_query.py               # * async query builder
│   ├── records.py
│   ├── async_records.py             # * async record operations
│   ├── tables.py
│   └── async_tables.py              # * async table operations
└── utils/
    └── _pandas.py
```
