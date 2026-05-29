# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-05-28

This is the first stable release. It establishes the v1 public API and removes all v0 beta methods.

### Breaking Changes

- All flat methods on `DataverseClient` (`create`, `update`, `delete`, `get`, `list`, `query_sql`, `upload_file`, etc.) are removed; use the namespaced operations: `client.records`, `client.query`, `client.tables`, `client.files`, `client.batch` (#175)
- `client.query.sql_select()`, `client.query.sql_joins()`, `client.query.sql_join()` are removed (#175)
- `QueryBuilder.execute()` now returns a flat `QueryResult` instead of `Iterable[Record]`; use `execute_pages()` for lazy iteration (#175)

  Run `dataverse-migrate --dry-run .` to automatically rewrite v0 call sites (`pip install PowerPlatform-Dataverse-Client[migration]`).

### Added

- **Async client** — `AsyncDataverseClient` with full feature parity to the sync SDK; all operation namespaces available with `async with` lifecycle management. Install with `pip install PowerPlatform-Dataverse-Client[async]` (#171)
- **Single-record fetch** — `client.records.retrieve(table, record_id, *, select, expand, include_annotations)` returns `None` on 404 instead of raising; `expand` adds `$expand` for navigation-property expansion on the single-record GET; `include_annotations` maps to the `Prefer: odata.include-annotations` header for formatted values and lookup labels (#175)
- **Multi-record fetch** — `client.records.list(table, *, filter, select, top, orderby, expand, page_size, count, include_annotations)` returns an eager flat `QueryResult`; GA replacement for `records.get()` without a record ID; `page_size` controls `Prefer: odata.maxpagesize`, `count=True` adds `$count=true`, `include_annotations` requests formatted values (#175)
- **Streaming multi-record fetch** — `client.records.list_pages(...)` yields one `QueryResult` per HTTP page; streaming counterpart to `list()` with the same parameter set (#175)
- **FetchXML** — `client.query.fetchxml(xml)` returns an inert `FetchXmlQuery`; no HTTP call until `.execute()` or `.execute_pages()`. Paging implements the documented Dataverse algorithm: annotation parsed as outer XML, `pagingcookie` attribute double URL-decoded, server-supplied `pagenumber` used for the next page, `morerecords` handled as both `bool` and `"true"` string, `UserWarning` emitted on simple-paging fallback, 32,768-character URL limit enforced (documented Dataverse GET cap), 10,000-page circuit breaker against runaway iteration (#175)
- **Streaming QueryBuilder** — `QueryBuilder.execute_pages()` lazily yields one `QueryResult` per HTTP page; replaces deprecated `execute(by_page=True)` (#175)
- **Composable filters** — `QueryBuilder.where(col("name") == "Contoso")` with Python operators (`==`, `>`, `&`, `|`, `~`); replaces the deprecated `filter_eq()`, `filter_contains()`, and other `filter_*` helpers (#175)
- **`QueryResult` indexing** — `result[0]` returns a `Record`; `result[1:5]` returns a new `QueryResult` (#175)
- **`DataverseModel` protocol** — structural `Protocol` in `models/protocol.py` for typed entity classes; enables CRUD operations without manual table names or dict serialization (#175)
- **Top-level re-exports** — `col()`, `raw()`, `QueryResult`, and `DataverseModel` importable directly from the top-level `PowerPlatform.Dataverse` package (#175)
- **Shorter imports** — public types (`Record`, `DataverseError`, `QueryBuilder`, `BatchResult`, and others) now importable directly from `PowerPlatform.Dataverse.models`, `.core`, and `.operations` (#165)
- **Migration tool** — installed as the `dataverse-migrate` console script (also runnable via `python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1`); rewrites v0 call sites to the v1 API with `--dry-run` support; covers `create`, `update`, `delete`, `get`, `list`, `fetchxml`, and query-builder patterns; emits a `[NEEDS-MANUAL]` label for files with no auto-rewrites but manual attention needed, and appends a trailing note on `[MIGRATED]` lines when manual items remain. Requires the `[migration]` optional extra (`pip install PowerPlatform-Dataverse-Client[migration]`) (#175)

### Changed

- `records.get()` deprecation extended: calling with a `record_id` emits `DeprecationWarning` directing callers to `retrieve()`; calling without a `record_id` directs callers to `list()` (#175)
- `OperationContext` now validates keys and values against an allowlist; unknown keys or non-conforming values raise `ValidationError`, preventing PII from reaching the `User-Agent` header (#181)
- Table creation now uses the `CreateEntities` API, improving performance (#183)
- `float`/`double` column precision default raised from 2 to 5 decimal places, preventing silent truncation of values like `2.718` (#185)
- Server inner error messages now surface in `HttpError.message` on both single-request and batch paths (#185)
- `pandas.DataFrame` with MultiIndex columns now raises a descriptive error with a flatten hint instead of failing deep in serialization (#185)

### Fixed

- Client creation no longer errors on Python 3.10 and 3.11 (#188)
- SQL guardrails now catch write statements hidden inside comments, string literals, or zero-width prefixes (#185)

### Deprecated

- `QueryBuilder.execute(by_page=True)` and `execute(by_page=False)` emit `UserWarning`; use `execute_pages()` and `execute()` respectively (#175)
- `client.query.odata_select()`, `client.query.odata_expands()`, `client.query.odata_expand()`, `client.query.odata_bind()` emit `DeprecationWarning`; navigation-property helpers are replaced by `QueryBuilder.expand()` (#175)

## [0.1.0b10] - 2026-05-12

### Added
- `DataverseClient(context=OperationContext(...))` keyword argument (also settable via `DataverseConfig(operation_context=...)`) that appends a parenthesized comment to the outbound `User-Agent` header so callers can attribute their traffic in Dataverse server logs — e.g. `DataverseSvcPythonClient:0.1.0b10 (app=dataverse-skills/1.2.1)` (#178)

## [0.1.0b9] - 2026-04-28

### Added
- `client.dataframe.sql()`: execute a SQL SELECT and get results directly as a pandas DataFrame (#141)
- Schema discovery: `client.query.list_columns()`, `client.query.list_relationships()`, and `client.query.list_table_relationships()` to inspect table columns and relationships from metadata (#141)
- SQL query helpers: `client.query.sql_columns()`, `client.query.sql_select()`, `client.query.sql_joins()`, and `client.query.sql_join()` to auto-build SQL statements from metadata without knowing column or join syntax manually (#141)
- OData query helpers: `client.query.odata_select()`, `client.query.odata_expands()`, `client.query.odata_expand()`, and `client.query.odata_bind()` to auto-discover navigation property names and build `@odata.bind` values from metadata (#141)
- `SELECT *` raises a `ValidationError` with a clear message instead of sending the query to the server, which does not support wildcard selects; use `client.query.sql_columns()` to discover available columns (#141)
- SQL safety guardrails: write statements (`INSERT`, `UPDATE`, `DELETE`, etc.) raise `ValidationError` before hitting the server; cartesian joins (`FROM a, b`) and leading-wildcard `LIKE` patterns emit `UserWarning` (#141)
- `client.tables.create()` now accepts an optional `display_name` parameter to set a human-readable label for the table in Dataverse; defaults to the schema name when omitted (#164)
- Opt-in HTTP diagnostics logging: pass a `LogConfig` to `DataverseConfig` to log all HTTP request and response traffic to rotating local log files with automatic redaction of sensitive headers (`Authorization`, etc.) (#135)

### Changed
- `client.tables.create_lookup_field()` now automatically lowercases `referencing_table` and `referenced_table` to valid Dataverse logical names; callers no longer need to call `.lower()` manually (#141)

## [0.1.0b8] - 2026-04-10

### Added
- Batch API: `client.batch` namespace for deferred-execution batch operations that pack multiple Dataverse Web API calls into a single `POST $batch` HTTP request (#129)
- Batch DataFrame integration: `client.batch.dataframe` namespace with pandas DataFrame wrappers for batch operations (#129)
- `client.records.upsert()` and `client.batch.records.upsert()` backed by the `UpsertMultiple` bound action with alternate-key support (#129)
- QueryBuilder: `client.query.builder("table")` with a fluent API, 20+ chainable methods (`select`, `filter_eq`, `filter_contains`, `order_by`, `expand`, etc.), and composable filter expressions using Python operators (`&`, `|`, `~`) (#118)
- Memo/multiline column type support: `"memo"` (or `"multiline"`) can now be passed as a column type in `client.tables.create()` and `client.tables.add_columns()` (#155)

### Changed
- Picklist label-to-integer resolution now uses a single bulk `PicklistAttributeMetadata` API call for the entire table instead of per-attribute requests, with a 1-hour TTL cache (#154)

### Fixed
- `client.query.sql()` silently truncated results at 5,000 rows. The method now follows `@odata.nextLink` pagination and returns all matching rows (#157).
- Alternate key fields were incorrectly merged into the `UpsertMultiple` request body, causing `400 Bad Request` on the create path (#129)
- Docstring type annotations corrected for Microsoft Learn API reference compatibility (#153)

## [0.1.0b7] - 2026-03-17

### Added
- DataFrame namespace: `client.dataframe.get()`, `.create()`, `.update()`, `.delete()` for working with Dataverse records as pandas DataFrames and Series — no manual dict conversion required (#98)
- Table metadata now includes `primary_name_attribute` and `primary_id_attribute` from `tables.create()` and `tables.get_info()` (#148)

### Changed
- `pandas>=2.0.0` is now a required dependency (#98)

## [0.1.0b6] - 2026-03-12

### Added
- Context manager support: `with DataverseClient(...) as client:` for automatic resource cleanup, HTTP connection pooling, and `close()` for explicit lifecycle management (#117)
- Typed return models `Record`, `TableInfo`, and `ColumnInfo` for record and table metadata operations, replacing raw `Dict[str, Any]` returns with full backward compatibility (`result["key"]` still works) (#115)
- Alternate key management: `client.tables.create_alternate_key()`, `client.tables.get_alternate_keys()`, `client.tables.delete_alternate_key()` with typed `AlternateKeyInfo` model (#126)

### Fixed
- `@odata.bind` lookup bindings now preserve navigation property casing (e.g., `new_CustomerId@odata.bind`), fixing `400 Bad Request` errors on create/update/upsert with lookup fields (#137)
- Reduced unnecessary HTTP round-trips on create/update/upsert when records contain `@odata.bind` keys (#137)
- Single-record `get()` now lowercases `$select` column names consistently with multi-record queries (#137)

## [0.1.0b5] - 2026-02-27

### Fixed
- UpsertMultiple: exclude alternate key fields from request body (#127). The create path of UpsertMultiple failed with `400 Bad Request` when alternate key column values appeared in both the body and `@odata.id`.

## [0.1.0b4] - 2026-02-25

### Added
- Operation namespaces: `client.records`, `client.query`, `client.tables`, `client.files` (#102)
- Relationship management: `create_one_to_many_relationship`, `create_many_to_many_relationship`, `get_relationship`, `delete_relationship`, `create_lookup_field` with typed `RelationshipInfo` return model (#105, #114)
- `client.records.upsert()` for upsert operations with alternate key support (#106)
- `client.files.upload()` for file upload operations (#111)
- `client.tables.list(filter=, select=)` parameters for filtering and projecting table metadata (#112)
- Cascade behavior constants (`CASCADE_BEHAVIOR_CASCADE`, `CASCADE_BEHAVIOR_REMOVE_LINK`, etc.) and input models (`CascadeConfiguration`, `LookupAttributeMetadata`, `Label`, `LocalizedLabel`)

### Deprecated
- All flat methods on `DataverseClient` (`create`, `update`, `delete`, `get`, `query_sql`, `upload_file`, etc.) now emit `DeprecationWarning` and delegate to the corresponding namespaced operations

## [0.1.0b3] - 2025-12-19

### Added
- Client-side correlation ID and client request ID for request tracing (#70)
- Unit tests for `DataverseClient` (#71)

### Changed
- Standardized package versioning (#84)
- Updated package link (#69)

### Fixed
- Retry logic for examples (#72)
- Removed double space formatting issue (#82)
- Updated CI trigger to include main branch (#81)

## [0.1.0b2] - 2025-11-17

### Added
- Enforce Black formatting across the codebase (#61, #62)
- Python 3.14 support added to `pyproject.toml` (#55)

### Changed
- Removed `pandas` dependency (#57)
- Refactored SDK architecture and quality improvements (#55)
- Prefixed table names with schema name for consistency (#51)
- Updated docstrings across core modules (#54, #63)

### Fixed
- Fixed `get` for single-select option set columns (#52)
- Fixed example filename references and documentation URLs (#60)
- Fixed API documentation link in examples (#64)
- Fixed CI pipeline to use modern `pyproject.toml` dev dependencies (#56, #59)

## [0.1.0b1] - 2025-11-14

### Added
- Initial beta release of Microsoft Dataverse SDK for Python
- Core `DataverseClient` with Azure Identity authentication support (Service Principal, Managed Identity, Interactive Browser) (#1)
- Complete CRUD operations (create, read, update, delete) for Dataverse records (#1)
- Advanced OData query support with filtering, sorting, and expansion
- SQL query execution via `query_sql()` method with result pagination (#14)
- Bulk operations including `CreateMultiple`, `UpdateMultiple`, and `BulkDelete` (#6, #8, #34)
- File upload capabilities for file and image columns (#17)
- Table metadata operations (create, inspect, delete custom tables)
- Comprehensive error handling with specific exception types (`DataverseError`, `AuthenticationError`, etc.) (#22, #24)
- HTTP retry logic with exponential backoff for resilient operations (#72)

[Unreleased]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b10...v1.0.0
[0.1.0b10]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b9...v0.1.0b10
[0.1.0b9]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b8...v0.1.0b9
[0.1.0b8]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b7...v0.1.0b8
[0.1.0b7]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b6...v0.1.0b7
[0.1.0b6]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b5...v0.1.0b6
[0.1.0b5]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b4...v0.1.0b5
[0.1.0b4]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b3...v0.1.0b4
[0.1.0b3]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases/tag/v0.1.0b1
