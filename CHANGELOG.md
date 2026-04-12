# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b8...HEAD
[0.1.0b8]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b7...v0.1.0b8
[0.1.0b7]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b6...v0.1.0b7
[0.1.0b6]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b5...v0.1.0b6
[0.1.0b5]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b4...v0.1.0b5
[0.1.0b4]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b3...v0.1.0b4
[0.1.0b3]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases/tag/v0.1.0b1
