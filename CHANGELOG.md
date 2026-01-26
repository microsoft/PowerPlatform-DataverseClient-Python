# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0b3]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases/tag/v0.1.0b1
