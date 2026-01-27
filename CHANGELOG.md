# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- **Fluent QueryBuilder**: Type-safe, discoverable query construction via `client.query.builder(table)`:
  - Selection: `select(*columns)` for specifying columns to retrieve
  - Comparison filters: `filter_eq`, `filter_ne`, `filter_gt`, `filter_ge`, `filter_lt`, `filter_le`
  - String filters: `filter_contains`, `filter_startswith`, `filter_endswith`
  - Null filters: `filter_null`, `filter_not_null`
  - Raw filter: `filter_raw(expression)` for complex OData expressions
  - Sorting: `order_by(column, descending=False)` with multi-column support
  - Limiting: `top(count)` for result limits
  - Navigation: `expand(*relations)` for navigation property expansion
- **QueryOperations methods**:
  - `client.query.builder(table)` - Factory method for QueryBuilder
  - `client.query.execute(query)` - Execute QueryBuilder, returns batches with telemetry
  - `client.query.iterate(table, ...)` - Convenience method yielding individual records
  - `client.query.iterate_query(query)` - Iterate records from QueryBuilder one at a time

## [0.1.0b1] - 2025-11-14

### Added
**Initial beta release** of Microsoft Dataverse SDK for Python

**Core Client & Authentication:**
- Core `DataverseClient` with Azure Identity authentication support
- Secure authentication using Azure Identity credentials (Service Principal, Managed Identity, Interactive Browser)
- TLS 1.2+ encryption for all API communications
- Proper credential handling without exposing secrets in logs

**Data Operations:**
- Complete CRUD operations (create, read, update, delete) for Dataverse records
- Advanced OData query support with filtering, sorting, and expansion
- SQL query execution via `query_sql()` method with result pagination
- Support for batch operations and transaction handling
- File upload capabilities for file and image columns

**Table Management:**
- Table metadata operations (create, inspect, delete custom tables)

**Reliability & Error Handling:**
- Comprehensive error handling with specific exception types (`DataverseError`, `AuthenticationError`, etc.)
- HTTP retry logic with exponential backoff for resilient operations

**Developer Experience:**
- Example scripts demonstrating common integration patterns
- Complete documentation with quickstart guides and API reference
- Modern Python packaging using `pyproject.toml` configuration

**Quality Assurance:**
- Comprehensive test suite with unit and integration tests
- GitHub Actions CI/CD pipeline for automated testing and validation
- Azure DevOps PR validation pipeline

### Changed
- N/A

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A
