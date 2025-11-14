# Changelog

All notable changes to this project will be documented in this file.

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

**Integration & Analysis:**
- Pandas DataFrame integration for seamless data analysis workflows

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
