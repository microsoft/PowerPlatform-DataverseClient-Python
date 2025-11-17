# PowerPlatform Dataverse Client for Python

[![PyPI version](https://badge.fury.io/py/PowerPlatform-Dataverse-Client.svg)](https://badge.fury.io/py/PowerPlatform-Dataverse-Client)
[![Python](https://img.shields.io/pypi/pyversions/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python client library for Microsoft Dataverse that provides a unified interface for CRUD operations, SQL queries, table metadata management, and file uploads through the Dataverse Web API.

**[Source code](https://github.com/microsoft/PowerPlatform-DataverseClient-Python)** | **[Package (PyPI)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)** | **[API reference documentation](https://learn.microsoft.com/python/api/dataverse-sdk-docs-python/dataverse-overview?view=dataverse-sdk-python-latest)** | **[Product documentation](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/)** | **[Samples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples)**

> [!IMPORTANT]
> This library is currently in **preview**. Preview versions are provided for early access to new features and may contain breaking changes.

## Table of contents

- [Key features](#key-features)
- [Getting started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Install the package](#install-the-package)  
  - [Authenticate the client](#authenticate-the-client)
- [Key concepts](#key-concepts)
- [Examples](#examples)
  - [Quick start](#quick-start)
  - [Basic CRUD operations](#basic-crud-operations)
  - [Bulk operations](#bulk-operations)
  - [Query data](#query-data)
  - [Table management](#table-management)
  - [File operations](#file-operations)
- [Next steps](#next-steps)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Key features

- **🔄 CRUD Operations**: Create, read, update, and delete records with support for bulk operations and automatic retry
- **⚡ True Bulk Operations**: Automatically uses Dataverse's native `CreateMultiple`, `UpdateMultiple`, and `BulkDelete` Web API operations for maximum performance and transactional integrity
- **📊 SQL Queries**: Execute read-only SQL queries via the Dataverse Web API `?sql=` parameter  
- **🏗️ Table Management**: Create, inspect, and delete custom tables and columns programmatically
- **📎 File Operations**: Upload files to Dataverse file columns with automatic chunking for large files
- **🔐 Azure Identity**: Built-in authentication using Azure Identity credential providers with comprehensive support
- **🛡️ Error Handling**: Structured exception hierarchy with detailed error context and retry guidance

## Getting started

### Prerequisites

- **Python 3.10+** (3.10, 3.11, 3.12, 3.13 supported)  
- **Microsoft Dataverse environment** with appropriate permissions
- **OAuth authentication configured** for your application

### Install the package

Install the PowerPlatform Dataverse Client using [pip](https://pypi.org/project/pip/):

```bash
# Install the latest stable release
pip install PowerPlatform-Dataverse-Client
```

For development from source:

```bash
git clone https://github.com/microsoft/PowerPlatform-DataverseClient-Python.git
cd PowerPlatform-DataverseClient-Python
pip install -e .
```

### Authenticate the client

The client requires any Azure Identity `TokenCredential` implementation for OAuth authentication with Dataverse:

```python
from azure.identity import (
    InteractiveBrowserCredential, 
    ClientSecretCredential,
    ClientCertificateCredential,
    AzureCliCredential
)
from PowerPlatform.Dataverse import DataverseClient

# Development options
credential = InteractiveBrowserCredential()  # Browser authentication
# credential = AzureCliCredential()          # If logged in via 'az login'

# Production options  
# credential = ClientSecretCredential(tenant_id, client_id, client_secret)
# credential = ClientCertificateCredential(tenant_id, client_id, cert_path)

client = DataverseClient("https://yourorg.crm.dynamics.com", credential)
```

> **Complete authentication setup**: See **[Use OAuth with Dataverse](https://learn.microsoft.com/power-apps/developer/data-platform/authenticate-oauth)** for app registration, all credential types, and security configuration.

## Key concepts

The SDK provides a simple, pythonic interface for Dataverse operations:

| Concept | Description |
|---------|-------------|
| **DataverseClient** | Main entry point for all operations with environment connection |
| **Records** | Dataverse records represented as Python dictionaries with column schema names |
| **Schema names** | Use table schema names (`"account"`, `"new_MyTestTable"`) and column schema names (`"name"`, `"new_MyTestColumn"`). See: [Table definitions in Microsoft Dataverse](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/entity-metadata) |  
| **Bulk Operations** | Efficient bulk processing for multiple records with automatic optimization |
| **Paging** | Automatic handling of large result sets with iterators |
| **Structured Errors** | Detailed exception hierarchy with retry guidance and diagnostic information |
| **Customization prefix values** | Custom tables and columns require a customization prefix value to be included for all operations (e.g., `"new_MyTestTable"`, not `"MyTestTable"`). See: [Table definitions in Microsoft Dataverse](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/entity-metadata) |

## Examples

### Quick start

```python
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse import DataverseClient

# Connect to Dataverse
credential = InteractiveBrowserCredential()
client = DataverseClient("https://yourorg.crm.dynamics.com", credential)

# Create a contact
contact_id = client.create("contact", {"firstname": "John", "lastname": "Doe"})[0]

# Read the contact back
contact = client.get("contact", contact_id, select=["firstname", "lastname"])
print(f"Created: {contact['firstname']} {contact['lastname']}")

# Clean up
client.delete("contact", contact_id)
```

### Basic CRUD operations

```python
# Create a record
account_ids = client.create("account", {"name": "Contoso Ltd"})
account_id = account_ids[0]

# Read a record
account = client.get("account", account_id)
print(account["name"])

# Update a record
client.update("account", account_id, {"telephone1": "555-0199"})

# Delete a record
client.delete("account", account_id)
```

### Bulk operations

```python
# Bulk create
payloads = [
    {"name": "Company A"},
    {"name": "Company B"},
    {"name": "Company C"}
]
ids = client.create("account", payloads)

# Bulk update (broadcast same change to all)
client.update("account", ids, {"industry": "Technology"})

# Bulk delete
client.delete("account", ids, use_bulk_delete=True)
```

### Query data

```python
# SQL query (read-only)
results = client.query_sql(
    "SELECT TOP 10 accountid, name FROM account WHERE statecode = 0"
)
for record in results:
    print(record["name"])

# OData query with paging
# Note: filter and expand parameters are case sensitive
pages = client.get(
    "account",
    select=["accountid", "name"],  # select is case-insensitive (automatically lowercased)
    filter="statecode eq 0",       # filter must use lowercase logical names (not transformed)
    top=100
)
for page in pages:
    for record in page:
        print(record["name"])

# Query with navigation property expansion (case-sensitive!)
pages = client.get(
    "account",
    select=["name"],
    expand=["primarycontactid"],  # Navigation property names are case-sensitive
    filter="statecode eq 0"       # Column names must be lowercase logical names
)
for page in pages:
    for account in page:
        contact = account.get("primarycontactid", {})
        print(f"{account['name']} - Contact: {contact.get('fullname', 'N/A')}")
```

> **Important**: When using `filter` and `expand` parameters:
> - **`filter`**: Column names must use exact lowercase logical names (e.g., `"statecode eq 0"`, not `"StateCode eq 0"`)
> - **`expand`**: Navigation property names are case-sensitive and must match the exact server names
> - **`select`** and **`orderby`**: Case-insensitive; automatically converted to lowercase

### Table management

```python
# Create a custom table, including the customization prefix value in the schema names for the table and columns.
table_info = client.create_table("new_Product", {
    "new_Code": "string",
    "new_Price": "decimal", 
    "new_Active": "bool"
})

# Create with custom primary column name and solution assignment
table_info = client.create_table(
    table_schema_name="new_Product",
    columns={
        "new_Code": "string",
        "new_Price": "decimal"
    },
    solution_unique_name="MyPublisher",  # Optional: add to specific solution
    primary_column_schema_name="new_ProductName"  # Optional: custom primary column (default is "{customization prefix value}_Name")
)

# Add columns to existing table (columns must include customization prefix value)
client.create_columns("new_Product", {"new_Category": "string"})

# Remove columns
client.delete_columns("new_Product", ["new_Category"])

# Clean up
client.delete_table("new_Product")
```

> **Important**: All custom column names must include the customization prefix value (e.g., `"new_"`). 
> This ensures explicit, predictable naming and aligns with Dataverse metadata requirements.

### File operations

```python
# Upload a file to a record
client.upload_file(
    table_schema_name="account",
    record_id=account_id,
    file_name_attribute="new_document",
    path="/path/to/document.pdf"
)
```

## Next steps

### More sample code

Explore our comprehensive examples in the [`examples/`](examples/) directory:

**🌱 Getting Started:**
- **[Installation & Setup](examples/basic/installation_example.py)** - Validate installation and basic usage patterns
- **[Functional Testing](examples/basic/functional_testing.py)** - Test core functionality in your environment

**🚀 Advanced Usage:**
- **[Complete Walkthrough](examples/advanced/walkthrough.py)** - Full feature demonstration with production patterns  
- **[File Upload](examples/advanced/file_upload.py)** - Upload files to Dataverse file columns

📖 See the [examples README](examples/README.md) for detailed guidance and learning progression.

### Additional documentation

For comprehensive information on Microsoft Dataverse and related technologies:

| Resource | Description |
|----------|-------------|
| **[Dataverse Developer Guide](https://learn.microsoft.com/power-apps/developer/data-platform/)** | Complete developer documentation for Microsoft Dataverse |
| **[Dataverse Web API Reference](https://learn.microsoft.com/power-apps/developer/data-platform/webapi/)** | Detailed Web API reference and examples |  
| **[Azure Identity for Python](https://learn.microsoft.com/python/api/overview/azure/identity-readme)** | Authentication library documentation and credential types |
| **[Power Platform Developer Center](https://learn.microsoft.com/power-platform/developer/)** | Broader Power Platform development resources |
| **[Dataverse SDK for .NET](https://learn.microsoft.com/power-apps/developer/data-platform/org-service/overview)** | Official .NET SDK for Microsoft Dataverse |

## Troubleshooting

### General

The client raises structured exceptions for different error scenarios:

```python
from PowerPlatform.Dataverse import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, ValidationError

try:
    client.get("account", "invalid-id")
except HttpError as e:
    print(f"HTTP {e.status_code}: {e.message}")
    print(f"Error code: {e.code}")
    print(f"Subcode: {e.subcode}")
    if e.is_transient:
        print("This error may be retryable")
except ValidationError as e:
    print(f"Validation error: {e.message}")
```

### Authentication issues

**Common fixes:** 
- Verify environment URL format: `https://yourorg.crm.dynamics.com` (no trailing slash)
- Ensure Azure Identity credentials have proper Dataverse permissions  
- Check app registration permissions are granted and admin-consented

### Performance considerations

For optimal performance in production environments:

| Best Practice | Description |
|---------------|-------------|
| **Bulk Operations** | Pass lists to `create()`, `update()`, and `delete()` for automatic bulk processing |
| **Select Fields** | Specify `select` parameter to limit returned columns and reduce payload size |
| **Page Size Control** | Use `top` and `page_size` parameters to control memory usage |
| **Connection Reuse** | Reuse `DataverseClient` instances across operations |
| **Production Credentials** | Use `ClientSecretCredential` or `ClientCertificateCredential` for unattended operations |
| **Error Handling** | Implement retry logic for transient errors (`e.is_transient`) |

### Limitations

- SQL queries are **read-only** and support a limited subset of SQL syntax
- Create Table supports a limited number of column types. Lookup columns are not yet supported.
- Creating relationships between tables is not yet supported.
- File uploads are limited by Dataverse file size restrictions (default 128MB per file)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
