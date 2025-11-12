# PowerPlatform Dataverse Client for Python

[![PyPI version](https://badge.fury.io/py/PowerPlatform-Dataverse-Client.svg)](https://badge.fury.io/py/PowerPlatform-Dataverse-Client)
[![Python](https://img.shields.io/pypi/pyversions/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python client library for Microsoft Dataverse that provides a unified interface for CRUD operations, SQL queries, table metadata management, and file uploads through the Dataverse Web API.

**[Source code](https://github.com/microsoft/PowerPlatform-DataverseClient-Python)** | **[Package (PyPI)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)** | **[API reference documentation](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples)** | **[Product documentation](https://learn.microsoft.com/power-apps/developer/data-platform/)** | **[Samples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples)**

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
- **📊 SQL Queries**: Execute read-only SQL queries via the Dataverse Web API `?sql=` parameter  
- **🏗️ Table Management**: Create, inspect, and delete custom tables and columns programmatically
- **📎 File Operations**: Upload files to Dataverse file columns with automatic chunking for large files
- **🔐 Azure Identity**: Built-in authentication using Azure Identity credential providers with comprehensive support
- **🛡️ Error Handling**: Structured exception hierarchy with detailed error context and retry guidance
- **🐼 Pandas Integration**: Preliminary DataFrame-oriented operations for data analysis workflows

## Getting started

### Prerequisites

- **Python 3.10+** (3.10, 3.11, 3.12, 3.13 supported)
- **Azure Identity credentials** - Configure authentication using one of:
  - Interactive browser authentication (development)
  - Service principal with client secret (production) 
  - Managed identity (Azure-hosted applications)
  - Device code flow (headless development environments)
- **Microsoft Dataverse environment** - Access to a Power Platform environment with appropriate permissions

### Install the package

Install the PowerPlatform Dataverse Client using [pip](https://pypi.org/project/pip/):

```bash
# Install the latest stable release
pip install PowerPlatform-Dataverse-Client

# Include Azure Identity for authentication
pip install PowerPlatform-Dataverse-Client azure-identity

# (pandas is included by default)
```

For development from source:

```bash
git clone https://github.com/microsoft/PowerPlatform-DataverseClient-Python.git
cd PowerPlatform-DataverseClient-Python
pip install -e .
```

### Authenticate the client

The client requires Azure Identity credentials. You can use various credential types:

```python
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse import DataverseClient

# For development - interactive browser authentication
credential = InteractiveBrowserCredential()

# For production - service principal authentication  
# credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# For headless environments
# credential = DeviceCodeCredential()

client = DataverseClient(
    base_url="https://yourorg.crm.dynamics.com",
    credential=credential
)
```

## Key concepts

The SDK provides a simple, pythonic interface for Dataverse operations:

| Concept | Description |
|---------|-------------|
| **DataverseClient** | Main entry point for all operations with environment connection |
| **Records** | Dataverse records represented as Python dictionaries with logical field names |
| **Logical Names** | Use table logical names (`"account"`) and column logical names (`"name"`) |  
| **Bulk Operations** | Efficient batch processing for multiple records with automatic optimization |
| **Paging** | Automatic handling of large result sets with iterators |
| **Structured Errors** | Detailed exception hierarchy with retry guidance and diagnostic information |

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
pages = client.get(
    "account",
    select=["accountid", "name"],
    filter="statecode eq 0",
    top=100
)
for page in pages:
    for record in page:
        print(record["name"])
```

### Table management

```python
# Create a custom table
table_info = client.create_table("Product", {
    "code": "string",
    "price": "decimal", 
    "active": "bool"
})

# Add columns to existing table
client.create_columns("Product", {"category": "string"})

# Clean up
client.delete_table("Product")
```

### File operations

```python
# Upload a file to a record
client.upload_file(
    logical_name="account",
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
- **[Complete Walkthrough](examples/advanced/complete_walkthrough.py)** - Full feature demonstration with production patterns  
- **[File Upload](examples/advanced/file_upload.py)** - Upload files to Dataverse file columns
- **[Pandas Integration](examples/advanced/pandas_integration.py)** - DataFrame-based operations for data analysis

📖 See the [examples README](examples/README.md) for detailed guidance and learning progression.

### Additional documentation

For comprehensive information on Microsoft Dataverse and related technologies:

| Resource | Description |
|----------|-------------|
| **[Dataverse Developer Guide](https://learn.microsoft.com/power-apps/developer/data-platform/)** | Complete developer documentation for Microsoft Dataverse |
| **[Dataverse Web API Reference](https://learn.microsoft.com/power-apps/developer/data-platform/webapi/)** | Detailed Web API reference and examples |  
| **[Azure Identity for Python](https://learn.microsoft.com/python/api/overview/azure/identity-readme)** | Authentication library documentation and credential types |
| **[Power Platform Developer Center](https://learn.microsoft.com/power-platform/developer/)** | Broader Power Platform development resources |
| **[Dataverse SDK for .NET](https://learn.microsoft.com/power-apps/developer/data-platform/dataverse-sdk-dotnet/)** | Official .NET SDK for Microsoft Dataverse |

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

- Ensure your credential has proper permissions to access Dataverse
- Verify the `base_url` format: `https://yourorg.crm.dynamics.com`
- Check that required Azure Identity environment variables are set

### Performance considerations

For optimal performance in production environments:

| Best Practice | Description |
|---------------|-------------|
| **Bulk Operations** | Use bulk methods for multiple records instead of individual operations |
| **Select Fields** | Specify `select` parameter to limit returned columns and reduce payload size |
| **Page Size Control** | Use `top` and `page_size` parameters to control memory usage |
| **Connection Reuse** | Reuse `DataverseClient` instances across operations |
| **Error Handling** | Implement retry logic for transient errors (`e.is_transient`) |

### Limitations

- SQL queries are **read-only** and support a limited subset of SQL syntax
- File uploads are limited by Dataverse file size restrictions (default 128MB per file)
- Custom table creation requires appropriate security privileges in the target environment
- Rate limits apply based on your Power Platform license and environment configuration

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
