# PowerPlatform Dataverse Client for Python

[![PyPI version](https://img.shields.io/pypi/v/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
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
  - [Relationship management](#relationship-management)
  - [File operations](#file-operations)
- [Next steps](#next-steps)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Key features

- **🔄 CRUD Operations**: Create, read, update, and delete records with support for bulk operations and automatic retry
- **⚡ True Bulk Operations**: Automatically uses Dataverse's native `CreateMultiple`, `UpdateMultiple`, and `BulkDelete` Web API operations for maximum performance and transactional integrity
- **📊 SQL Queries**: Execute read-only SQL queries via the Dataverse Web API `?sql=` parameter  
- **🏗️ Table Management**: Create, inspect, and delete custom tables and columns programmatically
- **🔗 Relationship Management**: Create one-to-many and many-to-many relationships between tables with full metadata control
- **📎 File Operations**: Upload files to Dataverse file columns with automatic chunking for large files
- **🔐 Azure Identity**: Built-in authentication using Azure Identity credential providers with comprehensive support
- **🛡️ Error Handling**: Structured exception hierarchy with detailed error context and retry guidance

## Getting started

### Prerequisites

- **Python 3.10+** (3.10, 3.11, 3.12, 3.13 supported)  test
- **Microsoft Dataverse environment** with appropriate permissions
- **OAuth authentication configured** for your application

### Install the package

Install the PowerPlatform Dataverse Client using [pip](https://pypi.org/project/pip/):

```bash
# Install the latest stable release
pip install PowerPlatform-Dataverse-Client
```

(Optional) Install Claude Skill globally with the Client:

```bash
pip install PowerPlatform-Dataverse-Client && dataverse-install-claude-skill
```

This installs two Claude Skills that enable Claude Code to:
- **dataverse-sdk-use**: Apply SDK best practices for using the SDK in your applications
- **dataverse-sdk-dev**: Provide guidance for developing/contributing to the SDK itself

The skills work with both the Claude Code CLI and VSCode extension. Once installed, Claude will automatically use the appropriate skill when working with Dataverse operations. For more information on Claude Skill see https://platform.claude.com/docs/en/agents-and-tools/agent-skills/overview. See skill definitions here: [.claude/skills/dataverse-sdk-use/SKILL.md](.claude/skills/dataverse-sdk-use/SKILL.md) and [.claude/skills/dataverse-sdk-dev/SKILL.md](.claude/skills/dataverse-sdk-dev/SKILL.md).

For development from source (Claude Skill auto loaded):

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
    CertificateCredential,
    AzureCliCredential
)
from PowerPlatform.Dataverse.client import DataverseClient

# Development options
credential = InteractiveBrowserCredential()  # Browser authentication
# credential = AzureCliCredential()          # If logged in via 'az login'

# Production options  
# credential = ClientSecretCredential(tenant_id, client_id, client_secret)
# credential = CertificateCredential(tenant_id, client_id, cert_path)

client = DataverseClient("https://yourorg.crm.dynamics.com", credential)
```

> **Complete authentication setup**: See **[Use OAuth with Dataverse](https://learn.microsoft.com/power-apps/developer/data-platform/authenticate-oauth)** for app registration, all credential types, and security configuration.

## Key concepts

The SDK provides a simple, pythonic interface for Dataverse operations:

| Concept | Description |
|---------|-------------|
| **DataverseClient** | Main entry point; provides `records`, `query`, and `tables` namespaces |
| **Namespaces** | Operations are organized into `client.records` (CRUD), `client.query` (queries), and `client.tables` (metadata) |
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
from PowerPlatform.Dataverse.client import DataverseClient

# Connect to Dataverse
credential = InteractiveBrowserCredential()
client = DataverseClient("https://yourorg.crm.dynamics.com", credential)

# Create a contact
contact_id = client.records.create("contact", {"firstname": "John", "lastname": "Doe"})

# Read the contact back
contact = client.records.get("contact", contact_id, select=["firstname", "lastname"])
print(f"Created: {contact['firstname']} {contact['lastname']}")

# Clean up
client.records.delete("contact", contact_id)
```

### Basic CRUD operations

```python
# Create a record
account_id = client.records.create("account", {"name": "Contoso Ltd"})

# Read a record
account = client.records.get("account", account_id)
print(account["name"])

# Update a record
client.records.update("account", account_id, {"telephone1": "555-0199"})

# Delete a record
client.records.delete("account", account_id)
```

### Bulk operations

```python
# Bulk create
payloads = [
    {"name": "Company A"},
    {"name": "Company B"},
    {"name": "Company C"}
]
ids = client.records.create("account", payloads)

# Bulk update (broadcast same change to all)
client.records.update("account", ids, {"industry": "Technology"})

# Bulk delete
client.records.delete("account", ids, use_bulk_delete=True)
```

### Query data

```python
# SQL query (read-only)
results = client.query.sql(
    "SELECT TOP 10 accountid, name FROM account WHERE statecode = 0"
)
for record in results:
    print(record["name"])

# OData query with paging
# Note: filter and expand parameters are case sensitive
for page in client.query.get(
    "account",
    select=["accountid", "name"],  # select is case-insensitive (automatically lowercased)
    filter="statecode eq 0",       # filter must use lowercase logical names (not transformed)
    top=100,
):
    for record in page:
        print(record["name"])

# Query with navigation property expansion (case-sensitive!)
for page in client.query.get(
    "account",
    select=["name"],
    expand=["primarycontactid"],  # Navigation property names are case-sensitive
    filter="statecode eq 0",      # Column names must be lowercase logical names
):
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
table_info = client.tables.create("new_Product", {
    "new_Code": "string",
    "new_Price": "decimal",
    "new_Active": "bool"
})

# Create with custom primary column name and solution assignment
table_info = client.tables.create(
    "new_Product",
    columns={
        "new_Code": "string",
        "new_Price": "decimal"
    },
    solution="MyPublisher",  # Optional: add to specific solution
    primary_column="new_ProductName",  # Optional: custom primary column (default is "{customization prefix value}_Name")
)

# Get table information
info = client.tables.get("new_Product")
print(f"Logical name: {info['table_logical_name']}")
print(f"Entity set: {info['entity_set_name']}")

# List all tables
tables = client.tables.list()
for table in tables:
    print(table)

# Add columns to existing table (columns must include customization prefix value)
client.tables.add_columns("new_Product", {"new_Category": "string"})

# Remove columns
client.tables.remove_columns("new_Product", ["new_Category"])

# Clean up
client.tables.delete("new_Product")
```

> **Important**: All custom column names must include the customization prefix value (e.g., `"new_"`).
> This ensures explicit, predictable naming and aligns with Dataverse metadata requirements.

### Relationship management

Create relationships between tables using the relationship API. For a complete working example, see [examples/advanced/relationships.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/relationships.py).

```python
from PowerPlatform.Dataverse.models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
)

# Create a one-to-many relationship: Department (1) -> Employee (N)
# This adds a "Department" lookup field to the Employee table
lookup = LookupAttributeMetadata(
    schema_name="new_DepartmentId",
    display_name=Label(localized_labels=[LocalizedLabel(label="Department", language_code=1033)]),
)

relationship = OneToManyRelationshipMetadata(
    schema_name="new_Department_Employee",
    referenced_entity="new_department",   # Parent table (the "one" side)
    referencing_entity="new_employee",    # Child table (the "many" side)
    referenced_attribute="new_departmentid",
)

result = client.create_one_to_many_relationship(lookup, relationship)
print(f"Created lookup field: {result['lookup_schema_name']}")

# Create a many-to-many relationship: Employee (N) <-> Project (N)
# Employees work on multiple projects; projects have multiple team members
m2m_relationship = ManyToManyRelationshipMetadata(
    schema_name="new_employee_project",
    entity1_logical_name="new_employee",
    entity2_logical_name="new_project",
)

result = client.create_many_to_many_relationship(m2m_relationship)
print(f"Created M:N relationship: {result['relationship_schema_name']}")

# Query relationship metadata
rel = client.get_relationship("new_Department_Employee")
if rel:
    print(f"Found: {rel['SchemaName']}")

# Delete a relationship
client.delete_relationship(result['relationship_id'])
```

For simpler scenarios, use the convenience method:

```python
# Quick way to create a lookup field with sensible defaults
result = client.create_lookup_field(
    referencing_table="contact",       # Child table gets the lookup field
    lookup_field_name="new_AccountId",
    referenced_table="account",        # Parent table being referenced
    display_name="Account",
)
```

### File operations

```python
# Upload a file to a record
client.upload_file(
    table_schema_name="account",
    record_id=account_id,
    file_name_attribute="new_Document",  # If the file column doesn't exist, it will be created automatically
    path="/path/to/document.pdf"
)
```

## Next steps

### More sample code

Explore our comprehensive examples in the [`examples/`](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples) directory:

**🌱 Getting Started:**
- **[Installation & Setup](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/basic/installation_example.py)** - Validate installation and basic usage patterns
- **[Functional Testing](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/basic/functional_testing.py)** - Test core functionality in your environment

**🚀 Advanced Usage:**
- **[Complete Walkthrough](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/walkthrough.py)** - Full feature demonstration with production patterns
- **[Relationship Management](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/relationships.py)** - Create and manage table relationships
- **[File Upload](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/file_upload.py)** - Upload files to Dataverse file columns

📖 See the [examples README](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/README.md) for detailed guidance and learning progression.

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
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, ValidationError

try:
    client.records.get("account", "invalid-id")
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
| **Bulk Operations** | Pass lists to `records.create()`, `records.update()` for automatic bulk processing, for `records.delete()`, set `use_bulk_delete` when passing lists to use bulk operation |
| **Select Fields** | Specify `select` parameter to limit returned columns and reduce payload size |
| **Page Size Control** | Use `top` and `page_size` parameters to control memory usage |
| **Connection Reuse** | Reuse `DataverseClient` instances across operations |
| **Production Credentials** | Use `ClientSecretCredential` or `CertificateCredential` for unattended operations |
| **Error Handling** | Implement retry logic for transient errors (`e.is_transient`) |

### Limitations

- SQL queries are **read-only** and support a limited subset of SQL syntax
- Create Table supports a limited number of column types (string, int, decimal, bool, datetime, picklist)
- File uploads are limited by Dataverse file size restrictions (default 128MB per file)

## Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### API Design Guidelines

When contributing new features to this SDK, please follow these guidelines:

1. **Public methods in operation namespaces** - New public methods go in the appropriate namespace module under [operations/](src/PowerPlatform/Dataverse/operations/). Public types and constants live in their own modules (e.g., `models/metadata.py`, `common/constants.py`)
2. **Add README example for public methods** - Add usage examples to this README for public API methods
3. **Document public APIs** - Include Sphinx-style docstrings with parameter descriptions and examples for all public methods
4. **Update documentation** when adding features - Keep README and SKILL files (note that each skill has 2 copies) in sync
5. **Internal vs public naming** - Modules, files, and functions not meant to be part of the public API must use a `_` prefix (e.g., `_odata.py`, `_relationships.py`). Files without the prefix (e.g., `constants.py`, `metadata.py`) are public and importable by SDK consumers

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
