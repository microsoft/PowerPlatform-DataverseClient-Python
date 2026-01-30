---
name: powerplatform-dataverseclient-python
description: Guidance for using the PowerPlatform Dataverse Client Python SDK. Use when calling the SDK like creating CRUD operations, SQL queries, table metadata management, and upload files.
---

# PowerPlatform Dataverse SDK Guide

## Overview

Use the PowerPlatform Dataverse Client Python SDK to interact with Microsoft Dataverse.

## Key Concepts

### Schema Names vs Display Names
- Standard tables: lowercase (e.g., `"account"`, `"contact"`)
- Custom tables: include customization prefix (e.g., `"new_Product"`, `"cr123_Invoice"`)
- Custom columns: include customization prefix (e.g., `"new_Price"`, `"cr123_Status"`)
- ALWAYS use **schema names** (logical names), NOT display names

### Bulk Operations
The SDK supports Dataverse's native bulk operations: Pass lists to `create()`, `update()` for automatic bulk processing, for `delete()`, set `use_bulk_delete` when passing lists to use bulk operation

### Paging
- Control page size with `page_size` parameter
- Use `top` parameter to limit total records returned

## Common Operations

### Import
```python
from azure.identity import (
    InteractiveBrowserCredential, 
    ClientSecretCredential,
    ClientCertificateCredential,
    AzureCliCredential
)
from PowerPlatform.Dataverse.client import DataverseClient
```

### Client Initialization
```python
# Development options
credential = InteractiveBrowserCredential()
credential = AzureCliCredential()

# Production options
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
credential = ClientCertificateCredential(tenant_id, client_id, cert_path)

# Create client (no trailing slash on URL!)
client = DataverseClient("https://yourorg.crm.dynamics.com", credential)
```

### CRUD Operations

#### Create Records
```python
# Single record
account_ids = client.create("account", {"name": "Contoso Ltd", "telephone1": "555-0100"})
account_id = account_ids[0]

# Bulk create (uses CreateMultiple API automatically)
contacts = [
    {"firstname": "John", "lastname": "Doe"},
    {"firstname": "Jane", "lastname": "Smith"}
]
contact_ids = client.create("contact", contacts)
```

#### Read Records
```python
# Get single record by ID
account = client.get("account", account_id, select=["name", "telephone1"])

# Query with filter
pages = client.get(
    "account",
    select=["accountid", "name"],      # select is case-insensitive (automatically lowercased)
    filter="statecode eq 0",           # filter must use lowercase logical names (not transformed)
    top=100
)
for page in pages:
    for record in page:
        print(record["name"])

# Query with navigation property expansion (case-sensitive!)
pages = client.get(
    "account",
    select=["name"],
    expand=["primarycontactid"],  # Navigation properties are case-sensitive!
    filter="statecode eq 0"       # Column names must be lowercase logical names
)
for page in pages:
    for account in page:
        contact = account.get("primarycontactid", {})
        print(f"{account['name']} - {contact.get('fullname', 'N/A')}")
```

#### Update Records
```python
# Single update
client.update("account", account_id, {"telephone1": "555-0200"})

# Bulk update (broadcast same change to multiple records)
client.update("account", [id1, id2, id3], {"industry": "Technology"})
```

#### Delete Records
```python
# Single delete
client.delete("account", account_id)

# Bulk delete (uses BulkDelete API)
client.delete("account", [id1, id2, id3], use_bulk_delete=True)
```

### SQL Queries

SQL queries are **read-only** and support limited SQL syntax. A single SELECT statement with optional WHERE, TOP (integer literal), ORDER BY (column names only), and a simple table alias after FROM is supported. But JOIN and subqueries may not be. Refer to the Dataverse documentation for the current feature set.

```python
# Basic SQL query
results = client.query_sql(
    "SELECT TOP 10 accountid, name FROM account WHERE statecode = 0"
)
for record in results:
    print(record["name"])
```

### Table Management

#### Create Custom Tables
```python
# Create table with columns (include customization prefix!)
table_info = client.create_table(
    table_schema_name="new_Product",
    columns={
        "new_Code": "string",
        "new_Price": "decimal",
        "new_Active": "bool",
        "new_Quantity": "int"
    }
)

# With solution assignment and custom primary column
table_info = client.create_table(
    table_schema_name="new_Product",
    columns={"new_Code": "string", "new_Price": "decimal"},
    solution_unique_name="MyPublisher",
    primary_column_schema_name="new_ProductCode"
)
```

#### Supported Column Types
Types on the same line map to the same exact format under the hood
- `"string"` or `"text"` - Single line of text
- `"int"` or `"integer"` - Whole number
- `"decimal"` or `"money"` - Decimal number
- `"float"` or `"double"` - Floating point number
- `"bool"` or `"boolean"` - Yes/No
- `"datetime"` or `"date"` - Date
- Enum subclass - Local option set (picklist)

#### Manage Columns
```python
# Add columns to existing table (must include customization prefix!)
client.create_columns("new_Product", {
    "new_Category": "string",
    "new_InStock": "bool"
})

# Remove columns
client.delete_columns("new_Product", ["new_Category"])
```

#### Inspect Tables
```python
# Get single table information
table_info = client.get_table_info("new_Product")
print(f"Logical name: {table_info['table_logical_name']}")
print(f"Entity set: {table_info['entity_set_name']}")

# List all tables
tables = client.list_tables()
for table in tables:
    print(table)
```

#### Delete Tables
```python
# Delete custom table
client.delete_table("new_Product")
```

### File Operations

```python
# Upload file to a file column
client.upload_file(
    table_schema_name="account",
    record_id=account_id,
    file_name_attribute="new_document",  # If the file column doesn't exist, it will be created automatically
    path="/path/to/document.pdf"
)
```

## Error Handling

The SDK provides structured exceptions with detailed error information:

```python
from PowerPlatform.Dataverse.core.errors import (
    DataverseError,
    HttpError,
    ValidationError,
    MetadataError,
    SQLParseError
)
from PowerPlatform.Dataverse.client import DataverseClient

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

### Common Error Patterns

**Authentication failures:**
- Check environment URL format (no trailing slash)
- Verify credentials have Dataverse permissions
- Ensure app registration is properly configured

**404 Not Found:**
- Verify table schema name is correct (lowercase for standard tables)
- Check record ID exists
- Ensure using schema names, not display names
- Cache issue could happen, so retry might help, especially for metadata creation

**400 Bad Request:**
- Check filter/expand parameters use correct case
- Verify column names exist and are spelled correctly
- Ensure custom columns include customization prefix

## Best Practices

### Performance Optimization

1. **Use bulk operations** - Pass lists to create/update/delete for automatic optimization
2. **Specify select fields** - Limit returned columns to reduce payload size
3. **Control page size** - Use `top` and `page_size` parameters appropriately
4. **Reuse client instances** - Don't create new clients for each operation
5. **Use production credentials** - ClientSecretCredential or ClientCertificateCredential for unattended operations
6. **Error handling** - Implement retry logic for transient errors (`e.is_transient`)
7. **Always include customization prefix** for custom tables/columns
8. **Use lowercase** - Generally using lowercase input won't go wrong, except for custom table/column naming
9. **Test in non-production environments** first

## Additional Resources

Load these resources as needed during development:

- [API Reference](https://learn.microsoft.com/python/api/dataverse-sdk-docs-python/dataverse-overview)
- [Product Documentation](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/)
- [Dataverse Web API](https://learn.microsoft.com/power-apps/developer/data-platform/webapi/)
- [Azure Identity](https://learn.microsoft.com/python/api/overview/azure/identity-readme)

## Key Reminders

1. **Schema names are required** - Never use display names
2. **Custom tables need prefixes** - Include customization prefix (e.g., "new_")
3. **Filter is case-sensitive** - Use lowercase logical names
4. **Bulk operations are encouraged** - Pass lists for optimization
5. **No trailing slashes in URLs** - Format: `https://org.crm.dynamics.com`
6. **Structured errors** - Check `is_transient` for retry logic
