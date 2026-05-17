---
name: dataverse-sdk-use
description: Guidance for using the PowerPlatform Dataverse Client Python SDK. Use when calling the SDK like creating CRUD operations, SQL queries, table metadata management, relationships, and upload files.
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

### Operation Namespaces
- `client.records` -- CRUD and OData queries
- `client.query` -- query and search operations
- `client.tables` -- table metadata, columns, and relationships
- `client.files` -- file upload operations
- `client.batch` -- batch multiple operations into a single HTTP request

### Bulk Operations
The SDK supports Dataverse's native bulk operations: Pass lists to `create()`, `update()` for automatic bulk processing, for `delete()`, set `use_bulk_delete` when passing lists to use bulk operation

### Paging
- Control page size with `page_size` parameter
- Use `top` parameter to limit total records returned

### DataFrame Support
- DataFrame operations are accessed via the `client.dataframe` namespace: `client.dataframe.get()`, `client.dataframe.create()`, `client.dataframe.update()`, `client.dataframe.delete()`

## Common Operations

### Import
```python
from azure.identity import (
    InteractiveBrowserCredential,
    ClientSecretCredential,
    CertificateCredential,
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
credential = CertificateCredential(tenant_id, client_id, cert_path)

# Create client with context manager (recommended -- enables HTTP connection pooling)
# No trailing slash on URL!
with DataverseClient("https://yourorg.crm.dynamics.com", credential) as client:
    ...  # all operations here
# Session closed, caches cleared automatically

# Or without context manager:
client = DataverseClient("https://yourorg.crm.dynamics.com", credential)
```

### CRUD Operations

#### Create Records
```python
# Single record
account_id = client.records.create("account", {"name": "Contoso Ltd", "telephone1": "555-0100"})

# Bulk create (uses CreateMultiple API automatically)
contacts = [
    {"firstname": "John", "lastname": "Doe"},
    {"firstname": "Jane", "lastname": "Smith"}
]
contact_ids = client.records.create("contact", contacts)
```

#### Read Records
```python
# Get single record by ID
account = client.records.get("account", account_id, select=["name", "telephone1"])

# Query with filter (paginated)
for page in client.records.get(
    "account",
    select=["accountid", "name"],      # select is case-insensitive (automatically lowercased)
    filter="statecode eq 0",           # filter must use lowercase logical names (not transformed)
    top=100,
):
    for record in page:
        print(record["name"])

# Query with navigation property expansion (case-sensitive!)
for page in client.records.get(
    "account",
    select=["name"],
    expand=["primarycontactid"],  # Navigation properties are case-sensitive!
    filter="statecode eq 0",      # Column names must be lowercase logical names
):
    for account in page:
        contact = account.get("primarycontactid", {})
        print(f"{account['name']} - {contact.get('fullname', 'N/A')}")
```

#### Create Records with Lookup Bindings (@odata.bind)
```python
# Set lookup fields using @odata.bind with PascalCase navigation property names
# CORRECT: use the navigation property name (case-sensitive, must match $metadata)
guid = client.records.create("new_ticket", {
    "new_name": "TKT-001",
    "new_CustomerId@odata.bind": f"/new_customers({customer_id})",
    "new_AgentId@odata.bind": f"/new_agents({agent_id})",
})

# WRONG: lowercase navigation property causes 400 error
# "new_customerid@odata.bind" -> ODataException: undeclared property 'new_customerid'
```

#### Update Records
```python
# Single update
client.records.update("account", account_id, {"telephone1": "555-0200"})

# Bulk update (broadcast same change to multiple records)
client.records.update("account", [id1, id2, id3], {"industry": "Technology"})
```

#### Upsert Records
Creates or updates records identified by alternate keys. Single item -> PATCH; multiple items -> `UpsertMultiple` bulk action.
> **Prerequisite**: The table must have an alternate key configured in Dataverse for the columns used in `alternate_key`. Without it, Dataverse will reject the request with a 400 error.
```python
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# Single upsert
client.records.upsert("account", [
    UpsertItem(
        alternate_key={"accountnumber": "ACC-001"},
        record={"name": "Contoso Ltd", "telephone1": "555-0100"},
    )
])

# Bulk upsert (uses UpsertMultiple API automatically)
client.records.upsert("account", [
    UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso Ltd"}),
    UpsertItem(alternate_key={"accountnumber": "ACC-002"}, record={"name": "Fabrikam Inc"}),
])

# Composite alternate key
client.records.upsert("account", [
    UpsertItem(
        alternate_key={"accountnumber": "ACC-001", "address1_postalcode": "98052"},
        record={"name": "Contoso Ltd"},
    )
])

# Plain dict syntax (no import needed)
client.records.upsert("account", [
    {"alternate_key": {"accountnumber": "ACC-001"}, "record": {"name": "Contoso Ltd"}}
])
```

#### Delete Records
```python
# Single delete
client.records.delete("account", account_id)

# Bulk delete (uses BulkDelete API)
client.records.delete("account", [id1, id2, id3], use_bulk_delete=True)
```

### DataFrame Operations

The SDK provides DataFrame wrappers for all CRUD operations via the `client.dataframe` namespace, using pandas DataFrames and Series as input/output.

```python
import pandas as pd

# Query records -- returns a single DataFrame
df = client.dataframe.get("account", filter="statecode eq 0", select=["name"])
print(f"Got {len(df)} rows")

# Limit results with top for large tables
df = client.dataframe.get("account", select=["name"], top=100)

# Fetch single record as one-row DataFrame
df = client.dataframe.get("account", record_id=account_id, select=["name"])

# Create records from a DataFrame (returns a Series of GUIDs)
new_accounts = pd.DataFrame([
    {"name": "Contoso", "telephone1": "555-0100"},
    {"name": "Fabrikam", "telephone1": "555-0200"},
])
new_accounts["accountid"] = client.dataframe.create("account", new_accounts)

# Update records from a DataFrame (id_column identifies the GUID column)
new_accounts["telephone1"] = ["555-0199", "555-0299"]
client.dataframe.update("account", new_accounts, id_column="accountid")

# Clear a field by setting clear_nulls=True (by default, NaN/None fields are skipped)
df = pd.DataFrame([{"accountid": "guid-1", "websiteurl": None}])
client.dataframe.update("account", df, id_column="accountid", clear_nulls=True)

# Delete records by passing a Series of GUIDs
client.dataframe.delete("account", new_accounts["accountid"])
```

### SQL Queries

SQL queries are **read-only** and support limited SQL syntax. A single SELECT statement with optional WHERE, TOP (integer literal), ORDER BY (column names only), and a simple table alias after FROM is supported. But JOIN and subqueries may not be. Refer to the Dataverse documentation for the current feature set.

```python
results = client.query.sql(
    "SELECT TOP 10 accountid, name FROM account WHERE statecode = 0"
)
for record in results:
    print(record["name"])
```

### Table Management

#### Create Custom Tables
```python
# Create table with columns (include customization prefix!)
table_info = client.tables.create(
    "new_Product",
    {
        "new_Code": "string",
        "new_Price": "decimal",
        "new_Active": "bool",
        "new_Quantity": "int",
    },
)

# With solution assignment and custom primary column
table_info = client.tables.create(
    "new_Product",
    {"new_Code": "string", "new_Price": "decimal"},
    solution="MyPublisher",
    primary_column="new_ProductCode",
)
```

#### Supported Column Types
Types on the same line map to the same exact format under the hood
- `"string"` or `"text"` - Single line of text
- `"memo"` or `"multiline"` - Multiple lines of text (4000 character default)
- `"int"` or `"integer"` - Whole number
- `"decimal"` or `"money"` - Decimal number
- `"float"` or `"double"` - Floating point number
- `"bool"` or `"boolean"` - Yes/No
- `"datetime"` or `"date"` - Date
- `"file"` - File column
- Enum subclass - Local option set (picklist)

#### Manage Columns
```python
# Add columns to existing table (must include customization prefix!)
client.tables.add_columns("new_Product", {
    "new_Category": "string",
    "new_InStock": "bool",
})

# Remove columns
client.tables.remove_columns("new_Product", ["new_Category"])
```

#### Inspect Tables
```python
# Get single table information
table_info = client.tables.get("new_Product")
print(f"Logical name: {table_info['table_logical_name']}")
print(f"Entity set: {table_info['entity_set_name']}")

# List all tables
tables = client.tables.list()
for table in tables:
    print(table)
```

#### Get Extended Table Metadata
```python
# Get table with column metadata
info = client.tables.get("account", include_columns=True)
for col in info["columns"]:
    print(f"{col.logical_name} ({col.attribute_type})")

# Get table with relationship metadata
info = client.tables.get("account", include_relationships=True)

# Get specific entity properties
info = client.tables.get("account", select=["DisplayName", "Description"])
```

#### List Columns
```python
from PowerPlatform.Dataverse.models.table_info import ColumnInfo

columns = client.tables.get_columns("account")
for col in columns:
    print(f"{col.schema_name}: {col.attribute_type} (required: {col.required_level})")

# Filter to specific column types (OData syntax, fully-qualified enum)
picklists = client.tables.get_columns(
    "account",
    filter="AttributeType eq Microsoft.Dynamics.CRM.AttributeTypeCode'Picklist'",
)
```

#### Get Single Column
```python
col = client.tables.get_column("account", "emailaddress1")
if col:
    print(f"Type: {col.attribute_type}, Required: {col.required_level}")
```

#### Get Column Options (Picklist/Choice Values)
```python
from PowerPlatform.Dataverse.models.table_info import OptionSetInfo

options = client.tables.get_column_options("account", "accountcategorycode")
if options:
    for opt in options.options:
        print(f"  Value={opt.value}, Label={opt.label}")
```

#### List Table Relationships
```python
# All relationships
rels = client.tables.list_relationships("account")

# Specific type: "one_to_many" / "1:N", "many_to_one" / "N:1", "many_to_many" / "N:N"
rels = client.tables.list_relationships("account", relationship_type="one_to_many")
for rel in rels:
    print(f"{rel['SchemaName']}: {rel.get('ReferencingEntity')}")
```

#### Delete Tables
```python
client.tables.delete("new_Product")
```

### Relationship Management

#### Create One-to-Many Relationship
```python
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
)
from PowerPlatform.Dataverse.common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

lookup = LookupAttributeMetadata(
    schema_name="new_DepartmentId",
    display_name=Label(
        localized_labels=[LocalizedLabel(label="Department", language_code=1033)]
    ),
)

relationship = OneToManyRelationshipMetadata(
    schema_name="new_Department_Employee",
    referenced_entity="new_department",
    referencing_entity="new_employee",
    referenced_attribute="new_departmentid",
    cascade_configuration=CascadeConfiguration(
        delete=CASCADE_BEHAVIOR_REMOVE_LINK,
    ),
)

result = client.tables.create_one_to_many_relationship(lookup, relationship)
print(f"Created lookup field: {result['lookup_schema_name']}")
```

#### Create Many-to-Many Relationship
```python
from PowerPlatform.Dataverse.models.relationship import ManyToManyRelationshipMetadata

relationship = ManyToManyRelationshipMetadata(
    schema_name="new_employee_project",
    entity1_logical_name="new_employee",
    entity2_logical_name="new_project",
)

result = client.tables.create_many_to_many_relationship(relationship)
print(f"Created: {result['relationship_schema_name']}")
```

#### Convenience Method for Lookup Fields
```python
result = client.tables.create_lookup_field(
    referencing_table="new_order",
    lookup_field_name="new_AccountId",
    referenced_table="account",
    display_name="Account",
    required=True,
)
```

#### Query and Delete Relationships
```python
# Get relationship metadata
rel = client.tables.get_relationship("new_Department_Employee")
if rel:
    print(f"Found: {rel['SchemaName']}")

# Delete relationship
client.tables.delete_relationship(result["relationship_id"])
```

### File Operations

```python
# Upload file to a file column
client.files.upload(
    table="account",
    record_id=account_id,
    file_column="new_Document",  # If the file column doesn't exist, it will be created automatically
    path="/path/to/document.pdf",
)
```

### Batch Operations

Use `client.batch` to send multiple operations in one HTTP request. All batch methods return `None`; results arrive via `BatchResult` after `execute()`.

```python
# Build a batch request
batch = client.batch.new()
batch.records.create("account", {"name": "Contoso"})
batch.records.update("account", account_id, {"telephone1": "555-0100"})
batch.records.get("account", account_id, select=["name"])
batch.query.sql("SELECT TOP 5 name FROM account")

result = batch.execute()
for item in result.responses:
    if item.is_success:
        print(f"[OK] {item.status_code} entity_id={item.entity_id}")
        if item.data:
            # GET responses populate item.data with the parsed JSON record
            print(item.data.get("name"))
    else:
        print(f"[ERR] {item.status_code}: {item.error_message}")

# Transactional changeset (all succeed or roll back)
with batch.changeset() as cs:
    ref = cs.records.create("contact", {"firstname": "Alice"})
    cs.records.update("account", account_id, {"primarycontactid@odata.bind": ref})

# Continue on error
result = batch.execute(continue_on_error=True)
print(f"Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
```

**BatchResult properties:**
- `result.responses` -- list of `BatchItemResponse` in submission order
- `result.succeeded` -- responses with 2xx status codes
- `result.failed` -- responses with non-2xx status codes
- `result.has_errors` -- True if any response failed
- `result.entity_ids` -- GUIDs from OData-EntityId headers (creates and updates)

**Batch limitations:**
- Maximum 1000 operations per batch
- Paginated `records.get()` (without `record_id`) is not supported in batch
- `flush_cache()` is not supported in batch

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
- For `@odata.bind` errors ("undeclared property"): the navigation property name before `@odata.bind` is case-sensitive and must match the entity's `$metadata` exactly (e.g., `new_CustomerId@odata.bind` for custom lookups, `parentaccountid@odata.bind` for system lookups). The SDK preserves `@odata.bind` key casing.

## Best Practices

### Performance Optimization

1. **Use bulk operations** - Pass lists to create/update/delete for automatic optimization
2. **Specify select fields** - Limit returned columns to reduce payload size
3. **Control page size** - Use `top` and `page_size` parameters appropriately
4. **Reuse client instances** - Don't create new clients for each operation
5. **Use production credentials** - ClientSecretCredential or CertificateCredential for unattended operations
6. **Error handling** - Implement retry logic for transient errors (`e.is_transient`)
7. **Always include customization prefix** for custom tables/columns
8. **Use lowercase for column names, match `$metadata` for navigation properties** - Column names in `$select`/`$filter`/record payloads use lowercase LogicalNames. Navigation properties in `$expand` and `@odata.bind` keys are case-sensitive and must match the entity's `$metadata` (PascalCase for custom lookups like `new_CustomerId`, lowercase for system lookups like `parentaccountid`)
9. **Test in non-production environments** first
10. **Use named constants** - Import cascade behavior constants from `PowerPlatform.Dataverse.common.constants`

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
