# PowerPlatform Dataverse Client for Python — Typed API

[![PyPI version](https://img.shields.io/pypi/v/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![Python](https://img.shields.io/pypi/pyversions/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A typed extension of the PowerPlatform Dataverse Client that provides strongly typed Python entity classes generated from your live Dataverse schema. Invalid field names and type mismatches are visible at authoring time, not at runtime.

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
  - [Generate entity types](#generate-entity-types)
  - [Quick start](#quick-start)
  - [Basic CRUD operations](#basic-crud-operations)
  - [Bulk operations](#bulk-operations)
  - [Upsert operations](#upsert-operations)
  - [DataFrame operations](#dataframe-operations)
  - [Query data](#query-data) *(typed QueryBuilder, SQL, raw OData)*
  - [Table management](#table-management)
  - [Relationship management](#relationship-management)
  - [File operations](#file-operations)
  - [Batch operations](#batch-operations)
- [Next steps](#next-steps)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Key features

- **🔄 CRUD Operations**: Create, read, update, and delete records using typed entity instances — invalid field names and value types are caught at authoring time
- **⚡ True Bulk Operations**: Automatically uses Dataverse's native `CreateMultiple`, `UpdateMultiple`, `UpsertMultiple`, and `BulkDelete` Web API operations for maximum performance and transactional integrity
- **🔷 Strongly Typed Entity Classes**: Generate schema-aligned Python classes from a live Dataverse environment; plain type annotations are all that's needed
- **🔍 Fluent Typed QueryBuilder**: `client.query.builder(Account).select().where()` query construction using typed field descriptors; conditions are Python operator expressions (`Account.statecode == 0`) that compile to correct OData automatically
- **📊 SQL Queries**: Execute read-only SQL queries via the Dataverse Web API `?sql=` parameter
- **🏗️ Table Management**: Create, inspect, and delete custom tables and columns programmatically — pass the entity class directly
- **🔗 Relationship Management**: Create one-to-many and many-to-many relationships between tables with full metadata control
- **🐼 DataFrame Support**: Pandas wrappers for all CRUD operations, returning DataFrames and Series
- **📎 File Operations**: Upload files to Dataverse file columns with automatic chunking for large files
- **📦 Batch Operations**: Send multiple CRUD, table metadata, and SQL query operations in a single HTTP request with optional transactional changesets
- **🔐 Azure Identity**: Built-in authentication using Azure Identity credential providers with comprehensive support
- **🛡️ Error Handling**: Structured exception hierarchy with detailed error context and retry guidance
- **📋 HTTP Diagnostics Logging**: Opt-in file-based logging of all HTTP requests and responses with automatic redaction of sensitive headers (e.g. `Authorization`)

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
| **DataverseClient** | Main entry point; provides `records`, `query`, `tables`, `files`, and `batch` namespaces |
| **Context Manager** | Use `with DataverseClient(...) as client:` for automatic cleanup and HTTP connection pooling |
| **Namespaces** | Operations are organized into `client.records` (CRUD & OData queries), `client.query` (typed QueryBuilder & SQL), `client.tables` (metadata), `client.files` (file uploads), and `client.batch` (batch requests) |
| **Entity classes** | Generated Python classes aligned to your Dataverse schema — each class has typed field descriptors (`Account.name`, `Account.statecode`) that provide IDE autocomplete and catch invalid field names at authoring time |
| **Field** | A typed field definition on an entity class. Auto-created from plain type annotations, or defined explicitly via `Field("name", str, schema_name="Name", dataverse_type="string")`. Supports Python operator overloads (`==`, `!=`, `>`, `>=`, `<`, `<=`) that produce filter expressions, so `Account.statecode == 0` compiles to `statecode eq 0` in OData. Explicit `Field()` definitions also carry `schema_name` and `dataverse_type`, enabling `tables.create(EntityClass)` without a separate columns dict. |
| **Entity instances** | Constructed via `Account(name="Contoso")` and passed directly to `client.records.create()` / `client.records.update()` — field names are validated at construction time |
| **Typed query results** | When `client.query.builder(Account)` is used, `.execute()` yields typed `Account` instances with attribute access (`account.name`) instead of dict lookup |
| **Schema names** | Use table schema names (`"account"`, `"new_MyTestTable"`) and column schema names (`"name"`, `"new_MyTestColumn"`). See: [Table definitions in Microsoft Dataverse](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/entity-metadata) |
| **Bulk Operations** | Efficient bulk processing for multiple records with automatic optimization |
| **Paging** | Automatic handling of large result sets with iterators |
| **Structured Errors** | Detailed exception hierarchy with retry guidance and diagnostic information |
| **Customization prefix values** | Custom tables and columns require a customization prefix value to be included for all operations (e.g., `"new_MyTestTable"`, not `"MyTestTable"`). See: [Table definitions in Microsoft Dataverse](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/entity-metadata) |

## Examples

### Generate entity types

Before using the typed API, generate Python entity classes from your live Dataverse environment. The generator connects to Dataverse, downloads entity metadata, and writes strongly typed Python classes to a `Types/` folder.

```python
from PowerPlatform.Dataverse.generator import generate
from azure.identity import InteractiveBrowserCredential

generate(
    org_url="https://yourorg.crm.dynamics.com",
    entities=["account", "contact", "lead"],
    credential=InteractiveBrowserCredential(),
)
```

Or use the CLI:

```bash
python -m PowerPlatform.Dataverse.generator \
    --url      https://yourorg.crm.dynamics.com \
    --entities account contact lead
```

This creates one class per entity under `Types/`:

```
Types/
  account.py       — Account(Entity) with typed fields
  contact.py
  lead.py
  picklists/       — option set enums
  booleans/        — boolean (two-option) types
  __init__.py
```

A generated class looks like this:

```python
# Types/account.py  (auto-generated — do not edit)
from PowerPlatform.Dataverse.models.entity import Entity, Field

class Account(Entity, table="account", primary_key="accountid"):
    accountid:  str
    name:       str
    telephone1: str
    revenue:    float
    statecode:  int
    statuscode: int
    websiteurl: str
```

> **Note**: The generator uses annotation-only syntax — sufficient for all query and CRUD operations.
> If you also want `tables.create(Account)` to work without a columns dict, switch to explicit
> `Field()` definitions that carry `schema_name` and `dataverse_type`.

Plain type annotations are all that's needed for query and CRUD operations — the SDK wires up `Field` descriptors automatically from those annotations. Each annotated field serves two roles:

- **On the class** (`Account.statecode`): returns a `Field` that overloads Python operators to produce filter expressions (`Account.statecode == 0` → `statecode eq 0`)
- **On an instance** (`account.statecode`): returns the stored field value (`0`, `1`, etc.)

For **table creation** (`client.tables.create(EntityClass)`), use explicit `Field()` definitions that carry `schema_name` and `dataverse_type` — these give the SDK everything it needs to derive the column schema automatically:

```python
from PowerPlatform.Dataverse.models.entity import Entity, Field

class Product(Entity, table="new_Product", primary_key="new_productid"):
    new_productid = Field("new_productid", str)
    new_title     = Field("new_title",  str,   schema_name="new_Title",  dataverse_type="string")
    new_price     = Field("new_price",  float, schema_name="new_Price",  dataverse_type="decimal")

# columns dict derived automatically from Field.schema_name / Field.dataverse_type
client.tables.create(Product)
```

Annotation-only syntax (no explicit `Field()`) is sufficient when you only need queries and CRUD, not table creation.

The generator also automatically discovers and fetches dependencies:

- **Lookup dependencies** — entities referenced by Lookup fields fetched automatically
- **M2M participants** — both sides of many-to-many relationships fetched automatically
- **OneToMany referencers** — entities that have a Lookup back to this entity (opt-in prompt)

> **Note**: After creating or modifying a custom table, re-run the entity generator to keep your
> `Types/` classes in sync with the latest schema.

### Quick start

```python
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from Types.contact import Contact

# Connect to Dataverse
credential = InteractiveBrowserCredential()

with DataverseClient("https://yourorg.crm.dynamics.com", credential) as client:
    # Create a contact — field names validated against the entity class at authoring time
    contact_id = client.records.create(Contact(firstname="John", lastname="Doe"))

    # Read the contact back — results are typed Contact instances
    for contact in (client.query.builder(Contact)
                    .select(Contact.firstname, Contact.lastname)
                    .where(Contact.contactid == contact_id)
                    .execute()):
        print(f"Created: {contact.firstname} {contact.lastname}")

    # Clean up
    client.records.delete(Contact, contact_id)
# Session closed, caches cleared automatically
```

### Basic CRUD operations

```python
from Types.account import Account

# Create a record — Account(...) validates field names at construction time
account_id = client.records.create(Account(name="Contoso Ltd"))

# Read a record — returns a typed Account instance directly when Entity class is passed
for account in (client.query.builder(Account)
                .select(Account.name)
                .where(Account.accountid == account_id)
                .execute()):
    print(account.name)

# Update a record
client.records.update(Account, account_id, Account(telephone1="555-0199"))

# Delete a record
client.records.delete(Account, account_id)
```

### Bulk operations

```python
from Types.account import Account

# Bulk create — entity instances carry the table; no separate table argument needed
ids = client.records.create([
    Account(name="Company A"),
    Account(name="Company B"),
    Account(name="Company C"),
])

# Bulk update (broadcast same change to all)
client.records.update(Account, ids, Account(industry="Technology"))

# Bulk delete
client.records.delete(Account, ids, use_bulk_delete=True)
```

### Upsert operations

Use `client.records.upsert()` to create or update records identified by alternate keys. When the
key matches an existing record it is updated; otherwise the record is created. A single item uses
a PATCH request; multiple items use the `UpsertMultiple` bulk action.

> **Prerequisite**: The table must have an **alternate key** configured in Dataverse for the
> columns used in `alternate_key`. Alternate keys are defined in the table's metadata (Power Apps
> maker portal → Table → Keys, or via the Dataverse API). Without a configured alternate key,
> upsert requests will be rejected by Dataverse with a 400 error.

```python
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from Types.account import Account

# UpsertItem.record takes a plain dict — use Account(...).to_dict() for typed, validated payloads
client.records.upsert(Account, [
    UpsertItem(
        alternate_key={"accountnumber": "ACC-001"},
        record=Account(name="Contoso Ltd", telephone1="555-0100").to_dict(),
    )
])

# Upsert multiple records (uses UpsertMultiple bulk action)
client.records.upsert(Account, [
    UpsertItem(
        alternate_key={"accountnumber": "ACC-001"},
        record=Account(name="Contoso Ltd").to_dict(),
    ),
    UpsertItem(
        alternate_key={"accountnumber": "ACC-002"},
        record=Account(name="Fabrikam Inc").to_dict(),
    ),
])

# Composite alternate key (multiple columns identify the record)
client.records.upsert(Account, [
    UpsertItem(
        alternate_key={"accountnumber": "ACC-001", "address1_postalcode": "98052"},
        record=Account(name="Contoso Ltd").to_dict(),
    )
])
```

### DataFrame operations

The SDK provides pandas wrappers via the `client.dataframe` namespace. Use the typed QueryBuilder for reads; pass the entity class directly for write operations.

```python
import pandas as pd
from Types.account import Account

# Query records as a single DataFrame using the typed builder
df = (client.query.builder(Account)
      .select(Account.name, Account.telephone1)
      .where(Account.statecode == 0)
      .to_dataframe())
print(f"Found {len(df)} accounts")

# Limit results with top for large tables
df = (client.query.builder(Account)
      .select(Account.name)
      .top(100)
      .to_dataframe())

# Fetch a single record as a one-row DataFrame
df = client.dataframe.get(Account, record_id=account_id, select=[Account.name.name])

# Create records from a DataFrame (returns a Series of GUIDs)
new_accounts = pd.DataFrame([
    {"name": "Contoso", "telephone1": "555-0100"},
    {"name": "Fabrikam", "telephone1": "555-0200"},
])
new_accounts["accountid"] = client.dataframe.create(Account, new_accounts)

# Update records from a DataFrame (id_column identifies the GUID column)
new_accounts["telephone1"] = ["555-0199", "555-0299"]
client.dataframe.update(Account, new_accounts, id_column="accountid")

# Clear a field by setting clear_nulls=True (by default, NaN/None fields are skipped)
df = pd.DataFrame([{"accountid": new_accounts["accountid"].iloc[0], "websiteurl": None}])
client.dataframe.update(Account, df, id_column="accountid", clear_nulls=True)

# Delete records by passing a Series of GUIDs
client.dataframe.delete(Account, new_accounts["accountid"])
```

### Query data

The **typed QueryBuilder** is the recommended way to query records. Pass an entity class to `client.query.builder()` and results are automatically hydrated into typed instances — attribute access instead of dict lookup, and IDE autocomplete on every field.

```python
from Types.account import Account

# Typed query — result is an iterable of Account instances
for account in (client.query.builder(Account)
                .select(Account.name, Account.revenue)
                .where(Account.statecode == 0)
                .where(Account.revenue > 1_000_000)
                .order_by(Account.revenue, descending=True)
                .top(100)
                .page_size(50)
                .execute()):
    print(f"{account.name}: {account.revenue}")   # attribute access, not dict lookup
```

The typed QueryBuilder handles value formatting, column name casing, and OData syntax automatically. All fields are discoverable via IDE autocomplete on the generated entity class:

```python
# Get results as a pandas DataFrame (consolidates all pages)
df = (client.query.builder(Account)
      .select(Account.name, Account.telephone1)
      .where(Account.statecode == 0)
      .top(100)
      .to_dataframe())
print(f"Got {len(df)} accounts")
```

```python
# Typed field descriptors compile directly to OData filter expressions
query = (client.query.builder(Account)
         .select(Account.name)
         .where(Account.statecode == 0)          # statecode eq 0
         .where(Account.revenue > 1_000_000)     # revenue gt 1000000
         .where(Account.name.contains("Corp"))   # contains(name, 'Corp')
         .where(Account.statecode.in_([0, 1]))   # Microsoft.Dynamics.CRM.In(...)
         .where(Account.revenue.between(100_000, 500_000))  # (revenue ge 100000 and revenue le 500000)
         .where(Account.telephone1.is_null())    # telephone1 eq null
         )
```

For complex logic (OR, NOT, grouping), compose conditions with `&`, `|`, and `~`:

```python
# OR conditions: (statecode = 0 OR statecode = 1) AND revenue > 100k
for account in (client.query.builder(Account)
                .select(Account.name, Account.revenue)
                .where((Account.statecode == 0) | (Account.statecode == 1))
                .where(Account.revenue > 100_000)
                .execute()):
    print(account.name)

# NOT and between
for account in (client.query.builder(Account)
                .select(Account.name)
                .where(~(Account.statecode == 2))               # NOT inactive
                .where(Account.revenue.between(100_000, 500_000))
                .execute()):
    print(account.name)
```

**Formatted values and annotations** — request localized labels, currency symbols, and display names:

> OData annotation keys (e.g. `statecode@OData.Community.Display.V1.FormattedValue`) are not declared
> as typed fields on entity classes, so they are not accessible on typed entity instances. Use the
> string-based builder when you need raw annotation access.

```python
# Use the string-based builder to access OData annotation keys
for record in (client.query.builder("account")
               .select("name", "statecode", "revenue")
               .include_formatted_values()
               .execute()):
    status = record["statecode@OData.Community.Display.V1.FormattedValue"]
    print(f"{record['name']}: {status}")
```

**Nested expand with options** — expand navigation properties with `$select`, `$filter`, `$orderby`, and `$top`:

> Declare navigation properties on the entity class using `NavField`. Expanded data is then
> accessible as a typed attribute on result instances.

```python
from PowerPlatform.Dataverse.models.entity import NavField
from Types.account import Account
from Types.task import Task

# Declare Account_Tasks as a NavField on Account (in the generated class or manually):
# class Account(Entity, table="account", primary_key="accountid"):
#     Account_Tasks = NavField("Account_Tasks")

# NavField fluent methods build the nested expand — no ExpandOption import needed
for account in (client.query.builder(Account)
                .select(Account.name)
                .expand(Account.Account_Tasks
                        .select(Task.subject, Task.createdon)
                        .filter_where(Task.statecode == 0)
                        .order_by(Task.createdon, descending=True)
                        .top(5))
                .execute()):
    print(account.name, account.Account_Tasks)
```

**Record count** — include `$count=true` in the request:

```python
results = (client.query.builder(Account)
           .where(Account.statecode == 0)
           .count()
           .execute())
```

**SQL queries** provide an alternative read-only query syntax:

```python
results = client.query.sql(
    "SELECT TOP 10 accountid, name FROM account WHERE statecode = 0"
)
for record in results:
    print(record["name"])
```

**Raw OData queries** are available via `records.get()` for cases where you need direct control over the OData filter string:

```python
for page in client.records.get(
    Account,
    select=[Account.name.name],          # records.get() select takes strings; use .name to extract
    filter=(Account.statecode == 0).to_odata(),  # Raw OData: column names must be lowercase
    expand=["primarycontactid"],          # Navigation properties are case-sensitive
    top=100,
):
    for record in page:
        print(record["name"])
```

### Table management

All table management methods accept an entity class or a plain string table name.

```python
from Types.account import Account

# Get table information using an entity class
info = client.tables.get(Account)
print(f"Logical name: {info.logical_name}")
print(f"Entity set: {info.entity_set_name}")

# Add and remove columns using an entity class
client.tables.add_columns(Account, {"new_Rating": "int"})
client.tables.remove_columns(Account, ["new_Rating"])

# List all tables
tables = client.tables.list()
for table in tables:
    print(table)
```

**Option 1 — typed** (recommended when you have an entity class with explicit `Field()` definitions):

```python
from PowerPlatform.Dataverse.models.entity import Entity, Field

class Product(Entity, table="new_Product", primary_key="new_productid"):
    new_productid   = Field("new_productid",   str)
    new_code        = Field("new_code",        str,   schema_name="new_Code",        dataverse_type="string")
    new_description = Field("new_description", str,   schema_name="new_Description", dataverse_type="memo")
    new_price       = Field("new_price",       float, schema_name="new_Price",       dataverse_type="decimal")
    new_active      = Field("new_active",      bool,  schema_name="new_Active",      dataverse_type="bool")

# columns dict derived automatically — no separate dict needed
table_info = client.tables.create(Product, solution="MyPublisher")
```

**Option 2 — string-based** (when no entity class exists yet; use a string, then re-run the generator):

```python
# Create a custom table with an explicit columns dict
table_info = client.tables.create("new_Product", {
    "new_Code": "string",
    "new_Description": "memo",
    "new_Price": "decimal",
    "new_Active": "bool",
}, solution="MyPublisher", primary_column="new_ProductName")
```

> **Important**: All custom column names must include the customization prefix value (e.g., `"new_"`).
> This ensures explicit, predictable naming and aligns with Dataverse metadata requirements.

### Relationship management

Create relationships between tables using the relationship API. For a complete working example, see [examples/advanced/relationships.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/relationships.py).

The convenience method `create_lookup_field` accepts entity classes directly. The lower-level metadata objects (`OneToManyRelationshipMetadata`, `ManyToManyRelationshipMetadata`) store table names as string fields inside the dataclass, so those still use plain strings.

```python
from Types.contact import Contact
from Types.account import Account

# Simplest way: create a lookup field using entity classes
result = client.tables.create_lookup_field(
    referencing_table=Contact,         # Child table gets the lookup field
    lookup_field_name="new_AccountId",
    referenced_table=Account,          # Parent table being referenced
    display_name="Account",
)
print(f"Created lookup: {result.lookup_schema_name}")
```

For full control over relationship metadata:

```python
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel

# Create a one-to-many relationship: Department (1) -> Employee (N)
# referenced_entity / referencing_entity are string fields on the dataclass
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

result = client.tables.create_one_to_many_relationship(lookup, relationship)
print(f"Created lookup field: {result.lookup_schema_name}")

# Create a many-to-many relationship: Employee (N) <-> Project (N)
m2m_relationship = ManyToManyRelationshipMetadata(
    schema_name="new_employee_project",
    entity1_logical_name="new_employee",
    entity2_logical_name="new_project",
)

result = client.tables.create_many_to_many_relationship(m2m_relationship)
print(f"Created M:N relationship: {result.relationship_schema_name}")

# Query and delete relationships (identified by schema name / ID, not table)
rel = client.tables.get_relationship("new_Department_Employee")
if rel:
    print(f"Found: {rel.relationship_schema_name}")

client.tables.delete_relationship(result.relationship_id)
```

### File operations

```python
from Types.account import Account

# Upload a file to a record
client.files.upload(
    Account,
    account_id,
    "new_Document",  # If the file column doesn't exist, it will be created automatically
    "/path/to/document.pdf",
)
```

### Batch operations

Use `client.batch` to send multiple operations in one HTTP request. The batch namespace mirrors `client.records`, `client.tables`, and `client.query`.

```python
from Types.account import Account

# Build a batch request and add operations
batch = client.batch.new()
batch.records.create(Account, Account(name="Contoso"))
batch.records.create(Account, [Account(name="Fabrikam"), Account(name="Woodgrove")])
batch.records.update(Account, account_id, Account(telephone1="555-0100"))
batch.records.delete(Account, old_id)
batch.records.get(Account, account_id, select=[Account.name])

result = batch.execute()
for item in result.responses:
    if item.is_success:
        print(f"[OK] {item.status_code} entity_id={item.entity_id}")
    else:
        print(f"[ERR] {item.status_code}: {item.error_message}")
```

**Transactional changeset** — all operations in a changeset succeed or roll back together:

```python
from Types.lead import Lead
from Types.contact import Contact
from Types.account import Account

batch = client.batch.new()
with batch.changeset() as cs:
    lead_ref    = cs.records.create(Lead,    Lead(firstname="Ada"))
    contact_ref = cs.records.create(Contact, Contact(firstname="Ada"))
    cs.records.create(Account, {
        **Account(name="Babbage & Co.").to_dict(),
        "originatingleadid@odata.bind": lead_ref,
        "primarycontactid@odata.bind":  contact_ref,
    })
result = batch.execute()
print(f"Created {len(result.entity_ids)} records atomically")
```

**Table metadata and SQL queries in a batch:**

```python
batch = client.batch.new()
batch.tables.create("new_Product", {"new_Price": "decimal", "new_InStock": "bool"})
batch.tables.add_columns("new_Product", {"new_Rating": "int"})
batch.tables.get("new_Product")
batch.query.sql("SELECT TOP 5 name FROM account")

result = batch.execute()
```

**Continue on error** — attempt all operations even when one fails:

```python
result = batch.execute(continue_on_error=True)
print(f"Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
for item in result.failed:
    print(f"[ERR] {item.status_code}: {item.error_message}")
```

**DataFrame integration** — feed pandas DataFrames directly into a batch:

```python
import pandas as pd
from Types.account import Account

batch = client.batch.new()

# Create records from a DataFrame
df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
batch.dataframe.create(Account, df)

# Update records from a DataFrame
updates = pd.DataFrame([
    {"accountid": id1, "telephone1": "555-0100"},
    {"accountid": id2, "telephone1": "555-0200"},
])
batch.dataframe.update(Account, updates, id_column="accountid")

# Delete records from a Series
batch.dataframe.delete(Account, pd.Series([id1, id2]))

result = batch.execute()
```

For a complete example see [examples/advanced/walkthrough_typed.py](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/walkthrough_typed.py).

## Next steps

### More sample code

Explore our comprehensive examples in the [`examples/`](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples) directory:

**🌱 Getting Started:**
- **[Installation & Setup](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/basic/installation_example.py)** - Validate installation and basic usage patterns
- **[Functional Testing](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/basic/functional_testing.py)** - Test core functionality in your environment

**🚀 Advanced Usage:**
- **[Typed Walkthrough](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/walkthrough_typed.py)** - Full typed API demonstration with production patterns
- **[Classic Walkthrough](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/walkthrough.py)** - String-based API for comparison
- **[Relationship Management](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/relationships.py)** - Create and manage table relationships
- **[File Upload](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/file_upload.py)** - Upload files to Dataverse file columns
- **[Batch Operations](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/advanced/batch.py)** - Send multiple operations in a single request with changesets

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
    client.records.get(Account, "invalid-id")
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
| **Select Fields** | Specify fields in `select()` to limit returned columns and reduce payload size |
| **Page Size Control** | Use `.top()` and `.page_size()` to control memory usage |
| **Connection Reuse** | Reuse `DataverseClient` instances across operations |
| **Production Credentials** | Use `ClientSecretCredential` or `CertificateCredential` for unattended operations |
| **Error Handling** | Implement retry logic for transient errors (`e.is_transient`) |

### HTTP diagnostics logging

Enable file-based HTTP logging to capture all requests and responses for debugging. Sensitive headers (e.g. `Authorization`) are automatically redacted.

```python
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.log_config import LogConfig

log_cfg = LogConfig(
    log_folder="./my_logs",      # Directory for log files (created if missing)
    log_file_prefix="crm_debug", # Filename prefix; timestamp appended automatically
    max_body_bytes=4096,         # Bytes of body to capture per entry — 0 (default) disables body capture
)
config = DataverseConfig(log_config=log_cfg)
client = DataverseClient("https://yourorg.crm.dynamics.com", credential, config=config)
```

Each log file is timestamped and rotated automatically (default 10 MB per file, 5 backups). Sample output:

```
[2026-04-11T15:27:31-0700] DEBUG >>> REQUEST  POST https://yourorg.crm.dynamics.com/api/data/v9.2/accounts
    Authorization: [REDACTED]
    Accept: application/json
    Content-Type: application/json
    OData-MaxVersion: 4.0
    OData-Version: 4.0
    User-Agent: DataverseSvcPythonClient:0.1.0b8
    x-ms-client-request-id: 7050c4d0-6bcc-48e3-a310-b4e8fa18ac69
    x-ms-correlation-id: 4cace77d-e4ee-4419-8c65-fc62beed6e71
    Body:    {"name":"Contoso Ltd"}
[2026-04-11T15:27:31-0700] DEBUG <<< RESPONSE 204 POST https://yourorg.crm.dynamics.com/api/data/v9.2/accounts (78.0ms)
    Content-Type: application/json; odata.metadata=minimal
    OData-Version: 4.0
    x-ms-service-request-id: a6d0b6c4-5dd1-47cb-83eb-b6fccf754216
    x-ms-ratelimit-burst-remaining-xrm-requests: 7998
```

> **Security note:** This feature is intended for development and debugging only.
> Log files are **plaintext** and may contain PII, sensitive business data, and
> Dataverse record IDs — even with `max_body_bytes=0` (the default), request URLs
> can include filter values and record identifiers.
>
> - **Never enable in production.** If required for production diagnostics, keep
>   `max_body_bytes=0` and treat log files as regulated data under your organization's
>   data handling policy.
> - **Restrict access.** Set file system permissions so only the process user can
>   read log files. Use an encrypted volume or folder in sensitive environments.
> - **Control retention.** Log rotation keeps up to 5 files by default (`backup_count`).
>   Delete logs after the debugging session; use secure deletion for regulated data.
> - **Prevent source control leaks.** Add the log folder to `.gitignore` immediately.

### Limitations

- SQL queries are **read-only** and support a limited subset of SQL syntax
- Create Table supports the following column types: string, memo, int, decimal, float, bool, datetime, file, and picklist (Enum subclass)
- File uploads are limited by Dataverse file size restrictions (default 128MB per file)
- Entity type generation requires network access to a live Dataverse environment; generated classes must be re-generated after schema changes

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
