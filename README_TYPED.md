# Strongly-Typed Entity Model

The Dataverse Python SDK supports a strongly-typed entity model as an opt-in layer on top of the standard string-based API. Typed entities replace plain dictionaries with descriptor-backed Python classes, giving you IDE autocompletion, refactoring support, and compile-time field names across your entire codebase.

Everything in this document is purely additive — all existing string-based code continues to work unchanged.

**[Walkthrough example](examples/advanced/walkthrough_typed.py)** | **[Main README](README.md)**

## Table of contents

- [Overview](#overview)
- [Defining entity classes](#defining-entity-classes)
  - [Field descriptors](#field-descriptors)
  - [Picklist (choice) fields](#picklist-choice-fields)
  - [Boolean (two-option) fields](#boolean-two-option-fields)
  - [Lookup fields](#lookup-fields)
- [Field types reference](#field-types-reference)
- [CRUD operations with typed entities](#crud-operations-with-typed-entities)
  - [Create](#create)
  - [Read](#read)
  - [Update](#update)
  - [Delete](#delete)
  - [Upsert](#upsert)
- [QueryBuilder with typed entities](#querybuilder-with-typed-entities)
- [DataFrame operations](#dataframe-operations)
- [Table management](#table-management)
- [Relationship management](#relationship-management)
- [File operations](#file-operations)
- [Batch operations](#batch-operations)
- [Payload helpers](#payload-helpers)
- [Entity introspection](#entity-introspection)
- [Code generator](#code-generator)

---

## Overview

A typed entity class is a Python class that subclasses `Entity` and declares its columns as **field descriptors**. Field descriptors implement the Python descriptor protocol: class-level access returns the descriptor itself (enabling filter expressions), while instance-level access returns the stored field value.

```python
from PowerPlatform.Dataverse.models.entity import Entity
from PowerPlatform.Dataverse.models.datatypes import Guid, Text, Integer, Money
from PowerPlatform.Dataverse.models.lookup import Lookup

class Account(Entity, table="account", primary_key="accountid"):
    accountid        = Guid(writable_on_create=False, writable_on_update=False)
    name             = Text(nullable=False, max_length=160)
    numberofemployees= Integer()
    revenue          = Money()
    primarycontactid = Lookup(target="contact")

# Construction
account = Account(name="Contoso", numberofemployees=500)

# Typed field access
print(account.name)              # "Contoso"
print(account.revenue)           # None  (not set)

# Class-level access returns the descriptor — used for filter expressions
expr = Account.name == "Contoso" # FilterExpression for use in QueryBuilder
```

---

## Defining entity classes

### Class declaration

Subclass `Entity` and pass table metadata as keyword arguments:

```python
from PowerPlatform.Dataverse.models.entity import Entity

class Contact(
    Entity,
    table="contact",           # Dataverse logical name
    primary_key="contactid",   # Primary key attribute name
    entity_set="contacts",     # OData entity set name (optional)
    primary_name="fullname",   # Primary name attribute (optional)
    label="Contact",           # Human-readable label (optional)
):
    ...
```

### Field descriptors

Declare columns as class-level descriptor instances. The Python attribute name becomes the Dataverse logical name automatically (via `__set_name__`). Override with `logical_name=` when the two differ.

```python
from PowerPlatform.Dataverse.models.datatypes import (
    Guid, Text, Memo, Integer, BigInt,
    DecimalNumber, Double, Money, DateTime,
)

class Order(Entity, table="new_order", primary_key="new_orderid"):
    new_orderid    = Guid(writable_on_create=False, writable_on_update=False)
    new_title      = Text(nullable=False, max_length=200)
    new_notes      = Memo()
    new_quantity   = Integer(min_value=0)
    new_lineitems  = BigInt()
    new_unitprice  = DecimalNumber(precision=2)
    new_taxrate    = Double()
    new_totalamount= Money()
    new_duedate    = DateTime(date_format="DateOnly")
    new_createdon  = DateTime(date_format="DateAndTime",
                              writable_on_create=False, writable_on_update=False)
```

All descriptor constructors accept these common keyword arguments:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nullable` | `bool` | `True` | Whether the field may be absent/None |
| `default` | value or None | `None` | Optional default value |
| `logical_name` | `str` | `""` | Override the Dataverse logical name |
| `label` | `str` | `""` | Human-readable label from metadata |
| `writable_on_create` | `bool` | `True` | Include in `to_create_payload()` |
| `writable_on_update` | `bool` | `True` | Include in `to_update_payload()` |

### Picklist (choice) fields

Define a `PicklistBase` subclass with named `PicklistOption` members, then use an instance of it as the column descriptor:

```python
from PowerPlatform.Dataverse.models.picklist import PicklistBase, PicklistOption

class OrderStatus(PicklistBase):
    Draft     = PicklistOption(1, "Draft")
    Submitted = PicklistOption(2, "Submitted")
    Approved  = PicklistOption(3, "Approved")
    Rejected  = PicklistOption(4, "Rejected")

class Order(Entity, table="new_order", primary_key="new_orderid"):
    new_status = OrderStatus()

# Option introspection
OrderStatus.options()             # {"Draft": PicklistOption(1), ...}
OrderStatus.from_value(2)         # PicklistOption(2, "Submitted")
OrderStatus.from_label("Approved")# PicklistOption(3, "Approved")

# Instance access
order = Order(new_status=OrderStatus.Submitted)
print(order.new_status)           # 2  (the integer code)

# Filter expression (class-level access)
expr = Order.new_status == OrderStatus.Approved
```

`Picklist`, `State`, and `Status` are type aliases for `PicklistBase` — use whichever communicates intent:

```python
from PowerPlatform.Dataverse.models.picklist import Picklist, State, Status
```

For multi-select choice columns use `MultiPicklist`:

```python
from PowerPlatform.Dataverse.models.picklist import MultiPicklist

class Account(Entity, table="account"):
    new_tags = MultiPicklist()
```

### Boolean (two-option) fields

Define a `BooleanBase` subclass with exactly one `True` and one `False` `BooleanOption`:

```python
from PowerPlatform.Dataverse.models.boolean import BooleanBase, BooleanOption

class AccountActive(BooleanBase):
    Active   = BooleanOption(True,  "Active")
    Inactive = BooleanOption(False, "Inactive")

class Account(Entity, table="account"):
    isdisabled = AccountActive()

# Introspection
AccountActive.true_option()          # BooleanOption(True, "Active")
AccountActive.false_option()         # BooleanOption(False, "Inactive")
AccountActive.from_value(False)      # BooleanOption(False, "Inactive")
AccountActive.from_label("active")   # BooleanOption(True, "Active")  — case-insensitive

# Filter expression
expr = Account.isdisabled == False
```

`Boolean` is a type alias for `BooleanBase`.

### Lookup fields

```python
from PowerPlatform.Dataverse.models.lookup import Lookup, CustomerLookup

class Contact(Entity, table="contact", primary_key="contactid"):
    # Single-target lookup
    parentaccountid = Lookup(target="account")

    # Polymorphic customer lookup (account or contact)
    customerid = CustomerLookup(targets=("account", "contact"))

# Instance access — value is the referenced record's GUID string
contact = Contact(parentaccountid="some-guid")
print(contact.parentaccountid)   # "some-guid"

# Filter expression
expr = Contact.parentaccountid == "some-guid"
```

---

## Field types reference

| Descriptor | Python value type | Dataverse type | Notes |
|------------|-------------------|----------------|-------|
| `Text` | `str` | String / nvarchar | `max_length` available |
| `Memo` | `str` | Memo / nvarchar(max) | Long text |
| `Integer` | `int` | Integer | `min_value`, `max_value` available |
| `BigInt` | `int` | BigInt | Large integers |
| `DecimalNumber` | `Decimal` | Decimal | `precision` available |
| `Double` | `float` | Double / float | |
| `Money` | `Decimal` | Money | Currency; use `Decimal` for precision |
| `DateTime` | `datetime` | DateTime | `date_format`, `datetime_behavior` |
| `Guid` | `str` | Uniqueidentifier | `Guid.new()` / `Guid.empty()` helpers |
| `PicklistBase` | `int` | Picklist / State / Status | Subclass with `PicklistOption` members |
| `MultiPicklist` | `list[int]` | Multiselectpicklist | Multi-select choices |
| `BooleanBase` | `bool` / `int` | Boolean (two-option) | Subclass with `BooleanOption` members |
| `Lookup` | `str` (GUID) | Lookup / Owner | `target` is the referenced table |
| `CustomerLookup` | `str` (GUID) | Customer | `targets` is a tuple of table names |

---

## CRUD operations with typed entities

Pass the entity **class** (not a string) as the first argument to any `client.records` method. The SDK resolves the table name from `_logical_name` and, for reads, hydrates results back into typed instances.

### Create

```python
# Single record — pass a typed entity instance
order = Order(
    new_title="Q1 Purchase",
    new_quantity=10,
    new_totalamount=Decimal("4999.99"),
    new_status=OrderStatus.Submitted,
)
order_id = client.records.create(Order, order)

# Multiple records — list of typed instances
orders = [
    Order(new_title="Order A", new_quantity=5,  new_status=OrderStatus.Draft),
    Order(new_title="Order B", new_quantity=12, new_status=OrderStatus.Approved),
]
ids = client.records.create(Order, orders)
```

`create()` automatically calls `to_create_payload()` before sending, which strips any fields marked `writable_on_create=False` (e.g. primary keys).

### Read

Single record — returns a typed instance:

```python
order: Order = client.records.get(Order, order_id)

print(order.new_title)         # "Q1 Purchase"
print(order.new_quantity)      # 10
status_opt = OrderStatus.from_value(order.new_status)
print(status_opt.label)        # "Submitted"
```

Multiple records — yields pages of typed instances:

```python
for page in client.records.get(Order, filter="new_quantity gt 5"):
    for order in page:
        print(order.new_title, order.new_quantity)
```

With `select`, pass either string logical names or descriptor references:

```python
for page in client.records.get(
    Order,
    select=[Order.new_title, Order.new_quantity],
    filter="new_status eq 2",
):
    ...
```

### Update

Pass a typed entity instance containing only the fields to change. `update()` calls `to_update_payload()` automatically:

```python
patch = Order(new_quantity=20, new_status=OrderStatus.Approved)
client.records.update(Order, order_id, patch)
```

Or pass a plain dict for a quick inline patch:

```python
client.records.update(Order, order_id, {"new_quantity": 20})
```

Bulk update — broadcast the same change to multiple records:

```python
client.records.update(Order, [id1, id2, id3], {"new_status": OrderStatus.Approved})
```

### Delete

```python
# Single delete
client.records.delete(Order, order_id)

# Bulk delete
job_id = client.records.delete(Order, [id1, id2, id3])
```

### Upsert

Pass the entity class as the table reference. Use `to_create_payload().as_dict()` to convert a typed instance into the record dict:

```python
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# Single upsert — entity class as table reference
client.records.upsert(Product, [
    UpsertItem(
        alternate_key={"new_productcode": "PROD-001"},
        record={"new_name": "Widget Pro", "new_price": 49.99},
    )
])

# Build the record dict from a typed instance
product = Product(new_name="Widget Pro", new_price=49.99)
client.records.upsert(Product, [
    UpsertItem(
        alternate_key={"new_productcode": "PROD-001"},
        record=product.to_create_payload().as_dict(),
    )
])

# Bulk upsert
client.records.upsert(Product, [
    UpsertItem(alternate_key={"new_productcode": "PROD-001"},
               record=Product(new_name="Widget Pro").to_create_payload().as_dict()),
    UpsertItem(alternate_key={"new_productcode": "PROD-002"},
               record=Product(new_name="Widget Lite").to_create_payload().as_dict()),
])
```

---

## QueryBuilder with typed entities

Pass the entity class to `client.query.builder()`. The builder returns typed instances from `execute()` and accepts field descriptors anywhere a column name is expected.

```python
# Descriptor-based filter and select
results = list(
    client.query.builder(Order)
    .select(Order.new_title, Order.new_totalamount, Order.new_status)
    .where(Order.new_status == OrderStatus.Submitted)
    .order_by(Order.new_totalamount, descending=True)
    .top(50)
    .execute()
)

for order in results:
    print(order.new_title, order.new_totalamount)
```

Descriptor filters combine with fluent filter helpers:

```python
from PowerPlatform.Dataverse.models.filters import gt

results = list(
    client.query.builder(Order)
    .where(Order.new_status == OrderStatus.Approved)
    .where(gt("new_totalamount", 1000))
    .order_by(Order.new_duedate)
    .execute()
)
```

Compound expressions using `&` and `|`:

```python
results = list(
    client.query.builder(Order)
    .where(
        (Order.new_status == OrderStatus.Draft) |
        (Order.new_status == OrderStatus.Submitted)
    )
    .where(Order.new_quantity > 5)
    .execute()
)
```

---

## DataFrame operations

Pass the entity class wherever a table name is expected. Results are always plain DataFrames — the entity class is used only to resolve the table name.

```python
import pandas as pd

# Query records as a DataFrame
df = client.dataframe.get(Account, filter="statecode eq 0", select=["name", "telephone1"])
print(f"Found {len(df)} accounts")

# Fetch a single record as a one-row DataFrame
df = client.dataframe.get(Account, record_id=account_id, select=["name"])

# Create records from a DataFrame
new_accounts = pd.DataFrame([
    {"name": "Contoso", "telephone1": "555-0100"},
    {"name": "Fabrikam", "telephone1": "555-0200"},
])
new_accounts["accountid"] = client.dataframe.create(Account, new_accounts)

# Update records from a DataFrame
new_accounts["telephone1"] = ["555-0199", "555-0299"]
client.dataframe.update(Account, new_accounts, id_column="accountid")

# Delete records by passing a Series of GUIDs
client.dataframe.delete(Account, new_accounts["accountid"])
```

---

## Table management

All `client.tables` operations accept an entity class wherever a table string is expected. The class's `_logical_name` is resolved automatically — no need to repeat the string.

```python
# Create a table — columns derived automatically from the entity class fields
table_info = client.tables.create(WalkthroughDemo)

# Optionally pin to a solution or specify the primary column
table_info = client.tables.create(
    WalkthroughDemo,
    solution="MyPublisher",
    primary_column="new_Title",
)

# All table operations accept the entity class
info = client.tables.get(WalkthroughDemo)
print(f"Logical name : {info['table_logical_name']}")
print(f"Entity set   : {info['entity_set_name']}")

tables = client.tables.list()

client.tables.add_columns(WalkthroughDemo, {"new_Tags": "string"})
client.tables.remove_columns(WalkthroughDemo, ["new_Tags"])
client.tables.delete(WalkthroughDemo)
```

> **Important**: All custom column names must include the customization prefix (e.g. `"new_"`).

---

## Relationship management

Entity class metadata (`_logical_name`, `_primary_id`) can be used to derive table and attribute names for relationship objects, eliminating hardcoded strings.

```python
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel

# One-to-many: Department (1) → Employee (N)
# Derive table/attribute names from entity class metadata
lookup = LookupAttributeMetadata(
    schema_name="new_DepartmentId",
    display_name=Label(localized_labels=[LocalizedLabel(label="Department", language_code=1033)]),
)

relationship = OneToManyRelationshipMetadata(
    schema_name="new_Department_Employee",
    referenced_entity=Department._logical_name,      # "new_department"
    referencing_entity=Employee._logical_name,       # "new_employee"
    referenced_attribute=Department._primary_id,     # "new_departmentid"
)

result = client.tables.create_one_to_many_relationship(lookup, relationship)
print(f"Created lookup field: {result['lookup_schema_name']}")

# Many-to-many: Employee (N) ↔ Project (N)
m2m = ManyToManyRelationshipMetadata(
    schema_name="new_employee_project",
    entity1_logical_name=Employee._logical_name,
    entity2_logical_name=Project._logical_name,
)
result = client.tables.create_many_to_many_relationship(m2m)
print(f"Created M:N relationship: {result['relationship_schema_name']}")

# Convenience method — pass entity classes directly as table references
result = client.tables.create_lookup_field(
    referencing_table=Contact,        # Entity class instead of "contact"
    lookup_field_name="new_AccountId",
    referenced_table=Account,         # Entity class instead of "account"
    display_name="Account",
)

# Relationship query and deletion (same as string-based)
rel = client.tables.get_relationship("new_Department_Employee")
if rel:
    print(f"Found: {rel['SchemaName']}")
client.tables.delete_relationship(result['relationship_id'])
```

For a complete working example, see [examples/advanced/relationships.py](examples/advanced/relationships.py).

---

## File operations

```python
# Pass the entity class instead of a string for the table name
client.files.upload(
    Account,         # Entity class instead of "account"
    account_id,
    "new_Document",  # File column name
    "/path/to/document.pdf",
    mime_type="application/pdf",
)
```

---

## Batch operations

All `client.batch.records` and `client.batch.tables` methods accept an entity class wherever a table string is expected.

```python
# Build a batch with entity classes as table references
batch = client.batch.new()
batch.records.create(Account, {"name": "Contoso"})
batch.records.create(Account, [{"name": "Fabrikam"}, {"name": "Woodgrove"}])
batch.records.update(Account, account_id, {"telephone1": "555-0100"})
batch.records.delete(Account, old_id)
batch.records.get(Account, account_id, select=["name"])

result = batch.execute()
for item in result.responses:
    if item.is_success:
        print(f"[OK] {item.status_code}  entity_id={item.entity_id}")
    else:
        print(f"[ERR] {item.status_code}: {item.error_message}")
```

**Transactional changeset:**

```python
batch = client.batch.new()
with batch.changeset() as cs:
    contact_ref = cs.records.create(Contact, {"firstname": "Ada", "lastname": "Lovelace"})
    cs.records.create(Account, {
        "name": "Babbage & Co.",
        "primarycontactid@odata.bind": contact_ref,
    })
result = batch.execute()
print(f"Created {len(result.entity_ids)} records atomically")
```

**Table metadata in a batch:**

```python
batch = client.batch.new()
batch.tables.get(Account)
batch.tables.add_columns(Account, {"new_Rating": "int"})
batch.query.sql("SELECT TOP 5 name FROM account")
result = batch.execute()
```

**Continue on error:**

```python
result = batch.execute(continue_on_error=True)
print(f"Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
for item in result.failed:
    print(f"[ERR] {item.status_code}: {item.error_message}")
```

For a complete batch example, see [examples/advanced/batch.py](examples/advanced/batch.py).

---

## Payload helpers

These methods return a new instance of the same type, filtered to only the fields appropriate for each operation.

```python
order = Order(
    new_orderid="some-guid",     # writable_on_create=False, writable_on_update=False
    new_title="Q1 Purchase",
    new_quantity=10,
)

# to_create_payload() — strips writable_on_create=False fields
create_dict = order.to_create_payload().as_dict()
# {"new_title": "Q1 Purchase", "new_quantity": 10}

# to_update_payload() — strips writable_on_update=False fields
update_dict = order.to_update_payload().as_dict()
# {"new_title": "Q1 Purchase", "new_quantity": 10}

# as_dict() — returns all fields that have been set
full_dict = order.as_dict()
# {"new_orderid": "some-guid", "new_title": "Q1 Purchase", "new_quantity": 10}
```

`from_dict()` hydrates a typed instance from a plain dictionary (e.g. an OData response):

```python
raw = {"new_title": "Q1 Purchase", "new_quantity": 10, "new_status": 2}
order = Order.from_dict(raw)
print(order.new_title)    # "Q1 Purchase"
print(type(order))        # <class 'Order'>
```

---

## Entity introspection

`fields()` returns all field descriptors defined on a class, traversing the MRO so subclass descriptors override parent ones:

```python
for attr_name, descriptor in Order.fields().items():
    print(
        f"{attr_name:<30} {type(descriptor).__name__:<20}"
        f"  create={'rw' if getattr(descriptor, 'writable_on_create', True) else 'r-'}"
        f"  update={'rw' if getattr(descriptor, 'writable_on_update', True) else 'r-'}"
    )
```

Example output:

```
new_orderid                    Guid                  create=r-  update=r-
new_title                      Text                  create=rw  update=rw
new_quantity                   Integer               create=rw  update=rw
new_totalamount                Money                 create=rw  update=rw
new_status                     OrderStatus           create=rw  update=rw
```

---

## Code generator

The SDK includes a code generator that connects to a live Dataverse org and generates typed entity classes for selected tables. This is the recommended way to get typed classes for standard Dataverse tables (account, contact, systemuser, etc.) or any table in your org.

```bash
python -m PowerPlatform.Dataverse.generator \
    --url https://yourorg.crm.dynamics.com \
    --tables account contact systemuser \
    --output ./Types
```

The generator produces:
- One `.py` file per table in `./Types/`, each containing a typed `Entity` subclass
- One `.py` file per picklist in `./Types/picklists/`
- One `.py` file per boolean option set in `./Types/booleans/`
- A `./Types/__init__.py` that re-exports all generated classes

Import and use immediately:

```python
from Types import Account, Contact

with DataverseClient(url, credential) as client:
    account: Account = client.records.get(Account, account_id)
    print(account.name)
    print(account.telephone1)
```

Re-run the generator whenever your table schema changes to regenerate classes with updated fields.
