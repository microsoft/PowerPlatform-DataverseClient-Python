# Integrating Relationship Metadata API with SDK Redesign

**Document Date:** January 2026
**Branches Compared:**
- `feature/querybuilder` - SDK redesign implementation (Priorities 1-5)
- `feature/relationship-metadata-api` - Relationship metadata operations

---

## Executive Summary

This document analyzes the gaps between the SDK redesign (namespaced operations, `OperationResult` wrapping, structured models) and the parallel relationship metadata API implementation. It provides recommendations for bringing both efforts into alignment.

### Key Findings

| Aspect | SDK Redesign (`feature/querybuilder`) | Relationships (`feature/relationship-metadata-api`) | Gap |
|--------|---------------------------------------|-----------------------------------------------------|-----|
| API style | Namespaced (`client.tables.create()`) | Flat (`client.create_one_to_many_relationship()`) | **Misaligned** |
| Return wrapping | `OperationResult` with telemetry | Raw `dict` / `None` | **Misaligned** |
| Parameter style | Keyword-only optionals, short names | Positional optionals, verbose names | **Misaligned** |
| Internal telemetry | Methods return `(result, telemetry)` tuples | Methods return result only | **Misaligned** |
| Data models | `Record`, `TableInfo` dataclasses | New `*Metadata` dataclasses | Aligned (good) |
| Design spec coverage | Documented in `sdk_design_recs.md` | Not documented | **Missing** |

---

## Analysis

### 1. API Style Divergence

The SDK redesign organizes operations under intuitive namespaces:

| Namespace | Purpose |
|-----------|---------|
| `client.records` | Record CRUD operations |
| `client.query` | Query and SQL operations |
| `client.tables` | Table metadata (create, delete, columns) |

The relationships branch adds methods directly to `DataverseClient`:

```python
# Current relationships API (flat)
client.create_one_to_many_relationship(lookup, relationship)
client.create_many_to_many_relationship(relationship)
client.delete_relationship(relationship_id)
client.get_relationship(schema_name)
client.create_lookup_field(...)  # convenience method
```

This creates an inconsistent developer experience where some operations are namespaced and others are not.

### 2. Missing `OperationResult` Wrapping

All SDK redesign methods return `OperationResult[T]` which provides:
- Direct result access (iteration, indexing)
- Telemetry via `.with_response_details()` (request IDs, correlation IDs)

The relationships API returns raw types:

| Method | Current Return | Expected Pattern |
|--------|---------------|------------------|
| `create_one_to_many_relationship()` | `Dict[str, Any]` | `OperationResult[RelationshipInfo]` |
| `create_many_to_many_relationship()` | `Dict[str, Any]` | `OperationResult[RelationshipInfo]` |
| `delete_relationship()` | `None` | `OperationResult[None]` |
| `get_relationship()` | `Dict[str, Any] \| None` | `OperationResult[RelationshipInfo \| None]` |

### 3. Parameter Style Inconsistencies

The SDK redesign uses keyword-only optional parameters and concise naming:

```python
# SDK redesign pattern (tables.create)
def create(
    self,
    table: str,
    columns: Dict[str, Any],
    *,  # keyword-only after this
    solution: Optional[str] = None,
    primary_column: Optional[str] = None,
) -> OperationResult[TableInfo]:
```

The relationships API uses positional optionals and verbose names:

```python
# Current relationships pattern
def create_one_to_many_relationship(
    self,
    lookup: LookupAttributeMetadata,
    relationship: OneToManyRelationshipMetadata,
    solution_unique_name: Optional[str] = None,  # positional, verbose
) -> Dict[str, Any]:
```

### 4. Internal Telemetry Capture

The SDK redesign's internal `_odata.py` methods return `(result, RequestTelemetryData)` tuples, enabling telemetry propagation. The `_relationships.py` mixin methods return results directly without telemetry:

```python
# Current _relationships.py
def _create_one_to_many_relationship(...) -> Dict[str, Any]:
    ...
    return {"relationship_id": relationship_id, ...}

# SDK pattern in _odata.py
def _create_table(...) -> Tuple[Dict[str, Any], RequestTelemetryData]:
    ...
    return result, self._get_telemetry_data()
```

---

## Recommendations

### For the Relationships Branch

#### 1. Add Methods to `client.tables` Namespace

Relationship operations are table schema/metadata operations and belong in the existing `tables` namespace. This keeps the API surface minimal (3 namespaces) and groups all schema operations together:

```python
# Recommended API - extend client.tables
result = client.tables.create_one_to_many(lookup, relationship)
result = client.tables.create_many_to_many(relationship)
client.tables.delete_relationship(relationship_id)
info = client.tables.get_relationship(schema_name)

# Convenience method
result = client.tables.create_lookup_field(
    referencing_table="new_order",
    lookup_field_name="new_AccountId",
    referenced_table="account"
)
```

**Why `client.tables` over a new namespace:**
- Relationships define table schema - they're metadata operations
- Keeps namespace count at 3 (`records`, `query`, `tables`)
- Consistent with how `add_columns()` and `remove_columns()` live in `tables`
- Avoids proliferating top-level namespaces

#### 2. Use Keyword-Only Optional Parameters

Add `*` separator to enforce keyword-only optional parameters:

```python
# Before
def create_one_to_many_relationship(
    self,
    lookup: LookupAttributeMetadata,
    relationship: OneToManyRelationshipMetadata,
    solution_unique_name: Optional[str] = None,
)

# After
def create_one_to_many(
    self,
    lookup: LookupAttributeMetadata,
    relationship: OneToManyRelationshipMetadata,
    *,
    solution: Optional[str] = None,
)
```

#### 3. Shorten Parameter Names

Align with SDK naming conventions:

| Current | Recommended |
|---------|-------------|
| `solution_unique_name` | `solution` |

#### 4. Update Internal Methods for Telemetry

Modify `_RelationshipOperationsMixin` methods to return telemetry tuples:

```python
# In _relationships.py
def _create_one_to_many_relationship(
    self,
    lookup,
    relationship,
    solution: Optional[str] = None,
) -> Tuple[Dict[str, Any], RequestTelemetryData]:
    ...
    r = self._request("post", url, headers=headers, json=payload)
    result = {
        "relationship_id": relationship_id,
        "relationship_schema_name": relationship.schema_name,
        ...
    }
    return result, self._get_telemetry_data()
```

#### 5. Wrap Returns in `OperationResult`

Follow the universal wrapping pattern:

```python
# In tables.py
def create_one_to_many(
    self,
    lookup: LookupAttributeMetadata,
    relationship: OneToManyRelationshipMetadata,
    *,
    solution: Optional[str] = None,
) -> OperationResult[RelationshipInfo]:
    with self._client._scoped_odata() as od:
        result, telemetry = od._create_one_to_many_relationship(
            lookup, relationship, solution
        )
        return OperationResult(RelationshipInfo.from_dict(result), telemetry)
```

#### 6. Introduce Structured Return Model

Create a `RelationshipInfo` dataclass to match the pattern of `TableInfo`:

```python
@dataclass
class RelationshipInfo:
    """Relationship metadata returned from create/get operations."""
    relationship_id: str
    schema_name: str
    relationship_type: str  # "OneToMany" | "ManyToMany"

    # One-to-many specific
    lookup_schema_name: Optional[str] = None
    referenced_entity: Optional[str] = None
    referencing_entity: Optional[str] = None

    # Many-to-many specific
    entity1_logical_name: Optional[str] = None
    entity2_logical_name: Optional[str] = None
    intersect_entity_name: Optional[str] = None

    # Dict-like access for backward compatibility
    def __getitem__(self, key: str) -> Any:
        return self._to_dict()[key]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RelationshipInfo":
        ...
```

#### 7. Simplify Method Names

Since the namespace provides context, method names can drop redundant words:

| Current (flat) | Recommended (in `tables`) |
|----------------|---------------------------|
| `create_one_to_many_relationship()` | `tables.create_one_to_many()` |
| `create_many_to_many_relationship()` | `tables.create_many_to_many()` |
| `delete_relationship()` | `tables.delete_relationship()` |
| `get_relationship()` | `tables.get_relationship()` |
| `create_lookup_field()` | `tables.create_lookup_field()` |

#### 8. Add Legacy Flat Methods with Deprecation Warnings

For backward compatibility during transition:

```python
# In client.py
def create_one_to_many_relationship(
    self,
    lookup: LookupAttributeMetadata,
    relationship: OneToManyRelationshipMetadata,
    solution_unique_name: Optional[str] = None,
) -> OperationResult[RelationshipInfo]:
    """
    .. deprecated::
        Use ``client.tables.create_one_to_many()`` instead.
    """
    warnings.warn(
        "client.create_one_to_many_relationship() is deprecated. "
        "Use client.tables.create_one_to_many() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return self.tables.create_one_to_many(
        lookup, relationship, solution=solution_unique_name
    )
```

---

### For the Design Spec

#### 1. Add Relationship Operations Section

Add to `implementation_plan.md` as Priority 6:

```markdown
## Priority 6: Relationship Operations

**Goal:** Add relationship management methods to `client.tables` namespace.

### Methods

| Method | Parameters | Returns |
|--------|------------|---------|
| `create_one_to_many()` | `lookup`, `relationship`, `*, solution?` | `OperationResult[RelationshipInfo]` |
| `create_many_to_many()` | `relationship`, `*, solution?` | `OperationResult[RelationshipInfo]` |
| `delete_relationship()` | `relationship_id` | `OperationResult[None]` |
| `get_relationship()` | `schema_name` | `OperationResult[RelationshipInfo \| None]` |
| `create_lookup_field()` | (convenience parameters) | `OperationResult[RelationshipInfo]` |
```

#### 2. Document Relationship Metadata Types

Add section documenting the input dataclasses:

| Type | Purpose | Key Fields |
|------|---------|------------|
| `LookupAttributeMetadata` | Defines lookup column | `schema_name`, `display_name`, `required_level` |
| `OneToManyRelationshipMetadata` | Defines 1:N relationship | `schema_name`, `referenced_entity`, `referencing_entity`, `cascade_configuration` |
| `ManyToManyRelationshipMetadata` | Defines N:N relationship | `schema_name`, `entity1_logical_name`, `entity2_logical_name` |
| `CascadeConfiguration` | Cascade behavior rules | `assign`, `delete`, `merge`, `reparent`, `share`, `unshare` |
| `Label`, `LocalizedLabel` | Multi-language labels | `label`, `language_code` |

#### 3. Update Implementation Plan

```markdown
## Phase 6: Relationship Operations

**Goal:** Add relationship management to `client.tables` namespace.

### PR 6.1: Internal Telemetry Support

**Files to Modify:**
- `src/PowerPlatform/Dataverse/data/_relationships.py` - Return `(result, telemetry)` tuples

### PR 6.2: TableOperations Extension

**Files to Create:**
- `src/PowerPlatform/Dataverse/models/relationship_info.py`

**Files to Modify:**
- `src/PowerPlatform/Dataverse/operations/tables.py` - Add relationship methods
- `src/PowerPlatform/Dataverse/client.py` - Add deprecated flat methods

### PR 6.3: Update Examples and Tests

**Files to Modify:**
- `examples/advanced/relationships.py` - Use new namespaced API
- `tests/unit/data/test_relationships.py` - Update for new signatures
```

---

## Implementation Checklist

### Changes to `feature/relationship-metadata-api`

#### Internal Layer (`_relationships.py`)
- [ ] Update `_create_one_to_many_relationship()` to return `(result, telemetry)` tuple
- [ ] Update `_create_many_to_many_relationship()` to return `(result, telemetry)` tuple
- [ ] Update `_delete_relationship()` to return `(None, telemetry)` tuple
- [ ] Update `_get_relationship()` to return `(result, telemetry)` tuple
- [ ] Rename `solution_unique_name` parameter to `solution`

#### Public API Layer (`tables.py`)
- [ ] Add `create_one_to_many()` method with keyword-only `solution` parameter
- [ ] Add `create_many_to_many()` method with keyword-only `solution` parameter
- [ ] Add `delete_relationship()` method
- [ ] Add `get_relationship()` method
- [ ] Add `create_lookup_field()` convenience method
- [ ] Wrap all returns in `OperationResult`

#### Models
- [ ] Create `models/relationship_info.py` with `RelationshipInfo` dataclass
- [ ] Add `from_dict()` factory method
- [ ] Add dict-like access for backward compatibility

#### Client (`client.py`)
- [ ] Add deprecated `create_one_to_many_relationship()` that delegates to `tables`
- [ ] Add deprecated `create_many_to_many_relationship()` that delegates to `tables`
- [ ] Add deprecated `delete_relationship()` that delegates to `tables`
- [ ] Add deprecated `get_relationship()` that delegates to `tables`
- [ ] Add deprecated `create_lookup_field()` that delegates to `tables`

#### Examples and Tests
- [ ] Update `examples/advanced/relationships.py` to use `client.tables.*` API
- [ ] Update unit tests for new method signatures and return types

### Changes to Design Spec

- [ ] Add Priority 6 section to `implementation_plan.md`
- [ ] Document relationship methods in method comparison tables
- [ ] Add `RelationshipInfo` to structured data models section

---

## Appendix: Full API Comparison

### Before Integration (Current Relationships Branch)

```python
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
)

client = DataverseClient(url, credential)

# Create one-to-many (flat method, raw dict return, no telemetry)
result = client.create_one_to_many_relationship(
    lookup,
    relationship,
    solution_unique_name="MySolution"  # positional allowed, verbose name
)
print(result["relationship_id"])  # Dict access only

# Get relationship
rel = client.get_relationship("new_Department_Employee")
if rel:
    print(rel["SchemaName"])  # Raw API response format
```

### After Integration (Recommended)

```python
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
)

with DataverseClient(url, credential) as client:
    # Create one-to-many (namespaced, OperationResult, telemetry)
    result = client.tables.create_one_to_many(
        lookup,
        relationship,
        solution="MySolution",  # keyword-only, concise name
    )
    print(result.relationship_id)     # Structured access
    print(result["relationship_id"])  # Dict-like access still works

    # With telemetry
    response = result.with_response_details()
    print(f"Request ID: {response.telemetry['client_request_id']}")

    # Convenience method
    result = client.tables.create_lookup_field(
        referencing_table="new_order",
        lookup_field_name="new_AccountId",
        referenced_table="account",
        display_name="Account",
        required=True,
    )

    # Get relationship
    info = client.tables.get_relationship("new_Department_Employee")
    if info:
        print(info.schema_name)  # Structured access
```

---

## Summary

The relationship metadata API introduces valuable functionality but diverges from the SDK redesign patterns. Integrating it requires:

| Change | Description |
|--------|-------------|
| **Namespace** | Add methods to `client.tables` (not new namespace) |
| **Return wrapping** | Wrap in `OperationResult` for telemetry |
| **Return type** | New `RelationshipInfo` dataclass |
| **Parameter naming** | `solution_unique_name` → `solution` |
| **Parameter style** | Add `*` for keyword-only optionals |
| **Internal telemetry** | Update `_relationships.py` to return tuples |
| **Backward compatibility** | Deprecated flat methods on client |

These changes align the relationship API with the established SDK patterns, providing a consistent developer experience across all operation types.
