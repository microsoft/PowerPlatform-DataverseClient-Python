# Spec: Support Module-Level Exports via `__all__`

## Goal

Populate the `__all__` lists in each package-level `__init__.py` so that public symbols
are re-exported at the package level. Users will be able to import from the package
namespace directly rather than reaching into submodules.

**Before:**
```python
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.core.errors import DataverseError
```

**After:**
```python
from PowerPlatform.Dataverse.models import Record
from PowerPlatform.Dataverse.core import DataverseError
```

---

## Current Status

`__all__` is already defined in every individual module (e.g. `models/filters.py`,
`core/errors.py`, `operations/records.py`), but all package-level `__init__.py` files
have empty exports:

| Package `__init__.py` | Current `__all__` |
|---|---|
| `PowerPlatform.Dataverse.models` | `[]` |
| `PowerPlatform.Dataverse.operations` | `[]` |
| `PowerPlatform.Dataverse.core` | `[]` |
| `PowerPlatform.Dataverse.data` | `[]` |

---

## The Challenge: Documentation Duplication Risk

The public API docs on Microsoft Learn are auto-generated from the installed package.
The concern is that re-exporting a class in `__init__.py` could cause it to appear
twice in the docs — once at its definition location (e.g. `operations.records.RecordOperations`)
and again at the package level (e.g. `operations.RecordOperations`).

**What we need to verify before merging:**
- [ ] Confirm with the team how the doc pipeline works and run a test build to check
      for duplicate entries.

---

## What Needs to Change

### `models/__init__.py`
Re-export from:
- `models.query_builder` → `QueryBuilder`, `QueryParams`, `ExpandOption`
- `models.filters` → `eq`, `ne`, `gt`, `lt`, `ge`, `le`, `contains`, `startswith`, `endswith`, `filter_in`, `between`, `and_`, `or_`, `not_`
- `models.batch` → `BatchItemResponse`, `BatchResult`
- `models.record` → `Record`
- `models.table_info` → `TableInfo`, `ColumnInfo`, `AlternateKeyInfo`
- `models.relationship` → `OneToManyRelationship`, `ManyToManyRelationship`, `RelationshipInfo` (etc.)
- `models.upsert` → `UpsertItem`
- `models.labels` → `LocalizedLabel`, `Label`

### `core/__init__.py`
Re-export from:
- `core.errors` → `DataverseError`, `HttpError`, `ValidationError`, `MetadataError`, `SQLParseError`
- `core.log_config` → `LogConfig`

### `operations/__init__.py`
Re-export from:
- `operations.records` → `RecordOperations`
- `operations.tables` → `TableOperations`
- `operations.query` → `QueryOperations`
- `operations.batch` → `BatchOperations`, `BatchRecordOperations`, `BatchTableOperations`
- `operations.dataframe` → `DataFrameOperations`
- `operations.files` → `FileOperations`

### `data/__init__.py`
No change — all submodules are internal (`_`-prefixed); `__all__` stays empty.

---

## Benefits

1. **Cleaner import paths** — users write `from PowerPlatform.Dataverse.models import Record`
   instead of navigating submodule paths.

2. **IDE discoverability** — autocompletion on `PowerPlatform.Dataverse.models.` surfaces
   all public types immediately; users do not need to know submodule names.

3. **No broken imports during refactoring** — if we ever rename or reorganise an internal
   submodule, users' import paths stay the same as long as the `__init__.py` re-exports
   are kept. Without this, any internal restructuring is a breaking change for users.

4. **Wildcard imports work correctly** — currently `from PowerPlatform.Dataverse.models import *`
   imports nothing, because `__all__ = []`. Once populated, wildcard imports pick up all
   intended public symbols as defined by Python's module documentation.

5. **Follows industry convention** — NumPy, pandas, and requests all expose their public
   API at the package level via `__all__` in `__init__.py`. Aligning with this pattern
   makes the SDK feel familiar to experienced Python users.
