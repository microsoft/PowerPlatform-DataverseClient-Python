---
name: dataverse-sdk-dev
description: Development guidance for contributing to the PowerPlatform Dataverse Client Python SDK repository. Use when working on SDK development tasks like adding features, fixing bugs, or writing tests.
---

# Dataverse SDK Development Guide

## Overview

This skill provides guidance for developers working on the PowerPlatform Dataverse Client Python SDK repository itself (not using the SDK).

## Best Practices

### API Design

1. **Public methods in operation namespaces** - New public methods go in the appropriate namespace module under `src/PowerPlatform/Dataverse/operations/` (`records.py`, `query.py`, `tables.py`). The `client.py` file exposes these via namespace properties (`client.records`, `client.query`, `client.tables`). Public types and constants live in their own modules (e.g., `models/metadata.py`, `common/constants.py`)
2. **Every public method needs README example** - Public API methods must have examples in README.md
3. **Reuse existing APIs** - Always check if an existing method can be used before making direct Web API calls
4. **Update documentation** when adding features - Keep README and SKILL files (both copies) in sync
5. **Consider backwards compatibility** - Avoid breaking changes
6. **Internal vs public naming** - Modules, files, and functions not meant to be part of the public API must use a `_` prefix (e.g., `_odata.py`, `_relationships.py`). Files without the prefix (e.g., `constants.py`, `metadata.py`) are public and importable by SDK consumers

### Dataverse Property Naming Rules

Dataverse uses two different naming conventions for properties. Getting this wrong causes 400 errors that are hard to debug.

| Property type | Name convention | Example | When used |
|---|---|---|---|
| **Structural** (columns) | LogicalName (always lowercase) | `new_name`, `new_priority` | `$select`, `$filter`, `$orderby`, record payload keys |
| **Navigation** (relationships / lookups) | Navigation Property Name (usually SchemaName, PascalCase, case-sensitive) | `new_CustomerId`, `new_AgentId` | `$expand`, `@odata.bind` annotation keys |

Navigation property names are case-sensitive and must match the entity's `$metadata`. Using the logical name instead of the navigation property name results in 400 Bad Request errors.

**Critical rule:** The OData parser validates `@odata.bind` property names **case-sensitively** against declared navigation properties. Lowercasing `new_CustomerId@odata.bind` to `new_customerid@odata.bind` causes: `ODataException: An undeclared property 'new_customerid' which only has property annotations...`

**SDK implementation:**

- `_lowercase_keys()` lowercases all keys EXCEPT those containing `@odata.` (preserves navigation property casing in `@odata.bind` keys)
- `_lowercase_list()` lowercases `$select` and `$orderby` params (structural properties)
- `$expand` params are passed as-is (navigation properties, PascalCase)
- `_convert_labels_to_ints()` skips `@odata.` keys entirely (they are annotations, not attributes)

**When adding new code that processes record dicts or builds query parameters:**

- Always use `_lowercase_keys()` for record payloads. Never manually call `.lower()` on all keys
- Never lowercase `$expand` values or `@odata.bind` key prefixes
- If iterating record keys, skip keys containing `@odata.` when doing attribute-level operations

### Code Style

6. **No emojis** - Do not use emoji in code, comments, or output
7. **Standardize output format** - Use `[INFO]`, `[WARN]`, `[ERR]`, `[OK]` prefixes for console output
8. **No noqa comments** - Do not add `# noqa: BLE001` or similar linter suppression comments
9. **Document public APIs** - Add Sphinx-style docstrings with examples for public methods
10. **Define __all__ in module files** - Each module declares its own exports via `__all__` (e.g., `errors.py` defines `__all__ = ["HttpError", ...]`). Package `__init__.py` files should not re-export or redefine another module's `__all__`; they use `__all__ = []` to indicate no star-import exports.
11. **Run black before committing** - Always run `python -m black <changed files>` before committing. CI will reject unformatted code. Config is in `pyproject.toml` under `[tool.black]`.
