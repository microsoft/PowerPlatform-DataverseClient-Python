---
name: dataverse-sdk-dev
description: Development guidance for contributing to the PowerPlatform Dataverse Client Python SDK repository. Use when working on SDK development tasks like adding features, fixing bugs, or writing tests.
---

# Dataverse SDK Development Guide

## Overview

This skill provides guidance for developers working on the PowerPlatform Dataverse Client Python SDK repository itself (not using the SDK).

## Best Practices

### API Design

1. **Public methods in operation namespaces** - New public methods go in the appropriate namespace module under `src/PowerPlatform/Dataverse/operations/` (`records.py`, `query.py`, `tables.py`, `batch.py`). The `client.py` file exposes these via namespace properties (`client.records`, `client.query`, `client.tables`, `client.batch`). Public types and constants live in their own modules (e.g., `models/metadata.py`, `models/batch.py`, `common/constants.py`)
2. **Every public method needs README example** - Public API methods must have examples in README.md
3. **Reuse existing APIs** - Always check if an existing method can be used before making direct Web API calls
4. **Update documentation** when adding features - Keep README and SKILL files (both copies) in sync
5. **Consider backwards compatibility** - Avoid breaking changes
6. **Internal vs public naming** - Modules, files, and functions not meant to be part of the public API must use a `_` prefix (e.g., `_odata.py`, `_relationships.py`). Files without the prefix (e.g., `constants.py`, `metadata.py`) are public and importable by SDK consumers

### Code Style

6. **No emojis** - Do not use emoji in code, comments, or output
7. **Standardize output format** - Use `[INFO]`, `[WARN]`, `[ERR]`, `[OK]` prefixes for console output
8. **No noqa comments** - Do not add `# noqa: BLE001` or similar linter suppression comments
9. **Document public APIs** - Add Sphinx-style docstrings with examples for public methods
10. **Define __all__ in module files** - Each module declares its own exports via `__all__` (e.g., `errors.py` defines `__all__ = ["HttpError", ...]`). Package `__init__.py` files should not re-export or redefine another module's `__all__`; they use `__all__ = []` to indicate no star-import exports.
11. **Run black before committing** - Always run `python -m black <changed files>` before committing. CI will reject unformatted code. Config is in `pyproject.toml` under `[tool.black]`.

### Docstring Type Annotations (Microsoft Learn Compatibility)

This SDK's API reference is published on Microsoft Learn. The Learn doc pipeline parses `:type:` and `:rtype:` directives differently from standard Sphinx -- every word between `:class:` references is treated as a separate cross-reference (`<xref:word>`). Using Sphinx-style `:class:\`list\` of :class:\`str\`` produces broken `<xref:of>` links on Learn.

**Rules for `:type:` and `:rtype:` directives:**

- Use Python bracket notation for generic types: `list[str]`, `dict[str, typing.Any]`, `list[dict]`
- Use `or` (without `:class:`) for union types: `str or None`, `dict or list[dict]`
- Use bracket nesting for complex types: `collections.abc.Iterable[list[dict]]`
- Use `~` prefix for SDK types to show short name: `list[~PowerPlatform.Dataverse.models.record.Record]`
- `:class:` is fine for single standalone types: `:class:\`str\``, `:class:\`bool\``

**Never** use `:class:\`X\` of :class:\`Y\`` or `:class:\`X\` mapping :class:\`Y\` to :class:\`Z\`` -- the words `of`, `mapping`, `to` become broken `<xref:>` links.

**Correct examples:**

```rst
:type data: dict or list[dict]
:rtype: list[str]
:rtype: collections.abc.Iterable[list[~PowerPlatform.Dataverse.models.record.Record]]
:type select: list[str] or None
:type columns: dict[str, typing.Any]
```

**Wrong examples (NEVER use):**

```rst
:type data: :class:`dict` or :class:`list` of :class:`dict`
:rtype: :class:`list` of :class:`str`
:type columns: :class:`dict` mapping :class:`str` to :class:`typing.Any`
```
