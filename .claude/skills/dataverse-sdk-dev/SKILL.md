---
name: dataverse-sdk-dev
description: Development guidance for contributing to the PowerPlatform Dataverse Client Python SDK repository. Use when working on SDK development tasks like adding features, fixing bugs, or writing tests.
---

# Dataverse SDK Development Guide

## Overview

This skill provides guidance for developers working on the PowerPlatform Dataverse Client Python SDK repository itself (not using the SDK).

## Best Practices

### API Design

1. **client.py** - client.py only contains public API methods and all public methods must be in client.py
2. **Every public method needs README example** - Public API methods must have examples in README.md
3. **Reuse existing APIs** - Always check if an existing method can be used before making direct Web API calls
4. **Update documentation** when adding features - Keep README and SKILL files (both copies) in sync
5. **Consider backwards compatibility** - Avoid breaking changes

### Code Style

6. **No emojis** - Do not use emoji in code, comments, or output
7. **Standardize output format** - Use `[INFO]`, `[WARN]`, `[ERR]`, `[OK]` prefixes for console output
8. **No noqa comments** - Do not add `# noqa: BLE001` or similar linter suppression comments
9. **Document public APIs** - Add Sphinx-style docstrings with examples for public methods
10. **Define __all__ in module files, not __init__.py** - Use `__all__` to control exports in the actual module file (e.g., errors.py), not in `__init__.py`.