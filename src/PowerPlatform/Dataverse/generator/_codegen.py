# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Python source-code generation for typed Dataverse entity classes.

Takes entity + attribute metadata dicts (as returned by :mod:`._fetch`) and
produces ready-to-write Python source files.
"""

from __future__ import annotations

import keyword
import re
from typing import Any, Dict, List, Optional, Tuple

from ._fetch import resolve_attr_type

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_class_name(schema_name: str) -> str:
    """Convert a Dataverse ``SchemaName`` to a valid Python class name.

    Most schema names are already PascalCase (``Account``, ``new_MyTable``).
    We strip underscores only from the *publisher prefix* (``new_``), keeping
    the rest intact so the name matches the Dataverse schema name visually.

    Examples::

        Account          → Account
        new_MyTable      → New_MyTable   # prefix uppercased, underscore kept
        new_walkthroughdemo → New_Walkthroughdemo
    """
    # If already valid identifier, use as-is (most system tables)
    if schema_name.isidentifier() and not keyword.iskeyword(schema_name):
        return schema_name

    # Replace any character that isn't alphanumeric or underscore
    clean = re.sub(r"[^A-Za-z0-9_]", "_", schema_name)
    # Ensure it doesn't start with a digit
    if clean and clean[0].isdigit():
        clean = "_" + clean
    return clean or "_Entity"


def _safe_attr_name(logical_name: str) -> str:
    """Return a safe Python identifier for a field attribute name.

    Uses the logical name directly; falls back to a ``_`` prefix if it
    clashes with a Python keyword.
    """
    if keyword.iskeyword(logical_name):
        return logical_name + "_"
    return logical_name


def _field_line(
    attr_name: str,
    logical_name: str,
    schema_name: str,
    python_type: str,
    dataverse_type: Optional[str],
    is_primary_key: bool,
) -> str:
    """Render a single ``Field(...)`` line for a class body."""
    args = [f'"{logical_name}"', python_type]

    # schema_name kwarg only when it differs from logical_name (case-sensitive)
    if schema_name and schema_name != logical_name:
        args.append(f'schema_name="{schema_name}"')

    # dataverse_type kwarg — omitted for primary keys and when None
    if not is_primary_key and dataverse_type is not None:
        args.append(f'dataverse_type="{dataverse_type}"')

    args_str = ", ".join(args)

    # Align: pad attr_name to column width for readability
    return f"    {attr_name} = Field({args_str})"


# ---------------------------------------------------------------------------
# Per-entity source generation
# ---------------------------------------------------------------------------

def generate_entity_source(
    entity: Dict[str, Any],
    attributes: List[Dict[str, Any]],
    *,
    include_nav_fields: bool = True,
) -> str:
    """Generate the full Python source for one entity class file.

    :param entity: Entity metadata dict (``LogicalName``, ``SchemaName``,
        ``EntitySetName``, ``PrimaryIdAttribute``, …).
    :param attributes: List of attribute metadata dicts from the
        ``Attributes`` endpoint.
    :param include_nav_fields: If ``True``, emit commented ``NavField``
        stubs for lookup columns so developers can uncomment them when
        they add expand support.
    :returns: Python source code string, ready to write to a ``.py`` file.
    """
    logical_name    = entity.get("LogicalName", "")
    schema_name     = entity.get("SchemaName", logical_name)
    primary_key     = entity.get("PrimaryIdAttribute", "")

    class_name = _to_class_name(schema_name)

    # Build ordered field lines
    field_lines: List[str] = []
    nav_stubs:  List[str] = []

    # Track max attr_name length for alignment pass
    rows: List[Tuple[str, str]] = []  # (attr_name_padded, rest_of_line)

    for attr in attributes:
        attr_logical = attr.get("LogicalName", "")
        attr_schema  = attr.get("SchemaName", attr_logical)

        result = resolve_attr_type(attr)
        if result is None:
            continue  # skip image / file / virtual

        python_type, dv_type = result

        if python_type == "Any":
            # Unknown type — still emit but with a comment
            attr_name = _safe_attr_name(attr_logical)
            rows.append((attr_name, f'Field("{attr_logical}", Any)  # unmapped type: {attr.get("@odata.type", "?")}'))
            continue

        attr_name = _safe_attr_name(attr_logical)
        is_pk = attr_logical == primary_key

        line = _field_line(attr_name, attr_logical, attr_schema, python_type, dv_type, is_pk)
        rows.append((attr_name, line[len(f"    {attr_name} = "):]))  # store suffix only

        if include_nav_fields and dv_type == "lookup":
            # Emit a commented NavField stub — the developer can uncomment and
            # set the correct OData navigation property name.
            nav_prop = attr_logical[:-2] if attr_logical.endswith("id") else attr_logical
            nav_stubs.append(
                f"    # {attr_name}_nav = NavField(\"{nav_prop}\")  "
                f"# uncomment + set nav-property name to enable .expand()"
            )

    # Alignment: find the longest attr name in rows
    if rows:
        max_len = max(len(r[0]) for r in rows)
    else:
        max_len = 0

    for attr_name, suffix in rows:
        padding = " " * (max_len - len(attr_name))
        field_lines.append(f"    {attr_name}{padding} = {suffix}")

    # Assemble source
    lines = [
        "# Auto-generated by PowerPlatform.Dataverse.generator — do not edit by hand.",
        f"# Entity : {logical_name}  (SchemaName: {schema_name})",
        "from __future__ import annotations",
        "",
        "from typing import Any",
        "",
        "from PowerPlatform.Dataverse.models.entity import Entity, Field, NavField",
        "",
        "",
        f'class {class_name}(Entity, table="{logical_name}", primary_key="{primary_key}"):',
        f'    """Typed entity class for the ``{logical_name}`` Dataverse table."""',
        "",
    ]

    if field_lines:
        lines.extend(field_lines)
    else:
        lines.append("    pass")

    if nav_stubs:
        lines.append("")
        lines.append("    # --- Navigation properties (expand) ---")
        lines.extend(nav_stubs)

    lines.append("")  # trailing newline
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# __init__.py for the output package
# ---------------------------------------------------------------------------

def generate_init_source(class_entries: List[Tuple[str, str]]) -> str:
    """Generate the ``__init__.py`` that re-exports all generated classes.

    :param class_entries: List of ``(module_name, class_name)`` tuples, e.g.
        ``[("account", "Account"), ("contact", "Contact")]``.
    """
    lines = [
        "# Auto-generated by PowerPlatform.Dataverse.generator — do not edit by hand.",
        "# Re-exports all entity classes for convenient ``from Types import Account``.",
        "from __future__ import annotations",
        "",
    ]
    for module, cls in sorted(class_entries, key=lambda x: x[1]):
        lines.append(f"from .{module} import {cls}")

    lines.append("")
    lines.append("__all__ = [")
    for _, cls in sorted(class_entries, key=lambda x: x[1]):
        lines.append(f'    "{cls}",')
    lines.append("]")
    lines.append("")
    return "\n".join(lines)
