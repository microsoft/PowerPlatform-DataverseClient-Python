#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
DV-Python-SDK v0 → v1 GA migration codemod.

Mechanically rewrites beta (0.1.0b*) call sites to their GA (1.0) equivalents
using LibCST (concrete syntax tree — preserves all whitespace and comments).

Usage::

    pip install PowerPlatform-Dataverse-Client[migration]
    python -m tools.migrate_v0_to_v1 path/to/scripts/
    python -m tools.migrate_v0_to_v1 examples/          # _codemon.py files only

Transformations applied
-----------------------
Builder methods (.filter_*  →  .where(col(...)...))::

    .filter_eq("col", v)               →  .where(col("col") == v)
    .filter_ne("col", v)               →  .where(col("col") != v)
    .filter_gt("col", v)               →  .where(col("col") > v)
    .filter_ge("col", v)               →  .where(col("col") >= v)
    .filter_lt("col", v)               →  .where(col("col") < v)
    .filter_le("col", v)               →  .where(col("col") <= v)
    .filter_contains("col", v)         →  .where(col("col").contains(v))
    .filter_startswith("col", v)       →  .where(col("col").startswith(v))
    .filter_endswith("col", v)         →  .where(col("col").endswith(v))
    .filter_in("col", vals)            →  .where(col("col").in_(vals))
    .filter_not_in("col", vals)        →  .where(col("col").not_in(vals))
    .filter_null("col")                →  .where(col("col").is_null())
    .filter_not_null("col")            →  .where(col("col").is_not_null())
    .filter_between("col", lo, hi)     →  .where(col("col").between(lo, hi))
    .filter_not_between("col", lo, hi) →  .where(col("col").not_between(lo, hi))
    .filter_raw("expr")                →  .where(raw("expr"))
    .filter("expr")                    →  .where(raw("expr"))
    .execute(by_page=True)             →  .execute_pages()
    .execute(by_page=False)            →  .execute()  (flag removed)

Record namespace::

    batch.records.get(t, id)     →  batch.records.retrieve(t, id)

Top-level shortcuts (removed at GA)::

    client.create(t, d)           →  client.records.create(t, d)
    client.update(t, id, d)       →  client.records.update(t, id, d)
    client.delete(t, id)          →  client.records.delete(t, id)
    client.get(t, id)             →  client.records.retrieve(t, id)
    client.query_sql(sql)         →  client.query.sql(sql)
    client.get_table_info(t)      →  client.tables.get(t)
    client.create_table(t, …)     →  client.tables.create(t, …)
    client.delete_table(t)        →  client.tables.delete(t)
    client.list_tables()          →  client.tables.list()
    client.create_columns(t, …)   →  client.tables.add_columns(t, …)
    client.delete_columns(t, …)   →  client.tables.remove_columns(t, …)
    client.upload_file(…)         →  client.files.upload(…)

Import management:
    Adds ``from PowerPlatform.Dataverse.models.filters import col`` when a
    .filter_* method is rewritten (if col is not already imported).
    Adds ``raw`` to the same import when .filter_raw or .filter is rewritten.

NOT handled by this codemod (manual migration required):
    execute(by_page=variable)      →  manual review required (variable argument, not literal)
    client.records.get(t, id)     →  client.records.retrieve(t, id)
        Return type changes: beta returns Record (raises on 404); GA retrieve() returns
        Record | None. Callers that do not guard against None will fail silently.
    client.records.get(t, kw=…)  →  client.records.list(t, kw=…)
        Return type changes: beta returns Iterable[List[Record]] (pages); GA list()
        returns QueryResult (flat iterable over Records). Any ``for page in result:
        for rec in page:`` iteration pattern breaks after a mechanical rename.
    client.dataframe.get()        →  client.query.builder(…).execute().to_dataframe()
        Expression reconstruction requires understanding caller intent.
    client.query.sql_select()/sql_join()/sql_joins()  →  removed (no mechanical replacement)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional, Sequence, Set

try:
    import libcst as cst
except ImportError:
    print(
        "ERROR: libcst is required. Install with:\n"
        "  pip install PowerPlatform-Dataverse-Client[migration]\n"
        "  # or: pip install 'libcst>=1.0.0'",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Filter-method → .where(col(...)) mapping
# ---------------------------------------------------------------------------

_UNARY_FILTER_MAP = {
    "filter_null": "is_null",
    "filter_not_null": "is_not_null",
}

_BINARY_OP_MAP = {
    "filter_eq": cst.Equal(),
    "filter_ne": cst.NotEqual(),
    "filter_gt": cst.GreaterThan(),
    "filter_ge": cst.GreaterThanEqual(),
    "filter_lt": cst.LessThan(),
    "filter_le": cst.LessThanEqual(),
}

_METHOD_FILTER_MAP = {
    "filter_contains": "contains",
    "filter_startswith": "startswith",
    "filter_endswith": "endswith",
    "filter_in": "in_",
    "filter_not_in": "not_in",
    "filter_between": "between",
    "filter_not_between": "not_between",
}

_ALL_FILTER_METHODS: Set[str] = set(_UNARY_FILTER_MAP) | set(_BINARY_OP_MAP) | set(_METHOD_FILTER_MAP) | {"filter_raw"}

# Standalone filter functions from filters module (beta API) → col() equivalents
# eq("f", v) → col("f") == v, between("f", lo, hi) → col("f").between(lo, hi), etc.
_FUNC_BINARY_OP_MAP = {
    "eq": cst.Equal(),
    "ne": cst.NotEqual(),
    "gt": cst.GreaterThan(),
    "ge": cst.GreaterThanEqual(),
    "lt": cst.LessThan(),
    "le": cst.LessThanEqual(),
}
_FUNC_METHOD_MAP = {
    "contains": "contains",
    "startswith": "startswith",
    "endswith": "endswith",
    "filter_in": "in_",
    "not_in": "not_in",
    "between": "between",
    "not_between": "not_between",
}
_FUNC_UNARY_MAP = {
    "is_null": "is_null",
    "is_not_null": "is_not_null",
}
_ALL_FILTER_FUNCS: Set[str] = set(_FUNC_BINARY_OP_MAP) | set(_FUNC_METHOD_MAP) | set(_FUNC_UNARY_MAP)

# Top-level client shortcut → (new_namespace, new_method)
_CLIENT_SHORTCUTS = {
    "create": ("records", "create"),
    "update": ("records", "update"),
    "delete": ("records", "delete"),
    "get": ("records", "retrieve"),
    "query_sql": ("query", "sql"),
    "get_table_info": ("tables", "get"),
    "create_table": ("tables", "create"),
    "delete_table": ("tables", "delete"),
    "list_tables": ("tables", "list"),
    "create_columns": ("tables", "add_columns"),
    "delete_columns": ("tables", "remove_columns"),
    "upload_file": ("files", "upload"),
}

_FILTERS_MODULE = "PowerPlatform.Dataverse.models.filters"


# ---------------------------------------------------------------------------
# Node helpers
# ---------------------------------------------------------------------------


def _name(s: str) -> cst.Name:
    return cst.Name(s)


def _attr(obj: cst.BaseExpression, attr: str) -> cst.Attribute:
    return cst.Attribute(value=obj, attr=cst.Name(attr))


def _call(func: cst.BaseExpression, *args: cst.BaseExpression) -> cst.Call:
    cst_args = []
    for i, a in enumerate(args):
        comma = (
            cst.MaybeSentinel.DEFAULT if i == len(args) - 1 else cst.Comma(whitespace_after=cst.SimpleWhitespace(" "))
        )
        cst_args.append(cst.Arg(value=a, comma=comma))
    return cst.Call(func=func, args=cst_args)


def _col_call(col_name_node: cst.BaseExpression) -> cst.Call:
    """col("field_name") call node."""
    return _call(_name("col"), col_name_node)


def _filters_module_attr() -> cst.Attribute:
    """Build the Attribute chain for PowerPlatform.Dataverse.models.filters."""
    return _attr(
        _attr(
            _attr(_name("PowerPlatform"), "Dataverse"),
            "models",
        ),
        "filters",
    )


# ---------------------------------------------------------------------------
# Positional argument helpers
# ---------------------------------------------------------------------------


def _pos_arg(args: Sequence[cst.Arg], n: int) -> Optional[cst.BaseExpression]:
    """Return the n-th (0-indexed) positional argument value, or None."""
    count = 0
    for a in args:
        if a.keyword is None:
            if count == n:
                return a.value
            count += 1
    return None


def _positional_count(args: Sequence[cst.Arg]) -> int:
    return sum(1 for a in args if a.keyword is None)


# ---------------------------------------------------------------------------
# Main transformer
# ---------------------------------------------------------------------------


class _V1Migrator(cst.CSTTransformer):
    """LibCST transformer rewriting DV-Python-SDK beta → v1 GA."""

    def __init__(self, client_var: str = "client") -> None:
        self._client_var = client_var
        self._needs_col = False
        self._needs_raw = False
        self._has_col = False
        self._has_raw = False
        # Names imported from filters module in this file (e.g. eq, gt, between)
        self._imported_filter_funcs: Set[str] = set()

    # ------------------------------------------------------------------
    # Track existing col / raw imports
    # ------------------------------------------------------------------

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        if isinstance(node.names, cst.ImportStar):
            return
        module_str = _dotted_name(node.module)
        if module_str != _FILTERS_MODULE:
            return
        for alias in node.names:
            name = alias.name.value if isinstance(alias.name, cst.Name) else ""
            if name == "col":
                self._has_col = True
            elif name == "raw":
                self._has_raw = True
            elif name in _ALL_FILTER_FUNCS:
                self._imported_filter_funcs.add(name)

    # ------------------------------------------------------------------
    # Rewrite call nodes
    # ------------------------------------------------------------------

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        func = updated_node.func

        # ----------------------------------------------------------------
        # Standalone filter functions: eq("f", v) → col("f") == v, etc.
        # Only transform names that were actually imported from filters module.
        # Wrap Comparison nodes in explicit parentheses so that combining with
        # & / | doesn't hit Python precedence bugs (& binds tighter than ==/>).
        # ----------------------------------------------------------------
        if isinstance(func, cst.Name) and func.value in self._imported_filter_funcs:
            result = self._build_filter_func_arg(func.value, updated_node.args)
            if result is not None:
                if isinstance(result, cst.Comparison):
                    result = result.with_changes(lpar=[cst.LeftParen()], rpar=[cst.RightParen()])
                return result

        if not isinstance(func, cst.Attribute):
            return updated_node

        method_name = func.attr.value if isinstance(func.attr, cst.Name) else ""

        # ----------------------------------------------------------------
        # .filter_*(...) → .where(col(...) ...)
        # ----------------------------------------------------------------
        if method_name in _ALL_FILTER_METHODS:
            where_arg = self._build_filter_arg(method_name, updated_node.args)
            if where_arg is not None:
                return updated_node.with_changes(
                    func=func.with_changes(attr=_name("where")),
                    args=[cst.Arg(value=where_arg)],
                )

        # ----------------------------------------------------------------
        # .filter("expr") → .where(raw("expr"))
        # QueryBuilder.filter() was removed at GA (not deprecated). Wrapping
        # in raw() preserves the OData string exactly for string-literal callers.
        # ----------------------------------------------------------------
        if method_name == "filter":
            expr_node = _pos_arg(updated_node.args, 0)
            if expr_node is not None and _positional_count(updated_node.args) == 1:
                self._needs_raw = True
                return updated_node.with_changes(
                    func=func.with_changes(attr=_name("where")),
                    args=[cst.Arg(value=_call(_name("raw"), expr_node))],
                )

        # ----------------------------------------------------------------
        # .execute(by_page=True)  → .execute_pages()
        # .execute(by_page=False) → .execute()  (flag removed)
        # Only literal True/False are codemod-able; variable by_page requires
        # manual review per section 8.5 of the GA spec.
        # ----------------------------------------------------------------
        if method_name == "execute":
            by_page_val = self._kwarg_bool_literal(updated_node.args, "by_page")
            if by_page_val is True:
                return updated_node.with_changes(
                    func=func.with_changes(attr=_name("execute_pages")),
                    args=[],
                )
            if by_page_val is False:
                other_args = [
                    a
                    for a in updated_node.args
                    if not (isinstance(a.keyword, cst.Name) and a.keyword.value == "by_page")
                ]
                return updated_node.with_changes(args=other_args)

        # ----------------------------------------------------------------
        # batch.records.get(table, id) → batch.records.retrieve(table, id)
        # NOTE: client.records.get() is NOT codemodded — the return type changes
        # between beta and GA (Record | None vs Record for single-id; QueryResult vs
        # Iterable[List[Record]] for multi-record). Surrounding iteration patterns
        # would silently break after a mechanical rename.
        # ----------------------------------------------------------------
        if method_name == "get" and isinstance(func.value, cst.Attribute):
            inner = func.value
            if isinstance(inner.attr, cst.Name) and inner.attr.value == "records":
                if isinstance(inner.value, cst.Name) and inner.value.value == "batch":
                    # batch.records.get() returns None in both versions — safe to rename
                    return updated_node.with_changes(func=func.with_changes(attr=_name("retrieve")))

        # ----------------------------------------------------------------
        # client.<shortcut>(...) top-level shortcuts removed at GA
        # Only match when receiver is the known client variable name to avoid
        # false positives on record.get("field"), table_info.get("field"), etc.
        # ----------------------------------------------------------------
        if (
            isinstance(func.value, cst.Name)
            and func.value.value == self._client_var
            and method_name in _CLIENT_SHORTCUTS
        ):
            new_ns, new_method = _CLIENT_SHORTCUTS[method_name]
            new_func = _attr(_attr(func.value, new_ns), new_method)
            return updated_node.with_changes(func=new_func)

        return updated_node

    # ------------------------------------------------------------------
    # Keyword-argument helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _kwarg_bool_literal(args: Sequence[cst.Arg], keyword: str) -> Optional[bool]:
        """Return True/False if *keyword* is a literal bool kwarg, else None."""
        for a in args:
            if isinstance(a.keyword, cst.Name) and a.keyword.value == keyword:
                if isinstance(a.value, cst.Name):
                    if a.value.value == "True":
                        return True
                    if a.value.value == "False":
                        return False
        return None

    # ------------------------------------------------------------------
    # Build the argument for .where() from .filter_*() args
    # ------------------------------------------------------------------

    def _build_filter_arg(
        self,
        method_name: str,
        args: Sequence[cst.Arg],
    ) -> Optional[cst.BaseExpression]:

        field_node = _pos_arg(args, 0)
        if field_node is None:
            return None

        # .filter_raw(expr) → raw(expr)
        if method_name == "filter_raw":
            self._needs_raw = True
            return _call(_name("raw"), field_node)

        # .filter_null / .filter_not_null → col("f").is_null() / .is_not_null()
        if method_name in _UNARY_FILTER_MAP:
            self._needs_col = True
            proxy = _UNARY_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy))

        # .filter_eq / .filter_ne / ... → col("f") OP val
        if method_name in _BINARY_OP_MAP:
            val_node = _pos_arg(args, 1)
            if val_node is None:
                return None
            self._needs_col = True
            return cst.Comparison(
                left=_col_call(field_node),
                comparisons=[
                    cst.ComparisonTarget(
                        operator=_BINARY_OP_MAP[method_name],
                        comparator=val_node,
                    )
                ],
            )

        # .filter_between / .filter_not_between → col("f").between(lo, hi)
        if method_name in ("filter_between", "filter_not_between"):
            lo = _pos_arg(args, 1)
            hi = _pos_arg(args, 2)
            if lo is None or hi is None:
                return None
            self._needs_col = True
            proxy = _METHOD_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy), lo, hi)

        # .filter_in / .filter_not_in / .filter_contains / etc.
        if method_name in _METHOD_FILTER_MAP:
            val_node = _pos_arg(args, 1)
            if val_node is None:
                return None
            self._needs_col = True
            proxy = _METHOD_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy), val_node)

        return None

    # ------------------------------------------------------------------
    # Standalone filter function: eq("f", v) → col("f") == v, etc.
    # ------------------------------------------------------------------

    def _build_filter_func_arg(
        self,
        func_name: str,
        args: Sequence[cst.Arg],
    ) -> Optional[cst.BaseExpression]:
        """Return the replacement expression node for a standalone filter call."""
        field_node = _pos_arg(args, 0)
        if field_node is None:
            return None

        if func_name in _FUNC_UNARY_MAP:
            self._needs_col = True
            proxy = _FUNC_UNARY_MAP[func_name]
            return _call(_attr(_col_call(field_node), proxy))

        if func_name in _FUNC_BINARY_OP_MAP:
            val_node = _pos_arg(args, 1)
            if val_node is None:
                return None
            self._needs_col = True
            return cst.Comparison(
                left=_col_call(field_node),
                comparisons=[
                    cst.ComparisonTarget(
                        operator=_FUNC_BINARY_OP_MAP[func_name],
                        comparator=val_node,
                    )
                ],
            )

        if func_name in ("between", "not_between"):
            lo = _pos_arg(args, 1)
            hi = _pos_arg(args, 2)
            if lo is None or hi is None:
                return None
            self._needs_col = True
            proxy = _FUNC_METHOD_MAP[func_name]
            return _call(_attr(_col_call(field_node), proxy), lo, hi)

        if func_name in _FUNC_METHOD_MAP:
            val_node = _pos_arg(args, 1)
            if val_node is None:
                return None
            self._needs_col = True
            proxy = _FUNC_METHOD_MAP[func_name]
            return _call(_attr(_col_call(field_node), proxy), val_node)

        return None

    # ------------------------------------------------------------------
    # Inject missing col / raw imports at module level
    # ------------------------------------------------------------------

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        to_add: List[str] = []
        if self._needs_col and not self._has_col:
            to_add.append("col")
        if self._needs_raw and not self._has_raw:
            to_add.append("raw")
        if not to_add:
            return updated_node

        new_body = list(updated_node.body)

        # Try to augment an existing filters import line
        for i, stmt in enumerate(new_body):
            if not (
                isinstance(stmt, cst.SimpleStatementLine)
                and len(stmt.body) == 1
                and isinstance(stmt.body[0], cst.ImportFrom)
            ):
                continue
            imp = stmt.body[0]
            if isinstance(imp.names, cst.ImportStar):
                continue
            if _dotted_name(imp.module) != _FILTERS_MODULE:
                continue
            existing_names = {alias.name.value for alias in imp.names if isinstance(alias.name, cst.Name)}
            need = [n for n in to_add if n not in existing_names]
            if not need:
                return updated_node  # already present
            all_aliases = list(imp.names) + [cst.ImportAlias(name=_name(n)) for n in need]
            # Re-apply commas
            fixed = _comma_separated(all_aliases)
            new_imp = imp.with_changes(names=fixed)
            new_body[i] = stmt.with_changes(body=[new_imp])
            return updated_node.with_changes(body=new_body)

        # No existing filters import — insert a new one after the last import block
        new_import_stmt = cst.SimpleStatementLine(
            body=[
                cst.ImportFrom(
                    module=_filters_module_attr(),
                    names=_comma_separated([cst.ImportAlias(name=_name(n)) for n in to_add]),
                )
            ]
        )
        last_import_idx = 0
        for i, stmt in enumerate(new_body):
            if isinstance(stmt, cst.SimpleStatementLine) and any(
                isinstance(s, (cst.Import, cst.ImportFrom)) for s in stmt.body
            ):
                last_import_idx = i
        new_body.insert(last_import_idx + 1, new_import_stmt)
        return updated_node.with_changes(body=new_body)


def _comma_separated(
    aliases: List[cst.ImportAlias],
) -> List[cst.ImportAlias]:
    """Return aliases with commas between each, last one without."""
    result = []
    for i, alias in enumerate(aliases):
        if i < len(aliases) - 1:
            result.append(alias.with_changes(comma=cst.Comma(whitespace_after=cst.SimpleWhitespace(" "))))
        else:
            result.append(alias.with_changes(comma=cst.MaybeSentinel.DEFAULT))
    return result


# ---------------------------------------------------------------------------
# Utility: dotted-name string from libcst Attribute / Name tree
# ---------------------------------------------------------------------------


def _dotted_name(node: Optional[cst.BaseExpression]) -> str:
    if node is None:
        return ""
    if isinstance(node, cst.Name):
        return node.value
    if isinstance(node, cst.Attribute):
        return f"{_dotted_name(node.value)}.{node.attr.value}"
    return ""


# ---------------------------------------------------------------------------
# File-level migration
# ---------------------------------------------------------------------------


def migrate_source(source: str, *, client_var: str = "client") -> str:
    """Parse *source*, apply transformations, return migrated source."""
    try:
        tree = cst.parse_module(source)
    except cst.ParserSyntaxError as exc:
        raise ValueError(f"Parse error: {exc}") from exc
    new_tree = tree.visit(_V1Migrator(client_var=client_var))
    return new_tree.code


def migrate_file(path: Path, *, dry_run: bool = False) -> bool:
    """Migrate *path* in place. Returns True if the file was changed."""
    original = path.read_text(encoding="utf-8")
    try:
        migrated = migrate_source(original)
    except ValueError as exc:
        print(f"  [SKIP] {path}: {exc}", file=sys.stderr)
        return False
    if migrated == original:
        return False
    if not dry_run:
        path.write_text(migrated, encoding="utf-8")
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _collect_targets(paths: List[str]) -> List[Path]:
    targets: List[Path] = []
    for p_str in paths:
        p = Path(p_str)
        if p.is_dir():
            root = p.resolve()
            for candidate in sorted(p.rglob("*.py")):
                resolved = candidate.resolve()
                if root == resolved or root in resolved.parents:
                    targets.append(candidate)
                else:
                    print(f"[WARN] Skipping symlink outside target directory: {candidate}", file=sys.stderr)
        elif p.is_file() and p.suffix == ".py":
            targets.append(p)
        else:
            print(f"[WARN] Not a file or directory: {p}", file=sys.stderr)
    return targets


def main(argv: Optional[List[str]] = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    dry_run = "--dry-run" in args
    remaining = [a for a in args if a != "--dry-run"]

    if not remaining:
        print(__doc__)
        print("\nUsage: python -m tools.migrate_v0_to_v1 [--dry-run] <path> [<path> ...]")
        return 1

    targets = _collect_targets(remaining)
    if not targets:
        print("[ERROR] No Python files found.", file=sys.stderr)
        return 1

    changed = skipped = 0
    for path in targets:
        if migrate_file(path, dry_run=dry_run):
            changed += 1
            tag = "[DRY-RUN]" if dry_run else "[MIGRATED]"
            print(f"{tag} {path}")
        else:
            skipped += 1

    print(f"\nDone: {changed} file(s) {'would be ' if dry_run else ''}modified, " f"{skipped} unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
