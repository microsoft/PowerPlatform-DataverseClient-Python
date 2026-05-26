#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
DV-Python-SDK v0 -> v1 GA migration codemod.

Mechanically rewrites beta (0.1.0b*) call sites to their GA (1.0) equivalents
using LibCST (concrete syntax tree -- preserves all whitespace and comments).

Usage::

    pip install PowerPlatform-Dataverse-Client[migration]
    dataverse-migrate path/to/your/scripts/
    dataverse-migrate path/to/your/scripts/ --dry-run          # preview without writing
    dataverse-migrate path/to/your/scripts/ --client-var=svc   # if client is named 'svc'

    # Or via module for development installs:
    python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1 path/to/your/scripts/

Transformations applied
-----------------------
Builder methods (.filter_*  ->  .where(col(...)...))::

    Both positional and keyword-argument forms are recognized. The kwarg
    names match the documented v0 signatures: column/value for binary ops,
    column/values for filter_in/not_in, column/low/high for filter_between,
    column for filter_null/not_null, filter_string for filter_raw.

    .filter_eq("col", v)                       ->  .where(col("col") == v)
    .filter_eq(column="col", value=v)          ->  .where(col("col") == v)
    .filter_ne("col", v)               ->  .where(col("col") != v)
    .filter_gt("col", v)               ->  .where(col("col") > v)
    .filter_ge("col", v)               ->  .where(col("col") >= v)
    .filter_lt("col", v)               ->  .where(col("col") < v)
    .filter_le("col", v)               ->  .where(col("col") <= v)
    .filter_contains("col", v)         ->  .where(col("col").contains(v))
    .filter_startswith("col", v)       ->  .where(col("col").startswith(v))
    .filter_endswith("col", v)         ->  .where(col("col").endswith(v))
    .filter_in("col", vals)            ->  .where(col("col").in_(vals))
    .filter_not_in("col", vals)        ->  .where(col("col").not_in(vals))
    .filter_null("col")                ->  .where(col("col").is_null())
    .filter_not_null("col")            ->  .where(col("col").is_not_null())
    .filter_between("col", lo, hi)     ->  .where(col("col").between(lo, hi))
    .filter_not_between("col", lo, hi) ->  .where(col("col").not_between(lo, hi))
    .filter_raw("expr")                ->  .where(raw("expr"))
    .filter("expr")                    ->  .where(raw("expr"))
    .execute(by_page=True)             ->  .execute_pages()
    .execute(by_page=False)            ->  .execute()  (flag removed)
    <builder_chain>.to_dataframe()     ->  <builder_chain>.execute().to_dataframe()
        Inserts .execute() when the receiver is a recognised QueryBuilder chain
        (contains .builder(), .select(), .where(), or a .filter_*() call).

Record namespace::

    batch.records.get(t, id)     ->  batch.records.retrieve(t, id)

Top-level shortcuts (removed at GA)::

    client.create(t, d)           ->  client.records.create(t, d)
    client.update(t, id, d)       ->  client.records.update(t, id, d)
    client.delete(t, id)          ->  client.records.delete(t, id)
    client.get(t, id)             ->  client.records.get(t, id)  [deprecated; see manual section]
    client.query_sql(sql)         ->  client.query.sql(sql)
    client.get_table_info(t)      ->  client.tables.get(t)
    client.create_table(t, ...)     ->  client.tables.create(t, ...)
    client.delete_table(t)        ->  client.tables.delete(t)
    client.list_tables()          ->  client.tables.list()
    client.create_columns(t, ...)   ->  client.tables.add_columns(t, ...)
    client.delete_columns(t, ...)   ->  client.tables.remove_columns(t, ...)
    client.upload_file(...)         ->  client.files.upload(...)

Import management:
    Adds ``from PowerPlatform.Dataverse.models.filters import col`` when a
    .filter_* method is rewritten (if col is not already imported).
    Adds ``raw`` to the same import when .filter_raw or .filter is rewritten.

NOT handled by this codemod (manual migration required):
    execute(by_page=variable)      ->  manual review required (variable argument, not literal)
    client.records.get(t, id)     ->  client.records.retrieve(t, id)
        Return type changes: beta returns Record (raises on 404); GA retrieve() returns
        Record | None. Callers that do not guard against None will fail silently.
    client.records.get(t, kw=...)  ->  client.records.list(t, kw=...)
        Return type changes: beta returns Iterable[List[Record]] (pages); GA list()
        returns QueryResult (flat iterable over Records). Any ``for page in result:
        for rec in page:`` iteration pattern breaks after a mechanical rename.
    client.dataframe.get()        ->  client.query.builder(...).execute().to_dataframe()
        Expression reconstruction requires understanding caller intent.
    client.query.sql_select()/sql_join()/sql_joins()  ->  removed (no mechanical replacement)
"""

from __future__ import annotations

import difflib
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple

try:
    import libcst as cst
except ImportError as _e:
    raise ImportError(
        "libcst is required. Install with:\n"
        "  pip install PowerPlatform-Dataverse-Client[migration]\n"
        "  # or: pip install 'libcst>=1.0.0'"
    ) from _e


# ---------------------------------------------------------------------------
# Filter-method -> .where(col(...)) mapping
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

# Standalone filter functions from filters module (beta API) -> col() equivalents
# eq("f", v) -> col("f") == v, between("f", lo, hi) -> col("f").between(lo, hi), etc.
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
    # The standalone function in models/filters.py is named ``filter_in`` (not ``in_``)
    # because ``in`` is a Python keyword. The key here MUST stay ``filter_in`` to match
    # what filters.py exports; the value ``in_`` is the method name on the col() proxy.
    # Do not "tidy up" to "in_": "in_" -- the codemod would stop matching real call sites.
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

# Methods that identify a QueryBuilder call chain (used to detect .to_dataframe() callers)
_BUILDER_CHAIN_METHODS: Set[str] = {"builder", "select", "where", "filter", "execute_pages"} | _ALL_FILTER_METHODS

# Top-level client shortcut -> (new_namespace, new_method)
_CLIENT_SHORTCUTS = {
    "create": ("records", "create"),
    "update": ("records", "update"),
    "delete": ("records", "delete"),
    "get": ("records", "get"),
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


def _kwarg(args: Sequence[cst.Arg], *names: str) -> Optional[cst.BaseExpression]:
    """Return the value of the first kwarg matching one of *names*, or None."""
    for a in args:
        if isinstance(a.keyword, cst.Name) and a.keyword.value in names:
            return a.value
    return None


def _arg(args: Sequence[cst.Arg], pos_index: int, *kwarg_names: str) -> Optional[cst.BaseExpression]:
    """Return the positional arg at *pos_index* if present, else a kwarg matching *kwarg_names*."""
    p = _pos_arg(args, pos_index)
    if p is not None:
        return p
    return _kwarg(args, *kwarg_names) if kwarg_names else None


# Canonical v0 kwarg names per filter method. Used by both the migrator and the
# manual-review finder so they agree on what counts as "recognized arg shape".
# These names come from the b10-era QueryBuilder.filter_* signatures and the
# standalone filter functions in models/filters.py.
_FILTER_KWARGS = {
    # QueryBuilder methods: (column_slot, value_slot, extra_slot)
    "filter_eq": ("column", "value", None),
    "filter_ne": ("column", "value", None),
    "filter_gt": ("column", "value", None),
    "filter_ge": ("column", "value", None),
    "filter_lt": ("column", "value", None),
    "filter_le": ("column", "value", None),
    "filter_contains": ("column", "value", None),
    "filter_startswith": ("column", "value", None),
    "filter_endswith": ("column", "value", None),
    "filter_null": ("column", None, None),
    "filter_not_null": ("column", None, None),
    "filter_in": ("column", "values", None),
    "filter_not_in": ("column", "values", None),
    "filter_between": ("column", "low", "high"),
    "filter_not_between": ("column", "low", "high"),
    "filter_raw": ("filter_string", None, None),
}

# Canonical v0 kwarg names per standalone filter function.
_FUNC_KWARGS = {
    "eq": ("column", "value", None),
    "ne": ("column", "value", None),
    "gt": ("column", "value", None),
    "ge": ("column", "value", None),
    "lt": ("column", "value", None),
    "le": ("column", "value", None),
    "contains": ("column", "value", None),
    "startswith": ("column", "value", None),
    "endswith": ("column", "value", None),
    "is_null": ("column", None, None),
    "is_not_null": ("column", None, None),
    # Key is ``filter_in`` (not ``in_``) to match the standalone function name
    # exported by models/filters.py -- ``in`` is a Python keyword. See the matching
    # note on _FUNC_METHOD_MAP above.
    "filter_in": ("column", "values", None),
    "not_in": ("column", "values", None),
    "between": ("column", "low", "high"),
    "not_between": ("column", "low", "high"),
}


# ---------------------------------------------------------------------------
# Main transformer
# ---------------------------------------------------------------------------


class _V1Migrator(cst.CSTTransformer):
    """LibCST transformer rewriting DV-Python-SDK beta -> v1 GA."""

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
        # Standalone filter functions: eq("f", v) -> col("f") == v, etc.
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
        # .filter_*(...) -> .where(col(...) ...)
        # Only rewrite when the args match the documented v0 shape exactly.
        # If they don't (e.g. extra positionals, unknown kwargs), leave the
        # call untouched -- the _ManualReviewFinder will emit a [MANUAL] note
        # so the user notices and rewrites it by hand. Without the shape gate
        # here, calls like .filter_raw('a', 'EXTRA') would be silently
        # rewritten to .where(raw('a')) with the 'EXTRA' arg dropped.
        # ----------------------------------------------------------------
        if method_name in _ALL_FILTER_METHODS and _can_extract_filter_method_args(method_name, updated_node.args):
            where_arg = self._build_filter_arg(method_name, updated_node.args)
            if where_arg is not None:
                return updated_node.with_changes(
                    func=func.with_changes(attr=_name("where")),
                    args=[cst.Arg(value=where_arg)],
                )

        # ----------------------------------------------------------------
        # .filter("expr") -> .where(raw("expr"))
        # QueryBuilder.filter() was removed at GA (not deprecated). Wrapping
        # in raw() preserves the OData string exactly for string-literal callers.
        # ----------------------------------------------------------------
        if method_name == "filter":
            expr_node = _arg(updated_node.args, 0, "filter_string")
            if expr_node is not None and len(updated_node.args) == 1:
                self._needs_raw = True
                return updated_node.with_changes(
                    func=func.with_changes(attr=_name("where")),
                    args=[cst.Arg(value=_call(_name("raw"), expr_node))],
                )

        # ----------------------------------------------------------------
        # .execute(by_page=True)  -> .execute_pages()
        # .execute(by_page=False) -> .execute()  (flag removed)
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
        # QueryBuilder.to_dataframe() -> .execute().to_dataframe()
        # Only rewrites when the receiver is a recognised QueryBuilder chain
        # (contains .builder(), .select(), .where(), or a .filter_*() call).
        # Skips if receiver is already a .execute() call (QueryResult.to_dataframe()
        # is the GA form and must not be touched).
        # ----------------------------------------------------------------
        if method_name == "to_dataframe":
            receiver = func.value
            already_executed = (
                isinstance(receiver, cst.Call)
                and isinstance(receiver.func, cst.Attribute)
                and isinstance(receiver.func.attr, cst.Name)
                and receiver.func.attr.value == "execute"
            )
            if not already_executed and self._is_query_builder_chain(receiver):
                # Preserve the .to_dataframe() call's leading-dot whitespace on the
                # inserted .execute() so multi-line fluent chains keep their
                # per-method-per-line layout. Without this, the new Attribute has
                # an empty Dot and .execute() collapses onto the receiver's line:
                #     .select(...).execute()
                #     .to_dataframe()
                # With it, both calls land on their own lines at the same indent:
                #     .select(...)
                #     .execute()
                #     .to_dataframe()
                execute_attr = cst.Attribute(
                    value=receiver,
                    attr=cst.Name("execute"),
                    dot=cst.Dot(whitespace_before=func.dot.whitespace_before),
                )
                execute_call = cst.Call(func=execute_attr)
                return updated_node.with_changes(func=func.with_changes(value=execute_call))

        # ----------------------------------------------------------------
        # batch.records.get(table, id) -> batch.records.retrieve(table, id)
        # NOTE: client.records.get() is NOT codemodded -- the return type changes
        # between beta and GA (Record | None vs Record for single-id; QueryResult vs
        # Iterable[List[Record]] for multi-record). Surrounding iteration patterns
        # would silently break after a mechanical rename.
        # ----------------------------------------------------------------
        if method_name == "get" and isinstance(func.value, cst.Attribute):
            inner = func.value
            if isinstance(inner.attr, cst.Name) and inner.attr.value == "records":
                if isinstance(inner.value, cst.Name) and inner.value.value == "batch":
                    # batch.records.get() returns None in both versions -- safe to rename
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

    @staticmethod
    def _is_query_builder_chain(node: cst.BaseExpression) -> bool:
        """Return True if *node* is a call chain that includes a QueryBuilder method."""
        cur: cst.BaseExpression = node
        while isinstance(cur, cst.Call):
            f = cur.func
            if isinstance(f, cst.Attribute) and isinstance(f.attr, cst.Name):
                if f.attr.value in _BUILDER_CHAIN_METHODS:
                    return True
                cur = f.value
            else:
                break
        return False

    # ------------------------------------------------------------------
    # Build the argument for .where() from .filter_*() args
    # ------------------------------------------------------------------

    def _build_filter_arg(
        self,
        method_name: str,
        args: Sequence[cst.Arg],
    ) -> Optional[cst.BaseExpression]:
        # Resolve the field/expression slot -- positional 0 or the documented v0 kwarg name.
        col_name, val_name, extra_name = _FILTER_KWARGS.get(method_name, ("column", "value", None))
        field_node = _arg(args, 0, col_name)
        if field_node is None:
            return None

        # .filter_raw(expr) -> raw(expr)
        if method_name == "filter_raw":
            self._needs_raw = True
            return _call(_name("raw"), field_node)

        # .filter_null / .filter_not_null -> col("f").is_null() / .is_not_null()
        if method_name in _UNARY_FILTER_MAP:
            self._needs_col = True
            proxy = _UNARY_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy))

        # .filter_eq / .filter_ne / ... -> col("f") OP val
        if method_name in _BINARY_OP_MAP:
            val_node = _arg(args, 1, val_name) if val_name else None
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

        # .filter_between / .filter_not_between -> col("f").between(lo, hi)
        if method_name in ("filter_between", "filter_not_between"):
            lo = _arg(args, 1, val_name) if val_name else None
            hi = _arg(args, 2, extra_name) if extra_name else None
            if lo is None or hi is None:
                return None
            self._needs_col = True
            proxy = _METHOD_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy), lo, hi)

        # .filter_in / .filter_not_in / .filter_contains / etc.
        if method_name in _METHOD_FILTER_MAP:
            val_node = _arg(args, 1, val_name) if val_name else None
            if val_node is None:
                return None
            self._needs_col = True
            proxy = _METHOD_FILTER_MAP[method_name]
            return _call(_attr(_col_call(field_node), proxy), val_node)

        return None

    # ------------------------------------------------------------------
    # Standalone filter function: eq("f", v) -> col("f") == v, etc.
    # ------------------------------------------------------------------

    def _build_filter_func_arg(
        self,
        func_name: str,
        args: Sequence[cst.Arg],
    ) -> Optional[cst.BaseExpression]:
        """Return the replacement expression node for a standalone filter call."""
        col_name, val_name, extra_name = _FUNC_KWARGS.get(func_name, ("column", "value", None))
        field_node = _arg(args, 0, col_name)
        if field_node is None:
            return None

        if func_name in _FUNC_UNARY_MAP:
            self._needs_col = True
            proxy = _FUNC_UNARY_MAP[func_name]
            return _call(_attr(_col_call(field_node), proxy))

        if func_name in _FUNC_BINARY_OP_MAP:
            val_node = _arg(args, 1, val_name) if val_name else None
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
            lo = _arg(args, 1, val_name) if val_name else None
            hi = _arg(args, 2, extra_name) if extra_name else None
            if lo is None or hi is None:
                return None
            self._needs_col = True
            proxy = _FUNC_METHOD_MAP[func_name]
            return _call(_attr(_col_call(field_node), proxy), lo, hi)

        if func_name in _FUNC_METHOD_MAP:
            val_node = _arg(args, 1, val_name) if val_name else None
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
            all_aliases = _append_aliases_preserving_layout(list(imp.names), need)
            new_imp = imp.with_changes(names=all_aliases)
            new_body[i] = stmt.with_changes(body=[new_imp])
            return updated_node.with_changes(body=new_body)

        # No existing filters import -- insert a new one after the last import block.
        new_import_stmt = cst.SimpleStatementLine(
            body=[
                cst.ImportFrom(
                    module=_filters_module_attr(),
                    names=_comma_separated([cst.ImportAlias(name=_name(n)) for n in to_add]),
                )
            ]
        )
        # Initialize to -1 so we can tell "no imports found" from "first statement
        # is an import." The previous default of 0 caused insertion at index 1 even
        # when the file had no imports at all -- placing the new import AFTER the
        # first statement, which then referenced the as-yet-undefined ``col`` name
        # at runtime (NameError: name 'col' is not defined).
        last_import_idx = -1
        for i, stmt in enumerate(new_body):
            if isinstance(stmt, cst.SimpleStatementLine) and any(
                isinstance(s, (cst.Import, cst.ImportFrom)) for s in stmt.body
            ):
                last_import_idx = i
        if last_import_idx == -1:
            # No imports at all. Insert at the top, but after a leading module
            # docstring if one is present (PEP 257 / PEP 8 ordering: docstring
            # first, then any imports). ``__future__`` imports would have been
            # detected above and bumped last_import_idx, so this branch only
            # fires when there are no imports of any kind.
            insert_idx = 1 if new_body and _is_module_docstring(new_body[0]) else 0
        else:
            insert_idx = last_import_idx + 1
        new_body.insert(insert_idx, new_import_stmt)
        return updated_node.with_changes(body=new_body)


def _is_module_docstring(stmt: cst.BaseStatement) -> bool:
    """Return True if *stmt* is a module-level docstring (PEP 257 first-statement string literal).

    Python only counts plain string literals (and adjacent-string concatenations)
    as docstrings -- f-strings and binary strings don't qualify. Used by import
    injection to keep a leading docstring at position 0 and slot the new import
    in at position 1 instead of clobbering the docstring.
    """
    if not (isinstance(stmt, cst.SimpleStatementLine) and len(stmt.body) == 1):
        return False
    expr = stmt.body[0]
    if not isinstance(expr, cst.Expr):
        return False
    return isinstance(expr.value, (cst.SimpleString, cst.ConcatenatedString))


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


def _append_aliases_preserving_layout(
    existing: List[cst.ImportAlias], new_names: Sequence[str]
) -> List[cst.ImportAlias]:
    """Append fresh ImportAliases for *new_names* to *existing* without flattening layout.

    Calling ``_comma_separated`` over the merged list rebuilds every comma with a
    plain single space, which silently flattens multi-line ``from X import (a,\\n  b,\\n)``
    blocks onto one long line. Instead, this helper:

    - Keeps every original alias's ``comma`` field exactly as-is (so per-alias
      newline+indent trivia survives).
    - Demotes the previous-last alias from "trailing" to "separator" style by
      copying the first existing alias's comma (which captures the multi-line
      or inline style of the original block).
    - Gives the new last alias the original-last alias's comma so the closing
      paren / trailing comma state is preserved.
    """
    if not existing:
        return _comma_separated([cst.ImportAlias(name=_name(n)) for n in new_names])
    if not new_names:
        return existing

    # Separator-comma template -- the comma+whitespace placed between *non-final*
    # aliases. For multi-alias inputs we copy existing[0].comma (which already
    # captures the original block's style). For single-alias inputs we still need
    # to detect a multi-line layout: ``from X import (\n    raw,\n)`` has only
    # one alias but its trailing comma carries the newline trivia, so we copy
    # *that* as the separator. Truly inline single-alias imports (``import raw``)
    # have ``MaybeSentinel.DEFAULT`` for the comma -- fall back to single-space.
    separator_template: Optional[cst.Comma] = None
    if len(existing) >= 2 and isinstance(existing[0].comma, cst.Comma):
        separator_template = existing[0].comma
    elif len(existing) == 1 and isinstance(existing[0].comma, cst.Comma):
        # Single-alias multi-line case: ``from X import (\n    raw,\n)`` has only
        # one alias, so existing[0].comma is the *trailing* comma whose
        # whitespace_after carries only ``\n`` (the closing paren sits at col 0
        # with no indent). That bare ``\n`` is not a usable separator -- using
        # it would put ``col`` flush against the left margin. Detect the
        # multi-line shape and synthesize a separator that re-indents to a
        # 4-space PEP 8 default (matches what black would produce for the
        # multi-alias form).
        sole = existing[0].comma
        ws = sole.whitespace_after
        is_multiline = False
        if isinstance(ws, cst.SimpleWhitespace) and "\n" in ws.value:
            is_multiline = True
        elif isinstance(ws, cst.ParenthesizedWhitespace):
            # ParenthesizedWhitespace always implies multi-line (it owns at least
            # one newline) -- safe to treat as the multi-line case.
            is_multiline = True
        if is_multiline:
            separator_template = cst.Comma(
                whitespace_after=cst.ParenthesizedWhitespace(
                    first_line=cst.TrailingWhitespace(),
                    empty_lines=[],
                    indent=True,
                    last_line=cst.SimpleWhitespace("    "),
                )
            )
    if separator_template is None:
        separator_template = cst.Comma(whitespace_after=cst.SimpleWhitespace(" "))
    trailing_template = existing[-1].comma

    out = list(existing)
    # Previous-last alias is no longer last: it now needs a separator-style comma.
    out[-1] = out[-1].with_changes(comma=separator_template)
    for j, n in enumerate(new_names):
        comma = trailing_template if j == len(new_names) - 1 else separator_template
        out.append(cst.ImportAlias(name=_name(n), comma=comma))
    return out


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


# ---------------------------------------------------------------------------
# Manual-review pattern detector
# ---------------------------------------------------------------------------

_REMOVED_QUERY_METHODS: Set[str] = {"sql_select", "sql_join", "sql_joins"}


def _can_extract_filter_method_args(method_name: str, args: Sequence[cst.Arg]) -> bool:
    """Return True if a v0 .filter_*() call's args satisfy the documented shape.

    Mirrors the extraction predicates in :meth:`_V1Migrator._build_filter_arg` so
    that the migrator and the manual-review finder agree on what counts as a
    rewriteable call. Used by the finder to emit a [MANUAL] note for any
    .filter_*() call shape the migrator cannot rewrite (e.g. unrecognized kwargs).

    Also rejects calls that contain *extra* args beyond the documented shape.
    Previously a call like ``.filter_raw('a eq 1', 'EXTRA')`` returned True
    (because position 0 was present) and the migrator silently dropped the
    extra arg in the rewrite -- losing real user code. By insisting on exact
    arity here, malformed calls fall into the ``[MANUAL]`` path instead of
    being miscompiled.
    """
    if method_name not in _ALL_FILTER_METHODS:
        return False
    col_name, val_name, extra_name = _FILTER_KWARGS.get(method_name, ("column", "value", None))
    if _arg(args, 0, col_name) is None:
        return False

    # Determine the exact expected arity for this method.
    if method_name == "filter_raw" or method_name in _UNARY_FILTER_MAP:
        expected_arity = 1
        allowed_kwargs = {col_name}
    elif method_name in ("filter_between", "filter_not_between"):
        if val_name is None or _arg(args, 1, val_name) is None:
            return False
        if extra_name is None or _arg(args, 2, extra_name) is None:
            return False
        expected_arity = 3
        allowed_kwargs = {col_name, val_name, extra_name}
    else:
        if val_name is None or _arg(args, 1, val_name) is None:
            return False
        expected_arity = 2
        allowed_kwargs = {col_name, val_name}

    if len(args) != expected_arity:
        return False
    for a in args:
        if isinstance(a.keyword, cst.Name) and a.keyword.value not in allowed_kwargs:
            return False
    return True


class _ManualReviewFinder(cst.CSTTransformer):
    """Visitor that detects patterns the codemod cannot safely rewrite automatically.

    Uses :class:`cst.metadata.PositionProvider` to capture each flagged call's
    source line/column, so emitted findings can be prefixed with a location
    that editors (VS Code, JetBrains, ``compile_error``-style tooling) can jump
    to directly. The format is ``"<line>:<col>: <message>"`` per finding;
    :func:`migrate_file` then prepends the file path to produce the canonical
    ``"<path>:<line>:<col>: <message>"`` shape.
    """

    METADATA_DEPENDENCIES = (cst.metadata.PositionProvider,)

    def __init__(self, client_var: str = "client") -> None:
        super().__init__()
        self._client_var = client_var
        self.findings: List[str] = []
        # Names known to be bound (directly or via alias) to a Dataverse client.
        # Seeded with the configured --client-var so a first-level alias like
        # ``my_client = client`` can be detected; populated as the visitor sees
        # ``<var> = DataverseClient(...)`` and ``<var> = <known_client>`` assigns.
        # Aliases are tracked transitively because real codebases chain them
        # (e.g. ``dv = my_client`` where ``my_client = client``).
        self._known_client_names: Set[str] = {client_var}

    def _emit(self, node: cst.CSTNode, message: str) -> None:
        """Append a finding tagged with the node's source line and column."""
        try:
            pos = self.get_metadata(cst.metadata.PositionProvider, node)
            prefix = f"{pos.start.line}:{pos.start.column}: "
        except KeyError:
            # PositionProvider not available (tree not wrapped in MetadataWrapper).
            # Fall back to an unprefixed message so the codemod never crashes,
            # but the output will lack location info -- find_manual_patterns is
            # responsible for wrapping the tree.
            prefix = ""
        self.findings.append(prefix + message)

    def _receiver_chain(self, node: cst.Attribute) -> List[str]:
        """Return the dotted name parts of an Attribute chain, innermost first."""
        parts: List[str] = []
        cur: cst.BaseExpression = node
        while isinstance(cur, cst.Attribute):
            if isinstance(cur.attr, cst.Name):
                parts.append(cur.attr.value)
            cur = cur.value
        if isinstance(cur, cst.Name):
            parts.append(cur.value)
        return parts  # e.g. ["get", "records", "client"] for client.records.get

    def visit_Call(self, node: cst.Call) -> None:
        func = node.func
        if not isinstance(func, cst.Attribute):
            return

        method = func.attr.value if isinstance(func.attr, cst.Name) else ""
        chain = self._receiver_chain(func)  # [method, ns, client_var, ...]

        # execute(by_page=<variable>) -- non-literal by_page cannot be codemodded
        if method == "execute":
            for a in node.args:
                if isinstance(a.keyword, cst.Name) and a.keyword.value == "by_page":
                    if not (isinstance(a.value, cst.Name) and a.value.value in ("True", "False")):
                        self._emit(
                            node,
                            "execute(by_page=<variable>) -- non-literal by_page requires manual review; "
                            "replace with execute_pages() or execute() depending on runtime value",
                        )

        # client.records.get() -- return type changes make a mechanical rename unsafe
        if method == "get" and len(chain) >= 3 and chain[1] == "records" and chain[2] == self._client_var:
            self._emit(
                node,
                f"{self._client_var}.records.get() -- use retrieve() for single-record lookup "
                "(return type changes: raises on 404 vs returns None) "
                "or list() for multi-record (iteration pattern changes)",
            )

        # client.dataframe.get() -- recipe depends on whether a record_id is passed.
        #   single-record (record_id present): records.retrieve(table, record_id, select=...)
        #     returns Record | None instead of a DataFrame, so callers need to adjust
        #     downstream code. The query-builder recipe is wrong for this case -- it
        #     produces a multi-row DataFrame query for a one-row lookup.
        #   multi-record (no record_id): query.builder(...).execute().to_dataframe()
        if method == "get" and len(chain) >= 3 and chain[1] == "dataframe" and chain[2] == self._client_var:
            # record_id is the 2nd positional slot OR the documented kwarg name.
            record_id_node = _arg(node.args, 1, "record_id")
            if record_id_node is not None:
                self._emit(
                    node,
                    f"{self._client_var}.dataframe.get(table, record_id=...) -- use "
                    f"{self._client_var}.records.retrieve(table, record_id, select=...); "
                    "returns Record | None instead of a DataFrame (semantic shift -- "
                    "adjust downstream code accordingly)",
                )
            else:
                self._emit(
                    node,
                    f"{self._client_var}.dataframe.get(table, ...) (no record_id) -- use "
                    f"{self._client_var}.query.builder(table).select(...).where(...)"
                    ".execute().to_dataframe(); requires manual reconstruction",
                )

        # client.query.sql_select/sql_join/sql_joins -- removed with no mechanical replacement
        if (
            method in _REMOVED_QUERY_METHODS
            and len(chain) >= 3
            and chain[1] == "query"
            and chain[2] == self._client_var
        ):
            self._emit(
                node,
                f"{self._client_var}.query.{method}() -- removed at GA with no mechanical replacement",
            )

        # .filter_*() call whose arg shape the codemod can't rewrite (e.g. unrecognized
        # kwargs like filter_eq(field=..., val=...)). filter_eq/filter_between/etc. are
        # removed at GA, so a silent pass would let the file blow up at runtime with
        # AttributeError. Flagging it here surfaces the call to the user as [MANUAL].
        if method in _ALL_FILTER_METHODS and not _can_extract_filter_method_args(method, node.args):
            self._emit(
                node,
                f".{method}(...) -- kwargs/argument shape not recognized; "
                f"removed at GA, rewrite manually to .where(col(...) ...)",
            )

    def _call_constructs_client(self, value: cst.BaseExpression) -> Optional[str]:
        """If *value* is a ``DataverseClient(...)`` / ``ServiceClient(...)`` call, return the class name.

        Matches both bare ``DataverseClient(...)`` and dotted forms like
        ``PowerPlatform.Dataverse.client.DataverseClient(...)`` -- only the
        right-most attribute name is inspected. Returns ``None`` otherwise.
        """
        if not isinstance(value, cst.Call):
            return None
        func = value.func
        if isinstance(func, cst.Name):
            name = func.value
        elif isinstance(func, cst.Attribute) and isinstance(func.attr, cst.Name):
            name = func.attr.value
        else:
            return None
        if name in ("DataverseClient", "ServiceClient"):
            return name
        return None

    def _flag_non_default_client_assignment(
        self, node: cst.CSTNode, target: cst.BaseExpression, value: cst.BaseExpression
    ) -> None:
        """Emit a [MANUAL] note for assignments that bind a client to a non-default name.

        Two foot-gun shapes are detected:

        1. ``<var> = DataverseClient(...)`` (or ``ServiceClient(...)``) where
           ``<var>`` differs from ``--client-var``. The user named their client
           ``svc`` / ``dv`` / ``cl`` and ran the codemod with the default
           ``client``, so every shortcut on that name went unmigrated.
        2. ``<var> = <known_client>`` -- a Name-to-Name alias of a name that is
           already known to refer to a client (the configured ``--client-var``,
           any earlier ``<var> = DataverseClient(...)``, or any earlier alias).
           Tracked transitively so chains like ``dv = my_client`` (where
           ``my_client = client``) still surface. The original repro for this
           bug had exactly this shape -- without the alias check the
           ``my_client.create(...)`` call site is silently missed.
        """
        if not isinstance(target, cst.Name):
            return

        cls = self._call_constructs_client(value)
        if cls is not None:
            # Record the binding regardless of whether it matches client_var,
            # so a later ``dv = cl`` chained alias can still be detected even
            # though ``cl = DataverseClient(...)`` was itself non-default.
            self._known_client_names.add(target.value)
            if target.value == self._client_var:
                return
            self._emit(
                node,
                f"{target.value} = {cls}(...) -- assigned to a name other than "
                f"--client-var ({self._client_var!r}); shortcut calls like "
                f"{target.value}.create()/.update()/.delete()/etc. WILL NOT be migrated. "
                f"Re-run with --client-var={target.value} to rewrite this client's call sites.",
            )
            return

        # Name-to-Name alias of a previously seen client.
        if isinstance(value, cst.Name) and value.value in self._known_client_names:
            self._known_client_names.add(target.value)
            if target.value == self._client_var:
                return
            self._emit(
                node,
                f"{target.value} = {value.value} -- aliases a Dataverse client to a name "
                f"other than --client-var ({self._client_var!r}); shortcut calls like "
                f"{target.value}.create()/.update()/.delete()/etc. WILL NOT be migrated. "
                f"Either inline the alias (use {value.value} directly) or re-run with "
                f"--client-var={target.value} to rewrite this alias's call sites.",
            )

    def visit_Assign(self, node: cst.Assign) -> None:
        # Multi-target ``a = b = DataverseClient(...)`` is rare and ambiguous re:
        # which name should be the client_var, so skip it -- the single-target
        # form covers the common foot-gun the bug report cites.
        if len(node.targets) != 1:
            return
        self._flag_non_default_client_assignment(node, node.targets[0].target, node.value)

    def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
        # ``cl: DataverseClient = DataverseClient(...)`` -- value is optional in
        # AnnAssign (e.g. just ``cl: DataverseClient``), so guard for None.
        if node.value is None:
            return
        self._flag_non_default_client_assignment(node, node.target, node.value)

    def visit_For(self, node: cst.For) -> None:
        # Targeted detection of the v0 nested-for paging idiom:
        #     for page in <client>.records.get(table, ...):
        #         for rec in page:
        #             ...
        # Under v1 the mechanical replacement records.list(table, ...) returns a
        # flat QueryResult, not Iterable[List[Record]] -- the inner `for rec in page`
        # loop will iterate the Record's attributes / fail at runtime. The generic
        # client.records.get() note above flags every records.get() call but does
        # not tell the dev which call sites carry the iteration-pattern landmine.
        if not isinstance(node.iter, cst.Call):
            return
        call = node.iter
        if not isinstance(call.func, cst.Attribute):
            return
        if not (isinstance(call.func.attr, cst.Name) and call.func.attr.value == "get"):
            return
        inner = call.func.value
        if not isinstance(inner, cst.Attribute):
            return
        if not (isinstance(inner.attr, cst.Name) and inner.attr.value == "records"):
            return
        # Require the .records receiver to actually be the configured client
        # variable (or a known alias of it). Otherwise an unrelated
        # ``database_pool.records.get(...)`` would be flagged with a note that
        # advises rewriting to ``client.records.list(...)`` -- replacing the
        # wrong receiver name. Comparing to _known_client_names (rather than
        # only _client_var) lets the check work when the user has aliased
        # the client (handled by _flag_non_default_client_assignment above).
        receiver = inner.value
        if not isinstance(receiver, cst.Name):
            return
        if receiver.value not in self._known_client_names:
            return
        # The outer iter is some <client>.records.get(...) call. Match only when
        # the loop target is a single Name so we can compare with the inner For's iter.
        if not isinstance(node.target, cst.Name):
            return
        outer_var = node.target.value
        if not isinstance(node.body, cst.IndentedBlock):
            return
        for stmt in node.body.body:
            if isinstance(stmt, cst.For) and isinstance(stmt.iter, cst.Name) and stmt.iter.value == outer_var:
                self._emit(
                    node,
                    f"nested-for paging over .records.get(...) -- v1 "
                    f"{self._client_var}.records.list(table, ...) returns a flat "
                    "QueryResult (not Iterable[List[Record]]); flatten to a single "
                    f"loop ('for rec in {self._client_var}.records.list(table, ...):') "
                    "or keep paging explicit with "
                    f"'for page in {self._client_var}.records.list_pages(table, ...): "
                    "for rec in page: ...'",
                )
                return


def find_manual_patterns(source: str, *, client_var: str = "client") -> List[str]:
    """Return descriptions of patterns in *source* that require manual migration.

    Each returned string is prefixed with ``"<line>:<col>: "`` (1-based line,
    0-based column matching libcst's PositionProvider). :func:`migrate_file`
    prepends the file path to produce the canonical
    ``"<path>:<line>:<col>: <message>"`` form most editors can navigate to.
    """
    try:
        tree = cst.parse_module(source)
    except cst.ParserSyntaxError:
        return []
    # MetadataWrapper computes the PositionProvider metadata that
    # _ManualReviewFinder consults via get_metadata(). Without wrapping, the
    # visitor would still run but its _emit() would fall back to unprefixed
    # output -- defeating the whole point of the change.
    wrapper = cst.MetadataWrapper(tree)
    finder = _ManualReviewFinder(client_var=client_var)
    wrapper.visit(finder)
    return finder.findings


# ---------------------------------------------------------------------------
# Unused-import cleanup for the filters module
# ---------------------------------------------------------------------------


class _UsedNamesCollector(cst.CSTVisitor):
    """Collect every ``cst.Name`` referenced outside of import statements.

    The standalone-filter rewrite (``eq("col", v)`` -> ``col("col") == v``)
    consumes references to the original imported names, leaving the original
    ``from ... import eq, gt, between`` line as dead F401 noise. We need to
    know which imported names are still actually referenced after the rewrite
    -- and the only ``cst.Name`` nodes we should ignore for that purpose are
    those that appear inside the import statements themselves.
    """

    def __init__(self) -> None:
        super().__init__()
        self.used: Set[str] = set()
        self._import_depth = 0

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        self._import_depth += 1

    def leave_ImportFrom(self, original_node: cst.ImportFrom) -> None:
        self._import_depth -= 1

    def visit_Import(self, node: cst.Import) -> None:
        self._import_depth += 1

    def leave_Import(self, original_node: cst.Import) -> None:
        self._import_depth -= 1

    def visit_Name(self, node: cst.Name) -> None:
        if self._import_depth == 0:
            self.used.add(node.value)


class _PruneUnusedFiltersImport(cst.CSTTransformer):
    """Drop now-unused aliases from ``from ...filters import ...`` lines.

    Only operates on the filters module so we don't touch unrelated imports.
    If every alias on a filters-module import becomes unused (e.g., the file
    only imported ``eq`` and every ``eq(...)`` call was rewritten), the whole
    statement is removed via :class:`cst.RemoveFromParent`.
    """

    def __init__(self, used: Set[str]) -> None:
        super().__init__()
        self._used = used

    def leave_SimpleStatementLine(
        self,
        original_node: cst.SimpleStatementLine,
        updated_node: cst.SimpleStatementLine,
    ) -> cst.BaseStatement:
        if not (len(updated_node.body) == 1 and isinstance(updated_node.body[0], cst.ImportFrom)):
            return updated_node
        imp = updated_node.body[0]
        if isinstance(imp.names, cst.ImportStar):
            return updated_node
        if _dotted_name(imp.module) != _FILTERS_MODULE:
            return updated_node

        names_list = list(imp.names)
        kept_indices: List[int] = []
        for i, alias in enumerate(names_list):
            # Local binding name is the alias if ``as`` is used, else the imported name.
            if alias.asname is not None and isinstance(alias.asname.name, cst.Name):
                local = alias.asname.name.value
            elif isinstance(alias.name, cst.Name):
                local = alias.name.value
            else:
                # Dotted import-from name (rare here) -- be conservative, keep it.
                kept_indices.append(i)
                continue
            if local in self._used:
                kept_indices.append(i)

        if len(kept_indices) == len(names_list):
            return updated_node  # nothing pruned
        if not kept_indices:
            return cst.RemoveFromParent()

        # Preserve per-alias trivia: keep intermediate aliases' commas as-is so
        # the multi-line layout survives, and transfer the original-last alias's
        # trailing comma onto the new-last alias so the closing paren still
        # lands at the original indent.
        original_trailing = names_list[-1].comma
        kept_aliases: List[cst.ImportAlias] = []
        for j, idx in enumerate(kept_indices):
            alias = names_list[idx]
            if j == len(kept_indices) - 1:
                kept_aliases.append(alias.with_changes(comma=original_trailing))
            else:
                kept_aliases.append(alias)
        return updated_node.with_changes(body=[imp.with_changes(names=kept_aliases)])


# ---------------------------------------------------------------------------
# File-level migration
# ---------------------------------------------------------------------------


def migrate_source(source: str, *, client_var: str = "client") -> str:
    """Parse *source*, apply transformations, return migrated source.

    Runs two passes:

    1. :class:`_V1Migrator` -- the main transform (filter rewrites, shortcut
       rewrites, ``.execute()`` insertion, etc.) plus import injection for any
       new ``col``/``raw`` names it produced.
    2. :class:`_PruneUnusedFiltersImport` -- removes any filter-module aliases
       (``eq``, ``gt``, ``between``, ...) that the rewrite consumed, so the
       output is free of F401 unused-import noise.
    """
    try:
        tree = cst.parse_module(source)
    except cst.ParserSyntaxError as exc:
        raise ValueError(f"Parse error: {exc}") from exc
    new_tree = tree.visit(_V1Migrator(client_var=client_var))
    collector = _UsedNamesCollector()
    new_tree.visit(collector)
    new_tree = new_tree.visit(_PruneUnusedFiltersImport(collector.used))
    return new_tree.code


# Substrings that prove the file has been touched by the codemod (or otherwise uses
# v1-exclusive APIs that did not exist in v0). Used to keep per-file labels stable
# across re-runs: a file with manual-only notes but no v1 sentinels really does need
# initial manual migration; one with v1 sentinels is already at the codemod's fixed
# point and any [MANUAL] notes are advisory follow-ups.
#
# Deliberately excludes ``.records.create(``/``.update(``/``.delete(``/``.records.get(``
# /``.query.sql(`` -- those namespace forms existed in v0 alongside the now-removed
# top-level shortcuts, so they don't prove the codemod ran.
_V1_SENTINELS: Tuple[str, ...] = (
    ".records.retrieve(",
    ".records.list(",
    ".records.list_pages(",
    ".execute_pages(",
    ".tables.add_columns(",
    ".tables.remove_columns(",
    ".where(col(",
    ".fetchxml(",
)


def _has_v1_sentinels(source: str) -> bool:
    """Return True if *source* contains a v1-exclusive API form."""
    return any(s in source for s in _V1_SENTINELS)


def migrate_file(path: Path, *, dry_run: bool = False, client_var: str = "client") -> Tuple[bool, bool, List[str]]:
    """Migrate *path* in place.

    Returns ``(was_changed, already_migrated, manual_review_notes)``:

    - ``was_changed``: True iff this run produced output different from the input.
    - ``already_migrated``: True iff the file is at the codemod's fixed point
      (``was_changed=False``) and contains a v1-exclusive sentinel -- i.e. a
      re-run on previously migrated content. Lets the CLI keep per-file labels
      stable across re-runs instead of flipping ``[MIGRATED]`` to
      ``[NEEDS-MANUAL]`` just because the same advisory notes re-fire.
    - ``manual_review_notes``: ``"<path>:<line>:<col>: <message>"`` strings so
      editors and ``grep -n``-style tools can navigate to each flagged call.
    """
    # Detect the file's dominant newline before universal-newlines normalization
    # eats the original style. Python's default ``write_text`` translates every
    # \n to os.linesep, which silently CRLF-converts LF-only sources on Windows
    # and produces a noise-diff for every line in the file. Preserve fidelity by
    # passing the detected style explicitly on write.
    raw_bytes = path.read_bytes()
    crlf_count = raw_bytes.count(b"\r\n")
    lf_only_count = raw_bytes.count(b"\n") - crlf_count
    source_newline = "\r\n" if crlf_count > lf_only_count else "\n"
    original = path.read_text(encoding="utf-8")
    try:
        migrated = migrate_source(original, client_var=client_var)
    except ValueError as exc:
        print(f"  [SKIP] {path}: {exc}", file=sys.stderr)
        return False, False, []
    # Run the manual-pattern finder against the *migrated* source so the
    # reported <line>:<col> coordinates match the file the user is now looking
    # at on disk. Running it against ``original`` produced stale coordinates
    # whenever the codemod inserted a new import (or otherwise shifted line
    # numbers), so jumping from a CI/editor message landed on the wrong line.
    # Calls that the codemod successfully rewrote no longer match the v0 finder
    # in ``migrated`` -- that is the desired behavior: a rewritten call should
    # not also produce a [MANUAL] note about its v0 shape.
    raw_notes = find_manual_patterns(migrated, client_var=client_var)
    manual = [f"{path}:{note}" for note in raw_notes]
    changed = migrated != original
    if changed and dry_run:
        # Show the proposed edits so --dry-run is actually a preview, not just
        # "this file would change." splitlines(keepends=True) preserves the file's
        # line endings, which difflib needs to produce a stable unified diff.
        diff = difflib.unified_diff(
            original.splitlines(keepends=True),
            migrated.splitlines(keepends=True),
            fromfile=str(path),
            tofile=f"{path} (migrated)",
        )
        sys.stdout.writelines(diff)
    if changed and not dry_run:
        # ``newline=source_newline`` writes the codemod's \n bytes as either \n or
        # \r\n, matching whatever the input used -- so an LF source stays LF, a
        # CRLF source stays CRLF, and git diff shows only the codemod's real edits.
        path.write_text(migrated, encoding="utf-8", newline=source_newline)
    already_migrated = (not changed) and _has_v1_sentinels(original)
    return changed, already_migrated, manual


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
    if "--help" in args or "-h" in args:
        print(__doc__)
        print("\nUsage: dataverse-migrate [--dry-run] [--client-var=NAME] <path> [<path> ...]")
        return 0
    dry_run = "--dry-run" in args
    client_var = "client"
    remaining = []
    for a in args:
        if a == "--dry-run":
            continue
        if a.startswith("--client-var="):
            client_var = a[len("--client-var=") :]
        else:
            remaining.append(a)

    if not remaining:
        print(__doc__)
        print("\nUsage: dataverse-migrate [--dry-run] [--client-var=NAME] <path> [<path> ...]")
        return 1

    targets = _collect_targets(remaining)
    if not targets:
        print("[ERROR] No Python files found.", file=sys.stderr)
        return 1

    changed = skipped = needs_manual = already_migrated_count = manual_total = 0
    for path in targets:
        was_changed, already_migrated, notes = migrate_file(path, dry_run=dry_run, client_var=client_var)
        if was_changed:
            changed += 1
            tag = "[DRY-RUN]" if dry_run else "[MIGRATED]"
            if notes:
                print(f"{tag} {path}  (auto-rewrites applied; manual review still required)")
            else:
                print(f"{tag} {path}")
        elif already_migrated:
            # File is at the codemod's fixed point -- labels stay stable across re-runs.
            already_migrated_count += 1
            if notes:
                print(f"[ALREADY-MIGRATED] {path}  (re-run no-op; manual notes still flagged for review)")
            else:
                print(f"[ALREADY-MIGRATED] {path}  (re-run no-op)")
        elif notes:
            needs_manual += 1
            print(f"[NEEDS-MANUAL] {path}  (no auto-rewrites to apply; manual migration required)")
        else:
            skipped += 1
        for note in notes:
            print(f"  [MANUAL] {note}")
            manual_total += 1

    suffix = "would be " if dry_run else ""
    parts = [f"{changed} file(s) {suffix}auto-migrated"]
    if already_migrated_count:
        parts.append(f"{already_migrated_count} already migrated (re-run no-op)")
    if needs_manual:
        parts.append(f"{needs_manual} need manual-only migration")
    parts.append(f"{skipped} unchanged")
    print(f"\nDone: {', '.join(parts)}.", end="")
    if manual_total:
        print(f" {manual_total} pattern(s) require manual review.")
    else:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
