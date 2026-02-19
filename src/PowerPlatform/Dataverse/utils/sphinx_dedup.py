# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Sphinx extension to prevent duplicate docs from __init__.py re-exports.

When ``__init__.py`` re-exports symbols via ``__all__`` and sphinx-apidoc
generates automodule directives for both the package and submodules, classes
appear twice in the rendered documentation.

This extension strips ``:members:`` from submodule automodule blocks when
the parent package has a non-empty ``__all__``, so classes are documented
once at the package level — matching the import path users actually use.

Works with both vanilla sphinx-apidoc and py2docfx (which uses
``--module-first --no-headings --no-toc --implicit-namespaces``).

Usage in conf.py::

    extensions = [..., "PowerPlatform.Dataverse.utils.sphinx_dedup"]

Or via py2docfx JSON config::

    "sphinx_extensions": ["PowerPlatform.Dataverse.utils.sphinx_dedup"]
"""

import importlib
import re

# Matches automodule directives: captures module name and the full option block
_AUTOMODULE_RE = re.compile(
    r"(\.\. automodule:: (\S+)\n)((?:   :\S+:.*\n)*)", re.MULTILINE
)


def _dedup_reexported_submodules(app, docname, source):
    """source-read hook: strip :members: from submodule blocks when
    the parent package re-exports those symbols via __all__."""
    text = source[0]

    # Collect all automodule directives in this RST file
    modules = [(m.group(2), m.start()) for m in _AUTOMODULE_RE.finditer(text)]
    if len(modules) < 2:
        return  # Need at least a package + submodule to have duplicates

    # The package module is the shortest name (e.g. "Foo.Bar.models")
    pkg_name = min(modules, key=lambda x: len(x[0]))[0]

    # Check if this package re-exports anything via __all__
    try:
        pkg = importlib.import_module(pkg_name)
        pkg_all = getattr(pkg, "__all__", [])
    except ImportError:
        return

    if not pkg_all:
        return  # Empty __all__ — keep submodule docs as-is

    # Strip :members: from submodule automodule blocks (not the package block)
    def strip_submodule_members(match):
        directive = match.group(1)  # ".. automodule:: X.Y.Z\n"
        mod_name = match.group(2)  # "X.Y.Z"
        options = match.group(3)  # "   :members:\n   :show-inheritance:\n..."

        if mod_name == pkg_name:
            return directive + options  # Keep package block as-is

        # It's a submodule — strip :members:
        options = re.sub(r"^   :members:.*\n", "", options, flags=re.MULTILINE)
        return directive + options

    source[0] = _AUTOMODULE_RE.sub(strip_submodule_members, text)


def setup(app):
    """Register the dedup hook with Sphinx."""
    app.connect("source-read", _dedup_reexported_submodules)
    return {"version": "1.0", "parallel_read_safe": True}
