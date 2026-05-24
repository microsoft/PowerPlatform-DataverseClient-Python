"""Minimal Sphinx config to reproduce the doc-generation behavior locally."""

import os
import sys

# Make the local source importable
sys.path.insert(0, os.path.abspath("../src"))

project = "PowerPlatform-Dataverse-Client"
author = "Microsoft Corporation"
release = "0.1.0b11"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "autoapi.extension",
]

# autoapi: auto-discover the package from source
autoapi_type = "python"
autoapi_dirs = ["../src/PowerPlatform"]
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    # 'imported-members' deliberately omitted: when re-exports are present in
    # __all__ (as in our package __init__.py files), 'imported-members' causes
    # autoapi to generate duplicate doc pages — one at the canonical module
    # path and one at the re-export path. Without it, only the canonical page
    # is generated and 'more than one target' warnings disappear.
]
autoapi_keep_files = True
autoapi_python_class_content = "both"

# Make warnings visible and counted
nitpicky = True
suppress_warnings = []

html_theme = "alabaster"
exclude_patterns = ["_build"]
