# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from importlib.metadata import version

# Set __version__ FIRST. Downstream modules (e.g. data/_odata_base.py) import
# this back from the top-level package, so it must be bound before any
# transitive import of those modules runs.
__version__ = version("PowerPlatform-Dataverse-Client")

from .client import DataverseClient
from .models.filters import col, raw
from .models.protocol import DataverseModel
from .models.record import QueryResult

__all__ = [
    "DataverseClient",
    "DataverseModel",
    "QueryResult",
    "__version__",
    "col",
    "raw",
]
