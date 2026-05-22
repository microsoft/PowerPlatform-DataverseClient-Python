# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from importlib.metadata import version

# Set __version__ FIRST. Downstream modules (e.g. data/_odata_base.py) import
# this back from the top-level package, so it must be bound before any
# transitive import of those modules runs.
__version__ = version("PowerPlatform-Dataverse-Client")

from .client import DataverseClient  # noqa: E402
from .models.filters import col, raw  # noqa: E402
from .models.protocol import DataverseModel  # noqa: E402
from .models.record import QueryResult  # noqa: E402

__all__ = [
    "DataverseClient",
    "DataverseModel",
    "QueryResult",
    "__version__",
    "col",
    "raw",
]
