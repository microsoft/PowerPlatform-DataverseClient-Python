# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from importlib.metadata import version

from .models.filters import col, raw
from .models.protocol import DataverseModel
from .models.record import QueryResult

__version__ = version("PowerPlatform-Dataverse-Client")

__all__ = ["__version__", "col", "raw", "DataverseModel", "QueryResult"]
