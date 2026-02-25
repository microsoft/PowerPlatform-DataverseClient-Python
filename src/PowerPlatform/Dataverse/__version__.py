# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Version information for PowerPlatform-Dataverse-Client package."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("PowerPlatform-Dataverse-Client")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
