# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Optional extensions for the Dataverse SDK.

Extensions provide higher-level convenience functions built on top of the core SDK.
"""

from .relationships import create_lookup_field

__all__ = ["create_lookup_field"]
