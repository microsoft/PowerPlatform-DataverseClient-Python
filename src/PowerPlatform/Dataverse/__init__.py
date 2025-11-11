# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Microsoft Dataverse SDK for Python.

This package provides a high-level Python client for interacting with Microsoft Dataverse
environments through the Web API. It supports CRUD operations, SQL queries, table metadata
management, and file uploads with Azure Identity authentication.

Key Features:
    - OData CRUD operations (create, read, update, delete)
    - SQL query support via Web API
    - Table metadata operations (create, inspect, delete custom tables)
    - File column upload capabilities
    - Pandas integration for DataFrame-based operations
    - Azure Identity credential support

.. note::
    This SDK requires Azure Identity credentials for authentication. See the
    `Azure Identity documentation <https://learn.microsoft.com/python/api/overview/azure/identity-readme>`_
    for supported credential types.

Example:
    Basic client initialization and usage::

        from azure.identity import InteractiveBrowserCredential
        from PowerPlatform.Dataverse import DataverseClient

        credential = InteractiveBrowserCredential()
        client = DataverseClient(
            "https://org.crm.dynamics.com",
            credential
        )

        # Create a record
        account_id = client.create("account", {"name": "Contoso"})[0]

        # Query records
        accounts = client.get("account", filter="name eq 'Contoso'")
        for batch in accounts:
            for record in batch:
                print(record["name"])
"""

from .__version__ import __version__
from .client import DataverseClient

__all__ = ["DataverseClient", "__version__"]
