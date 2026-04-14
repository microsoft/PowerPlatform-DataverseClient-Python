# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async Dataverse SDK entry point.

Provides :class:`AsyncDataverseClient` — the async counterpart of
:class:`~PowerPlatform.Dataverse.client.DataverseClient`.

Usage::

    from azure.identity.aio import ClientSecretCredential
    from PowerPlatform.Dataverse.aio import AsyncDataverseClient

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    async with AsyncDataverseClient("https://org.crm.dynamics.com", credential) as client:
        guid = await client.records.create("account", {"name": "Contoso"})
        record = await client.records.get("account", guid)
        await client.records.delete("account", guid)
"""

from .async_client import AsyncDataverseClient

__all__ = ["AsyncDataverseClient"]
