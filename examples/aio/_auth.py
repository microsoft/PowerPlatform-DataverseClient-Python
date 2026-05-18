# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async credential helper for the async example scripts.

azure-identity's InteractiveBrowserCredential is only available in the sync
namespace (azure.identity), not the async one (azure.identity.aio).  This
module wraps the sync credential so it satisfies the AsyncTokenCredential
protocol required by AsyncDataverseClient.

Usage::

    from _auth import AsyncInteractiveBrowserCredential

    credential = AsyncInteractiveBrowserCredential()
    try:
        async with AsyncDataverseClient(org_url, credential) as client:
            ...
    finally:
        await credential.close()
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

from azure.identity import InteractiveBrowserCredential


class AsyncInteractiveBrowserCredential:
    """
    Async wrapper around the sync InteractiveBrowserCredential.

    get_token() is dispatched to a dedicated thread so the event loop stays
    free during the browser popup / token exchange.  Subsequent calls hit the
    in-process token cache and return almost immediately.
    """

    def __init__(self, **kwargs):
        self._credential = InteractiveBrowserCredential(**kwargs)
        self._executor = ThreadPoolExecutor(max_workers=1)

    async def get_token(self, *scopes, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self._credential.get_token(*scopes, **kwargs),
        )

    async def close(self):
        self._executor.shutdown(wait=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
