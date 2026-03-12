# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async file operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncFileOperations"]


class AsyncFileOperations:
    """Async namespace for file operations.

    Accessed via ``client.files`` on
    :class:`~PowerPlatform.Dataverse.async_client.AsyncDataverseClient`.

    :param client: The parent async client instance.
    :type client: ~PowerPlatform.Dataverse.async_client.AsyncDataverseClient

    Example::

        async with AsyncDataverseClient(base_url, credential) as client:
            await client.files.upload(
                "account", account_id, "new_Document", "/path/to/file.pdf"
            )
    """

    def __init__(self, client: AsyncDataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- upload

    async def upload(
        self,
        table: str,
        record_id: str,
        file_column: str,
        path: str,
        *,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file to a Dataverse file column.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param record_id: GUID of the target record.
        :type record_id: :class:`str`
        :param file_column: Schema name of the file column attribute. If the
            column doesn't exist, it will be created automatically.
        :type file_column: :class:`str`
        :param path: Local filesystem path to the file.
        :type path: :class:`str`
        :param mode: Upload strategy: ``"auto"`` (default), ``"small"``, or ``"chunk"``.
        :type mode: :class:`str` or None
        :param mime_type: Explicit MIME type. If not provided, defaults to
            ``"application/octet-stream"``.
        :type mime_type: :class:`str` or None
        :param if_none_match: When True (default), only succeeds if the column
            is currently empty. Set False to overwrite.
        :type if_none_match: :class:`bool`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the upload fails.
        :raises FileNotFoundError: If the specified file path does not exist.

        Example::

            await client.files.upload(
                "account",
                account_id,
                "new_Contract",
                "/path/to/contract.pdf",
                mime_type="application/pdf",
            )
        """
        async with self._client._scoped_odata() as od:
            await od._upload_file(
                table,
                record_id,
                file_column,
                path,
                mode=mode,
                mime_type=mime_type,
                if_none_match=if_none_match,
            )
