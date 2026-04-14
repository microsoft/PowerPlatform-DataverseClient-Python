# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async file operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient

__all__ = ["AsyncFileOperations"]


class AsyncFileOperations:
    """Async namespace for file operations.

    Accessed via ``client.files``.  Provides file upload operations for
    Dataverse file columns.  Async counterpart of
    :class:`~PowerPlatform.Dataverse.operations.files.FileOperations`.

    :param client: The parent
        :class:`~PowerPlatform.Dataverse.aio.AsyncDataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.aio.AsyncDataverseClient

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

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param record_id: GUID of the target record.
        :type record_id: :class:`str`
        :param file_column: Schema name of the file column attribute (e.g.,
            ``"new_Document"``). If the column doesn't exist, it will be
            created automatically.
        :type file_column: :class:`str`
        :param path: Local filesystem path to the file. The stored filename
            will be the basename of this path.
        :type path: :class:`str`
        :param mode: Upload strategy: ``"auto"`` (default), ``"small"``, or
            ``"chunk"``. Auto mode selects small or chunked upload based on
            file size.
        :type mode: :class:`str` or None
        :param mime_type: Explicit MIME type to store with the file (e.g.
            ``"application/pdf"``). If not provided, defaults to
            ``"application/octet-stream"``.
        :type mime_type: :class:`str` or None
        :param if_none_match: When True (default), sends
            ``If-None-Match: null`` header to only succeed if the column is
            currently empty. Set False to always overwrite using
            ``If-Match: *``.
        :type if_none_match: :class:`bool`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the upload fails or the file column is not empty when
            ``if_none_match=True``.
        :raises FileNotFoundError: If the specified file path does not exist.

        Example:
            Upload a PDF file::

                await client.files.upload(
                    "account",
                    account_id,
                    "new_Contract",
                    "/path/to/contract.pdf",
                    mime_type="application/pdf",
                )

            Upload with auto mode selection::

                await client.files.upload(
                    "email",
                    email_id,
                    "new_Attachment",
                    "/path/to/large_file.zip",
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
