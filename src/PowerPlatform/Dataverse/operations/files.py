# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""File operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["FileOperations"]


class FileOperations:
    """Namespace for file operations.

    Accessed via ``client.files``. Provides file upload operations for
    Dataverse file columns.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        client.files.upload(
            "account", account_id, "new_Document", "/path/to/file.pdf"
        )
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- upload

    def upload(
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

                client.files.upload(
                    "account",
                    account_id,
                    "new_Contract",
                    "/path/to/contract.pdf",
                    mime_type="application/pdf",
                )

            Upload with auto mode selection::

                client.files.upload(
                    "email",
                    email_id,
                    "new_Attachment",
                    "/path/to/large_file.zip",
                )
        """
        with self._client._scoped_odata() as od:
            od._upload_file(
                table,
                record_id,
                file_column,
                path,
                mode=mode,
                mime_type=mime_type,
                if_none_match=if_none_match,
            )
