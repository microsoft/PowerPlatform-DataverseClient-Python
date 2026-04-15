# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async file upload helpers for Dataverse Web API.

Provides :class:`_AsyncFileUploadMixin`, the async counterpart of
:class:`~PowerPlatform.Dataverse.data._upload._FileUploadMixin`.
"""

from __future__ import annotations

import asyncio
import math
import os
from typing import Dict, Optional
from urllib.parse import quote as _url_quote


class _AsyncFileUploadMixin:
    """Async mixin providing file upload capabilities (small + chunked).

    Designed to be composed with :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`.

    Depends on:

    - ``self.api`` — API base URL string.
    - ``self._request()`` — async HTTP coroutine.
    - ``self._format_key()`` — pure sync helper (from
      :class:`~PowerPlatform.Dataverse.data._odata._ODataClient` base).
    - ``self._entity_set_from_schema_name()`` — async coroutine.
    - ``self._get_entity_by_table_schema_name()`` — async coroutine.
    - ``self._get_attribute_metadata()`` — async coroutine.
    - ``self._create_columns()`` — async coroutine.
    - ``self._wait_for_attribute_visibility()`` — async coroutine.
    """

    async def _upload_file(
        self,
        table_schema_name: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file to a Dataverse file column.

        Auto-creates the column if it does not yet exist, then delegates to
        :meth:`_upload_file_small` (< 128 MB) or :meth:`_upload_file_chunk`
        (≥ 128 MB) depending on *mode*.

        :param table_schema_name: Schema name of the table.
        :param record_id: GUID of the target record.
        :param file_name_attribute: Schema name of the file column.
        :param path: Local filesystem path of the file to upload.
        :param mode: ``"auto"`` (default), ``"small"``, or ``"chunk"``.
        :param mime_type: Content-Type override for small-mode uploads.
        :param if_none_match: When ``True`` (default) sets ``If-None-Match: null``
            (create-or-fail); when ``False`` sets ``If-Match: *`` (overwrite).

        :raises FileNotFoundError: If *path* does not exist (auto/small mode).
        :raises ValueError: If *mode* is not a recognised value.
        :raises HttpError: If the Web API request fails.
        """
        # Resolve entity set from table schema name
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        # Check if the file column exists, create it if it doesn't
        entity_metadata = await self._get_entity_by_table_schema_name(table_schema_name)
        if entity_metadata:
            metadata_id = entity_metadata.get("MetadataId")
            if metadata_id:
                attr_metadata = await self._get_attribute_metadata(metadata_id, file_name_attribute)
                if not attr_metadata:
                    # Attribute doesn't exist, create it
                    await self._create_columns(table_schema_name, {file_name_attribute: "file"})
                    # Wait for the attribute to become visible in the data API
                    # Raises RuntimeError with underlying exception if timeout occurs
                    await self._wait_for_attribute_visibility(entity_set, file_name_attribute)

        mode = (mode or "auto").lower()
        if mode == "auto":
            if not await asyncio.to_thread(os.path.isfile, path):
                raise FileNotFoundError(f"File not found: {path}")
            size = await asyncio.to_thread(os.path.getsize, path)
            mode = "small" if size < 128 * 1024 * 1024 else "chunk"

        # Convert schema name to lowercase logical name for URL usage
        logical_name = file_name_attribute.lower()
        if mode == "small":
            await self._upload_file_small(
                entity_set, record_id, logical_name, path, content_type=mime_type, if_none_match=if_none_match
            )
        elif mode == "chunk":
            await self._upload_file_chunk(entity_set, record_id, logical_name, path, if_none_match=if_none_match)
        else:
            raise ValueError(f"Invalid mode '{mode}'. Use 'auto', 'small', or 'chunk'.")

    async def _upload_file_small(
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        content_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file (< 128 MB) via a single PATCH request.

        :param entity_set: Resolved entity set (plural) name.
        :param record_id: GUID of the target record.
        :param file_name_attribute: Logical (lowercase) name of the file column.
        :param path: Local filesystem path of the file to upload.
        :param content_type: MIME type; defaults to ``"application/octet-stream"``.
        :param if_none_match: When ``True`` sets ``If-None-Match: null``;
            when ``False`` sets ``If-Match: *``.

        :raises ValueError: If *record_id* is empty or file exceeds the 128 MB limit.
        :raises FileNotFoundError: If *path* does not exist.
        :raises HttpError: If the Web API request fails.
        """
        if not record_id:
            raise ValueError("record_id required")
        if not await asyncio.to_thread(os.path.isfile, path):
            raise FileNotFoundError(f"File not found: {path}")
        size = await asyncio.to_thread(os.path.getsize, path)
        limit = 128 * 1024 * 1024
        if size > limit:
            raise ValueError(f"File size {size} exceeds single-upload limit {limit}; use chunk mode.")
        # Read entire file in a thread pool to avoid blocking the event loop.
        data = await asyncio.to_thread(_read_file, path)
        fname = os.path.basename(path)  # pure string operation, no I/O
        key = self._format_key(record_id)
        url = f"{self.api}/{entity_set}{key}/{file_name_attribute}"
        headers: Dict[str, str] = {
            "Content-Type": content_type or "application/octet-stream",
            "x-ms-file-name": fname,
        }
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        # Single PATCH upload; allow default success codes (includes 204)
        await self._request("patch", url, headers=headers, data=data)

    async def _upload_file_chunk(
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        if_none_match: bool = True,
    ) -> None:
        """Stream a file using the Dataverse native chunked PATCH protocol.

        Initiates a chunked-upload session (``PATCH`` with
        ``x-ms-transfer-mode: chunked``), reads the server-recommended chunk
        size from the ``x-ms-chunk-size`` response header, then sends each
        chunk with a ``Content-Range`` header.

        Each chunk is read from disk in a thread pool worker so the event loop
        is not blocked between HTTP requests.

        :param entity_set: Resolved entity set (plural) name.
        :param record_id: GUID of the target record.
        :param file_name_attribute: Logical (lowercase) name of the file column.
        :param path: Local filesystem path of the file to upload.
        :param if_none_match: When ``True`` sets ``If-None-Match: null``;
            when ``False`` sets ``If-Match: *``.

        :raises ValueError: If *record_id* is empty or the effective chunk size
            is not positive.
        :raises FileNotFoundError: If *path* does not exist.
        :raises RuntimeError: If the server does not return a ``Location`` header
            with the upload session token.
        :raises HttpError: If any HTTP request fails.
        """
        if not record_id:
            raise ValueError("record_id required")
        if not await asyncio.to_thread(os.path.isfile, path):
            raise FileNotFoundError(f"File not found: {path}")
        total_size = await asyncio.to_thread(os.path.getsize, path)
        fname = os.path.basename(path)  # pure string operation, no I/O
        key = self._format_key(record_id)
        init_url = f"{self.api}/{entity_set}{key}/{file_name_attribute}?x-ms-file-name={_url_quote(fname)}"
        headers: Dict[str, str] = {"x-ms-transfer-mode": "chunked"}
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        response = await self._request("patch", init_url, headers=headers, data=b"")
        location = response.headers.get("Location") or response.headers.get("location")
        if not location:
            raise RuntimeError("Missing Location header with sessiontoken for chunked upload")
        # Use server-recommended chunk size from response header; fall back to 4 MB
        rec_hdr = response.headers.get("x-ms-chunk-size") or response.headers.get("X-MS-CHUNK-SIZE")
        try:
            recommended_size = int(rec_hdr) if rec_hdr else None
        except Exception:
            recommended_size = None
        effective_size = recommended_size or (4 * 1024 * 1024)
        if effective_size <= 0:
            raise ValueError("effective chunk size must be positive")
        total_chunks = int(math.ceil(total_size / effective_size)) if total_size else 1
        uploaded_bytes = 0
        # Open the file handle synchronously (fast fd acquisition), then read
        # each chunk in a thread pool to keep the event loop unblocked.
        fh = open(path, "rb")
        try:
            for _ in range(total_chunks):
                chunk = await asyncio.to_thread(fh.read, effective_size)
                if not chunk:
                    break
                start = uploaded_bytes
                end = start + len(chunk) - 1
                chunk_headers = {
                    "x-ms-file-name": fname,
                    "Content-Type": "application/octet-stream",
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Content-Length": str(len(chunk)),
                }
                # Each chunk returns 206 (partial) or 204 (final). Accept both.
                await self._request("patch", location, headers=chunk_headers, data=chunk, expected=(206, 204))
                uploaded_bytes += len(chunk)
        finally:
            fh.close()


def _read_file(path: str) -> bytes:
    """Read an entire file as bytes (runs in a thread pool worker)."""
    with open(path, "rb") as fh:
        return fh.read()
