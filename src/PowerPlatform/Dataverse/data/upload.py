# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""File upload helpers."""

from __future__ import annotations

from typing import Optional


class ODataFileUpload:
    """File upload capabilities (small + chunk) with auto selection."""

    def upload_file(
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file to a Dataverse file column with automatic method selection.

        Parameters
        ----------
        entity_set : ``str``
            Target entity set (plural logical name), e.g. "accounts".
        record_id : ``str``
            GUID of the target record.
        file_name_attribute : ``str``
            Logical name of the file column attribute
        path : ``str``
            Local filesystem path to the file.
        mode : ``str`` | ``None``
            Upload strategy: "auto" (default), "small", or "chunk".
        mime_type : ``str`` | ``None``
            Explicit MIME type. If omitted falls back to application/octet-stream.
        if_none_match : ``bool``
            When True (default) only succeeds if column empty. When False overwrites (If-Match: *).
        """
        import os

        mode = (mode or "auto").lower()

        if mode == "auto":
            if not os.path.isfile(path):
                raise FileNotFoundError(f"File not found: {path}")
            size = os.path.getsize(path)
            mode = "small" if size < 128 * 1024 * 1024 else "chunk"

        if mode == "small":
            return self._upload_file_small(
                entity_set, record_id, file_name_attribute, path, content_type=mime_type, if_none_match=if_none_match
            )
        if mode == "chunk":
            return self._upload_file_chunk(
                entity_set, record_id, file_name_attribute, path, if_none_match=if_none_match
            )
        raise ValueError(f"Invalid mode '{mode}'. Use 'auto', 'small', or 'chunk'.")

    def _upload_file_small(
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        content_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file (<128MB) via single PATCH."""
        import os

        if not record_id:
            raise ValueError("record_id required")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        size = os.path.getsize(path)
        limit = 128 * 1024 * 1024
        if size > limit:
            raise ValueError(f"File size {size} exceeds single-upload limit {limit}; use chunk mode.")
        with open(path, "rb") as fh:
            data = fh.read()
        fname = os.path.basename(path)
        key = self._format_key(record_id)
        url = f"{self.api}/{entity_set}{key}/{file_name_attribute}"
        headers = {
            "Content-Type": content_type or "application/octet-stream",
            "x-ms-file-name": fname,
        }
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        # Single PATCH upload; allow default success codes (includes 204)
        self._request("patch", url, headers=headers, data=data)
        return None

    def _upload_file_chunk(
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        if_none_match: bool = True,
    ) -> None:
        """Stream a local file using Dataverse native chunked PATCH protocol.
        1. Initial PATCH with header x-ms-transfer-mode: chunked (empty body) to start session.
        2. Subsequent PATCH calls to Location URL including sessiontoken with binary body segments and headers. Returns 206 for partial chunks and 204 on final.

        Parameters
        ----------
        entity_set : ``str``
            Target entity set (plural logical name), e.g. "accounts".
        record_id : ``str``
            GUID of the target record.
        file_name_attribute : ``str``
            Logical name of the file column attribute.
        path : ``str``
            Local filesystem path to the file.
        if_none_match : ``bool``
            When True sends ``If-None-Match: null`` to only succeed if the column is currently empty.
            Set False to always overwrite (uses ``If-Match: *``).
        Returns
        -------
        ``None``
            Returns nothing on success. Any failure raises an exception.
        """
        import os, math
        from urllib.parse import quote

        if not record_id:
            raise ValueError("record_id required")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        total_size = os.path.getsize(path)
        fname = os.path.basename(path)
        key = self._format_key(record_id)
        init_url = f"{self.api}/{entity_set}{key}/{file_name_attribute}?x-ms-file-name={quote(fname)}"
        headers = {
            "x-ms-transfer-mode": "chunked",
        }
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        r_init = self._request("patch", init_url, headers=headers, data=b"")
        location = r_init.headers.get("Location") or r_init.headers.get("location")
        if not location:
            raise RuntimeError("Missing Location header with sessiontoken for chunked upload")
        rec_hdr = r_init.headers.get("x-ms-chunk-size") or r_init.headers.get("X-MS-CHUNK-SIZE")
        try:
            recommended_size = int(rec_hdr) if rec_hdr else None
        except Exception:  # noqa: BLE001
            recommended_size = None
        effective_size = recommended_size or (4 * 1024 * 1024)
        if effective_size <= 0:
            raise ValueError("effective chunk size must be positive")
        total_chunks = int(math.ceil(total_size / effective_size)) if total_size else 1
        uploaded_bytes = 0
        with open(path, "rb") as fh:
            for idx in range(total_chunks):
                chunk = fh.read(effective_size)
                if not chunk:
                    break
                start = uploaded_bytes
                end = start + len(chunk) - 1
                c_headers = {
                    "x-ms-file-name": fname,
                    "Content-Type": "application/octet-stream",
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Content-Length": str(len(chunk)),
                }
                # Each chunk returns 206 (partial) or 204 (final). Accept both.
                self._request("patch", location, headers=c_headers, data=chunk, expected=(206, 204))
                uploaded_bytes += len(chunk)
        return None
