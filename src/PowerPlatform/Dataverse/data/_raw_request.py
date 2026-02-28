# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Resolved HTTP request dataclass shared by _odata.py and _batch.py."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

__all__ = []


@dataclass
class _RawRequest:
    """A fully-resolved HTTP request ready for execution or multipart serialisation.

    Used by ``_ODataClient._build_*`` methods to return a constructed request
    without executing it, and by ``_BatchClient`` to serialise the batch body.

    :param method: HTTP method (``GET``, ``POST``, ``PATCH``, ``DELETE``).
    :param url: Absolute URL (``https://org.crm.dynamics.com/api/data/v9.2/...``).
    :param body: JSON-serialised request body, or ``None`` for bodyless requests.
    :param headers: Extra inner-request headers (e.g. ``{"If-Match": "*"}``).
    :param content_id: Emits a ``Content-ID: n`` header in the MIME part when set.
        Only relevant for changeset items; enables ``$n`` URI references.
    """

    method: str
    url: str
    body: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    content_id: Optional[int] = None
