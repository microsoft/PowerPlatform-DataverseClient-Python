"""PowerPlatform.FinOps — Python SDK for Microsoft Dynamics 365 Finance & Operations.

Step 1 (this branch) covers the four CRUD operations against the FinOps OData
endpoint (``/data/{EntitySet}``). See ``FinOps-SDK-Plan.docx`` for the full roadmap.
"""
from .client import FinOpsClient
from .errors import (
    FinOpsError,
    FinOpsAuthError,
    FinOpsHttpError,
    FinOpsNotFoundError,
    FinOpsConcurrencyError,
    FinOpsThrottledError,
)

__all__ = [
    "FinOpsClient",
    "FinOpsError",
    "FinOpsAuthError",
    "FinOpsHttpError",
    "FinOpsNotFoundError",
    "FinOpsConcurrencyError",
    "FinOpsThrottledError",
]
__version__ = "0.0.1.dev0"
