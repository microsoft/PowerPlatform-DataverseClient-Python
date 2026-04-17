# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Entity class generator for the Dataverse SDK.

Connects to a live Dataverse environment and generates strongly-typed Python
entity classes (``Entity`` / ``Field`` subclasses) from the table metadata.

Quickstart::

    from azure.identity import InteractiveBrowserCredential
    from PowerPlatform.Dataverse.client import DataverseClient
    from PowerPlatform.Dataverse.generator import generate

    credential = InteractiveBrowserCredential()
    client = DataverseClient("https://yourorg.crm.dynamics.com", credential)

    generate(
        client,
        entities=["account", "contact", "new_mycustomtable"],
        output_dir="Types/",
    )

Or from the command line::

    python -m PowerPlatform.Dataverse.generator \\
        --url https://yourorg.crm.dynamics.com \\
        --entities account contact new_mycustomtable \\
        --output Types/
"""

from __future__ import annotations

import os
from typing import List, Optional, TYPE_CHECKING

from ._fetch import list_entities, list_attributes
from ._codegen import generate_entity_source, generate_init_source, _to_class_name

if TYPE_CHECKING:
    from ..client import DataverseClient

__all__ = ["generate"]


def generate(
    client: "DataverseClient",
    entities: Optional[List[str]] = None,
    output_dir: str = "Types",
    *,
    verbose: bool = True,
) -> List[str]:
    """Generate typed Python entity class files from a live Dataverse environment.

    For each entity a separate ``<logical_name>.py`` file is written to
    *output_dir*.  An ``__init__.py`` that re-exports all classes is also
    written so callers can do ``from Types import Account, Contact``.

    :param client: An authenticated :class:`~PowerPlatform.Dataverse.client.DataverseClient`.
    :param entities: Optional list of table *logical names* to generate (e.g.
        ``["account", "contact", "new_mycustomtable"]``).  When ``None``,
        **all** non-private entities in the org are generated — this may be a
        large number; filtering to the tables you actually use is recommended.
    :param output_dir: Directory where the generated ``*.py`` files are
        written.  Created if it does not exist.
    :param verbose: Print progress to stdout (default ``True``).
    :returns: List of file paths that were written.

    :raises RuntimeError: If the Dataverse metadata API is unreachable or
        returns an unexpected response.

    Example output structure::

        Types/
        ├── __init__.py        ← re-exports all classes
        ├── account.py         ← class Account(Entity, …)
        ├── contact.py         ← class Contact(Entity, …)
        └── new_mycustomtable.py
    """
    odata = client._get_odata()  # lazily-initialized internal _ODataClient

    # ------------------------------------------------------------------
    # 1. Fetch entity metadata
    # ------------------------------------------------------------------
    if verbose:
        scope = f"{len(entities)} table(s)" if entities else "all non-private tables"
        print(f"[generator] Fetching entity definitions ({scope})…")

    entity_rows = list_entities(odata, logical_names=entities)

    if not entity_rows:
        if verbose:
            print("[generator] No entities found — nothing generated.")
        return []

    if verbose:
        print(f"[generator] Found {len(entity_rows)} entity(ies).")

    # ------------------------------------------------------------------
    # 2. Ensure output directory exists
    # ------------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # 3. Generate one file per entity
    # ------------------------------------------------------------------
    written: List[str] = []
    class_entries: List[tuple[str, str]] = []  # (module_name, class_name)

    for ent in entity_rows:
        logical   = ent.get("LogicalName", "")
        schema    = ent.get("SchemaName", logical)
        meta_id   = ent.get("MetadataId", "")
        class_name = _to_class_name(schema)

        if not logical or not meta_id:
            if verbose:
                print(f"[generator]   SKIP (missing LogicalName or MetadataId): {ent}")
            continue

        if verbose:
            print(f"[generator]   Fetching attributes for {logical} ({class_name})…", end=" ", flush=True)

        attrs = list_attributes(odata, meta_id)

        if verbose:
            print(f"{len(attrs)} attribute(s).")

        source = generate_entity_source(ent, attrs)

        # Use logical_name as filename — safe for filesystem (always lowercase ASCII)
        filename = f"{logical}.py"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as fh:
            fh.write(source)

        written.append(filepath)
        class_entries.append((logical, class_name))

    # ------------------------------------------------------------------
    # 4. Write __init__.py
    # ------------------------------------------------------------------
    init_path = os.path.join(output_dir, "__init__.py")
    with open(init_path, "w", encoding="utf-8") as fh:
        fh.write(generate_init_source(class_entries))
    written.append(init_path)

    if verbose:
        print(f"[generator] Done. {len(written)} file(s) written to '{output_dir}/'.")

    return written
