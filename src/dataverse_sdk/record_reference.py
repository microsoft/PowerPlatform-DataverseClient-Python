from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class RecordReference:
    """Represents a pointer to a specific Dataverse record.

    Supports identifying a row by:
      * GUID id (primary key value)
      * Alternate key attribute map (one or more attributes that define a unique key)
      * Elastic table partition id (partition_id) optionally combined with id or alternate keys

    Resolution precedence / rules:
      1. If ``id`` is supplied (and optionally ``partition_id``) then that is used.
      2. Else if ``alternate_keys`` provided (and optionally ``partition_id``) they are used.
      3. Exactly one of (id, alternate_keys) must be provided (not both, not neither).

    Parameters
    ----------
    entity_set : str | None
        Plural logical entity set name (e.g. 'accounts'). Optional if caller instead supplies
        ``logical_name`` and the client can resolve the entity set. Provide if already known to
        avoid an extra metadata round trip.
    logical_name : str | None
        Singular logical name (e.g. 'account'). Optional if entity_set provided. One of the two
        (entity_set or logical_name) must be set so the client can build the path.
    id : str | None
        GUID (36 char, hyphenated) id value.
    alternate_keys : dict[str, Any] | None
        Mapping of alternate key attribute logical names to values.
    partition_id : str | None
        Partition id for elastic tables (GUID format). Included inside the key segment.
    primary_id_attribute : str | None
        The primary id attribute logical name (e.g. 'accountid'). Optional; if absent and ``id``
        is provided the client uses bare (id) form. If present, the client may prefer named form
        for readability in some contexts (not required for OData addressing, which uses (id)).
    """

    entity_set: Optional[str] = None
    logical_name: Optional[str] = None
    id: Optional[str] = None
    alternate_keys: Optional[Dict[str, Any]] = None
    partition_id: Optional[str] = None
    primary_id_attribute: Optional[str] = None

    def __post_init__(self) -> None:  # noqa: D401
        has_id = bool(self.id)
        has_alt = bool(self.alternate_keys)
        if has_id == has_alt:  # both true or both false
            raise ValueError("Provide exactly one of 'id' or 'alternate_keys'.")
        if not (self.entity_set or self.logical_name):
            raise ValueError("Provide at least one of entity_set or logical_name.")
        if self.id:
            gid = self.id.strip()
            if len(gid) != 36 or '-' not in gid:
                raise ValueError(f"id does not look like a GUID: {gid}")
        if self.partition_id:
            pid = self.partition_id.strip()
            if len(pid) != 36 or '-' not in pid:
                raise ValueError(f"partition_id does not look like a GUID: {pid}")
        if self.alternate_keys:
            if not isinstance(self.alternate_keys, dict) or not self.alternate_keys:
                raise ValueError("alternate_keys must be a non-empty dict when provided")
            for k, v in self.alternate_keys.items():
                if v is None:
                    raise ValueError(f"Alternate key value for '{k}' cannot be None")
