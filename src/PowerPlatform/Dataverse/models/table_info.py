# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table and column metadata models for Dataverse."""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Iterator, KeysView, List, Optional

__all__ = ["TableInfo", "ColumnInfo", "AlternateKeyInfo", "OptionItem", "OptionSetInfo"]


@dataclass
class ColumnInfo:
    """Column metadata from a Dataverse table definition.

    :param logical_name: Logical name of the column (e.g., ``"emailaddress1"``).
    :type logical_name: :class:`str`
    :param schema_name: Schema name of the column (e.g., ``"EMailAddress1"``).
    :type schema_name: :class:`str`
    :param display_name: Localized display name, or ``None`` if not available.
    :type display_name: :class:`str` or None
    :param description: Column description, or ``None`` if not available.
    :type description: :class:`str` or None
    :param attribute_type: Attribute type (e.g., ``"String"``, ``"Picklist"``).
    :type attribute_type: :class:`str`
    :param attribute_type_name: Attribute type name (e.g., ``"StringType"``).
    :type attribute_type_name: :class:`str` or None
    :param is_custom_attribute: Whether the column is custom.
    :type is_custom_attribute: :class:`bool`
    :param is_primary_id: Whether this is the primary ID column.
    :type is_primary_id: :class:`bool`
    :param is_primary_name: Whether this is the primary name column.
    :type is_primary_name: :class:`bool`
    :param required_level: Required level (e.g., ``"None"``, ``"SystemRequired"``).
    :type required_level: :class:`str` or None
    :param is_valid_for_create: Whether valid for create operations.
    :type is_valid_for_create: :class:`bool`
    :param is_valid_for_update: Whether valid for update operations.
    :type is_valid_for_update: :class:`bool`
    :param is_valid_for_read: Whether valid for read operations.
    :type is_valid_for_read: :class:`bool`
    :param max_length: Maximum length for string columns.
    :type max_length: :class:`int` or None
    :param metadata_id: GUID of the attribute metadata.
    :type metadata_id: :class:`str` or None
    """

    logical_name: str = ""
    schema_name: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    attribute_type: str = ""
    attribute_type_name: Optional[str] = None
    is_custom_attribute: bool = False
    is_primary_id: bool = False
    is_primary_name: bool = False
    required_level: Optional[str] = None
    is_valid_for_create: bool = False
    is_valid_for_update: bool = False
    is_valid_for_read: bool = False
    max_length: Optional[int] = None
    metadata_id: Optional[str] = None

    # ---------------------------------------------- deprecated property aliases

    @property
    def type(self) -> str:
        """Column type name (deprecated, use ``attribute_type_name`` or ``attribute_type``)."""
        warnings.warn(
            "ColumnInfo.type is deprecated. Use attribute_type_name or attribute_type instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.attribute_type_name or self.attribute_type

    @property
    def is_primary(self) -> bool:
        """Whether this is the primary name column (deprecated, use ``is_primary_name``)."""
        warnings.warn(
            "ColumnInfo.is_primary is deprecated. Use is_primary_name instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.is_primary_name

    @property
    def is_required(self) -> bool:
        """Whether the column is required (deprecated, use ``required_level``)."""
        warnings.warn(
            "ColumnInfo.is_required is deprecated. Use required_level instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.required_level not in (None, "", "None")

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> ColumnInfo:
        """Create from a raw Dataverse ``AttributeMetadata`` API response.

        :param data: Raw attribute metadata dict (PascalCase keys).
        :type data: :class:`dict`
        :rtype: :class:`ColumnInfo`
        """
        display_name = None
        dn = data.get("DisplayName")
        if isinstance(dn, dict):
            ull = dn.get("UserLocalizedLabel")
            if isinstance(ull, dict):
                display_name = ull.get("Label")

        description = None
        desc_obj = data.get("Description")
        if isinstance(desc_obj, dict):
            desc_label = desc_obj.get("UserLocalizedLabel")
            if isinstance(desc_label, dict):
                description = desc_label.get("Label")

        attribute_type_name = None
        atn = data.get("AttributeTypeName")
        if isinstance(atn, dict):
            attribute_type_name = atn.get("Value")

        required_level = None
        rl = data.get("RequiredLevel")
        if isinstance(rl, dict):
            required_level = rl.get("Value")

        return cls(
            logical_name=data.get("LogicalName", ""),
            schema_name=data.get("SchemaName", ""),
            display_name=display_name,
            description=description,
            attribute_type=data.get("AttributeType", ""),
            attribute_type_name=attribute_type_name,
            is_custom_attribute=data.get("IsCustomAttribute", False),
            is_primary_id=data.get("IsPrimaryId", False),
            is_primary_name=data.get("IsPrimaryName", False),
            required_level=required_level,
            is_valid_for_create=data.get("IsValidForCreate", False),
            is_valid_for_update=data.get("IsValidForUpdate", False),
            is_valid_for_read=data.get("IsValidForRead", False),
            max_length=data.get("MaxLength"),
            metadata_id=data.get("MetadataId"),
        )


@dataclass
class TableInfo:
    """Table metadata with dict-like backward compatibility.

    Supports both new attribute access (``info.schema_name``) and legacy
    dict-key access (``info["table_schema_name"]``) for backward
    compatibility with code written against the raw dict API.

    :param schema_name: Table schema name (e.g. ``"Account"``).
    :type schema_name: :class:`str`
    :param logical_name: Table logical name (lowercase).
    :type logical_name: :class:`str`
    :param entity_set_name: OData entity set name.
    :type entity_set_name: :class:`str`
    :param metadata_id: Metadata GUID.
    :type metadata_id: :class:`str`
    :param display_name: Human-readable display name.
    :type display_name: :class:`str` or None
    :param description: Table description.
    :type description: :class:`str` or None
    :param columns: Column metadata (when retrieved).
    :type columns: list[ColumnInfo] or None
    :param columns_created: Column schema names created with the table.
    :type columns_created: list[str] or None

    Example::

        info = client.tables.create("new_Product", {"new_Price": "decimal"})
        print(info.schema_name)              # new attribute access
        print(info["table_schema_name"])     # legacy dict-key access
    """

    schema_name: str = ""
    logical_name: str = ""
    entity_set_name: str = ""
    metadata_id: str = ""
    primary_name_attribute: Optional[str] = None
    primary_id_attribute: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[ColumnInfo]] = field(default=None, repr=False)
    columns_created: Optional[List[str]] = field(default=None, repr=False)
    one_to_many_relationships: Optional[List[Dict[str, Any]]] = field(default=None, repr=False)
    many_to_one_relationships: Optional[List[Dict[str, Any]]] = field(default=None, repr=False)
    many_to_many_relationships: Optional[List[Dict[str, Any]]] = field(default=None, repr=False)
    _extra: Dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    # Maps legacy dict keys (used by existing code) to attribute names.
    _LEGACY_KEY_MAP: ClassVar[Dict[str, str]] = {
        "table_schema_name": "schema_name",
        "table_logical_name": "logical_name",
        "entity_set_name": "entity_set_name",
        "metadata_id": "metadata_id",
        "primary_name_attribute": "primary_name_attribute",
        "primary_id_attribute": "primary_id_attribute",
        "columns_created": "columns_created",
    }

    # --------------------------------------------------------- dict-like access

    def _resolve_key(self, key: str) -> str:
        """Resolve a legacy or direct key to an attribute name."""
        return self._LEGACY_KEY_MAP.get(key, key)

    def __getitem__(self, key: str) -> Any:
        if key in self._extra:
            return self._extra[key]
        attr = self._resolve_key(key)
        if hasattr(self, attr):
            val = getattr(self, attr)
            if val is not None or key in self._LEGACY_KEY_MAP:
                return val
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key in self._extra:
            return True
        attr = self._resolve_key(key)
        if not hasattr(self, attr):
            return False
        return getattr(self, attr) is not None

    def __iter__(self) -> Iterator[str]:
        return iter(self._LEGACY_KEY_MAP)

    def __len__(self) -> int:
        return len(self._LEGACY_KEY_MAP)

    def get(self, key: str, default: Any = None) -> Any:
        """Return value for *key*, or *default* if not present."""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> KeysView[str]:
        """Return legacy dict keys."""
        return self._LEGACY_KEY_MAP.keys()

    def values(self) -> List[Any]:
        """Return values corresponding to legacy dict keys."""
        return [getattr(self, attr) for attr in self._LEGACY_KEY_MAP.values()]

    def items(self) -> List[tuple]:
        """Return (legacy_key, value) pairs."""
        return [(k, getattr(self, attr)) for k, attr in self._LEGACY_KEY_MAP.items()]

    # -------------------------------------------------------------- factories

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TableInfo:
        """Create from an SDK internal dict (snake_case keys).

        This handles the dict format returned by ``_create_table`` and
        ``_get_table_info`` in the OData layer.

        :param data: Dictionary with SDK snake_case keys.
        :type data: :class:`dict`
        :rtype: :class:`TableInfo`
        """
        return cls(
            schema_name=data.get("table_schema_name", ""),
            logical_name=data.get("table_logical_name", ""),
            entity_set_name=data.get("entity_set_name", ""),
            metadata_id=data.get("metadata_id", ""),
            primary_name_attribute=data.get("primary_name_attribute"),
            primary_id_attribute=data.get("primary_id_attribute"),
            columns_created=data.get("columns_created"),
        )

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> TableInfo:
        """Create from a raw Dataverse ``EntityDefinition`` API response.

        :param response_data: Raw entity metadata dict (PascalCase keys).
        :type response_data: :class:`dict`
        :rtype: :class:`TableInfo`
        """
        # Extract display name from nested structure
        display_name_obj = response_data.get("DisplayName", {})
        user_label = display_name_obj.get("UserLocalizedLabel") or {}
        display_name = user_label.get("Label")

        # Extract description from nested structure
        desc_obj = response_data.get("Description", {})
        desc_label = desc_obj.get("UserLocalizedLabel") or {}
        description = desc_label.get("Label")

        # Parse columns if Attributes are present
        columns = None
        if "Attributes" in response_data:
            columns = [ColumnInfo.from_api_response(a) for a in response_data["Attributes"]]

        return cls(
            schema_name=response_data.get("SchemaName", ""),
            logical_name=response_data.get("LogicalName", ""),
            entity_set_name=response_data.get("EntitySetName", ""),
            metadata_id=response_data.get("MetadataId", ""),
            primary_name_attribute=response_data.get("PrimaryNameAttribute"),
            primary_id_attribute=response_data.get("PrimaryIdAttribute"),
            display_name=display_name,
            description=description,
            columns=columns,
            one_to_many_relationships=response_data.get("OneToManyRelationships"),
            many_to_one_relationships=response_data.get("ManyToOneRelationships"),
            many_to_many_relationships=response_data.get("ManyToManyRelationships"),
        )

    # -------------------------------------------------------------- conversion

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict with legacy keys for backward compatibility."""
        return {k: getattr(self, attr) for k, attr in self._LEGACY_KEY_MAP.items()}


@dataclass
class AlternateKeyInfo:
    """Alternate key metadata for a Dataverse table.

    :param metadata_id: Key metadata GUID.
    :type metadata_id: :class:`str`
    :param schema_name: Key schema name.
    :type schema_name: :class:`str`
    :param key_attributes: List of column logical names that compose the key.
    :type key_attributes: list[str]
    :param status: Index creation status (``"Active"``, ``"Pending"``, ``"InProgress"``, ``"Failed"``).
    :type status: :class:`str`
    """

    metadata_id: str = ""
    schema_name: str = ""
    key_attributes: List[str] = field(default_factory=list)
    status: str = ""

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> AlternateKeyInfo:
        """Create from raw EntityKeyMetadata API response.

        :param response_data: Raw key metadata dictionary from the Web API.
        :type response_data: :class:`dict`
        :rtype: :class:`AlternateKeyInfo`
        """
        return cls(
            metadata_id=response_data.get("MetadataId", ""),
            schema_name=response_data.get("SchemaName", ""),
            key_attributes=response_data.get("KeyAttributes", []),
            status=response_data.get("EntityKeyIndexStatus", ""),
        )


@dataclass
class OptionItem:
    """A single option/choice value in an option set.

    :param value: Numeric option value.
    :type value: :class:`int`
    :param label: Localized display text, or ``None`` if not available.
    :type label: :class:`str` or None
    """

    value: int = 0
    label: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> OptionItem:
        """Create an ``OptionItem`` from a raw Web API option response.

        :param data: Raw JSON dict for a single option/choice value.
        :type data: :class:`dict`
        :return: Parsed option item.
        :rtype: :class:`OptionItem`
        """
        label = None
        lbl = data.get("Label")
        if isinstance(lbl, dict):
            ull = lbl.get("UserLocalizedLabel")
            if isinstance(ull, dict):
                label = ull.get("Label")
        return cls(value=data.get("Value", 0), label=label)


@dataclass
class OptionSetInfo:
    """Option set definition including all option values.

    .. note::
        For Boolean option sets, options are ordered as
        ``[FalseOption, TrueOption]``. Use :attr:`OptionItem.value` to
        distinguish rather than relying on list index.

    :param name: Option set name.
    :type name: :class:`str` or None
    :param display_name: Localized display name.
    :type display_name: :class:`str` or None
    :param is_global: Whether this is a global option set.
    :type is_global: :class:`bool`
    :param option_set_type: Type (e.g., ``"Picklist"`` or ``"Boolean"``).
    :type option_set_type: :class:`str` or None
    :param options: List of option items.
    :type options: :class:`list` of :class:`OptionItem`
    :param metadata_id: GUID of the option set metadata.
    :type metadata_id: :class:`str` or None
    """

    name: Optional[str] = None
    display_name: Optional[str] = None
    is_global: bool = False
    option_set_type: Optional[str] = None
    options: List[OptionItem] = field(default_factory=list)
    metadata_id: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> OptionSetInfo:
        """Create an ``OptionSetInfo`` from a raw Web API option set response.

        Handles both picklist-style (``Options`` array) and boolean-style
        (``TrueOption``/``FalseOption``) option sets.

        :param data: Raw JSON dict from the Dataverse Web API.
        :type data: :class:`dict`
        :return: Parsed option set info.
        :rtype: :class:`OptionSetInfo`
        """
        display_name = None
        dn = data.get("DisplayName")
        if isinstance(dn, dict):
            ull = dn.get("UserLocalizedLabel")
            if isinstance(ull, dict):
                display_name = ull.get("Label")

        options: List[OptionItem] = []
        raw_options = data.get("Options")
        if isinstance(raw_options, list):
            options = [OptionItem.from_api_response(o) for o in raw_options]
        else:
            false_opt = data.get("FalseOption")
            true_opt = data.get("TrueOption")
            if isinstance(false_opt, dict):
                options.append(OptionItem.from_api_response(false_opt))
            if isinstance(true_opt, dict):
                options.append(OptionItem.from_api_response(true_opt))

        return cls(
            name=data.get("Name"),
            display_name=display_name,
            is_global=data.get("IsGlobal", False),
            option_set_type=data.get("OptionSetType"),
            options=options,
            metadata_id=data.get("MetadataId"),
        )
