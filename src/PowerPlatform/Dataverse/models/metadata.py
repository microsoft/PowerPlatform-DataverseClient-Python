# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Metadata models for table column and option set definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

__all__ = ["ColumnMetadata", "OptionItem", "OptionSetInfo"]


@dataclass
class ColumnMetadata:
    """
    Metadata for a single table column (attribute).

    :param logical_name: Logical name of the column (e.g., ``"emailaddress1"``).
    :type logical_name: :class:`str`
    :param schema_name: Schema name of the column (e.g., ``"EMailAddress1"``).
    :type schema_name: :class:`str`
    :param display_name: Localized display name, or ``None`` if not available.
    :type display_name: :class:`str` or None
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
    :param metadata_id: GUID of the attribute metadata.
    :type metadata_id: :class:`str` or None
    """

    logical_name: str = ""
    schema_name: str = ""
    display_name: Optional[str] = None
    attribute_type: str = ""
    attribute_type_name: Optional[str] = None
    is_custom_attribute: bool = False
    is_primary_id: bool = False
    is_primary_name: bool = False
    required_level: Optional[str] = None
    is_valid_for_create: bool = False
    is_valid_for_update: bool = False
    is_valid_for_read: bool = False
    metadata_id: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> ColumnMetadata:
        """Create a ``ColumnMetadata`` from a raw Web API attribute response.

        :param data: Raw JSON dict from the Dataverse Web API.
        :type data: :class:`dict`
        :return: Parsed column metadata instance.
        :rtype: :class:`ColumnMetadata`
        """
        display_name = None
        dn = data.get("DisplayName")
        if isinstance(dn, dict):
            ull = dn.get("UserLocalizedLabel")
            if isinstance(ull, dict):
                display_name = ull.get("Label")

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
            attribute_type=data.get("AttributeType", ""),
            attribute_type_name=attribute_type_name,
            is_custom_attribute=data.get("IsCustomAttribute", False),
            is_primary_id=data.get("IsPrimaryId", False),
            is_primary_name=data.get("IsPrimaryName", False),
            required_level=required_level,
            is_valid_for_create=data.get("IsValidForCreate", False),
            is_valid_for_update=data.get("IsValidForUpdate", False),
            is_valid_for_read=data.get("IsValidForRead", False),
            metadata_id=data.get("MetadataId"),
        )


@dataclass
class OptionItem:
    """
    A single option/choice value in an option set.

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
    """
    Option set definition including all option values.

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
