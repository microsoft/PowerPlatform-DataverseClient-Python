# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Label models for Dataverse metadata."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from ..common.constants import (
    ODATA_TYPE_LOCALIZED_LABEL,
    ODATA_TYPE_LABEL,
)


@dataclass
class LocalizedLabel:
    """
    Represents a localized label with a language code.

    :param label: The text of the label.
    :type label: str
    :param language_code: The language code (LCID), e.g., 1033 for English.
    :type language_code: int
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    label: str
    language_code: int
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> label = LocalizedLabel(label="Account", language_code=1033)
            >>> label.to_dict()
            {
                '@odata.type': 'Microsoft.Dynamics.CRM.LocalizedLabel',
                'Label': 'Account',
                'LanguageCode': 1033
            }
        """
        result = {
            "@odata.type": ODATA_TYPE_LOCALIZED_LABEL,
            "Label": self.label,
            "LanguageCode": self.language_code,
        }
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


@dataclass
class Label:
    """
    Represents a label that can have multiple localized versions.

    :param localized_labels: List of LocalizedLabel instances.
    :type localized_labels: List[LocalizedLabel]
    :param user_localized_label: Optional user-specific localized label.
    :type user_localized_label: Optional[LocalizedLabel]
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    localized_labels: List[LocalizedLabel]
    user_localized_label: Optional[LocalizedLabel] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> label = Label(localized_labels=[LocalizedLabel("Account", 1033)])
            >>> label.to_dict()
            {
                '@odata.type': 'Microsoft.Dynamics.CRM.Label',
                'LocalizedLabels': [
                    {'@odata.type': '...', 'Label': 'Account', 'LanguageCode': 1033}
                ],
                'UserLocalizedLabel': {'@odata.type': '...', 'Label': 'Account', ...}
            }
        """
        result = {
            "@odata.type": ODATA_TYPE_LABEL,
            "LocalizedLabels": [ll.to_dict() for ll in self.localized_labels],
        }
        # Use explicit user_localized_label, or default to first localized label
        if self.user_localized_label:
            result["UserLocalizedLabel"] = self.user_localized_label.to_dict()
        elif self.localized_labels:
            result["UserLocalizedLabel"] = self.localized_labels[0].to_dict()
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


__all__ = ["LocalizedLabel", "Label"]
