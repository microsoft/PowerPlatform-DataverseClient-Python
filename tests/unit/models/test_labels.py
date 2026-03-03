# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for label models."""

from PowerPlatform.Dataverse.models.labels import (
    LocalizedLabel,
    Label,
)


class TestLocalizedLabel:
    """Tests for LocalizedLabel."""

    def test_to_dict_basic(self):
        """Test basic serialization."""
        label = LocalizedLabel(label="Test", language_code=1033)
        result = label.to_dict()

        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.LocalizedLabel"
        assert result["Label"] == "Test"
        assert result["LanguageCode"] == 1033

    def test_to_dict_with_additional_properties(self):
        """Test that additional_properties are merged."""
        label = LocalizedLabel(
            label="Test",
            language_code=1033,
            additional_properties={"IsManaged": True, "MetadataId": "abc-123"},
        )
        result = label.to_dict()

        assert result["Label"] == "Test"
        assert result["IsManaged"] is True
        assert result["MetadataId"] == "abc-123"

    def test_additional_properties_can_override(self):
        """Test that additional_properties can override default values."""
        label = LocalizedLabel(
            label="Original",
            language_code=1033,
            additional_properties={"Label": "Overridden"},
        )
        result = label.to_dict()

        assert result["Label"] == "Overridden"


class TestLabel:
    """Tests for Label."""

    def test_to_dict_basic(self):
        """Test basic serialization with auto UserLocalizedLabel."""
        label = Label(localized_labels=[LocalizedLabel(label="Test", language_code=1033)])
        result = label.to_dict()

        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.Label"
        assert len(result["LocalizedLabels"]) == 1
        assert result["LocalizedLabels"][0]["Label"] == "Test"
        # UserLocalizedLabel should default to first localized label
        assert result["UserLocalizedLabel"]["Label"] == "Test"

    def test_to_dict_with_explicit_user_label(self):
        """Test that explicit user_localized_label is used."""
        label = Label(
            localized_labels=[
                LocalizedLabel(label="English", language_code=1033),
                LocalizedLabel(label="French", language_code=1036),
            ],
            user_localized_label=LocalizedLabel(label="French", language_code=1036),
        )
        result = label.to_dict()

        assert result["UserLocalizedLabel"]["Label"] == "French"
        assert result["UserLocalizedLabel"]["LanguageCode"] == 1036

    def test_to_dict_with_additional_properties(self):
        """Test that additional_properties are merged."""
        label = Label(
            localized_labels=[LocalizedLabel(label="Test", language_code=1033)],
            additional_properties={"CustomProperty": "value"},
        )
        result = label.to_dict()

        assert result["CustomProperty"] == "value"
