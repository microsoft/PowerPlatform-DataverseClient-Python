# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for metadata models."""

from PowerPlatform.Dataverse.models.metadata import (
    ColumnMetadata,
    OptionItem,
    OptionSetInfo,
)


class TestColumnMetadata:
    """Tests for ColumnMetadata."""

    def test_from_api_response_full(self):
        """Test full API response maps all 13 fields correctly."""
        data = {
            "@odata.type": "#Microsoft.Dynamics.CRM.StringAttributeMetadata",
            "LogicalName": "emailaddress1",
            "SchemaName": "EMailAddress1",
            "DisplayName": {
                "UserLocalizedLabel": {"Label": "Email", "LanguageCode": 1033},
            },
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "IsCustomAttribute": False,
            "IsPrimaryId": False,
            "IsPrimaryName": False,
            "RequiredLevel": {"Value": "None"},
            "IsValidForCreate": True,
            "IsValidForUpdate": True,
            "IsValidForRead": True,
            "MetadataId": "def-456",
        }
        col = ColumnMetadata.from_api_response(data)
        assert col.logical_name == "emailaddress1"
        assert col.schema_name == "EMailAddress1"
        assert col.display_name == "Email"
        assert col.attribute_type == "String"
        assert col.attribute_type_name == "StringType"
        assert col.is_custom_attribute is False
        assert col.is_primary_id is False
        assert col.is_primary_name is False
        assert col.required_level == "None"
        assert col.is_valid_for_create is True
        assert col.is_valid_for_update is True
        assert col.is_valid_for_read is True
        assert col.metadata_id == "def-456"

    def test_from_api_response_minimal(self):
        """Test minimal dict with only LogicalName and SchemaName uses defaults."""
        data = {"LogicalName": "name", "SchemaName": "Name"}
        col = ColumnMetadata.from_api_response(data)
        assert col.logical_name == "name"
        assert col.schema_name == "Name"
        assert col.display_name is None
        assert col.attribute_type == ""
        assert col.attribute_type_name is None
        assert col.is_custom_attribute is False
        assert col.is_primary_id is False
        assert col.is_primary_name is False
        assert col.required_level is None
        assert col.is_valid_for_create is False
        assert col.is_valid_for_update is False
        assert col.is_valid_for_read is False
        assert col.metadata_id is None

    def test_display_name_nested_none(self):
        """Test DisplayName exists but UserLocalizedLabel is None."""
        data = {
            "LogicalName": "col",
            "SchemaName": "Col",
            "DisplayName": {"UserLocalizedLabel": None},
        }
        col = ColumnMetadata.from_api_response(data)
        assert col.display_name is None

    def test_display_name_missing_entirely(self):
        """Test dict without DisplayName key."""
        data = {"LogicalName": "col", "SchemaName": "Col"}
        col = ColumnMetadata.from_api_response(data)
        assert col.display_name is None

    def test_required_level_extraction(self):
        """Test RequiredLevel.Value is extracted correctly."""
        data = {
            "LogicalName": "col",
            "SchemaName": "Col",
            "RequiredLevel": {"Value": "ApplicationRequired"},
        }
        col = ColumnMetadata.from_api_response(data)
        assert col.required_level == "ApplicationRequired"


class TestOptionItem:
    """Tests for OptionItem."""

    def test_from_api_response(self):
        """Test option with Value and Label."""
        data = {
            "Value": 1,
            "Label": {
                "UserLocalizedLabel": {"Label": "Preferred Customer", "LanguageCode": 1033},
            },
        }
        opt = OptionItem.from_api_response(data)
        assert opt.value == 1
        assert opt.label == "Preferred Customer"

    def test_from_api_response_no_label(self):
        """Test option with Value but Label.UserLocalizedLabel is None."""
        data = {"Value": 2, "Label": {"UserLocalizedLabel": None}}
        opt = OptionItem.from_api_response(data)
        assert opt.value == 2
        assert opt.label is None


class TestOptionSetInfo:
    """Tests for OptionSetInfo."""

    def test_from_api_response_picklist(self):
        """Test picklist-style OptionSet with Options array."""
        data = {
            "Name": "account_accountcategorycode",
            "DisplayName": {"UserLocalizedLabel": {"Label": "Category", "LanguageCode": 1033}},
            "IsGlobal": False,
            "OptionSetType": "Picklist",
            "Options": [
                {"Value": 1, "Label": {"UserLocalizedLabel": {"Label": "Preferred Customer"}}},
                {"Value": 2, "Label": {"UserLocalizedLabel": {"Label": "Standard"}}},
            ],
            "MetadataId": "meta-guid",
        }
        opt_set = OptionSetInfo.from_api_response(data)
        assert opt_set.option_set_type == "Picklist"
        assert opt_set.name == "account_accountcategorycode"
        assert opt_set.display_name == "Category"
        assert opt_set.is_global is False
        assert len(opt_set.options) == 2
        assert opt_set.options[0].value == 1
        assert opt_set.options[0].label == "Preferred Customer"
        assert opt_set.options[1].value == 2
        assert opt_set.options[1].label == "Standard"
        assert opt_set.metadata_id == "meta-guid"

    def test_from_api_response_boolean(self):
        """Test boolean-style OptionSet with TrueOption and FalseOption."""
        data = {
            "OptionSetType": "Boolean",
            "TrueOption": {"Value": 1, "Label": {"UserLocalizedLabel": {"Label": "Do Not Allow"}}},
            "FalseOption": {"Value": 0, "Label": {"UserLocalizedLabel": {"Label": "Allow"}}},
        }
        opt_set = OptionSetInfo.from_api_response(data)
        assert opt_set.option_set_type == "Boolean"
        assert len(opt_set.options) == 2
        values = [o.value for o in opt_set.options]
        assert 0 in values
        assert 1 in values
        labels = {o.value: o.label for o in opt_set.options}
        assert labels[0] == "Allow"
        assert labels[1] == "Do Not Allow"

    def test_from_api_response_empty_options(self):
        """Test OptionSet with empty Options array."""
        data = {"Options": [], "OptionSetType": "Picklist"}
        opt_set = OptionSetInfo.from_api_response(data)
        assert opt_set.options == []
        assert opt_set.option_set_type == "Picklist"

    def test_from_api_response_global_optionset(self):
        """Test OptionSet with IsGlobal True."""
        data = {
            "Name": "global_options",
            "IsGlobal": True,
            "Options": [],
            "OptionSetType": "Picklist",
        }
        opt_set = OptionSetInfo.from_api_response(data)
        assert opt_set.is_global is True
        assert opt_set.name == "global_options"
