# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for metadata models."""

from PowerPlatform.Dataverse.models.table_info import (
    ColumnInfo,
    OptionItem,
    OptionSetInfo,
)
from tests.fixtures.test_data import (
    ACCOUNT_NAME_COLUMN,
    BOOLEAN_OPTIONSET,
    EMAILADDRESS1_COLUMN,
    PICKLIST_COLUMN,
    PICKLIST_OPTIONSET,
    STATE_COLUMN,
    STATE_OPTIONSET,
    STATUS_COLUMN,
    STATUS_OPTIONSET,
    UNIQUEID_COLUMN,
)


class TestColumnInfo:
    """Tests for ColumnInfo."""

    def test_from_api_response_full(self):
        """Test full API response maps all 13 fields correctly."""
        col = ColumnInfo.from_api_response(EMAILADDRESS1_COLUMN)
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
        assert col.metadata_id == "024a2ee3-b983-4fd8-8991-f8d548a227e0"

    def test_from_api_response_minimal(self):
        """Test minimal dict with only LogicalName and SchemaName uses defaults."""
        data = {"LogicalName": "name", "SchemaName": "Name"}
        col = ColumnInfo.from_api_response(data)
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
        col = ColumnInfo.from_api_response(data)
        assert col.display_name is None

    def test_display_name_missing_entirely(self):
        """Test dict without DisplayName key."""
        data = {"LogicalName": "col", "SchemaName": "Col"}
        col = ColumnInfo.from_api_response(data)
        assert col.display_name is None

    def test_from_api_response_primary_name_column(self):
        """Test primary name column (account.name) with ApplicationRequired level."""
        col = ColumnInfo.from_api_response(ACCOUNT_NAME_COLUMN)
        assert col.logical_name == "name"
        assert col.schema_name == "Name"
        assert col.display_name == "Account Name"
        assert col.is_primary_name is True
        assert col.is_primary_id is False
        assert col.required_level == "ApplicationRequired"

    def test_from_api_response_picklist_column(self):
        """Test picklist column (account.accountcategorycode) maps correctly."""
        col = ColumnInfo.from_api_response(PICKLIST_COLUMN)
        assert col.logical_name == "accountcategorycode"
        assert col.schema_name == "AccountCategoryCode"
        assert col.attribute_type == "Picklist"
        assert col.attribute_type_name == "PicklistType"
        assert col.display_name == "Category"
        assert col.required_level == "None"

    def test_from_api_response_status_column(self):
        """Test status column (account.statuscode) maps correctly."""
        col = ColumnInfo.from_api_response(STATUS_COLUMN)
        assert col.logical_name == "statuscode"
        assert col.schema_name == "StatusCode"
        assert col.attribute_type == "Status"
        assert col.attribute_type_name == "StatusType"
        assert col.display_name == "Status Reason"

    def test_from_api_response_state_column(self):
        """Test state column (contact.statecode) maps correctly."""
        col = ColumnInfo.from_api_response(STATE_COLUMN)
        assert col.logical_name == "statecode"
        assert col.schema_name == "StateCode"
        assert col.attribute_type == "State"
        assert col.attribute_type_name == "StateType"
        assert col.display_name == "Status"
        assert col.required_level == "SystemRequired"
        assert col.is_valid_for_create is False
        assert col.is_valid_for_update is True

    def test_from_api_response_uniqueidentifier_column(self):
        """Test primary ID column (account.accountid) maps correctly."""
        col = ColumnInfo.from_api_response(UNIQUEID_COLUMN)
        assert col.logical_name == "accountid"
        assert col.is_primary_id is True
        assert col.is_primary_name is False
        assert col.attribute_type == "Uniqueidentifier"
        assert col.attribute_type_name == "UniqueidentifierType"
        assert col.is_valid_for_update is False

    def test_required_level_extraction(self):
        """Test RequiredLevel.Value is extracted correctly."""
        data = {
            "LogicalName": "col",
            "SchemaName": "Col",
            "RequiredLevel": {"Value": "ApplicationRequired"},
        }
        col = ColumnInfo.from_api_response(data)
        assert col.required_level == "ApplicationRequired"


class TestOptionItem:
    """Tests for OptionItem."""

    def test_from_api_response(self):
        """Test option with Value and Label."""
        opt = OptionItem.from_api_response(PICKLIST_OPTIONSET["Options"][0])
        assert opt.value == 1
        assert opt.label == "Preferred Customer"

    def test_from_api_response_no_label(self):
        """Test option with Value but Label.UserLocalizedLabel is None."""
        data = {"Value": 2, "Label": {"UserLocalizedLabel": None}}
        opt = OptionItem.from_api_response(data)
        assert opt.value == 2
        assert opt.label is None

    def test_from_api_response_status_option(self):
        """Test StatusOptionMetadata with extra State and TransitionData fields."""
        opt = OptionItem.from_api_response(STATUS_OPTIONSET["Options"][0])
        assert opt.value == 1
        assert opt.label == "Active"

    def test_from_api_response_state_option(self):
        """Test StateOptionMetadata with DefaultStatus and InvariantName fields."""
        opt = OptionItem.from_api_response(STATE_OPTIONSET["Options"][0])
        assert opt.value == 0
        assert opt.label == "Active"


class TestOptionSetInfo:
    """Tests for OptionSetInfo."""

    def test_from_api_response_picklist(self):
        """Test picklist-style OptionSet with Options array."""
        opt_set = OptionSetInfo.from_api_response(PICKLIST_OPTIONSET)
        assert opt_set.option_set_type == "Picklist"
        assert opt_set.name == "account_accountcategorycode"
        assert opt_set.display_name == "Category"
        assert opt_set.is_global is False
        assert opt_set.metadata_id == "b994cdd8-5ce9-4ab9-bdd3-8888ebdb0407"
        assert len(opt_set.options) == 2
        assert opt_set.options[0].value == 1
        assert opt_set.options[0].label == "Preferred Customer"
        assert opt_set.options[1].value == 2
        assert opt_set.options[1].label == "Standard"

    def test_from_api_response_boolean(self):
        """Test boolean-style OptionSet with TrueOption and FalseOption."""
        opt_set = OptionSetInfo.from_api_response(BOOLEAN_OPTIONSET)
        assert opt_set.option_set_type == "Boolean"
        assert opt_set.name == "contact_donotphone"
        assert opt_set.display_name == "Do not allow Phone Calls"
        assert len(opt_set.options) == 2
        values = [o.value for o in opt_set.options]
        assert 0 in values
        assert 1 in values
        labels = {o.value: o.label for o in opt_set.options}
        assert labels[0] == "Allow"
        assert labels[1] == "Do Not Allow"

    def test_from_api_response_status_optionset(self):
        """Test Status-type OptionSet (account.statuscode) with StatusOptionMetadata."""
        opt_set = OptionSetInfo.from_api_response(STATUS_OPTIONSET)
        assert opt_set.option_set_type == "Status"
        assert opt_set.name == "account_statuscode"
        assert opt_set.display_name == "Status Reason"
        assert opt_set.is_global is False
        assert opt_set.metadata_id == "75ad977d-6f28-4c5c-ae44-7816d366ba21"
        assert len(opt_set.options) == 2
        assert opt_set.options[0].value == 1
        assert opt_set.options[0].label == "Active"
        assert opt_set.options[1].value == 2
        assert opt_set.options[1].label == "Inactive"

    def test_from_api_response_state_optionset(self):
        """Test State-type OptionSet (contact.statecode) with StateOptionMetadata."""
        opt_set = OptionSetInfo.from_api_response(STATE_OPTIONSET)
        assert opt_set.option_set_type == "State"
        assert opt_set.name == "contact_statecode"
        assert opt_set.display_name == "Status"
        assert opt_set.metadata_id == "88fa5ad0-2a4b-4281-ac9c-b4e71fb77920"
        assert len(opt_set.options) == 2
        values = [o.value for o in opt_set.options]
        assert 0 in values
        assert 1 in values
        labels = {o.value: o.label for o in opt_set.options}
        assert labels[0] == "Active"
        assert labels[1] == "Inactive"

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
