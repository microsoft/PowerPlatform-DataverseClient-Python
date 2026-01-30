# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for metadata entity types."""

from PowerPlatform.Dataverse.models.metadata import (
    LocalizedLabel,
    Label,
    CascadeConfiguration,
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
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


class TestCascadeConfiguration:
    """Tests for CascadeConfiguration."""

    def test_to_dict_defaults(self):
        """Test default values."""
        cascade = CascadeConfiguration()
        result = cascade.to_dict()

        assert result["Assign"] == "NoCascade"
        assert result["Delete"] == "RemoveLink"
        assert result["Merge"] == "NoCascade"
        assert result["Reparent"] == "NoCascade"
        assert result["Share"] == "NoCascade"
        assert result["Unshare"] == "NoCascade"

    def test_to_dict_custom_values(self):
        """Test custom cascade values."""
        cascade = CascadeConfiguration(
            assign="Cascade",
            delete="Restrict",
        )
        result = cascade.to_dict()

        assert result["Assign"] == "Cascade"
        assert result["Delete"] == "Restrict"

    def test_to_dict_with_additional_properties(self):
        """Test additional properties like Archive and RollupView."""
        cascade = CascadeConfiguration(
            additional_properties={
                "Archive": "NoCascade",
                "RollupView": "NoCascade",
            }
        )
        result = cascade.to_dict()

        assert result["Archive"] == "NoCascade"
        assert result["RollupView"] == "NoCascade"


class TestLookupAttributeMetadata:
    """Tests for LookupAttributeMetadata."""

    def test_to_dict_basic(self):
        """Test basic serialization."""
        lookup = LookupAttributeMetadata(
            schema_name="new_AccountId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Account", language_code=1033)]),
        )
        result = lookup.to_dict()

        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.LookupAttributeMetadata"
        assert result["SchemaName"] == "new_AccountId"
        assert result["AttributeType"] == "Lookup"
        assert result["AttributeTypeName"]["Value"] == "LookupType"
        assert result["RequiredLevel"]["Value"] == "None"

    def test_to_dict_required(self):
        """Test required level."""
        lookup = LookupAttributeMetadata(
            schema_name="new_AccountId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Account", language_code=1033)]),
            required_level="ApplicationRequired",
        )
        result = lookup.to_dict()

        assert result["RequiredLevel"]["Value"] == "ApplicationRequired"

    def test_to_dict_with_description(self):
        """Test with description."""
        lookup = LookupAttributeMetadata(
            schema_name="new_AccountId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Account", language_code=1033)]),
            description=Label(localized_labels=[LocalizedLabel(label="The related account", language_code=1033)]),
        )
        result = lookup.to_dict()

        assert "Description" in result
        assert result["Description"]["LocalizedLabels"][0]["Label"] == "The related account"

    def test_to_dict_with_additional_properties(self):
        """Test additional properties like Targets and IsSecured."""
        lookup = LookupAttributeMetadata(
            schema_name="new_ParentId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Parent", language_code=1033)]),
            additional_properties={
                "Targets": ["account", "contact"],
                "IsSecured": True,
                "IsValidForAdvancedFind": True,
            },
        )
        result = lookup.to_dict()

        assert result["Targets"] == ["account", "contact"]
        assert result["IsSecured"] is True
        assert result["IsValidForAdvancedFind"] is True


class TestOneToManyRelationshipMetadata:
    """Tests for OneToManyRelationshipMetadata."""

    def test_to_dict_basic(self):
        """Test basic serialization."""
        rel = OneToManyRelationshipMetadata(
            schema_name="new_account_orders",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
        )
        result = rel.to_dict()

        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata"
        assert result["SchemaName"] == "new_account_orders"
        assert result["ReferencedEntity"] == "account"
        assert result["ReferencingEntity"] == "new_order"
        assert result["ReferencedAttribute"] == "accountid"
        assert "CascadeConfiguration" in result

    def test_to_dict_with_custom_cascade(self):
        """Test with custom cascade configuration."""
        rel = OneToManyRelationshipMetadata(
            schema_name="new_account_orders",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
            cascade_configuration=CascadeConfiguration(
                delete="Cascade",
                assign="Cascade",
            ),
        )
        result = rel.to_dict()

        assert result["CascadeConfiguration"]["Delete"] == "Cascade"
        assert result["CascadeConfiguration"]["Assign"] == "Cascade"

    def test_to_dict_with_referencing_attribute(self):
        """Test with explicit referencing attribute."""
        rel = OneToManyRelationshipMetadata(
            schema_name="new_account_orders",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
            referencing_attribute="new_accountid",
        )
        result = rel.to_dict()

        assert result["ReferencingAttribute"] == "new_accountid"

    def test_to_dict_with_additional_properties(self):
        """Test additional properties like IsCustomizable."""
        rel = OneToManyRelationshipMetadata(
            schema_name="new_account_orders",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
            additional_properties={
                "IsCustomizable": {"Value": True, "CanBeChanged": True},
                "IsValidForAdvancedFind": True,
                "SecurityTypes": "None",
            },
        )
        result = rel.to_dict()

        assert result["IsCustomizable"]["Value"] is True
        assert result["IsValidForAdvancedFind"] is True
        assert result["SecurityTypes"] == "None"


class TestManyToManyRelationshipMetadata:
    """Tests for ManyToManyRelationshipMetadata."""

    def test_to_dict_basic(self):
        """Test basic serialization with auto intersect name."""
        rel = ManyToManyRelationshipMetadata(
            schema_name="new_account_contact",
            entity1_logical_name="account",
            entity2_logical_name="contact",
        )
        result = rel.to_dict()

        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata"
        assert result["SchemaName"] == "new_account_contact"
        assert result["Entity1LogicalName"] == "account"
        assert result["Entity2LogicalName"] == "contact"
        # IntersectEntityName should default to schema_name
        assert result["IntersectEntityName"] == "new_account_contact"

    def test_to_dict_with_explicit_intersect_name(self):
        """Test with explicit intersect entity name."""
        rel = ManyToManyRelationshipMetadata(
            schema_name="new_account_contact",
            entity1_logical_name="account",
            entity2_logical_name="contact",
            intersect_entity_name="new_account_contact_assoc",
        )
        result = rel.to_dict()

        assert result["IntersectEntityName"] == "new_account_contact_assoc"

    def test_to_dict_with_additional_properties(self):
        """Test additional properties like navigation property names."""
        rel = ManyToManyRelationshipMetadata(
            schema_name="new_account_contact",
            entity1_logical_name="account",
            entity2_logical_name="contact",
            additional_properties={
                "Entity1NavigationPropertyName": "new_contacts",
                "Entity2NavigationPropertyName": "new_accounts",
                "IsCustomizable": {"Value": True, "CanBeChanged": True},
            },
        )
        result = rel.to_dict()

        assert result["Entity1NavigationPropertyName"] == "new_contacts"
        assert result["Entity2NavigationPropertyName"] == "new_accounts"
        assert result["IsCustomizable"]["Value"] is True
