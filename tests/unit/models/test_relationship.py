# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse.models.relationship import (
    RelationshipInfo,
    CascadeConfiguration,
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel


class TestRelationshipInfoFromOneToMany(unittest.TestCase):
    """Tests for RelationshipInfo.from_one_to_many factory."""

    def test_sets_fields(self):
        """from_one_to_many should populate all 1:N fields."""
        info = RelationshipInfo.from_one_to_many(
            relationship_id="rel-guid-1",
            relationship_schema_name="new_Dept_Emp",
            lookup_schema_name="new_DeptId",
            referenced_entity="new_department",
            referencing_entity="new_employee",
        )
        self.assertEqual(info.relationship_id, "rel-guid-1")
        self.assertEqual(info.relationship_schema_name, "new_Dept_Emp")
        self.assertEqual(info.lookup_schema_name, "new_DeptId")
        self.assertEqual(info.referenced_entity, "new_department")
        self.assertEqual(info.referencing_entity, "new_employee")

    def test_relationship_type(self):
        """from_one_to_many should set relationship_type to 'one_to_many'."""
        info = RelationshipInfo.from_one_to_many(
            relationship_id=None,
            relationship_schema_name="rel",
            lookup_schema_name="lk",
            referenced_entity="a",
            referencing_entity="b",
        )
        self.assertEqual(info.relationship_type, "one_to_many")

    def test_nn_fields_are_none(self):
        """N:N-specific fields should be None on a 1:N instance."""
        info = RelationshipInfo.from_one_to_many(
            relationship_id=None,
            relationship_schema_name="rel",
            lookup_schema_name="lk",
            referenced_entity="a",
            referencing_entity="b",
        )
        self.assertIsNone(info.entity1_logical_name)
        self.assertIsNone(info.entity2_logical_name)


class TestRelationshipInfoFromManyToMany(unittest.TestCase):
    """Tests for RelationshipInfo.from_many_to_many factory."""

    def test_sets_fields(self):
        """from_many_to_many should populate all N:N fields."""
        info = RelationshipInfo.from_many_to_many(
            relationship_id="rel-guid-2",
            relationship_schema_name="new_emp_proj",
            entity1_logical_name="new_employee",
            entity2_logical_name="new_project",
        )
        self.assertEqual(info.relationship_id, "rel-guid-2")
        self.assertEqual(info.relationship_schema_name, "new_emp_proj")
        self.assertEqual(info.entity1_logical_name, "new_employee")
        self.assertEqual(info.entity2_logical_name, "new_project")

    def test_relationship_type(self):
        """from_many_to_many should set relationship_type to 'many_to_many'."""
        info = RelationshipInfo.from_many_to_many(
            relationship_id=None,
            relationship_schema_name="rel",
            entity1_logical_name="a",
            entity2_logical_name="b",
        )
        self.assertEqual(info.relationship_type, "many_to_many")

    def test_otm_fields_are_none(self):
        """1:N-specific fields should be None on a N:N instance."""
        info = RelationshipInfo.from_many_to_many(
            relationship_id=None,
            relationship_schema_name="rel",
            entity1_logical_name="a",
            entity2_logical_name="b",
        )
        self.assertIsNone(info.lookup_schema_name)
        self.assertIsNone(info.referenced_entity)
        self.assertIsNone(info.referencing_entity)


class TestRelationshipInfoFromApiResponse(unittest.TestCase):
    """Tests for RelationshipInfo.from_api_response factory."""

    def test_one_to_many_detection(self):
        """Should detect 1:N from @odata.type and map PascalCase fields."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "MetadataId": "rel-guid-1",
            "SchemaName": "new_Dept_Emp",
            "ReferencedEntity": "new_department",
            "ReferencingEntity": "new_employee",
            "ReferencingEntityNavigationPropertyName": "new_DeptId",
        }
        info = RelationshipInfo.from_api_response(raw)
        self.assertEqual(info.relationship_type, "one_to_many")
        self.assertEqual(info.relationship_id, "rel-guid-1")
        self.assertEqual(info.relationship_schema_name, "new_Dept_Emp")
        self.assertEqual(info.referenced_entity, "new_department")
        self.assertEqual(info.referencing_entity, "new_employee")
        self.assertEqual(info.lookup_schema_name, "new_DeptId")

    def test_many_to_many_detection(self):
        """Should detect N:N from @odata.type and map PascalCase fields."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata",
            "MetadataId": "rel-guid-2",
            "SchemaName": "new_emp_proj",
            "Entity1LogicalName": "new_employee",
            "Entity2LogicalName": "new_project",
        }
        info = RelationshipInfo.from_api_response(raw)
        self.assertEqual(info.relationship_type, "many_to_many")
        self.assertEqual(info.relationship_id, "rel-guid-2")
        self.assertEqual(info.relationship_schema_name, "new_emp_proj")
        self.assertEqual(info.entity1_logical_name, "new_employee")
        self.assertEqual(info.entity2_logical_name, "new_project")

    def test_unknown_type_raises(self):
        """Should raise ValueError for unrecognized @odata.type."""
        raw = {"MetadataId": "guid", "SchemaName": "unknown_rel"}
        with self.assertRaises(ValueError) as ctx:
            RelationshipInfo.from_api_response(raw)
        self.assertIn("Unrecognized relationship", str(ctx.exception))

    def test_missing_fields(self):
        """Should handle missing optional fields without error."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": "minimal",
        }
        info = RelationshipInfo.from_api_response(raw)
        self.assertEqual(info.relationship_type, "one_to_many")
        self.assertIsNone(info.relationship_id)
        self.assertEqual(info.referenced_entity, "")


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


if __name__ == "__main__":
    unittest.main()
