# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse.models.relationship import RelationshipInfo


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
        """Should raise ValueError for unknown @odata.type."""
        raw = {"MetadataId": "guid", "SchemaName": "unknown_rel"}
        with self.assertRaises(ValueError):
            RelationshipInfo.from_api_response(raw)

    def test_missing_optional_fields(self):
        """Should handle missing optional fields without error."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": "minimal",
            "ReferencedEntity": "new_department",
            "ReferencingEntity": "new_employee",
        }
        info = RelationshipInfo.from_api_response(raw)
        self.assertEqual(info.relationship_type, "one_to_many")
        self.assertIsNone(info.relationship_id)
        self.assertEqual(info.lookup_schema_name, "")


if __name__ == "__main__":
    unittest.main()
