# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
MetadataClient for Dataverse metadata operations.

This module provides a dedicated client for metadata operations including
relationship management. It is accessed via DataverseClient.metadata property.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from .data._odata import _ODataClient


class MetadataClient:
    """
    Client for Dataverse metadata operations.

    This client provides methods for managing relationships and other metadata
    operations. It is accessed via the ``metadata`` property on DataverseClient.

    :param odata_getter: Callable that returns the internal OData client.
    :type odata_getter: Callable[[], _ODataClient]

    Example:
        Access metadata operations via DataverseClient::

            from PowerPlatform.Dataverse.client import DataverseClient

            client = DataverseClient(base_url, credential)

            # Create a relationship via metadata client
            result = client.metadata.create_one_to_many_relationship(
                lookup=lookup_metadata,
                relationship=relationship_metadata
            )
    """

    def __init__(self, odata_getter) -> None:
        self._get_odata = odata_getter

    def create_one_to_many_relationship(
        self,
        lookup: Any,
        relationship: Any,
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a one-to-many relationship between tables.

        This operation creates both the relationship and the lookup attribute
        on the referencing table. It mirrors the CreateOneToManyRequest from
        the .NET SDK.

        :param lookup: Metadata defining the lookup attribute.
        :type lookup: ~PowerPlatform.Dataverse.models.metadata.LookupAttributeMetadata
        :param relationship: Metadata defining the relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.OneToManyRelationshipMetadata
        :param solution_unique_name: Optional solution to add relationship to.
        :type solution_unique_name: :class:`str` or None

        :return: Dictionary with relationship_id, lookup_schema_name, and related metadata.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Create a one-to-many relationship with full control::

                from PowerPlatform.Dataverse.models.metadata import (
                    LookupAttributeMetadata,
                    OneToManyRelationshipMetadata,
                    Label,
                    LocalizedLabel,
                    CascadeConfiguration,
                )

                # Define the lookup attribute
                lookup = LookupAttributeMetadata(
                    schema_name="new_DepartmentId",
                    display_name=Label(
                        localized_labels=[
                            LocalizedLabel(label="Department", language_code=1033)
                        ]
                    ),
                    required_level="None"
                )

                # Define the relationship
                relationship = OneToManyRelationshipMetadata(
                    schema_name="new_Department_Employee",
                    referenced_entity="new_department",
                    referencing_entity="new_employee",
                    referenced_attribute="new_departmentid",
                    cascade_configuration=CascadeConfiguration(delete="RemoveLink")
                )

                # Create the relationship
                result = client.metadata.create_one_to_many_relationship(lookup, relationship)
                print(f"Created relationship: {result['relationship_schema_name']}")
                print(f"Created lookup field: {result['lookup_schema_name']}")
        """
        od = self._get_odata()
        with od._call_scope():
            return od._create_one_to_many_relationship(
                lookup,
                relationship,
                solution_unique_name,
            )

    def create_many_to_many_relationship(
        self,
        relationship: Any,
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a many-to-many relationship between tables.

        This operation creates a many-to-many relationship and an intersect table
        to manage the relationship. It mirrors the CreateManyToManyRequest from
        the .NET SDK.

        :param relationship: Metadata defining the many-to-many relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.ManyToManyRelationshipMetadata
        :param solution_unique_name: Optional solution to add relationship to.
        :type solution_unique_name: :class:`str` or None

        :return: Dictionary with relationship_id, relationship_schema_name, and entity names.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Create a many-to-many relationship::

                from PowerPlatform.Dataverse.models.metadata import (
                    ManyToManyRelationshipMetadata,
                )

                relationship = ManyToManyRelationshipMetadata(
                    schema_name="new_employee_project",
                    entity1_logical_name="new_employee",
                    entity2_logical_name="new_project",
                )

                result = client.metadata.create_many_to_many_relationship(relationship)
                print(f"Created M:N relationship: {result['relationship_schema_name']}")
        """
        od = self._get_odata()
        with od._call_scope():
            return od._create_many_to_many_relationship(
                relationship,
                solution_unique_name,
            )

    def delete_relationship(self, relationship_id: str) -> None:
        """
        Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        .. warning::
            Deleting a relationship also removes the associated lookup attribute
            for one-to-many relationships. This operation is irreversible.

        Example:
            Delete a relationship::

                client.metadata.delete_relationship("12345678-1234-1234-1234-123456789abc")
        """
        od = self._get_odata()
        with od._call_scope():
            od._delete_relationship(relationship_id)

    def get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: :class:`str`

        :return: Relationship metadata dictionary, or None if not found.
        :rtype: :class:`dict` or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Get relationship metadata::

                rel = client.metadata.get_relationship("new_Department_Employee")
                if rel:
                    print(f"Found relationship: {rel['SchemaName']}")
        """
        od = self._get_odata()
        with od._call_scope():
            return od._get_relationship(schema_name)


__all__ = ["MetadataClient"]
