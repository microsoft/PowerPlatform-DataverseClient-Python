# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Convenience helpers for relationship creation.

These are higher-level functions that simplify common relationship scenarios.
Users can choose to use these helpers or work directly with the core SDK methods
for more control over the metadata.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Any, Optional

if TYPE_CHECKING:
    from ..client import DataverseClient

from ..models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
)


def create_lookup_field(
    client: "DataverseClient",
    referencing_table: str,
    lookup_field_name: str,
    referenced_table: str,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
    required: bool = False,
    cascade_delete: str = "RemoveLink",
    solution_unique_name: Optional[str] = None,
    language_code: int = 1033,
) -> Dict[str, Any]:
    """
    Helper to create a simple lookup field relationship.

    This is a convenience wrapper around create_one_to_many_relationship
    for the common case of adding a lookup to an existing table.

    :param client: DataverseClient instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient
    :param referencing_table: Logical name of the table that will have the lookup field (child table).
    :type referencing_table: str
    :param lookup_field_name: Schema name for the lookup field (e.g., "new_AccountId").
    :type lookup_field_name: str
    :param referenced_table: Logical name of the table being referenced (parent table).
    :type referenced_table: str
    :param display_name: Display name for the lookup field (defaults to referenced table name).
    :type display_name: str or None
    :param description: Optional description for the lookup field.
    :type description: str or None
    :param required: Whether the lookup is required.
    :type required: bool
    :param cascade_delete: Delete behavior ("RemoveLink", "Cascade", "Restrict").
    :type cascade_delete: str
    :param solution_unique_name: Optional solution to add the relationship to.
    :type solution_unique_name: str or None
    :param language_code: Language code for labels (default 1033 for English).
    :type language_code: int

    :return: Dictionary with relationship_id, lookup_schema_name, and related metadata.
    :rtype: dict

    :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

    Example:
        Create a simple lookup field::

            from PowerPlatform.Dataverse.extensions.relationships import create_lookup_field

            result = create_lookup_field(
                client,
                referencing_table="new_order",
                lookup_field_name="new_AccountId",
                referenced_table="account",
                display_name="Account",
                required=True,
                cascade_delete="RemoveLink"
            )

            print(f"Created lookup: {result['lookup_schema_name']}")
    """
    # Build the label
    localized_labels = [LocalizedLabel(
        label=display_name or referenced_table,
        language_code=language_code
    )]

    # Build the lookup attribute
    lookup = LookupAttributeMetadata(
        schema_name=lookup_field_name,
        display_name=Label(localized_labels=localized_labels),
        required_level="ApplicationRequired" if required else "None"
    )

    # Add description if provided
    if description:
        lookup.description = Label(
            localized_labels=[LocalizedLabel(
                label=description,
                language_code=language_code
            )]
        )

    # Generate a relationship name if not provided
    relationship_name = f"{referenced_table}_{referencing_table}_{lookup_field_name}"

    # Build the relationship metadata
    relationship = OneToManyRelationshipMetadata(
        schema_name=relationship_name,
        referenced_entity=referenced_table,
        referencing_entity=referencing_table,
        referenced_attribute=f"{referenced_table}id",
        cascade_configuration=CascadeConfiguration(delete=cascade_delete)
    )

    # Delegate to client
    return client.create_one_to_many_relationship(
        lookup,
        relationship,
        solution_unique_name
    )


__all__ = ["create_lookup_field"]
