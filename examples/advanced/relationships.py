# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Relationship Management Example for Dataverse SDK.

This example demonstrates:
- Creating one-to-many relationships using the core SDK API
- Creating lookup fields using the convenience extension helper
- Creating many-to-many relationships
- Querying and deleting relationships
- Working with relationship metadata types

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import sys
from pathlib import Path
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
    AssociatedMenuConfiguration,
)
from PowerPlatform.Dataverse.extensions.relationships import create_lookup_field
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError

# Import shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import backoff


# Simple logging helper
def log_call(description):
    print(f"\n-> {description}")


def delete_table_if_exists(client, table_name):
    """Delete a table only if it exists."""
    if client.get_table_info(table_name):
        client.delete_table(table_name)
        print(f"   (Cleaned up existing table: {table_name})")
        return True
    return False


def main():
    print("=" * 80)
    print("Dataverse SDK - Relationship Management Example")
    print("=" * 80)

    # ============================================================================
    # 1. SETUP & AUTHENTICATION
    # ============================================================================
    print("\n" + "=" * 80)
    print("1. Setup & Authentication")
    print("=" * 80)

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)

    base_url = base_url.rstrip("/")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{base_url}', credential=...)")
    client = DataverseClient(base_url=base_url, credential=credential)
    print(f"[OK] Connected to: {base_url}")

    # ============================================================================
    # 2. CREATE SAMPLE TABLES
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Create Sample Tables")
    print("=" * 80)

    # Create a parent table (Department)
    log_call("Creating 'new_Department' table")
    delete_table_if_exists(client, "new_Department")

    dept_table = backoff(
        lambda: client.create_table(
            "new_Department",
            {
                "new_DepartmentCode": "string",
                "new_Budget": "decimal",
            },
        )
    )
    print(f"[OK] Created table: {dept_table['table_schema_name']}")

    # Create a child table (Employee)
    log_call("Creating 'new_Employee' table")
    delete_table_if_exists(client, "new_Employee")

    emp_table = backoff(
        lambda: client.create_table(
            "new_Employee",
            {
                "new_EmployeeNumber": "string",
                "new_Salary": "decimal",
            },
        )
    )
    print(f"[OK] Created table: {emp_table['table_schema_name']}")

    # Create a project table for many-to-many example
    log_call("Creating 'new_Project' table")
    delete_table_if_exists(client, "new_Project")

    proj_table = backoff(
        lambda: client.create_table(
            "new_Project",
            {
                "new_ProjectCode": "string",
                "new_StartDate": "datetime",
            },
        )
    )
    print(f"[OK] Created table: {proj_table['table_schema_name']}")

    # ============================================================================
    # 3. CREATE ONE-TO-MANY RELATIONSHIP (Core SDK API)
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create One-to-Many Relationship (Core API)")
    print("=" * 80)

    log_call("Creating lookup field on Employee referencing Department")

    # Define the lookup attribute metadata
    lookup = LookupAttributeMetadata(
        schema_name="new_DepartmentId",
        display_name=Label(
            localized_labels=[
                LocalizedLabel(label="Department", language_code=1033)
            ]
        ),
        required_level="None",
    )

    # Define the relationship metadata
    relationship = OneToManyRelationshipMetadata(
        schema_name="new_Department_Employee",
        referenced_entity=dept_table["table_logical_name"],
        referencing_entity=emp_table["table_logical_name"],
        referenced_attribute=f"{dept_table['table_logical_name']}id",
        cascade_configuration=CascadeConfiguration(
            delete="RemoveLink",  # When department is deleted, remove the link but keep employees
            assign="NoCascade",
            merge="NoCascade",
        ),
        associated_menu_configuration=AssociatedMenuConfiguration(
            behavior="UseLabel",
            group="Details",
            label=Label(
                localized_labels=[
                    LocalizedLabel(label="Employees", language_code=1033)
                ]
            ),
            order=10000,
        ),
    )

    # Create the relationship
    result = backoff(
        lambda: client.metadata.create_one_to_many_relationship(
            lookup=lookup,
            relationship=relationship,
        )
    )

    print(f"[OK] Created relationship: {result['relationship_schema_name']}")
    print(f"  Lookup field: {result['lookup_schema_name']}")
    print(f"  Relationship ID: {result['relationship_id']}")

    rel_id_1 = result['relationship_id']

    # ============================================================================
    # 4. CREATE LOOKUP FIELD (Extension Helper)
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Create Lookup Field (Extension Helper)")
    print("=" * 80)

    log_call("Creating lookup field on Employee referencing Contact as Manager")

    # Use the convenience helper for simpler scenarios
    # An Employee has a Manager (who is a Contact in the system)
    result2 = backoff(
        lambda: create_lookup_field(
            client,
            referencing_table=emp_table["table_logical_name"],
            lookup_field_name="new_ManagerId",
            referenced_table="contact",
            display_name="Manager",
            description="The employee's direct manager",
            required=False,
            cascade_delete="RemoveLink",
        )
    )

    print(f"[OK] Created lookup using helper: {result2['lookup_schema_name']}")
    print(f"  Relationship: {result2['relationship_schema_name']}")

    rel_id_2 = result2['relationship_id']

    # ============================================================================
    # 5. CREATE MANY-TO-MANY RELATIONSHIP
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Create Many-to-Many Relationship")
    print("=" * 80)

    log_call("Creating M:N relationship between Employee and Project")

    # Define many-to-many relationship
    m2m_relationship = ManyToManyRelationshipMetadata(
        schema_name="new_employee_project",
        entity1_logical_name=emp_table["table_logical_name"],
        entity2_logical_name=proj_table["table_logical_name"],
        entity1_associated_menu_configuration=AssociatedMenuConfiguration(
            behavior="UseLabel",
            group="Details",
            label=Label(
                localized_labels=[
                    LocalizedLabel(label="Projects", language_code=1033)
                ]
            ),
        ),
        entity2_associated_menu_configuration=AssociatedMenuConfiguration(
            behavior="UseLabel",
            group="Details",
            label=Label(
                localized_labels=[
                    LocalizedLabel(label="Team Members", language_code=1033)
                ]
            ),
        ),
    )

    result3 = backoff(
        lambda: client.metadata.create_many_to_many_relationship(
            relationship=m2m_relationship,
        )
    )

    print(f"[OK] Created M:N relationship: {result3['relationship_schema_name']}")
    print(f"  Relationship ID: {result3['relationship_id']}")

    rel_id_3 = result3['relationship_id']

    # ============================================================================
    # 6. QUERY RELATIONSHIP METADATA
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Query Relationship Metadata")
    print("=" * 80)

    log_call("Retrieving relationship by schema name")

    rel_metadata = client.metadata.get_relationship("new_Department_Employee")
    if rel_metadata:
        print(f"[OK] Found relationship: {rel_metadata.get('SchemaName')}")
        print(f"  Type: {rel_metadata.get('@odata.type')}")
        print(f"  Referenced Entity: {rel_metadata.get('ReferencedEntity')}")
        print(f"  Referencing Entity: {rel_metadata.get('ReferencingEntity')}")
    else:
        print("  Relationship not found")

    # ============================================================================
    # 7. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. Cleanup")
    print("=" * 80)

    cleanup = input("\nDelete created relationships and tables? (y/n): ").strip().lower()

    if cleanup == "y":
        # Delete relationships first (required before deleting tables)
        log_call("Deleting relationships")
        try:
            if rel_id_1:
                backoff(lambda: client.metadata.delete_relationship(rel_id_1))
                print(f"  [OK] Deleted relationship: new_Department_Employee")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 1: {e}")

        try:
            if rel_id_2:
                backoff(lambda: client.metadata.delete_relationship(rel_id_2))
                print(f"  [OK] Deleted relationship: contact->employee (Manager)")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 2: {e}")

        try:
            if rel_id_3:
                backoff(lambda: client.metadata.delete_relationship(rel_id_3))
                print(f"  [OK] Deleted relationship: new_employee_project")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 3: {e}")

        # Delete tables
        log_call("Deleting tables")
        for table_name in ["new_Employee", "new_Department", "new_Project"]:
            try:
                backoff(lambda: client.delete_table(table_name))
                print(f"  [OK] Deleted table: {table_name}")
            except Exception as e:
                print(f"  [WARN] Error deleting {table_name}: {e}")

        print("\n[OK] Cleanup complete")
    else:
        print("\nSkipping cleanup. Remember to manually delete:")
        print("  - Relationships: new_Department_Employee, contact->employee (Manager), new_employee_project")
        print("  - Tables: new_Employee, new_Department, new_Project")

    print("\n" + "=" * 80)
    print("Example Complete!")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExample interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
