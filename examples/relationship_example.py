"""
Example demonstrating how to create lookup fields (n:1 relationships) between tables.

This example:
1. Creates two tables: 'Project' and 'Task'
2. Creates a lookup field in Task that references Project
3. Creates a record in Project
4. Creates a Task record linked to the Project
5. Queries both records showing the relationship
"""

from dataverse_sdk import DataverseClient
import os
from datetime import datetime

# Get credentials from environment variables
BASE_URL = os.environ.get("DATAVERSE_URL")

# Initialize client
client = DataverseClient(BASE_URL)  # Uses DefaultAzureCredential by default

def main():
    # 1. Create the Project table
    project_schema = {
        "name": "string",
        "description": "string",
        "start_date": "datetime",
        "end_date": "datetime",
        "budget": "decimal"
    }
    
    print("Creating Project table...")
    project_info = client.create_table("Project", project_schema)
    project_entity = project_info["entity_logical_name"]
    project_entity_set = project_info["entity_set_name"]
    print(f"Created Project table: {project_entity} (Set: {project_entity_set})")
    
    # 2. Create the Task table
    task_schema = {
        "title": "string",
        "description": "string", 
        "status": "string",
        "due_date": "datetime",
        "estimated_hours": "decimal",
    }
    
    print("Creating Task table...")
    task_info = client.create_table("Task", task_schema)
    task_entity = task_info["entity_logical_name"]
    task_entity_set = task_info["entity_set_name"]
    print(f"Created Task table: {task_entity} (Set: {task_entity_set})")
    
    # 3. Create a lookup field from Task to Project
    print("Creating lookup relationship...")
    relationship_info = client.create_lookup_field(
        table_name=task_entity,
        field_name="project",
        target_table=project_entity,
        display_name="Project",
        description="The project this task belongs to",
        required_level="Recommended",  # Recommended but not required
        cascade_delete="Cascade"  # Delete tasks when project is deleted
    )
    
    print(f"Created relationship: {relationship_info['relationship_name']}")
    print(f"Lookup field created: {relationship_info['lookup_field']}")
    
    # 4. Create a project record
    project_data = {
        "new_name": "Website Redesign",
        "new_description": "Complete overhaul of company website",
        "new_start_date": datetime.now().isoformat(),
        "new_end_date": datetime(2023, 12, 31).isoformat(),
        "new_budget": 25000.00
    }
    
    print("Creating project record...")
    project_record = client.create(project_entity_set, project_data)
    project_id = project_record["new_projectid"]
    print(f"Created project with ID: {project_id}")
    
    # 5. Create a task linked to the project
    # The lookup field name follows the pattern: new_project_id
    lookup_field_name = relationship_info["lookup_field"].lower() + "id"
    
    task_data = {
        "new_title": "Design homepage mockup",
        "new_description": "Create initial design mockups for homepage",
        "new_status": "Not Started",
        "new_due_date": datetime(2023, 10, 15).isoformat(),
        "new_estimated_hours": 16.5,
        # Add the lookup reference
        lookup_field_name: project_id
    }
    
    print("Creating task record with project reference...")
    task_record = client.create(task_entity_set, task_data)
    task_id = task_record["new_taskid"]
    print(f"Created task with ID: {task_id}")
    
    # 6. Fetch the task with project reference
    print("Fetching task with project information...")
    expand_field = relationship_info["lookup_field"].lower() + "_expand"
    
    # Use the OData $expand syntax to retrieve the related project
    odata_client = client._get_odata()
    task_with_project = odata_client.get(task_entity_set, task_id, expand=[expand_field])
    
    # Display the relationship information
    print("\nTask details:")
    print(f"Title: {task_with_project['new_title']}")
    print(f"Status: {task_with_project['new_status']}")
    
    project_ref = task_with_project.get(expand_field)
    if project_ref:
        print("\nLinked Project:")
        print(f"Name: {project_ref['new_name']}")
        print(f"Budget: ${project_ref['new_budget']}")
    
    print("\nRelationship successfully created and verified!")

if __name__ == "__main__":
    main()
