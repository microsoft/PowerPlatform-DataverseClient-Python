"""
Example demonstrating how to create tables with lookup fields inline.

This example:
1. Creates a Project table with basic fields
2. Creates a Task table with a lookup field to Project in a single step
3. Creates records in both tables and demonstrates the relationship
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
    
    # 2. Create the Task table with an inline lookup field to Project
    task_schema = {
        "title": "string",
        "description": "string", 
        "status": "string",
        "due_date": "datetime",
        "estimated_hours": "decimal",
        # Define a lookup field inline
        "project": {
            "lookup": project_entity,  # Reference the logical name of the target table
            "display_name": "Project",
            "description": "The project this task belongs to",
            "required_level": "Recommended",
            "cascade_delete": "Cascade"  # Delete tasks when project is deleted
        }
    }
    
    print("Creating Task table with project lookup...")
    task_info = client.create_table("Task", task_schema)
    task_entity = task_info["entity_logical_name"]
    task_entity_set = task_info["entity_set_name"]
    print(f"Created Task table: {task_entity} (Set: {task_entity_set})")
    print(f"Columns created: {task_info['columns_created']}")
    
    # Find the created lookup field name
    lookup_field = None
    for column in task_info["columns_created"]:
        if "project" in column.lower():
            lookup_field = column
            break
    
    if not lookup_field:
        print("Could not find project lookup field!")
        return
        
    print(f"Created lookup field: {lookup_field}")
    
    # 3. Create a project record
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
    
    # 4. Create a task linked to the project
    # The lookup field name follows the pattern: new_project_id
    lookup_field_name = lookup_field.lower() + "id"
    
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
    
    # 5. Fetch the task with project reference
    print("Fetching task with project information...")
    expand_field = lookup_field.lower() + "_expand"
    
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
    
    print("\nInline lookup field creation successfully demonstrated!")

if __name__ == "__main__":
    main()
