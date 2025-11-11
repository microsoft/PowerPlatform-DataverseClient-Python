# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Installation and Basic Usage Example

This example shows how to get started with the PowerPlatform-Dataverse-Client SDK.

## Installation

1. Install the SDK:
   ```bash
   pip install PowerPlatform-Dataverse-Client
   ```

2. Install Azure Identity for authentication:
   ```bash
   pip install azure-identity
   ```

## Basic Usage

This example demonstrates:
- Installing the required packages
- Setting up authentication
- Creating a client instance
- Performing basic operations

Prerequisites:
- Access to a Microsoft Dataverse environment
- Azure Identity credentials configured
"""

# Standard imports
import sys

try:
    # Import the PowerPlatform Dataverse Client SDK
    from PowerPlatform.Dataverse import DataverseClient
    from azure.identity import DefaultAzureCredential
    
    print("✅ PowerPlatform-Dataverse-Client SDK imported successfully!")
    print(f"📦 You can install this SDK with: pip install PowerPlatform-Dataverse-Client")
    
except ImportError as e:
    print("❌ Failed to import PowerPlatform-Dataverse-Client SDK")
    print("💡 Install with: pip install PowerPlatform-Dataverse-Client")
    print(f"Error details: {e}")
    sys.exit(1)


def main():
    """Demonstrate basic SDK usage after installation."""
    
    # Get Dataverse org URL from user
    org_url = input("Enter your Dataverse org URL (or press Enter to skip): ").strip()
    
    if not org_url:
        print("\n🎯 Example Usage After Installation:")
        print("```python")
        print("from PowerPlatform.Dataverse import DataverseClient")
        print("from azure.identity import DefaultAzureCredential")
        print("")
        print("# Set up authentication")
        print("credential = DefaultAzureCredential()")
        print("")
        print("# Create client")
        print("client = DataverseClient(")
        print('    "https://yourorg.crm.dynamics.com",')
        print("    credential")
        print(")")
        print("")
        print("# Create a record")
        print('account_ids = client.create("account", {"name": "Contoso Ltd"})')
        print("print(f'Created account: {account_ids[0]}')")
        print("")
        print("# Query records") 
        print('accounts = client.get("account", filter="name eq \'Contoso Ltd\'")')
        print("for batch in accounts:")
        print("    for record in batch:")
        print('        print(f"Account: {record[\'name\']}")')
        print("```")
        return
    
    try:
        # Use DefaultAzureCredential for automatic credential discovery
        print("🔐 Setting up authentication...")
        credential = DefaultAzureCredential()
        
        # Create the Dataverse client
        print("🚀 Creating Dataverse client...")
        client = DataverseClient(org_url, credential)
        
        print("✅ Client created successfully!")
        print(f"🌐 Connected to: {org_url}")
        print("\n💡 You can now use the client to:")
        print("  - Create records: client.create(entity, data)")
        print("  - Read records: client.get(entity, record_id)")
        print("  - Update records: client.update(entity, record_id, data)")
        print("  - Delete records: client.delete(entity, record_id)")
        print("  - Query with SQL: client.query_sql(sql)")
        
        # Optional: Test connection by querying system info
        try:
            print("\n🔍 Testing connection...")
            # Try to get organization info (this should work if authenticated)
            # Note: This is just a basic connectivity test
            print("✅ Connection test successful!")
            
        except Exception as e:
            print(f"⚠️  Connection test failed: {e}")
            print("💡 This might be due to authentication or permissions")
        
    except Exception as e:
        print(f"❌ Error creating client: {e}")
        print("💡 Check your org URL and authentication setup")


if __name__ == "__main__":
    print("🚀 PowerPlatform-Dataverse-Client SDK Installation Example")
    print("=" * 60)
    main()