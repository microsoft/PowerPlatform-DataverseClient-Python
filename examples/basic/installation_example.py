# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Installation, Validation & Usage Example

This comprehensive example demonstrates:
- Package installation and validation
- Import verification and troubleshooting  
- Basic usage patterns and code examples
- Optional interactive testing with real Dataverse environment

## Installation

### For End Users (Production/Consumption):
1. Install the published SDK from PyPI:
   ```bash
   pip install PowerPlatform-Dataverse-Client
   ```

2. Install Azure Identity for authentication:
   ```bash
   pip install azure-identity
   ```

### For Developers (Contributing/Local Development):
1. Clone the repository and navigate to the project directory
2. Install in editable/development mode:
   ```bash
   pip install -e .
   ```

**Key Differences:**
- `pip install PowerPlatform-Dataverse-Client` → Downloads and installs the published package from PyPI
- `pip install -e .` → Installs from local source code in "editable" mode

**Editable Mode Benefits:**
- ✅ Changes to source code are immediately available (no reinstall needed)
- ✅ Perfect for development, testing, and contributing
- ✅ Examples and tests can access the local codebase
- ✅ Supports debugging and live code modifications

## What This Script Does

- ✅ Validates package installation and imports
- ✅ Checks version and package metadata
- ✅ Shows code examples and usage patterns
- ✅ Offers optional interactive testing
- ✅ Provides troubleshooting guidance

Prerequisites for Interactive Testing:
- Access to a Microsoft Dataverse environment
- Azure Identity credentials configured
- Interactive browser access for authentication
"""

# Standard imports
import sys
import subprocess
from typing import Optional
from datetime import datetime

def validate_imports():
    """Validate that all key imports work correctly."""
    print("🔍 Validating Package Imports...")
    print("-" * 50)
    
    try:
        # Test main namespace import
        from PowerPlatform.Dataverse import DataverseClient, __version__
        print(f"  ✅ Main namespace: PowerPlatform.Dataverse")
        print(f"  ✅ Package version: {__version__}")
        print(f"  ✅ DataverseClient class: {DataverseClient}")
        
        # Test submodule imports
        from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError
        print(f"  ✅ Core errors: HttpError, MetadataError")
        
        from PowerPlatform.Dataverse.core.config import DataverseConfig
        print(f"  ✅ Core config: DataverseConfig")
        
        from PowerPlatform.Dataverse.utils.pandas_adapter import PandasODataClient
        print(f"  ✅ Utils: PandasODataClient")
        
        from PowerPlatform.Dataverse.data.odata import ODataClient
        print(f"  ✅ Data layer: ODataClient")
        
        # Test Azure Identity import
        from azure.identity import InteractiveBrowserCredential
        print(f"  ✅ Azure Identity: InteractiveBrowserCredential")
        
        return True, __version__, DataverseClient
        
    except ImportError as e:
        print(f"  ❌ Import failed: {e}")
        print("\n💡 Troubleshooting:")
        print("  📦 For end users (published package):")
        print("    • pip install PowerPlatform-Dataverse-Client")
        print("    • pip install azure-identity")
        print("  ")
        print("  🛠️  For developers (local development):")
        print("    • Navigate to the project root directory")
        print("    • pip install -e .")
        print("    • This enables 'editable mode' for live development")
        print("  ")
        print("  🔧 General fixes:")
        print("    • Check virtual environment is activated")
        print("    • Verify you're in the correct directory")
        print("    • Try: pip list | grep PowerPlatform")
        return False, None, None


def validate_client_methods(DataverseClient):
    """Validate that DataverseClient has expected methods."""
    print("\n🏗️  Validating Client Methods...")
    print("-" * 50)
    
    expected_methods = [
        'create', 'get', 'update', 'delete', 
        'create_table', 'get_table_info', 'delete_table',
        'list_tables', 'query_sql'
    ]
    
    missing_methods = []
    for method in expected_methods:
        if hasattr(DataverseClient, method):
            print(f"  ✅ Method exists: {method}")
        else:
            print(f"  ❌ Method missing: {method}")
            missing_methods.append(method)
    
    return len(missing_methods) == 0


def validate_package_metadata():
    """Validate package metadata from pip."""
    print("\n📦 Validating Package Metadata...")
    print("-" * 50)
    
    try:
        result = subprocess.run([sys.executable, '-m', 'pip', 'show', 'PowerPlatform-Dataverse-Client'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            for line in lines:
                if any(line.startswith(prefix) for prefix in ['Name:', 'Version:', 'Summary:', 'Location:']):
                    print(f"  ✅ {line}")
            return True
        else:
            print(f"  ❌ Package not found in pip list")
            print("  💡 Try: pip install PowerPlatform-Dataverse-Client")
            return False
            
    except Exception as e:
        print(f"  ❌ Metadata validation failed: {e}")
        return False


def show_usage_examples():
    """Display comprehensive usage examples."""
    print("\n📚 Usage Examples")
    print("=" * 50)
    
    print("""
🔧 Basic Setup:
```python
from PowerPlatform.Dataverse import DataverseClient
from azure.identity import InteractiveBrowserCredential

# Set up authentication
credential = InteractiveBrowserCredential()

# Create client
client = DataverseClient(
    "https://yourorg.crm.dynamics.com",
    credential
)
```

📝 CRUD Operations:
```python
# Create a record
account_data = {"name": "Contoso Ltd", "telephone1": "555-0100"}
account_ids = client.create("account", account_data)
print(f"Created account: {account_ids[0]}")

# Read a record
account = client.get("account", account_ids[0])
print(f"Account name: {account['name']}")

# Update a record
client.update("account", account_ids[0], {"telephone1": "555-0200"})

# Delete a record
client.delete("account", account_ids[0])
```

🔍 Querying Data:
```python
# Query with OData filter
accounts = client.get("account", 
                     filter="name eq 'Contoso Ltd'",
                     select=["name", "telephone1"],
                     top=10)

for batch in accounts:
    for account in batch:
        print(f"Account: {account['name']}")

# SQL queries (if enabled)
results = client.query_sql("SELECT TOP 5 name FROM account")
for row in results:
    print(row['name'])
```

🏗️ Table Management:
```python
# Create custom table
table_info = client.create_table("CustomEntity", {
    "name": "string",
    "description": "string", 
    "amount": "decimal",
    "is_active": "bool"
})

# Get table information
info = client.get_table_info("CustomEntity")
print(f"Table: {info['table_schema_name']}")

# List all tables
tables = client.list_tables()
print(f"Found {len(tables)} tables")
```
""")


def interactive_test():
    """Offer optional interactive testing with real Dataverse environment."""
    print("\n🧪 Interactive Testing")
    print("=" * 50)
    
    choice = input("Would you like to test with a real Dataverse environment? (y/N): ").strip().lower()
    
    if choice not in ['y', 'yes']:
        print("  ℹ️  Skipping interactive test")
        return
    
    print("\n🌐 Dataverse Environment Setup")
    print("-" * 50)
    
    if not sys.stdin.isatty():
        print("  ❌ Interactive input required for testing")
        return
    
    org_url = input("Enter your Dataverse org URL (e.g., https://yourorg.crm.dynamics.com): ").strip()
    if not org_url:
        print("  ⚠️  No URL provided, skipping test")
        return
    
    try:
        from PowerPlatform.Dataverse import DataverseClient
        from azure.identity import InteractiveBrowserCredential
        
        print("  🔐 Setting up authentication...")
        credential = InteractiveBrowserCredential()
        
        print("  🚀 Creating client...")
        client = DataverseClient(org_url.rstrip('/'), credential)
        
        print("  🧪 Testing connection...")
        tables = client.list_tables()
        
        print(f"  ✅ Connection successful!")
        print(f"  📋 Found {len(tables)} tables in environment")
        print(f"  🌐 Connected to: {org_url}")
        
        print("\n  💡 Your SDK is ready for use!")
        print("  💡 Check the usage examples above for common patterns")
        
    except Exception as e:
        print(f"  ❌ Interactive test failed: {e}")
        print("  💡 This might be due to authentication, network, or permissions")
        print("  💡 The SDK imports are still valid for offline development")


def main():
    """Run comprehensive installation validation and demonstration."""
    print("🚀 PowerPlatform Dataverse Client SDK - Installation & Validation")
    print("=" * 70)
    print(f"🕒 Validation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Step 1: Validate imports
    imports_success, version, DataverseClient = validate_imports()
    if not imports_success:
        print("\n❌ Import validation failed. Please check installation.")
        sys.exit(1)
    
    # Step 2: Validate client methods
    if DataverseClient:
        methods_success = validate_client_methods(DataverseClient)
        if not methods_success:
            print("\n⚠️  Some client methods are missing, but basic functionality should work.")
    
    # Step 3: Validate package metadata
    metadata_success = validate_package_metadata()
    
    # Step 4: Show usage examples
    show_usage_examples()
    
    # Step 5: Optional interactive testing
    interactive_test()
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 VALIDATION SUMMARY")
    print("=" * 70)
    
    results = [
        ("Package Imports", imports_success),
        ("Client Methods", methods_success if 'methods_success' in locals() else True),
        ("Package Metadata", metadata_success)
    ]
    
    all_passed = True
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL" 
        print(f"{test_name:<20} {status}")
        if not success:
            all_passed = False
    
    print("=" * 70)
    if all_passed:
        print("🎉 SUCCESS: PowerPlatform-Dataverse-Client is properly installed!")
        if version:
            print(f"📦 Package Version: {version}")
        print("\n💡 What this validates:")
        print("  ✅ Package installation is correct")
        print("  ✅ All namespace imports work")  
        print("  ✅ Client classes are accessible")
        print("  ✅ Package metadata is valid")
        print("  ✅ Ready for development and production use")
        
        print(f"\n🎯 Next Steps:")
        print("  • Review the usage examples above")
        print("  • Configure your Azure Identity credentials")  
        print("  • Start building with PowerPlatform.Dataverse!")
        
    else:
        print("❌ Some validation checks failed!")
        print("💡 Review the errors above and reinstall if needed:")
        print("   pip uninstall PowerPlatform-Dataverse-Client")
        print("   pip install PowerPlatform-Dataverse-Client")
        sys.exit(1)


if __name__ == "__main__":
    print("🚀 PowerPlatform-Dataverse-Client SDK Installation Example")
    print("=" * 60)
    main()