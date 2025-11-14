# PowerPlatform Dataverse Client Examples

This directory contains comprehensive examples demonstrating how to use the **PowerPlatform-Dataverse-Client** SDK for Python. The examples are organized in a progressive learning path: **Install → Learn → Test**.

## 📦 Installation

Install the PowerPlatform Dataverse Client SDK:

```bash
pip install PowerPlatform-Dataverse-Client
```

## 📁 Directory Structure

### 🌱 Basic Examples (`basic/`)
Start here for getting up and running with the SDK:

- **`installation_example.py`** - **START HERE** 🎯
  - Package installation validation and import verification
  - Method availability checking and troubleshooting
  - Basic usage examples and code patterns  
  - Optional interactive testing with real environment
  - Perfect for first-run validation after installation

- **`functional_testing.py`** - **TEST BASIC FUNCTIONALITY** 🧪
  - Simple functional testing in real Dataverse environments
  - Basic CRUD operations validation with clean patterns
  - Table creation and basic querying tests
  - Interactive cleanup and straightforward validation
  - Perfect for verifying SDK works in your environment

### 🔬 Advanced Examples (`advanced/`)
Deep-dive into production-ready patterns and specialized functionality:

- **`walkthrough.py`** - **COMPREHENSIVE DEMO** 🚀
  - Full SDK feature demonstration with production-ready patterns
  - Table creation with custom schemas and enums
  - Single and bulk CRUD operations with error handling
  - Advanced querying (SQL and OData) with paging
  - Column metadata management and multi-language support  
  - Interactive cleanup and best practices

- **`file_upload.py`** - **FILE OPERATIONS** 📎
  - File upload to Dataverse file columns with chunking
  - Advanced file handling patterns


## 🚀 Getting Started

Follow this recommended progression for the best learning experience:

### 📋 Step 1: Validate Installation
```bash
# Install the SDK and dependencies
pip install PowerPlatform-Dataverse-Client azure-identity

# Validate installation and imports
python examples/basic/installation_example.py
```

### 🧪 Step 2: Test Basic Functionality (Optional)
```bash
# Basic functional testing in your environment
python examples/basic/functional_testing.py
```

### 🚀 Step 3: Master Advanced Features
```bash
# Comprehensive walkthrough with production patterns
python examples/advanced/complete_walkthrough.py
```

## 🎯 Quick Start Recommendations

- **New to the SDK?** → Start with `examples/basic/installation_example.py`
- **Need to test/validate?** → Use `examples/basic/functional_testing.py`  
- **Want to see all features?** → Run `examples/advanced/complete_walkthrough.py`
- **Building production apps?** → Study patterns in `examples/advanced/complete_walkthrough.py`

## 📋 Prerequisites

- Python 3.10+
- PowerPlatform-Dataverse-Client SDK installed (`pip install PowerPlatform-Dataverse-Client`)
- Azure Identity credentials configured
- Access to a Dataverse environment

## 🔒 Authentication

All examples use Azure Identity for authentication. Common patterns:
- `InteractiveBrowserCredential` for development and interactive scenarios
- `DeviceCodeCredential` for development on headless systems
- `ClientSecretCredential` for production services with service principals

## 📖 Documentation

For detailed API documentation, visit: [Dataverse SDK Documentation](link-to-docs)

## 🤝 Contributing

When adding new examples:
1. Follow the existing code style and structure
2. Include comprehensive comments and docstrings
3. Add error handling and validation
4. Update this README with your example
5. Test thoroughly before submitting