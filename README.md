# PowerPlatform Dataverse Client for Python

[![PyPI version](https://img.shields.io/pypi/v/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![Python](https://img.shields.io/pypi/pyversions/PowerPlatform-Dataverse-Client.svg)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The Dataverse SDK for Python lets Python developers access, manage, and manipulate Microsoft Dataverse business data using familiar Python syntax — no .NET knowledge required. It wraps the Dataverse Web API in a single typed client that works with tables and records as native Python dictionaries and pandas DataFrames.

**[Source code](https://github.com/microsoft/PowerPlatform-DataverseClient-Python)** | **[Package (PyPI)](https://pypi.org/project/PowerPlatform-Dataverse-Client/)** | **[API reference](https://learn.microsoft.com/python/api/dataverse-sdk-docs-python/dataverse-overview?view=dataverse-sdk-python-latest)** | **[Product documentation](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/)** | **[Samples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples)**

## Table of contents

- [Learn to use](#learn-to-use)
- [Sample code](#sample-code)
- [Contribute](#contribute)

## Learn to use

Find complete documentation about how to use this SDK in the [Dataverse SDK for Python documentation](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/):

- [Overview](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/overview)
- [Quick guide to Dataverse](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/quick-guide-dataverse)
- [Getting started](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/get-started)
- [Work with data](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/work-data)
- [Query data](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/query)
- [Customize tables and columns](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/metadata)
- [Manage table relationships](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/relationships)
- [Asynchronous client operations](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/async-client)
- [Handle errors and enable HTTP diagnostics](https://learn.microsoft.com/power-apps/developer/data-platform/sdk-python/error-handling)
- [Python Package Reference](https://learn.microsoft.com/python/api/dataverse-sdk-docs-python/dataverse-overview?view=dataverse-sdk-python-latest)

### Sample code

The [examples/](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples) directory has complete, runnable scripts for every operation, including advanced scenarios — relationships, batch changesets, file uploads, SQL and FetchXML queries, and DataFrames — plus a full [async mirror](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/examples/aio). Start with the [examples guide](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/examples/README.md) for a suggested learning progression.

### Contribute

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### API Design Guidelines

When contributing new features to this SDK, please follow these guidelines:

1. **Public methods in operation namespaces** - New public methods go in the appropriate namespace module under [operations/](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/main/src/PowerPlatform/Dataverse/operations). Public types and constants live in their own modules (e.g., `models/metadata.py`, `common/constants.py`)
2. **Add README example for public methods** - Add usage examples to this README for public API methods
3. **Document public APIs** - Include Sphinx-style docstrings with parameter descriptions and examples for all public methods
4. **Update documentation** when adding features - Keep README and SKILL files (note that each skill has 2 copies) in sync
5. **Internal vs public naming** - Modules, files, and functions not meant to be part of the public API must use a `_` prefix (e.g., `_odata.py`, `_relationships.py`). Files without the prefix (e.g., `constants.py`, `metadata.py`) are public and importable by SDK consumers

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
