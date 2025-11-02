# Pipeline Setup Summary

This document summarizes the CI/CD infrastructure setup for `dataverse-client-python`, following the Agents-for-Python pattern.

## Files Created

### Package Configuration
- ✅ **`pyproject.toml`** - Complete package metadata, dependencies, and build config
  - Package name: `dataverse-client-python`
  - Import name: `dataverse_sdk`
  - Version: 0.1.0 (manual, update in pyproject.toml)
  - Python: >=3.10
  
- ✅ **`src/dataverse_sdk/__version__.py`** - Version module (also exported from `__init__.py`)

### CI/CD Pipelines
- ✅ **`.github/workflows/python-package.yml`** - GitHub Actions for PR validation
  - Triggers: push/PR to `main`
  - Python 3.12 only (simplified, can expand to matrix later)
  - Steps: black format check → flake8 lint → build → install → pytest
  - **No publish step** (matches Agents-for-Python public repo)

- ✅ **`.azdo/ci-pr.yaml`** - Azure DevOps PR validation
  - Triggers: PR to `main` only
  - Identical steps to GitHub Actions
  - Publishes test results to ADO

### Development Tools
- ✅ **`dev_dependencies.txt`** - Development tools (pytest, black, flake8, build, twine)
- ✅ **`.flake8`** - Linting configuration (max-line-length 120, ignore Black conflicts)
- ✅ **`CONTRIBUTING.md`** - Developer documentation
- ✅ **`CHANGELOG.md`** - Release notes template

## Local Testing

### Install Dev Dependencies
```powershell
pip install -r dev_dependencies.txt
```

### Format Code
```powershell
black src tests
```

### Lint Code
```powershell
flake8 src tests
```

### Run Tests
```powershell
pytest
```

### Build Package
```powershell
python -m build
```

This creates:
- `dist/dataverse_client_python-0.1.0.tar.gz` (source distribution)
- `dist/dataverse_client_python-0.1.0-py3-none-any.whl` (wheel)

### Test Installation
```powershell
pip install dist/*.whl
python -c "from dataverse_sdk import DataverseClient, __version__; print(__version__)"
```

## Next Steps (Not Yet Configured)

### Azure DevOps Setup Required:
1. **Create GitHub service connection** in OneCRM ADO project
   - Project Settings → Service Connections → New → GitHub
   - Name: `GitHubConnection` (or reuse existing)
   - Grant access to `microsoft/PowerPlatform-DataverseClient-Python`

2. **Create pipeline in ADO**
   - Pipelines → New → Existing YAML
   - Select `.azdo/ci-pr.yaml`
   - Configure to run on PRs

### Future Enhancements:
- [ ] Add matrix testing (Python 3.10, 3.11, 3.12, 3.13) when needed
- [ ] Create release pipeline (manual/gated) for publishing to TestPyPI/PyPI
- [ ] Add 1ES template when infrastructure access granted
- [ ] Add ESRP code signing for official releases
- [ ] Consider `setuptools-git-versioning` for automatic version management

## Differences from Agents-for-Python

| Feature | Agents-for-Python | This Project | Reason |
|---------|-------------------|--------------|--------|
| Versioning | `setuptools-git-versioning` | Manual in `pyproject.toml` | User preference for simplicity |
| Python versions | Matrix 3.10-3.14 | Single 3.12 | Simplified for initial setup |
| Package structure | Multi-package (`libraries/*`) | Single package | Different project scope |
| Release pipeline | Not visible (likely private) | Not yet implemented | To be added later |

## Build Verification

✅ Build tested successfully:
```
Successfully built dataverse_client_python-0.1.0.tar.gz and dataverse_client_python-0.1.0-py3-none-any.whl
```

## Known Issues

- ⚠️ License format warning (cosmetic, can be updated later to SPDX expression)
- No publish automation yet (intentional, matches Agents-for-Python public repo)

## Version Update Process

To release a new version:

1. Update version in `pyproject.toml`:
   ```toml
   version = "0.2.0"
   ```

2. Update `CHANGELOG.md` with release notes

3. Commit changes:
   ```powershell
   git add pyproject.toml CHANGELOG.md
   git commit -m "Release v0.2.0"
   ```

4. Create and push tag:
   ```powershell
   git tag v0.2.0
   git push origin main --tags
   ```

5. (Future) Release pipeline will build and publish automatically
