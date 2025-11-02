# Contributing to PowerPlatform-DataverseClient-Python

Thank you for your interest in contributing to the Dataverse Python SDK!

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/microsoft/PowerPlatform-DataverseClient-Python.git
   cd PowerPlatform-DataverseClient-Python
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   ```

3. **Activate the virtual environment**
   - Windows (PowerShell): `.venv\Scripts\Activate.ps1`
   - Windows (CMD): `.venv\Scripts\activate.bat`
   - Linux/Mac: `source .venv/bin/activate`

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r dev_dependencies.txt
   ```

5. **Install the package in editable mode**
   ```bash
   pip install -e .
   ```

## Code Quality

Before submitting a pull request, ensure your code passes all quality checks:

### Format Code with Black
```bash
black src tests
```

### Lint with Flake8
```bash
flake8 src tests
```

### Run Tests
```bash
pytest
```

### Build Package Locally
```bash
python -m build
```

This will create `.whl` and `.tar.gz` files in the `dist/` directory.

### Test Installation
```bash
pip install dist/*.whl
python -c "from dataverse_sdk import DataverseClient; print(DataverseClient.__version__)"
```

## Pull Request Process

1. **Create a feature branch** from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** with appropriate tests

3. **Ensure all checks pass**
   - Black formatting: `black src tests --check`
   - Flake8 linting: `flake8 .`
   - Tests: `pytest`

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```

5. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Submit a pull request** to the `main` branch
   - Provide a clear description of the changes
   - Reference any related issues
   - Ensure CI checks pass

## Testing

- Write tests for new functionality in the `tests/` directory
- Follow existing test patterns
- Aim for good test coverage of new code
- Test files should be named `test_*.py`

## Release Process

(For maintainers only)

1. Update version in `src/dataverse_sdk/__version__.py`
2. Update `CHANGELOG.md` with release notes
3. Create a PR with version bump and changelog updates
4. After merge to `main`, create and push a git tag:
   ```bash
   git tag v0.x.0
   git push origin v0.x.0
   ```
5. Release pipeline will handle package building and publishing

## Code Style Guidelines

- Follow PEP 8 style guidelines (enforced by Black and Flake8)
- Maximum line length: 120 characters
- Use type hints where appropriate
- Write clear docstrings for public APIs
- Keep functions focused and testable

## Questions or Issues?

- Open an issue on GitHub for bugs or feature requests
- Check existing issues and pull requests before creating new ones
- Be respectful and constructive in all interactions

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.
