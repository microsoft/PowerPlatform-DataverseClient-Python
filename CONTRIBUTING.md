# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Development Guidelines

### Versioning

This project follows open-source industry standard [Semantic Versioning](https://semver.org/):

**Version Format:** `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)

**When to bump:**
- **MAJOR** (e.g., 1.0.0 → 2.0.0): Breaking changes that require users to update their code
  - Removing public methods, classes, or modules
  - Changing method signatures (parameters, return types)
  - Changing default behavior that breaks existing code
  - Dropping Python version support

- **MINOR** (e.g., 1.0.0 → 1.1.0): New features that are backwards-compatible
  - Adding new public methods or classes
  - Adding optional parameters (with defaults)
  - New features that don't break existing code
  - Adding Python version support

- **PATCH** (e.g., 1.0.0 → 1.0.1): Bug fixes that are backwards-compatible
  - Fixing bugs without changing the API
  - Documentation updates
  - Security fixes (non-breaking)
  - Internal refactoring

### Changelog

We maintain a [CHANGELOG.md](CHANGELOG.md) following the [Keep a Changelog](https://keepachangelog.com/) format.

**For Contributors:** You don't need to update the changelog with your PRs. Maintainers will update it at release time.

**For Maintainers (Release Process):**

Before each release, review merged PRs and update the changelog with:

**What to include:**
- ✅ New features (→ **Added**)
- ✅ Changes to existing functionality (→ **Changed**)
- ✅ Soon-to-be removed features (→ **Deprecated**)
- ✅ Removed features (→ **Removed**)
- ✅ Bug fixes (→ **Fixed**)
- ✅ Security fixes (→ **Security**)
- ❌ Internal refactoring (unless it affects performance/behavior)
- ❌ Test-only changes
- ❌ CI/CD changes
- ❌ Documentation-only updates

**Process:**
1. Review all PRs merged since last release
2. Add user-facing changes to CHANGELOG.md under appropriate categories
3. Include PR numbers for reference (e.g., `(#123)`)
4. Focus on **why it matters to users**, not implementation details

**Adding version links to CHANGELOG.md:**

After creating tags, add version comparison links at the bottom of CHANGELOG.md:

```markdown
[0.1.0b3]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases/tag/v0.1.0b1
```

### Git Tags and Releases

We use git tags to mark release points and GitHub Releases for announcements.

**Creating Git Tags:**

Git tags should be created for every release published to PyPI:

```bash
# Create annotated tag for version X.Y.Z
git tag -a vX.Y.Z -m "Release vX.Y.Z"

# Push tag to remote
git push origin --tags
```

**GitHub Releases:**

After publishing to PyPI, create a GitHub Release based on CHANGELOG.md

**Release notes format:**

```markdown
Brief summary of the release

### Added
- Feature 1 (#123)
- Feature 2 (#124)

### Fixed
- Bug fix 1 (#125)
- Bug fix 2 (#126)
```