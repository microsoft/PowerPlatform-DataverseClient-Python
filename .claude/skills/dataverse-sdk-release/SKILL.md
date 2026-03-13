---
name: dataverse-sdk-release
description: Step-by-step release process for the PowerPlatform Dataverse Client Python SDK. Use when creating a new release, publishing to PyPI, or performing any release-related tasks.
---

# Dataverse SDK Release Guide

## Overview

This skill provides the complete release process for the PowerPlatform Dataverse Client Python SDK. Follow these steps in order when preparing a new release.

## Prerequisites

- Write access to the GitHub repository (microsoft/PowerPlatform-DataverseClient-Python)
- Access to the Azure DevOps CI/CD pipeline for PyPI publishing: https://dev.azure.com/dynamicscrm/OneCRM/_build?definitionId=29949
- _(Optional)_ **GitHub CLI (`gh`)** — enables automated PR creation and GitHub releases from the terminal. Install: `winget install GitHub.cli`, then `gh auth login`.

## Release Checklist

### Step 1: Identify Changes Since Last Release

1. Find the last release version and date in CHANGELOG.md (the current dev version is in `pyproject.toml` under `version`)
2. List merged PRs since the last release:
   - GitHub UI: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/pulls?q=is%3Apr+is%3Amerged
   - Or via git: `git log --oneline --since="<last-release-date>"`
3. For each PR, read the full description to understand the user-facing impact

### Step 2: Update CHANGELOG.md

1. Create a branch: `git checkout -b release/v<version>` (e.g., `release/v0.1.0b6`)
2. Add a new section at the top of CHANGELOG.md (below the header), using today's date:

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- Description of new feature (#PR_NUMBER)

### Fixed
- Description of bug fix (#PR_NUMBER)
```

3. Add a version comparison link at the bottom of CHANGELOG.md:

```markdown
[X.Y.Z]: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/compare/vPREVIOUS...vX.Y.Z
```

**Changelog writing rules:**

- **Focus on why it matters to users**, not implementation details
- Do NOT reference internal function names
- Do NOT reference internal implementation choices
- DO describe the user-visible behavior change or new capability
- Include PR numbers for reference: `(#123)`

**What to include (categorize each change under the appropriate Changelog heading):**
- New features -> **Added**
- Changes to existing functionality -> **Changed**
- Soon-to-be removed features -> **Deprecated**
- Removed features -> **Removed**
- Bug fixes -> **Fixed**
- Security fixes -> **Security**

**What to exclude:**
- Internal refactoring (unless it affects performance/behavior)
- Test-only changes
- CI/CD changes
- Documentation-only updates

### Step 3: Create PR for Changelog

1. Commit: `git add CHANGELOG.md && git commit -m "Update CHANGELOG.md for v<version> release"`
2. Push: `git push -u origin release/v<version>`
3. Create a PR on GitHub targeting `main`:
   - **With `gh` CLI:** `gh pr create --base main --title "Update CHANGELOG.md for v<version> release" --body "Release changelog for v<version>"`
4. Get the PR reviewed and merged

### Step 4: Create Git Tag

After the changelog PR is merged:

1. Pull latest main:
```bash
git switch main
git pull origin main
```

2. Create and push the tag:
```bash
git tag -a v<version> -m "Release v<version>"
git push origin --tags
```

**Important:** The tag must be on the `main` commit that includes the changelog update.

### Step 5: Publish to PyPI

Trigger the Azure DevOps CI/CD pipeline:
- Pipeline: https://dev.azure.com/dynamicscrm/OneCRM/_build?definitionId=29949

**Runtime variables (set when queuing the pipeline):**
- `PushToPyPI` — Set to `true` to publish to PyPI. If not set, the pipeline builds, tests, and produces artifacts without publishing.
- `PackageVersion` — The version string for the release (e.g., `0.1.0b7`). Leave empty to use the version from `pyproject.toml`.

**Recommendation:** First run the pipeline **without** setting `PushToPyPI` to `true`. This validates the build, tests, and packaging. Once the dry run succeeds, queue again with `PushToPyPI` set to `true` to publish.

- Verify the package appears on PyPI: https://pypi.org/project/PowerPlatform-Dataverse-Client/

### Step 6: Create GitHub Release

**Before writing release notes:** Review previous releases at https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases to match the tone, detail level, and formatting conventions.

- **With `gh` CLI:** Extract the release notes from CHANGELOG.md (the Added/Fixed/Changed sections for this version) into a temp file, then run:
  ```bash
  gh release create v<version> --title "v<version>" --notes-file <notes-file> --prerelease
  ```
  Omit `--prerelease` if the version does **not** contain `a`, `b`, or `rc`.
- **Without `gh` CLI:**
  1. Go to: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases/new
  2. Select the tag: `v<version>`
  3. Title: `v<version>`
  4. Copy release notes from CHANGELOG.md
  5. Check **"Set as a pre-release"** if the version contains `a`, `b`, or `rc` (alpha/beta/release candidate)
  6. Click **Publish release**

### Step 7: Post-Release Version Bump

Immediately after the release, bump the version for the next development cycle:

1. Create a branch: `git checkout -b post-release/bump-<next-version>`
2. Update `version` in `pyproject.toml` to the next beta (e.g., `0.1.0b6` -> `0.1.0b7`)
3. Stage and commit:
```bash
git add pyproject.toml
git commit -m "Bump version to <next-version> for next development cycle"
```
4. Push: `git push -u origin post-release/bump-<next-version>`
5. Create a PR on GitHub and merge it:
   - **With `gh` CLI:** `gh pr create --base main --title "Bump version to <next-version> for next development cycle" --body "Post-release version bump"`

## Version Numbering

This project uses Semantic Versioning with PEP 440 pre-release identifiers:
- Beta releases: `0.1.0b1`, `0.1.0b2`, `0.1.0b3`, ...
- The version in `pyproject.toml` on `main` should always be one ahead of the latest published release

## Key Links

- **Repository**: https://github.com/microsoft/PowerPlatform-DataverseClient-Python
- **PyPI Package**: https://pypi.org/project/PowerPlatform-Dataverse-Client/
- **CI/CD Pipeline**: https://dev.azure.com/dynamicscrm/OneCRM/_build?definitionId=29949
- **Releases**: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/releases
- **CONTRIBUTING.md**: https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/CONTRIBUTING.md
