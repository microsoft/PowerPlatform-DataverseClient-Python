# Copilot Instructions for PowerPlatform-DataverseClient-Python

## Branch Naming Convention

Feature branches **must** follow this naming pattern:

```
users/<github-username>/<short-description>
```

Examples:
- `users/saurabhrb/e2e-relationship-tests`
- `users/tpellissier-msft/add-batch-operations`
- `users/maxwang96/fix-odata-expand`

Rules:
- `<github-username>` is your GitHub login (e.g., `saurabhrb`)
- `<short-description>` uses lowercase kebab-case
- Branch from `origin/main` and rebase before creating a PR

## Python Virtual Environment

**All Python commands** (running tests, installing packages, executing scripts, etc.) in this repository **must** use the local virtual environment at `.venv/` in the repo root.

- Before running any Python command, activate the venv:
  - **PowerShell:** `.\.venv\Scripts\Activate.ps1`
  - **CMD:** `.venv\Scripts\activate.bat`
  - **Linux/macOS:** `source .venv/bin/activate`
- If `.venv/` does not exist, create it first: `python -m venv .venv`
- After activating, install dev dependencies: `pip install -e ".[dev]"` (or as defined in `pyproject.toml`).
- The `.venv/` directory is excluded via `.git/info/exclude` and must **never** be committed.
- **Never** use the system Python or a global environment for repo operations.

## Before Running Examples, Demos, or Tests

**Every time** before running any example script, demo, or test suite, perform these steps in order:

1. **Fetch latest main:** `git fetch origin main`
2. **Merge main into current branch:** `git merge origin/main --no-edit` (use merge, not rebase, to avoid conflict storms on long-lived branches)
3. **Reinstall SDK from local repo:** `.\.venv\Scripts\Activate.ps1; pip install -e ".[dev]"`
4. **Then** run the example/test

This ensures you are always using the latest SDK code, including any fixes merged to `main` since the branch was created.

## GitHub CLI

When using `gh` CLI in this repository, always use:
- `--hostname github.com`
- Authenticated as user `saurabhrb`

Always set the active account before running any `gh` commands by prefixing with: `gh auth switch --hostname github.com --user saurabhrb;`

Example: `gh auth switch --hostname github.com --user saurabhrb; gh issue comment 75 --repo microsoft/PowerPlatform-DataverseClient-Python --hostname github.com --body "..."`

Note: For subcommands that don't support `--hostname` (e.g., `gh issue comment`), set the environment variable instead: `$env:GH_HOST = "github.com"`

## PR Comment Handling

When asked to "check PR comments" or "handle PR comments", this means the **full lifecycle** -- not just reading them:

1. **Read** all open review comments on the PR
2. **Analyze** each comment to determine what code change is needed
3. **Fix** the code to address each comment
4. **Verify** the fix (syntax check, run tests if applicable)
5. **Commit** the fix with a descriptive message referencing the comment
6. **Push** to the PR branch
7. **Reply** to each comment on GitHub confirming the fix (include commit SHA)
8. **Resolve** the conversation if the GitHub API supports it; otherwise note that the user must resolve manually in the UI

Do NOT just read and summarize comments -- take action on every addressable comment in a single pass.

## Temporary Files

All temporary scripts, scratch files, issue body drafts, and other ephemeral artifacts **must** be created inside the `.scratch/` directory at the repository root. This directory is git-ignored locally and should never be committed.

- Create `.scratch/` if it doesn't exist before writing any temp file.
- Use descriptive filenames (e.g., `.scratch/issue_body.md`, `.scratch/test_query.py`).
- Clean up files in `.scratch/` when they are no longer needed.
- **Never** create temp files in the repo root or any tracked directory.

## PR & Issue Triage Process

This repo follows an incremental, numbered triage process for tracking community contributions (open PRs and issues). Each triage is a GitHub issue that captures a snapshot of open contributions, categorizes them, and assigns action items.

### Microsoft Internal Dev Team

The following GitHub users are Microsoft internal developers. PRs/issues from these authors are typically **excluded** from community triage (they are tracked internally):

- `saurabhrb`
- `maxwang96`
- `sagebree`
- `suyask-msft`
- `tpellissier-msft`
- `zhaodongwang-msft`
- `JimDaly`

PRs/issues from any other author are considered **community contributions** and should be triaged.

### Triage Issue Format

- **Title:** `Community Contribution Intake -- Triage #N (YYYY-MM-DD)`
- **Labels:** `triage`, `internal-tracking`
- **Triage Window:** Each triage covers PRs/issues opened or updated **after** the previous triage's cutoff date. The first triage (#1, issue #144) covered everything up to `2026-03-13T23:59:59Z`.
- **Reference previous triage:** Always link to the prior triage issue (e.g., "Continues from #144") so the chain is traceable.

> Note: The title uses `#N` because titles do NOT auto-link. In the issue **body**, use `Triage No. N` to prevent unwanted auto-linking of the triage sequence number.

### Required Sections

1. **Header block** — Triage number, window dates, triaged-by, current SDK version, total PRs + issues reviewed. To determine the current public SDK version: check `version` in [`pyproject.toml` on `main`](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/pyproject.toml) — the version shown there is the **next** release version; the current public release is always one version lower (e.g., if `main` shows `0.1.0b7`, the current public release is `v0.1.0b6`). To verify against the source of truth, run `pip install "PowerPlatform-Dataverse-Client==99.99.99" 2>&1` — the error message lists all published versions on [PyPI](https://pypi.org/project/PowerPlatform-Dataverse-Client/); the highest is the current public release.
2. **Internal Development Status** — Table of active internal-team PRs/issues with current status and **target version**. If an internal PR/issue addresses or overlaps with a community contribution, cross-reference the community PR/issue number so contributors can see their input is being acted on.
3. **Community Contributions — PRs** — Community PRs grouped into priority tiers:
   - **Required** — Fixes, improvements, or features the team considers essential for upcoming releases
   - **Good to Have** — Valuable contributions that improve the SDK but are not blocking
   - **Future Consideration** — Ideas or work that align with the long-term roadmap but are not currently prioritized
   - **Addressed / Duplicates** — Community PRs already covered by internal work, or duplicates of other PRs/issues (cross-reference the relevant internal or community item, include commit SHA or release version where the work was shipped)
   Each tier uses a numbered table:
   | Column | Description |
   |--------|-------------|
   | `#` | Category-scoped ID (e.g., 1.1, 2.3) |
   | `PR` | PR number link |
   | `Title` | PR title |
   | `Author` | GitHub handle |
   | `Opened` | Date opened |
   | `Status` | Open / Draft / Stale |
   | `Action` | What to do next (review, close, rebase, merge, etc.) |
   | `Target` | Target release version (e.g., `v0.1.0b6`*) or `TBD` if not yet scheduled. Always mark with `*` to indicate estimate. |
4. **Community Contributions — Issues** — Community-filed issues grouped into the same priority tiers as PRs:
   | Column | Description |
   |--------|-------------|
   | `#` | Category-scoped ID (e.g., I-1.1) |
   | `Issue` | Issue number link |
   | `Title` | Issue title |
   | `Author` | GitHub handle |
   | `Opened` | Date opened |
   | `Status` | Open / Stale |
   | `Action` | What to do next (investigate, fix, reply, close, etc.) |
   | `Target` | Target release version or `TBD`. Always mark with `*` to indicate estimate. |
5. **Release Roadmap** — Brief summary of upcoming versions and what community contributions are being considered for each. Include version number, estimated timeline, and list of PR/issue numbers. All timelines must be marked as estimates.
6. **Disclaimer** — Every triage must include a formal disclaimer stating that all target versions and timelines are aspirational estimates provided for transparency, not commitments, and are subject to change based on internal reviews, testing, and priorities.
7. **Action Items** — Checkbox list of concrete next steps (both PR and issue actions). Do not include "Post acknowledgment comments" or "Post stale notice" as action items — contributors are auto-notified via `@mentions` in the triage body.
8. **Stale Contributions** — Table of PRs/issues with 90+ days of inactivity, noting last activity date and days inactive.
9. **Process Notes** — (Triage No. 1 only) Instructions for how to create the next triage. Omit this section from Triage No. 2 onward — the instructions live in `copilot-instructions.md`.

When a community contribution is merged or addressed, update the **Addressed / Duplicates** tier with the commit SHA and/or release version where the work shipped. This gives contributors a permanent record of their impact.

### Tone and Contributor Etiquette

This is a public-facing repository representing Microsoft. All triage content must:
- **Acknowledge every community contribution** — thank contributors by name, recognize the effort regardless of whether the PR/issue is accepted.
- **Be transparent about status** — clearly explain why a contribution is deferred, needs changes, or is already addressed by internal work. Never leave contributors without a response.
- **Encourage future participation** — when closing or deferring, provide constructive guidance on how the contributor can help in other areas.
- **Maintain professional, respectful language** — represent Microsoft's commitment to an inclusive, welcoming open-source community.

### Response SLA

The team aims to provide an **initial acknowledgment comment within 15 business days** of a community PR or issue being filed. This may be a simple thank-you with a note that the contribution will be reviewed in the next triage cycle. As the SDK is in early beta with a small team, response times may vary — but the goal is to ensure no contributor feels ignored.

- If a PR/issue is triaged and a decision is made, post a comment on the PR/issue itself summarizing the outcome so the contributor is notified directly.
- If a PR/issue cannot be reviewed immediately, comment with a brief status and expected timeline.

### Triage Cadence

Triages are conducted on a **monthly** basis (first week of each month). Ad-hoc triages may be run when there is a spike in community activity or before a major release.

### Stale Contribution Policy

A community PR or issue is considered **Stale** after **90 days** without activity (no commits, comments, or updates).

When a contribution becomes stale:
1. Post a courteous comment explaining the stale status and asking the contributor if they plan to continue.
2. Add the `stale` label.
3. If no response after an additional **30 days**, close the PR/issue with a comment thanking the contributor and inviting them to reopen when ready.
4. Never close without explanation — always provide context and encouragement.

### Community Notification Comments

After each triage, post a brief comment on every community PR/issue that was triaged, summarizing the team's decision. This ensures contributors are notified directly (not just via the triage issue). Use a professional, appreciative tone.

> **Escaping `#` in GitHub Markdown:** When referencing PR/issue numbers in issue bodies or comments, use plain `#NN` (e.g., `#131`) to leverage GitHub's native auto-linking with hover previews. This works correctly within the same repository. **Important:** `&#35;` does NOT prevent auto-linking in issue bodies -- GitHub decodes HTML entities before auto-linking. To avoid unwanted auto-linking of sequence numbers (e.g., triage numbers), use `No.` format instead (e.g., `Triage No. 1`). Note: issue/PR **titles** do NOT auto-link `#NN`, so no escaping is needed there.

Example comment template (no overlapping internal work):
```
Hi @{author} — thank you for this contribution! We reviewed this in our
[Triage #N](link-to-triage-issue) and have categorized it as **{tier}**.

**Next steps:** {action}

We appreciate your time and effort. Please don't hesitate to reach out
if you have any questions.
```

Example comment template (when internal work overlaps):
```
Hi @{author} — thank you for this contribution! We reviewed this in our
[Triage #N](link-to-triage-issue) and have categorized it as **{tier}**.

We want you to know that we've acknowledged your work and are actively
incorporating it as part of our internal PR #{internal_pr_number}. Your
contribution directly informed our approach.

**Next steps:** {action}

We appreciate your time and effort. Please don't hesitate to reach out
if you have any questions.
```

### How to Run a New Triage

1. Fetch all open PRs: `$env:GH_HOST = "github.com"; gh pr list --repo microsoft/PowerPlatform-DataverseClient-Python --state open --json number,title,body,labels,author,createdAt,updatedAt,headRefName --limit 100`
2. Fetch all open issues: `$env:GH_HOST = "github.com"; gh issue list --repo microsoft/PowerPlatform-DataverseClient-Python --state open --json number,title,body,labels,author,createdAt,updatedAt --limit 100`
3. Filter to only PRs/issues opened or updated **after** the previous triage cutoff date.
4. Auto-exclude PRs/issues authored by Microsoft internal devs (see list above) — list them in the Internal Development Status section.
5. For each internal PR/issue, check if any community PR/issue overlaps — cross-reference with `Addresses community #NN` or `Related to community #NN`.
6. For each community PR/issue, check if any internal PR/issue already addresses or overlaps with it. If so, note the internal PR/issue number in the community item's **Action** column (e.g., "Being incorporated in internal PR #141"). This ensures the notification comment posted on the community PR/issue can reference the specific internal work.
7. **Completeness check:** Verify that **every** non-internal open PR and **every** non-internal open issue appears in the triage — either under a community tier (Required / Good to Have / Future Consideration / Addressed) or explicitly noted as out of scope with a reason. Nothing should be silently skipped.
8. Cross-reference against the previous triage's items to avoid re-triaging unchanged items.
9. Carry forward any **unresolved action items** from the prior triage.
10. Write the issue body to `.scratch/triage_body.md`, create the issue via `gh issue create --body-file .scratch/triage_body.md`, then clean up.
11. Categories to use (add new ones as needed): Async, Metadata/Schema, Telemetry/Observability, Query, File Operations, Performance, Relationships, Batch, DataFrame, Other.

### Triage Lifecycle (Incremental Close-and-Carry-Forward)

Each triage issue is a **point-in-time snapshot**. When a new triage is created:

1. **Close the previous triage issue** with a comment: *"Superseded by Triage #N — see #NNN."*
2. **Carry forward** all unresolved action items from the previous triage into the new one.
3. Items completed since the last triage should be moved to the **Addressed / Duplicates** tier in the new triage — with commit SHA and/or release version for traceability.
4. This keeps only **one active triage issue** open at any time, avoiding confusion about which is current.

### Human Review Gate for Public-Facing Content

**All content destined for the public repository — triage issues, PR/issue comments, stale notices, acknowledgment messages — must be drafted locally first and reviewed by a human engineer before posting.**

Workflow:
1. Draft the content to a file in `.scratch/` (e.g., `.scratch/triage_body.md`, `.scratch/comment_pr131.md`).
2. Present the draft to the human engineer for review.
3. **Do not post** until the engineer explicitly approves.
4. After posting, clean up the `.scratch/` file.

This applies to:
- New triage issues (`gh issue create`)
- Updates to existing triage issues (`gh issue edit`)
- Comments on community PRs and issues (`gh issue comment`, `gh pr comment`)
- Stale notices and closure comments
- Any other public-facing content on this repository

### Content Quality Audit

Before posting any public-facing content, perform a final audit:

1. **No non-ASCII characters** -- use only ASCII in issue bodies and comments. Use `--` instead of em dashes, straight quotes instead of curly quotes, etc. Non-ASCII characters (e.g., `U+2014` em dash) can corrupt to garbled symbols (`ΓÇö`, `╬ô├ç├╢`) due to encoding mismatches between tools.
2. **No BOM markers** -- ensure files written for `--body-file` do not contain a UTF-8 BOM (`EF BB BF` / `U+FEFF`). Use `[System.IO.File]::WriteAllText()` or the `create_file` tool instead of `Set-Content -Encoding UTF8` (which adds BOM in older PowerShell).
3. **Proper `#` handling** -- PR/issue references (`#131`) use plain `#` for auto-linking. Triage sequence numbers must use `No.` format (e.g., `Triage No. 1`) because `&#35;` does NOT prevent auto-linking in issue bodies. Titles never need escaping.
4. **No corrupted symbols** -- scan the final file for any garbled characters before posting. If any non-ASCII is detected, replace with ASCII equivalents.
5. **Version accuracy** -- verify SDK version against `pyproject.toml` on `main` (next release) and PyPI (`pip install "PowerPlatform-Dataverse-Client==99.99.99" 2>&1`). Current public = highest on PyPI.
6. **Issue count accuracy** -- count only genuine community issues/PRs. Exclude internal triage issues, closed duplicates, and self-referential items.
7. **Target version consistency** -- all target versions should align with the release roadmap. Current release targets should match `pyproject.toml` version on `main`.
8. **Cross-reference completeness** -- verify every internal PR that addresses a community item has the cross-reference column filled, and vice versa.
9. **Post-publish verification** -- after posting, view the live issue on GitHub and visually verify: no garbled characters, no unwanted auto-links on triage numbers, all PR/issue references render correctly with hover previews.
