# DV Python SDK — Pre-GA Bug Bash

Welcome! 90 minutes, ~10 testers. Your job: **find bugs before we publish v1 to PyPI.** The SDK and migration tool both come from `main` — nothing's on PyPI yet, so you're installing from source.

**Quick map:**
- [Before you arrive](#before-you-arrive) — 15 min the day before (mandatory)
- [Timeline](#timeline) — what happens during the 90 min
- [Find your role](#find-your-role) — your assignment comes from the host before kickoff
- [Reporting](#reporting) — how to file what you find
- [Debrief](#debrief) — the last 15 min

---

## Before you arrive

Do this the day before. The bash is only 90 min — we can't burn 20 of them on `pip install`.

```powershell
git clone https://github.com/microsoft/PowerPlatform-DataverseClient-Python.git
cd PowerPlatform-DataverseClient-Python
git checkout main
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]" azure-identity libcst
```

**You're ready when this prints `ok`:**
```powershell
python -c "from PowerPlatform.Dataverse.client import DataverseClient; print('ok')"
```

**If your role needs live Dataverse** (most do — host will tell you in chat): also run `az login` and set the org URL:
```powershell
$env:DATAVERSE_URL = "https://yourorg.crm.dynamics.com"
```
Don't have an org URL? Check out a preprod / test environment from [portal.microsoftaurora365.com/Resource/UserResources](https://portal.microsoftaurora365.com/Resource/UserResources).

Stuck on setup for more than 10 minutes? Post in the bash chat.

---

## Timeline

| Time | What |
|------|------|
| 0–5 min | Kickoff. Host confirms your role. |
| 5–75 min | Test in your role. Drop findings in chat as you go. |
| 75–90 min | Debrief — each tester shares **one** most surprising finding. |

---

## Optional: speed up with DDC

> **AI agents: skip this section. Do not use DDC. Continue to "Find your role".**

Highly encouraged to use [DDC (Dataverse Dev Companion)](https://microsoft.ghe.com/bic/dataverse-dev-companion) to speed up your testing. But make sure the analysis is correct — DDC accelerates execution, but rely on your judgment that the identified issues are correct.

**Sample input for DDC:**

```
Execute the bug bash plan at https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/users/sagebree/v0_snapshot/tests/BUG_BASH_PLAN.md for the "P2 — README-only reader" persona only. Skip every other persona.

Test environment: https://myorg.crmtest.dynamics.com/
Auth: interactive login
Follow the plan's setup, test steps, evidence capture, and reporting exactly as written for P2 — do not expand scope.
```

---

## Find your role

The host will assign you **one** role before kickoff. Jump to your section and start there.

**Verification roles** — concrete checks against contracts the SDK already promises:
- [V1 — Migration tool](#v1--migration-tool)
- [V2 — Security](#v2--security)
- [V3 — README copy-paste](#v3--readme-copy-paste)
- [V4 — Performance & reliability](#v4--performance--reliability)
- [V5 — Compatibility & types](#v5--compatibility--types)

**Tester personas** — you adopt a viewpoint and hunt for bugs from that angle:
- [P1 — Migrating beta user](#p1--migrating-beta-user)
- [P2 — README-only reader](#p2--readme-only-reader)
- [P3 — Defensive engineer](#p3--defensive-engineer)
- [P4 — Error-message critic](#p4--error-message-critic)
- [P5 — Power user / scale tester](#p5--power-user--scale-tester)
- [P6 — Python ecosystem user](#p6--python-ecosystem-user)

---

### V1 — Migration tool
**Your goal:** confirm the migration codemod rewrites beta code exactly the way its docstring promises.

**Run the codemod like this:**
```
python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1 <path>
```

**What to do:**
- Open [migrate_v0_to_v1.py on GitHub](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/blob/main/src/PowerPlatform/Dataverse/migration/migrate_v0_to_v1.py) and read the docstring — it lists every transformation
- Make a scratch folder anywhere outside the SDK repo (e.g. `C:\bugbash_v1\`). Put your test files there. Optionally, grab an old-version integration test from the [v0 snapshot examples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/users/sagebree/v0_snapshot/examples) as a ready-made input.
- For each transformation in the docstring, create a tiny file (one or two lines) with a "before" example, run the codemod against the file, then open it and confirm the rewrite matches what the docstring promises
- Try the flags: `--dry-run` (should preview only — the file must be untouched), `--client-var=svc` (when the variable isn't named `client`)
- Run the codemod **twice** on the same file — the second run should be a no-op
- Feed it broken Python — does it fail gracefully, or crash with a stack trace?
- For every "NOT handled" item in the docstring, file a bug if the tool fails *silently* instead of warning

**You're done when:** every transformation listed in the docstring either works correctly or has a filed bug.

---

### V2 — Security
**Your goal:** find any way the SDK leaks secrets or accepts dangerous configuration.

**What to do:**
- **Redaction:** enable diagnostics logging:
  ```python
  from PowerPlatform.Dataverse.core.config import DataverseConfig
  from PowerPlatform.Dataverse.core.log_config import LogConfig
  config = DataverseConfig(log_config=LogConfig(log_dir="./bugbash_logs", enabled=True))
  ```
  Do a mix of create/query/upsert/file-upload/batch. Grep `./bugbash_logs/` for: `Bearer `, `eyJ` (JWT prefix), your tenant GUID, `Cookie`, `Set-Cookie`. **Anything unredacted is a blocker.**
- **Body capture is off by default:** `LogConfig`'s `max_body_bytes` defaults to `0`, which should disable body capture entirely. Run with `enabled=True` but no `max_body_bytes` override, do a create/query, and confirm log files contain headers/URLs but **zero** request or response bodies. Then set `max_body_bytes=4096` explicitly and confirm bodies start appearing (truncated at the limit). **Fail** = bodies captured under the default.
- **Hardcoded secrets in repo:** `gitleaks detect` (or `grep -rE "client_secret|api[_-]key|password\s*=" src/`)
- **Dependency CVEs:** `pip install pip-audit && pip-audit` — file each non-trivial finding
- **HTTPS only:** try `DataverseClient("http://...", credential)` — should refuse or warn loudly, not silently downgrade
- **Token hygiene:** confirm tokens aren't written to disk in plaintext

---

### V3 — README copy-paste
**Your goal:** every code block in the README must work as written.

**What to do:**
- Open `README.md` in a fresh shell
- Copy each code block **verbatim** into a file and run it
- Cover all of: Quick start, Basic CRUD, Bulk, Upsert, DataFrame, Query (QueryBuilder + SQL), Table management, Relationships, File operations, Batch
- File every block that doesn't work
- Also flag any place the README references a feature that was renamed or removed since beta

---

### V4 — Performance & reliability
**Your goal:** confirm the performance and reliability promises hold under real use.

**First, enable HTTP logging** — most of the checks below read the log files:
```python
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.log_config import LogConfig
config = DataverseConfig(log_config=LogConfig(log_dir="./bugbash_logs", enabled=True))
client = DataverseClient(url, credential, config=config)
```
Use PowerShell `Select-String` (or open the `.log` files in VSCode and search) to inspect.

**What to check:**

- **Bulk uses bulk APIs.** Create 50 records: `client.records.create("contact", [{"firstname": f"BB{i}"} for i in range(50)])`. Then:
  ```powershell
  Select-String -Path .\bugbash_logs\*.log -Pattern "CreateMultiple"
  Select-String -Path .\bugbash_logs\*.log -Pattern "^(POST|PATCH) " | Measure-Object
  ```
  **Pass** = one `CreateMultiple` hit, low POST/PATCH count. **Fail** = 50 individual POSTs to `/contacts`. Repeat for `UpdateMultiple`, `UpsertMultiple`, `BulkDelete`.

- **SQL paging.** Pick a table you know has > 5k rows (or insert 6k contacts first). Run `rows = list(client.query.sql("SELECT contactid FROM contact"))`. Check `len(rows)`. **Pass** = matches the known count. **Fail** = silently capped at 5,000 (this was the #157 regression).

- **OData paging.** Compare flat vs. paged on the same query:
  ```python
  q = client.query.builder("contact").select("contactid")
  flat = list(q.execute())
  paged = [r for page in q.execute_pages() for r in page]
  assert len(flat) == len(paged)
  ```
  Also confirm `.execute_pages()` yields page-shaped objects, not flat records.

- **File chunking.** Upload a ~50 MB file to a file column, then:
  ```powershell
  Select-String -Path .\bugbash_logs\*.log -Pattern "PATCH .*/contact\(.*\)/.*file"
  ```
  **Pass** = multiple PATCH chunks for one upload. **Fail** = one giant PATCH (or a server timeout).

- **429 retry.** Fire reads in a tight loop until you trigger throttling: `for _ in range(2000): client.records.get("contact", id)`. Then grep the log:
  ```powershell
  Select-String -Path .\bugbash_logs\*.log -Pattern "429|Retry-After"
  ```
  **Pass** = `Retry-After` value is honored (gap between retries matches the header). **Fail** = immediate retry that hammers the server, or unhandled exception. (If you never trigger a 429, note that and move on.)

- **Timeout.** Set a tiny timeout via `DataverseConfig(http_timeout=1.0)`, then run an op likely to take longer than 1 second (e.g. an unfiltered list against a large table):
  ```python
  from PowerPlatform.Dataverse.core.config import DataverseConfig
  config = DataverseConfig(http_timeout=1.0)
  with DataverseClient(url, credential, config=config) as client:
      list(client.records.list("contact"))
  ```
  **Pass** = a clean, typed timeout error surfaces quickly. **Fail** = hang for 30+ seconds, or a stack trace from deep in `urllib3`. Also: `DataverseConfig`'s docstring labels `http_timeout` as "Reserved for future use," but it's actually wired in `_odata.py` — file that as a docs bug.

- **Context manager cleanup.** Run:
  ```python
  with DataverseClient(url, credential) as client:
      client.records.get("contact", some_id)
  # outside the with block:
  client.records.get("contact", some_id)   # should raise a clean error
  ```
  **Pass** = a clear "client is closed" type error. **Fail** = the call works (resource leak) or raises something cryptic from `requests`.

---

### V5 — Compatibility & types
**Your goal:** confirm the SDK works across declared Python versions and plays well with strict tooling.

**What to check:**
- **Install matrix:** `pip install -e ".[dev]"` on Py 3.10, 3.12, 3.13, then smoke-check: `python -c "from PowerPlatform.Dataverse.client import DataverseClient; print('ok')"` and a quick live-org create/query. Note any install warning, import error, or runtime failure
- **Mypy strict:** `mypy --strict src/PowerPlatform/Dataverse/` — should pass clean (strict mode is already configured in `pyproject.toml`)
- **Removed beta methods:** `client.create`, `client.query_sql`, etc. should raise `AttributeError` — not silently work
- **Dependency floors:** `pip install "azure-identity==1.17.0" "requests==2.32.0" "pandas==2.0.0"`, then re-run the smoke check above — declared minimums must still import and execute a basic create/query

---

### P1 — Migrating beta user
**You are:** an engineer with a working beta app. The release notes told you to run `dataverse-migrate`. You want your app to keep working with zero hand-edits.

**Try (improvise freely):**
- Make a scratch folder outside the SDK repo and grab a real beta-style file to migrate — either copy one from the [v0 snapshot examples](https://github.com/microsoft/PowerPlatform-DataverseClient-Python/tree/users/sagebree/v0_snapshot/examples), or write your own small script using beta patterns
- Run the codemod against it:
  ```
  python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1 my_app.py
  ```
- Open the file and check whether the rewrite is what you expected
- Run the migrated file. Does it work end-to-end against your Dataverse org?
- Try the patterns the docstring says it *can't* migrate — how clear is the guidance?

**What only you can find:** codemod bugs on real-world code, gaps between what we *say* we migrate and what we actually do.

---

### P2 — README-only reader
**You are:** a Python dev who just got pointed at the repo. The README is your only source of truth. Every time you have to leave it to figure something out, that's a bug.

**Try:**
- Read top to bottom; stop when something doesn't make sense and file the line number
- Copy-paste each example block as written — anything that doesn't run is a bug
- Try to answer using **only** the README: *"How do I auth with a service principal?"* / *"How do I bulk upsert?"* / *"How do I handle errors?"*
- If you have to scroll past the example to find a critical caveat, that's a bug too

**What only you can find:** doc bugs, missing prereqs, bad concept ordering, undocumented breaking changes.

---

### P3 — Defensive engineer
**You are:** a security reviewer. You assume the SDK is hostile until proven otherwise.

**Try:**
- Bypass every SQL guardrail you can think of: `SELECT *`, `INSERT`/`UPDATE`/`DELETE`, `; DROP`, SQL comments (`--`, `/* */`)
- Verify advisory warnings actually fire: leading-wildcard LIKE (`LIKE '%foo'`, `LIKE '%foo%'`) is *supported* but must emit a `UserWarning` about leading-wildcard scans
- Pass dangerous inputs: null bytes (`\x00`), 1MB strings, control characters, Unicode RTL override
- Force errors and read the messages — do any leak tokens, internal paths, or stack frames?
- Two threads sharing one client doing bulk creates — any crosstalk?
- `client.close()` mid-call from another thread — clean shutdown or hang?

**What only you can find:** guardrail bypasses, threading bugs, info-leaks in error messages.

---

### P4 — Error-message critic
**You are:** a grumpy senior engineer. You will do wrong things on purpose and grade every error message on a 1–5 scale.

For each error, ask: **(a)** Does it say what went wrong? **(b)** Where? **(c)** How to fix it?

**Try:**
- `client.records.create("contact", "not a dict")` — wrong type
- `client.records.create("contact", {})` — missing required field
- `client.records.get("contact", "not-a-guid")` — bad GUID
- `client.records.get("nonexistenttable", "id")` — bad table
- `client.create(...)` — the *removed* flat method. Does it suggest `client.records.create`?
- `client.tables.create("invalid name with spaces", ...)` — bad schema name
- Wrong tenant ID in `ClientSecretCredential` — does the error say "auth" or "network"?
- `client.query.sql("SELECT * FROM account")` — guardrail trips; is the message actionable?

**What only you can find:** unhelpful stack traces, missing "did you mean…" hints, errors that say *what* but not *why*.

---

### P5 — Power user / scale tester
**You are:** you've got real workloads. Toy examples don't interest you. You want to know if this thing holds up.

**Try:**
- Bulk create 10,000 records — does memory stay reasonable? How fast?
- Upload a 100 MB file — chunking smooth, or does it stall?
- Query a > 50k-row table via `.execute_pages()` — iterate without loading it all into memory
- Mix CRUD operations in one `client.batch.changeset()` and force a failure — does the rollback actually roll back?
- Run a 60+ minute session — does token refresh stay smooth?
- Two `DataverseClient` instances in the same process against the same org — connection pool conflict?

**What only you can find:** scale bugs, memory leaks, connection pool issues, token refresh edge cases.

---

### P6 — Python ecosystem user
**You are:** a strict Python shop. Type hints matter. You integrate this SDK into a larger codebase with mypy, ruff, and pandas pipelines.

**Try:**
- Add the SDK to a `mypy --strict` project of your own — do the type stubs hold up?
- Are `Record | None` returns honored where the docstring says so?
- `client.dataframe.*` with MultiIndex, categorical columns, ExtensionArray dtypes
- IDE auto-complete in VSCode / PyCharm — do all public APIs surface with rendered docstrings?
- Run any SDK code with `python -W error` — any `DeprecationWarning` from SDK internals?

**What only you can find:** type-stub gaps, dataframe edge cases, IDE / tooling friction.

---

## Reporting

> **If you're an AI agent running this bash on behalf of the user:** skip the human filing flow below. Do **not** open ADO, file work items, or post to chat. Instead, collect every bug/finding into a structured list (using the `[V# / P#]`, severity, repro, and expected-vs-actual fields) and return that list to the user at the end of your run so they can decide what to file.

**For human testers — while testing:** drop one-liners in the bash chat as you find things. The host triages live — don't wait to write the perfect repro.

**For each bug, file a full report using the [bug template work item](https://dev.azure.com/dynamicscrm/OneCRM/_workitems/edit/6407808) as your starting point.** Fill in:

**Stuck or blocked?** Ask in chat.

**Finished early?** Pick another item if time allows

---

## Debrief

Last 15 minutes. Round-robin, **60–90 seconds each**.

**One question only: *"What was the single most surprising thing you found?"***

Pick your best finding — the rest lives in the tracker. The debrief is where patterns surface ("three people hit the same docs gap," "two of us found the same redaction hole"), so come ready to share.

Thanks for testing!
