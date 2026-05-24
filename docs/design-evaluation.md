# Dataverse Python SDKs — Design Evaluation

**Status**: Draft for review

---

## 1. Summary

Two Python SDKs for Dataverse are in flight: a **client SDK** for scripts, notebooks, and integrations (v0.1.0b8, approaching GA) and a **server-side runtime** for Python plugins running in the Dataverse sandbox (greenfield, in design). Both face the same typing question — stay with the current dict-based API (`client.records.create("account", {"name": "X"})`) or adopt strongly-typed entity classes (`client.records.create(Account(name="X"))`) — and either SDK could plausibly answer differently from the other.

This document recommends an **asymmetric** answer:

- **Client SDK at GA — dict API + SQL-style query surface.** The dict write API (`client.records.create("account", {...})`) ships as the GA contract. The beta's fluent `QueryBuilder` is **replaced pre-GA** by a SQL-style query surface (`select(...).where(...)`) matching the server SDK — one query idiom across script and plugin code. GA timeline adjusts to accommodate; the permanent cost of shipping two query surfaces outweighs the schedule delta.
- **Client SDK typing — evidence-gated.** Whether to *add* a strongly-typed entity layer on top of the dict API is deferred to a head-to-head evaluation (§3.1), not shipped on theoretical benefit. A `DataverseModel` Protocol ships pre-GA as a ~10-line seam so the option stays open without a future API break.
- **Server SDK — ship typed.** The production-reliability case (§3.2) justifies typing independent of agent-authoring behavior.
- **Both SDKs share the same query idiom and shared model package.** SQL-style (`select` / `insert_into` / `update` / `delete`) is the single query surface. `Entity`, `FilterExpression`, `Record`, and the SQL-style free functions live in one shared package consumed by both SDKs.

### 1.1 Decisions at a glance

| Question | Decision |
|---|---|
| Delay GA for **typing** work? | No — typing decision is evidence-gated (§3.1) |
| Delay GA for the **SQL-style query-surface** replacement? | Yes — accept a schedule shift; the permanent two-surface tax outweighs the delta (§3.3) |
| One pre-GA change worth making (beyond the query surface)? | Yes — define `DataverseModel` Protocol and widen write-signatures to accept it (~10 lines, zero behavioral change) |
| Ship a typed entity layer in the client SDK? | Evaluation-gated — see §3.1 |
| Ship a typed entity layer in the server SDK? | Yes — see §3.2 |
| Require code generation to use either SDK? | No. Codegen is always opt-in, build-time, scoped via `--tables` |
| Shared `Entity` / query IR across client and server? | Yes — one package, one generator, one output format |
| Primary query idiom in both SDKs? | SQL-style (`select` / `insert_into` / `update` / `delete`) — **sole** programmatic query builder at GA |
| Keep fluent `QueryBuilder` from beta at GA? | No. One query idiom, not two. Beta callers migrate via codemod |
| Runtime metadata fetching at client startup? | No — defeats static analysis and introduces cold-start tax |
| Deprecate the dict API ever? | No |

### 1.2 Phased plan

1. **Client SDK GA** — ship the dict API. Replace the beta's fluent `QueryBuilder` with SQL-style (`select` / `insert_into` / `update` / `delete`) as the sole programmatic query builder. Add the `DataverseModel` Protocol seam (§4). Publish a codemod to migrate beta users.
2. **Evaluation (1–2 weeks)** — run 20 representative tasks across three API arms; decide whether typed client-side adds measurable value (§3.1).
3. **Client SDK post-GA minor** — path branches on the evaluation. Either stop at GA's dict + SQL-style surface, or additionally ship the typed entity layer.
4. **Server SDK alpha** — typed-first, SQL-style, shared query IR with client. Independent of client evaluation result.

---

## 2. The three approaches

### 2.1 Dict-based (current shipping client SDK)

```python
client.records.create("account", {"name": "Contoso", "revenue": 1_000_000})
row = client.records.get("account", guid)
row["name"]              # string keys; no IDE autocomplete; typos surface at runtime
```

Pros: zero ceremony, excellent for scripts and notebooks, trivially Pythonic.
Cons: no compile-time validation, no autocomplete.

### 2.2 Strongly-typed with mandatory codegen

Two sub-variants; both are rejected.

**Runtime generation** (types built from a live metadata fetch at startup):

```python
Account = await client.get_type("account")    # network round-trip required
client.records.create(Account(name="Contoso"))
```

Failure modes:
- Static analyzers (mypy / pyright / Pylance) cannot see types built by `type(...)` at runtime — they resolve to `Any`. The headline benefit of typing does not arrive.
- Per-cold-invocation metadata fetch in serverless environments.
- Schema drift between runs is silent and non-reviewable.
- Offline testing requires a metadata mock or a live org.

**Build-time with hard cutover** (generator emits `.py` files; dict API removed):
- A breaking change for existing users with no benefit over the additive approach below.

### 2.3 Additive typed layer (recommended shape if typing ships)

```python
# Dict form continues to work unchanged
client.records.create("account", {"name": "Contoso"})

# Typed form is opt-in
class Account(Entity, table="account", primary_key="accountid"):
    name    = Text(max_length=160)
    revenue = Money()

client.records.create(Account(name="Contoso", revenue=1_000_000))
```

- All operations accept `Union[str, type[Entity]]` and `Union[dict, Entity]`.
- Code generation is build-time, produces editable Python files checked into source control, and is scoped with `--tables` (no recursive relationship descent).
- Types are versioned snapshots of server schema; regenerating is the sync command.

This matches the pattern used by Prisma, gRPC/protobuf, boto3-stubs, and the .NET Dataverse SDK itself (see Appendix A).

---

## 3. Why asymmetric

### 3.1 Client-side: the agent-grounding argument has weakened

The strongest original argument for client-side typing was that agents would make fewer schema mistakes. Observed behavior of the shipping Dataverse Skills plugin contradicts this — agents write correct Python against the dict API on common scenarios.

Grading what remains:

| Claim | Holds once agents are already competent with dicts? |
|---|---|
| IDE autocomplete on 500-field entities | No — agents don't use IDEs |
| mypy / pyright catches typos pre-deploy | Only if CI runs typechecking on the agent-authored call sites, which most teams don't |
| Refactoring support on column renames | No — agents regenerate code rather than refactoring it |
| Self-documenting schema in source | Weak — duplicates metadata available via MCP and the portal |
| .NET SDK parity | Aesthetic; the .NET SDK's early-bound value was human-IDE autocomplete, an agent-era artifact |
| Less agent hallucination | Contradicted by observed behavior |
| Token efficiency on complex multi-entity workflows | Holds — types in workspace eliminate exploratory metadata round-trips |
| Enterprise / ISV long-term code maintenance | Holds — stable contracts and static verifiability matter at scale |

The remaining value is real but narrower than the initial pitch. Whether it justifies the investment is an empirical question.

**Proposed evaluation** (1–2 weeks, cheap relative to the 6–8 weeks of post-GA work a typed-client layer would require):

- **20 representative tasks**: simple CRUD, multi-entity workflows, polymorphic lookups, option-set enums, bulk operations, tenant-custom schemas.
- **Three API arms, answering two distinct questions**:
  - **(A) dict + current fluent `QueryBuilder`** — *reversibility check* on the SQL-style commitment in §3.3. If A materially beats B, the plan to remove fluent at GA should be reopened.
  - **(B) dict + SQL-style** — the GA baseline.
  - **(C) typed + SQL-style** — the additive typed layer under evaluation.
- **Measure**: task success rate, tokens consumed, correction cycles, wall-clock.
- **Decision thresholds** (concrete, not qualitative):
  - **Ship typed client-side only if (C) vs (B)**: task-success parity (within ±3 pp) AND tokens ≥15% lower **OR** correction cycles ≥20% lower.
  - **Reconsider the SQL-style commitment only if (A) vs (B)** clears the same bar in favor of A. Expected outcome: parity or minor B advantage; the industry precedent in §3.3 would need direct contradiction to move.
  - **If results are mixed** (e.g., C wins on tokens but loses on success): hold the typed client. The Protocol seam (§4) keeps the option open for re-evaluation with better tooling later.

### 3.2 Server-side: production reliability stands independently

Plugin code runs on live business events. Five structural asymmetries make left-shifted error detection more valuable server-side than client-side:

1. **Test coverage is structurally harder.** Plugin context, pre/post-images, sync-vs-async triggers, and organization-service state are complex to mock. Agent-generated plugins rarely ship with comprehensive tests; script tests are easier and typically have better coverage.
2. **Silent-failure modes are common.** Plugin code can catch-and-log exceptions and continue in corrupted state. Scripts crash loudly and immediately.
3. **Blast radius differs by an order of magnitude.** Script failure → developer fixes script. Plugin failure → failed business transaction, possibly across many records, possibly customer-visible.
4. **Feedback-loop latency.** Script error: seconds (someone is watching the terminal). Plugin error: minutes to days (trace log, incident triage). Types collapse detection to edit time regardless.
5. **Deploy / rollback cycle.** Plugins take minutes to re-register and re-deploy. Every typo caught at typecheck-time saves real wall-clock.

None of these are absolute — good tests and CI can catch typos without types. But the expected value of left-shifted detection is meaningfully higher server-side. This is the same argument that has kept early-bound entities the dominant plugin-authoring style on the Dataverse .NET SDK for 15 years, and it is independent of how well or poorly agents write plugin code.

**Costs we accept on the server side.** Typing is not free. Shipping server-typed adds (a) a generator we must keep aligned with client-side codegen, (b) shared-package release coordination so client and server stay on compatible Entity contracts, (c) generated files in plugin authors' repos that require re-running the generator on schema changes, and (d) documentation surface covering both typed-only and dict-interop patterns. These are real, but each is bounded: the generator is shared with the client (§3.5), and schema-drift handling is already required for any plugin that reads metadata. The production-reliability wins outweigh these specific costs; the calculus would be different client-side, which is why §3.1 exists.

**What "ship typed server SDK" means concretely** — same `Entity` base class and field descriptors as the client-side typed layer (if one ships) or as the shared model package regardless; SQL-style query surface (§3.3); generator CLI; a starter set of pre-generated platform-entity types (§5.5 applies); plugin-context execution model that accepts both Entity instances and dicts for the first release. Appendix B points at a prototype demonstrating this shape.

### 3.3 Query syntax: one idiom, both SDKs

Current divergence:

| | Client SDK shape | Server SDK shape |
|---|---|---|
| Read | `client.query.builder(Account).where(Account.name == "X").execute()` | `ctx.dataverse.sql(select(Account).where(Account.name == "X")).execute()` |
| Create | `client.records.create(Account(name="X"))` | `ctx.dataverse.sql(insert_into(Account).value(Account.name, "X")).execute()` |
| Update from fetched | `client.records.update(obj)` | `ctx.dataverse.sql(update(Lead).from_entity(payload).where(Lead.leadid == id)).execute()` |

Both compile to the same conceptual query; only the surface differs. Users and agents copying code across the boundary feel the seam.

**Recommendation: adopt SQL-style (`select` / `insert_into` / `update` / `delete`) as the one primary query idiom in both SDKs.**

Reasons:

1. **Re-learnability is zero** — plugin code and script code look identical.
2. **SQLAlchemy 2.0 deliberately moved in this direction**, from fluent `session.query(User).filter(...)` to `select(User).where(...)`. Stated rationale: maps more directly to SQL, composes better, more explicit about execution.
3. **SQL is universally known** by developers and by LLMs — no new vocabulary.
4. **Client SDK is pre-GA.** Beta callers are signing up for churn; the cost of changing the recommended surface now is bounded.

The existing fluent `QueryBuilder` in the beta is **removed before GA**. One programmatic query builder, not two. Beta users migrate via a mechanical rewrite, publishable as a codemod (an AST-based code-transformation tool — LibCST / Bowler in Python); the transformation is a 1:1 method-to-function mapping, e.g. `builder("account").filter_eq("statecode", 0).execute()` → `select("account").where(eq("statecode", 0)).execute()`. The beta population is small (~500 users) and signed up for churn; the permanent two-surface documentation/test/agent-training cost of keeping both outweighs the one-time migration burden.

### 3.4 The consolidated GA surface

Everything the SDK ships at GA, and why each surface is defensible once the fluent `QueryBuilder` is cut:

| Surface | Why it stays |
|---|---|
| **SQL-style query** (`select(...).where(...).top(...)` etc.) | The one programmatic query builder |
| **FilterExpression primitive** (`eq`, `gt`, `contains` + `& \| ~`) | Shared building block consumed by `where(...)`; already shipping, reused unchanged |
| **Imperative CRUD** (`client.records.{create, update, get, delete, upsert}`) | Single-record ergonomics; the `session.add(obj)` vs `insert(...)` split in SQLAlchemy |
| **Raw SQL string** (`client.query.sql("SELECT …")`) | Power-user escape hatch at a different abstraction level |
| **DataFrame adapter** (`client.dataframe.*`) | Output-shape concern, not a competing query syntax; accepts SQL-style queries |
| **Metadata operations** (`client.tables.*`) | Unrelated to data query/write |
| **File upload** (`client.files.*`) | Unrelated to data query/write |

Two adjacent cleanups to do while the query surface is in motion:

1. The existing `QueryBuilder` already has two internal ways to filter — 15 `filter_*(column, value)` methods and `where(FilterExpression)`. Only the `where(...)` form carries into SQL-style's surface. The `filter_*` method variants do not need to be reproduced.
2. `client.dataframe.*` should accept SQL-style queries (`client.dataframe.query(select(...).where(...))`) rather than carry its own query DSL.

### 3.5 What is shared between client and server SDK regardless

These pieces belong in a single package consumed by both SDKs:

- `Entity` base class and field descriptors (`Text`, `Integer`, `Money`, `Lookup`, `Picklist`, `Boolean`, `DateTime`, `Guid`, …)
- `Record` read-wrapper with `.id`, `.table`, `.etag`, dict-like access
- `FilterExpression` tree and class-level operator overloads (`Account.name == "X"` → expression object)
- `to_create_payload()` / `to_update_payload()` helpers that strip non-writable fields
- The SQL-style free functions (`select`, `insert_into`, `update`, `delete`)
- **One** metadata-driven code generator, **one** output file format

Per-transport compilation (OData `?sql=` on client; query expression / fetch XML in-process on server) stays in each SDK's `operations/` layer.

Two generators emitting subtly different Python is the concrete failure mode to avoid.

### 3.6 Metadata-management boundary (server SDK)

Server SDK v1 is read/write-data-only. Plugins do not mutate schema. This matches the .NET plugin model. Revisit in a later release if a schema-as-code story emerges.

---

## 4. The `DataverseModel` Protocol — the one pre-GA change

The single typing-related change worth making before client SDK GA:

```python
from typing import Any, ClassVar, Protocol, Self, Union

class DataverseModel(Protocol):
    """Any class that can be passed to a Dataverse write operation.

    Generated entity classes implement this protocol. Users may also hand-roll
    their own models (dataclass, Pydantic BaseModel, etc.) and pass them
    interchangeably with raw dicts.
    """
    __entity_logical_name__: ClassVar[str]
    __entity_set_name__:     ClassVar[str]

    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self: ...

# Widened signatures — dict callers unchanged; typed callers supported
Payload = Union[dict[str, Any], DataverseModel]

def create(
    table_or_model: Union[str, type[DataverseModel]],
    data: Union[Payload, list[Payload]],
) -> Union[str, list[str]]: ...
```

**No code generator ships. No generated classes ship. No user-visible behavior changes.** Existing dict callers continue to work unchanged.

Why it is worth the pre-GA effort: the Protocol is the permanent contract between the SDK and any future typed-codegen approach. If it ships in GA, codegen can land in a minor release without a public-API change. If it does not, every future typed-API approach requires a signature-widening and a deprecation cycle. Cheap now, expensive to retrofit.

---

## 5. Implementation considerations

### 5.1 Codegen will not cascade into the whole org

Navigation-property fields reference target tables by **logical-name string**, not by imported class. Users list entities explicitly (`--tables a b c`). No recursive descent into the relationship closure. Same mechanism as protobuf's explicit imports.

### 5.2 Generated file trees are not large

A generated entity file is roughly 20 KB. 500 entities ≈ 10 MB. Non-issue for disk, git, or IDE indexing. Users generate only the tables they use.

### 5.3 Schema drift is detectable and reviewable

Dataverse's `RetrieveMetadataChanges` API supports e-tag-based incremental metadata sync. Generator re-runs are fast. Three sub-cases:

| Change | Dict API | Typed API |
|---|---|---|
| Server adds column, caller doesn't reference it | Harmless | Harmless (typed access unknown, dict access via `record.data[...]` still works) |
| Server adds column, caller wants to use it | Works immediately | Re-run generator; commit diff |
| Server removes column that caller references | 400 at runtime | Re-run generator, code fails at typecheck OR runtime — strictly more information than the dict case |

### 5.4 "What is the source of truth?"

The server is the source of truth for platform-managed metadata. The generated Python file is a versioned snapshot — same pattern as `git submodule`, `go.sum`, or a generated gRPC stub. The local file is authoritative for compilation; the server is authoritative for the live schema. They can drift, and the generator is the sync command.

For user-defined custom entities (a future schema-as-code direction), local files could be push-authoritative. Out of scope for this decision.

### 5.5 Pre-shipped out-of-box entity types (conditional)

Applies only if the client-side evaluation justifies a typed client SDK.

Concern: Dataverse schemas are tenant-specific. Every org has different custom columns on the same "platform" entity. A pre-shipped `Account` class might mislead users about what their schema actually contains.

Resolution:

- Ship types covering **platform-guaranteed columns only** (`accountid`, `name`, `createdon`, `modifiedon`, `statecode`, `statuscode`, `ownerid`, …).
- Each shipped file carries a docstring: *"This class contains platform-guaranteed columns only. Run the generator to extend with tenant-specific custom columns."*
- Typed access for unknown fields falls through to `record.data[...]` — no correctness cliff, only missing autocomplete for tenant-custom columns.
- The model is boto3-stubs: cover the stable core; users accept that custom/evolving surface requires regeneration.

If false-completeness concerns show up in practice, deprecate the pre-shipped set and require user-side regeneration. Cheap to reverse.

### 5.6 Security

The server SDK's SQL-style surface routes through the same governed query path as the client. Parameterization is mandatory; there is no raw-SQL executor. The typing decision is orthogonal to injection defense.

---

## 6. Non-goals

- Shipping a runtime metadata cache
- Auto-generating the full relationship closure
- Converging the C# and Python SDK surfaces
- Requiring users to run a code generator before their first script

---

## 7. Follow-ups

1. **Evaluation design and execution.** 20 representative tasks × 3 API arms. Single highest-leverage action; determines whether client-side typing ships at all. 1–2 week window. Needs an owner.
2. **Pre-GA Protocol seam implementation.** ~1 week. Blocker only for permanent typed-codegen optionality at GA time.
3. **SQL-style query surface on the client SDK.** Built on the existing `FilterExpression` primitives; replaces the beta's fluent `QueryBuilder`. Ships at GA. GA timeline may need adjustment to accommodate; accepted in exchange for a clean single-query-idiom contract. Needs a codemod for beta users.
4. **Shared model package** (Entity, field descriptors, filter expressions, SQL-style functions). Owner, repo, release cadence — blocker for client/server symmetry under every path.
5. **Generator consolidation.** Two exploratory generators exist; reconcile to one shared tool with the best of each.
6. **Generator distribution.** PyPI extra (`pip install PowerPlatform-Dataverse-Client[gen]`) vs. a separate `dataverse-gen` package. Lean toward extra.
7. **Plugin metadata boundary.** Confirm server SDK is read/write-data-only for v1.
8. **GA version number and release cadence.** Does GA ship as `1.0.0` (stable-API signal) or continue the current `0.1.x` line? Post-GA minor cadence? This doc defers to the SDK team's PyPI/release policy.
9. **Codemod tooling and publication.** Which codemod framework (LibCST, Bowler, other) and where is it distributed (PyPI package? `pip install PowerPlatform-Dataverse-Client[migrate]`? a one-shot script in the repo)?

---

## Appendix A — Prior art

| Ecosystem | Typing approach | Lesson |
|---|---|---|
| .NET Dataverse SDK (15+ years) | Build-time: `CrmSvcUtil` / `pac modelbuilder build` emits early-bound classes. Late-bound `entity["name"]` remains alongside forever. | The closest structural precedent. Both idioms coexist; neither was ever deprecated. |
| Prisma (TypeScript) | Build-time: `prisma generate` emits a typed client from `schema.prisma`. Zero runtime generation. | Widely cited as best-in-class backend DX. |
| gRPC / protobuf | Build-time: `.proto` → generated Python classes. Runtime generation is explicitly unsupported. | Industry default for cross-language contracts. |
| boto3-stubs / mypy-boto3-builder | Stringly-typed at runtime; separate PyPI package ships pre-generated `.pyi` stubs. | Demonstrates additive typed layer on a scripting-first SDK. |
| SQLAlchemy `automap_base()` | Runtime reflection from a live DB schema. | Cautionary anti-pattern; consistently the least-used, least-trusted part of SQLAlchemy because static analyzers cannot see the result. Same failure mode as Option B-runtime in §2.2. |

Two observations from the list:

1. Every well-regarded typed SDK generates at **build time**. We could find no successful counter-example.
2. The closest structural analogue to Dataverse — its own .NET SDK — ships both early-bound and late-bound and has kept both. That is the right model for Python too.

## Appendix B — Existing prototype references

These branches and packages are exploratory prior work, not plans of record. Each has design ideas worth borrowing; none is the committed direction for either SDK.

- **Client SDK main** (`microsoft/PowerPlatform-DataverseClient-Python` at v0.1.0b8): dict API, fluent `QueryBuilder` with composable filter expressions, SQL endpoint via OData `?sql=`, Pandas bridge, batch operations, upsert with alternate keys, file upload, context-manager connection pooling.
- **Client SDK typed-layer prototype branch** (`users/*/typed_entity_model`): working additive typed layer — `Entity` base class, field descriptors (`Text`, `Integer`, `Money`, `Lookup`, `Picklist`, `Boolean`, `DateTime`, …), class-level operator overloads, generator CLI, ~1,300 unit tests passing. All operations accept `Union[str, type[Entity]]`. Demonstrates the additive shape from §2.3.
- **Server-side plugin runtime prototype** (`Dataverse.Sandbox.Runtime`): typed-first, SQL-style query surface, pre-generated types for a starter set of ~16 platform entities, metadata-driven generator tool, plugin-context execution model.

## Appendix C — Rejected options

- **Runtime type generation.** Defeats static analysis; introduces cold-start tax; silent schema drift; breaks offline testing. Types built via `type(...)` from a live metadata fetch are opaque `Any` to mypy/pyright/Pylance.
- **Mandatory typed-only client SDK (hard cutover).** Breaking change for existing dict callers with no benefit over the additive shape.
- **Two generators, two Entity models, two output formats.** Subtly different emitted Python across client and server is the specific failure mode that reintroduces the cross-context re-learning cost we are trying to remove.