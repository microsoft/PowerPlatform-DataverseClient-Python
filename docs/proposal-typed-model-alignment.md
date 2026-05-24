# Proposal: Typed Entity Model — Alignment with Server SDK and GA Scope

**Status:** Draft for discussion  
**Date:** 2026-04-20

---

## Background

The Python **client SDK** (`PowerPlatform.Dataverse`, `main` branch) uses a string-based programming model: table names, column names, and filter expressions are all plain strings at runtime. The **server SDK** (`Dataverse.Server.Runtime.Extension`) uses a strongly typed entity model where each table is a generated Python class, fields are typed descriptors, and queries are expressed as composable condition objects.

A prototype branch explored what it would take to add typed entity support to the client SDK. It produced a working implementation — but the baseline for this proposal is the **`main` branch**, not the prototype. The prototype is evidence of feasibility and reveals the specific gaps.

---

## API Comparison: Client SDK vs. Server SDK

The table below covers both SDKs as they exist today (`main` branch for the client, current state for the server).

| Category | Feature | Client SDK | Server SDK |
|---|---|---|---|
| **Entity / Type Model** | Entity base class | No (main); prototype branch adds it | Yes — `Entity` with `_logical_name`, `_entity_set`, `_primary_id`, `_primary_name` |
| | Field descriptors | No (main); prototype adds `Text`, `Memo`, `Integer`, `BigInt`, `DecimalNumber`, `Double`, `Money`, `DateTime`, `Guid` | Yes — same set |
| | `Lookup` / `CustomerLookup` | No (main); prototype adds both | Yes |
| | `PicklistBase` / `PicklistOption` / `MultiPicklist` | No (main); prototype adds all | Yes |
| | `BooleanBase` / `BooleanOption` | No (main); prototype adds both | Yes |
| | `Entity.as_dict()` | No (main); prototype adds | Yes |
| | `Entity.to_create_payload()` / `to_update_payload()` | No (main); prototype adds | Yes |
| | `Entity.from_dict()` | No (main); prototype adds | Yes |
| | `Entity.fields()` | No (main); prototype adds | Yes |
| | Entity class declaration syntax | N/A (main); prototype adds keyword-arg style: `class Foo(Entity, table="…")` | Plain class attributes only: `_logical_name = "…"` |
| **Query / Filter DSL** | Comparison operators on field descriptors (`==`, `!=`, `>`, etc.) | No (main); prototype adds via `_ComparisonFilter` | Yes — via `Condition` |
| | Logical composition (`&`, `\|`, `~`) | Yes (on `FilterExpression` objects, string-based) | Yes (on `Condition` / `CompositeCondition`) |
| | Filter rendering target | OData `$filter` strings | Parameterized SQL fragments (`.to_sql()`) |
| | `filter_in` / `filter_not_in` | Yes | No |
| | `filter_between` | Yes | No |
| | String function filters (`contains`, `startswith`, `endswith`) | Yes | No |
| | Null checks | Yes | No |
| | Raw filter passthrough | Yes | No |
| **CRUD Operations** | Single record create | Yes — `client.records.create(table, dict)` | No |
| | Bulk create (`CreateMultiple`) | Yes | No |
| | Single record read | Yes — `client.records.get(table, id)` | No |
| | Multi-record read (paginated) | Yes | No |
| | Single record update (PATCH) | Yes | No |
| | Bulk update (`UpdateMultiple`) | Yes | No |
| | Single record delete | Yes | No |
| | Bulk delete (`BulkDelete` async job) | Yes | No |
| | Upsert (alternate key) | Yes — `client.records.upsert()` with `UpsertItem` | No |
| | Typed entity class accepted in place of table string | Prototype only | N/A — no execution layer |
| **Query Execution** | OData `$filter` queries | Yes | No |
| | SQL queries (read-only, constrained subset) | Yes — `client.query.sql()` | No |
| | `$expand` (related record navigation) | Yes | No |
| | `$orderby` / `$top` / `$count` | Yes | No |
| | Pagination (`$skiptoken`) | Yes | No |
| | Fluent QueryBuilder | Yes | No |
| | `QueryBuilder.to_dataframe()` | Yes | No |
| | Aggregations (GROUP BY, SUM, AVG, COUNT) | No | No |
| | Joins | No (`$expand` only for navigation properties) | No |
| | FetchXML | No | No |
| **Table / Metadata Management** | Create table | Yes — `client.tables.create()` | No |
| | Delete table | Yes | No |
| | Get table info | Yes | No |
| | List tables | Yes | No |
| | Add / remove columns | Yes | No |
| | Create 1:N relationship | Yes | No |
| | Create N:N relationship | Yes | No |
| | Create lookup field | Yes | No |
| | Delete relationship | Yes | No |
| | Alternate key management | Yes (create / get / delete) | No |
| **Batch & Transactions** | OData `$batch` multi-request | Yes — `client.batch` | No |
| | Atomic changesets (all-or-nothing) | Yes — `batch.changeset()` | No |
| | Content-ID cross-referencing within changeset | Yes | No |
| | Max operations per batch | 1000 | N/A |
| **DataFrame (pandas)** | `client.dataframe.get()` → `DataFrame` | Yes | No |
| | `client.dataframe.create()` from `DataFrame` | Yes | No |
| | `client.dataframe.update()` from `DataFrame` | Yes | No |
| | `client.dataframe.delete()` from `Series` | Yes | No |
| **File Operations** | File column upload | Yes — `client.files.upload()` (small + chunked) | No |
| | File column download | No | No |
| | File column delete | No | No |
| **Code Generator** | Generate entity classes from Dataverse metadata | No (scaffold exists, not implemented) | Yes — `generator.generate(org_url, entities, credential, output_dir)` |
| | Generates picklist / boolean / intersect types | No | Yes |
| | Auto-fetches Lookup and M2M dependencies | N/A | Yes |
| | Output imports from shared base classes | N/A | Yes — imports from `core` |
| **Annotations** | Formatted values (`OData.Community.Display.V1.FormattedValue`) | Yes — `include_formatted_values()` | No |
| | Custom OData annotation patterns | Yes — `include_annotations(pattern)` | No |
| **Diagnostics** | HTTP request/response logging | Yes — opt-in via `log_config` | No |
| | Automatic header redaction in logs | Yes | No |
| **Change Tracking** | Delta queries / change tracking | No | No |
| **Real-time / Push** | Webhooks, server-sent events | No | No |

### Reading the table

**Client SDK** is a full HTTP execution SDK — it handles auth, HTTP, OData serialization, pagination, and bulk operations. Its gap today is the typed entity model (no generated classes, no field descriptors, no type-safe query DSL).

**Server SDK** is a type-system and code-generation framework with no execution layer. Its strength is the generator and the richness of the typed entity model. It has no way to actually send a request to Dataverse.

The two SDKs are currently **complementary, not overlapping**: the server SDK produces the types; the client SDK executes the operations. The alignment question is whether the type layer can be shared so that entity classes generated by the server SDK's generator also work as typed arguments to the client SDK's execution layer.

---

## The Four Decisions

1. **Alignment**: Should the client SDK adopt a typed entity model, and if so, how closely should it align with the server SDK's contract?
2. **Divergence risk**: What happens if both SDKs evolve their entity models independently over time?
3. **Lock-in and GA timing**: If the string model ships at GA and the typed model ships later, how difficult is it for developers to switch — and does the timing of GA matter?
4. **Shared schema library**: Could both SDKs share a common package for entity/field descriptor definitions? Is that feasible with the current layout?

---

## Current State of the Two SDKs

### Client SDK (`main` branch)

| Layer | Current state |
|---|---|
| Table/column names | Plain strings everywhere |
| Filter expressions | OData strings (e.g., `"statecode eq 0"`) |
| Record access | `result.data["fieldname"]` dict access |
| Type safety | None — no IDE completion, no compile-time checks |
| Code generator | Not present |

### Server SDK

| Layer | Current state |
|---|---|
| Table/column names | Typed Python classes (generated) |
| Filter expressions | Composable `Condition` objects that render to SQL |
| Record access | `account.name` typed attribute access |
| Type safety | Full — IDE completion, Pylance inference |
| Code generator | Implemented (`generator/`) |

### Key structural differences

The server SDK's `core/` layer (`entity.py`, `datatypes.py`, `picklist.py`, `boolean.py`, `lookup.py`) is already logically a standalone schema library — it has no HTTP, OData, or Dataverse API dependencies. However, the two SDKs differ in ways that matter for alignment:

| Concept | Server SDK | Client SDK (main) | Notes |
|---|---|---|---|
| Entity base | `Entity` with `_entity_set`, `_primary_name` | No typed entity model | — |
| Condition rendering | `Condition.to_sql()` — SQL fragments | OData strings | Fundamentally different targets |
| Payload methods | `to_create_payload()` returns `Entity` instance | Not present | Server returns typed objects; client needs HTTP-serializable dicts |
| Boolean type | `BooleanBase` + `BooleanOption` | Not present | Server SDK naming already uses `BooleanBase` |
| Picklist type | `PicklistBase` + `PicklistOption` | Not present | Naming is aligned |
| Generator | Implemented | Not present | Both would need to produce from same base classes |

The condition rendering difference is the most significant: server-side execution speaks SQL; client-side execution speaks OData. A shared base class can define the operator overloads (`==`, `>=`, etc.) but the rendering backend must be environment-specific.

---

## Decision 1: Should the Client SDK Adopt a Typed Model?

The prototype demonstrated that the typed model can be added to the client SDK as a purely opt-in, backward-compatible layer — the string-based API is unaffected. The benefit is real: IDE completion, refactor safety, no string duplication across schema, create/update/query consistency.

The question is not whether to add it, but whether to add it in a way that is aligned with the server SDK or in a way that is independent.

**Recommendation: Yes, aligned.** Independent implementation is the path to the divergence problem described in Decision 2.

---

## Decision 2: Lock-in and GA Timing

The string model and the typed model are not equally easy to adopt at any point in time. There is an asymmetry: switching from strings to typed entities requires active code changes, while the reverse (typed to strings) rarely happens. This creates a one-way lock-in dynamic.

### What developers build on top of the string model

Once a team adopts the string-based client SDK, they tend to build their own abstractions on top of it:
- Configuration files or constants holding table and column name strings
- Wrapper functions like `def get_accounts(client, select): return client.records.get("account", select=select, ...)`
- Generic utilities parameterized by table name strings
- Automated tooling that generates string-based calls from metadata

None of these abstractions migrate cleanly to typed entities. A typed model requires that the *entity class itself* be the unit of schema definition — string-based wrappers are structurally incompatible, not just inconvenient.

### The adoption momentum problem

GA is not just a version number — it is a signal to developers that this is the stable, recommended way to build. Whatever model is present at GA becomes:
- The pattern in the first blog posts and tutorials
- The model in internal starter templates and onboarding guides
- The shape of the first production codebases

If the string model is the only model at GA, early adopters build on strings. When typed entities arrive later, those developers face a real refactoring cost to switch — and many will not. The string model becomes entrenched not because it is better, but because it came first.

This is different from a typical backward-compatible addition (e.g., adding a new method). Adding a new optional parameter does not require existing callers to change anything. Adding typed entities as an *alternative* to strings means developers must actively choose to rebuild what they already have. The later typed entities arrive, the more code already exists that won't be rebuilt.

### The cost of the switch, concretely

Given the prototype work, the migration from string model to typed model for an existing codebase involves:
1. Defining (or generating) an entity class for every table the codebase touches
2. Replacing every string-literal table name with the class reference
3. Replacing every string-literal column reference in `select`, `filter`, `orderby` with the typed field or string equivalent on the class
4. Replacing dict-based record access (`result.data["fieldname"]`) with typed attribute access

Steps 1 and 4 are the expensive ones for large codebases. Step 1 requires the generator to be available. Step 4 requires touching every consumer of query results. Even with the coexistence approach (strings still work), there is no incremental migration path — you either have an entity class for a table or you don't.

### Implication for GA timing

The typed model does not need to be the *only* model at GA, but it needs to be present and recommended at GA if it is the intended long-term programming model. A typed model that ships 6–12 months post-GA will face an installed base that has no reason to migrate, regardless of how good the model is.

**Recommendation:** If the typed entity model is the intended long-term direction, it must ship at GA or within a short post-GA release that is clearly signaled before GA. Shipping the string model as the only model at GA without any typed entity support, and without explicitly communicating that typed entities are coming, risks cementing the string model as the community standard.

---

## Decision 3: Divergence Risk

If both SDKs independently define their own `Entity`, `Text`, `PicklistBase`, etc., divergence is guaranteed over time:

- **Field type drift**: a new field type added to one SDK (e.g., a `RichText` or `BigInt` descriptor) will not appear in the other unless both teams coordinate every change.
- **Generated code portability**: if a user generates entity classes using one SDK's generator, those classes will not work with the other SDK. A developer running code both client-side (HTTP calls) and server-side (plugin execution) would maintain two sets of entity classes.
- **Parameter drift**: the prototype already found one divergence (`DateTime.format=` on the server vs `DateTime.date_format=` on the client prototype). Without a shared source of truth, these accumulate.
- **Behavioral drift**: `to_create_payload()` returning a typed `Entity` vs. a plain `dict` — if both SDKs add payload helpers independently, their signatures will diverge and user mental models will break.

The cost of divergence grows with adoption. Before GA, fixing it is cheap. After GA, it is a breaking change.

---

## Decision 4: Shared Schema Library

### What could be shared

The schema definition layer — `Entity`, `_FieldBase`, `Text`, `Integer`, `DateTime`, `PicklistBase`, `PicklistOption`, `BooleanBase`, `BooleanOption`, `Lookup` — has no runtime dependencies. It is pure Python. Both SDKs use it only for:

1. Defining entity class schemas (descriptor protocol)
2. Schema introspection (`Entity.fields()`, `Picklist.options()`)
3. Operator overloads for building filter/condition expressions

This layer is a natural candidate for extraction into a standalone package, e.g., `microsoft-dataverse-schema`.

### What cannot be shared (environment-specific)

| Layer | Server SDK | Client SDK |
|---|---|---|
| Condition rendering | `.to_sql()` → SQL + params | `.to_odata()` → OData filter string |
| Payload serialization | Return `Entity` instance | Return `dict` for HTTP body |
| Code generator runtime | Calls OData metadata API via `requests` | Same, but different output templates |

Each SDK would subclass or extend the shared base to add its own rendering layer. The shared package defines `_FieldBase.__eq__` etc. and returns an abstract `Condition` object; each SDK provides the concrete renderer.

### Is this feasible with the current layout?

**Technically: yes, with modest refactoring.** The server SDK's `core/` module is already structured as a self-contained library. The changes needed are:

1. **Extract `core/` from the server SDK** into its own repository/package with its own versioning.
2. **Refactor client SDK**: replace `src/PowerPlatform/Dataverse/models/` (as prototyped) with an import of the shared package. The client SDK extends the shared `Entity` and `_FieldBase` with OData rendering.
3. **Align naming differences**: `_entity_set` vs `_entity_set_name`, `BooleanOption` naming, `DateTime.format` parameter — resolve these before extraction so the shared package has one canonical API.
4. **Shared generator**: the generator backend (metadata fetch, normalization) can remain SDK-specific; but the code generation templates that emit entity classes should produce code that imports from the shared package.

**Organizationally: this is the harder part.** A shared package requires:
- An owner (which team? both?)
- A release process independent of both SDKs
- A versioning policy so both SDKs can consume updates without coupling their release schedules
- Agreement on the naming discrepancies before they become public API

The prototype demonstrates that the descriptor design is sound. The risk is not technical; it is governance and coordination.

---

## Recommendation Summary

| Decision | Recommendation |
|---|---|
| Adopt typed model in client SDK? | **Yes.** The prototype proves it is backward compatible and low-risk to add. |
| Align with server SDK? | **Yes, intentionally.** Independent implementations guarantee divergence and impose a future migration cost on developers who use both SDKs. |
| How to align? | **Extract a shared `microsoft-dataverse-schema` package.** Both SDKs import from it; each adds its own rendering layer (OData vs. SQL) on top. |
| Lock-in / GA timing | **The typed model must be present at GA, or clearly committed with a near-term date.** Shipping the string model as the only model at GA without typed entities risks cementing string-based patterns as the community standard before the typed model arrives. The migration cost only grows after GA. |
| Push GA for this? | **Yes, if the typed model is the intended long-term direction.** The naming alignment fixes (DateTime parameter, BooleanOption naming) are small. The generator is the main scope item — without it, typed entities are incomplete for connecting to existing large schemas. The correct question is not "is a slip worth it?" but "what pattern do we want the first wave of production code to follow?" If the answer is typed entities, they need to be at GA. |
| Worst outcome | Shipping GA with strings only, adding typed entities 6+ months later, and discovering the installed base has no migration incentive — leaving both models in the SDK indefinitely with neither clearly recommended. |

---

## Open Questions for Discussion

1. **Is the typed entity model the intended long-term programming model for the client SDK?** If yes, it must ship at or very close to GA. If no (strings remain the primary model indefinitely), the alignment work is lower priority.
2. **What is the acceptable GA slip to include typed entities?** The prototype took a few days to build. The generator and naming alignment are the remaining scope. A concrete estimate is needed to make the GA trade-off decision.
3. **Is there a customer scenario today where a user runs the same entity class code on both client and server?** If yes, the shared schema library is urgent. If no, the timeline pressure is lower but the design should still account for it.
4. Who owns the shared schema package? Is it a new repo under the same org, or does it live with one of the SDKs?
5. Are the naming discrepancies (listed above) acceptable breaking changes before GA, or do they need to be carried as deprecated aliases?
6. Should the generator be shared (one tool, two output modes: client vs. server), or remain separate tools that produce compatible output?
