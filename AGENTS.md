# AGENTS.md

## Project

- Codename: Mnemonic Engine (ME)
- Repository: `mnemonic`
- Primary crate: `engine` (Rust library crate: `mnemonic-engine`)
- License: Apache-2.0 (`engine/LICENSE`)

## Vision

Mnemonic Engine is a local-first, temporal, structured knowledge engine built around atomic `Cell` entities and structural `Fabric`.

Canonical structure:

- `Document` is the API/application root
- A `Document` owns one root `Cell`
- A `Cell` may reference one `Fabric`
- A `Fabric` contains an ordered collection of `Cell`
- The data layer stores fabric membership and ordering in `fabric_cells`

Core objective:

- Store small, atomic data units (`Cell`)
- Organize units through `Fabric`
- Preserve full history with temporal versioning
- Reconstruct deterministic state at any point in time

## Design Principles

1. Database enforces invariants.
2. Engine enforces business logic.
3. One canonical present state.
4. History is append-only.
5. Structure is explicit.
6. Composition over mutation.
7. Deterministic reconstruction.

## Core Domain Model

### Cell

A `Cell` is the atomic data unit.

Canonical meaning:

- A `Cell` is the smallest logical data container in the engine
- A `Cell` may exist on its own
- A `Cell` may optionally reference one `Fabric`
- A `Cell` does not stop being atomic just because it owns or references structure

Fields:

- `id` (stable identifier)
- `cell_type` (semantic classification)
- `format` (`text`, `markdown`, `json`, `binary`, etc.)
- `content` (payload)
- `valid_from`
- `valid_to`
- `fabric_id` (optional reference to a `Fabric`)

Versioning and keys:

- Composite primary key: `(id, valid_to)`
- Canonical open-ended sentinel for a live row: `valid_to = FUTURE_TIME`

Invariant:

- At most one active row per `id` at a given timestamp

### Fabric

`Fabric` represents the structure that contains and orders `Cell` entities.

Canonical meaning:

- A `Fabric` is built from cells
- A `Fabric` contains cells
- A `Fabric` preserves order among its member cells
- A `Fabric` is a logical structure, not a separate application root

Logical model:

- `Cell` may reference one `Fabric`
- `Fabric` contains an ordered collection of `Cell`
- Ordering is part of the logical model
- Storage implementation for ordering is a data-layer concern
- A `Fabric` is not the API root and must not replace `Document` as the public usage concept

Preferred logic-layer wording:

- Use `Cell`, `Fabric`, and `FabricCell`
- Do not introduce graph-specific wording such as `Edge` unless the system is explicitly being modeled as a graph engine
- Do not rename fabric membership into `Edge`, `FabricEdge`, or other graph-oriented terms

Storage model:

- `fabric_cell`: fabric metadata
- `fabric_cells`: membership and ordering of cells inside a fabric

Storage guidance:

- `fabric_cells` is the data-layer representation of fabric membership and order
- Ordering may be stored with ordinal positions, linked-list style pointers, or left/right tree-style indexing
- Storage strategy must not change the logical meaning of `Fabric` as an ordered collection of `Cell`

Supported structures:

- Set (unordered, no duplicates)
- List (ordered, duplicates allowed when the use case allows it)
- Nested fabric (through cells that reference a fabric)

Typical uses:

- Documents
- Product specifications
- Research structures
- Narrative or other hierarchical compositions

### Document

`Document` is the API/application root concept.

Canonical meaning:

- A `Document` owns one root `Cell`
- The root `Cell` may reference a `Fabric`
- The `Fabric` contains the ordered `Cell` collection that forms the document structure
- `Document` is the public usage model for application and API flows
- `Document` does not directly replace `Cell` or `Fabric`
- `Document` must not be described as directly owning a `Fabric`; it owns a root `Cell` that may reference one

Extension model:

- `Document` is the root of usage types exposed by the API
- Specialized forms such as text, table, dictionary, list, and map are document extensions
- `Cell` and `Fabric` remain the engine-layer primitives that power those extensions

## Naming Policy

Three naming layers must remain distinct:

- Engine layer: `Cell`, `Fabric`, `FabricCell`
- API/application layer: `Document`
- Data layer: storage-oriented names such as `data_cell`, `meta_cell`, `fabric_cell`, `fabric_cells`

Rules:

- Do not introduce extra conceptual nouns that blur the model
- Do not use storage terminology to redefine engine concepts
- Do not use graph terminology such as `edge` in the main logic model unless graph semantics are intentionally first-class
- Do not use `edge`, `node`, or similar graph words to describe the `Fabric` to `Cell` relationship in the normal logic model
- Use `FabricCell` in the engine layer and `fabric_cells` in the data layer for fabric membership representation
- Keep storage names focused on persistence shape and efficiency
- Keep API names focused on application usage and user-facing behavior

Canonical interpretation sequence:

- API/application view: `Document`
- Engine root: root `Cell`
- Structural view: referenced `Fabric`
- Structural members: ordered `Cell` collection
- Data-layer storage: `fabric_cells`

## Temporal Model

### Validity Window

Each version is valid in the interval:

- `valid_from <= T < valid_to`

Active version:

- `valid_from <= CURRENT_TIMESTAMP < valid_to`

Current-version query rule:

- Query using database time window semantics: `valid_from <= NOW() AND valid_to > NOW()`
- Do not require equality against the sentinel value for reads
- The sentinel value is only the default future end point for a live row

Update algorithm (canonical):

1. Close current active row by setting `valid_to = NOW()`
2. Insert new row with:
   - `valid_from = NOW()`
   - `valid_to = FUTURE_TIME`

Delete algorithm (canonical):

1. Close current active row by setting `valid_to = NOW()`
2. Do not insert a replacement row

Guarantees:

- No overlapping active versions
- Deterministic current state
- Full historical reconstruction

### Canonical Timeline Policy

Parallel active histories are not allowed.

If concurrent/offline updates conflict for `(id, valid_to = MAX_TIME)`:

1. Resolve conflict before promoting a new active version
2. Close previous active version
3. Insert merged or selected canonical version

Rationale:

- Deterministic merges
- Strong invariant enforcement
- Simpler sync logic
- No branch tree maintenance

## Storage Layer

### Primary Database

- SQLite

Reasons:

- Embedded and portable
- Reliable and local-first friendly
- No server requirement
- Supports partial indexes and CTEs

Future migration targets:

- PostgreSQL
- MySQL

Schema is intended to remain portable.

### Time Representation

- Database-native temporal types are preferred
- `valid_from` and `valid_to` may use `DATETIME` or `TIMESTAMP`
- Use the database current-time function such as `NOW()` or `CURRENT_TIMESTAMP()` for current-state comparisons
- Use a configured future sentinel such as `2100-01-01 00:00:00` or the maximum practical timestamp supported by the database

Guidance:

- The data layer should own the exact temporal column type and default values
- The logic layer should rely on validity-window semantics, not hard-code database time expressions
- Rust may use integers or other time representations internally when needed, but that should not redefine the database temporal model

Responsibility split for temporal handling:

- `valid_from` / `valid_to` are part of the storage/database mechanism for temporal history
- The engine layer should not treat timestamp mechanics as its primary concern
- Engine concerns are:
  - content update orchestration
  - fabric membership and ordering behavior
  - document context handling
- Storage concerns are:
  - table mapping
  - DDL defaults for temporal columns
  - current-state and historical-state filtering
  - active-row closure and append-only version storage
- The engine depends on storage-owned temporal behavior instead of owning database time expressions directly

### Tables

Core tables:

- `data_cell`
- `meta_cell`
- `fabric_cell`
- `fabric_cells`

Notes:

- Cell tables are temporal
- `fabric_cells` stores fabric membership and ordering
- Ordering may be implemented with ordinal position, linked-list style pointers, or tree-style indexing
- Storage strategy must not change the logical meaning of `Fabric` as an ordered collection of cells
- Temporal defaults and current-time evaluation belong to the database layer

Optional table:

- `change_log` (event log for sync/audit)

## Engine Architecture

### Core Engine Responsibilities

Implemented in Rust.

- Enforce invariants
- Rely on storage-owned temporal behavior while enforcing domain invariants
- Provide query APIs
- Define transaction boundaries
- Support optional sync module
- Perform lazy loading of fabric relationships

Responsibility split:

- Database = storage/invariant substrate
- Engine = `Cell` / `Fabric` domain logic and deterministic behavior
- API/application layer = `Document` as the public root across usage types
- Storage layer = performance-oriented schemas, including specialized tables for major usage patterns

### API Exposure (Planned)

- HTTP API (polyglot clients)
- FFI bindings (for Java/Node and others)
- Optional gRPC

Engine remains the authoritative state layer.

### Unified Concept vs Specialized Storage

The model is logically unified and physically optimized:

- Logical/engine model: data is built from `Cell` and organized through `Fabric`
- API/application model: usage is exposed through `Document`
- Physical/storage model: major usage categories can use dedicated tables to maximize performance and feature support
- Requirement: specialized tables must still follow temporal invariants and deterministic reconstruction rules

## Reconstruction Model

Current-state reconstruction (`now`):

1. Resolve the root `Cell`
2. Resolve the referenced active `Fabric` if present
3. Fetch active `fabric_cells`
4. Fetch active cells in document order
5. Assemble in memory

Query rule:

- Current state: filter by database time window (`valid_from <= NOW() AND valid_to > NOW()`)
- Historical state: filter by time window (`valid_from <= :time_point AND valid_to > :time_point`)

Database rule:

- For a temporal item such as `Cell` or `Fabric`, the live row is the row whose validity window contains the current database time
- A uniqueness constraint or primary key using `(id, valid_to)` ensures there is only one live row for an item under the canonical future sentinel policy

Expected engine methods:

- `get_cell(id)`
- `get_cell_at(id, timestamp)`
- `build_context(fabric_id)`
- `build_context_at_time(fabric_id, timestamp)`

## Sync Strategy (Future Phase)

Model:

1. Pull remote changes
2. Apply in deterministic order
3. Resolve active-version conflicts before insertion
4. Write canonical result as the new active row

Conflict strategy options:

- Last-write-wins (timestamp-based)
- Logical clock ordering
- Optional content-level CRDT merge

Temporal history is always preserved; resolution only determines canonical present.

## Extension Strategy

Extend behavior by:

- Adding new `cell_type`
- Adding new `relation_type`
- Extending engine logic

No schema redesign should be required for these extensions.

## Non-Goals

- Distributed consensus layer
- Multi-branch version trees
- Monolithic document-style storage
- Heavy ORM abstraction
- Premature CRDT-first architecture

## Tests

- `engine/tests/basic.rs`
  - End-to-end lifecycle coverage for:
    - cell creation
    - linking and relation lookup
    - temporal content updates

## Build and Validation

Run from `engine/`:

- `cargo fmt`
- `cargo test`
