# AGENTS.md

## Project

- Codename: Mnemonic Engine (ME)
- Repository: `mnemonic`
- Primary crate: `engine` (Rust library crate: `mnemonic-engine`)
- License: Apache-2.0 (`engine/LICENSE`)

## Vision

Mnemonic Engine is a local-first, temporal, structured knowledge engine built around atomic `Cell` entities and relational `Fabric`.

Core objective:

- Store small, atomic data units (`Cell`)
- Connect units through `Fabric`
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

Fields:

- `id` (stable identifier)
- `cell_type` (semantic classification)
- `format` (`text`, `markdown`, `json`, `binary`, etc.)
- `content` (payload)
- `valid_from`
- `valid_to`

Versioning and keys:

- Composite primary key: `(id, valid_to)`
- Canonical open-ended sentinel for new active rows: `valid_to = MAX_TIME`

Invariant:

- At most one active row per `id` at a given timestamp

### Fabric

`Fabric` represents relationships among `Cell` entities.

Storage model:

- `fabric_cell`: fabric node metadata
- `fabric_edge`: relationships between cells/fabrics

Supported structures:

- Set (unordered, no duplicates)
- List (ordered, duplicates allowed)
- Graph (typed edges via `relation_type`)
- Nested fabric (fabric referencing fabric)

Typical uses:

- Documents
- Product specifications
- Research structures
- Narrative or other hierarchical compositions

## Temporal Model

### Validity Window

Each version is valid in the interval:

- `valid_from <= T < valid_to`

Active version:

- `valid_from <= now < valid_to`

Current-version query rule:

- Query using time window semantics: `valid_from <= now AND valid_to > now`
- Do not require `valid_to = MAX_TIME` for reads
- `MAX_TIME` remains the canonical write sentinel for open-ended rows

Update algorithm (canonical):

1. Close current active row by setting `valid_to = now`
2. Insert new row with:
   - `valid_from = now`
   - `valid_to = MAX_TIME`

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

- Integer timestamp (`epoch ms` or `epoch us`)
- `MAX_TIME = i64::MAX`

Do not store temporal fields as datetime strings.

### Tables

Core tables:

- `data_cell`
- `meta_cell`
- `fabric_cell`
- `fabric_edge`

Notes:

- Cell tables are temporal
- `fabric_edge` may be temporal or non-temporal based on configuration

Optional table:

- `change_log` (event log for sync/audit)

## Engine Architecture

### Core Engine Responsibilities

Implemented in Rust.

- Enforce invariants
- Manage temporal updates
- Provide query APIs
- Define transaction boundaries
- Support optional sync module
- Perform lazy loading of fabric relationships

Responsibility split:

- Database = storage/invariant substrate
- Engine = domain logic and deterministic behavior
- Intelligent layer = unified `Cell` abstraction across usage types
- Storage layer = performance-oriented schemas, including specialized tables for major usage patterns

### API Exposure (Planned)

- HTTP API (polyglot clients)
- FFI bindings (for Java/Node and others)
- Optional gRPC

Engine remains the authoritative state layer.

### Unified Concept vs Specialized Storage

The model is logically unified and physically optimized:

- Logical/intelligent model: all data is treated as `Cell` under one conceptual API
- Physical/storage model: major usage categories can use dedicated tables to maximize performance and feature support
- Requirement: specialized tables must still follow temporal invariants and deterministic reconstruction rules

## Reconstruction Model

Current-state reconstruction (`now`):

1. Fetch active fabric
2. Fetch active edges
3. Fetch active cells
4. Assemble in memory

Query rule:

- Current state: filter by window (`valid_from <= now AND valid_to > now`)
- Historical state: filter by time window (`valid_from <= T < valid_to`)

Expected engine methods:

- `get_current(id)`
- `get_at_time(id, timestamp)`
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
