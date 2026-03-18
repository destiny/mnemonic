# Mnemonic

Mnemonic is a local-first, temporal, structured data engine built around a small set of stable concepts:

- `Document` as the API and application root
- `Cell` as the atomic data unit
- `Fabric` as the structure that contains and orders cells
- `FabricCell` as the engine-layer representation of fabric membership

`AGENTS.md` is the canonical source for naming and model policy. This README explains the approach, why the model is shaped this way, and the main tradeoffs.

## Model Summary

### Document

`Document` is the API and application concept.

- A `Document` owns one root `Cell`
- The root `Cell` may reference one `Fabric`
- That `Fabric` contains the ordered `Cell` collection that forms the document structure

This keeps the public model document-first without replacing the engine primitives underneath it.

### Cell

`Cell` is the smallest logical data container.

- A `Cell` can stand on its own
- A `Cell` can optionally reference a `Fabric`
- A `Cell` remains atomic even when it is the root of a larger structure

### Fabric

`Fabric` is the engine-layer structure concept.

- A `Fabric` is built from cells
- A `Fabric` contains cells
- A `Fabric` preserves order among its member cells

The ordering behavior is part of the logical model even though the persistence strategy may vary.

### FabricCell

`FabricCell` is the engine-layer representation of membership inside a fabric.

- In engine logic, use `FabricCell`
- In storage, use `fabric_cells`
- Do not replace this with graph language such as `edge`

## Layer Responsibilities

### API / Application Layer

The API layer speaks in terms of `Document`.

- Clients work with documents and document-oriented behavior
- Specialized usage types such as text, table, dictionary, list, and map are document extensions
- The current server payload still exposes the root `Cell` in responses for compatibility, but that does not change the public concept

### Engine / Domain Layer

The engine owns domain behavior.

- create and update cell content
- manage fabric membership and ordering
- build document and fabric context in memory
- enforce naming and structural invariants

The engine should not own database timestamp mechanics directly.

### Storage / Database Layer

The storage layer owns persistence behavior.

- map logical records to tables such as `data_cell`, `meta_cell`, `fabric_cell`, and `fabric_cells`
- own temporal defaults and validity-window filtering
- close active rows and materialize replacement rows
- preserve append-only history and current-state lookup behavior

## Temporal and History Approach

Temporal history is implemented with `valid_from` and `valid_to`.

- A row is active when its validity window contains the current database time
- Historical lookup uses the same window rule with a supplied timestamp
- Full history reads skip the validity-window filter

The future `valid_to` sentinel exists to support a canonical active row and stable uniqueness rules.

- the active row uses an open-ended future `valid_to`
- `(id, valid_to)` or equivalent uniqueness rules make the live version identifiable
- updates close the current row and insert a new active row
- deletes close the current row without inserting a replacement

This is intentionally treated as storage behavior.

- DDL owns default values for `valid_from` and `valid_to`
- database current-time functions own current-state comparison
- engine logic depends on this behavior instead of constructing timestamp policy itself

## Ordering Approach

Logical order belongs to `Fabric`.

The storage layer may represent that order in different ways:

- ordinal positions
- linked-list style pointers
- tree or indexed ordering strategies

These are storage choices only. They must not change the logical meaning of `Fabric` as an ordered collection of `Cell`.

## Tradeoffs

### Benefits

- local-first operation with embedded database support
- append-only history with deterministic reconstruction
- stable core vocabulary across engine, API, and storage layers
- explicit structure rather than implicit document blobs

### Costs

- storage queries and DDL are more complex than flat current-state tables
- temporal history requires careful active-row uniqueness rules
- API compatibility may temporarily expose root-cell payloads even while the public concept is `Document`

### Why avoid extra nouns

Avoiding extra words such as `edge` keeps the model teachable.

- `Cell`, `Fabric`, `FabricCell`, and `Document` already cover the important concepts
- graph terms would introduce a second mental model without improving the actual logic
- storage names should stay storage-shaped, not redefine engine concepts

## Canonical Reference

For the normative version of the naming policy and model rules, use [`AGENTS.md`](./AGENTS.md).
