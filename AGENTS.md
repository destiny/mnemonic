# AGENTS.md

## Project record
- **Repository**: `mnemonic`
- **Primary crate**: `engine` (Rust library crate: `mnemonic-engine`)
- **Core purpose**: provide a lightweight data engine built around `Cell` objects with typed content, temporal validity windows, version updates, and relationship handling.

## Current architecture

### Modules
- `engine/src/models.rs`
  - Domain models, including:
    - `Cell` (base storage unit)
    - `CellType` and `ContentFormat` (data typing/format metadata)
    - `RelationType` and `FabricEdge` (relationship semantics)
- `engine/src/engine.rs`
  - High-level API (`Engine`) for:
    - creating cells
    - updating cell content with version history semantics
    - adding legacy child links
    - adding/querying relation edges
- `engine/src/storage.rs`
  - SQLite persistence (`SqliteStorage`) and schema initialization.
  - Maintains:
    - `cells` table for cell versions
    - `cell_edges` table for relation/fabric edges
- `engine/src/error.rs`
  - Shared error type (`EngineError`) and `Result<T>` alias.
- `engine/src/lib.rs`
  - Public module and type re-exports.

### Tests
- `engine/tests/basic.rs`
  - End-to-end lifecycle test for creating cells, linking, relation lookup, and content version update behavior.

## Build and validation
Run from `engine/`:
- `cargo fmt`
- `cargo test`

## Development notes
- Keep `Cell` focused on payload/type/format and temporal validity concerns.
- Prefer relation semantics in `RelationType`/`FabricEdge` for graph-like links.
- Preserve backward compatibility when changing child/link behavior.
- Avoid unchecked deserialization (`unwrap`) in storage read paths.

## License
- Apache-2.0 (see `engine/LICENSE`).
