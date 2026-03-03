# Mnemonic Server

`mnemonic-server` is a simple RESTful HTTP API crate that sits next to `engine` and exposes document I/O for quick API navigation and evaluation.

## Design

- **Document DTO**: The server reuses `mnemonic_engine::Document` directly as the API data transfer object.
- **Context**: `AppContext` is the communication container that shares engine state across handlers.
- **Authority**: Temporal behavior and invariants remain in `mnemonic-engine`.

## Run

```bash
cargo run -p mnemonic-server
```

Environment variables:

- `MNEMONIC_DB_PATH` (default: `mnemonic.sqlite`)
- `MNEMONIC_SERVER_ADDR` (default: `0.0.0.0:3000`)

## Endpoints

- `GET /` - API route index for simple navigation
- `GET /health` - health check
- `POST /documents` - create a document
- `GET /documents/{id}` - get current document
- `PUT /documents/{id}` - update document content (new temporal version)
- `GET /documents/{id}/history?timestamp=<ts>` - retrieve document at timestamp

## Quick Evaluation

```bash
curl -s localhost:3000/
curl -s localhost:3000/health
curl -s -X POST localhost:3000/documents \
  -H 'content-type: application/json' \
  -d '{"cell_type":"Data","format":"Markdown","content":"# hello"}'
```
