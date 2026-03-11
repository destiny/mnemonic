# Mnemonic Server

`mnemonic-server` is a simple RESTful HTTP API crate that sits next to `engine` and exposes document I/O for quick API navigation and evaluation.

## Design

- **Document Root**: The API uses `Document` as the public concept and returns the document root cell as `root`.
- **Ownership Model**: A document owns one root cell, and that root cell may reference fabric-backed structure.
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
- `POST /documents` - create a document root
- `GET /documents/{id}` - get current document root
- `PUT /documents/{id}` - update document root content (new temporal version)
- `GET /documents/{id}/history?timestamp=<ts>` - retrieve document root at timestamp (`RFC3339` or `YYYY-MM-DD HH:MM:SS[.ffffff]`)

## Quick Evaluation

```bash
curl -s localhost:3000/
curl -s localhost:3000/health
curl -s -X POST localhost:3000/documents \
  -H 'content-type: application/json' \
  -d '{"cell_type":"Data","format":"Markdown","content":"# hello"}'
```

Response shape:

```json
{
  "root_cell_id": "uuid",
  "root": {
    "id": "uuid",
    "cell_type": "Data",
    "format": "Markdown",
    "content": [35, 32, 104, 101, 108, 108, 111],
    "valid_from": "2026-03-11T02:30:45.123456Z",
    "valid_to": "2100-01-01T00:00:00Z",
    "fabric_id": null
  }
}
```
