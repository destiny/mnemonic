# Mnemonic Server

`mnemonic-server` is a simple RESTful HTTP API crate that sits next to `engine` and exposes document I/O for quick API navigation and evaluation.

## Design

- **Public Concept**: The API uses `Document` as the public concept.
- **Current Wire Shape**: The current response payload still carries the root `Cell` as `root`. This is a wire-shape detail for compatibility, not a redefinition of the public model.
- **Ownership Model**: A document owns one root cell, and that root cell may reference fabric-backed structure.
- **Context**: `AppContext` is the communication container that shares engine state across handlers.
- **Authority**: Document behavior stays in the engine layer, while temporal validity mechanics remain storage-backed behavior exposed through engine APIs.

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
- `POST /documents` - create a document; response currently returns the root cell payload
- `GET /documents/{id}` - get the current document view through the root cell payload
- `PUT /documents/{id}` - update the document by replacing the root cell content with a new version
- `GET /documents/{id}/history?timestamp=<ts>` - retrieve the document view at timestamp (`RFC3339` or `YYYY-MM-DD HH:MM:SS[.ffffff]`)

## Quick Evaluation

```bash
curl -s localhost:3000/
curl -s localhost:3000/health
curl -s -X POST localhost:3000/documents \
  -H 'content-type: application/json' \
  -d '{"cell_type":"Data","format":"Markdown","content":"# hello"}'
```

Response shape:

Compatibility note:

- The public API concept is `Document`
- The current payload carrier is the root `Cell`
- This response shape preserves compatibility while the application model remains document-first

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
