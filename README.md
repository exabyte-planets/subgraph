# subgraph

Transitive-closure extraction from large typed node graphs stored as NDJSON.

Given a seed node type (e.g. `"person"`), **subgraph** finds every node
reachable by following `related` links and writes the matching records to a
new NDJSON file — without ever holding more than a small working set in RAM.

## Design

| Constraint | Approach |
|---|---|
| Source files up to 70 GB | Stream NDJSON line-by-line; never `json.load` the whole file |
| 1 GiB RAM budget | All graph state (adjacency, BFS frontiers, closure) lives in SQLite on disk |
| Fast output | Store the byte offset of each line at index time; seek directly to closure records — no re-parse, no re-serialize, byte-identical output |

The workflow is two commands:

```
index   → streams source → SQLite (uuid, type, offset, edges)
query   → BFS in SQLite  → seek source offsets → write NDJSON
```

## Data format

Source files must be **NDJSON** — one JSON object per line:

```json
{"type": "person", "uuid": "alice", "related": ["bob", "sf-office"]}
{"type": "person", "uuid": "bob",   "related": ["nyc-office"]}
{"type": "city",   "uuid": "sf-office",  "related": []}
{"type": "city",   "uuid": "nyc-office", "related": []}
```

Every record needs `type` (string), `uuid` (string), and `related` (list of
UUID strings). All other fields are preserved verbatim in the output.

An optional `timestamp` field (ISO 8601 string) on any record enables
time-range filtering of BFS seeds via `--after` / `--before`.  Records
without `timestamp` are valid and simply excluded from seeding when a filter
is active.

## Installation

```bash
uv sync
```

## CLI

### Build an index

```bash
uv run subgraph index data.ndjson data.db
```

Re-running rebuilds the index from scratch.

### Query a closure

```bash
uv run subgraph query data.db data.ndjson person output.ndjson
```

Computes the transitive closure of all `person` nodes and writes their full
records — plus every node reachable from them — to `output.ndjson`.

Optionally filter which seed nodes start the BFS by their `timestamp` field:

```bash
# Only seed from persons active in Q1 2024
uv run subgraph query data.db data.ndjson person output.ndjson \
    --after 2024-01-01T00:00:00Z \
    --before 2024-03-31T23:59:59Z
```

`--after` and `--before` are both optional ISO 8601 strings.  Nodes without a
`timestamp` field are excluded from seeding when either bound is present, but
remain reachable as non-seed nodes in the closure.

> **Note:** timestamps are compared lexicographically (as text), not as
> instants. Range filtering is therefore only correct when every record's
> `timestamp` and the supplied bounds share one fixed format — same UTC
> representation (e.g. all `...Z`), same precision, no mixing of `Z` with
> `+00:00` offsets. The `generate_sample.py` helper emits a consistent
> `%Y-%m-%dT%H:%M:%SZ` format suitable for this.

## Python API

```python
from subgraph import Graph, build_index, copy_records, stream_nodes

# One-time index build
build_index("data.ndjson", "data.db", progress=True)

# Compute closure and copy raw records (fastest path)
with Graph("data.db") as g:
    count = g.transitive_closure("person", progress=True)
    print(f"{count} nodes in closure")

    with open("output.ndjson", "wb") as fh:
        copy_records("data.ndjson", g, fh, progress=True)

# Or iterate as structured Node objects
with Graph("data.db") as g:
    g.transitive_closure("person")
    for node in stream_nodes("data.ndjson", g):
        print(node.uuid, node.type, node.extra)
```

### Key types

| Symbol | Description |
|---|---|
| `build_index(src, db, *, progress)` | Stream NDJSON → SQLite adjacency index |
| `Graph(db)` | Open an index; context-manager, call `.close()` when done |
| `Graph.transitive_closure(seed_type, *, after, before, progress) → int` | BFS from nodes of `seed_type` (optionally filtered by `timestamp`); persists closure to db; returns count |
| `Graph.closure_size() → int` | Number of nodes in the most recent closure |
| `Graph.iter_closure_uuids()` | Iterate UUIDs in the closure |
| `copy_records(src, graph, out_fh, *, progress) → int` | Copy raw source bytes for closure nodes to an open binary file handle |
| `stream_nodes(src, graph, *, progress)` | Yield parsed `Node` objects for closure nodes |
| `Node` | Dataclass: `type`, `uuid`, `related: list[str]`, `extra: dict` |

## Logging

The library logs to the `subgraph` hierarchy at `INFO` (lifecycle events) and
`DEBUG` (per-batch / per-hop detail). The CLI configures a stdout handler at
`INFO`. To enable debug output:

```python
import logging
logging.getLogger("subgraph").setLevel(logging.DEBUG)
```

## Examples

See [`examples/`](examples/) for runnable scripts.

| Script | Description |
|---|---|
| [`generate_sample.py`](examples/generate_sample.py) | Generate a synthetic NDJSON graph of configurable size |
| [`basic_pipeline.py`](examples/basic_pipeline.py) | Full end-to-end walkthrough: generate → index → closure → copy/stream |

### Run the pipeline example

```bash
uv run python examples/basic_pipeline.py
```

### Generate a larger dataset for benchmarking

```bash
uv run python examples/generate_sample.py \
    --persons 100000 --cities 500 --files 1000 \
    --out big.ndjson

uv run subgraph index big.ndjson big.db
uv run subgraph query big.db big.ndjson person output.ndjson
```
