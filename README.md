# subgraph

Transitive-closure extraction from large typed node graphs stored as JSON.

Given a seed node type (e.g. `"person"`), **subgraph** finds every node
reachable by following `related` links and writes the matching records to a
new JSON file — without ever holding more than a small working set in RAM.

## Design

The common workflow is a single command:

```
query   → auto-index if needed → BFS in SQLite → seek source offsets → write JSON
```

When repeated queries will hit the same source file, pre-building the index once
saves time:

```
index   → streams source → SQLite (uuid, type, offset, edges)
```

## Data format

Source files must be **JSON** with one JSON object per line:

```json
[
{"type": "person", "Id": "alice", "RelatedIds": ["bob", "sf-office"]}
{"type": "person", "Id": "bob",   "RelatedIds": ["nyc-office"]}
{"type": "city",   "Id": "sf-office",  "RelatedIds": []}
{"type": "city",   "Id": "nyc-office", "RelatedIds": []}
]
```

Every record needs `type` (string), `Id` (string), and `RelatedIds` (list of
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

### Query a closure

```bash
uv run subgraph query data.json person
```

Computes the transitive closure of all `person` nodes and writes their full
records — plus every node reachable from them — to `data_person.json` next to
the source file.  If `data.db` does not exist it is built automatically first.

Pass an explicit output path as a third argument to override the default:

```bash
uv run subgraph query data.json person output/subset.json
```

Optionally filter which seed nodes start the BFS by their `timestamp` field:

```bash
# Only seed from persons active in Q1 2024
uv run subgraph query data.json person \
    --after 2024-01-01T00:00:00Z \
    --before 2024-03-31T23:59:59Z
```

`--after` and `--before` are both optional ISO 8601 strings.  Nodes without a
`timestamp` field are excluded from seeding when either bound is present, but
remain reachable as non-seed nodes in the closure.

### Pre-build the index

When you plan to run many queries against the same source file, build the index
once up front rather than paying for it on the first `query`:

```bash
uv run subgraph index data.json
```

This writes `data.db` alongside `data.json`.  Re-running rebuilds from
scratch.

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
build_index("data.json", "data.db", progress=True)

# Compute closure and copy raw records (fastest path)
with Graph("data.db") as g:
    count = g.transitive_closure("person", progress=True)
    print(f"{count} nodes in closure")

    with open("output.json", "wb") as fh:
        copy_records("data.json", g, fh, progress=True)

# Or iterate as structured Node objects
with Graph("data.db") as g:
    g.transitive_closure("person")
    for node in stream_nodes("data.json", g):
        print(node.uuid, node.type, node.extra)
```

## Examples

See [`examples/`](examples/) for runnable scripts.

| Script | Description |
|---|---|
| [`generate_sample.py`](examples/generate_sample.py) | Generate a synthetic json graph of configurable size |
| [`basic_pipeline.py`](examples/basic_pipeline.py) | Full end-to-end walkthrough: generate → index → closure → copy/stream |

### Run the pipeline example

```bash
uv run python examples/basic_pipeline.py
```

### Generate a larger dataset for benchmarking

```bash
uv run python examples/generate_sample.py \
    --persons 100000 --cities 500 --files 1000 \
    --out big.json

# Pre-build the index once, then run as many queries as you like
uv run subgraph index big.json
uv run subgraph query big.json person
uv run subgraph query big.json city
```
