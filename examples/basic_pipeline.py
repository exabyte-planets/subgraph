"""End-to-end example: generate data, build an index, run a closure query.

Runs entirely in the examples/ directory so it doesn't touch the test fixtures.

Usage:
    uv run python examples/basic_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from subgraph import Graph, build_index, copy_records, stream_nodes


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "graph.ndjson"
        db = Path(tmp) / "graph.db"
        out = Path(tmp) / "closure.ndjson"

        # --- 1. Generate a synthetic dataset -----------------------------------
        print("generating sample data …")
        subprocess.run(
            [
                sys.executable,
                "examples/generate_sample.py",
                "--persons",
                "500",
                "--cities",
                "50",
                "--files",
                "100",
                "--out",
                str(data),
            ],
            check=True,
        )

        # --- 2. Build the SQLite index (one-time, reusable) --------------------
        print("\nbuilding index …")
        build_index(data, db, progress=True)

        # --- 3. Compute transitive closure from all "person" nodes -------------
        print("\ncomputing closure …")
        with Graph(db) as g:
            total_nodes = len(g)
            closure_count = g.transitive_closure("person", progress=True)
            print(f"\nclosure: {closure_count} / {total_nodes} nodes reachable from 'person'")

            # --- 4a. Raw byte-copy path (fastest, output is NDJSON) ------------
            print("\ncopying closure records (raw bytes) …")
            with open(out, "wb") as fh:
                written = copy_records(data, g, fh, progress=True)
            print(f"wrote {written} records → {out}")

            # --- 4b. Structured path (parse into Node objects) -----------------
            print("\nstreaming closure as Node objects …")
            types: dict[str, int] = {}
            for node in stream_nodes(data, g, progress=True):
                types[node.type] = types.get(node.type, 0) + 1
            print("type breakdown:", types)


if __name__ == "__main__":
    main()
