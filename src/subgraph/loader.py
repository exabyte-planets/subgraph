from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from .graph import Graph, Node

_BUILD_BATCH = 50_000


def _iter_raw_with_offset(path: Path) -> Iterator[tuple[int, dict]]:
    """Yield ``(byte_offset, record)`` for each non-blank line of an NDJSON file.

    ``byte_offset`` is the position of the line's first byte from the start of
    the file, computed from cumulative line lengths (not ``tell()``), so it is
    accurate regardless of read buffering.
    """
    with open(path, "rb") as fh:
        offset = 0
        for raw_line in fh:
            line = raw_line.strip()
            if line:
                yield offset, json.loads(line)
            offset += len(raw_line)


def build_index(src_path: str | Path, db_path: str | Path) -> None:
    """Stream *src_path* and write a SQLite adjacency index to *db_path*.

    For each record we store ``uuid``, ``type``, and the line's byte ``offset``
    in the source file, plus its outgoing edges.  Full records are never copied
    into the index — they are recovered later by seeking to the stored offset.
    Calling this again on the same *db_path* rebuilds the index from scratch.
    """
    db = sqlite3.connect(db_path)
    # Rebuild tables from scratch so re-indexing is clean
    db.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous  = NORMAL;
        PRAGMA cache_size   = -131072;

        DROP TABLE IF EXISTS edges;
        DROP TABLE IF EXISTS nodes;
        DROP TABLE IF EXISTS closure;

        CREATE TABLE nodes (
            uuid TEXT PRIMARY KEY, type TEXT NOT NULL, offset INTEGER NOT NULL
        ) STRICT;
        CREATE TABLE edges (src TEXT NOT NULL, dst TEXT NOT NULL) STRICT;
        CREATE TABLE closure (uuid TEXT PRIMARY KEY) STRICT;
        """
    )

    node_buf: list[tuple[str, str, int]] = []
    edge_buf: list[tuple[str, str]] = []

    def _flush() -> None:
        with db:
            db.executemany("INSERT OR REPLACE INTO nodes VALUES (?, ?, ?)", node_buf)
            db.executemany("INSERT INTO edges VALUES (?, ?)", edge_buf)
        node_buf.clear()
        edge_buf.clear()

    for offset, rec in _iter_raw_with_offset(Path(src_path)):
        node_buf.append((rec["uuid"], rec["type"], offset))
        for dst in rec.get("related") or []:
            edge_buf.append((rec["uuid"], dst))
        if len(node_buf) >= _BUILD_BATCH:
            _flush()

    _flush()

    # Build edge index after bulk load — far faster than maintaining it inline
    with db:
        db.execute("CREATE INDEX idx_edges_src ON edges (src)")

    db.close()


def _read_line_at(fh: BinaryIO, offset: int) -> bytes:
    """Seek to *offset* and return the raw line bytes there (including any
    trailing newline)."""
    fh.seek(offset)
    return fh.readline()


def copy_records(src_path: str | Path, graph: Graph, out_fh: BinaryIO) -> int:
    """Copy the raw source bytes of every closure node to *out_fh* as NDJSON.

    Records are located by their stored byte offset and copied verbatim — no
    parse, no re-serialisation — so output is byte-identical to the source
    lines.  Offsets are visited in ascending order for near-sequential I/O.
    Returns the number of records written.
    """
    written = 0
    with open(src_path, "rb") as fh:
        for offset in graph.iter_closure_offsets():
            raw = _read_line_at(fh, offset)
            if not raw.endswith(b"\n"):
                raw += b"\n"
            out_fh.write(raw)
            written += 1
    return written


def stream_nodes(src_path: str | Path, graph: Graph) -> Iterator[Node]:
    """Yield :class:`Node` objects for every record in the stored closure.

    Drives off the closure's stored byte offsets, seeking directly to each
    line so only closure records are read and parsed — the bulk of the source
    file is skipped entirely.
    """
    with open(src_path, "rb") as fh:
        for offset in graph.iter_closure_offsets():
            rec = json.loads(_read_line_at(fh, offset))
            yield Node(
                type=rec.pop("type"),
                uuid=rec.pop("uuid"),
                related=list(rec.pop("related", None) or []),
                extra=rec,
            )
