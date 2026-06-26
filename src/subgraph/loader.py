from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

import orjson
from tqdm import tqdm

from .graph import Graph, Node

logger = logging.getLogger(__name__)

_BUILD_BATCH = 50_000


def _parse_raw_line(raw_line: bytes) -> dict:
    """Parse one raw source line into a flat record with ``type`` as a top-level key.

    The source format is a JSON array whose elements look like::

        {"typename": {"Id": "...", "RelatedIds": [{"Value": "..."}, ...], ...}}

    This function strips any trailing comma (JSON array separator), parses the
    wrapper object, and returns ``{"type": typename, **fields}``.
    """
    json_bytes = raw_line.strip().rstrip(b",")
    wrapper: dict = orjson.loads(json_bytes)
    type_ = next(iter(wrapper))
    return {"type": type_, **wrapper[type_]}


def _iter_raw_with_offset(path: Path) -> Iterator[tuple[int, dict]]:
    """Yield ``(byte_offset, record)`` for each data line of a JSON-array source file.

    The source is a JSON array of ``{"typename": {fields}}`` objects, one per
    line, surrounded by ``[`` / ``]`` bracket lines.  Bracket lines and blank
    lines are skipped; ``byte_offset`` is the position of the line's first byte
    from the start of the file.
    """
    with open(path, "rb") as fh:
        offset = 0
        for raw_line in fh:
            stripped = raw_line.strip()
            if stripped and stripped not in (b"[", b"]"):
                yield offset, _parse_raw_line(raw_line)
            offset += len(raw_line)


def build_index(src_path: str | Path, db_path: str | Path, *, progress: bool = False) -> None:
    """Stream *src_path* and write a SQLite adjacency index to *db_path*.

    For each record we store ``uuid``, ``type``, and the line's byte ``offset``
    in the source file, plus its outgoing edges.  Full records are never copied
    into the index — they are recovered later by seeking to the stored offset.
    Calling this again on the same *db_path* rebuilds the index from scratch.
    """
    src_path = Path(src_path)
    logger.info("building index from %s", src_path)
    db = sqlite3.connect(db_path)
    # journal/synchronous are disabled because the index is fully derived from
    # the source and rebuilt from scratch on every run, so a crash mid-build
    # just means re-running it.  temp_store is left on disk on purpose: the
    # post-load CREATE INDEX sorts externally, and forcing that into memory
    # would blow a tight RAM budget on a large graph.
    #
    # nodes is created WITHOUT its uuid / type indexes.  They are built in a
    # single pass after the bulk load (see below), which turns what would be
    # hundreds of millions of random B-tree insertions during the load into
    # one external-merge sort — far kinder to a slow SSD and a small page cache.
    db.executescript(
        """
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous  = OFF;
        PRAGMA cache_size   = -131072;

        DROP TABLE IF EXISTS edges;
        DROP TABLE IF EXISTS nodes;
        DROP TABLE IF EXISTS closure;

        CREATE TABLE nodes (
            uuid TEXT NOT NULL, type TEXT NOT NULL,
            offset INTEGER NOT NULL, timestamp TEXT
        ) STRICT;
        CREATE TABLE edges (src TEXT NOT NULL, dst TEXT NOT NULL) STRICT;
        CREATE TABLE closure (uuid TEXT PRIMARY KEY) STRICT;
        """
    )

    node_buf: list[tuple[str, str, int, str | None]] = []
    edge_buf: list[tuple[str, str]] = []
    batches_flushed = 0

    def _flush() -> None:
        nonlocal batches_flushed
        with db:
            db.executemany("INSERT INTO nodes VALUES (?, ?, ?, ?)", node_buf)
            db.executemany("INSERT INTO edges VALUES (?, ?)", edge_buf)
        batches_flushed += 1
        logger.debug("flushed batch %d (%d nodes)", batches_flushed, len(node_buf))
        node_buf.clear()
        edge_buf.clear()

    with tqdm(desc="indexing", unit="rec", disable=not progress) as pbar:
        for offset, rec in _iter_raw_with_offset(src_path):
            try:
                uuid, type_ = rec["Id"], rec["type"]
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"record at byte offset {offset} is missing required field {exc}"
                ) from exc
            node_buf.append((uuid, type_, offset, rec.get("timestamp")))
            for item in rec.get("RelatedIds") or []:
                edge_buf.append((uuid, item["Value"]))
            if len(node_buf) >= _BUILD_BATCH:
                _flush()
            pbar.update(1)

    _flush()

    # Build every index once, now that the bulk load is done.
    with db:
        db.execute("CREATE INDEX idx_edges_src ON edges (src)")
    try:
        with db:
            db.execute("CREATE UNIQUE INDEX idx_nodes_uuid ON nodes (uuid)")
    except sqlite3.IntegrityError:
        # Duplicate uuids in the source.  Keep the last occurrence — matching
        # the previous INSERT OR REPLACE behaviour — then build the index.
        logger.warning("duplicate uuids found; keeping the last occurrence of each")
        with db:
            db.execute(
                "DELETE FROM nodes WHERE rowid NOT IN "
                "(SELECT MAX(rowid) FROM nodes GROUP BY uuid)"
            )
            db.execute("CREATE UNIQUE INDEX idx_nodes_uuid ON nodes (uuid)")
    with db:
        db.execute("CREATE INDEX idx_nodes_type_ts ON nodes (type, timestamp)")

    node_count: int = db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    edge_count: int = db.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    logger.info("index complete: %d nodes, %d edges", node_count, edge_count)
    db.close()


def _read_line_at(fh: BinaryIO, offset: int) -> bytes:
    """Seek to *offset* and return the raw line bytes there (including any
    trailing newline)."""
    fh.seek(offset)
    return fh.readline()


def copy_records(
    src_path: str | Path, graph: Graph, out_fh: BinaryIO, *, progress: bool = False
) -> int:
    """Copy the raw source bytes of every closure node to *out_fh* as a JSON array.

    Records are located by their stored byte offset and copied verbatim — no
    parse, no re-serialisation.  Offsets are visited in ascending order for
    near-sequential I/O.  Output is a well-formed JSON array readable by
    ``json.load``.  Returns the number of records written.
    """
    total = graph.closure_size()
    logger.info("copying %d records", total)
    written = 0
    out_fh.write(b"[\n")
    with open(src_path, "rb") as fh:
        for offset in tqdm(
            graph.iter_closure_offsets(),
            total=total,
            desc="copying",
            unit="rec",
            disable=not progress,
        ):
            raw = _read_line_at(fh, offset).strip().rstrip(b",")
            if written > 0:
                out_fh.write(b",\n")
            out_fh.write(raw)
            written += 1
    out_fh.write(b"\n]\n")
    logger.info("wrote %d records", written)
    return written


def stream_nodes(src_path: str | Path, graph: Graph, *, progress: bool = False) -> Iterator[Node]:
    """Yield :class:`Node` objects for every record in the stored closure.

    Drives off the closure's stored byte offsets, seeking directly to each
    line so only closure records are read and parsed — the bulk of the source
    file is skipped entirely.
    """
    total = graph.closure_size()
    logger.info("streaming %d nodes from %s", total, src_path)
    with open(src_path, "rb") as fh:
        for offset in tqdm(
            graph.iter_closure_offsets(),
            total=total,
            desc="streaming",
            unit="rec",
            disable=not progress,
        ):
            rec = _parse_raw_line(_read_line_at(fh, offset))
            related_ids = rec.pop("RelatedIds", None) or []
            yield Node(
                type=rec.pop("type"),
                uuid=rec.pop("Id"),
                related=[item["Value"] for item in related_ids],
                extra=rec,
            )
