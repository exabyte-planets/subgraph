from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, cast

import orjson
from tqdm import tqdm

from .graph import Graph, Node, guid_to_bytes
from .sources import Source, SourceSpec, open_source

logger = logging.getLogger(__name__)

_BUILD_BATCH = 50_000


def _parse_raw_line(raw_line: bytes) -> dict:
    """Parse one raw source line into a flat record with ``type`` as a top-level key.

    The source format is a JSON array whose elements look like::

        {"typename": {"Id": "...", "RelatedIds": [{"Value": "..."}, ...], ...}}

    This function strips any trailing comma (JSON array separator), parses the
    wrapper object, and returns ``{"type": typename, **fields}``.

    Raises :class:`ValueError` (without a byte offset — the caller adds that) on a
    line that is not valid JSON, not a non-empty object, or whose type value is
    not itself an object.
    """
    json_bytes = raw_line.strip().rstrip(b",")
    try:
        wrapper = orjson.loads(json_bytes)
    except orjson.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc

    if not isinstance(wrapper, dict) or not wrapper:
        raise ValueError("record must be a non-empty object keyed by type name")

    type_ = next(iter(wrapper))
    fields = wrapper[type_]

    if not isinstance(fields, dict):
        raise ValueError(f"value for type {type_!r} must be an object, got {type(fields).__name__}")
    return {"type": type_, **fields}


def _iter_raw_with_offset(src: Source) -> Iterator[tuple[int, dict]]:
    """Yield ``(byte_offset, record)`` for each data line of a JSON-array source.

    The source is a JSON array of ``{"typename": {fields}}`` objects, one per
    line, surrounded by ``[`` / ``]`` bracket lines.  Bracket lines and blank
    lines are skipped; ``byte_offset`` is the position of the line's first byte
    from the start of the (decompressed) stream — the same address space every
    later seek uses, so the offsets stay valid for compressed sources too.
    """
    with open_source(src) as fh:
        offset = 0
        for raw_line in fh:
            stripped = raw_line.strip()
            if stripped and stripped not in (b"[", b"]"):
                try:
                    rec = _parse_raw_line(raw_line)
                except ValueError as exc:
                    raise ValueError(f"record at byte offset {offset}: {exc}") from exc
                yield offset, rec
            offset += len(raw_line)


def _guid_bytes(value: object, field: str) -> bytes:
    """Convert *value* to its 16-byte id form, naming *field* on failure."""
    try:
        # value may be any JSON type here; a non-str raises TypeError below.
        return guid_to_bytes(cast(str, value))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field} {value!r}: {exc}") from exc


def _validate_record(rec: dict) -> tuple[bytes, str, list[bytes]]:
    """Extract ``(uuid_bytes, type, [dst_bytes, ...])`` from a parsed record.

    Raises :class:`ValueError` describing the problem (without the byte offset,
    which the caller adds) when a required field is missing, an id is not a valid
    32-char hex string, or ``RelatedIds`` is not a list of ``{"Value": ...}``
    objects.
    """
    try:
        uuid, type_ = rec["Id"], rec["type"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"missing required field {exc}") from exc

    uuid_bytes = _guid_bytes(uuid, "Id")

    related = rec.get("RelatedIds")

    if related is None:
        related = []
    elif not isinstance(related, list):
        raise ValueError(f"RelatedIds must be a list, got {type(related).__name__}")

    dsts: list[bytes] = []
    for i, item in enumerate(related):
        if not isinstance(item, dict) or "Value" not in item:
            raise ValueError(
                f"RelatedIds[{i}] must be an object with a 'Value' field, got {item!r}"
            )
        dsts.append(_guid_bytes(item["Value"], f"RelatedIds[{i}].Value"))

    return uuid_bytes, type_, dsts


def build_index(src_path: Source, db_path: str | Path, *, progress: bool = False) -> None:
    """Stream *src_path* and write a SQLite adjacency index to *db_path*.

    For each record we store ``uuid``, ``type``, and the line's byte ``offset``
    in the source file, plus its outgoing edges.  Full records are never copied
    into the index — they are recovered later by seeking to the stored offset.
    Calling this again on the same *db_path* rebuilds the index from scratch.
    """
    spec = SourceSpec.parse(src_path)
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
            uuid BLOB NOT NULL, type TEXT NOT NULL,
            offset INTEGER NOT NULL
        ) STRICT;
        CREATE TABLE edges (src BLOB NOT NULL, dst BLOB NOT NULL) STRICT;
        CREATE TABLE closure (uuid BLOB PRIMARY KEY) STRICT;
        """
    )

    node_buf: list[tuple[bytes, str, int]] = []
    edge_buf: list[tuple[bytes, bytes]] = []
    batches_flushed = 0

    def _flush() -> None:
        nonlocal batches_flushed
        with db:
            db.executemany("INSERT INTO nodes VALUES (?, ?, ?)", node_buf)
            db.executemany("INSERT INTO edges VALUES (?, ?)", edge_buf)
        batches_flushed += 1
        logger.debug("flushed batch %d (%d nodes)", batches_flushed, len(node_buf))
        node_buf.clear()
        edge_buf.clear()

    with tqdm(desc="indexing", unit="rec", disable=not progress) as pbar:
        for offset, rec in _iter_raw_with_offset(spec):
            try:
                uuid_bytes, type_, dsts = _validate_record(rec)
            except ValueError as exc:
                raise ValueError(f"record at byte offset {offset}: {exc}") from exc

            node_buf.append((uuid_bytes, type_, offset))
            edge_buf.extend((uuid_bytes, dst) for dst in dsts)

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
                "DELETE FROM nodes WHERE rowid NOT IN (SELECT MAX(rowid) FROM nodes GROUP BY uuid)"
            )
            db.execute("CREATE UNIQUE INDEX idx_nodes_uuid ON nodes (uuid)")
    with db:
        db.execute("CREATE INDEX idx_nodes_type ON nodes (type)")

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
    src_path: Source, graph: Graph, out_fh: BinaryIO, *, progress: bool = False
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
    with open_source(src_path) as fh:
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


def _record_matches(rec: dict, where: list[tuple[str, str]]) -> bool:
    """Return True iff *rec* exactly matches every ``(property, value)`` pair.

    Values are compared as strings (the CLI supplies string values), so a
    numeric field ``123`` matches ``"123"``.  A record missing the property
    never matches.
    """
    return all(key in rec and str(rec[key]) == value for key, value in where)


def iter_property_seed_uuids(
    src_path: Source,
    graph: Graph,
    seed_type: str,
    where: list[tuple[str, str]],
    *,
    progress: bool = False,
) -> Iterator[str]:
    """Yield the uuid of every *seed_type* node whose fields match *where*.

    Streams the seed-type nodes in source-offset order, seeking to each one and
    parsing only that line, so the bulk of a large source file is skipped and
    memory stays bounded.  ``where`` is a list of ``(property, value)`` pairs
    combined with AND (exact string equality).  The matching uuids are intended
    to be fed into :meth:`Graph.apply_seed_filter`.
    """
    logger.info("scanning %r nodes for property filter %s", seed_type, where)
    with open_source(src_path) as fh:
        for uuid, offset in tqdm(
            graph.iter_type_offsets(seed_type),
            desc="filtering",
            unit="rec",
            disable=not progress,
        ):
            rec = _parse_raw_line(_read_line_at(fh, offset))
            if _record_matches(rec, where):
                yield uuid


def estimate_output_bytes(src_path: Source, graph: Graph, *, progress: bool = False) -> int:
    """Return the exact number of bytes that :func:`copy_records` would write.

    Reads each closure record from *src_path* (same I/O as a real copy) but
    discards the bytes instead of writing them.  Use this to check against a
    size budget before committing to a full write.
    """
    total = graph.closure_size()
    if total == 0:
        return 5  # "[\n" + "\n]\n"

    raw_total = 0
    with open_source(src_path) as fh:
        for offset in tqdm(
            graph.iter_closure_offsets(),
            total=total,
            desc="estimating",
            unit="rec",
            disable=not progress,
        ):
            raw = _read_line_at(fh, offset).strip().rstrip(b",")
            raw_total += len(raw)

    # "[\n" (2) + raw bytes + (n-1) * ",\n" separators (2 each) + "\n]\n" (3)
    return raw_total + 2 * (total - 1) + 5


def stream_nodes(src_path: Source, graph: Graph, *, progress: bool = False) -> Iterator[Node]:
    """Yield :class:`Node` objects for every record in the stored closure.

    Drives off the closure's stored byte offsets, seeking directly to each
    line so only closure records are read and parsed — the bulk of the source
    file is skipped entirely.
    """
    total = graph.closure_size()
    logger.info("streaming %d nodes from %s", total, src_path)
    with open_source(src_path) as fh:
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
