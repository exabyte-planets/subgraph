from __future__ import annotations

import logging
import sqlite3
import uuid as uuidlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)

# Node ids are GUID4s.  Storing them as canonical 36-char TEXT would waste 20
# bytes per id across nodes, edges (×2) and the closure — significant at graph
# scale.  We store the 16-byte big-endian form instead and convert at the
# Python boundary, so the public API still speaks canonical GUID strings.
_SCHEMA = """\
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA cache_size   = -65536;

CREATE TABLE IF NOT EXISTS nodes (
    uuid   BLOB PRIMARY KEY,
    type   TEXT NOT NULL,
    offset INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS edges (
    src BLOB NOT NULL,
    dst BLOB NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_edges_src  ON edges (src);
CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes (type);

CREATE TABLE IF NOT EXISTS closure (
    uuid BLOB PRIMARY KEY
) STRICT;
"""


def guid_to_bytes(guid: str) -> bytes:
    """Convert a canonical GUID4 string to its 16-byte form for storage."""
    return uuidlib.UUID(guid).bytes


def bytes_to_guid(raw: bytes) -> str:
    """Convert a stored 16-byte id back to its canonical GUID4 string."""
    return str(uuidlib.UUID(bytes=raw))


@dataclass
class Node:
    """Full node record including arbitrary extra fields from the source file."""

    type: str
    uuid: str
    related: list[str] = field(default_factory=list)
    extra: dict = field(default_factory=dict)


class Graph:
    """SQLite-backed graph for memory-bounded transitive closure computation.

    The adjacency index, BFS working tables, and closure result all live on
    disk so peak RAM use stays independent of graph size.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db = sqlite3.connect(db_path)
        # Whether a property-based seed filter is currently active.  When True a
        # per-connection TEMP table ``seed_filter`` restricts which nodes may
        # seed the BFS (see :meth:`apply_seed_filter`).
        self._seed_filter_active = False
        self._db.executescript(_SCHEMA)

    # ------------------------------------------------------------------ #
    # Sizing                                                               #
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]

    def closure_size(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM closure").fetchone()[0]

    def count_type(self, type_name: str) -> int:
        """Return the number of nodes of *type_name* that match the active filters.

        Uses the same conditions as :meth:`transitive_closure` — the type match
        plus any property-based seed filter set via :meth:`apply_seed_filter` —
        so the result equals the number of BFS seeds for an equivalent call.
        """
        where, params = self._seed_conditions(type_name)
        return self._db.execute(f"SELECT COUNT(*) FROM nodes WHERE {where}", params).fetchone()[0]

    def _seed_conditions(self, type_name: str) -> tuple[str, list[str]]:
        """Build the shared WHERE clause that selects BFS seed nodes.

        Combines the type match with — when a property filter is active —
        membership in the ``seed_filter`` table.  ``count_type`` and
        ``transitive_closure`` both use this so their seed sets always agree.
        """
        conditions = ["type = ?"]
        params: list[str] = [type_name]
        if self._seed_filter_active:
            conditions.append("uuid IN (SELECT uuid FROM seed_filter)")
        return " AND ".join(conditions), params

    # ------------------------------------------------------------------ #
    # Property seed filter                                                 #
    # ------------------------------------------------------------------ #

    def iter_type_offsets(self, type_name: str) -> Iterator[tuple[str, int]]:
        """Yield ``(guid, offset)`` for every node of *type_name*, in offset order.

        Used to re-read seed-type records from the source file when evaluating a
        property filter.  Offset order keeps the downstream seeks near-sequential.
        ``guid`` is the canonical string form of the stored 16-byte id.
        """
        cur = self._db.execute(
            "SELECT uuid, offset FROM nodes WHERE type = ? ORDER BY offset", (type_name,)
        )
        for raw, offset in cur:
            yield bytes_to_guid(raw), offset

    def apply_seed_filter(self, uuids: Iterator[str]) -> int:
        """Restrict subsequent BFS seeds to *uuids*; return how many were stored.

        ``uuids`` are canonical GUID strings.  They are converted to their stored
        byte form and streamed into a per-connection TEMP table, so the on-disk
        index is never modified and the filter is discarded when this ``Graph``
        is closed.  Once applied, :meth:`transitive_closure` and
        :meth:`count_type` only consider these uuids (still subject to the type
        condition).  Calling this again replaces the previous filter.
        """
        db = self._db
        db.execute("DROP TABLE IF EXISTS temp.seed_filter")
        db.execute("CREATE TEMP TABLE seed_filter (uuid BLOB PRIMARY KEY)")
        db.executemany(
            "INSERT OR IGNORE INTO seed_filter VALUES (?)", ((guid_to_bytes(u),) for u in uuids)
        )
        self._seed_filter_active = True
        return db.execute("SELECT COUNT(*) FROM seed_filter").fetchone()[0]

    # ------------------------------------------------------------------ #
    # Closure access                                                       #
    # ------------------------------------------------------------------ #

    def iter_closure_uuids(self) -> Iterator[str]:
        for (raw,) in self._db.execute("SELECT uuid FROM closure"):
            yield bytes_to_guid(raw)

    def iter_closure_offsets(self) -> Iterator[int]:
        """Yield the source byte offset of every node in the closure, in
        ascending order.

        Sorting turns the downstream ``seek()`` calls into forward-only skips
        rather than scattered random reads, which keeps I/O close to
        sequential.  Dangling references (UUIDs in the closure with no node
        row) are naturally excluded by the join.  The cursor streams from
        SQLite, so RAM stays bounded regardless of closure size.
        """
        cur = self._db.execute(
            """
            SELECT n.offset
            FROM   closure c
            JOIN   nodes   n ON n.uuid = c.uuid
            ORDER  BY n.offset
            """
        )
        for (offset,) in cur:
            yield offset

    # ------------------------------------------------------------------ #
    # Core algorithm                                                       #
    # ------------------------------------------------------------------ #

    def transitive_closure(self, seed_type: str, *, progress: bool = False) -> int:
        """BFS from every node of *seed_type*; persist result to the closure table.

        A property seed filter set via :meth:`apply_seed_filter` restricts the
        seeds to a stored set of uuids, combined with the type match (AND).  All
        nodes reachable from the seeds are included in the closure regardless of
        their own properties.

        Working state is kept in SQLite temp tables so memory use is O(1) in
        graph size.  Returns the number of reachable nodes.

        Each call replaces the previously stored closure.
        """
        logger.info("starting BFS from seed type %r", seed_type)
        db = self._db
        db.execute("DELETE FROM closure")
        db.execute("DROP TABLE IF EXISTS temp._frontier")
        db.execute("DROP TABLE IF EXISTS temp._new_frontier")
        db.execute("CREATE TEMP TABLE _frontier     (uuid BLOB PRIMARY KEY)")
        db.execute("CREATE TEMP TABLE _new_frontier (uuid BLOB PRIMARY KEY)")

        # Seed: all nodes of the requested type that satisfy the active filters
        # (any property seed filter).
        where, params = self._seed_conditions(seed_type)
        db.execute(
            f"INSERT OR IGNORE INTO _frontier SELECT uuid FROM nodes WHERE {where}",
            params,
        )
        db.execute("INSERT OR IGNORE INTO closure SELECT uuid FROM _frontier")
        closure_total: int = db.execute("SELECT COUNT(*) FROM _frontier").fetchone()[0]
        logger.debug("seeded %d nodes of type %r", closure_total, seed_type)

        # BFS: expand one hop per iteration until the frontier empties
        with tqdm(desc="BFS", unit="hop", disable=not progress) as pbar:
            while True:
                db.execute("DELETE FROM _new_frontier")
                db.execute(
                    """
                    INSERT OR IGNORE INTO _new_frontier
                    SELECT DISTINCT e.dst
                    FROM   edges     e
                    JOIN   _frontier f ON e.src = f.uuid
                    WHERE  e.dst NOT IN (SELECT uuid FROM closure)
                    """
                )
                new_count: int = db.execute("SELECT COUNT(*) FROM _new_frontier").fetchone()[0]
                if new_count == 0:
                    break
                db.execute("INSERT OR IGNORE INTO closure   SELECT uuid FROM _new_frontier")
                db.execute("DELETE FROM _frontier")
                db.execute("INSERT INTO _frontier SELECT uuid FROM _new_frontier")
                closure_total += new_count
                pbar.update(1)
                pbar.set_postfix({"new": new_count, "total": closure_total})
                logger.debug("hop: +%d nodes, %d total", new_count, closure_total)

        db.execute("DROP TABLE temp._frontier")
        db.execute("DROP TABLE temp._new_frontier")
        size = self.closure_size()
        logger.info("BFS complete: %d nodes in closure", size)
        return size

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> Graph:
        return self

    def __exit__(self, *_) -> None:
        self.close()
