from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

_SCHEMA = """\
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA cache_size   = -65536;

CREATE TABLE IF NOT EXISTS nodes (
    uuid   TEXT PRIMARY KEY,
    type   TEXT NOT NULL,
    offset INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS edges (
    src TEXT NOT NULL,
    dst TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_edges_src ON edges (src);

CREATE TABLE IF NOT EXISTS closure (
    uuid TEXT PRIMARY KEY
) STRICT;
"""


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
        self._db.executescript(_SCHEMA)

    # ------------------------------------------------------------------ #
    # Sizing                                                               #
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]

    def closure_size(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM closure").fetchone()[0]

    # ------------------------------------------------------------------ #
    # Closure access                                                       #
    # ------------------------------------------------------------------ #

    def iter_closure_uuids(self) -> Iterator[str]:
        for (uuid,) in self._db.execute("SELECT uuid FROM closure"):
            yield uuid

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

    def transitive_closure(self, seed_type: str) -> int:
        """BFS from every node of *seed_type*; persist result to the closure table.

        Working state is kept in SQLite temp tables so memory use is O(1) in
        graph size.  Returns the number of reachable nodes.

        Each call replaces the previously stored closure.
        """
        db = self._db
        db.execute("DELETE FROM closure")
        db.execute("DROP TABLE IF EXISTS temp._frontier")
        db.execute("DROP TABLE IF EXISTS temp._new_frontier")
        db.execute("CREATE TEMP TABLE _frontier     (uuid TEXT PRIMARY KEY)")
        db.execute("CREATE TEMP TABLE _new_frontier (uuid TEXT PRIMARY KEY)")

        # Seed: all nodes of the requested type
        db.execute(
            "INSERT OR IGNORE INTO _frontier SELECT uuid FROM nodes WHERE type = ?",
            (seed_type,),
        )
        db.execute("INSERT OR IGNORE INTO closure SELECT uuid FROM _frontier")

        # BFS: expand one hop per iteration until the frontier empties
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
            if db.execute("SELECT COUNT(*) FROM _new_frontier").fetchone()[0] == 0:
                break
            db.execute("INSERT OR IGNORE INTO closure   SELECT uuid FROM _new_frontier")
            db.execute("DELETE FROM _frontier")
            db.execute("INSERT INTO _frontier SELECT uuid FROM _new_frontier")

        db.execute("DROP TABLE temp._frontier")
        db.execute("DROP TABLE temp._new_frontier")
        return self.closure_size()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> Graph:
        return self

    def __exit__(self, *_) -> None:
        self.close()
