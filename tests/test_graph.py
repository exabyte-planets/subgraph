import json
import sqlite3
import subprocess
import sys

import pytest

from subgraph import (
    Graph,
    Node,
    build_index,
    copy_records,
    estimate_output_bytes,
    iter_property_seed_uuids,
    stream_nodes,
)
from subgraph.graph import bytes_to_guid, guid_to_bytes

SAMPLE = "tests/data/sample.json"

# Node ids are 32-char hex strings.  These constants mirror the ids in
# tests/data/sample.json so the assertions below stay readable.
A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
C = "cccccccccccccccccccccccccccccccc"
D = "dddddddddddddddddddddddddddddddd"
E = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

#
#  Sample graph:
#
#  A (person, Name=Alice) -> B, C, E
#  B (person, Name=Bob)   -> D
#  C (city,   Place=City C) -> (none)
#  D (city,   Place=City D) -> (none)
#  E (file,   FileName=Sample File) -> (none)  extra: path="tests/data/sample.data"


# ------------------------------------------------------------------ #
# Fixtures                                                             #
# ------------------------------------------------------------------ #


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    build_index(SAMPLE, db_path)
    g = Graph(db_path)
    yield g
    g.close()


# ------------------------------------------------------------------ #
# Index build                                                          #
# ------------------------------------------------------------------ #


def test_index_node_count(db):
    assert len(db) == 5


def test_index_rebuild_is_clean(tmp_path):
    db_path = tmp_path / "rebuild.db"
    build_index(SAMPLE, db_path)
    build_index(SAMPLE, db_path)  # second call must not double-count
    with Graph(db_path) as g:
        assert len(g) == 5


# ------------------------------------------------------------------ #
# Edge index                                                          #
# ------------------------------------------------------------------ #
#
#  Edges from the sample: A->B, A->C, A->E, B->D.  Cities and the file node
#  have no outgoing edges.  These tests guard against a regression where the
#  edges table came up empty after an index build.


def test_edges_created_in_db(tmp_path):
    db_path = tmp_path / "edges.db"
    build_index(SAMPLE, db_path)
    con = sqlite3.connect(db_path)
    try:
        count = con.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        edges = {
            (bytes_to_guid(src), bytes_to_guid(dst))
            for src, dst in con.execute("SELECT src, dst FROM edges")
        }
    finally:
        con.close()
    assert count == 4
    assert edges == {(A, B), (A, C), (A, E), (B, D)}


def test_edges_stored_as_16_byte_blobs(tmp_path):
    # GUID4 ids are stored as their compact 16-byte form, not as text.
    db_path = tmp_path / "blob.db"
    build_index(SAMPLE, db_path)
    con = sqlite3.connect(db_path)
    try:
        src, dst = con.execute("SELECT src, dst FROM edges LIMIT 1").fetchone()
        (uuid,) = con.execute("SELECT uuid FROM nodes LIMIT 1").fetchone()
    finally:
        con.close()
    for value in (src, dst, uuid):
        assert isinstance(value, bytes)
        assert len(value) == 16


def test_node_with_edges_produces_edge_rows(tmp_path):
    # A single person with two RelatedIds must yield exactly two edge rows.
    src = tmp_path / "two.ndjson"
    src.write_text(
        "[\n"
        + json.dumps({"person": {"Id": A, "RelatedIds": [{"Value": B}, {"Value": C}]}})
        + "\n]\n"
    )
    db_path = tmp_path / "two.db"
    build_index(src, db_path)
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute("SELECT dst FROM edges WHERE src = ?", (guid_to_bytes(A),))
        dsts = {bytes_to_guid(d) for (d,) in rows}
    finally:
        con.close()
    assert dsts == {B, C}


# ------------------------------------------------------------------ #
# Transitive closure — seed type "person"                             #
# ------------------------------------------------------------------ #
#
#  Seeds: {A, B}
#  BFS adds C, E (from A) and D (from B)
#  Closure: {A, B, C, D, E} — entire graph


def test_person_closure_count(db):
    assert db.transitive_closure("person") == 5


def test_person_closure_uuids(db):
    db.transitive_closure("person")
    assert set(db.iter_closure_uuids()) == {A, B, C, D, E}


# ------------------------------------------------------------------ #
# Transitive closure — seed type "city"                               #
# ------------------------------------------------------------------ #
#
#  C and D have no outgoing edges → closure == {C, D}


def test_city_closure_count(db):
    assert db.transitive_closure("city") == 2


def test_city_closure_uuids(db):
    db.transitive_closure("city")
    assert set(db.iter_closure_uuids()) == {C, D}


# ------------------------------------------------------------------ #
# Edge cases                                                           #
# ------------------------------------------------------------------ #


def test_unknown_type_empty(db):
    assert db.transitive_closure("widget") == 0


def test_closure_replaced_on_second_call(db):
    db.transitive_closure("person")
    db.transitive_closure("city")
    assert set(db.iter_closure_uuids()) == {C, D}


def test_dangling_related_in_closure(tmp_path):
    # A related id that has no node row of its own is still reachable.
    node = "11111111111111111111111111111111"
    missing = "22222222222222222222222222222222"
    data_file = tmp_path / "dangle.ndjson"
    data_file.write_text(
        "[\n" + json.dumps({"person": {"Id": node, "RelatedIds": [{"Value": missing}]}}) + "\n]\n"
    )
    db_path = tmp_path / "dangle.db"
    build_index(data_file, db_path)
    with Graph(db_path) as g:
        g.transitive_closure("person")
        closure = set(g.iter_closure_uuids())
    assert node in closure
    assert missing in closure  # reachable even though absent from nodes table


# ------------------------------------------------------------------ #
# stream_nodes                                                         #
# ------------------------------------------------------------------ #


def test_stream_nodes_count(db):
    db.transitive_closure("person")
    assert len(list(stream_nodes(SAMPLE, db))) == 5


def test_stream_nodes_extra_fields(db):
    db.transitive_closure("person")
    nodes = list(stream_nodes(SAMPLE, db))
    e = next(n for n in nodes if n.uuid == E)
    assert e.extra.get("path") == "tests/data/sample.data"


def test_stream_nodes_city_only(db):
    db.transitive_closure("city")
    nodes = list(stream_nodes(SAMPLE, db))
    assert {n.uuid for n in nodes} == {C, D}


def test_stream_nodes_returns_node_objects(db):
    db.transitive_closure("city")
    nodes = list(stream_nodes(SAMPLE, db))
    assert all(isinstance(n, Node) for n in nodes)


# ------------------------------------------------------------------ #
# copy_records — raw byte-copy output                                  #
# ------------------------------------------------------------------ #


def test_copy_records_count(db, tmp_path):
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        assert copy_records(SAMPLE, db, fh) == 5


def test_copy_records_output_matches_source_records(db, tmp_path):
    # Full-graph closure: output must be a valid JSON array containing the same
    # records as the source, in offset order.
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    with open(SAMPLE) as src:
        source_records = json.load(src)
    assert json.loads(out.read_text()) == source_records


def test_copy_records_subset_lines(db, tmp_path):
    db.transitive_closure("city")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    records = json.loads(out.read_text())
    uuids = {next(iter(rec.values()))["Id"] for rec in records}
    assert uuids == {C, D}


def test_copy_records_preserves_extra_fields(db, tmp_path):
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    records = [next(iter(rec.values())) for rec in json.loads(out.read_text())]
    e = next(r for r in records if r["Id"] == E)
    assert e["path"] == "tests/data/sample.data"


# ------------------------------------------------------------------ #
# Index build edge cases                                              #
# ------------------------------------------------------------------ #


def test_duplicate_uuid_keeps_last_occurrence(tmp_path):
    # Deferred index build dedups duplicate uuids, keeping the last occurrence
    # (matching the prior INSERT OR REPLACE behaviour).
    src = tmp_path / "dup.ndjson"
    src.write_text(
        "[\n"
        + json.dumps({"person": {"Id": A, "RelatedIds": [], "v": 1}})
        + ",\n"
        + json.dumps({"person": {"Id": A, "RelatedIds": [], "v": 2}})
        + "\n"
        + "]\n"
    )
    db_path = tmp_path / "dup.db"
    build_index(src, db_path)
    with Graph(db_path) as g:
        assert len(g) == 1  # deduped to a single node
        g.transitive_closure("person")
        nodes = list(stream_nodes(src, g))
    assert len(nodes) == 1
    assert nodes[0].extra["v"] == 2  # the last occurrence won


def test_build_index_reports_offset_for_bad_record(tmp_path):
    # A record missing a required field aborts the build with the byte offset
    # of the offending line so it can be located in a large source file.
    src = tmp_path / "bad.ndjson"
    src.write_text(
        "[\n"
        + json.dumps({"person": {"Id": A, "RelatedIds": []}})
        + ",\n"
        + json.dumps({"person": {"RelatedIds": []}})
        + "\n"  # missing Id
        + "]\n"
    )
    db_path = tmp_path / "bad.db"
    with pytest.raises(ValueError, match="byte offset"):
        build_index(src, db_path)


def test_copy_records_appends_missing_newline(tmp_path):
    # A source whose last line has no trailing newline must still produce
    # newline-terminated NDJSON output.
    src = tmp_path / "no_newline.ndjson"
    src.write_bytes(b'[\n{"person": {"Id": "' + A.encode() + b'", "RelatedIds": []}}')
    db_path = tmp_path / "nn.db"
    build_index(src, db_path)
    out = tmp_path / "out.ndjson"
    with Graph(db_path) as g:
        g.transitive_closure("person")
        with open(out, "wb") as fh:
            copy_records(src, g, fh)
    data = json.loads(out.read_text())
    assert len(data) == 1


# ------------------------------------------------------------------ #
# estimate_output_bytes                                                #
# ------------------------------------------------------------------ #


def test_estimate_matches_copy_records(db, tmp_path):
    db.transitive_closure("person")
    out = tmp_path / "out.json"
    estimated = estimate_output_bytes(SAMPLE, db)
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    assert estimated == out.stat().st_size


def test_estimate_empty_closure(db):
    db.transitive_closure("widget")  # no nodes of this type
    assert estimate_output_bytes(SAMPLE, db) == 5  # "[\n\n]\n"


def test_estimate_city_subset(db, tmp_path):
    db.transitive_closure("city")
    out = tmp_path / "out.json"
    estimated = estimate_output_bytes(SAMPLE, db)
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    assert estimated == out.stat().st_size


# ------------------------------------------------------------------ #
# Graph.count_type                                                     #
# ------------------------------------------------------------------ #


def test_count_type_seed_matches_person(db):
    assert db.count_type("person") == 2


def test_count_type_seed_matches_city(db):
    assert db.count_type("city") == 2


def test_count_type_unknown_returns_zero(db):
    assert db.count_type("widget") == 0


# ------------------------------------------------------------------ #
# Property seed filter                                                 #
# ------------------------------------------------------------------ #
#
#  Seeds chosen by matching a property value, then BFS expands as usual.
#  Name=Alice → seed {A} → closure {A,B,C,D,E}
#  Name=Bob   → seed {B} → closure {B,D}
#  Name=Nobody → no seeds → empty closure


def test_iter_property_seed_uuids_alice(db):
    assert list(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Alice")])) == [A]


def test_iter_property_seed_uuids_no_match(db):
    assert list(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Nobody")])) == []


def test_property_filter_seeds_only_matching(db):
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Bob")]))
    count = db.transitive_closure("person")
    assert count == 2
    assert set(db.iter_closure_uuids()) == {B, D}


def test_property_filter_alice_full_closure(db):
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Alice")]))
    assert db.transitive_closure("person") == 5
    assert set(db.iter_closure_uuids()) == {A, B, C, D, E}


def test_property_filter_no_match_empty_closure(db):
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Nobody")]))
    assert db.transitive_closure("person") == 0


def test_property_filter_count_type_respects_filter(db):
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Bob")]))
    assert db.count_type("person") == 1


def test_property_filter_missing_property_excludes(db):
    # cities have no "Name" field, so a Name filter matches none of them
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "city", [("Name", "Alice")]))
    assert db.transitive_closure("city") == 0


def test_property_filter_reapply_replaces(db):
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Alice")]))
    db.apply_seed_filter(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Bob")]))
    db.transitive_closure("person")
    assert set(db.iter_closure_uuids()) == {B, D}


def test_property_filter_multiple_where_and(db):
    # Both conditions true for A → seeds {A}
    seeds = list(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Alice"), ("Id", A)]))
    assert seeds == [A]
    # Conflicting conditions → no match
    none = list(iter_property_seed_uuids(SAMPLE, db, "person", [("Name", "Alice"), ("Id", B)]))
    assert none == []


# ------------------------------------------------------------------ #
# CLI threshold flags                                                  #
# ------------------------------------------------------------------ #


def _run_query(args: list[str], tmp_path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "subgraph.cli", "query", *args],
        capture_output=True,
        text=True,
    )


def test_cli_max_records_passes(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query(
        [SAMPLE, "person", str(out), "--max-records", "10"],
        tmp_path,
    )
    assert result.returncode == 0
    assert out.exists()


def test_cli_default_max_records_allows_small_closure(tmp_path):
    # sample has 5 nodes — well under the 4 M default
    out = tmp_path / "out.json"
    result = _run_query([SAMPLE, "person", str(out)], tmp_path)
    assert result.returncode == 0
    assert out.exists()


def test_cli_max_records_blocks(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query(
        [SAMPLE, "person", str(out), "--max-records", "3"],
        tmp_path,
    )
    assert result.returncode == 1
    assert not out.exists()


def test_cli_max_bytes_passes(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query(
        [SAMPLE, "person", str(out), "--max-bytes", str(4 * 1024 * 1024 * 1024)],
        tmp_path,
    )
    assert result.returncode == 0
    assert out.exists()


def test_cli_max_bytes_blocks(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query(
        [SAMPLE, "person", str(out), "--max-bytes", "10"],
        tmp_path,
    )
    assert result.returncode == 1
    assert not out.exists()


def test_cli_where_filters_seeds(tmp_path):
    # --where Name=Bob seeds only B → closure {B, D}
    out = tmp_path / "out.json"
    result = _run_query([SAMPLE, "person", str(out), "--where", "Name=Bob"], tmp_path)
    assert result.returncode == 0
    records = json.loads(out.read_text())
    uuids = {next(iter(rec.values()))["Id"] for rec in records}
    assert uuids == {B, D}


def test_cli_where_no_match_empty_output(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query([SAMPLE, "person", str(out), "--where", "Name=Nobody"], tmp_path)
    assert result.returncode == 0
    assert json.loads(out.read_text()) == []


def test_cli_where_invalid_format_errors(tmp_path):
    out = tmp_path / "out.json"
    result = _run_query([SAMPLE, "person", str(out), "--where", "Name"], tmp_path)
    assert result.returncode == 2  # argparse usage error
    assert not out.exists()
