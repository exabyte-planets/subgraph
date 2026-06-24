import json

import pytest

from subgraph import Graph, Node, build_index, copy_records, stream_nodes

SAMPLE = "tests/data/sample.ndjson"

#
#  Sample graph:
#
#  A (person) -> B, C, E
#  B (person) -> D
#  C (city)   -> (none)
#  D (city)   -> (none)
#  E (file)   -> (none)  extra: path="tests/data/sample.data"


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
    assert set(db.iter_closure_uuids()) == set("ABCDE")


# ------------------------------------------------------------------ #
# Transitive closure — seed type "city"                               #
# ------------------------------------------------------------------ #
#
#  C and D have no outgoing edges → closure == {C, D}


def test_city_closure_count(db):
    assert db.transitive_closure("city") == 2


def test_city_closure_uuids(db):
    db.transitive_closure("city")
    assert set(db.iter_closure_uuids()) == {"C", "D"}


# ------------------------------------------------------------------ #
# Edge cases                                                           #
# ------------------------------------------------------------------ #


def test_unknown_type_empty(db):
    assert db.transitive_closure("widget") == 0


def test_closure_replaced_on_second_call(db):
    db.transitive_closure("person")
    db.transitive_closure("city")
    assert set(db.iter_closure_uuids()) == {"C", "D"}


def test_dangling_related_in_closure(tmp_path):
    data_file = tmp_path / "dangle.ndjson"
    data_file.write_text(json.dumps({"type": "person", "uuid": "X", "related": ["MISSING"]}) + "\n")
    db_path = tmp_path / "dangle.db"
    build_index(data_file, db_path)
    with Graph(db_path) as g:
        g.transitive_closure("person")
        closure = set(g.iter_closure_uuids())
    assert "X" in closure
    assert "MISSING" in closure  # reachable even though absent from nodes table


# ------------------------------------------------------------------ #
# stream_nodes                                                         #
# ------------------------------------------------------------------ #


def test_stream_nodes_count(db):
    db.transitive_closure("person")
    assert len(list(stream_nodes(SAMPLE, db))) == 5


def test_stream_nodes_extra_fields(db):
    db.transitive_closure("person")
    nodes = list(stream_nodes(SAMPLE, db))
    e = next(n for n in nodes if n.uuid == "E")
    assert e.extra.get("path") == "tests/data/sample.data"


def test_stream_nodes_city_only(db):
    db.transitive_closure("city")
    nodes = list(stream_nodes(SAMPLE, db))
    assert {n.uuid for n in nodes} == {"C", "D"}


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


def test_copy_records_is_byte_identical_to_source(db, tmp_path):
    # The full-graph closure copies every source line verbatim, so the output
    # must equal the original file byte-for-byte (offsets visited in order).
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    with open(SAMPLE, "rb") as src:
        assert out.read_bytes() == src.read()


def test_copy_records_subset_lines(db, tmp_path):
    db.transitive_closure("city")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    lines = out.read_text().splitlines()
    uuids = {json.loads(line)["uuid"] for line in lines}
    assert uuids == {"C", "D"}


def test_copy_records_preserves_extra_fields(db, tmp_path):
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    records = [json.loads(line) for line in out.read_text().splitlines()]
    e = next(r for r in records if r["uuid"] == "E")
    assert e["path"] == "tests/data/sample.data"


def test_copy_records_appends_missing_newline(tmp_path):
    # A source whose last line has no trailing newline must still produce
    # newline-terminated NDJSON output.
    src = tmp_path / "no_newline.ndjson"
    src.write_bytes(b'{"type": "person", "uuid": "A", "related": []}')
    db_path = tmp_path / "nn.db"
    build_index(src, db_path)
    out = tmp_path / "out.ndjson"
    with Graph(db_path) as g:
        g.transitive_closure("person")
        with open(out, "wb") as fh:
            copy_records(src, g, fh)
    assert out.read_bytes().endswith(b"\n")
    assert out.read_text().count("\n") == 1
