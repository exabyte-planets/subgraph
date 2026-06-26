import json
import subprocess
import sys

import pytest

from subgraph import Graph, Node, build_index, copy_records, estimate_output_bytes, stream_nodes

SAMPLE = "tests/data/sample.json"

#
#  Sample graph:
#
#  A (person, timestamp=2024-03-01T10:00:00Z) -> B, C, E
#  B (person, timestamp=2024-06-15T10:00:00Z) -> D
#  C (city,   no timestamp)                   -> (none)
#  D (city,   no timestamp)                   -> (none)
#  E (file,   no timestamp)                   -> (none)  extra: path="tests/data/sample.data"


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
    data_file.write_text(
        "[\n" + json.dumps({"person": {"Id": "X", "RelatedIds": [{"Value": "MISSING"}]}}) + "\n]\n"
    )
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
    assert uuids == {"C", "D"}


def test_copy_records_preserves_extra_fields(db, tmp_path):
    db.transitive_closure("person")
    out = tmp_path / "out.ndjson"
    with open(out, "wb") as fh:
        copy_records(SAMPLE, db, fh)
    records = [next(iter(rec.values())) for rec in json.loads(out.read_text())]
    e = next(r for r in records if r["Id"] == "E")
    assert e["path"] == "tests/data/sample.data"


# ------------------------------------------------------------------ #
# Timestamp filtering                                                   #
# ------------------------------------------------------------------ #
#
#  No filter   → seeds {A, B} → closure {A,B,C,D,E}
#  after=2024-04-01 → only B qualifies → closure {B, D}
#  before=2024-04-01 → only A qualifies → closure {A,B,C,D,E} (A reaches B)
#  after=2025-01-01 → no seeds → empty closure
#  after+before spanning both → seeds {A,B} → closure {A,B,C,D,E}
#  filter on city (no timestamps) → no seeds → empty


def test_timestamp_no_filter_unchanged(db):
    assert db.transitive_closure("person") == 5


def test_timestamp_after_excludes_early_seed(db):
    # only B (2024-06-15) qualifies; A (2024-03-01) is filtered out
    count = db.transitive_closure("person", after="2024-04-01T00:00:00Z")
    assert count == 2
    assert set(db.iter_closure_uuids()) == {"B", "D"}


def test_timestamp_before_excludes_late_seed(db):
    # only A (2024-03-01) qualifies; A reaches everything
    count = db.transitive_closure("person", before="2024-04-01T00:00:00Z")
    assert count == 5
    assert set(db.iter_closure_uuids()) == set("ABCDE")


def test_timestamp_after_no_match(db):
    assert db.transitive_closure("person", after="2025-01-01T00:00:00Z") == 0


def test_timestamp_range_both_seeds(db):
    count = db.transitive_closure(
        "person", after="2024-01-01T00:00:00Z", before="2024-12-31T23:59:59Z"
    )
    assert count == 5


def test_timestamp_null_nodes_excluded_by_filter(db):
    # cities have no timestamp — applying any filter excludes them from seeds
    assert db.transitive_closure("city", after="2024-01-01T00:00:00Z") == 0


def test_timestamp_null_nodes_still_reachable(db):
    # cities have no timestamp but are reachable via person A's edges
    db.transitive_closure("person", before="2024-04-01T00:00:00Z")
    assert "C" in set(db.iter_closure_uuids())  # reached from A, not seeded itself


def test_duplicate_uuid_keeps_last_occurrence(tmp_path):
    # Deferred index build dedups duplicate uuids, keeping the last occurrence
    # (matching the prior INSERT OR REPLACE behaviour).
    src = tmp_path / "dup.ndjson"
    src.write_text(
        "[\n"
        + json.dumps({"person": {"Id": "A", "RelatedIds": [], "v": 1}})
        + ",\n"
        + json.dumps({"person": {"Id": "A", "RelatedIds": [], "v": 2}})
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
        + json.dumps({"person": {"Id": "A", "RelatedIds": []}})
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
    src.write_bytes(b'[\n{"person": {"Id": "A", "RelatedIds": []}}')
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


def test_count_type_with_after_filter(db):
    # only B (2024-06-15) qualifies when seeding after 2024-04-01
    assert db.count_type("person", after="2024-04-01T00:00:00Z") == 1


def test_count_type_with_before_filter(db):
    # only A (2024-03-01) qualifies before 2024-04-01
    assert db.count_type("person", before="2024-04-01T00:00:00Z") == 1


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
    # sample has 5 nodes — well under the 5 M default
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
