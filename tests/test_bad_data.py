"""Malformed-source handling for :func:`subgraph.build_index`.

Every bad record must abort the build with a :class:`ValueError` that names the
**byte offset** of the offending line, so a single bad record in a multi-GB
source can be located.  These tests pin both the offset and the descriptive
message for each kind of malformation.
"""

import json

import pytest

from subgraph import Graph, build_index, stream_nodes

A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _write(tmp_path, *record_lines: str):
    """Write *record_lines* as a JSON-array NDJSON source; return its path."""
    src = tmp_path / "bad.ndjson"
    body = ",\n".join(record_lines)
    src.write_text(f"[\n{body}\n]\n")
    return src


def _build(tmp_path, *record_lines: str):
    """Run build_index over *record_lines* and return the raised exception info."""
    src = _write(tmp_path, *record_lines)
    with pytest.raises(ValueError) as excinfo:
        build_index(src, tmp_path / "bad.db")
    return excinfo.value


# ------------------------------------------------------------------ #
# RelatedIds shape (the motivating cases)                             #
# ------------------------------------------------------------------ #


def test_relatedids_list_of_str(tmp_path):
    # RelatedIds is a list of bare id strings instead of {"Value": ...} objects.
    err = _build(tmp_path, json.dumps({"person": {"Id": A, "RelatedIds": [B]}}))
    msg = str(err)
    assert "byte offset" in msg
    assert "RelatedIds[0]" in msg
    assert "'Value'" in msg


def test_relatedids_item_missing_value(tmp_path):
    err = _build(tmp_path, json.dumps({"person": {"Id": A, "RelatedIds": [{"X": B}]}}))
    assert "byte offset" in str(err)
    assert "RelatedIds[0]" in str(err)


def test_relatedids_not_a_list(tmp_path):
    err = _build(tmp_path, json.dumps({"person": {"Id": A, "RelatedIds": "nope"}}))
    assert "byte offset" in str(err)
    assert "RelatedIds must be a list" in str(err)


def test_relatedids_falsy_non_list_still_errors(tmp_path):
    # A falsy non-list (false/0/{}/"") must not slip past the list guard as "no
    # edges" — it is malformed and must abort the build.
    err = _build(tmp_path, json.dumps({"person": {"Id": A, "RelatedIds": False}}))
    assert "byte offset" in str(err)
    assert "RelatedIds must be a list" in str(err)


def test_relatedids_bad_item_reports_its_index(tmp_path):
    # The first item is well-formed; the failure must point at index 1.
    rec = {"person": {"Id": A, "RelatedIds": [{"Value": B}, 42]}}
    err = _build(tmp_path, json.dumps(rec))
    assert "RelatedIds[1]" in str(err)


# ------------------------------------------------------------------ #
# Invalid ids                                                         #
# ------------------------------------------------------------------ #


def test_non_hex_id(tmp_path):
    err = _build(tmp_path, json.dumps({"person": {"Id": "not-hex", "RelatedIds": []}}))
    assert "byte offset" in str(err)
    assert "invalid Id" in str(err)


def test_non_hex_related_value(tmp_path):
    err = _build(tmp_path, json.dumps({"person": {"Id": A, "RelatedIds": [{"Value": "xyz"}]}}))
    assert "byte offset" in str(err)
    assert "invalid RelatedIds[0].Value" in str(err)


# ------------------------------------------------------------------ #
# Missing / malformed wrapper                                         #
# ------------------------------------------------------------------ #


def test_missing_id(tmp_path):
    err = _build(tmp_path, json.dumps({"person": {"RelatedIds": []}}))
    assert "byte offset" in str(err)
    assert "missing required field" in str(err)
    assert "Id" in str(err)


def test_empty_wrapper_object(tmp_path):
    # Regression: an empty {} used to leak StopIteration as a RuntimeError.
    err = _build(tmp_path, json.dumps({}))
    assert "byte offset" in str(err)
    assert "non-empty object" in str(err)


def test_type_value_not_object(tmp_path):
    err = _build(tmp_path, json.dumps({"person": "x"}))
    assert "byte offset" in str(err)
    assert "must be an object" in str(err)


def test_malformed_json_line(tmp_path):
    src = tmp_path / "bad.ndjson"
    src.write_text('[\n{"person": {"Id": \n]\n')  # truncated JSON
    with pytest.raises(ValueError) as excinfo:
        build_index(src, tmp_path / "bad.db")
    assert "byte offset" in str(excinfo.value)
    assert "invalid JSON" in str(excinfo.value)


# ------------------------------------------------------------------ #
# Offset accuracy                                                     #
# ------------------------------------------------------------------ #


def test_offset_points_at_the_bad_line(tmp_path):
    # A valid record precedes the bad one; the reported offset must be the byte
    # position of the bad line, not the start of the file.
    good = json.dumps({"person": {"Id": A, "RelatedIds": []}})
    bad = json.dumps({"person": {"Id": B, "RelatedIds": [42]}})
    src = _write(tmp_path, good, bad)
    raw = src.read_bytes()
    expected = raw.index(bad.encode())

    with pytest.raises(ValueError) as excinfo:
        build_index(src, tmp_path / "bad.db")
    assert f"byte offset {expected}" in str(excinfo.value)


# ------------------------------------------------------------------ #
# Well-formed RelatedIds still works (guards against over-strictness) #
# ------------------------------------------------------------------ #


def test_valid_related_ids_still_build_and_stream(tmp_path):
    rec = {"person": {"Id": A, "RelatedIds": [{"Value": B}], "Name": "Alice"}}
    src = _write(tmp_path, rec_line := json.dumps(rec))
    assert rec_line  # written
    db = tmp_path / "ok.db"
    build_index(src, db)
    with Graph(db) as g:
        g.transitive_closure("person")
        nodes = list(stream_nodes(src, g))
    node = next(n for n in nodes if n.uuid == A)
    assert node.related == [B]
    assert node.extra.get("Name") == "Alice"


def test_missing_related_ids_field_is_ok(tmp_path):
    # RelatedIds is optional; a record without it indexes as an edgeless node.
    src = _write(tmp_path, json.dumps({"city": {"Id": A}}))
    db = tmp_path / "ok.db"
    build_index(src, db)
    with Graph(db) as g:
        assert len(g) == 1
