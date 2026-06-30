import gzip
import io
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from subgraph import (
    Graph,
    SourceSpec,
    build_index,
    copy_records,
    db_path_for,
    open_output,
    stream_nodes,
)
from subgraph.sources import _ForwardOnlyReader, open_source, resolve_member

SAMPLE = "tests/data/sample.json"
SAMPLE_ZIP = "tests/data/sample.zip"  # holds sample.json + sample.data

A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
C = "cccccccccccccccccccccccccccccccc"
D = "dddddddddddddddddddddddddddddddd"
E = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


def _closure_records(src, seed_type, workdir) -> list:
    """Index *src*, take the closure for *seed_type*, return the copied records."""
    workdir.mkdir(parents=True, exist_ok=True)
    db = workdir / "idx.db"
    build_index(src, db)
    out = workdir / "out.json"
    with Graph(db) as g:
        g.transitive_closure(seed_type)
        with open(out, "wb") as fh:
            copy_records(src, g, fh)
    return json.loads(out.read_text())


# ------------------------------------------------------------------ #
# SourceSpec.parse                                                     #
# ------------------------------------------------------------------ #


def test_parse_plain():
    spec = SourceSpec.parse("data/foo.json")
    assert spec.kind == "plain"
    assert spec.member is None


def test_parse_gzip():
    spec = SourceSpec.parse("data/foo.json.gz")
    assert spec.kind == "gzip"


def test_parse_zip_with_member():
    spec = SourceSpec.parse("a.zip::*.json")
    assert spec.kind == "zip"
    assert spec.member == "*.json"


def test_parse_member_on_non_zip_errors():
    with pytest.raises(ValueError, match="only valid for a .zip"):
        SourceSpec.parse("foo.json::x")


def test_parse_empty_member_errors():
    with pytest.raises(ValueError, match="empty member selector"):
        SourceSpec.parse("a.zip::")


def test_parse_idempotent():
    spec = SourceSpec.parse("a.zip::x.json")
    assert SourceSpec.parse(spec) is spec


# ------------------------------------------------------------------ #
# Member resolution                                                   #
# ------------------------------------------------------------------ #


def test_resolve_glob_single_match():
    assert resolve_member(f"{SAMPLE_ZIP}::*.json") == "sample.json"


def test_resolve_no_glob_multi_member_errors():
    # sample.zip has two members, so an unqualified reference is ambiguous.
    with pytest.raises(ValueError, match="name one with"):
        resolve_member(SAMPLE_ZIP)


def test_resolve_glob_no_match_errors():
    with pytest.raises(ValueError, match="no member matches"):
        resolve_member(f"{SAMPLE_ZIP}::*.csv")


def test_resolve_glob_multi_match_errors():
    with pytest.raises(ValueError, match="matches 2 members"):
        resolve_member(f"{SAMPLE_ZIP}::sample.*")


def test_resolve_single_member_auto(tmp_path):
    z = tmp_path / "one.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("only.json", "[]\n")
    assert resolve_member(str(z)) == "only.json"


# ------------------------------------------------------------------ #
# db_path_for                                                          #
# ------------------------------------------------------------------ #


def test_db_path_plain():
    assert db_path_for("data/foo.json") == Path("data/foo.db")


def test_db_path_gzip():
    assert db_path_for("data/foo.json.gz") == Path("data/foo.db")


def test_db_path_zip_includes_member():
    assert db_path_for(f"{SAMPLE_ZIP}::*.json") == Path("tests/data/sample.zip.sample.json.db")


# ------------------------------------------------------------------ #
# _ForwardOnlyReader                                                   #
# ------------------------------------------------------------------ #


def test_forward_reader_seek_and_readline():
    r = _ForwardOnlyReader(io.BytesIO(b"line0\nline1\nline2\n"))
    r.seek(6)
    assert r.readline() == b"line1\n"
    assert r.tell() == 12


def test_forward_reader_backward_seek_raises():
    r = _ForwardOnlyReader(io.BytesIO(b"abcdef"))
    r.seek(4)
    with pytest.raises(ValueError, match="backward seek"):
        r.seek(1)


def test_forward_reader_seek_past_eof_lands_at_eof():
    r = _ForwardOnlyReader(io.BytesIO(b"abc"))
    r.seek(100)
    assert r.read() == b""


# ------------------------------------------------------------------ #
# Round-trips: compressed input == plain input                        #
# ------------------------------------------------------------------ #


def test_zip_roundtrip_matches_plain(tmp_path):
    plain = _closure_records(SAMPLE, "person", tmp_path / "plain")
    zipped = _closure_records(f"{SAMPLE_ZIP}::sample.json", "person", tmp_path / "zip")
    assert zipped == plain


def test_gzip_roundtrip_matches_plain(tmp_path):
    gz = tmp_path / "sample.json.gz"
    with open(SAMPLE, "rb") as src, gzip.open(gz, "wb") as dst:
        dst.write(src.read())
    plain = _closure_records(SAMPLE, "city", tmp_path / "plain")
    gzipped = _closure_records(str(gz), "city", tmp_path / "gz")
    assert gzipped == plain


def test_stream_nodes_over_zip(tmp_path):
    db = tmp_path / "idx.db"
    src = f"{SAMPLE_ZIP}::sample.json"
    build_index(src, db)
    with Graph(db) as g:
        g.transitive_closure("person")
        uuids = {n.uuid for n in stream_nodes(src, g)}
    assert uuids == {A, B, C, D, E}


def test_open_source_iterates_lines_over_zip():
    with open_source(f"{SAMPLE_ZIP}::sample.json") as fh:
        first = fh.readline()
    assert first.strip() == b"["


# ------------------------------------------------------------------ #
# Compressed output                                                   #
# ------------------------------------------------------------------ #


def test_output_gz_roundtrip(tmp_path):
    db = tmp_path / "idx.db"
    build_index(SAMPLE, db)
    out = tmp_path / "out.json.gz"
    with Graph(db) as g:
        g.transitive_closure("person")
        with open_output(str(out)) as fh:
            copy_records(SAMPLE, g, fh)
    with gzip.open(out, "rb") as fh:
        records = json.loads(fh.read())
    assert {next(iter(r.values()))["Id"] for r in records} == {A, B, C, D, E}


def test_output_zip_roundtrip(tmp_path):
    db = tmp_path / "idx.db"
    build_index(SAMPLE, db)
    out = tmp_path / "out.zip"
    with Graph(db) as g:
        g.transitive_closure("city")
        with open_output(str(out)) as fh:
            copy_records(SAMPLE, g, fh)
    with zipfile.ZipFile(out) as zf:
        # default member name is "<stem>.json"
        records = json.loads(zf.read("out.json"))
    assert {next(iter(r.values()))["Id"] for r in records} == {C, D}


# ------------------------------------------------------------------ #
# CLI over a zip source                                               #
# ------------------------------------------------------------------ #


def test_cli_query_over_zip(tmp_path):
    # Copy the archive so the generated .db index lands in tmp_path, not tests/data.
    archive = tmp_path / "sample.zip"
    shutil.copy(SAMPLE_ZIP, archive)
    out = tmp_path / "out.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "subgraph.cli",
            "query",
            f"{archive}::sample.json",
            "person",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    records = json.loads(out.read_text())
    assert {next(iter(r.values()))["Id"] for r in records} == {A, B, C, D, E}
