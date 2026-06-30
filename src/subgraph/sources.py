"""Transparent reading of JSON sources that live inside compressed containers.

The rest of the package treats a *source* as a stream of bytes addressed by
absolute offset: :func:`subgraph.loader.build_index` records the byte offset of
each record's line, and every later read seeks back to that offset.  A plain
file supports that directly.  A compressed container (a ``.zip`` member or a
``.gz`` stream) does not — there is no cheap random access into the
*decompressed* byte stream; reaching offset *N* means decompressing everything
before it.

This module bridges the two.  The key observation is that every offset iterator
in :mod:`subgraph.graph` yields offsets in **ascending** order, so every read
pass is really a forward-only scan.  :class:`_ForwardOnlyReader` exploits that:
it wraps a sequential decompressing stream and turns ``seek(offset)`` into a
read-and-discard skip, refusing to ever move backward.  Each top-level pass
re-opens the container from the start, so no temporary extraction to disk is
needed and memory stays bounded regardless of the decompressed size.
"""

from __future__ import annotations

import fnmatch
import gzip
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal, Protocol, cast

# Skip size when fast-forwarding a forward-only reader to a target offset.
_SKIP_CHUNK = 1 << 20

Kind = Literal["plain", "zip", "gzip"]


class _RawStream(Protocol):
    """The minimal read interface a :class:`_ForwardOnlyReader` wraps.

    Matched structurally by ``gzip.GzipFile`` and ``zipfile.ZipExtFile``.
    """

    def read(self, size: int = ..., /) -> bytes: ...
    def readline(self, size: int = ..., /) -> bytes: ...


def _detect_kind(path: Path) -> Kind:
    """Classify a container by its file extension (case-insensitive)."""
    suffix = path.suffix.lower()
    if suffix == ".zip":
        return "zip"
    if suffix == ".gz":
        return "gzip"
    return "plain"


@dataclass(frozen=True)
class SourceSpec:
    """A parsed reference to a JSON source, possibly inside a container.

    The text form is ``<path>`` for a plain file or ``.gz`` stream, and
    ``<archive.zip>::<glob>`` to name a member inside a zip (the ``::<glob>``
    part is optional when the archive holds a single member).
    """

    path: Path
    kind: Kind
    member: str | None = None  # zip member glob; None means "infer"

    @classmethod
    def parse(cls, raw: str | Path | SourceSpec) -> SourceSpec:
        """Parse a source string into a :class:`SourceSpec` (idempotent)."""
        if isinstance(raw, SourceSpec):
            return raw

        text = str(raw)
        member: str | None = None
        if "::" in text:
            container, _, member = text.partition("::")
            if not member:
                raise ValueError(f"empty member selector after '::' in {text!r}")
        else:
            container = text

        path = Path(container)
        kind = _detect_kind(path)
        if member is not None and kind != "zip":
            raise ValueError(
                f"member selector '::{member}' is only valid for a .zip archive, not {path.name!r}"
            )
        return cls(path=path, kind=kind, member=member)


# Anything the public API accepts as a source reference.
Source = str | Path | SourceSpec


def _zip_member_names(zf: zipfile.ZipFile) -> list[str]:
    """Return the archive's file members, excluding directory entries."""
    return [n for n in zf.namelist() if not n.endswith("/")]


def _resolve_member_name(zf: zipfile.ZipFile, glob: str | None) -> str:
    """Resolve *glob* to a single member name within *zf*.

    With no glob, the archive must hold exactly one member.  With a glob, it
    must match exactly one member.  Anything else raises ``ValueError`` whose
    message lists the available members so the caller can correct the selector.
    """
    names = _zip_member_names(zf)
    available = ", ".join(names) or "(none)"

    if glob is None:
        if len(names) == 1:
            return names[0]
        raise ValueError(
            f"archive holds {len(names)} members; name one with '::<glob>'. Available: {available}"
        )

    matches = fnmatch.filter(names, glob)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"no member matches '::{glob}'. Available: {available}")
    raise ValueError(f"'::{glob}' matches {len(matches)} members: {', '.join(matches)}")


def resolve_member(spec: str | Path | SourceSpec) -> str:
    """Open the zip named by *spec* and return its resolved member name."""
    spec = SourceSpec.parse(spec)
    if spec.kind != "zip":
        raise ValueError("resolve_member is only meaningful for a zip source")
    with zipfile.ZipFile(spec.path) as zf:
        return _resolve_member_name(zf, spec.member)


class _ForwardOnlyReader:
    """Forward-only positioned reader over a sequential decompressing stream.

    Exposes just the slice of the binary-file interface the loader needs — line
    iteration, ``readline``, ``read`` and an *absolute, forward-only* ``seek`` —
    on top of a stream that can only be read front-to-back.  A backward seek
    raises rather than silently re-decompressing from byte zero, turning the
    callers' ascending-offset invariant into an enforced guarantee.
    """

    def __init__(self, raw: _RawStream) -> None:
        self._raw = raw
        self._pos = 0

    def tell(self) -> int:
        return self._pos

    def readline(self) -> bytes:
        line = self._raw.readline()
        self._pos += len(line)
        return line

    def read(self, size: int = -1) -> bytes:
        data = self._raw.read(size)
        self._pos += len(data)
        return data

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence != 0:
            raise ValueError("forward-only reader supports only absolute (whence=0) seeks")
        if offset < self._pos:
            raise ValueError(
                f"backward seek from {self._pos} to {offset} is not supported on a compressed "
                "source; offsets must be visited in ascending order"
            )
        remaining = offset - self._pos
        while remaining:
            chunk = self._raw.read(min(remaining, _SKIP_CHUNK))
            if not chunk:
                break  # past EOF: land at EOF, mirroring file semantics
            self._pos += len(chunk)
            remaining -= len(chunk)
        return self._pos

    def __iter__(self) -> _ForwardOnlyReader:
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line


@contextmanager
def open_source(spec: str | Path | SourceSpec) -> Iterator[BinaryIO]:
    """Open *spec* for reading, decompressing transparently.

    Yields a binary stream that supports line iteration, ``readline`` and a
    forward-only ``seek`` — the contract the loader relies on.  Plain files are
    returned as the native handle (which also supports backward seeks); zip and
    gzip sources are wrapped in a :class:`_ForwardOnlyReader`.
    """
    spec = SourceSpec.parse(spec)
    if spec.kind == "plain":
        with open(spec.path, "rb") as fh:
            yield fh
    elif spec.kind == "gzip":
        with gzip.open(spec.path, "rb") as raw:
            yield cast(BinaryIO, _ForwardOnlyReader(raw))
    else:  # zip
        with zipfile.ZipFile(spec.path) as zf:
            member = _resolve_member_name(zf, spec.member)
            with zf.open(member) as raw:
                yield cast(BinaryIO, _ForwardOnlyReader(raw))


@contextmanager
def open_output(target: str | Path) -> Iterator[BinaryIO]:
    """Open an output sink, compressing transparently by extension.

    ``out.gz`` writes a gzip stream; ``out.zip`` (optionally ``out.zip::name``)
    writes a single deflate-compressed member, defaulting its name to
    ``<stem>.json``; anything else writes a plain file.  Yields a binary
    writable handle.
    """
    text = str(target)
    member: str | None = None
    if "::" in text:
        container, _, member = text.partition("::")
    else:
        container = text

    path = Path(container)
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "wb") as fh:
            yield cast(BinaryIO, fh)
    elif suffix == ".zip":
        name = member or f"{path.stem}.json"
        # force_zip64: the member size is unknown when streaming, so without this
        # a closure over 2 GiB raises RuntimeError at close (ZIP64_LIMIT).
        with (
            zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf,
            zf.open(name, "w", force_zip64=True) as fh,
        ):
            yield cast(BinaryIO, fh)
    else:
        with open(path, "wb") as fh:
            yield fh


def db_path_for(spec: str | Path | SourceSpec) -> Path:
    """Return the SQLite index path that pairs with *spec*.

    Plain ``foo.json`` and gzip ``foo.json.gz`` both index to ``foo.db`` (the
    container suffix is dropped).  A zip indexes per-member to
    ``<archive>.<member>.db`` so two members of one archive never share — and so
    never clobber — a single index.
    """
    spec = SourceSpec.parse(spec)
    if spec.kind == "plain":
        return spec.path.with_suffix(".db")
    if spec.kind == "gzip":
        # Drop ".gz", then treat the inner name like a plain file: foo.json.gz -> foo.db
        return spec.path.with_suffix("").with_suffix(".db")
    # zip: fold the resolved member into the name so members don't collide.
    member = resolve_member(spec)
    safe = member.replace("/", "_").replace("\\", "_")
    return spec.path.with_name(f"{spec.path.name}.{safe}.db")
