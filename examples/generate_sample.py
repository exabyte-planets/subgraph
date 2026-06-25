"""Generate a synthetic NDJSON graph for testing and benchmarking.

Usage:
    uv run python examples/generate_sample.py                  # defaults
    uv run python examples/generate_sample.py --persons 50000 --out big.ndjson
    uv run python examples/generate_sample.py --ts-start 2024-01-01 --ts-end 2024-12-31
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta


def generate(
    persons: int,
    cities: int,
    files: int,
    edges_per_person: int,
    seed: int,
    out_path: str,
    ts_start: datetime | None,
    ts_end: datetime | None,
) -> None:
    rng = random.Random(seed)

    person_uuids = [f"person-{i:06d}" for i in range(persons)]
    city_uuids = [f"city-{i:04d}" for i in range(cities)]
    file_uuids = [f"file-{i:05d}" for i in range(files)]
    all_uuids = person_uuids + city_uuids + file_uuids

    ts_range_seconds = int((ts_end - ts_start).total_seconds()) if ts_start and ts_end else None

    def random_timestamp() -> str | None:
        if ts_start is None or ts_range_seconds is None:
            return None
        dt = ts_start + timedelta(seconds=rng.randint(0, ts_range_seconds))
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_records: list[dict] = []

    for uuid in person_uuids:
        k = min(edges_per_person, len(all_uuids) - 1)
        # Sample k+1 distinct uuids so we can drop self (if drawn) and still
        # have k.  rng.sample is O(k), so generation stays O(persons * k)
        # rather than rebuilding an N-element exclude list per person.
        related = [u for u in rng.sample(all_uuids, k + 1) if u != uuid][:k]
        fields: dict = {"uuid": uuid, "related": related}
        ts = random_timestamp()
        if ts:
            fields["timestamp"] = ts
        all_records.append({"person": fields})

    for uuid in city_uuids:
        all_records.append({"city": {"uuid": uuid, "related": []}})

    for i, uuid in enumerate(file_uuids):
        all_records.append(
            {
                "file": {
                    "uuid": uuid,
                    "related": [],
                    "path": f"/data/files/{i:05d}.bin",
                    "size_bytes": rng.randint(1024, 10 * 1024 * 1024),
                }
            }
        )

    written = 0
    with open(out_path, "w") as fh:
        fh.write("[\n")
        for i, rec in enumerate(all_records):
            suffix = "," if i < len(all_records) - 1 else ""
            fh.write(json.dumps(rec) + suffix + "\n")
            written += 1
        fh.write("]\n")

    print(f"wrote {written} records to {out_path}")
    print(f"  {persons} persons, {cities} cities, {files} files")
    if ts_start and ts_end:
        print(f"  timestamps: {ts_start.date()} – {ts_end.date()}")


def _parse_date(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--persons", type=int, default=1_000)
    parser.add_argument("--cities", type=int, default=100)
    parser.add_argument("--files", type=int, default=200)
    parser.add_argument("--edges-per-person", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="examples/sample.ndjson")
    parser.add_argument(
        "--ts-start",
        metavar="DATE",
        type=_parse_date,
        default=None,
        help="Start of random timestamp range for person nodes (e.g. 2024-01-01)",
    )
    parser.add_argument(
        "--ts-end",
        metavar="DATE",
        type=_parse_date,
        default=None,
        help="End of random timestamp range for person nodes (e.g. 2024-12-31)",
    )
    args = parser.parse_args()

    generate(
        persons=args.persons,
        cities=args.cities,
        files=args.files,
        edges_per_person=args.edges_per_person,
        seed=args.seed,
        out_path=args.out,
        ts_start=args.ts_start,
        ts_end=args.ts_end,
    )


if __name__ == "__main__":
    main()
