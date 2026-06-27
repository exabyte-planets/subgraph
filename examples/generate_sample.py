"""Generate a synthetic NDJSON graph for testing and benchmarking.

Usage:
    uv run python examples/generate_sample.py                  # defaults
    uv run python examples/generate_sample.py --persons 50000 --out big.ndjson
"""

from __future__ import annotations

import argparse
import json
import random
import uuid


def generate(
    persons: int,
    cities: int,
    files: int,
    edges_per_person: int,
    seed: int,
    out_path: str,
) -> None:
    rng = random.Random(seed)

    def guid() -> str:
        # Draw from the seeded RNG so output is reproducible for a given --seed.
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))

    person_uuids = [guid() for _ in range(persons)]
    city_uuids = [guid() for _ in range(cities)]
    file_uuids = [guid() for _ in range(files)]
    all_uuids = person_uuids + city_uuids + file_uuids

    all_records: list[dict] = []

    for uuid_ in person_uuids:
        k = min(edges_per_person, len(all_uuids) - 1)
        # Sample k+1 distinct uuids so we can drop self (if drawn) and still
        # have k.  rng.sample is O(k), so generation stays O(persons * k)
        # rather than rebuilding an N-element exclude list per person.
        related = [u for u in rng.sample(all_uuids, k + 1) if u != uuid_][:k]
        all_records.append({"person": {"Id": uuid_, "RelatedIds": [{"Value": r} for r in related]}})

    for uuid_ in city_uuids:
        all_records.append({"city": {"Id": uuid_, "RelatedIds": []}})

    for i, uuid_ in enumerate(file_uuids):
        all_records.append(
            {
                "file": {
                    "Id": uuid_,
                    "RelatedIds": [],
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--persons", type=int, default=1_000)
    parser.add_argument("--cities", type=int, default=100)
    parser.add_argument("--files", type=int, default=200)
    parser.add_argument("--edges-per-person", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="examples/sample.ndjson")
    args = parser.parse_args()

    generate(
        persons=args.persons,
        cities=args.cities,
        files=args.files,
        edges_per_person=args.edges_per_person,
        seed=args.seed,
        out_path=args.out,
    )


if __name__ == "__main__":
    main()
