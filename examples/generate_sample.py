"""Generate a synthetic NDJSON graph for testing and benchmarking.

Usage:
    uv run python examples/generate_sample.py                  # defaults
    uv run python examples/generate_sample.py --persons 50000 --out big.ndjson
"""

from __future__ import annotations

import argparse
import json
import random


def generate(
    persons: int,
    cities: int,
    files: int,
    edges_per_person: int,
    seed: int,
    out_path: str,
) -> None:
    rng = random.Random(seed)

    person_uuids = [f"person-{i:06d}" for i in range(persons)]
    city_uuids = [f"city-{i:04d}" for i in range(cities)]
    file_uuids = [f"file-{i:05d}" for i in range(files)]
    all_uuids = person_uuids + city_uuids + file_uuids

    written = 0
    with open(out_path, "w") as fh:
        for uuid in person_uuids:
            k = min(edges_per_person, len(all_uuids) - 1)
            related = rng.sample([u for u in all_uuids if u != uuid], k)
            fh.write(json.dumps({"type": "person", "uuid": uuid, "related": related}) + "\n")
            written += 1

        for uuid in city_uuids:
            fh.write(json.dumps({"type": "city", "uuid": uuid, "related": []}) + "\n")
            written += 1

        for i, uuid in enumerate(file_uuids):
            fh.write(
                json.dumps(
                    {
                        "type": "file",
                        "uuid": uuid,
                        "related": [],
                        "path": f"/data/files/{i:05d}.bin",
                        "size_bytes": rng.randint(1024, 10 * 1024 * 1024),
                    }
                )
                + "\n"
            )
            written += 1

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
