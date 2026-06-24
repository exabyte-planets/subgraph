import argparse

from subgraph import Graph, build_index, copy_records


def cmd_index(args: argparse.Namespace) -> None:
    build_index(args.file, args.db)
    with Graph(args.db) as g:
        print(f"Indexed {len(g)} nodes into {args.db}")


def cmd_query(args: argparse.Namespace) -> None:
    with Graph(args.db) as g, open(args.output, "wb") as fh:
        g.transitive_closure(args.seed_type)
        count = copy_records(args.file, g, fh)
    print(f"Wrote {count} records to {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transitive-closure subgraph extraction.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_index = sub.add_parser("index", help="Build SQLite index from a data file.")
    p_index.add_argument("file", help="Path to NDJSON source file")
    p_index.add_argument("db", help="Path to write the SQLite index")
    p_index.set_defaults(func=cmd_index)

    p_query = sub.add_parser("query", help="Compute closure and write matching nodes to a file.")
    p_query.add_argument("db", help="Path to the SQLite index")
    p_query.add_argument("file", help="Path to NDJSON source file (for full records)")
    p_query.add_argument("seed_type", help="Node type to seed the closure from")
    p_query.add_argument("output", help="Path to write the result NDJSON")
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
