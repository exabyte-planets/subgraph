import argparse
import logging
import sys

from tqdm.contrib.logging import logging_redirect_tqdm

from subgraph import Graph, build_index, copy_records

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def cmd_index(args: argparse.Namespace) -> None:
    with logging_redirect_tqdm():
        build_index(args.file, args.db, progress=True)
    with Graph(args.db) as g:
        logger.info("index ready: %d nodes in %s", len(g), args.db)


def cmd_query(args: argparse.Namespace) -> None:
    with Graph(args.db) as g, open(args.output, "wb") as fh, logging_redirect_tqdm():
        g.transitive_closure(args.seed_type, progress=True)
        count = copy_records(args.file, g, fh, progress=True)
    logger.info("wrote %d records to %s", count, args.output)


def main() -> None:
    _configure_logging()

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
