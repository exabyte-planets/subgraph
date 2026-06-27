import argparse
import logging
import sys
from pathlib import Path

from tqdm.contrib.logging import logging_redirect_tqdm

from subgraph import (
    Graph,
    build_index,
    copy_records,
    estimate_output_bytes,
    iter_property_seed_uuids,
)

logger = logging.getLogger("subgraph")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def _where(value: str) -> tuple[str, str]:
    """argparse type: parse a ``PROPERTY=VALUE`` seed filter into a pair.

    Splits on the first ``=`` so values may themselves contain ``=``.
    """
    key, sep, val = value.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"--where must be PROPERTY=VALUE, got {value!r}")
    return key, val


def _db_for(file: Path) -> Path:
    return file.with_suffix(".db")


def cmd_index(args: argparse.Namespace) -> None:
    file = Path(args.file)
    db = _db_for(file)
    with logging_redirect_tqdm():
        build_index(file, db, progress=True)
    with Graph(db) as g:
        logger.info("index ready: %d nodes in %s", len(g), db)


def _fmt_bytes(n: int) -> str:
    for unit, threshold in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if n >= threshold:
            return f"{n / threshold:.1f} {unit} ({n:,} bytes)"
    return f"{n:,} bytes"


def cmd_query(args: argparse.Namespace) -> None:
    file = Path(args.file)
    db = _db_for(file)

    if not db.exists():
        logger.info("index %s not found — building now", db)
        with logging_redirect_tqdm():
            build_index(file, db, progress=True)

    output = (
        Path(args.output) if args.output else file.with_name(f"{file.stem}_{args.seed_type}.json")
    )

    with Graph(db) as g, logging_redirect_tqdm():
        if args.where:
            matched = g.apply_seed_filter(
                iter_property_seed_uuids(file, g, args.seed_type, args.where, progress=True)
            )
            logger.info(
                "property filter %s matched %d %r node(s)",
                " AND ".join(f"{k}={v}" for k, v in args.where),
                matched,
                args.seed_type,
            )

        g.transitive_closure(args.seed_type, progress=True)

        total_nodes = len(g)
        seed_count = g.count_type(args.seed_type)
        record_count = g.closure_size()
        expansion = record_count - seed_count
        coverage = 100.0 * record_count / total_nodes if total_nodes else 0.0

        logger.info(
            "closure stats — seed type: %r | seeds: %d | expansion: +%d"
            " | closure: %d / %d nodes (%.1f%% of graph)",
            args.seed_type,
            seed_count,
            expansion,
            record_count,
            total_nodes,
            coverage,
        )

        if args.max_records is not None and record_count > args.max_records:
            logger.error(
                "closure contains %d records, which exceeds --max-records %d; aborting",
                record_count,
                args.max_records,
            )
            sys.exit(1)

        if args.max_bytes is not None:
            estimated = estimate_output_bytes(file, g, progress=True)
            if estimated > args.max_bytes:
                logger.error(
                    "estimated output size %s exceeds --max-bytes %s; aborting",
                    _fmt_bytes(estimated),
                    _fmt_bytes(args.max_bytes),
                )
                sys.exit(1)
            logger.info("estimated output size: %s (within limit)", _fmt_bytes(estimated))

        with open(output, "wb") as fh:
            count = copy_records(file, g, fh, progress=True)
    logger.info("wrote %d records to %s", count, output)


def main() -> None:
    _configure_logging()

    parser = argparse.ArgumentParser(description="Transitive-closure subgraph extraction.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_index = sub.add_parser("index", help="Build SQLite index from a data file.")
    p_index.add_argument("file", help="Path to NDJSON source file (.db index written alongside it)")
    p_index.set_defaults(func=cmd_index)

    p_query = sub.add_parser("query", help="Compute closure and write matching nodes to a file.")
    p_query.add_argument("file", help="Path to NDJSON source file")
    p_query.add_argument("seed_type", help="Node type to seed the closure from")
    p_query.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Output path (default: <stem>_<seed_type>.json next to the input file)",
    )
    p_query.add_argument(
        "--where",
        metavar="PROPERTY=VALUE",
        type=_where,
        action="append",
        default=None,
        help=(
            "Only seed nodes whose PROPERTY exactly equals VALUE. Repeatable;"
            " multiple --where filters are combined with AND."
        ),
    )
    p_query.add_argument(
        "--max-records",
        metavar="N",
        type=int,
        default=4_000_000,
        help=(
            "Abort without writing output if the closure exceeds N records"
            " (default: 4,000,000 — safe limit for 4 GB)"
        ),
    )
    p_query.add_argument(
        "--max-bytes",
        metavar="N",
        type=int,
        default=None,
        help=(
            "Abort without writing output if the estimated output JSON exceeds N bytes"
            " (default: None — no limit)"
        ),
    )
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
