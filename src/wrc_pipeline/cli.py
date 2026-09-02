"""Command-line runner for the crawl.

    python -m wrc_pipeline.cli --start-date 2024-01-01 --end-date 2024-01-31 --bodies wrc
    python -m wrc_pipeline.cli --dry-run          # print the planned requests, fetch nothing

Exits non-zero if the run did not reconcile, so a scheduler notices a crawl that
quietly lost records.

Dagster (Phase 7) calls the same spider with the same settings dict rather than
shelling out to this file.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from scrapy import signals
from scrapy.crawler import CrawlerProcess

from .logging_config import get_logger
from .settings import get_settings

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Every default comes from config, so bare `python -m wrc_pipeline.cli` works."""
    cfg = get_settings()
    parser = argparse.ArgumentParser(prog="wrc-pipeline", description="Scrape WRC decisions.")
    parser.add_argument("--start-date", default=cfg.partitions.start_date.isoformat())
    parser.add_argument("--end-date", default=cfg.partitions.end_date.isoformat())
    parser.add_argument("--bodies", default=None, help="comma-separated slugs (default: all)")
    parser.add_argument(
        "--partition-size",
        default=None,
        choices=[size.value for size in type(cfg.partitions.size)],
    )
    parser.add_argument("--log-level", default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the requests that would be made, without fetching anything",
    )
    return parser


def totals_of(counters: dict[Any, Any]) -> dict[str, int | bool]:
    """Aggregate the per-partition counters into run totals."""
    totals = {
        "listings": 0,
        "scraped": 0,
        "skipped": 0,
        "failed": 0,
        "distinct": 0,
        "duplicate_listings": 0,
        "listings_unaccounted": 0,
        "records_unaccounted": 0,
    }
    for counter in counters.values():
        summary = counter.as_dict()
        totals["listings"] += summary["listings_found"]
        totals["scraped"] += counter.scraped
        totals["skipped"] += counter.skipped
        totals["failed"] += counter.failed
        totals["distinct"] += summary["records_distinct"]
        totals["duplicate_listings"] += summary["duplicate_listings"]
        totals["listings_unaccounted"] += summary["listings_unaccounted"]
        totals["records_unaccounted"] += summary["records_unaccounted"]

    # Two checks: did we see every advertised listing, and did every distinct
    # decision reach an outcome. Duplicated listings are the source's own page
    # overlap and violate neither.
    totals["reconciled"] = (
        totals["listings_unaccounted"] == 0 and totals["records_unaccounted"] == 0
    )
    return totals


def incomplete_partitions(counters: dict[Any, Any]) -> list[str]:
    """Partitions where the store holds fewer records than the site advertised.

    This is the question a scheduler needs answered: re-run, or move on. Asked of
    MongoDB rather than of the run's counters, because a partition can be
    complete in the store even when the pass that just finished saw less than the
    full set.
    """
    from .storage import mongo

    short = []
    for counter in counters.values():
        held = mongo.count_partition_records(counter.body, counter.partition_key)
        if held < counter.listings:
            short.append(f"{counter.body}/{counter.partition_key}")
    return short


def dry_run(spider: Any) -> int:
    """List the planned start requests without making any."""
    for request in spider.start_requests():
        print(request.url)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    from .logging_config import configure_logging
    from .scraper.scrapy_settings import build_scrapy_settings
    from .scraper.spiders.wrc_decisions import WrcDecisionsSpider

    cfg = get_settings()
    # Before CrawlerProcess: Scrapy takes the root logger during construction,
    # and we want the JSON formatter to own it.
    configure_logging(
        args.log_level or cfg.logging.level,
        log_file=cfg.logging.file,
        console=cfg.logging.console,
    )

    spider_kwargs = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "bodies": args.bodies,
        "partition_size": args.partition_size,
    }

    if args.dry_run:
        return dry_run(WrcDecisionsSpider(config=cfg, **spider_kwargs))

    process = CrawlerProcess(build_scrapy_settings(cfg))
    crawler = process.create_crawler(WrcDecisionsSpider)

    # The spider's counters are the run's verdict, and the spider object is gone
    # once the process finishes -- so capture them as it closes.
    captured: dict[Any, Any] = {}
    planned: list[int] = []

    def _capture(spider: Any, reason: str) -> None:
        captured.update(spider.counters)
        planned.append(spider.planned_requests)

    crawler.signals.connect(_capture, signal=signals.spider_closed)

    process.crawl(crawler, **spider_kwargs)
    process.start()

    totals = totals_of(captured)

    # Completeness of the STORE decides the exit code, not what this pass saw.
    # The source's page windows overlap non-deterministically, so a pass can miss
    # records a previous pass already stored; exiting non-zero on that would make
    # a complete partition fail forever. Measured here rather than trusted from
    # the counters, because the pipeline may not have run at all (--dry-run, or a
    # crawl that failed before opening).
    incomplete = incomplete_partitions(captured)

    # A crawl that planned requests and processed no partitions reconciles
    # trivially (0 == 0) and would otherwise exit 0. That is exactly how the
    # missing async start() hid itself on the first live run.
    if planned and planned[0] > 0 and not captured:
        log.error(
            "run.no_partitions_processed",
            extra={"planned_requests": planned[0], "partitions": 0},
        )
        return 1

    if incomplete:
        log.error(
            "run.incomplete",
            extra={**totals, "partitions_incomplete": incomplete},
        )
        return 1

    if not totals["reconciled"]:
        # The store is complete but this pass fell short -- worth knowing, not
        # worth failing. Re-running converges, and nothing is missing.
        log.warning("run.pass_incomplete", extra=totals)

    return 0


if __name__ == "__main__":
    sys.exit(main())