"""Dagster assets: ingestion and transformation as separate, dependent tasks.

Two assets over the same partition space:

    landing_documents  ->  curated_documents
"""

# NOTE: no `from __future__ import annotations` here, deliberately. It turns
# annotations into strings, and Dagster resolves the `context` parameter's type
# by identity -- with postponed evaluation it raises
# "Cannot annotate `context` parameter with type AssetExecutionContext" even
# though the annotation is correct. Found by loading the Definitions.
import subprocess
import sys
from datetime import date, datetime
from typing import Any

from dagster import (
    AssetExecutionContext,
    Backoff,
    MaterializeResult,
    MetadataValue,
    MultiPartitionsDefinition,
    RetryPolicy,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
    asset,
)

from ..logging_config import get_logger
from ..partitions import window_for
from ..settings import PartitionSize, Settings, get_settings
from ..storage import mongo
from ..transform.runner import transform_range

log = get_logger(__name__)

# The name of the time dimension. Fixed rather than named after the configured
# size, because a Dagster partition key embeds its dimension names: renaming the
# dimension when PARTITION_SIZE changes would orphan every materialisation
# recorded under the old name, on top of the key change that is already
# unavoidable (see build_time_partitions).
TIME_DIMENSION = "period"

# Weeks start on MONDAY (day_offset=1; Dagster's default 0 is Sunday), because
# partitions._key_for derives weekly keys from isocalendar() and ISO weeks are
# Monday-based. A mismatch would pair Dagster partition "2024-01-07" with an
# internal key of "2024-W01" covering a different seven days -- an off-by-one
# that surfaces only as quietly wrong counts.
WEEK_START_DAY = 1

_settings = get_settings()


BODY_PARTITIONS = StaticPartitionsDefinition([body.slug for body in _settings.scraping.bodies])
# WEEKLY, pinned. 
PARTITION_SIZE = PartitionSize.WEEKLY
TIME_PARTITIONS = WeeklyPartitionsDefinition(
    start_date=_settings.partitions.start_date.isoformat(),
    day_offset=WEEK_START_DAY,
)
DOCUMENT_PARTITIONS = MultiPartitionsDefinition(
    {"body": BODY_PARTITIONS, TIME_DIMENSION: TIME_PARTITIONS}
)


INGEST_RETRY = RetryPolicy(max_retries=2, delay=30, backoff=Backoff.LINEAR)


def _dimensions(context: AssetExecutionContext) -> tuple[str, date, date, str]:
    """Unpack a multi-partition key into (body, start, end, internal_key).
    """
    keys = context.partition_key.keys_by_dimension
    start = datetime.strptime(keys[TIME_DIMENSION], "%Y-%m-%d").date()
    window = window_for(start, PARTITION_SIZE)
    return keys["body"], window.start, window.end, window.key


def _partition_metadata(
    body_slug: str,
    partition_key: str,
    start: date,
    end: date,
    settings: Settings,
    **extra: Any,
) -> dict[str, Any]:
    """Materialisation metadata, read from the store rather than from logs.

    Parsing our own log output would couple the orchestrator to a log format and
    report what a run *said* it did. Querying MongoDB reports what is actually
    held, which is the question a partition's status should answer.
    """
    held = mongo.count_partition_records(body_slug, partition_key, settings)
    return {
        "body": body_slug,
        "partition_key": partition_key,
        "window": MetadataValue.text(f"{start.isoformat()} .. {end.isoformat()}"),
        "records_in_store": MetadataValue.int(held),
        **extra,
    }


@asset(
    partitions_def=DOCUMENT_PARTITIONS,
    retry_policy=INGEST_RETRY,
    group_name="landing",
    description=(
        "Scrape one body-week of decisions into the landing zone: metadata in "
        "MongoDB, documents in object storage under content-addressed keys."
    ),
)
def landing_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Crawl one (body, week) partition.

    Fails when the crawl exits non-zero -- which it does when the store holds
    fewer records than the site advertised -- so the retry policy does the
    recovering. Raising is right even though nothing is broken: the partition is
    incomplete, and the only way to complete it is to fetch again.
    """
    settings = get_settings()
    body_slug, start, end, partition_key = _dimensions(context)

    command = [
        sys.executable,
        "-m",
        "wrc_pipeline.cli",
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--bodies",
        body_slug,
        # The same size the grid was built from, so a partition's window and its
        # internal key agree end to end.
        "--partition-size",
        PARTITION_SIZE.value,
    ]

    context.log.info("crawling %s %s: %s", body_slug, partition_key, " ".join(command))
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    # The crawl's own JSON logs already went to logs/pipeline.jsonl; only the
    # tail is echoed here, so a failure is diagnosable from the Dagster UI
    # without opening the log file.
    if result.returncode != 0:
        context.log.error("crawl exited %s\n%s", result.returncode, result.stderr[-4000:])

    held = mongo.count_partition_records(body_slug, partition_key, settings)

    if result.returncode != 0:
        raise RuntimeError(
            f"crawl of {body_slug}/{partition_key} exited {result.returncode} "
            f"with {held} record(s) stored; see logs/pipeline.jsonl"
        )

    return MaterializeResult(
        metadata=_partition_metadata(
            body_slug,
            partition_key,
            start,
            end,
            settings,
            exit_code=result.returncode,
        )
    )


@asset(
    partitions_def=DOCUMENT_PARTITIONS,
    deps=[landing_documents],
    group_name="curated",
    description=(
        "Clean one body-week of landing documents into the curated zone: HTML "
        "reduced to its decision content, files renamed to identifier.ext, "
        "hashes recomputed."
    ),
)
def curated_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Transform one (body, week) partition.

    ``deps=[landing_documents]`` over the same partition space is what makes this
    an orchestrated pipeline rather than two scripts: Dagster maps each curated
    partition to the landing partition of the same key, so a backfill runs them
    in the right order and a stale landing partition marks its curated
    counterpart stale too.

    Runs in-process. No reactor is involved, so there is no reason to pay for a
    subprocess, and an exception here surfaces with its traceback intact.
    """
    settings = get_settings()
    body_slug, start, end, partition_key = _dimensions(context)

    counters = transform_range(start, end, body_slug, settings)
    summary = counters.as_dict()

    if not summary["reconciled"]:
        # Unlike the crawl, this compares our own collection against our own
        # processing, so a shortfall is a bug or an infrastructure fault -- never
        # the source misbehaving. Worth failing loudly.
        raise RuntimeError(f"transform of {body_slug}/{partition_key} did not reconcile: {summary}")

    return MaterializeResult(
        metadata=_partition_metadata(
            body_slug,
            partition_key,
            start,
            end,
            settings,
            records_cleaned=MetadataValue.int(summary["records_cleaned"]),
            records_passed_through=MetadataValue.int(summary["records_passed_through"]),
            records_skipped=MetadataValue.int(summary["records_skipped"]),
            records_failed=MetadataValue.int(summary["records_failed"]),
            records_flagged_thin=MetadataValue.int(summary["records_flagged_thin"]),
        )
    )