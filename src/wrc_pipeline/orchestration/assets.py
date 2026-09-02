"""Dagster assets: ingestion and transformation as separate, dependent tasks.

Two assets over the same partition space:

    landing_documents  ->  curated_documents

Partitioning
------------
``MultiPartitionsDefinition`` of (body x period), where the period size comes
from ``partitions.size`` in config. That is not a presentation
choice -- it is the pipeline's actual unit of work, and has been since the
spider was written: ``PartitionCounters`` is keyed on ``(body, partition_key)``,
object keys embed both, and a single one can be re-run in isolation. Expressing
it natively means the Dagster UI shows a grid where one cell is one body-week,
and a backfill or a retry can target exactly that cell.

The alternative -- one partition per week, all four bodies inside it -- would
make a single body's failure fail the week, and a retry would re-crawl the three
bodies that were fine.

Why ingestion runs in a subprocess
----------------------------------
Twisted's reactor cannot be restarted inside a process it has already run in.
Dagster materialises several partitions in one process (a backfill, or two
partitions in one run), so the second ``CrawlerProcess`` would fail with
``ReactorNotRestartable``. Shelling out to ``python -m wrc_pipeline.cli`` gives
each crawl a fresh interpreter.

That is also why the CLI is not redundant with the orchestrator: it is the
process boundary Dagster drives, and both entry points sit on the same core
(``build_scrapy_settings`` + ``WrcDecisionsSpider``).

The transformation has no reactor involved, so it runs in-process -- no
subprocess, no serialisation, and exceptions surface directly.

Retries and the source's pagination
-----------------------------------
Ingestion carries ``RetryPolicy(max_retries=2)`` and FAILS a partition whose
store is incomplete. Both follow from a measured property of this source: its
search pages serve overlapping windows non-deterministically, so a single pass
can miss records that a second pass picks up (see ARCHITECTURE.md). Because the
pipeline is idempotent, a retry re-fetches cheaply and only the missing
documents cost anything -- so Dagster converges on a complete partition by
itself, which is precisely what an orchestrator should be doing here.
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
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    RetryPolicy,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
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


def build_time_partitions(
    size: PartitionSize,
    start_date: date,
) -> TimeWindowPartitionsDefinition:
    """The Dagster time-partition definition matching a configured size.

    Driven by ``partitions.size`` so ``config.yaml`` is genuinely authoritative.
    Without this the config claimed to control partition size while the
    orchestrator was hardcoded to weekly -- the sort of inconsistency that makes
    every other config value suspect.

    Note what changing this costs: Dagster stores materialisation state per
    partition KEY, and the keys differ by size ("2024-01-29" weekly versus
    "2024-01-01" monthly). Switching size therefore discards the history of the
    old granularity rather than converting it. That is why this is a
    deployment-time setting read once at load, not a per-run parameter -- and
    why the CLI keeps its own ``--partition-size`` flag for ad-hoc runs.

    Quarterly and yearly have no dedicated Dagster class, so they are built from
    cron expressions; the resulting keys are still the window's first day, which
    is what ``window_for`` expects.
    """
    start = start_date.isoformat()

    if size is PartitionSize.DAILY:
        return DailyPartitionsDefinition(start_date=start)
    if size is PartitionSize.WEEKLY:
        return WeeklyPartitionsDefinition(start_date=start, day_offset=WEEK_START_DAY)
    if size is PartitionSize.MONTHLY:
        return MonthlyPartitionsDefinition(start_date=start)
    if size is PartitionSize.QUARTERLY:
        return TimeWindowPartitionsDefinition(
            cron_schedule="0 0 1 1,4,7,10 *", start=start, fmt="%Y-%m-%d"
        )
    if size is PartitionSize.YEARLY:
        return TimeWindowPartitionsDefinition(
            cron_schedule="0 0 1 1 *", start=start, fmt="%Y-%m-%d"
        )

    raise ValueError(f"unhandled partition size: {size!r}")


BODY_PARTITIONS = StaticPartitionsDefinition([body.slug for body in _settings.scraping.bodies])
TIME_PARTITIONS = build_time_partitions(_settings.partitions.size, _settings.partitions.start_date)
DOCUMENT_PARTITIONS = MultiPartitionsDefinition(
    {"body": BODY_PARTITIONS, TIME_DIMENSION: TIME_PARTITIONS}
)

# Two attempts after the first, with backoff. Tuned to the failure it exists for:
# a pass that lost records to page-window overlap, which a re-fetch usually
# fixes. Not a substitute for the failure ledger -- a 404 stays a 404.
INGEST_RETRY = RetryPolicy(max_retries=2, delay=30, backoff=Backoff.LINEAR)


def _dimensions(
    context: AssetExecutionContext,
    settings: Settings | None = None,
) -> tuple[str, date, date, str]:
    """Unpack a multi-partition key into (body, start, end, internal_key).

    The time dimension gives the window's first day; its end and the pipeline's
    own key both come from ``partitions.window_for`` at the configured size.
    Deriving them there rather than here is deliberate -- the spider computes the
    same key from the same function, and two implementations of "which week is
    this" would eventually disagree by a day.
    """
    cfg = settings or get_settings()
    keys = context.partition_key.keys_by_dimension
    start = datetime.strptime(keys[TIME_DIMENSION], "%Y-%m-%d").date()
    window = window_for(start, cfg.partitions.size)
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
    body_slug, start, end, partition_key = _dimensions(context, settings)

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
        # The crawl is told the same size the Dagster grid was built from, so a
        # partition's window and its internal key agree end to end.
        "--partition-size",
        settings.partitions.size.value,
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
    body_slug, start, end, partition_key = _dimensions(context, settings)

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