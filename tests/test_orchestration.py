"""Tests for the Dagster orchestration layer.

The assets are thin -- unpack a partition key, shell out or call the
transformation, report metadata -- so these tests target exactly that: does the
partition key map to the right date window and the right internal partition key,
does a failing crawl fail the asset, and is the dependency wired so a backfill
runs the two stages in order.

Neither asset's real work runs here: ``subprocess.run`` and ``transform_range``
are replaced. Their behaviour is covered by test_cli.py, test_spider.py and
test_transform_runner.py.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest
from dagster import (
    AssetKey,
    MultiPartitionKey,
    RetryPolicy,
    build_asset_context,
)

from wrc_pipeline.orchestration import assets as orchestration
from wrc_pipeline.orchestration.assets import (
    BODY_PARTITIONS,
    DOCUMENT_PARTITIONS,
    TIME_DIMENSION,
    TIME_PARTITIONS,
    _dimensions,
    build_time_partitions,
    curated_documents,
    landing_documents,
)
from wrc_pipeline.partitions import window_for
from wrc_pipeline.settings import PartitionSize, load_settings


class FakeCompleted:
    def __init__(self, returncode: int = 0, stderr: str = "") -> None:
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = ""


class FakeCounters:
    def __init__(self, summary: dict[str, Any]) -> None:
        self._summary = summary

    def as_dict(self) -> dict[str, Any]:
        return self._summary


def _summary(**overrides: Any) -> dict[str, Any]:
    base = {
        "records_found": 46,
        "records_cleaned": 46,
        "records_passed_through": 0,
        "records_skipped": 0,
        "records_failed": 0,
        "records_flagged_thin": 0,
        "reconciled": True,
    }
    base.update(overrides)
    return base


def _context(body: str = "wrc", period: str = "2024-01-29"):
    return build_asset_context(
        partition_key=MultiPartitionKey({"body": body, TIME_DIMENSION: period})
    )


@pytest.fixture(autouse=True)
def _stub_mongo(monkeypatch):
    """Every asset reads stored counts for its metadata; no database here."""
    monkeypatch.setattr(orchestration.mongo, "count_partition_records", lambda *a, **k: 46)


# --------------------------------------------------------------------------- #
# Partition definitions
# --------------------------------------------------------------------------- #
def test_partitions_are_body_by_period():
    """The pipeline's unit of work has always been (body, partition): counters
    are keyed on it and object keys embed it. Expressing that natively is what
    lets a single body-period be retried or backfilled on its own."""
    assert [d.name for d in DOCUMENT_PARTITIONS.partitions_defs] == ["body", TIME_DIMENSION]


def test_the_time_dimension_matches_the_configured_size():
    """config.yaml is authoritative. Before this, it claimed to control the
    partition size while the orchestrator was hardcoded to weekly."""
    expected = {
        PartitionSize.DAILY: "DailyPartitionsDefinition",
        PartitionSize.WEEKLY: "WeeklyPartitionsDefinition",
        PartitionSize.MONTHLY: "MonthlyPartitionsDefinition",
        PartitionSize.QUARTERLY: "TimeWindowPartitionsDefinition",
        PartitionSize.YEARLY: "TimeWindowPartitionsDefinition",
    }[load_settings().partitions.size]
    assert type(TIME_PARTITIONS).__name__ == expected


@pytest.mark.parametrize("size", list(PartitionSize))
def test_every_configured_size_builds_a_definition(size):
    """All five sizes the config accepts must actually work in the orchestrator,
    not just the one currently selected."""
    assert build_time_partitions(size, date(2024, 1, 1)) is not None


@pytest.mark.parametrize("size", list(PartitionSize))
def test_dagster_keys_round_trip_through_window_for(size):
    """Every Dagster partition key must be a window start the pipeline
    recognises. If they disagreed, the metadata query would count a partition
    that does not exist -- and would report zero rather than raising."""
    definition = build_time_partitions(size, date(2024, 1, 1))
    keys = definition.get_partition_keys(current_time=datetime(2026, 1, 1))
    assert keys, f"{size.value} produced no partitions"

    for key in keys[:6]:
        start = date.fromisoformat(key)
        assert window_for(start, size).start == start


def test_every_configured_body_is_a_partition():
    """Read from config, so adding a body to config.yaml adds a partition
    dimension value with no code change."""
    configured = {body.slug for body in load_settings().scraping.bodies}
    assert set(BODY_PARTITIONS.get_partition_keys()) == configured
    assert len(configured) == 4


def test_weeks_start_on_monday():
    """Dagster's default week offset is Sunday, but partitions._key_for derives
    weekly keys from isocalendar() and ISO weeks are Monday-based. A mismatch
    would pair Dagster partition '2024-01-07' with internal key '2024-W01'
    covering different days -- an off-by-one that shows up only as quietly wrong
    counts."""
    weekly = build_time_partitions(PartitionSize.WEEKLY, date(2024, 1, 1))
    for key in weekly.get_partition_keys(current_time=datetime(2024, 2, 15))[:10]:
        assert date.fromisoformat(key).weekday() == 0, f"{key} is not a Monday"


# --------------------------------------------------------------------------- #
# Key mapping
# --------------------------------------------------------------------------- #
def test_dimensions_unpacks_body_window_and_internal_key():
    body, start, end, key = _dimensions(_context(body="labour-court", period="2024-01-08"))
    assert body == "labour-court"
    assert start == date(2024, 1, 8)
    assert end == date(2024, 1, 14)  # inclusive, and at the CONFIGURED size
    assert key == "2024-W02"


def test_dimensions_follows_the_configured_size(monkeypatch):
    """The window end and the internal key both come from the size in config, so
    a monthly deployment gets a month-long window, not seven days."""
    settings = load_settings()
    monthly = settings.model_copy(
        update={
            "partitions": settings.partitions.model_copy(update={"size": PartitionSize.MONTHLY})
        }
    )
    _, start, end, key = _dimensions(_context(period="2024-01-01"), monthly)

    assert (start, end) == (date(2024, 1, 1), date(2024, 1, 31))
    assert key == "2024-01"


def test_internal_keys_agree_with_generate_partitions():
    """The orchestrator and the spider must label the same window identically.
    Compared against the real function rather than hardcoded strings, since that
    function is the one the spider uses."""
    from wrc_pipeline.partitions import generate_partitions

    for partition in generate_partitions(date(2024, 1, 1), date(2024, 3, 31), PartitionSize.WEEKLY):
        assert window_for(partition.start, PartitionSize.WEEKLY).key == partition.key


# --------------------------------------------------------------------------- #
# Ingestion asset
# --------------------------------------------------------------------------- #
def test_ingest_shells_out_with_the_partition_window(monkeypatch):
    """Runs in a subprocess because Twisted's reactor cannot be restarted, and
    Dagster materialises several partitions per process."""
    captured: dict[str, Any] = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        return FakeCompleted()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    landing_documents(_context(body="wrc", period="2024-01-29"))

    command = captured["command"]
    assert "wrc_pipeline.cli" in command
    assert "--start-date" in command and "2024-01-29" in command
    assert "--end-date" in command and "2024-02-04" in command
    assert "--bodies" in command and "wrc" in command
    # The crawl is told the same size the grid was built from, so the window and
    # the internal key agree end to end.
    assert load_settings().partitions.size.value in command


def test_ingest_reports_stored_records_as_metadata(monkeypatch):
    """Read from MongoDB, not parsed from our own logs: the status should reflect
    what is held, not what a run claimed to do."""
    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: FakeCompleted())
    result = landing_documents(_context())

    assert result.metadata["records_in_store"].value == 46
    assert result.metadata["partition_key"] == "2024-W05"


def test_ingest_fails_when_the_crawl_exits_non_zero(monkeypatch):
    """The CLI exits 1 when the store is short for a partition, so the retry
    policy gets a chance to fill it in -- which works because the pipeline is
    idempotent and a re-fetch only costs the missing documents."""
    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: FakeCompleted(1, "boom"))

    with pytest.raises(RuntimeError, match="exited 1"):
        landing_documents(_context())


def test_ingest_error_names_the_partition_and_the_stored_count(monkeypatch):
    """A failure message that says which body-week and how much landed is
    diagnosable from the Dagster UI without opening the log file."""
    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: FakeCompleted(1, "boom"))

    with pytest.raises(RuntimeError) as excinfo:
        landing_documents(_context(body="labour-court", period="2024-01-08"))

    message = str(excinfo.value)
    assert "labour-court/2024-W02" in message
    assert "46 record" in message


def test_ingest_has_a_retry_policy():
    """Present specifically because this source's pagination is
    non-deterministic; a retry usually recovers what one pass missed."""
    policy = landing_documents.op.retry_policy
    assert isinstance(policy, RetryPolicy)
    assert policy.max_retries == 2


# --------------------------------------------------------------------------- #
# Transformation asset
# --------------------------------------------------------------------------- #
def test_transform_runs_in_process_over_the_partition_window(monkeypatch):
    """No reactor involved, so no subprocess -- and an exception keeps its
    traceback."""
    captured: dict[str, Any] = {}

    def fake_transform(start, end, body, settings):
        captured.update(start=start, end=end, body=body)
        return FakeCounters(_summary())

    monkeypatch.setattr(orchestration, "transform_range", fake_transform)
    curated_documents(_context(body="wrc", period="2024-01-08"))

    assert captured == {"start": date(2024, 1, 8), "end": date(2024, 1, 14), "body": "wrc"}


def test_transform_reports_its_counts_as_metadata(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "transform_range",
        lambda *a, **k: FakeCounters(_summary(records_cleaned=40, records_skipped=6)),
    )
    result = curated_documents(_context())

    assert result.metadata["records_cleaned"].value == 40
    assert result.metadata["records_skipped"].value == 6


def test_transform_fails_when_it_does_not_reconcile(monkeypatch):
    """This check compares our own collection against our own processing, so a
    shortfall is a bug or an infrastructure fault -- never the source
    misbehaving. No retry would help, hence no retry policy here."""
    monkeypatch.setattr(
        orchestration,
        "transform_range",
        lambda *a, **k: FakeCounters(
            _summary(reconciled=False, records_found=46, records_cleaned=40)
        ),
    )

    with pytest.raises(RuntimeError, match="did not reconcile"):
        curated_documents(_context())


def test_transform_has_no_retry_policy():
    """Deliberate asymmetry with ingestion: retrying a deterministic
    transformation of unchanged bytes produces the same result."""
    assert curated_documents.op.retry_policy is None


# --------------------------------------------------------------------------- #
# Wiring
# --------------------------------------------------------------------------- #
def test_curated_depends_on_landing():
    """What makes this an orchestrated pipeline rather than two scripts: Dagster
    maps each curated partition to the landing partition of the same key, so a
    backfill runs them in order and a stale landing partition marks its curated
    counterpart stale."""
    assert (
        AssetKey(["landing_documents"])
        in curated_documents.asset_deps[AssetKey(["curated_documents"])]
    )


def test_both_assets_share_the_partition_space():
    """Required for the partition-to-partition mapping above to exist at all."""
    assert landing_documents.partitions_def == curated_documents.partitions_def


def test_definitions_load():
    """A syntactically valid asset graph that Dagster refuses to load is a
    failure mode no other test here would catch."""
    from wrc_pipeline.orchestration.definitions import defs, ingest_and_transform

    assert ingest_and_transform.name == "ingest_and_transform"
    assert defs is not None