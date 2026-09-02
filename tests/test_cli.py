"""Tests for the CLI. No network -- argument handling and the exit-code rule."""

from __future__ import annotations

import logging

import pytest

from wrc_pipeline.cli import build_parser, dry_run, main, totals_of
from wrc_pipeline.logging_config import PartitionCounters
from wrc_pipeline.scraper.spiders.wrc_decisions import WrcDecisionsSpider, partition_size_value
from wrc_pipeline.settings import PartitionSize, load_settings


@pytest.fixture(autouse=True)
def _restore_logging():
    """Undo any logging setup a test performs.

    main() calls configure_logging(console=True), which attaches a handler to
    pytest's captured stdout. That handler outlives the test, so every later log
    line writes to a closed stream and pytest prints an unrelated
    "I/O operation on closed file" traceback next to whatever actually failed.
    """
    root = logging.getLogger()
    before = list(root.handlers)
    yield
    for handler in list(root.handlers):
        if handler not in before:
            root.removeHandler(handler)
            handler.close()


def _counter(
    found=0, scraped=0, skipped=0, failed=0, distinct=None, listings_seen=None
) -> PartitionCounters:
    """Build a counter for testing.

    *found* is the site's stated listing total. *distinct* is how many distinct
    references were parsed (defaults to the number of operations, the
    well-behaved case). *listings_seen* models entries actually encountered when
    that differs from *found* -- i.e. the site advertised more than it served.
    """
    counter = PartitionCounters("wrc", "2024-01")
    counter.listings, counter.scraped, counter.skipped, counter.failed = (
        found,
        scraped,
        skipped,
        failed,
    )
    seen = scraped + skipped + failed if distinct is None else distinct
    for i in range(seen):
        counter.record_seen(f"ADJ-{i:05d}")
    counter.entries = seen if listings_seen is None else listings_seen
    return counter


# --------------------------------------------------------------------------- #
# Arguments
# --------------------------------------------------------------------------- #
def test_defaults_come_from_config():
    """Bare `python -m wrc_pipeline.cli` must be runnable."""
    cfg = load_settings()
    args = build_parser().parse_args([])
    assert args.start_date == cfg.partitions.start_date.isoformat()
    assert args.end_date == cfg.partitions.end_date.isoformat()
    assert args.bodies is None


def test_arguments_override_config():
    args = build_parser().parse_args(
        ["--start-date", "2024-03-01", "--end-date", "2024-03-31", "--bodies", "wrc"]
    )
    assert args.start_date == "2024-03-01"
    assert args.bodies == "wrc"


def test_invalid_partition_size_is_rejected():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--partition-size", "fortnightly"])


# --------------------------------------------------------------------------- #
# Totals and exit code
# --------------------------------------------------------------------------- #
def test_totals_sum_across_partitions():
    counters = {
        ("wrc", "2024-01"): _counter(found=10, scraped=10),
        ("wrc", "2024-02"): _counter(found=5, skipped=5),
    }
    totals = totals_of(counters)
    assert totals["listings"] == 15
    assert totals["scraped"] == 10
    assert totals["skipped"] == 5
    assert totals["reconciled"] is True


def test_failures_still_reconcile():
    """A logged failure is accounted for; it does not make the run unreconciled."""
    totals = totals_of({("wrc", "2024-01"): _counter(found=10, scraped=8, failed=2)})
    assert totals["reconciled"] is True


def test_a_decision_found_but_never_stored_fails_the_records_check():
    """We parsed 10 distinct decisions and only resolved 8. Real loss."""
    totals = totals_of({("wrc", "2024-01"): _counter(found=10, scraped=8, distinct=10)})
    assert totals["records_unaccounted"] == 2
    assert totals["reconciled"] is False


def test_overlapping_listings_are_reported_as_loss():
    """A second run of the same range returned all 46 distinct, proving the 6
    duplicates in the first run displaced 6 real decisions. The exit code must
    reflect that."""
    counter = _counter(found=46, scraped=40, distinct=40, listings_seen=46)
    totals = totals_of({("wrc", "2024-01"): counter})

    assert totals["listings"] == 46
    assert totals["distinct"] == 40
    assert totals["duplicate_listings"] == 6
    assert totals["listings_unaccounted"] == 6
    assert totals["reconciled"] is False


def test_incomplete_partitions_asks_the_store_not_the_run(monkeypatch):
    """A pass that saw 226 of 234 is not a failure if the store holds all 234."""
    from wrc_pipeline import cli
    from wrc_pipeline.storage import mongo

    monkeypatch.setattr(mongo, "count_partition_records", lambda *a, **k: 234)
    counters = {("wrc", "2024-01"): _counter(found=234, scraped=226, distinct=226, listings_seen=234)}
    assert cli.incomplete_partitions(counters) == []


def test_incomplete_partitions_reports_a_short_store(monkeypatch):
    from wrc_pipeline import cli
    from wrc_pipeline.storage import mongo

    monkeypatch.setattr(mongo, "count_partition_records", lambda *a, **k: 200)
    counters = {("wrc", "2024-01"): _counter(found=234, scraped=200, distinct=200, listings_seen=234)}
    assert cli.incomplete_partitions(counters) == ["wrc/2024-01"]


def test_a_clean_partition_exits_zero():
    counter = _counter(found=46, scraped=46, distinct=46, listings_seen=46)
    totals = totals_of({("wrc", "2024-01"): counter})

    assert totals["duplicate_listings"] == 0
    assert totals["listings_unaccounted"] == 0
    assert totals["reconciled"] is True


def test_empty_run_reconciles():
    assert totals_of({})["reconciled"] is True


# --------------------------------------------------------------------------- #
# Dry run
# --------------------------------------------------------------------------- #
def test_dry_run_prints_urls_and_fetches_nothing(capsys):
    # Monthly pinned: this test is about dry-run output, not about which
    # partition size config currently defaults to.
    spider = WrcDecisionsSpider(
        config=load_settings(),
        start_date="2024-01-01",
        end_date="2024-02-29",
        bodies="wrc",
        partition_size="monthly",
    )
    assert dry_run(spider) == 0

    printed = capsys.readouterr().out.strip().splitlines()
    assert len(printed) == 2
    assert "from=1/1/2024" in printed[0]
    assert "from=1/2/2024" in printed[1]


def test_dry_run_via_main(capsys):
    code = main(
        [
            "--dry-run",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-31",
            "--bodies",
            "wrc",
            "--partition-size",
            "monthly",
        ]
    )
    assert code == 0
    assert "workplacerelations.ie" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# The enum stringification bug
# --------------------------------------------------------------------------- #
def test_partition_size_value_unwraps_the_enum():
    """str() on a (str, Enum) member gives 'PartitionSize.MONTHLY' -- a Python
    repr that would otherwise leak into the logs and the runs collection."""
    assert partition_size_value(PartitionSize.MONTHLY) == "monthly"


def test_partition_size_value_passes_strings_through():
    """The CLI supplies a plain string."""
    assert partition_size_value("weekly") == "weekly"