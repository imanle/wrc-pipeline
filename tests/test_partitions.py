"""Tests for the partition generator.

These are the cheapest high-value tests in the project: partitioning bugs cause
silently missing documents, which is the one failure mode a reviewer cannot see
by eyeballing output.
"""

from datetime import date

import pytest

from wrc_pipeline.partitions import Partition, generate_partitions, partition_count, window_for
from wrc_pipeline.settings import PartitionSize


def test_monthly_covers_a_full_year_without_gaps_or_overlaps():
    parts = list(generate_partitions(date(2024, 1, 1), date(2024, 12, 31), "monthly"))
    assert len(parts) == 12
    assert parts[0].start == date(2024, 1, 1)
    assert parts[-1].end == date(2024, 12, 31)
    # Each window must begin the day after the previous one ends.
    for previous, current in zip(parts, parts[1:]):
        assert (current.start - previous.end).days == 1


def test_last_window_is_clamped_to_end_date():
    parts = list(generate_partitions(date(2024, 1, 1), date(2024, 3, 15), "monthly"))
    assert [p.key for p in parts] == ["2024-01", "2024-02", "2024-03"]
    assert parts[-1].end == date(2024, 3, 15)


def test_first_window_is_clamped_to_start_date():
    parts = list(generate_partitions(date(2024, 2, 14), date(2024, 3, 31), "monthly"))
    assert parts[0].start == date(2024, 2, 14)
    assert parts[0].end == date(2024, 2, 29)  # leap year


def test_leap_day_is_included():
    parts = list(generate_partitions(date(2024, 2, 1), date(2024, 2, 29), "monthly"))
    assert len(parts) == 1
    assert parts[0].end == date(2024, 2, 29)


def test_single_day_range_yields_one_window():
    parts = list(generate_partitions(date(2024, 6, 10), date(2024, 6, 10), "daily"))
    assert len(parts) == 1
    assert parts[0].start == parts[0].end == date(2024, 6, 10)


@pytest.mark.parametrize(
    ("size", "expected_first_key", "expected_count"),
    [
        ("daily", "2024-01-01", 366),
        ("monthly", "2024-01", 12),
        ("quarterly", "2024-Q1", 4),
        ("yearly", "2024", 1),
    ],
)
def test_key_format_and_count_per_size(size, expected_first_key, expected_count):
    parts = list(generate_partitions(date(2024, 1, 1), date(2024, 12, 31), size))
    assert parts[0].key == expected_first_key
    assert len(parts) == expected_count


def test_quarterly_boundaries_align_to_calendar_quarters():
    parts = list(generate_partitions(date(2024, 1, 1), date(2024, 12, 31), "quarterly"))
    assert [(p.start.month, p.end.month) for p in parts] == [(1, 3), (4, 6), (7, 9), (10, 12)]


def test_weekly_window_is_seven_days():
    parts = list(generate_partitions(date(2024, 1, 1), date(2024, 1, 21), "weekly"))
    assert all((p.end - p.start).days == 6 for p in parts)
    assert len(parts) == 3


def test_reversed_range_raises():
    with pytest.raises(ValueError, match="after end_date"):
        list(generate_partitions(date(2024, 5, 1), date(2024, 4, 1), "monthly"))


def test_partition_metadata_is_serialisable():
    part = next(iter(generate_partitions(date(2024, 3, 1), date(2024, 3, 31), "monthly")))
    assert part.as_dict() == {
        "partition_key": "2024-03",
        "partition_date": "2024-03-01",
        "partition_start": "2024-03-01",
        "partition_end": "2024-03-31",
        "partition_size": "monthly",
    }


def test_partition_is_frozen_and_hashable():
    part = Partition(date(2024, 1, 1), date(2024, 1, 31), "2024-01", PartitionSize.MONTHLY)
    assert {part}  # hashable -> safe as a dict key for per-partition counters
    with pytest.raises(Exception):
        part.start = date(2024, 2, 1)  # type: ignore[misc]


def test_partition_count_matches_generator():
    assert partition_count(date(2020, 1, 1), date(2025, 1, 1), "yearly") == 6


# --------------------------------------------------------------------------- #
# window_for -- the single window the orchestrator asks about
# --------------------------------------------------------------------------- #
def test_window_for_matches_generate_partitions_for_every_size():
    """The orchestrator is handed a partition-start date and derives the window
    and key from this function; the spider derives the same key from
    generate_partitions. Two implementations would eventually disagree by a day,
    so they must be the same code path."""
    for size in PartitionSize:
        for partition in generate_partitions(date(2024, 1, 1), date(2025, 12, 31), size):
            single = window_for(partition.start, size)
            assert single.key == partition.key
            assert single.start == partition.start


def test_window_for_is_not_clamped_to_a_caller_range():
    """A weekly window starting 29 January genuinely ends 4 February, even
    though a request for "January" clamps it to the 31st. The orchestrator wants
    the true window -- its partitions are not bounded by a month."""
    clamped = list(generate_partitions(date(2024, 1, 29), date(2024, 1, 31), "weekly"))[0]
    assert clamped.end == date(2024, 1, 31)
    assert window_for(date(2024, 1, 29), "weekly").end == date(2024, 2, 4)


def test_window_for_accepts_a_size_string():
    assert window_for(date(2024, 1, 1), "monthly").key == "2024-01"