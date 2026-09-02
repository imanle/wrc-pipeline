"""Date-range partitioning.

The spine of requirements 2 and 3: the crawl walks ``[start_date, end_date]`` in
fixed-size windows, and every scraped record carries the window it came from.

Deliberately free of I/O and of any Scrapy/Dagster imports so it can be unit
tested in milliseconds and reused verbatim by both the spider and the
orchestrator's partition definitions.

Conventions
-----------
* Windows are **inclusive on both ends**, because the site's Start/Finish Date
  filters are inclusive. Half-open windows here would silently drop one day's
  decisions per partition.
* Windows are clamped to the requested range, so a monthly walk starting on the
  14th yields a short first window rather than reaching back to the 1st.
* ``partition_key`` is a sortable, filename-safe label; ``partition_date`` is the
  window's start and is what gets written to Mongo.
"""

from __future__ import annotations

import calendar
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

from .settings import PartitionSize

_ONE_DAY = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class Partition:
    """One inclusive date window of work."""

    start: date
    end: date
    key: str
    size: PartitionSize

    @property
    def partition_date(self) -> date:
        """Canonical date stamped onto every record from this window."""
        return self.start

    def as_dict(self) -> dict[str, str]:
        return {
            "partition_key": self.key,
            "partition_date": self.start.isoformat(),
            "partition_start": self.start.isoformat(),
            "partition_end": self.end.isoformat(),
            "partition_size": self.size.value,
        }

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return f"{self.key} [{self.start} .. {self.end}]"


# --------------------------------------------------------------------------- #
# Window boundaries
# --------------------------------------------------------------------------- #
def _end_of_window(current: date, size: PartitionSize) -> date:
    """Last day of the window that *current* opens, ignoring the range end."""
    if size is PartitionSize.DAILY:
        return current

    if size is PartitionSize.WEEKLY:
        return current + timedelta(days=6)

    if size is PartitionSize.MONTHLY:
        last_day = calendar.monthrange(current.year, current.month)[1]
        return date(current.year, current.month, last_day)

    if size is PartitionSize.QUARTERLY:
        # Quarter containing `current`: months 1-3, 4-6, 7-9, 10-12.
        last_month = 3 * ((current.month - 1) // 3) + 3
        last_day = calendar.monthrange(current.year, last_month)[1]
        return date(current.year, last_month, last_day)

    if size is PartitionSize.YEARLY:
        return date(current.year, 12, 31)

    raise ValueError(f"unhandled partition size: {size!r}")


def _key_for(window_start: date, size: PartitionSize) -> str:
    """Sortable, filename-safe label for a window."""
    if size is PartitionSize.DAILY:
        return window_start.isoformat()
    if size is PartitionSize.WEEKLY:
        iso_year, iso_week, _ = window_start.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    if size is PartitionSize.MONTHLY:
        return f"{window_start.year}-{window_start.month:02d}"
    if size is PartitionSize.QUARTERLY:
        return f"{window_start.year}-Q{(window_start.month - 1) // 3 + 1}"
    if size is PartitionSize.YEARLY:
        return str(window_start.year)
    raise ValueError(f"unhandled partition size: {size!r}")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def generate_partitions(
    start_date: date,
    end_date: date,
    size: PartitionSize | str = PartitionSize.MONTHLY,
) -> Iterator[Partition]:
    """Yield inclusive windows covering ``[start_date, end_date]``.

    Examples
    --------
    >>> parts = list(generate_partitions(date(2024, 1, 1), date(2024, 3, 15), "monthly"))
    >>> [p.key for p in parts]
    ['2024-01', '2024-02', '2024-03']
    >>> parts[-1].end
    datetime.date(2024, 3, 15)
    """
    if isinstance(size, str):
        size = PartitionSize(size)
    if start_date > end_date:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    cursor = start_date
    while cursor <= end_date:
        raw_end = _end_of_window(cursor, size)
        # Clamp so we never query beyond what the caller asked for.
        window_end = min(raw_end, end_date)
        yield Partition(
            start=cursor,
            end=window_end,
            key=_key_for(cursor, size),
            size=size,
        )
        cursor = raw_end + _ONE_DAY


def window_for(
    window_start: date,
    size: PartitionSize | str = PartitionSize.MONTHLY,
) -> Partition:
    """The single window beginning at *window_start*, at the given size.

    Public because the Dagster layer needs exactly this: it is handed a
    partition-start date and must derive the window's end and the pipeline's own
    key. Doing that arithmetic there would duplicate ``_end_of_window`` and
    ``_key_for``, and the copies would drift -- an ISO-week key paired with a
    seven-day window computed a different way is an off-by-one waiting to
    happen.

    Unlike :func:`generate_partitions`, the end is NOT clamped to a caller's
    range: a weekly window starting 29 January genuinely ends 4 February, even
    though a request for "January" would have stopped at the 31st.
    """
    if isinstance(size, str):
        size = PartitionSize(size)
    return Partition(
        start=window_start,
        end=_end_of_window(window_start, size),
        key=_key_for(window_start, size),
        size=size,
    )


def partition_count(
    start_date: date,
    end_date: date,
    size: PartitionSize | str = PartitionSize.MONTHLY,
) -> int:
    """Number of windows the range produces. Used for progress logging."""
    return sum(1 for _ in generate_partitions(start_date, end_date, size))
