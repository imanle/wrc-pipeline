"""Tests for PartitionCounters reconciliation.

Three numbers describe a partition and they are all different:

  * ``listings``  -- what the site's "of N results" advertised
  * ``entries``   -- listing entries actually served across the pages fetched
  * ``seen``      -- distinct references those entries represent

The source's page windows overlap, so entries > distinct routinely. Conflating
any two of these produces a reconciliation check that either cries wolf or, worse,
stays silent while records go missing.
"""

from __future__ import annotations

from wrc_pipeline.logging_config import PartitionCounters


def _counters(listings: int, entries: int, refs: list[str]) -> PartitionCounters:
    counters = PartitionCounters("wrc", "2024-W05")
    counters.listings = listings
    counters.entries = entries
    for ref in refs:
        counters.record_seen(ref)
    return counters


def test_clean_partition_reconciles():
    counters = _counters(3, 3, ["ADJ-1", "ADJ-2", "ADJ-3"])
    counters.scraped = 3
    summary = counters.as_dict()

    assert summary["duplicate_listings"] == 0
    assert summary["reconciled"] is True


def test_overlapping_page_windows_are_loss_not_bookkeeping():
    """Both halves of this were measured on the live site, same date range
    (WRC 2024-01-29..31, stated total 46):

      run A, sequential with a 0.5s delay: 46 entries served, 6 of them repeats
                                           of page 1 on page 2 -> 40 distinct
      run B, normal crawl:                 46 entries served, 46 distinct

    Run B proves 46 distinct decisions exist, so run A's duplicates displaced 6
    real ones. Overlap is the MECHANISM of loss, not an artefact of how the site
    counts -- which is why this must fail rather than be explained away.
    """
    refs = [f"ADJ-{i}" for i in range(40)]
    counters = _counters(listings=46, entries=46, refs=refs)
    counters.scraped = 40
    summary = counters.as_dict()

    assert summary["listings_found"] == 46
    assert summary["records_distinct"] == 40
    assert summary["duplicate_listings"] == 6  # explains the shortfall
    assert summary["listings_unaccounted"] == 6  # and it IS a shortfall
    assert summary["records_reconciled"] is True  # storage did its job
    assert summary["listings_reconciled"] is False  # fetching did not
    assert summary["reconciled"] is False


def test_a_clean_fetch_of_the_same_partition_reconciles():
    """Run B above: the same 46, all distinct. The check must not fire on a good
    run, or a false alarm every time would make it worthless."""
    counters = _counters(46, 46, [f"ADJ-{i}" for i in range(46)])
    counters.scraped = 46
    summary = counters.as_dict()

    assert summary["duplicate_listings"] == 0
    assert summary["listings_unaccounted"] == 0
    assert summary["reconciled"] is True


def test_entries_never_served_also_fail_the_listings_check():
    """The other route to a shortfall: the site advertises 50 and serves 46, with
    no duplicates. Distinguished from overlap by duplicate_listings being 0."""
    counters = _counters(listings=50, entries=46, refs=[f"ADJ-{i}" for i in range(46)])
    counters.scraped = 46
    summary = counters.as_dict()

    assert summary["duplicate_listings"] == 0
    assert summary["listings_unaccounted"] == 4
    assert summary["listings_reconciled"] is False
    assert summary["records_reconciled"] is True


def test_parsed_but_never_stored_fails_the_records_check():
    """Points at storage: decisions we found and then lost."""
    counters = _counters(listings=10, entries=10, refs=[f"ADJ-{i}" for i in range(10)])
    counters.scraped = 8
    summary = counters.as_dict()

    assert summary["records_unaccounted"] == 2
    assert summary["records_reconciled"] is False
    assert summary["listings_reconciled"] is True  # fetching was fine
    assert summary["reconciled"] is False


def test_failures_count_towards_the_records_check():
    """A logged failure is an accounted-for record, not a missing one."""
    counters = _counters(listings=10, entries=10, refs=[f"ADJ-{i}" for i in range(10)])
    counters.scraped = 8
    counters.failed = 2
    assert counters.as_dict()["records_reconciled"] is True


def test_unreadable_entries_are_not_counted_as_duplicates():
    """An entry with no readable reference is served but not distinct. Counting it
    as overlap would hide a broken selector behind a benign-looking number."""
    counters = _counters(listings=10, entries=10, refs=[f"ADJ-{i}" for i in range(9)])
    counters.unidentified = 1
    counters.scraped = 9
    summary = counters.as_dict()

    assert summary["duplicate_listings"] == 0
    assert summary["listings_unidentified"] == 1


def test_record_failure_registers_the_reference_as_seen():
    """Otherwise a failed record would look like one that was never found."""
    counters = PartitionCounters("wrc", "2024-W05")
    counters.record_failure("https://x.ie/a.html", "document_http_error", 404, "ADJ-1")
    assert counters.seen == {"ADJ-1"}
    assert counters.failed == 1


def test_found_is_an_alias_for_listings():
    """The spider and CLI were written against `found`; the attribute is named
    for what the number actually is."""
    counters = PartitionCounters("wrc", "2024-W05")
    counters.found = 46
    assert counters.listings == 46
    assert counters.found == 46