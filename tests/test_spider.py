"""Tests for the listing spider.

No network, no MongoDB, no MinIO. Every test drives the spider with a fabricated
``HtmlResponse`` whose markup mirrors what recon confirmed on the live site,
including the awkward cases: literal spaces in hrefs, the identifier appearing
three times, and a displayed date that falls outside its own partition.

The fabricated markup is the honest limitation here -- it proves the spider
handles the DOM as documented, not that the documentation is still accurate. A
live smoke run over one month is what checks the latter.
"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest
from scrapy.http import HtmlResponse, Request

from wrc_pipeline.partitions import Partition
from wrc_pipeline.scraper.items import DocumentRecord, RecordStatus
from wrc_pipeline.scraper.spiders.wrc_decisions import (
    WrcDecisionsSpider,
    _extension_of,
    _normalise_ref,
)
from wrc_pipeline.settings import PartitionSize, load_settings

SEARCH_URL = "https://www.workplacerelations.ie/en/search/?decisions=1&from=1/1/2024&to=31/1/2024&body=15376"


@pytest.fixture(scope="module")
def cfg():
    return load_settings()


@pytest.fixture
def partition():
    return Partition(
        start=date(2024, 1, 1), end=date(2024, 1, 31), key="2024-01", size=PartitionSize.MONTHLY
    )


def _item_html(
    identifier: str = "IR - SC - 00000787",
    crosscheck: str | None = None,
    date_text: str = "31/01/2024",
    href: str = "/en/cases/2024/january/ir- sc- 00000787.html",
    description: str = "Office Administrator V A Translation Service",
) -> str:
    """One `li.each-item` block, matching the markup confirmed during recon."""
    ref = crosscheck if crosscheck is not None else identifier
    link = f'<div class="col-sm-3 link"><a href="{href}">View Page</a></div>' if href else ""
    return f"""
    <li class="each-item clearfix">
      <h2 class="title" title="{identifier}"><a href="{href}">{identifier}</a></h2>
      <span class="date">{date_text}</span>
      <p class="fullpath" title="{href}"></p>
      <p class="description" title="{description}">{description}</p>
      <div class="row bottom-ref">
        <span class="refNO">{ref}</span>
        {link}
      </div>
    </li>
    """


def _page(total: int | None = 234, items: str | None = None, url: str = SEARCH_URL) -> HtmlResponse:
    """A results page. `total=None` omits the count line entirely."""
    count_line = f"<p>Shows 1 to 10 of {total} results</p>" if total is not None else ""
    body = f"""
    <html><body>
      <h1>Decisions and Determinations</h1>
      {count_line}
      <ul class="results">{items if items is not None else _item_html()}</ul>
    </body></html>
    """
    return HtmlResponse(
        url=url, body=body.encode("utf-8"), encoding="utf-8", request=Request(url=url)
    )


def _spider(cfg, **kwargs) -> WrcDecisionsSpider:
    """Build a spider for testing.

    The partition size is pinned to monthly rather than inherited from config.
    Tests that assert request counts would otherwise change meaning whenever the
    configured default changes -- which it did, from monthly to weekly, after
    pagination drift was observed on a live run. A test asserting "3 partitions
    for a 3-month range" should be about partition generation, not about which
    default happens to be in the YAML.
    """
    kwargs.setdefault("start_date", "2024-01-01")
    kwargs.setdefault("end_date", "2024-01-31")
    kwargs.setdefault("partition_size", "monthly")
    return WrcDecisionsSpider(config=cfg, **kwargs)


# --------------------------------------------------------------------------- #
# Scope resolution
# --------------------------------------------------------------------------- #
def test_partition_size_argument_is_honoured(cfg):
    """January is one monthly partition but five weekly ones. Weekly is the
    configured default because a live run showed pagination drift across 24
    concurrent pages; the effective value is not asserted here, since overriding
    it through PARTITION_SIZE is legitimate use."""
    spider = _spider(
        cfg,
        start_date="2024-01-01",
        end_date="2024-01-31",
        bodies="wrc",
        partition_size="weekly",
    )
    assert len(list(spider.start_requests())) == 5


def test_cli_dates_are_parsed(cfg):
    spider = _spider(cfg, start_date="2024-03-01", end_date="2024-04-30")
    assert spider.start_date == date(2024, 3, 1)
    assert spider.end_date == date(2024, 4, 30)


def test_inverted_dates_are_rejected_before_any_request(cfg):
    """Fail on construction, not after generating traffic."""
    with pytest.raises(ValueError, match="after end_date"):
        _spider(cfg, start_date="2024-06-01", end_date="2024-01-01")


def test_body_allowlist_is_honoured(cfg):
    spider = _spider(cfg, bodies="wrc,labour-court")
    assert [body.slug for body in spider.bodies] == ["wrc", "labour-court"]


def test_unknown_body_slug_fails_immediately(cfg):
    with pytest.raises(KeyError):
        _spider(cfg, bodies="not-a-body")


def test_all_bodies_by_default(cfg):
    assert len(_spider(cfg).bodies) == 4


# --------------------------------------------------------------------------- #
# Request generation
# --------------------------------------------------------------------------- #
def test_one_start_request_per_body_and_partition(cfg):
    spider = _spider(cfg, start_date="2024-01-01", end_date="2024-03-31", bodies="wrc")
    requests = list(spider.start_requests())
    assert len(requests) == 3  # three monthly partitions, one body


def test_start_yields_the_planned_requests(cfg):
    """Scrapy 2.13+ calls start(), NOT start_requests().

    Regression test for a silent failure found on the first live run: without an
    explicit async start(), Scrapy's default implementation reads start_urls,
    finds none, issues zero requests, and the run still exits 0 with
    "reconciled: true" because nothing was ever found.
    """
    spider = _spider(cfg, start_date="2024-01-01", end_date="2024-03-31", bodies="wrc")

    async def collect():
        return [request async for request in spider.start()]

    requests = asyncio.run(collect())
    assert len(requests) == 3
    assert all(isinstance(request, Request) for request in requests)


def test_planned_requests_is_recorded(cfg):
    """Lets the caller tell "nothing to do" apart from "planned work, did none"."""
    spider = _spider(cfg, start_date="2024-01-01", end_date="2024-03-31", bodies="wrc")
    list(spider.start_requests())
    assert spider.planned_requests == 3


def test_planned_requests_is_zero_for_an_uncovered_range(cfg):
    spider = _spider(cfg, start_date="2014-01-01", end_date="2014-03-31", bodies="wrc")
    list(spider.start_requests())
    assert spider.planned_requests == 0


def test_partitions_before_coverage_are_skipped(cfg):
    """The WRC has no decisions before 2015-10; querying 2014 is wasted traffic
    and, worse, makes an empty result ambiguous."""
    spider = _spider(cfg, start_date="2014-01-01", end_date="2014-03-31", bodies="wrc")
    assert list(spider.start_requests()) == []


def test_labour_court_covers_the_same_early_range(cfg):
    """Same range, different body: coverage floors are per-body, not global."""
    spider = _spider(cfg, start_date="2014-01-01", end_date="2014-03-31", bodies="labour-court")
    assert len(list(spider.start_requests())) == 3


def test_start_request_url_carries_unpadded_dates(cfg):
    """The site's from/to params require 1/1/2024, not 01/01/2024."""
    spider = _spider(cfg, bodies="wrc")
    url = next(iter(spider.start_requests())).url
    assert "from=1/1/2024" in url
    assert "to=31/1/2024" in url
    assert "body=15376" in url


def test_page_one_omits_the_page_parameter(cfg):
    spider = _spider(cfg, bodies="wrc")
    assert "pageNumber" not in next(iter(spider.start_requests())).url


# --------------------------------------------------------------------------- #
# Page 1: baseline and fan-out
# --------------------------------------------------------------------------- #
def test_records_found_comes_from_the_stated_total(cfg, partition):
    """Not from the number of blocks parsed. The stated total is the only
    baseline independent of our own selectors."""
    spider = _spider(cfg)
    list(spider.parse_listing(_page(total=234), "wrc", partition, 1))
    assert spider.counters[("wrc", "2024-01")].found == 234


def test_page_two_onwards_are_fired_in_parallel(cfg, partition):
    """234 results at 10 per page = 24 pages, so 23 further requests, all issued
    from page 1 rather than by following 'next' links."""
    spider = _spider(cfg)
    output = list(spider.parse_listing(_page(total=234), "wrc", partition, 1))
    requests = [item for item in output if isinstance(item, Request)]
    assert len(requests) == 23
    assert "pageNumber=2" in requests[0].url
    assert "pageNumber=24" in requests[-1].url


def test_single_page_partition_fires_no_extra_requests(cfg, partition):
    spider = _spider(cfg)
    output = list(spider.parse_listing(_page(total=7), "wrc", partition, 1))
    assert [item for item in output if isinstance(item, Request)] == []


def test_unparseable_total_is_a_hard_failure(cfg, partition):
    """Requirement: no baseline means the partition cannot be reconciled, so it
    fails loudly rather than being recorded as empty."""
    spider = _spider(cfg)
    output = list(spider.parse_listing(_page(total=None), "wrc", partition, 1))

    assert output == []  # no records, no further pages
    assert spider.counters[("wrc", "2024-01")].failed == 1
    assert spider.unreconcilable == [{"body": "wrc", "partition_key": "2024-01"}]


def test_zero_results_is_not_a_failure(cfg, partition):
    """Distinct from the case above: the site said zero, and we believe it."""
    spider = _spider(cfg)
    output = list(spider.parse_listing(_page(total=0, items=""), "wrc", partition, 1))

    assert output == []
    assert spider.counters[("wrc", "2024-01")].found == 0
    assert spider.counters[("wrc", "2024-01")].failed == 0


def test_the_sites_no_results_page_is_an_empty_partition_not_a_failure(cfg, partition):
    """An empty search renders a message instead of a count line, so this page
    has no "Shows ... of N" at all -- yet it is a complete, verified zero.

    Live consequence before this worked: four Employment Appeals Tribunal
    partitions for 2024 weeks failed with `partition.no_baseline` and exhausted
    their Dagster retries, for a body that has no decisions that recent.
    """
    spider = _spider(cfg)
    body = (
        b"<html><body><div id='searchResult'>"
        b"There are no search results fitting your keywords"
        b"</div></body></html>"
    )
    response = HtmlResponse(
        url=SEARCH_URL, body=body, encoding="utf-8", request=Request(url=SEARCH_URL)
    )

    output = list(spider.parse_listing(response, "wrc", partition, 1))
    counters = spider.counters[("wrc", "2024-01")]

    assert output == []
    assert counters.found == 0
    assert counters.failed == 0
    assert spider.unreconcilable == []
    assert counters.as_dict()["reconciled"] is True


def test_comma_formatted_total_is_parsed(cfg, partition):
    """The site renders large totals as '62,789'."""
    spider = _spider(cfg)
    list(spider.parse_listing(_page(total="1,234", items=""), "wrc", partition, 1))
    assert spider.counters[("wrc", "2024-01")].found == 1234


def test_http_error_on_listing_is_recorded(cfg, partition):
    spider = _spider(cfg)
    response = _page().replace(status=503)
    assert list(spider.parse_listing(response, "wrc", partition, 1)) == []
    assert spider.counters[("wrc", "2024-01")].failed == 1


# --------------------------------------------------------------------------- #
# Record parsing
# --------------------------------------------------------------------------- #
def _records(spider, response, partition) -> list[DocumentRecord]:
    return [
        item
        for item in spider.parse_page(response, "wrc", partition, 2)
        if isinstance(item, DocumentRecord)
    ]


def test_a_record_is_parsed_completely(cfg, partition):
    spider = _spider(cfg)
    (record,) = _records(spider, _page(), partition)

    assert record.identifier == "IR - SC - 00000787"
    assert record.identifier_safe == "IR-SC-00000787"
    assert record.description == "Office Administrator V A Translation Service"
    assert record.published_date == date(2024, 1, 31)
    assert record.partition_key == "2024-01"
    assert record.partition_date == date(2024, 1, 1)
    assert record.body == "Workplace Relations Commission"
    assert record.status is RecordStatus.PENDING


def test_document_url_is_absolute_and_keeps_literal_spaces(cfg, partition):
    """Recon's trap: the site's own href contains spaces and must be followed
    verbatim, never rebuilt from the identifier."""
    spider = _spider(cfg)
    (record,) = _records(spider, _page(), partition)
    assert record.document_url == (
        "https://www.workplacerelations.ie/en/cases/2024/january/ir- sc- 00000787.html"
    )


def test_file_ext_is_taken_from_the_url(cfg, partition):
    spider = _spider(cfg)
    (record,) = _records(spider, _page(), partition)
    assert record.file_ext == ".html"


def test_multiple_records_on_one_page(cfg, partition):
    spider = _spider(cfg)
    items = _item_html(identifier="ADJ-00054658", href="/en/cases/2024/january/adj-00054658.html")
    items += _item_html(identifier="ADJ-00057058", href="/en/cases/2024/january/adj-00057058.html")
    assert len(_records(spider, _page(items=items), partition)) == 2


def test_identifier_mismatch_is_a_failure(cfg, partition):
    """The identifier appears three times; disagreement means our parse has
    drifted from the DOM, and a wrong reference is worse than no record."""
    spider = _spider(cfg)
    items = _item_html(identifier="ADJ-00054658", crosscheck="ADJ-00099999")
    assert _records(spider, _page(items=items), partition) == []
    assert spider.counters[("wrc", "2024-01")].failed == 1
    assert "identifier_mismatch" in spider.counters[("wrc", "2024-01")].failures[0]["reason"]


def test_whitespace_only_difference_is_not_a_mismatch(cfg, partition):
    """`IR - SC - 00000787` and `IR-SC-00000787` are the same reference rendered
    two ways in the same markup."""
    spider = _spider(cfg)
    items = _item_html(identifier="IR - SC - 00000787", crosscheck="IR-SC-00000787")
    assert len(_records(spider, _page(items=items), partition)) == 1


def test_identifier_failing_the_body_pattern_is_a_failure(cfg, partition):
    """Guards against a selector that silently returns page chrome."""
    spider = _spider(cfg)
    items = _item_html(identifier="Click here for a Guide")
    assert _records(spider, _page(items=items), partition) == []
    assert spider.counters[("wrc", "2024-01")].failed == 1


def test_unparseable_date_is_a_failure(cfg, partition):
    spider = _spider(cfg)
    items = _item_html(date_text="not a date")
    assert _records(spider, _page(items=items), partition) == []
    assert "date_unparseable" in spider.counters[("wrc", "2024-01")].failures[0]["reason"]


def test_missing_link_falls_back_to_fullpath(cfg, partition):
    """Two selectors for the document link, tried in order."""
    spider = _spider(cfg)
    items = _item_html(identifier="ADJ-00054658", href="/en/cases/2024/january/adj-00054658.html")
    items = items.replace('<div class="col-sm-3 link"><a href="', '<div class="gone"><a data-x="')
    (record,) = _records(spider, _page(items=items), partition)
    assert record.document_url.endswith("adj-00054658.html")


def test_one_bad_record_does_not_cost_the_others(cfg, partition):
    """A malformed block is counted and skipped; the other nine still land."""
    spider = _spider(cfg)
    items = _item_html(identifier="Click here for a Guide")
    items += _item_html(identifier="ADJ-00054658", href="/en/cases/2024/january/a.html")
    records = _records(spider, _page(items=items), partition)

    assert len(records) == 1
    assert spider.counters[("wrc", "2024-01")].failed == 1


def test_date_outside_the_partition_is_kept_not_rejected(cfg, partition):
    """Decision date and displayed date disagree on this site; rejecting would
    discard legitimate records."""
    spider = _spider(cfg)
    items = _item_html(identifier="ADJ-00054658", date_text="20/12/2009")
    (record,) = _records(spider, _page(items=items), partition)

    assert record.date_in_partition_window is False
    assert spider.counters[("wrc", "2024-01")].failed == 0


# --------------------------------------------------------------------------- #
# Counters and summaries
# --------------------------------------------------------------------------- #
def test_counters_are_isolated_per_body_and_partition(cfg):
    """Pages from many partitions are in flight at once; their counts must not
    bleed into each other."""
    spider = _spider(cfg)
    january = Partition(date(2024, 1, 1), date(2024, 1, 31), "2024-01", PartitionSize.MONTHLY)
    february = Partition(date(2024, 2, 1), date(2024, 2, 29), "2024-02", PartitionSize.MONTHLY)

    list(spider.parse_listing(_page(total=5, items=""), "wrc", january, 1))
    list(spider.parse_listing(_page(total=9, items=""), "wrc", february, 1))
    list(spider.parse_listing(_page(total=3, items=""), "labour-court", january, 1))

    assert spider.counters[("wrc", "2024-01")].found == 5
    assert spider.counters[("wrc", "2024-02")].found == 9
    assert spider.counters[("labour-court", "2024-01")].found == 3


def test_closed_emits_a_summary_per_partition(cfg, partition, caplog):
    spider = _spider(cfg)
    list(spider.parse_listing(_page(total=1), "wrc", partition, 1))

    with caplog.at_level("INFO"):
        spider.closed("finished")

    events = [record.msg for record in caplog.records]
    assert "partition.summary" in events
    assert "crawl.summary" in events


def test_run_summary_reports_unreconciled_partitions(cfg, partition, caplog):
    """The site states 1 result but the page holds none, so one record is
    unaccounted for and the summary must say so rather than look healthy."""
    spider = _spider(cfg)
    list(spider.parse_listing(_page(total=1, items=""), "wrc", partition, 1))

    with caplog.at_level("INFO"):
        spider.closed("finished")

    summary = next(r for r in caplog.records if r.msg == "crawl.summary")
    assert summary.reconciled is False
    assert summary.unreconciled_partitions == ["wrc/2024-01"]
    # The site advertised one entry and served none, so it is the LISTINGS check
    # that fails here, not the records check -- which is exactly the distinction
    # the split buys: this points at fetching, not at storage.
    assert summary.listings_unaccounted == 1
    assert summary.records_unaccounted == 0


def test_parsed_records_are_registered_as_distinct(cfg, partition):
    """Keyed on identifier_safe so the site's two renderings of one reference
    (`IR-SC-00001259` and `IR - SC - 00001897`) do not count as two records."""
    spider = _spider(cfg)
    items = _item_html(identifier="IR - SC - 00000787")
    items += _item_html(identifier="IR-SC-00000787", href="/en/cases/2024/january/x.html")
    list(spider.parse_page(_page(items=items), "wrc", partition, 2))

    assert spider.counters[("wrc", "2024-01")].seen == {"IR-SC-00000787"}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://x.ie/a/b.html", ".html"),
        ("https://x.ie/a/b.PDF", ".pdf"),
        ("https://x.ie/a/ir- sc- 787.html", ".html"),
        ("https://x.ie/a/b.html?x=1#y", ".html"),
        ("https://x.ie/a/b", None),
        ("https://x.ie/", None),
    ],
)
def test_extension_of(url, expected):
    assert _extension_of(url) == expected


@pytest.mark.parametrize(
    "left,right",
    [
        ("IR - SC - 00000787", "IR-SC-00000787"),
        ("adj-00054658", "ADJ-00054658"),
        ("  EDA2471  ", "EDA2471"),
    ],
)
def test_normalise_ref_treats_these_as_equal(left, right):
    assert _normalise_ref(left) == _normalise_ref(right)


def test_normalise_ref_still_distinguishes_real_differences():
    assert _normalise_ref("ADJ-00054658") != _normalise_ref("ADJ-00099999")