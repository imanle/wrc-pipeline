"""The decisions spider.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from typing import Any

import scrapy
from scrapy.http import Response

from ...logging_config import PartitionCounters, get_logger
from ...partitions import Partition, generate_partitions
from ...settings import BodySettings, Settings, get_settings
from ..items import DocumentRecord, record_from_listing

log = get_logger(__name__)


def partition_size_value(size: Any) -> str:
    """Plain string for a partition size.
    """
    return getattr(size, "value", str(size))


def _parse_cli_date(value: str | date | None, fallback: date) -> date:
    """Accept a date from the command line, config, or Dagster.
    """
    if value is None:
        return fallback
    if isinstance(value, date):
        return value
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


class WrcDecisionsSpider(scrapy.Spider):
    """Scrape decision metadata from the Workplace Relations search.

    Usage::

        scrapy crawl wrc_decisions \\
            -a start_date=2024-01-01 -a end_date=2024-03-31 -a bodies=wrc

    All three arguments are optional; they default to ``partitions.start_date``,
    ``partitions.end_date`` and every configured body.
    """

    name = "wrc_decisions"

    def __init__(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        bodies: str | None = None,
        partition_size: str | None = None,
        config: Settings | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Resolve the crawl's scope before a single request is made.

        The parameter is ``config``, not ``settings``: ``scrapy.Spider`` already
        owns a ``self.settings`` attribute for Scrapy's own settings object, and
        shadowing it breaks Scrapy internals in ways that surface much later.
        """
        super().__init__(*args, **kwargs)
        self.cfg = config or get_settings()

        partitions_cfg = self.cfg.partitions
        self.start_date = _parse_cli_date(start_date, partitions_cfg.start_date)
        self.end_date = _parse_cli_date(end_date, partitions_cfg.end_date)
        if self.start_date > self.end_date:
            raise ValueError(f"start_date {self.start_date} is after end_date {self.end_date}")

        self.partition_size = partition_size or partitions_cfg.size

        # A comma-separated allowlist of slugs, validated now rather than on the
        # first request: a typo should fail before any traffic is generated.
        if bodies:
            wanted = [slug.strip() for slug in bodies.split(",") if slug.strip()]
            self.bodies = [self.cfg.scraping.body_by_slug(slug) for slug in wanted]
        else:
            self.bodies = list(self.cfg.scraping.bodies)

        # Found-vs-scraped accounting, one counter per unit of work. Keyed on the
        # tuple rather than held as spider state because pages from many
        # partitions are in flight simultaneously and their counts must not mix.
        self.counters: dict[tuple[str, str], PartitionCounters] = {}
        # How many (body, partition) requests were planned. Lets the caller
        # distinguish "nothing to do" from "we planned work and did none".
        self.planned_requests = 0
        # Partitions whose result total could not be parsed. Tracked separately
        # because they have no trustworthy baseline, so they cannot be reconciled
        # at all -- distinct from a partition that reconciled badly.
        self.unreconcilable: list[dict[str, str]] = []

    # ------------------------------------------------------------------ #
    # Setup
    # ------------------------------------------------------------------ #
    @classmethod
    def from_crawler(cls, crawler: Any, *args: Any, **kwargs: Any) -> WrcDecisionsSpider:
        """Standard Scrapy hook; kept explicit so the pipeline can reach the
        spider's counters via ``crawler.spider``."""
        spider = super().from_crawler(crawler, *args, **kwargs)
        return spider

    def counters_for(self, body_slug: str, partition_key: str) -> PartitionCounters:
        """Fetch or create the counter for one unit of work."""
        key = (body_slug, partition_key)
        if key not in self.counters:
            self.counters[key] = PartitionCounters(body_slug, partition_key)
        return self.counters[key]

    def partitions(self) -> list[Partition]:
        return list(generate_partitions(self.start_date, self.end_date, self.partition_size))

    # ------------------------------------------------------------------ #
    # Request generation
    # ------------------------------------------------------------------ #
    async def start(self) -> Any:
        """Scrapy 2.13+ entry point for initial requests.
        """
        for request in self.start_requests():
            yield request

    def start_requests(self) -> Iterator[scrapy.Request]:
        partitions = self.partitions()
        planned = 0

        for body in self.bodies:
            for partition in partitions:
                if not body.covers(partition.end):
                    log.debug(
                        "partition.skipped_before_coverage",
                        extra={
                            "body": body.slug,
                            "partition_key": partition.key,
                            "earliest_date": str(body.earliest_date),
                        },
                    )
                    continue

                planned += 1
                yield self._listing_request(body, partition, page=1)

        self.planned_requests = planned
        log.info(
            "run.plan",
            extra={
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "partition_size": partition_size_value(self.partition_size),
                "bodies": [body.slug for body in self.bodies],
                "partitions": len(partitions),
                "start_requests": planned,
            },
        )

    def _listing_request(
        self,
        body: BodySettings,
        partition: Partition,
        page: int,
    ) -> scrapy.Request:
        """Build one search request.

        The URL is built by ``ScrapingSettings.search_url`` rather than here, so
        the spider, the tests and any future source adapter cannot disagree about
        the site's contract.
        """
        url = self.cfg.scraping.search_url(body, partition.start, partition.end, page=page)
        return scrapy.Request(
            url,
            callback=self.parse_listing if page == 1 else self.parse_page,
            errback=self.on_error,
            cb_kwargs={"body_slug": body.slug, "partition": partition, "page": page},
            # A permanent 404 must reach the failure ledger with its code rather
            # than being swallowed by Scrapy's default error handling.
            meta={"handle_httpstatus_list": [404]},
        )

    # ------------------------------------------------------------------ #
    # Callbacks
    # ------------------------------------------------------------------ #
    def parse_listing(
        self,
        response: Response,
        body_slug: str,
        partition: Partition,
        page: int,
    ) -> Iterator[DocumentRecord | scrapy.Request]:
        """Page 1: establish the baseline, fan out, then parse records.

        Separate from :meth:`parse_page` so the fan-out logic provably runs once
        per partition rather than once per page.
        """
        counters = self.counters_for(body_slug, partition.key)
        body = self.cfg.scraping.body_by_slug(body_slug)

        if response.status != 200:
            counters.record_failure(response.url, "listing_http_error", response.status)
            return

        total = self.cfg.scraping.parse_result_count(response.text)

        if total is None:
            self.unreconcilable.append({"body": body_slug, "partition_key": partition.key})
            counters.record_failure(response.url, "result_count_unparseable", "no_baseline")
            log.error(
                "partition.no_baseline",
                extra={
                    "body": body_slug,
                    "partition_key": partition.key,
                    "url": response.url,
                    "reason": "could not parse 'Shows X to Y of N results'",
                },
            )
            return

        counters.found = total
        page_count = self.cfg.scraping.page_count(total)

        log.info(
            "partition.start",
            extra={
                "body": body_slug,
                "partition_key": partition.key,
                "partition_start": partition.start.isoformat(),
                "partition_end": partition.end.isoformat(),
                "records_found": total,
                "pages": page_count,
                "url": response.url,
            },
        )

        if total == 0:
            return

        # Pages 2..N are issued now, in parallel. The stateless GET is what makes
        # this legal: no page depends on having visited the previous one.
        for next_page in range(2, page_count + 1):
            yield self._listing_request(body, partition, page=next_page)

        yield from self._parse_records(response, body, partition, counters)

    def parse_page(
        self,
        response: Response,
        body_slug: str,
        partition: Partition,
        page: int,
    ) -> Iterator[DocumentRecord]:
        """Pages 2..N: records only. The baseline is already established."""
        counters = self.counters_for(body_slug, partition.key)

        if response.status != 200:
            counters.record_failure(response.url, "listing_http_error", response.status)
            return

        body = self.cfg.scraping.body_by_slug(body_slug)
        yield from self._parse_records(response, body, partition, counters)

    def on_error(self, failure: Any) -> None:
        """Terminal request failure: retries exhausted, DNS, timeout, connection.

        Every one of these is a record we will not scrape, so it belongs in the
        ledger with the reason -- that is the "200 minus X, and every X logged"
        requirement.
        """
        request = failure.request
        body_slug = request.cb_kwargs.get("body_slug", "unknown")
        partition = request.cb_kwargs.get("partition")
        partition_key = partition.key if partition else "unknown"

        self.counters_for(body_slug, partition_key).record_failure(
            request.url,
            f"request_failed:{failure.type.__name__}",
            getattr(failure.value, "response", None) and failure.value.response.status,
        )

    # ------------------------------------------------------------------ #
    # Record parsing
    # ------------------------------------------------------------------ #
    def _parse_records(
        self,
        response: Response,
        body: BodySettings,
        partition: Partition,
        counters: PartitionCounters,
    ) -> Iterator[DocumentRecord]:
        """Turn every ``li.each-item`` on a listing page into a PENDING record.

        """
        selectors = self.cfg.scraping.listing

        for block in response.css(selectors.record):
            # Counted before validation: this is an entry the site served, which
            # is a different question from whether we could read it.
            counters.record_entry()
            identifier = self._first(block, selectors.identifier)
            if identifier:
                # Strip listing decorations before anything compares or
                # validates this. The Equality Tribunal appends
                # " - Full Case Report" to the title attribute but not to
                # span.refNO, which failed the cross-check on every record.
                identifier = self.cfg.scraping.clean_identifier(identifier)

            if not identifier:
                counters.record_unidentified()
                counters.record_failure(response.url, "identifier_missing", "selector")
                continue

            case_number = (
                self._first(block, selectors.identifier_crosscheck)
                if selectors.identifier_crosscheck
                else None
            )

            # Where the two ARE meant to agree, comparing them is a free
            # integrity test: disagreement means our parse has drifted from the
            # DOM, and a record with a wrong reference is silently wrong forever
            # where a missing one is in the ledger.
            #
            # Skipped for bodies that declare the fields distinct -- see
            # BodySettings.crosscheck_identifier.
            if body.crosscheck_identifier and case_number:
                crosscheck = case_number
                if _normalise_ref(crosscheck) != _normalise_ref(identifier):
                    counters.record_failure(
                        response.url,
                        f"identifier_mismatch:{crosscheck}",
                        "dom_drift",
                        identifier,
                    )
                    continue

            # Guards against a selector that silently returns page chrome rather
            # than a reference. Without it, a site tweak yields hundreds of
            # plausible-looking corrupt records instead of a countable failure.
            if not body.validate_identifier(identifier):
                counters.record_failure(
                    response.url, "identifier_pattern_mismatch", "validation", identifier
                )
                continue

            raw_date = self._first(block, selectors.published_date)
            try:
                published = self.cfg.scraping.parse_display_date(raw_date or "")
            except ValueError:
                counters.record_failure(
                    response.url, f"date_unparseable:{raw_date!r}", "parse", identifier
                )
                continue

            href = self._first(block, selectors.document_link)
            if not href and selectors.document_link_fallback:
                href = self._first(block, selectors.document_link_fallback)
            if not href:
                counters.record_failure(
                    response.url, "document_link_missing", "selector", identifier
                )
                continue

            document_url = response.urljoin(href)

            record = record_from_listing(
                identifier=identifier,
                body_slug=body.slug,
                published_date=published,
                source_url=response.url,
                document_url=document_url,
                partition_date=partition.partition_date,
                partition_key=partition.key,
                description=self._first(block, selectors.description),
                case_number=case_number,
                file_ext=_extension_of(document_url),
                settings=self.cfg,
            )

            if not record.date_in_partition_window:
                log.warning(
                    "record.date_outside_partition",
                    extra={
                        "body": body.slug,
                        "partition_key": partition.key,
                        "identifier": identifier,
                        "published_date": published.isoformat(),
                        "partition_start": partition.start.isoformat(),
                    },
                )

            counters.record_seen(record.identifier_safe)
            yield record

    @staticmethod
    def _first(block: Any, selector: str) -> str | None:
        """First match for *selector*, stripped, or ``None``.
        """
        value = block.css(selector).get()
        return value.strip() if value and value.strip() else None

    # ------------------------------------------------------------------ #
    # Teardown
    # ------------------------------------------------------------------ #
    def closed(self, reason: str) -> None:
        """Emit one summary per partition, plus the run summary (requirement 10).
        """
        totals = {
            "records_found": 0,
            "records_scraped": 0,
            "records_skipped_unchanged": 0,
            "records_failed": 0,
            "records_distinct": 0,
            "listings_served": 0,
            "duplicate_listings": 0,
            "listings_unidentified": 0,
            "listings_unaccounted": 0,
            "records_unaccounted": 0,
        }
        unreconciled: list[str] = []

        for counters in self.counters.values():
            counters.emit_summary()
            summary = counters.as_dict()
            for field in totals:
                totals[field] += summary[field]
            if not summary["reconciled"]:
                unreconciled.append(f"{counters.body}/{counters.partition_key}")

        log.info(
            "crawl.summary",
            extra={
                "reason": reason,
                "partitions": len(self.counters),
                **totals,
                "reconciled": not unreconciled,
                "unreconciled_partitions": unreconciled,
                "partitions_without_baseline": self.unreconcilable,
            },
        )


def _normalise_ref(value: str) -> str:
    """Compare references ignoring whitespace and case.

    ``IR - SC - 00000787`` and ``IR-SC-00000787`` are the same reference rendered
    differently in two places in the same markup, so a literal comparison would
    report drift on every single record.
    """
    return "".join(value.split()).upper()


def _extension_of(url: str) -> str | None:
    """File extension from a URL path, or ``None``.

    Only a hint: the download pipeline cross-checks it against the response's
    ``Content-Type`` before deciding whether a document is HTML or binary, since
    a URL is a claim and a header is evidence.
    """
    path = url.split("?", 1)[0].split("#", 1)[0]
    tail = path.rsplit("/", 1)[-1]
    if "." not in tail:
        return None
    return f".{tail.rsplit('.', 1)[-1].lower()}"