"""Tests for the document download pipeline.

No network, no MongoDB, no MinIO. The pipeline's collaborators -- Scrapy's engine,
``mongo`` and ``objectstore`` -- are replaced with fakes that record what they
were asked to do, which is the only way to assert on things like "the second run
sent an If-None-Match header" or "a 404 was written to the failure ledger with
its code".

``asyncio.run`` drives the coroutine directly rather than pulling in an async
pytest plugin: one dependency fewer, and the pipeline's own control flow is what
is under test, not Scrapy's reactor.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import pytest
from scrapy.exceptions import DropItem
from scrapy.http import Request, Response
from twisted.internet import defer

from wrc_pipeline.logging_config import PartitionCounters
from wrc_pipeline.scraper import pipelines
from wrc_pipeline.scraper.items import DocumentRecord, RecordStatus
from wrc_pipeline.scraper.pipelines import DocumentDownloadPipeline
from wrc_pipeline.settings import load_settings
from wrc_pipeline.storage.mongo import WriteOutcome
from wrc_pipeline.storage.objectstore import StoredObject, UploadOutcome

DOC_URL = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00054658.html"


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class FakeEngine:
    """Captures the request and returns a canned response."""

    def __init__(self, response: Response | Exception) -> None:
        self.response = response
        self.requests: list[Request] = []

    def download(self, request: Request) -> Any:
        """Returns a Twisted Deferred, as the real engine does.

        Not an asyncio.Future: maybe_deferred_to_future() takes a Deferred, and a
        fake that returns the wrong type tests nothing but itself.
        """
        self.requests.append(request)
        if isinstance(self.response, Exception):
            return defer.fail(self.response)
        return defer.succeed(self.response)


class FakeCrawler:
    def __init__(self, engine: FakeEngine) -> None:
        self.engine = engine


class FakeSpider:
    """Just enough spider for the pipeline: counters and run scope."""

    def __init__(self) -> None:
        self.counters: dict[tuple[str, str], PartitionCounters] = {}
        self.start_date = date(2024, 1, 1)
        self.end_date = date(2024, 1, 31)
        self.partition_size = "monthly"
        self.bodies = []

    def counters_for(self, body_slug: str, partition_key: str) -> PartitionCounters:
        key = (body_slug, partition_key)
        if key not in self.counters:
            self.counters[key] = PartitionCounters(body_slug, partition_key)
        return self.counters[key]


class MongoSpy:
    """Stands in for the mongo module, recording every call."""

    def __init__(self, existing: dict[str, Any] | None = None, outcome=WriteOutcome.INSERTED):
        self.existing = existing
        self.outcome = outcome
        self.upserts: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.runs: list[Any] = []

    def find_existing(self, body_slug, identifier, settings=None):
        return self.existing

    def upsert_landing_record(self, record, settings=None):
        self.upserts.append(record)
        return self.outcome

    def record_failure(self, **kwargs):
        self.failures.append(kwargs)

    def ensure_indexes(self, settings=None):
        pass

    def start_run(self, **kwargs):
        self.runs.append(("start", kwargs))

    def finish_run(self, run_id, totals, status="completed", settings=None):
        self.runs.append(("finish", totals, status))
        return None


class StoreSpy:
    """Stands in for the objectstore module."""

    def __init__(self, outcome=UploadOutcome.UPLOADED, error: Exception | None = None):
        self.outcome = outcome
        self.error = error
        self.uploads: list[dict[str, Any]] = []

    def ensure_buckets(self, settings=None):
        pass

    def put_landing_document(self, **kwargs):
        if self.error:
            raise self.error
        self.uploads.append(kwargs)
        return StoredObject(
            bucket="wrc-landing",
            key=f"{kwargs['body_slug']}/{kwargs['partition_key']}/"
            f"{kwargs['identifier_safe']}__abc12345{kwargs['ext']}",
            file_hash="abc12345" + "0" * 56,
            file_size=len(kwargs["data"]),
            content_type="text/html; charset=utf-8",
            outcome=self.outcome,
        )


@pytest.fixture(scope="module")
def cfg():
    return load_settings()


@pytest.fixture
def item(cfg) -> DocumentRecord:
    return DocumentRecord(
        identifier="ADJ-00054658",
        body="Workplace Relations Commission",
        body_slug="wrc",
        published_date=date(2024, 1, 17),
        partition_date=date(2024, 1, 1),
        partition_key="2024-01",
        source_url="https://www.workplacerelations.ie/en/search/?decisions=1",
        document_url=DOC_URL,
        file_ext=".html",
    )


def _response(
    status: int = 200,
    body: bytes = b"<html><body>decision</body></html>",
    headers: dict[str, str] | None = None,
) -> Response:
    # The base Response, not HtmlResponse: HtmlResponse injects a default
    # Content-Type, which would make the "no header at all" case untestable.
    # `headers=None` means no header; `headers={}` would be indistinguishable.
    return Response(
        url=DOC_URL,
        status=status,
        body=body,
        headers={"Content-Type": "text/html; charset=utf-8"} if headers is None else headers,
        request=Request(url=DOC_URL),
    )


def _pipeline(cfg, monkeypatch, response, mongo_spy=None, store_spy=None):
    mongo_spy = mongo_spy or MongoSpy()
    store_spy = store_spy or StoreSpy()
    monkeypatch.setattr(pipelines, "mongo", mongo_spy)
    monkeypatch.setattr(pipelines, "store", store_spy)

    pipeline = DocumentDownloadPipeline(FakeCrawler(FakeEngine(response)), config=cfg)
    pipeline.run_id = "test-run"
    return pipeline, mongo_spy, store_spy


def _run(pipeline, item, spider):
    return asyncio.run(pipeline.process_item(item, spider))


# --------------------------------------------------------------------------- #
# The happy path
# --------------------------------------------------------------------------- #
def test_document_is_downloaded_uploaded_and_recorded(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, mongo_spy, store_spy = _pipeline(cfg, monkeypatch, _response())

    record = _run(pipeline, item, spider)

    assert len(store_spy.uploads) == 1
    assert len(mongo_spy.upserts) == 1
    assert record.status is RecordStatus.SCRAPED
    assert record.file_hash == "abc12345" + "0" * 56
    assert record.file_bucket == "wrc-landing"
    assert spider.counters[("wrc", "2024-01")].scraped == 1


def test_bytes_go_straight_from_response_to_upload(cfg, monkeypatch, item):
    """The no-local-storage constraint: response.body is handed to the object
    store as bytes, never via a temporary file."""
    spider = FakeSpider()
    body = b"<html>the actual decision text</html>"
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response(body=body))

    _run(pipeline, item, spider)
    assert store_spy.uploads[0]["data"] == body


def test_etag_and_last_modified_are_stored(cfg, monkeypatch, item):
    """Kept so the NEXT run can ask the server whether anything changed."""
    spider = FakeSpider()
    headers = {
        "Content-Type": "text/html",
        "ETag": '"a1b2c3"',
        "Last-Modified": "Wed, 17 Jan 2024 09:00:00 GMT",
    }
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response(headers=headers))

    record = _run(pipeline, item, spider)
    assert record.etag == '"a1b2c3"'
    assert record.last_modified == "Wed, 17 Jan 2024 09:00:00 GMT"


def test_request_uses_dont_filter(cfg, monkeypatch, item):
    """Required, not tidiness: one file serves three case references on this
    site (rp2147_2009_mn1794_2009_wt796_2009.html), so records legitimately
    share a document URL and must not be deduplicated away."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response())

    _run(pipeline, item, spider)
    assert pipeline.crawler.engine.requests[0].dont_filter is True


def test_request_handles_all_http_statuses_itself(cfg, monkeypatch, item):
    """304 is a success and 404 is a ledger entry; neither should arrive as an
    exception."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response())

    _run(pipeline, item, spider)
    assert pipeline.crawler.engine.requests[0].meta["handle_httpstatus_all"] is True


# --------------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------------- #
def test_no_conditional_headers_on_first_sighting(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response(), MongoSpy(existing=None))

    _run(pipeline, item, spider)
    headers = pipeline.crawler.engine.requests[0].headers
    assert b"If-None-Match" not in headers


def test_stored_validators_become_conditional_headers(cfg, monkeypatch, item):
    """The half of requirement 9 that a hash cannot satisfy: ask the server
    whether the bytes changed instead of fetching them to find out."""
    spider = FakeSpider()
    existing = {
        "file_hash": "old" + "0" * 61,
        "file_path": "wrc/2024-01/ADJ-00054658__old.html",
        "etag": '"a1b2c3"',
        "last_modified": "Wed, 17 Jan 2024 09:00:00 GMT",
    }
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response(), MongoSpy(existing=existing))

    _run(pipeline, item, spider)
    headers = pipeline.crawler.engine.requests[0].headers
    assert headers[b"If-None-Match"] == b'"a1b2c3"'
    assert headers[b"If-Modified-Since"] == b"Wed, 17 Jan 2024 09:00:00 GMT"


def test_304_skips_without_uploading(cfg, monkeypatch, item):
    """No body was transferred, so nothing is re-uploaded and the stored hash is
    carried forward."""
    spider = FakeSpider()
    existing = {
        "file_hash": "old" + "0" * 61,
        "file_path": "wrc/2024-01/ADJ-00054658__old00000.html",
        "etag": '"a1b2c3"',
    }
    pipeline, mongo_spy, store_spy = _pipeline(
        cfg, monkeypatch, _response(status=304, body=b""), MongoSpy(existing=existing)
    )

    record = _run(pipeline, item, spider)

    assert store_spy.uploads == []
    assert record.status is RecordStatus.SKIPPED
    assert record.file_hash == "old" + "0" * 61
    assert record.file_path == "wrc/2024-01/ADJ-00054658__old00000.html"
    assert spider.counters[("wrc", "2024-01")].skipped == 1
    assert spider.counters[("wrc", "2024-01")].scraped == 0
    # Still upserted, so last_seen_at is touched and the run accounts for it.
    assert len(mongo_spy.upserts) == 1


def test_unchanged_mongo_outcome_counts_as_skipped(cfg, monkeypatch, item):
    """The fallback path: no validator, so the file was downloaded, but the hash
    matched and nothing new was written."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(
        cfg,
        monkeypatch,
        _response(),
        MongoSpy(outcome=WriteOutcome.UNCHANGED),
        StoreSpy(outcome=UploadOutcome.SKIPPED_UNCHANGED),
    )

    record = _run(pipeline, item, spider)

    assert record.status is RecordStatus.SKIPPED
    assert spider.counters[("wrc", "2024-01")].skipped == 1


def test_changed_content_counts_as_scraped(cfg, monkeypatch, item):
    """An amended decision is new work, not a skip."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(
        cfg, monkeypatch, _response(), MongoSpy(outcome=WriteOutcome.UPDATED)
    )

    record = _run(pipeline, item, spider)
    assert record.status is RecordStatus.SCRAPED
    assert spider.counters[("wrc", "2024-01")].scraped == 1


def test_counting_follows_mongo_not_the_object_store(cfg, monkeypatch, item):
    """An object can survive from an interrupted run whose metadata write never
    landed. The record is still new, so it counts as scraped even though the
    upload was a no-op."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(
        cfg,
        monkeypatch,
        _response(),
        MongoSpy(outcome=WriteOutcome.INSERTED),
        StoreSpy(outcome=UploadOutcome.SKIPPED_UNCHANGED),
    )

    _run(pipeline, item, spider)
    assert spider.counters[("wrc", "2024-01")].scraped == 1
    assert spider.counters[("wrc", "2024-01")].skipped == 0


# --------------------------------------------------------------------------- #
# Failures
# --------------------------------------------------------------------------- #
def test_404_is_dropped_counted_and_logged_with_its_code(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, mongo_spy, store_spy = _pipeline(cfg, monkeypatch, _response(status=404))

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)

    counters = spider.counters[("wrc", "2024-01")]
    assert counters.failed == 1
    assert counters.failures[0]["error_code"] == 404
    assert counters.failures[0]["url"] == DOC_URL
    assert mongo_spy.failures[0]["error_code"] == 404
    assert store_spy.uploads == []


def test_500_is_recorded_as_a_failure(cfg, monkeypatch, item):
    """Retries are exhausted by the time the pipeline sees it."""
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response(status=500))

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)
    assert spider.counters[("wrc", "2024-01")].failed == 1


def test_download_exception_is_recorded(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, mongo_spy, _ = _pipeline(cfg, monkeypatch, TimeoutError("timed out"))

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)

    assert "download_failed:TimeoutError" in spider.counters[("wrc", "2024-01")].failures[0][
        "reason"
    ]
    assert len(mongo_spy.failures) == 1


def test_empty_body_is_a_failure_not_a_zero_byte_object(cfg, monkeypatch, item):
    """A 200 with no content would otherwise store a zero-byte file and a
    plausible-looking record."""
    spider = FakeSpider()
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response(body=b""))

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)
    assert store_spy.uploads == []
    assert spider.counters[("wrc", "2024-01")].failed == 1


def test_upload_failure_is_recorded(cfg, monkeypatch, item):
    from wrc_pipeline.storage.objectstore import ObjectStoreError

    spider = FakeSpider()
    pipeline, mongo_spy, _ = _pipeline(
        cfg, monkeypatch, _response(), None, StoreSpy(error=ObjectStoreError("minio down"))
    )

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)

    assert "upload_failed" in spider.counters[("wrc", "2024-01")].failures[0]["reason"]
    assert mongo_spy.upserts == []  # no metadata for a document we do not hold


def test_a_failed_record_is_not_written_to_the_landing_collection(cfg, monkeypatch, item):
    """upsert_landing_record keys its unchanged/changed logic off file_hash, and
    a failed record has none. It goes to the failures collection instead."""
    spider = FakeSpider()
    pipeline, mongo_spy, _ = _pipeline(cfg, monkeypatch, _response(status=404))

    with pytest.raises(DropItem):
        _run(pipeline, item, spider)

    assert mongo_spy.upserts == []
    assert len(mongo_spy.failures) == 1


# --------------------------------------------------------------------------- #
# Extension resolution
# --------------------------------------------------------------------------- #
def test_content_type_wins_over_the_url_extension(cfg, monkeypatch, item):
    """A URL is a claim; a Content-Type is evidence. Requirement 6 branches on
    what the file actually is, and sending a PDF down the BeautifulSoup path
    would be a silent corruption."""
    spider = FakeSpider()
    pipeline, _, store_spy = _pipeline(
        cfg, monkeypatch, _response(headers={"Content-Type": "application/pdf"})
    )

    record = _run(pipeline, item, spider)
    assert store_spy.uploads[0]["ext"] == ".pdf"
    assert record.file_ext == ".pdf"


def test_html_url_and_html_header_agree(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response())
    _run(pipeline, item, spider)
    assert store_spy.uploads[0]["ext"] == ".html"


def test_aspx_url_with_html_header_is_not_a_mismatch(cfg, monkeypatch, item):
    """.aspx, .htm and .html are one family on this site, not a disagreement."""
    spider = FakeSpider()
    item = item.model_copy(update={"file_ext": ".aspx"})
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response())
    _run(pipeline, item, spider)
    assert store_spy.uploads[0]["ext"] == ".html"


def test_no_content_type_falls_back_to_the_url_extension(cfg, monkeypatch, item):
    spider = FakeSpider()
    item = item.model_copy(update={"file_ext": ".pdf"})
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response(headers={}))
    _run(pipeline, item, spider)
    assert store_spy.uploads[0]["ext"] == ".pdf"


def test_nothing_conclusive_defaults_to_html(cfg, monkeypatch, item):
    """Every document sampled during recon was HTML; the default is logged so
    the assumption stays visible."""
    spider = FakeSpider()
    item = item.model_copy(update={"file_ext": None})
    pipeline, _, store_spy = _pipeline(cfg, monkeypatch, _response(headers={}))
    _run(pipeline, item, spider)
    assert store_spy.uploads[0]["ext"] == ".html"


# --------------------------------------------------------------------------- #
# Run bookkeeping
# --------------------------------------------------------------------------- #
def test_close_spider_persists_reconciled_totals(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, mongo_spy, _ = _pipeline(cfg, monkeypatch, _response())

    counters = spider.counters_for("wrc", "2024-01")
    counters.found = 2
    _run(pipeline, item, spider)
    counters.skipped = 1

    pipeline.close_spider(spider)

    action, totals, status = mongo_spy.runs[-1]
    assert action == "finish"
    assert totals["records_found"] == 2
    assert totals["records_scraped"] == 1
    assert totals["records_skipped_unchanged"] == 1
    assert totals["reconciled"] is True
    assert status == "completed"


def test_discrepancy_is_reflected_in_the_run_status(cfg, monkeypatch, item):
    """A run where records vanished unexplained must not be recorded as clean."""
    spider = FakeSpider()
    pipeline, mongo_spy, _ = _pipeline(cfg, monkeypatch, _response())

    spider.counters_for("wrc", "2024-01").found = 10
    pipeline.close_spider(spider)

    _, totals, status = mongo_spy.runs[-1]
    assert totals["reconciled"] is False
    assert status == "completed_with_discrepancies"


def test_non_record_items_pass_through(cfg, monkeypatch, item):
    spider = FakeSpider()
    pipeline, _, _ = _pipeline(cfg, monkeypatch, _response())
    sentinel = {"not": "a record"}
    assert _run(pipeline, sentinel, spider) is sentinel
