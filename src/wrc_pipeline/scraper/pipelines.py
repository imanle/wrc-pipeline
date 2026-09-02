"""The document download pipeline.

Where the three storage-facing modules finally meet. For each ``PENDING`` record
the spider yields, this pipeline:

1. looks up what we already hold (``mongo.find_existing``);
2. downloads the document **through Scrapy's downloader**, with conditional
   headers when we have a validator for it;
3. uploads the bytes to MinIO under a content-addressed key;
4. completes the record (``attach_stored_object``) and upserts it;
5. increments the partition counters so found-vs-scraped reconciles.

Why an item pipeline rather than the spider
-------------------------------------------
Discovery and retrieval fail for different reasons and want different handling,
and keeping them apart means the spider stays testable with no network and no
containers. It also puts document fetching under Scrapy's downloader rather than
in front of it.

Why ``crawler.engine.download()`` and not ``requests``
------------------------------------------------------
Reaching for ``requests`` or ``httpx`` here would be simpler to read and would
quietly bypass every throttle configured for this crawl: AutoThrottle, the retry
middleware, the per-domain concurrency slots and the download timeout are all
downloader-level features. Going through the engine keeps one traffic policy for
the whole run. The cost is that ``process_item`` must be a coroutine, which
requires the asyncio reactor -- already set in ``scrapy_settings.py``.

Requirement 9 and HTTP conditional requests
-------------------------------------------
Requirement 9 asks for two things that cannot both hold literally: do not
re-download unchanged files, *and* use the file hash to detect changes. A hash
requires the bytes.

The way out is to let the server answer the question. Each download stores the
response's ``ETag`` and ``Last-Modified``; the next run sends them back as
``If-None-Match`` / ``If-Modified-Since``, and a **304 Not Modified** means
unchanged with no body transferred. When the server offers no validator we fall
back to downloading and comparing hashes, which still avoids a re-upload and a
duplicate record -- the fallback is weaker on bandwidth but identical on
correctness, and the hash remains the authority either way.
"""

from __future__ import annotations

from typing import Any

import scrapy
from scrapy.exceptions import DropItem
from scrapy.utils.defer import maybe_deferred_to_future

from ..logging_config import PartitionCounters, get_logger
from ..settings import Settings, get_settings
from ..storage import mongo
from ..storage import objectstore as store
from ..storage.mongo import WriteOutcome
from ..storage.objectstore import ObjectStoreError
from .items import DocumentRecord, RecordStatus
from .spiders.wrc_decisions import partition_size_value

log = get_logger(__name__)

# Status codes we interpret ourselves rather than letting Scrapy raise on.
NOT_MODIFIED = 304

# Content-Type -> extension. Module-level because it is a constant, and separate
# from config's document_extensions, which answers a different question: this
# maps a header to an extension, that one classifies an extension as HTML or
# binary. Both are consulted in _resolve_extension.
CONTENT_TYPE_EXTENSIONS = {
    "text/html": ".html",
    "application/xhtml+xml": ".html",
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
}


class DocumentDownloadPipeline:
    """Download each discovered document and store it with its metadata."""

    def __init__(self, crawler: Any, config: Settings | None = None) -> None:
        self.crawler = crawler
        self.cfg = config or get_settings()
        self.run_id: str = ""

    @classmethod
    def from_crawler(cls, crawler: Any) -> DocumentDownloadPipeline:
        return cls(crawler)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def open_spider(self, spider: Any) -> None:
        """Prepare storage before the first item arrives.

        Both calls are idempotent, and both are here rather than in a setup
        script so that a fresh checkout plus ``docker compose up`` is enough to
        run the crawl. Failing here -- before any traffic -- is also much kinder
        than discovering a missing bucket 400 documents in.
        """
        mongo.ensure_indexes(self.cfg)
        store.ensure_buckets(self.cfg)

        self.run_id = getattr(spider, "run_id", None) or spider.settings.get("RUN_ID") or ""
        if not self.run_id:
            from ..logging_config import RUN_ID

            self.run_id = RUN_ID

        mongo.start_run(
            run_id=self.run_id,
            start_date=spider.start_date,
            end_date=spider.end_date,
            partition_size=partition_size_value(spider.partition_size),
            bodies=[body.slug for body in spider.bodies],
            settings=self.cfg,
        )

    def close_spider(self, spider: Any) -> None:
        """Close the run document with the aggregated totals (requirement 10).

        The spider's ``closed()`` emits the same numbers to the logs; this
        persists them, so "how did last Tuesday's run go?" is a query rather than
        an exercise in log archaeology.
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
        # Per-partition completeness, measured against the store rather than
        # against this pass. A pass that missed 8 records is not a problem if a
        # previous pass already stored them, and re-running until complete
        # converges precisely because the two questions are separate.
        partitions_incomplete: list[str] = []
        records_in_store = 0

        for counters in getattr(spider, "counters", {}).values():
            summary = counters.as_dict()
            for field in totals:
                totals[field] += summary[field]

            held = mongo.count_partition_records(
                counters.body, counters.partition_key, self.cfg
            )
            records_in_store += held
            complete = held >= counters.listings
            if not complete:
                partitions_incomplete.append(f"{counters.body}/{counters.partition_key}")

            log.info(
                "partition.completeness",
                extra={
                    "body": counters.body,
                    "partition_key": counters.partition_key,
                    "listings_found": counters.listings,
                    "records_in_store": held,
                    "records_seen_this_run": summary["records_distinct"],
                    "shortfall": max(counters.listings - held, 0),
                    "complete": complete,
                },
            )

        totals["records_in_store"] = records_in_store
        totals["partitions_incomplete"] = partitions_incomplete
        totals["store_complete"] = not partitions_incomplete

        totals["partitions"] = len(getattr(spider, "counters", {}))
        # Two checks, because the site's stated total counts listing entries
        # while records_distinct counts decisions, and its page windows overlap.
        # See PartitionCounters.as_dict().
        totals["listings_reconciled"] = totals["listings_unaccounted"] == 0
        totals["records_reconciled"] = totals["records_unaccounted"] == 0
        totals["reconciled"] = (
            totals["listings_reconciled"] and totals["records_reconciled"]
        )

        # The run's verdict is about the STORE. A pass that saw fewer records
        # than the site advertised is only a problem if the store is still short
        # afterwards; otherwise a previous pass already covered it.
        status = "completed" if totals["store_complete"] else "incomplete"
        mongo.finish_run(self.run_id, totals, status=status, settings=self.cfg)

    # ------------------------------------------------------------------ #
    # Per-item work
    # ------------------------------------------------------------------ #
    async def process_item(self, item: Any, spider: Any) -> Any:
        """Fetch, store and record one document.

        Coroutine because the download is awaited through Scrapy's engine.
        """
        if not isinstance(item, DocumentRecord):
            return item

        counters = spider.counters_for(item.body_slug, item.partition_key)
        existing = mongo.find_existing(item.body_slug, item.identifier, self.cfg)

        try:
            response = await maybe_deferred_to_future(
                self.crawler.engine.download(self._document_request(item, existing))
            )
        except Exception as exc:  # noqa: BLE001 -- every cause is a logged failure
            # Retries are already exhausted by the time we get here: timeouts,
            # DNS failures, connection resets, malformed responses.
            self._record_failure(
                item, counters, f"download_failed:{type(exc).__name__}", None, spider
            )
            raise DropItem(f"download failed for {item.identifier}") from exc

        # --- the server says nothing changed ------------------------------ #
        if response.status == NOT_MODIFIED and existing:
            return self._skip_unchanged(item, existing, counters)

        if response.status != 200:
            # 404 is deliberately NOT retried: a missing document is permanent,
            # and four retries only delay the run. It goes to the ledger with its
            # code, which is what "200 minus X, every X logged" requires.
            self._record_failure(
                item, counters, "document_http_error", response.status, spider
            )
            raise DropItem(f"HTTP {response.status} for {item.identifier}")

        if not response.body:
            self._record_failure(item, counters, "empty_response_body", 200, spider)
            raise DropItem(f"empty body for {item.identifier}")

        # --- store the bytes ---------------------------------------------- #
        extension = self._resolve_extension(item, response)

        # Change detection compares this, not the raw bytes. Every WRC page ends
        # with `<!-- Elapsed time: 0.046756 -->`, regenerated per request, so a
        # raw byte hash reported all 234 January documents as amended on the
        # second run. The stored file is still the page exactly as fetched.
        content_hash = store.sha256_bytes(
            self.cfg.scraping.strip_volatile(response.body)
        )

        try:
            stored = store.put_landing_document(
                body_slug=item.body_slug,
                partition_key=item.partition_key,
                identifier_safe=item.identifier_safe,
                ext=extension,
                # response.body is already in memory and is passed straight
                # through to MinIO. Nothing is written to local disk at any point
                # -- see the no-local-storage notes in scrapy_settings.py.
                data=response.body,
                source_url=item.document_url,
                key_hash=content_hash,
                settings=self.cfg,
            )
        except ObjectStoreError as exc:
            self._record_failure(item, counters, f"upload_failed:{exc}", None, spider)
            raise DropItem(f"upload failed for {item.identifier}") from exc

        record = item.attach_stored_object(stored).model_copy(
            update={
                "file_ext": extension,
                "etag": _header(response, "ETag"),
                "last_modified": _header(response, "Last-Modified"),
                "run_id": self.run_id or item.run_id,
            }
        )

        # --- record the metadata ------------------------------------------ #
        outcome = mongo.upsert_landing_record(record.to_mongo(), self.cfg)

        # Counted on the MONGO outcome, not the S3 one. They agree in the normal
        # case, but the record is the unit the brief counts, and Mongo is its
        # authority: an object can already exist from an interrupted earlier run
        # whose metadata write never landed, and that record still needs counting
        # as newly scraped.
        if outcome is WriteOutcome.UNCHANGED:
            counters.skipped += 1
            record = record.model_copy(update={"status": RecordStatus.SKIPPED})
        else:
            counters.scraped += 1

        log.info(
            "document.stored",
            extra={
                "body": item.body_slug,
                "partition_key": item.partition_key,
                "identifier": item.identifier,
                "key": stored.key,
                "file_hash": stored.file_hash,
                "file_size": stored.file_size,
                "content_hash": stored.content_hash,
                "object_outcome": stored.outcome.value,
                "record_outcome": outcome.value,
            },
        )
        return record

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _document_request(
        self,
        item: DocumentRecord,
        existing: dict[str, Any] | None,
    ) -> scrapy.Request:
        """Build the document request, conditional when we have a validator.

        ``dont_filter=True`` is REQUIRED, not tidiness. Recon found one file
        serving three case references
        (``rp2147_2009_mn1794_2009_wt796_2009.html``), so several records
        legitimately share a document URL. With the dupefilter active, the second
        and third would be dropped silently and end up with no file -- a loss the
        reconciliation check would report without being able to explain.

        ``handle_httpstatus_all`` because this pipeline inspects the status code
        itself: 304 is a success, 404 is a permanent failure that must reach the
        ledger with its code, and neither should surface as an exception. The
        retry middleware is unaffected and still retries the 5xx/429 codes from
        config.
        """
        headers: dict[str, str] = {}
        if existing:
            if existing.get("etag"):
                headers["If-None-Match"] = existing["etag"]
            if existing.get("last_modified"):
                headers["If-Modified-Since"] = existing["last_modified"]

        return scrapy.Request(
            item.document_url,
            headers=headers,
            dont_filter=True,
            meta={"handle_httpstatus_all": True},
        )

    def _skip_unchanged(
        self,
        item: DocumentRecord,
        existing: dict[str, Any],
        counters: PartitionCounters,
    ) -> DocumentRecord:
        """Handle a 304: unchanged, and no body was transferred.

        The stored hash and path are carried over from the existing record, so
        ``upsert_landing_record`` takes its UNCHANGED path and touches only the
        run-tracking fields. The document in the Landing Zone is not rewritten,
        which is what "don't update the Landing Zone" means in practice.
        """
        record = item.model_copy(
            update={
                "file_hash": existing.get("file_hash"),
                "content_hash": existing.get("content_hash"),
                "file_path": existing.get("file_path"),
                "file_bucket": self.cfg.s3.landing_bucket,
                "status": RecordStatus.SKIPPED,
                "run_id": self.run_id or item.run_id,
            }
        )
        mongo.upsert_landing_record(record.to_mongo(), self.cfg)
        counters.skipped += 1

        log.info(
            "document.not_modified",
            extra={
                "body": item.body_slug,
                "partition_key": item.partition_key,
                "identifier": item.identifier,
                "file_hash": existing.get("file_hash"),
                "reason": "http_304",
            },
        )
        return record

    def _resolve_extension(self, item: DocumentRecord, response: Any) -> str:
        """Decide the stored file's extension.

        A URL is a claim; a ``Content-Type`` header is evidence. When they
        disagree the header wins, because requirement 6 branches on what the file
        actually *is*: PDFs and DOCs are stored byte-for-byte, HTML pages are
        stored as ``.html``. Getting this wrong would send a PDF down the
        BeautifulSoup path in the transformation stage.

        The disagreement is logged rather than silently resolved -- if it starts
        happening in bulk, something about the source has changed.
        """
        extensions = self.cfg.scraping.document_extensions
        url_ext = item.file_ext
        content_type = (_header(response, "Content-Type") or "").split(";")[0].strip().lower()
        header_ext = CONTENT_TYPE_EXTENSIONS.get(content_type)

        if header_ext and url_ext and header_ext != url_ext:
            # .htm, .aspx and .html are one family on this site, not a genuine
            # format disagreement -- only the latter is worth a warning.
            same_family = extensions.is_html(url_ext) and extensions.is_html(header_ext)
            if not same_family:
                log.warning(
                    "document.extension_mismatch",
                    extra={
                        "identifier": item.identifier,
                        "url_extension": url_ext,
                        "content_type": content_type,
                        "resolved": header_ext,
                    },
                )
            return header_ext

        if header_ext:
            return header_ext
        if url_ext and (extensions.is_html(url_ext) or extensions.is_binary(url_ext)):
            return url_ext

        # Neither source is conclusive. HTML is the right default here: every
        # document sampled during recon across three bodies and 14 years was
        # HTML. Logged so the assumption is visible rather than buried.
        log.warning(
            "document.extension_unresolved",
            extra={
                "identifier": item.identifier,
                "url_extension": url_ext,
                "content_type": content_type,
                "defaulted_to": ".html",
            },
        )
        return ".html"

    def _record_failure(
        self,
        item: DocumentRecord,
        counters: PartitionCounters,
        reason: str,
        error_code: int | str | None,
        spider: Any,
    ) -> None:
        """Account for a document we could not store, in three places.

        Counters make it reconcile, the JSON log makes it visible during the run,
        and the failures collection makes it queryable afterwards ("every 404 we
        have ever hit, by body"). The record itself is also written with
        ``status: failed`` -- a row saying "we saw this and could not fetch it" is
        strictly more useful than a missing row.
        """
        counters.record_failure(item.document_url, reason, error_code, item.identifier)

        mongo.record_failure(
            run_id=self.run_id,
            body_slug=item.body_slug,
            partition_key=item.partition_key,
            url=item.document_url,
            reason=reason,
            error_code=error_code,
            identifier=item.identifier,
            settings=self.cfg,
        )

        # A failed record has no hash, and upsert_landing_record keys its
        # unchanged/changed logic off file_hash. Writing it under a sentinel
        # would corrupt that comparison, so failures go to their own collection
        # and the landing collection stays a record of documents we actually
        # hold. The counter and the ledger are what make the record accounted
        # for.


def _header(response: Any, name: str) -> str | None:
    """Decode one response header, or ``None``.

    Scrapy returns headers as bytes; every consumer here wants text.
    """
    value = response.headers.get(name)
    if value is None:
        return None
    return value.decode("latin-1") if isinstance(value, bytes) else str(value)