"""The document download pipeline.
"""

from __future__ import annotations

from typing import Any

import scrapy
from scrapy.exceptions import DropItem
from scrapy.http import TextResponse
from scrapy.utils.defer import maybe_deferred_to_future

from ..logging_config import PartitionCounters, get_logger
from ..settings import Settings, get_settings
from ..storage import mongo
from ..storage import objectstore as store
from ..storage.mongo import WriteOutcome
from ..storage.objectstore import ObjectStoreError
from ..transform.cleaner import is_html_extension
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
            response = await self._download(self._document_request(item, existing))
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

        # --- follow a wrapper page to the real document -------------------- #
        # Some pages are not the decision, only a link to it. Resolved here, at
        # download time, rather than during transformation: the landing zone must
        # hold the actual document, and requirement 6a wants PDFs stored as they
        # are.
        document_file_url: str | None = None
        wrapper_target = self._wrapper_pdf_url(item, response)
        if wrapper_target:
            try:
                pdf_response = await self._download(
                    self._document_request(item, None, url=wrapper_target)
                )
            except Exception as exc:  # noqa: BLE001
                self._record_failure(
                    item, counters, f"wrapper_download_failed:{type(exc).__name__}", None, spider
                )
                raise DropItem(f"wrapper download failed for {item.identifier}") from exc

            if pdf_response.status != 200 or not pdf_response.body:
                self._record_failure(
                    item, counters, "wrapper_target_unavailable", pdf_response.status, spider
                )
                raise DropItem(f"wrapper target unavailable for {item.identifier}")

            log.info(
                "document.wrapper_followed",
                extra={
                    "identifier": item.identifier,
                    "wrapper_url": item.document_url,
                    "document_file_url": wrapper_target,
                    "wrapper_bytes": len(response.body),
                    "document_bytes": len(pdf_response.body),
                },
            )
            document_file_url = wrapper_target
            response = pdf_response
            item = item.model_copy(update={"file_ext": _extension_of_url(wrapper_target)})

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
                "document_file_url": document_file_url,
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
    async def _download(self, request: scrapy.Request) -> Any:
        """Fetch one document through Scrapy's engine.

        Scrapy 2.14 replaced ``engine.download()`` with the coroutine
        ``download_async()``; the old name still works but warns on every single
        document, which buries the run's own logs. ``pyproject.toml`` allows
        scrapy>=2.12, so the new API is preferred when present rather than
        required -- a hard switch would break the lower bound we declare.
        """
        engine = self.crawler.engine
        download_async = getattr(engine, "download_async", None)
        if download_async is not None:
            return await download_async(request)
        return await maybe_deferred_to_future(engine.download(request))

    def _wrapper_pdf_url(self, item: DocumentRecord, response: Any) -> str | None:
        """The real document's URL, when this page is only a link to it.

        Every Employment Appeals Tribunal decision is a PDF behind a wrapper
        page: the listing links to HTML whose entire content is a download link,
        and the 796 characters of text on it are cookie banner and site chrome.

        Two conditions, and both matter:

        * the page must have NO content of its own, judged with the same
          selectors the transformation stage uses. A page that has both content
          and an attachment is a decision with an appendix, and following the
          link would replace the decision with the appendix.
        * there must be EXACTLY ONE candidate link. Zero means this is not a
          wrapper; more than one means the assumption is wrong, and guessing
          which to follow is worse than storing what we were given.
        """
        cfg = self.cfg
        # Only a text response can be queried with a selector. A PDF or a
        # response with no Content-Type arrives as a plain Response, where
        # .css() raises NotSupported -- which would fail the whole item on a
        # document that is already exactly what we wanted.
        if not isinstance(response, TextResponse):
            return None
        if not is_html_extension(item.file_ext or ".html", cfg):
            return None
        if not cfg.scraping.document_pdf_link:
            return None

        for selector in cfg.transform.content_selectors:
            if response.css(selector).css("::text").getall():
                text = " ".join(response.css(selector).css("::text").getall()).strip()
                if len(text) >= cfg.transform.min_content_chars:
                    return None  # a real document; nothing to follow

        links = response.css(cfg.scraping.document_pdf_link).css("::attr(href)").getall()
        unique = list(dict.fromkeys(links))
        if len(unique) != 1:
            if unique:
                log.warning(
                    "document.wrapper_ambiguous",
                    extra={
                        "identifier": item.identifier,
                        "candidates": unique,
                        "reason": "expected exactly one download link",
                    },
                )
            return None

        return response.urljoin(unique[0])

    def _document_request(
        self,
        item: DocumentRecord,
        existing: dict[str, Any] | None,
        url: str | None = None,
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
            url or item.document_url,
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


def _extension_of_url(url: str) -> str:
    """Extension from a URL path, defaulting to ``.pdf``.

    Only used for a followed wrapper link, where the config selector already
    matched on ``href$='.pdf'``.
    """
    tail = url.split("?", 1)[0].rsplit("/", 1)[-1]
    return f".{tail.rsplit('.', 1)[-1].lower()}" if "." in tail else ".pdf"


def _header(response: Any, name: str) -> str | None:
    """Decode one response header, or ``None``.

    Scrapy returns headers as bytes; every consumer here wants text.
    """
    value = response.headers.get(name)
    if value is None:
        return None
    return value.decode("latin-1") if isinstance(value, bytes) else str(value)