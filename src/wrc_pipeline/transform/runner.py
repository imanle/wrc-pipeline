"""The transformation stage.

Reads landing metadata for a date range, fetches each stored document, cleans it
if it is HTML, and writes the result to the curated bucket as ``identifier.ext``
with a fresh hash and its own metadata record.

    python -m wrc_pipeline.transform --start-date 2024-01-01 --end-date 2024-01-31

Landing is immutable
--------------------
Nothing here writes to the landing bucket or the landing collection. The brief is
explicit ("Don't delete/update any of the stored data in the Landing Zone") and
the separation is what makes the stage safe to re-run: if the cleaning logic
improves, the raw capture is still there to re-derive from. Every curated record
therefore carries ``source_file_path`` and ``source_file_hash``, so any output
can be traced back to the exact bytes it came from.

Curated is overwritten, not versioned
-------------------------------------
The opposite policy to landing, on purpose. Curated output is derived, so a
second pass with better selectors *should* replace it -- versioning derived data
would accumulate copies of the same document cleaned different ways, with no way
to say which is current.

Idempotency
-----------
A record is skipped when the curated collection already holds it with the same
``source_file_hash`` AND the same ``cleaner_version``. Both conditions matter:
the first catches "the source has not changed", the second catches "our cleaning
logic has not changed". Skipping on the first alone would make an improved
cleaner silently ineffective.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from ..logging_config import RUN_ID, configure_logging, get_logger
from ..settings import Settings, get_settings
from ..storage import mongo
from ..storage import objectstore as store
from ..storage.objectstore import ObjectStoreError
from .cleaner import CLEANER_VERSION, clean_html, is_html_extension, summarise

log = get_logger(__name__)


@dataclass
class TransformCounters:
    """Found-vs-transformed accounting, mirroring the crawl's counters."""

    found: int = 0
    cleaned: int = 0  # HTML, parsed and reduced
    passed_through: int = 0  # PDF/DOC, stored unchanged
    skipped: int = 0  # already curated from the same source, same cleaner
    failed: int = 0
    flagged: int = 0  # written, but the extraction looks too thin
    failures: list[dict[str, Any]] = field(default_factory=list)

    def record_failure(self, identifier: str, reason: str) -> None:
        self.failed += 1
        self.failures.append({"identifier": identifier, "reason": reason})
        log.error("transform.record_failed", extra={"identifier": identifier, "reason": reason})

    def as_dict(self) -> dict[str, Any]:
        handled = self.cleaned + self.passed_through + self.skipped + self.failed
        return {
            "records_found": self.found,
            "records_cleaned": self.cleaned,
            "records_passed_through": self.passed_through,
            "records_skipped": self.skipped,
            "records_failed": self.failed,
            "records_flagged_thin": self.flagged,
            # Every landing record in range must reach exactly one outcome. This
            # is the transformation stage's equivalent of the crawl's
            # reconciliation, and it is a true identity rather than a
            # comparison against a number the source gave us -- the input is our
            # own collection, so a mismatch is a bug in this file.
            "reconciled": self.found == handled,
        }


def _already_curated(
    record: dict[str, Any],
    settings: Settings,
) -> bool:
    """Whether the curated zone already holds this exact derivation."""
    existing = mongo.curated_collection(settings).find_one(
        {"body_slug": record["body_slug"], "identifier": record["identifier"]},
        projection={"source_file_hash": 1, "cleaner_version": 1},
    )
    if not existing:
        return False
    return (
        existing.get("source_file_hash") == record.get("file_hash")
        and existing.get("cleaner_version") == CLEANER_VERSION
    )


def _curated_record(
    landing: dict[str, Any],
    stored: store.StoredObject,
    cleaning: dict[str, Any] | None,
    quality_flag: str | None,
) -> dict[str, Any]:
    """Build the curated metadata record.

    Carries the landing metadata forward -- the identifier, dates and partition
    are the same facts about the same decision -- and replaces only what the
    transformation changed: the path, the hash, the size. ``source_file_path``
    and ``source_file_hash`` are added so the derivation is traceable, and
    ``_id`` is dropped so Mongo assigns a new one rather than colliding.
    """
    carried = {
        key: landing[key]
        for key in (
            "identifier",
            "identifier_safe",
            "body",
            "body_slug",
            "title",
            "description",
            "case_number",
            "published_date",
            "partition_date",
            "partition_key",
            "source_url",
            "document_url",
            "document_file_url",
            "file_ext",
            "content_type",
        )
        if key in landing
    }

    record = {
        **carried,
        "file_bucket": stored.bucket,
        "file_path": stored.key,
        "file_hash": stored.file_hash,
        "file_size": stored.file_size,
        # Provenance: which landing bytes this was derived from, and by what.
        "source_file_bucket": landing.get("file_bucket"),
        "source_file_path": landing.get("file_path"),
        "source_file_hash": landing.get("file_hash"),
        "cleaner_version": CLEANER_VERSION,
        "transform_run_id": RUN_ID,
    }
    if cleaning:
        record["cleaning"] = cleaning
    if quality_flag:
        record["quality_flag"] = quality_flag
    return record


def transform_record(
    landing: dict[str, Any],
    counters: TransformCounters,
    settings: Settings,
) -> bool:
    """Transform one landing record. Returns whether anything was written."""
    identifier = landing.get("identifier", "<unknown>")

    if not landing.get("file_path") or not landing.get("file_bucket"):
        # A landing record with no file: the crawl recorded it and the download
        # failed. Nothing to transform, and it is already in the crawl's ledger.
        counters.record_failure(identifier, "no_stored_file")
        return False

    if _already_curated(landing, settings):
        counters.skipped += 1
        log.info(
            "transform.skipped_unchanged",
            extra={
                "identifier": identifier,
                "source_file_hash": landing.get("file_hash"),
                "cleaner_version": CLEANER_VERSION,
            },
        )
        return False

    try:
        raw = store.get_bytes(landing["file_bucket"], landing["file_path"], settings)
    except ObjectStoreError as exc:
        counters.record_failure(identifier, f"source_unreadable:{exc}")
        return False

    ext = landing.get("file_ext") or ".html"
    cleaning: dict[str, Any] | None = None
    quality_flag: str | None = None

    if is_html_extension(ext, settings):
        document = clean_html(raw, settings)

        if not document.content:
            # No whitelist selector matched. Failing rather than falling back to
            # the whole page: a fallback would write a document full of
            # navigation that looks superficially fine.
            counters.record_failure(identifier, "no_content_selector_matched")
            return False

        payload: bytes = document.content
        cleaning = summarise(document)

        if document.text_chars < settings.transform.min_content_chars:
            # Flagged, not dropped. Below the threshold the extraction probably
            # grabbed the wrong node -- but losing a document because our
            # selector underperformed is worse than storing it with a warning
            # that can be queried later.
            quality_flag = "thin_content"
            counters.flagged += 1
            log.warning(
                "transform.thin_content",
                extra={
                    "identifier": identifier,
                    "text_chars": document.text_chars,
                    "min_content_chars": settings.transform.min_content_chars,
                    "selector_used": document.selector_used,
                },
            )
        outcome = "cleaned"
    else:
        # Requirement 6a: PDFs and DOCs are stored as they are. The hash is
        # therefore unchanged from the landing copy -- only the key changes, to
        # identifier.ext.
        payload = raw
        outcome = "passed_through"

    try:
        stored = store.put_curated_document(
            body_slug=landing["body_slug"],
            identifier_safe=landing.get("identifier_safe") or landing["identifier"],
            ext=ext,
            data=payload,
            settings=settings,
        )
    except ObjectStoreError as exc:
        counters.record_failure(identifier, f"curated_upload_failed:{exc}")
        return False

    mongo.upsert_curated_record(
        _curated_record(landing, stored, cleaning, quality_flag), settings
    )

    if outcome == "cleaned":
        counters.cleaned += 1
    else:
        counters.passed_through += 1

    log.info(
        "transform.stored",
        extra={
            "identifier": identifier,
            "body": landing["body_slug"],
            "key": stored.key,
            "file_hash": stored.file_hash,
            "file_size": stored.file_size,
            "source_file_hash": landing.get("file_hash"),
            "outcome": outcome,
            "quality_flag": quality_flag,
            **(cleaning or {}),
        },
    )
    return True


def transform_range(
    start_date: date,
    end_date: date,
    body_slug: str | None = None,
    settings: Settings | None = None,
) -> TransformCounters:
    """Transform every landing record published in a date range."""
    settings = settings or get_settings()
    counters = TransformCounters()

    mongo.ensure_indexes(settings)
    store.ensure_buckets(settings)

    log.info(
        "transform.start",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "body": body_slug,
            "cleaner_version": CLEANER_VERSION,
            "curated_bucket": settings.s3.curated_bucket,
        },
    )

    for landing in mongo.iter_landing_records(start_date, end_date, body_slug, settings):
        counters.found += 1
        transform_record(landing, counters, settings)

    summary = counters.as_dict()
    log.info("transform.summary", extra=summary)
    return counters


def build_parser() -> argparse.ArgumentParser:
    cfg = get_settings()
    parser = argparse.ArgumentParser(
        prog="wrc-transform", description="Transform landing documents into the curated zone."
    )
    parser.add_argument("--start-date", default=cfg.partitions.start_date.isoformat())
    parser.add_argument("--end-date", default=cfg.partitions.end_date.isoformat())
    parser.add_argument("--body", default=None, help="body slug (default: all)")
    parser.add_argument("--log-level", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = get_settings()
    configure_logging(
        args.log_level or cfg.logging.level,
        log_file=cfg.logging.file,
        console=cfg.logging.console,
    )

    counters = transform_range(
        datetime.strptime(args.start_date, "%Y-%m-%d").date(),
        datetime.strptime(args.end_date, "%Y-%m-%d").date(),
        args.body,
        cfg,
    )

    # Non-zero when a landing record did not reach an outcome. Unlike the crawl's
    # check, this compares our own collection against our own processing, so a
    # failure here is always a bug or an infrastructure fault -- never the
    # source misbehaving.
    return 0 if counters.as_dict()["reconciled"] else 1


if __name__ == "__main__":
    raise SystemExit(main())