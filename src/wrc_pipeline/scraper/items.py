"""The metadata record.

One Pydantic model shared by every stage: the spider builds it, the download
pipeline completes it, MongoDB stores it, the transformation stage reads it back,
and the Dagster assets count it.

Why not ``scrapy.Item``
-----------------------
``scrapy.Item`` is a dict with declared keys and no validation, and it only makes
sense inside Scrapy. Half the consumers of this record -- the transformation
script and the Dagster assets -- never import Scrapy, and would end up passing
plain dicts around. A single Pydantic model means all four stages validate
against the same type, and a field renamed here is a type error there rather
than a silently missing key in Mongo.

Two-phase lifecycle
-------------------
A record exists before its document has been downloaded::

    spider parses the listing   ->  identifier, dates, URLs known
                                    file_hash / file_path unknown
    download pipeline runs      ->  attach_stored_object() fills the rest

So the file fields are optional at construction. This is the real sequence of
events, not laxness: making them required would force the spider to invent
placeholder values, and a placeholder hash is worse than no hash.

Dates
-----
The model holds real ``date`` objects so ``2024-13-45`` cannot get in. MongoDB
receives ISO strings, because ``mongo.py`` queries ranges with ``.isoformat()``
and ``YYYY-MM-DD`` compares correctly as text. :meth:`DocumentRecord.to_mongo`
is the single conversion point between the two.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..logging_config import RUN_ID
from ..settings import Settings, get_settings
from ..storage.objectstore import StoredObject


class RecordStatus(str, Enum):
    """Fate of one record within a run.

    Mirrors the buckets in :class:`~wrc_pipeline.logging_config.PartitionCounters`
    so the reconciliation assertion (``found == scraped + skipped + failed``) can
    be checked against the database as well as against the in-memory counters --
    two independent views of the same number.
    """

    PENDING = "pending"    # parsed from the listing, not yet downloaded
    SCRAPED = "scraped"    # downloaded and stored this run
    SKIPPED = "skipped"    # already held, content unchanged -> idempotent no-op
    FAILED = "failed"      # could not be stored; reason is in the failure ledger


def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp, matching ``mongo.py``."""
    return datetime.now(tz=timezone.utc)


class DocumentRecord(BaseModel):
    """Metadata for one decision or determination (requirement 4).

    ``extra="forbid"`` is deliberate. Without it a typo such as ``file_has=...``
    is accepted, dropped, and only noticed when a query returns nothing several
    hundred records later. With it, the typo is an immediate error naming the
    offending key.

    ``str_strip_whitespace`` because values are scraped from HTML attributes and
    leading/trailing whitespace is the norm, not the exception.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    # --- identity ---------------------------------------------------------- #
    # The site's own reference, verbatim. Kept raw for fidelity even though it
    # can contain slashes and spaces (`IR - SC - 00000787`, `CD/12/572`).
    identifier: str = Field(min_length=1)
    # Key-safe form, DERIVED below -- never passed in by a caller. If this were
    # an ordinary field, one caller would eventually omit it and a slash-bearing
    # reference would silently create extra prefixes in the object store.
    identifier_safe: str = ""

    # --- provenance -------------------------------------------------------- #
    body: str = Field(min_length=1)       # human-readable tribunal name
    body_slug: str = Field(min_length=1)  # config slug; half of the unique index

    # --- content metadata -------------------------------------------------- #
    # On this site the listing's title IS the identifier (`h2.title@title`), so
    # it defaults to it below. Kept as a distinct field because the brief names
    # it and because a second source will populate it differently.
    title: str | None = None
    description: str | None = None
    published_date: date

    # --- URLs -------------------------------------------------------------- #
    # The search page this record was found on -- lets any record be traced back
    # to the exact (body, partition, page) request that produced it.
    source_url: str
    # The document itself, taken from the site's own href. NEVER rebuilt from the
    # identifier: three incompatible manglings were observed during recon.
    document_url: str

    # --- partitioning (requirement 3) -------------------------------------- #
    partition_date: date
    partition_key: str = Field(min_length=1)

    # --- storage (requirements 7 and 8; filled in after download) ---------- #
    # Bucket-relative key, with the bucket stored beside it rather than a single
    # `s3://bucket/key` URI. The transformation stage can then call
    # get_bytes(bucket, key) directly instead of string-parsing a URI apart.
    file_bucket: str | None = None
    file_path: str | None = None
    file_hash: str | None = None  # SHA-256 of the bytes stored (requirement 8)
    # SHA-256 after per-request noise is stripped. Stored because it, not
    # file_hash, is what makes change detection stable: these pages carry an
    # ASP.NET render timer, so their raw bytes differ on every fetch.
    content_hash: str | None = None
    file_ext: str | None = None
    content_type: str | None = None
    file_size: int | None = Field(default=None, ge=0)

    # --- run bookkeeping --------------------------------------------------- #
    status: RecordStatus = RecordStatus.PENDING
    scraped_at: datetime = Field(default_factory=_utcnow)
    run_id: str = RUN_ID

    # ------------------------------------------------------------------ #
    # Derivations
    # ------------------------------------------------------------------ #
    @field_validator("identifier", "body_slug", "partition_key")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        """Reject whitespace-only values.

        ``min_length=1`` alone would accept ``" "``. These three fields form the
        unique index and the object key, and a blank one produces a record that
        is technically valid and completely useless.
        """
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    @model_validator(mode="after")
    def _derive_defaults(self) -> DocumentRecord:
        """Fill ``identifier_safe`` and ``title`` from the identifier.

        Mutating in a post-validator rather than using ``default_factory``
        because both derive from another field, which a factory cannot see.
        ``object.__setattr__`` is not needed -- the model is not frozen, since
        the download pipeline legitimately completes the record later.
        """
        if not self.identifier_safe:
            self.identifier_safe = get_settings().scraping.safe_identifier(self.identifier)
        if self.title is None:
            self.title = self.identifier
        return self

    # ------------------------------------------------------------------ #
    # Integrity helpers
    # ------------------------------------------------------------------ #
    @property
    def date_in_partition_window(self) -> bool:
        """Whether ``published_date`` falls inside the partition it came from.

        Deliberately a property and NOT a validator. The site's ``from``/``to``
        filter matches the *decision* date while the listing displays a date that
        does not always agree -- recon found documents under ``/2010/december/``
        with references ending ``_2009``. A hard validator would therefore reject
        legitimate records. The spider logs this as a warning instead, so the
        discrepancy is visible without being fatal.
        """
        return self.partition_date <= self.published_date

    @property
    def is_stored(self) -> bool:
        """True once the document has actually been written to object storage."""
        return bool(self.file_path and self.file_hash and self.file_bucket)

    # ------------------------------------------------------------------ #
    # Stage transitions
    # ------------------------------------------------------------------ #
    def attach_stored_object(
        self,
        stored: StoredObject,
        status: RecordStatus | None = None,
    ) -> DocumentRecord:
        """Complete the record from an object-store result.

        This is the seam between ``objectstore.py`` and ``mongo.py``: the upload
        returns a :class:`StoredObject`, and every field the metadata record
        needs comes from it. Centralised here so the mapping exists once instead
        of being re-derived inside the download pipeline.

        Returns a new instance rather than mutating in place, so a caller cannot
        accidentally half-update a record and write it.

        ``status`` defaults to the outcome the upload reported -- an unchanged
        file becomes ``SKIPPED``, which is what makes a second run over the same
        range visibly idempotent rather than merely quiet.
        """
        from ..storage.objectstore import UploadOutcome  # local: avoids cycles

        if status is None:
            status = (
                RecordStatus.SKIPPED
                if stored.outcome is UploadOutcome.SKIPPED_UNCHANGED
                else RecordStatus.SCRAPED
            )

        return self.model_copy(
            update={
                "file_bucket": stored.bucket,
                "file_path": stored.key,
                "file_hash": stored.file_hash,
                "content_hash": stored.content_hash,
                "file_size": stored.file_size,
                "content_type": stored.content_type,
                "status": status,
            }
        )

    def mark_failed(self) -> DocumentRecord:
        """Flag a record whose document could not be stored.

        The record is still written: requirement 10 asks that every unscraped
        record be accounted for, and a row saying "we saw this and failed" is
        strictly more useful than a missing row.
        """
        return self.model_copy(update={"status": RecordStatus.FAILED})

    # ------------------------------------------------------------------ #
    # Serialisation
    # ------------------------------------------------------------------ #
    def to_mongo(self) -> dict[str, Any]:
        """Render as the document ``mongo.py`` expects.

        Dates become ISO strings. ``mongo.iter_landing_records`` queries ranges
        with ``.isoformat()`` and ``YYYY-MM-DD`` orders correctly as text, so the
        two layers agree; a mismatch here would produce a date-range query that
        silently returns nothing, which is the worst kind of bug because it looks
        like an empty result rather than a failure.
        """
        data = self.model_dump(mode="json")
        data["status"] = self.status.value
        return data

    @classmethod
    def from_mongo(cls, document: dict[str, Any]) -> DocumentRecord:
        """Rebuild a record from a stored document (the transformation's input).

        Drops ``_id`` and the bookkeeping fields ``mongo.py`` adds on write
        (``first_seen_at``, ``versions``, ...), which the model does not declare
        and would reject under ``extra="forbid"``. Round-tripping through the
        model means the transformation stage gets validated objects rather than
        raw dicts whose shape it has to trust.
        """
        known = set(cls.model_fields)
        return cls.model_validate({k: v for k, v in document.items() if k in known})


def record_from_listing(
    identifier: str,
    body_slug: str,
    published_date: date,
    source_url: str,
    document_url: str,
    partition_date: date,
    partition_key: str,
    description: str | None = None,
    file_ext: str | None = None,
    settings: Settings | None = None,
) -> DocumentRecord:
    """Build a PENDING record from one parsed listing entry.

    Convenience for the spider: it holds a ``body_slug`` and config, not the
    human-readable body name, and looking that up in the spider would mean
    every call site repeating the same two lines.
    """
    settings = settings or get_settings()
    return DocumentRecord(
        identifier=identifier,
        body=settings.scraping.body_by_slug(body_slug).name,
        body_slug=body_slug,
        description=description,
        published_date=published_date,
        source_url=source_url,
        document_url=document_url,
        partition_date=partition_date,
        partition_key=partition_key,
        file_ext=file_ext,
    )