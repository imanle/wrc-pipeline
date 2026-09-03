"""The metadata record.

One Pydantic model shared by every stage: the spider builds it, the download
pipeline completes it, MongoDB stores it, the transformation stage reads it back,
and the Dagster assets count it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..logging_config import RUN_ID
from ..settings import Settings, get_settings
from ..storage.objectstore import StoredObject


class RecordStatus(str, Enum):
    """Fate of one record within a run.
    """

    PENDING = "pending"    # parsed from the listing, not yet downloaded
    SCRAPED = "scraped"    # downloaded and stored this run
    SKIPPED = "skipped"    # already held, content unchanged -> idempotent no-op
    FAILED = "failed"      # could not be stored; reason is in the failure ledger


def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp, matching ``mongo.py``."""
    return datetime.now(tz=UTC)


class DocumentRecord(BaseModel):
    """Metadata for one decision or determination .
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    identifier: str = Field(min_length=1)
    identifier_safe: str = ""
    body: str = Field(min_length=1)       
    body_slug: str = Field(min_length=1)  
    title: str | None = None
    description: str | None = None
    case_number: str | None = None
    published_date: date
    source_url: str
    document_url: str
    document_file_url: str | None = None
    partition_date: date
    partition_key: str = Field(min_length=1)
    file_bucket: str | None = None
    file_path: str | None = None
    file_hash: str | None = None  # SHA-256 of the bytes stored (requirement 8)
    content_hash: str | None = None
    file_ext: str | None = None
    content_type: str | None = None
    file_size: int | None = Field(default=None, ge=0)
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
        """
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    @model_validator(mode="after")
    def _derive_defaults(self) -> DocumentRecord:
        """Fill ``identifier_safe`` and ``title`` from the identifier.
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
        """
        return self.model_copy(update={"status": RecordStatus.FAILED})

    # ------------------------------------------------------------------ #
    # Serialisation
    # ------------------------------------------------------------------ #
    def to_mongo(self) -> dict[str, Any]:
        """Render as the document ``mongo.py`` expects.
        """
        data = self.model_dump(mode="json")
        data["status"] = self.status.value
        return data

    @classmethod
    def from_mongo(cls, document: dict[str, Any]) -> DocumentRecord:
        """Rebuild a record from a stored document (the transformation's input).
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
    case_number: str | None = None,
    file_ext: str | None = None,
    settings: Settings | None = None,
) -> DocumentRecord:
    """Build a PENDING record from one parsed listing entry.
    """
    settings = settings or get_settings()
    return DocumentRecord(
        identifier=identifier,
        body=settings.scraping.body_by_slug(body_slug).name,
        body_slug=body_slug,
        description=description,
        case_number=case_number,
        published_date=published_date,
        source_url=source_url,
        document_url=document_url,
        partition_date=partition_date,
        partition_key=partition_key,
        file_ext=file_ext,
    )