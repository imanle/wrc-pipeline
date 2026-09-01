"""Tests for the metadata record model.

Pure -- no MongoDB, no MinIO, no network. Everything here is validation logic and
field mapping, which is exactly the kind of code that looks obviously correct and
quietly is not.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from wrc_pipeline.scraper.items import DocumentRecord, RecordStatus, record_from_listing
from wrc_pipeline.storage.objectstore import StoredObject, UploadOutcome


def _record(**overrides) -> DocumentRecord:
    fields = {
        "identifier": "ADJ-00054658",
        "body": "Workplace Relations Commission",
        "body_slug": "wrc",
        "description": "Declan Holden V Ger Brennan Construction",
        "published_date": date(2024, 1, 17),
        "source_url": "https://www.workplacerelations.ie/en/search/?decisions=1&body=15376",
        "document_url": "https://www.workplacerelations.ie/en/cases/2024/january/adj-00054658.html",
        "partition_date": date(2024, 1, 1),
        "partition_key": "2024-01",
        "file_ext": ".html",
    }
    fields.update(overrides)
    return DocumentRecord(**fields)


# --------------------------------------------------------------------------- #
# Construction and defaults
# --------------------------------------------------------------------------- #
def test_new_record_is_pending_and_unstored():
    """The spider creates records before anything is downloaded."""
    record = _record()
    assert record.status is RecordStatus.PENDING
    assert record.is_stored is False
    assert record.file_hash is None


def test_identifier_safe_is_derived():
    """Never passed in by callers -- a forgotten value would corrupt object keys."""
    assert _record(identifier="CD/12/572").identifier_safe == "CD-12-572"


def test_identifier_safe_handles_spaces():
    """Recon found references such as `IR - SC - 00000787` with literal spaces."""
    assert _record(identifier="IR - SC - 00000787").identifier_safe == "IR-SC-00000787"


def test_title_defaults_to_the_identifier():
    """On this site the listing's title attribute IS the reference."""
    assert _record().title == "ADJ-00054658"


def test_explicit_title_is_kept():
    assert _record(title="A different title").title == "A different title"


def test_scraped_at_is_timezone_aware():
    """Naive timestamps make cross-run comparison unreliable when the pipeline
    and the data live in different regions."""
    assert _record().scraped_at.tzinfo is not None


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def test_blank_identifier_is_rejected():
    """A whitespace-only identifier passes min_length=1 but is useless: it is
    half of the unique index and part of every object key."""
    with pytest.raises(ValidationError):
        _record(identifier="   ")


def test_missing_required_field_is_rejected():
    with pytest.raises(ValidationError):
        DocumentRecord(identifier="ADJ-1", body_slug="wrc")  # type: ignore[call-arg]


def test_unknown_field_is_rejected():
    """extra='forbid' turns a typo into an immediate error instead of a field
    that silently never reaches Mongo."""
    with pytest.raises(ValidationError, match="file_has"):
        _record(file_has="abc")


def test_impossible_date_is_rejected():
    """The reason the model holds `date` objects rather than ISO strings."""
    with pytest.raises(ValidationError):
        _record(published_date="2024-13-45")


def test_iso_date_strings_are_coerced():
    """Convenient at the boundary: Scrapy hands over parsed strings."""
    assert _record(published_date="2024-01-17").published_date == date(2024, 1, 17)


def test_negative_file_size_is_rejected():
    with pytest.raises(ValidationError):
        _record(file_size=-1)


def test_whitespace_is_stripped():
    """Scraped attribute values routinely carry surrounding whitespace."""
    assert _record(description="  Party A V Party B  ").description == "Party A V Party B"


# --------------------------------------------------------------------------- #
# The date-window check that is deliberately NOT a validator
# --------------------------------------------------------------------------- #
def test_date_in_partition_window_true_for_normal_record():
    assert _record().date_in_partition_window is True


def test_out_of_window_date_is_allowed_but_flagged():
    """Recon found the displayed date disagreeing with the partition filter
    (documents under /2010/december/ with references ending _2009). A hard
    validator would reject legitimate records, so this is observable, not fatal.
    """
    record = _record(published_date=date(2009, 12, 20), partition_date=date(2010, 12, 1))
    assert record.date_in_partition_window is False  # flagged
    assert record.identifier  # but constructed successfully


# --------------------------------------------------------------------------- #
# Stage transitions
# --------------------------------------------------------------------------- #
def _stored(outcome=UploadOutcome.UPLOADED) -> StoredObject:
    return StoredObject(
        bucket="wrc-landing",
        key="wrc/2024-01/ADJ-00054658__a3f9c1d4.html",
        file_hash="a3f9c1d4" + "0" * 56,
        content_hash="a3f9c1d4" + "0" * 56,
        file_size=4096,
        content_type="text/html; charset=utf-8",
        outcome=outcome,
    )


def test_attach_stored_object_fills_every_storage_field():
    """The seam between objectstore and mongo: one mapping, defined once."""
    record = _record().attach_stored_object(_stored())

    assert record.file_bucket == "wrc-landing"
    assert record.file_path == "wrc/2024-01/ADJ-00054658__a3f9c1d4.html"
    assert record.file_hash == "a3f9c1d4" + "0" * 56
    assert record.file_size == 4096
    assert record.content_type == "text/html; charset=utf-8"
    assert record.is_stored is True


def test_content_hash_is_carried_onto_the_record():
    """Change detection depends on it, so it must survive the transition."""
    record = _record().attach_stored_object(_stored())
    assert record.content_hash == "a3f9c1d4" + "0" * 56


def test_uploaded_becomes_scraped_and_skipped_becomes_skipped():
    """What makes a second run over the same range visibly idempotent."""
    assert _record().attach_stored_object(_stored()).status is RecordStatus.SCRAPED
    assert (
        _record().attach_stored_object(_stored(UploadOutcome.SKIPPED_UNCHANGED)).status
        is RecordStatus.SKIPPED
    )


def test_attach_returns_a_copy_and_leaves_the_original_alone():
    """A caller cannot half-update a record and then write it."""
    original = _record()
    updated = original.attach_stored_object(_stored())

    assert original.file_hash is None
    assert updated.file_hash is not None
    assert original is not updated


def test_explicit_status_overrides_the_inferred_one():
    record = _record().attach_stored_object(_stored(), status=RecordStatus.FAILED)
    assert record.status is RecordStatus.FAILED


def test_mark_failed_keeps_the_record():
    """Requirement 10: every unscraped record must be accounted for. A row
    saying 'seen, failed' beats a missing row."""
    record = _record().mark_failed()
    assert record.status is RecordStatus.FAILED
    assert record.identifier == "ADJ-00054658"


# --------------------------------------------------------------------------- #
# Serialisation
# --------------------------------------------------------------------------- #
def test_to_mongo_renders_dates_as_iso_strings():
    """mongo.iter_landing_records queries with .isoformat(); the two layers must
    agree or a date-range query silently returns nothing."""
    data = _record().to_mongo()
    assert data["published_date"] == "2024-01-17"
    assert data["partition_date"] == "2024-01-01"


def test_to_mongo_renders_status_as_a_plain_string():
    """Mongo should hold 'pending', not a Python enum repr."""
    assert _record().to_mongo()["status"] == "pending"


def test_to_mongo_covers_the_fields_the_mongo_layer_requires():
    """upsert_landing_record indexes on (body_slug, identifier) and compares
    file_hash. Those three must always be present in the serialised form."""
    data = _record().attach_stored_object(_stored()).to_mongo()
    for field in ("body_slug", "identifier", "file_hash", "file_path", "partition_key"):
        assert field in data


def test_round_trip_through_mongo_form():
    original = _record().attach_stored_object(_stored())
    assert DocumentRecord.from_mongo(original.to_mongo()) == original


def test_from_mongo_ignores_fields_the_database_adds():
    """mongo.py writes _id, first_seen_at, versions and friends. extra='forbid'
    would reject them, so from_mongo must filter rather than explode."""
    document = _record().to_mongo()
    document.update(
        {
            "_id": "65f1c0ffee",
            "first_seen_at": datetime(2024, 2, 1, tzinfo=timezone.utc),
            "last_seen_at": datetime(2024, 3, 1, tzinfo=timezone.utc),
            "versions": [{"file_hash": "old"}],
        }
    )
    assert DocumentRecord.from_mongo(document).identifier == "ADJ-00054658"


# --------------------------------------------------------------------------- #
# Spider convenience helper
# --------------------------------------------------------------------------- #
def test_record_from_listing_looks_up_the_body_name():
    """The spider holds a slug; the record needs the human-readable name."""
    record = record_from_listing(
        identifier="ADJ-00054658",
        body_slug="wrc",
        published_date=date(2024, 1, 17),
        source_url="https://www.workplacerelations.ie/en/search/?decisions=1",
        document_url="https://www.workplacerelations.ie/en/cases/2024/january/x.html",
        partition_date=date(2024, 1, 1),
        partition_key="2024-01",
        file_ext=".html",
    )
    assert record.body == "Workplace Relations Commission"
    assert record.status is RecordStatus.PENDING


def test_record_from_listing_rejects_an_unknown_slug():
    """A slug typo must fail loudly rather than produce records attributed to
    an empty body name."""
    with pytest.raises(KeyError):
        record_from_listing(
            identifier="ADJ-1",
            body_slug="not-a-body",
            published_date=date(2024, 1, 17),
            source_url="https://example.ie/search",
            document_url="https://example.ie/doc.html",
            partition_date=date(2024, 1, 1),
            partition_key="2024-01",
        )
