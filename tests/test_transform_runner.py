"""Tests for the transformation runner.

Storage is faked, so these assert on control flow: what gets cleaned, what passes
through untouched, what is skipped, what fails, and what the curated record ends
up containing. The cleaning itself is covered in test_cleaner.py.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from wrc_pipeline.settings import load_settings
from wrc_pipeline.storage.mongo import WriteOutcome
from wrc_pipeline.storage.objectstore import ObjectStoreError, StoredObject, UploadOutcome
from wrc_pipeline.transform import runner
from wrc_pipeline.transform.cleaner import CLEANER_VERSION
from wrc_pipeline.transform.runner import TransformCounters, transform_range, transform_record

DECISION_PAGE = b"""<html><body>
  <header><nav>Home | Search</nav></header>
  <div class="col-sm-9">
    <h1 class="page-title">ADJ-00047352</h1>
    <div class="content">
      <p>ADJUDICATION OFFICER Recommendation under Industrial Relations Act 1969.</p>
      <p>Parties: A Worker v An Employer. """ + b"Findings. " * 30 + b"""</p>
      <div class="table-responsive"><table><tr><td>Employee</td><td></td></tr></table></div>
    </div>
  </div>
  <footer>Privacy Policy</footer>
</body></html>"""

PDF_BYTES = b"%PDF-1.4 synthetic determination" + b"\x00" * 500


@pytest.fixture(scope="module")
def cfg():
    return load_settings()


def _landing(**overrides: Any) -> dict[str, Any]:
    record = {
        "_id": "65f1c0ffee",
        "identifier": "ADJ-00047352",
        "identifier_safe": "ADJ-00047352",
        "body": "Workplace Relations Commission",
        "body_slug": "wrc",
        "title": "ADJ-00047352",
        "description": "Car Valet V Motor Garage",
        "published_date": "2024-01-31",
        "partition_date": "2024-01-29",
        "partition_key": "2024-W05",
        "source_url": "https://www.workplacerelations.ie/en/search/?decisions=1",
        "document_url": "https://www.workplacerelations.ie/en/cases/2024/january/x.html",
        "file_bucket": "decisions-landing",
        "file_path": "wrc/2024-W05/ADJ-00047352__abc12345.html",
        "file_hash": "abc12345" + "0" * 56,
        "file_ext": ".html",
        "content_type": "text/html; charset=utf-8",
    }
    record.update(overrides)
    return record


class MongoSpy:
    def __init__(self, curated: dict[str, Any] | None = None, landing: list[dict] | None = None):
        self.curated = curated
        self.landing = landing or []
        self.upserts: list[dict[str, Any]] = []

    def ensure_indexes(self, settings=None):
        pass

    def curated_collection(self, settings=None):
        spy = self

        class _Collection:
            def find_one(self, *a, **k):
                return spy.curated

        return _Collection()

    def iter_landing_records(self, start, end, body=None, settings=None):
        yield from self.landing

    def upsert_curated_record(self, record, settings=None):
        self.upserts.append(record)
        return WriteOutcome.INSERTED


class StoreSpy:
    def __init__(self, payload: bytes = DECISION_PAGE, read_error=None, write_error=None):
        self.payload = payload
        self.read_error = read_error
        self.write_error = write_error
        self.uploads: list[dict[str, Any]] = []

    def ensure_buckets(self, settings=None):
        pass

    def get_bytes(self, bucket, key, settings=None):
        if self.read_error:
            raise self.read_error
        return self.payload

    def put_curated_document(self, **kwargs):
        if self.write_error:
            raise self.write_error
        self.uploads.append(kwargs)
        import hashlib

        digest = hashlib.sha256(kwargs["data"]).hexdigest()
        return StoredObject(
            bucket="decisions-curated",
            key=f"{kwargs['body_slug']}/{kwargs['identifier_safe']}{kwargs['ext']}",
            file_hash=digest,
            content_hash=digest,
            file_size=len(kwargs["data"]),
            content_type="text/html; charset=utf-8",
            outcome=UploadOutcome.UPLOADED,
        )

    # The runner reaches through the module for these.
    StoredObject = StoredObject


def _patch(monkeypatch, mongo_spy=None, store_spy=None):
    mongo_spy = mongo_spy or MongoSpy()
    store_spy = store_spy or StoreSpy()
    monkeypatch.setattr(runner, "mongo", mongo_spy)
    monkeypatch.setattr(runner, "store", store_spy)
    return mongo_spy, store_spy


# --------------------------------------------------------------------------- #
# The HTML path
# --------------------------------------------------------------------------- #
def test_html_is_cleaned_and_stored_as_identifier_ext(cfg, monkeypatch):
    """Requirement: files are renamed to identifier.ext in the curated zone."""
    mongo_spy, store_spy = _patch(monkeypatch)
    counters = TransformCounters()

    assert transform_record(_landing(), counters, cfg) is True
    assert counters.cleaned == 1
    assert store_spy.uploads[0]["ext"] == ".html"
    assert mongo_spy.upserts[0]["file_path"] == "wrc/ADJ-00047352.html"


def test_the_stored_payload_is_the_cleaned_html(cfg, monkeypatch):
    _, store_spy = _patch(monkeypatch)
    transform_record(_landing(), TransformCounters(), cfg)

    written = store_spy.uploads[0]["data"].decode()
    assert "ADJUDICATION OFFICER" in written
    assert "Privacy Policy" not in written  # footer gone
    assert "Home | Search" not in written  # nav gone
    assert len(store_spy.uploads[0]["data"]) < len(DECISION_PAGE)


def test_the_hash_is_recomputed_from_the_cleaned_bytes(cfg, monkeypatch):
    """Requirement: calculate the new file_hash of the cleaned file. It must
    differ from the landing hash, or nothing was actually transformed."""
    import hashlib

    mongo_spy, store_spy = _patch(monkeypatch)
    landing = _landing()
    transform_record(landing, TransformCounters(), cfg)

    curated = mongo_spy.upserts[0]
    assert curated["file_hash"] == hashlib.sha256(store_spy.uploads[0]["data"]).hexdigest()
    assert curated["file_hash"] != landing["file_hash"]


def test_cleaning_statistics_are_recorded(cfg, monkeypatch):
    """Per document, so "which ones did the cleaner struggle with?" is a query."""
    mongo_spy, _ = _patch(monkeypatch)
    transform_record(_landing(), TransformCounters(), cfg)

    cleaning = mongo_spy.upserts[0]["cleaning"]
    assert cleaning["selector_used"] == "div.content"  # the primary selector
    assert cleaning["text_chars"] > 200
    assert cleaning["tables_kept"] == 1


# --------------------------------------------------------------------------- #
# The binary path (requirement 6a)
# --------------------------------------------------------------------------- #
def test_pdfs_pass_through_untouched(cfg, monkeypatch):
    """No transformation applied, so the bytes and the hash are unchanged --
    only the key changes."""
    import hashlib

    mongo_spy, store_spy = _patch(monkeypatch, store_spy=StoreSpy(payload=PDF_BYTES))
    counters = TransformCounters()

    transform_record(_landing(file_ext=".pdf"), counters, cfg)

    assert counters.passed_through == 1
    assert counters.cleaned == 0
    assert store_spy.uploads[0]["data"] == PDF_BYTES
    assert mongo_spy.upserts[0]["file_hash"] == hashlib.sha256(PDF_BYTES).hexdigest()
    assert mongo_spy.upserts[0]["file_path"] == "wrc/ADJ-00047352.pdf"
    assert "cleaning" not in mongo_spy.upserts[0]


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def test_the_curated_record_traces_back_to_its_source(cfg, monkeypatch):
    """Landing is immutable, so any curated output must be re-derivable from the
    exact bytes it came from."""
    mongo_spy, _ = _patch(monkeypatch)
    landing = _landing()
    transform_record(landing, TransformCounters(), cfg)

    curated = mongo_spy.upserts[0]
    assert curated["source_file_path"] == landing["file_path"]
    assert curated["source_file_hash"] == landing["file_hash"]
    assert curated["source_file_bucket"] == landing["file_bucket"]
    assert curated["cleaner_version"] == CLEANER_VERSION


def test_landing_metadata_is_carried_forward(cfg, monkeypatch):
    mongo_spy, _ = _patch(monkeypatch)
    transform_record(_landing(), TransformCounters(), cfg)

    curated = mongo_spy.upserts[0]
    for field in ("identifier", "body_slug", "published_date", "partition_key", "description"):
        assert curated[field] == _landing()[field]


def test_the_mongo_id_is_not_copied(cfg, monkeypatch):
    """Copying _id would collide with the landing document's own key."""
    mongo_spy, _ = _patch(monkeypatch)
    transform_record(_landing(), TransformCounters(), cfg)
    assert "_id" not in mongo_spy.upserts[0]


# --------------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------------- #
def test_unchanged_source_and_same_cleaner_is_skipped(cfg, monkeypatch):
    landing = _landing()
    curated = {"source_file_hash": landing["file_hash"], "cleaner_version": CLEANER_VERSION}
    mongo_spy, store_spy = _patch(monkeypatch, MongoSpy(curated=curated))
    counters = TransformCounters()

    assert transform_record(landing, counters, cfg) is False
    assert counters.skipped == 1
    assert store_spy.uploads == []
    assert mongo_spy.upserts == []


def test_a_changed_source_is_re_transformed(cfg, monkeypatch):
    curated = {"source_file_hash": "an-older-hash", "cleaner_version": CLEANER_VERSION}
    _, store_spy = _patch(monkeypatch, MongoSpy(curated=curated))
    counters = TransformCounters()

    transform_record(_landing(), counters, cfg)
    assert counters.cleaned == 1
    assert len(store_spy.uploads) == 1


def test_a_bumped_cleaner_version_forces_a_re_transform(cfg, monkeypatch):
    """The reason the skip checks two conditions. Skipping on source hash alone
    would make improved cleaning logic silently ineffective."""
    landing = _landing()
    curated = {"source_file_hash": landing["file_hash"], "cleaner_version": "0"}
    _, store_spy = _patch(monkeypatch, MongoSpy(curated=curated))
    counters = TransformCounters()

    transform_record(landing, counters, cfg)
    assert counters.cleaned == 1
    assert len(store_spy.uploads) == 1


# --------------------------------------------------------------------------- #
# Failures
# --------------------------------------------------------------------------- #
def test_a_landing_record_with_no_file_fails(cfg, monkeypatch):
    """The crawl recorded it and the download failed; there is nothing to read."""
    _, store_spy = _patch(monkeypatch)
    counters = TransformCounters()

    assert transform_record(_landing(file_path=None), counters, cfg) is False
    assert counters.failures[0]["reason"] == "no_stored_file"
    assert store_spy.uploads == []


def test_an_unreadable_source_fails(cfg, monkeypatch):
    _patch(monkeypatch, store_spy=StoreSpy(read_error=ObjectStoreError("gone")))
    counters = TransformCounters()

    assert transform_record(_landing(), counters, cfg) is False
    assert "source_unreadable" in counters.failures[0]["reason"]


def test_a_curated_upload_failure_writes_no_metadata(cfg, monkeypatch):
    """No record for a document we do not hold."""
    mongo_spy, _ = _patch(monkeypatch, store_spy=StoreSpy(write_error=ObjectStoreError("down")))
    counters = TransformCounters()

    assert transform_record(_landing(), counters, cfg) is False
    assert "curated_upload_failed" in counters.failures[0]["reason"]
    assert mongo_spy.upserts == []


def test_a_page_with_no_content_selector_fails(cfg, monkeypatch):
    """Rather than falling back to the whole page, which would write navigation
    that looks superficially fine."""
    _patch(monkeypatch, store_spy=StoreSpy(payload=b"<html><body><p>nope</p></body></html>"))
    counters = TransformCounters()

    assert transform_record(_landing(), counters, cfg) is False
    assert counters.failures[0]["reason"] == "no_content_selector_matched"


# --------------------------------------------------------------------------- #
# Thin content: flagged, not dropped
# --------------------------------------------------------------------------- #
def test_thin_content_is_stored_with_a_flag(cfg, monkeypatch):
    """Losing a document because our selector underperformed would be worse than
    storing it with a warning that can be queried."""
    thin = b'<html><body><div class="content"><p>Short.</p></div></body></html>'
    mongo_spy, store_spy = _patch(monkeypatch, store_spy=StoreSpy(payload=thin))
    counters = TransformCounters()

    assert transform_record(_landing(), counters, cfg) is True
    assert counters.flagged == 1
    assert counters.cleaned == 1
    assert counters.failed == 0
    assert mongo_spy.upserts[0]["quality_flag"] == "thin_content"
    assert len(store_spy.uploads) == 1


def test_a_full_decision_carries_no_flag(cfg, monkeypatch):
    mongo_spy, _ = _patch(monkeypatch)
    transform_record(_landing(), TransformCounters(), cfg)
    assert "quality_flag" not in mongo_spy.upserts[0]


# --------------------------------------------------------------------------- #
# Range and reconciliation
# --------------------------------------------------------------------------- #
def test_every_record_in_range_reaches_an_outcome(cfg, monkeypatch):
    """The stage's reconciliation: unlike the crawl's, this compares our own
    collection against our own processing, so a mismatch is a bug here."""
    landing = [
        _landing(identifier="ADJ-1", identifier_safe="ADJ-1"),
        _landing(identifier="ADJ-2", identifier_safe="ADJ-2"),
        _landing(identifier="ADJ-3", identifier_safe="ADJ-3", file_path=None),
    ]
    _patch(monkeypatch, MongoSpy(landing=landing))

    counters = transform_range(date(2024, 1, 1), date(2024, 1, 31), settings=cfg)
    summary = counters.as_dict()

    assert summary["records_found"] == 3
    assert summary["records_cleaned"] == 2
    assert summary["records_failed"] == 1
    assert summary["reconciled"] is True


def test_an_empty_range_reconciles(cfg, monkeypatch):
    _patch(monkeypatch, MongoSpy(landing=[]))
    counters = transform_range(date(2024, 1, 1), date(2024, 1, 31), settings=cfg)
    assert counters.as_dict()["reconciled"] is True
    assert counters.as_dict()["records_found"] == 0
