"""Tests for the MongoDB layer.

Run against a real MongoDB when one is available (``docker compose up -d``) and
skipped otherwise, so the suite still passes on a machine without Docker. The
behaviours proved here -- the unique index, idempotent re-writes, and content
change detection -- are the ones requirement 9 turns on, and they are hard to
verify by eye during a real run.
"""

from __future__ import annotations

from datetime import date

import pytest
from pymongo.errors import DuplicateKeyError

from wrc_pipeline.settings import load_settings
from wrc_pipeline.storage import mongo as mongo_store
from wrc_pipeline.storage.mongo import WriteOutcome


@pytest.fixture(scope="module")
def settings():
    """Point every test at a throwaway database, never the real one."""
    base = load_settings()
    return base.model_copy(
        update={"mongo": base.mongo.model_copy(update={"database": "wrc_test"})}
    )


@pytest.fixture(scope="module")
def live_db(settings):
    """Skip the whole module if MongoDB is not reachable.

    The skip reason carries the underlying exception. A test that skips quietly
    is nearly as dangerous as one that passes quietly -- you would believe 28
    tests ran when 13 never executed. Run ``pytest -rs`` to see skip reasons, or
    set ``WRC_REQUIRE_MONGO=1`` to turn an unreachable database into a failure
    (which is what CI should do).
    """
    import os

    try:
        mongo_store._build_client.cache_clear()
        client = mongo_store.get_client(settings)
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        message = (
            f"MongoDB not reachable at {settings.mongo.uri.split('@')[-1]}: "
            f"{type(exc).__name__}: {exc}. Run `docker compose up -d`."
        )
        if os.environ.get("WRC_REQUIRE_MONGO"):
            pytest.fail(message)
        pytest.skip(message)
    yield client
    client.drop_database(settings.mongo.database)


@pytest.fixture(autouse=True)
def clean_collections(live_db, settings):
    """Start every test from an empty database."""
    db = live_db[settings.mongo.database]
    for name in db.list_collection_names():
        db[name].delete_many({})
    mongo_store.ensure_indexes(settings)
    yield


def _record(identifier="ADJ-00054658", file_hash="hash-aaa", body="wrc"):
    return {
        "identifier": identifier,
        "identifier_safe": identifier.replace("/", "-"),
        "body_slug": body,
        "body": "Workplace Relations Commission",
        "description": "Test v Test Ltd",
        "published_date": "2024-01-17",
        "partition_key": "2024-01",
        "partition_date": "2024-01-01",
        "document_url": f"https://example.ie/{identifier}.html",
        "file_path": f"{body}/2024-01/{identifier}__{file_hash[:6]}.html",
        "file_hash": file_hash,
        "run_id": "test-run-1",
    }


# --------------------------------------------------------------------------- #
# Idempotency -- requirement 9
# --------------------------------------------------------------------------- #
def test_first_write_reports_inserted(settings):
    assert mongo_store.upsert_landing_record(_record(), settings) is WriteOutcome.INSERTED
    assert mongo_store.landing_collection(settings).count_documents({}) == 1


def test_identical_rewrite_is_a_no_op(settings):
    """The whole point of requirement 9: run twice, get one record."""
    mongo_store.upsert_landing_record(_record(), settings)
    outcome = mongo_store.upsert_landing_record(_record(), settings)

    assert outcome is WriteOutcome.UNCHANGED
    assert mongo_store.landing_collection(settings).count_documents({}) == 1


def test_ten_rewrites_still_yield_one_record(settings):
    for _ in range(10):
        mongo_store.upsert_landing_record(_record(), settings)
    assert mongo_store.landing_collection(settings).count_documents({}) == 1


def test_unique_index_rejects_a_raw_duplicate_insert(settings):
    """Proves the guarantee is enforced by the DATABASE, not by our code.

    Even a direct insert that bypasses upsert_landing_record entirely cannot
    create a duplicate. This is the difference between a convention and an
    invariant, and it is what makes the guarantee hold under concurrency.
    """
    collection = mongo_store.landing_collection(settings)
    collection.insert_one(_record())

    with pytest.raises(DuplicateKeyError):
        collection.insert_one(_record())


def test_same_identifier_under_different_bodies_is_allowed(settings):
    """Uniqueness is scoped per body, since reference formats can overlap."""
    mongo_store.upsert_landing_record(_record(identifier="47955", body="wrc"), settings)
    outcome = mongo_store.upsert_landing_record(
        _record(identifier="47955", body="labour-court"), settings
    )

    assert outcome is WriteOutcome.INSERTED
    assert mongo_store.landing_collection(settings).count_documents({}) == 2


# --------------------------------------------------------------------------- #
# Change detection -- "use the file hash to detect changes between runs"
# --------------------------------------------------------------------------- #
def test_changed_hash_reports_updated(settings):
    mongo_store.upsert_landing_record(_record(file_hash="hash-aaa"), settings)
    outcome = mongo_store.upsert_landing_record(_record(file_hash="hash-bbb"), settings)

    assert outcome is WriteOutcome.UPDATED
    assert mongo_store.landing_collection(settings).count_documents({}) == 1


def test_previous_version_is_preserved_not_overwritten(settings):
    """Landing-zone immutability: the superseded state survives the update."""
    mongo_store.upsert_landing_record(_record(file_hash="hash-aaa"), settings)
    mongo_store.upsert_landing_record(_record(file_hash="hash-bbb"), settings)

    stored = mongo_store.landing_collection(settings).find_one({"identifier": "ADJ-00054658"})
    assert stored["file_hash"] == "hash-bbb"
    assert len(stored["versions"]) == 1
    assert stored["versions"][0]["file_hash"] == "hash-aaa"
    assert "content_changed_at" in stored


def test_first_seen_at_is_never_modified(settings):
    """$setOnInsert fields describe the document as first seen."""
    mongo_store.upsert_landing_record(_record(file_hash="hash-aaa"), settings)
    collection = mongo_store.landing_collection(settings)
    original = collection.find_one({})["first_seen_at"]

    mongo_store.upsert_landing_record(_record(file_hash="hash-bbb"), settings)

    assert collection.find_one({})["first_seen_at"] == original


# --------------------------------------------------------------------------- #
# Lookups
# --------------------------------------------------------------------------- #
def test_find_existing_returns_none_when_absent(settings):
    assert mongo_store.find_existing("wrc", "does-not-exist", settings) is None


def test_find_existing_returns_hash_for_change_detection(settings):
    mongo_store.upsert_landing_record(_record(), settings)
    found = mongo_store.find_existing("wrc", "ADJ-00054658", settings)

    assert found is not None
    assert found["file_hash"] == "hash-aaa"


def test_iter_landing_records_filters_by_inclusive_date_range(settings):
    for day, ident in [("2024-01-05", "A1"), ("2024-01-31", "A2"), ("2024-02-10", "A3")]:
        rec = _record(identifier=ident)
        rec["published_date"] = day
        mongo_store.upsert_landing_record(rec, settings)

    january = list(
        mongo_store.iter_landing_records(date(2024, 1, 1), date(2024, 1, 31), settings=settings)
    )

    # A2 sits exactly on the boundary and must be included: partition windows
    # are inclusive at both ends, matching the site's own date filters.
    assert {r["identifier"] for r in january} == {"A1", "A2"}


# --------------------------------------------------------------------------- #
# Failure ledger and run bookkeeping
# --------------------------------------------------------------------------- #
def test_record_failure_persists_the_reason(settings):
    mongo_store.record_failure(
        run_id="test-run-1",
        body_slug="wrc",
        partition_key="2024-01",
        url="https://example.ie/missing.html",
        reason="http_error",
        error_code=404,
        settings=settings,
    )

    stored = mongo_store.failures_collection(settings).find_one({})
    assert stored["reason"] == "http_error"
    assert stored["error_code"] == 404


def test_run_lifecycle_records_totals(settings):
    mongo_store.start_run("test-run-1", date(2024, 1, 1), date(2024, 1, 31),
                          "monthly", ["wrc"], settings)
    mongo_store.finish_run("test-run-1", {"records_scraped": 234}, settings=settings)

    stored = mongo_store.runs_collection(settings).find_one({"run_id": "test-run-1"})
    assert stored["status"] == "completed"
    assert stored["totals"]["records_scraped"] == 234
    assert "finished_at" in stored
