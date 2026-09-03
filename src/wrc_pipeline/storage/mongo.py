"""MongoDB access layer.
"""

from __future__ import annotations

import functools
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Iterator

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, DuplicateKeyError, ServerSelectionTimeoutError

from ..logging_config import get_logger
from ..settings import Settings, get_settings

log = get_logger(__name__)


class WriteOutcome(str, Enum):
    """What a save actually did.
    """

    INSERTED = "inserted"    # first time we have seen this record
    UPDATED = "updated"      # seen before, but the file hash differs
    UNCHANGED = "unchanged"  # seen before, identical hash -> nothing to do


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #
@functools.lru_cache(maxsize=4)
def _build_client(uri: str, timeout_ms: int) -> MongoClient:
    """Create and verify a client, cached per connection string.
    """
    client: MongoClient = MongoClient(
        uri,
        serverSelectionTimeoutMS=timeout_ms,
        tz_aware=True,
    )
    try:
        client.admin.command("ping")
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        raise ConnectionFailure(
            f"Cannot reach MongoDB. Is `docker compose up -d` running?\n"
            f"  host: {uri.split('@')[-1]}\n"  # never log credentials
            f"  {exc}"
        ) from exc
    return client


def get_client(settings: Settings | None = None) -> MongoClient:
    """Return the process-wide MongoClient for the configured URI."""
    cfg = (settings or get_settings()).mongo
    return _build_client(cfg.uri, cfg.server_selection_timeout_ms)


def get_database(settings: Settings | None = None) -> Database:
    cfg = (settings or get_settings()).mongo
    return get_client(settings)[cfg.database]


def landing_collection(settings: Settings | None = None) -> Collection:
    cfg = (settings or get_settings()).mongo
    return get_database(settings)[cfg.landing_collection]


def curated_collection(settings: Settings | None = None) -> Collection:
    cfg = (settings or get_settings()).mongo
    return get_database(settings)[cfg.curated_collection]


def runs_collection(settings: Settings | None = None) -> Collection:
    cfg = (settings or get_settings()).mongo
    return get_database(settings)[cfg.runs_collection]


def failures_collection(settings: Settings | None = None) -> Collection:
    cfg = (settings or get_settings()).mongo
    return get_database(settings)[cfg.failures_collection]


# --------------------------------------------------------------------------- #
# Indexes
# --------------------------------------------------------------------------- #
def ensure_indexes(settings: Settings | None = None) -> None:
    """Create every index the pipeline relies on. Safe to call repeatedly.
    """
    settings = settings or get_settings()
    landing = landing_collection(settings)
    curated = curated_collection(settings)

    # THE idempotency guarantee. Compound rather than on `identifier` alone
    # because reference formats differ per body (a bare EAT determination number
    # such as "47955" could plausibly collide with another body's scheme) and we
    # cannot prove global uniqueness. Scoping per body is correct either way.
    landing.create_index(
        [("body_slug", ASCENDING), ("identifier", ASCENDING)],
        unique=True,
        name="uniq_body_identifier",
    )

    landing.create_index(
        [("body_slug", ASCENDING), ("partition_key", ASCENDING)],
        name="body_partition",
    )
    # The transformation script fetches by date range (its stated input).
    landing.create_index([("published_date", ASCENDING)], name="published_date")
    # Change detection lookups.
    landing.create_index([("file_hash", ASCENDING)], name="file_hash")

    # Curated mirrors landing: same uniqueness rule, same access patterns.
    curated.create_index(
        [("body_slug", ASCENDING), ("identifier", ASCENDING)],
        unique=True,
        name="uniq_body_identifier",
    )
    curated.create_index([("published_date", ASCENDING)], name="published_date")

    failures_collection(settings).create_index(
        [("run_id", ASCENDING), ("body_slug", ASCENDING)], name="run_body"
    )
    runs_collection(settings).create_index([("run_id", ASCENDING)], unique=True, name="uniq_run")

    log.info("mongo.indexes_ready")


# --------------------------------------------------------------------------- #
# Landing zone reads and writes
# --------------------------------------------------------------------------- #
def find_existing(
    body_slug: str,
    identifier: str,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    """Fetch a landing record if we already hold it.
    """
    return landing_collection(settings).find_one(
        {"body_slug": body_slug, "identifier": identifier},
        projection={
            "file_hash": 1,
            "content_hash": 1,
            "file_path": 1,
            "etag": 1,
            "last_modified": 1,
            "versions": 1,
        },
    )


def count_partition_records(
    body_slug: str,
    partition_key: str,
    settings: Settings | None = None,
) -> int:
    """How many distinct decisions we hold for one (body, partition).
    """
    return landing_collection(settings).count_documents(
        {"body_slug": body_slug, "partition_key": partition_key}
    )


def upsert_landing_record(
    record: dict[str, Any],
    settings: Settings | None = None,
) -> WriteOutcome:
    """Write one landing record, returning what actually happened.

    Requires ``body_slug``, ``identifier`` and ``file_hash`` in *record*.

    Three cases:

    * Not seen before          -> INSERTED
    * Seen, same file_hash     -> UNCHANGED (idempotent no-op)
    * Seen, different hash     -> UPDATED, previous version appended to
      ``versions`` so the earlier state is preserved, never overwritten.
    """
    collection = landing_collection(settings)
    key = {"body_slug": record["body_slug"], "identifier": record["identifier"]}
    now = _utcnow()

    existing = collection.find_one(
        key,
        projection={"file_hash": 1, "file_path": 1, "content_hash": 1, "versions": 1},
    )

    # --- unchanged -------------------------------------------------------- #
    if existing and existing.get("file_hash") == record.get("file_hash"):
        collection.update_one(
            key,
            {"$set": {"last_seen_at": now, "last_run_id": record.get("run_id")}},
        )
        return WriteOutcome.UNCHANGED

    # --- changed ---------------------------------------------------------- #
    if existing:
        previous_version = {
            "file_hash": existing.get("file_hash"),
            "file_path": existing.get("file_path"),
            "content_hash": existing.get("content_hash"),
            "superseded_at": now,
        }

        history = [
            v
            for v in (existing.get("versions") or [])
            # drop any entry matching the NEW current content (rule 2)
            if v.get("file_hash") != record.get("file_hash")
        ]
        if previous_version["file_hash"] not in {v.get("file_hash") for v in history}:
            history.append(previous_version)  

        # Rewriting the array is a last-write-wins operation, so two workers
        # updating the SAME identifier at the same moment could drop one entry.
        # Accepted: partitions are the unit of concurrency and an identifier
        # belongs to one partition, so this only arises for the republished
        # documents above, where both versions are still safe in object storage.
        collection.update_one(
            key,
            {
                "$set": {
                    **record,
                    "versions": history,
                    "last_seen_at": now,
                    "content_changed_at": now,
                }
            },
        )
        log.warning(
            "record.content_changed",
            extra={
                "body": record["body_slug"],
                "identifier": record["identifier"],
                "old_hash": existing.get("file_hash"),
                "new_hash": record.get("file_hash"),
            },
        )
        return WriteOutcome.UPDATED

    # --- first sighting --------------------------------------------------- #
    try:
        collection.update_one(
            key,
            {
                "$setOnInsert": {**record, "first_seen_at": now},
                "$set": {"last_seen_at": now},
            },
            upsert=True,
        )
        return WriteOutcome.INSERTED
    except DuplicateKeyError:
        log.debug("record.insert_race", extra={"identifier": record["identifier"]})
        return WriteOutcome.UNCHANGED


def iter_landing_records(
    start_date: date,
    end_date: date,
    body_slug: str | None = None,
    settings: Settings | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream landing records for a date range (the transformation stage's input).
    """
    query: dict[str, Any] = {
        "published_date": {
            "$gte": start_date.isoformat(),
            "$lte": end_date.isoformat(),
        }
    }
    if body_slug:
        query["body_slug"] = body_slug

    # no_cursor_timeout is deliberately NOT set: transformation is fast enough
    # per record that the default 10-minute cursor lifetime is ample, and
    # leaked cursors are worse than a restart.
    yield from landing_collection(settings).find(query).sort("published_date", ASCENDING)


def upsert_curated_record(
    record: dict[str, Any],
    settings: Settings | None = None,
) -> WriteOutcome:
    """Write one curated record.

    Simpler than the landing equivalent: the curated zone is *derived*, so it is
    safe to overwrite. Re-running the transformation with improved cleaning logic
    should replace the old output, not version it.
    """
    collection = curated_collection(settings)
    key = {"body_slug": record["body_slug"], "identifier": record["identifier"]}
    now = _utcnow()

    result = collection.update_one(
        key,
        {"$set": {**record, "transformed_at": now}, "$setOnInsert": {"first_seen_at": now}},
        upsert=True,
    )
    return WriteOutcome.INSERTED if result.upserted_id else WriteOutcome.UPDATED


# --------------------------------------------------------------------------- #
# Failure ledger
# --------------------------------------------------------------------------- #
def record_failure(
    run_id: str,
    body_slug: str,
    partition_key: str,
    url: str,
    reason: str,
    error_code: int | str | None = None,
    identifier: str | None = None,
    settings: Settings | None = None,
) -> None:
    """Persist one unscraped record with its reason.
    """
    try:
        failures_collection(settings).insert_one(
            {
                "run_id": run_id,
                "body_slug": body_slug,
                "partition_key": partition_key,
                "url": url,
                "reason": reason,
                "error_code": error_code,
                "identifier": identifier,
                "recorded_at": _utcnow(),
            }
        )
    except Exception: 
        log.exception("mongo.failure_write_failed", extra={"url": url})


# --------------------------------------------------------------------------- #
# Run bookkeeping
# --------------------------------------------------------------------------- #
def start_run(
    run_id: str,
    start_date: date,
    end_date: date,
    partition_size: str,
    bodies: list[str],
    settings: Settings | None = None,
) -> None:
    """Open a run document. Upsert so a retried run reuses its id cleanly."""
    runs_collection(settings).update_one(
        {"run_id": run_id},
        {
            "$set": {
                "status": "running",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "partition_size": partition_size,
                "bodies": bodies,
                "started_at": _utcnow(),
            }
        },
        upsert=True,
    )
    log.info(
        "run.started",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "partition_size": partition_size,
            "bodies": bodies,
        },
    )


def finish_run(
    run_id: str,
    totals: dict[str, Any],
    status: str = "completed",
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    """Close a run and emit the end-of-run summary the brief requires."""
    now = _utcnow()
    document = runs_collection(settings).find_one_and_update(
        {"run_id": run_id},
        {"$set": {"status": status, "finished_at": now, "totals": totals}},
        return_document=ReturnDocument.AFTER,
    )

    duration = None
    if document and document.get("started_at"):
        duration = round((now - document["started_at"]).total_seconds(), 1)

    log.info("run.summary", extra={"status": status, "duration_seconds": duration, **totals})
    return document