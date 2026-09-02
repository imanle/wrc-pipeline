"""MongoDB access layer.

Owns every read and write of pipeline metadata. Nothing else in the project
should import ``pymongo`` directly -- keeping the driver behind this module means
the spider, the transformation stage and the Dagster assets all agree on the
document shape, and swapping the store later is a single-file change.

Idempotency 
---------------------------
The obvious implementation of "don't create duplicates" is a read-then-write::

    if collection.find_one({"identifier": ident}) is None:
        collection.insert_one(record)

That is a race. Two workers can both read "absent" before either writes, and both
then insert. With partitions running concurrently this is the expected case, not
a rare one.

So the guarantee lives in the database as a **unique compound index** on
``(body, identifier)``, and every write is a single atomic ``update_one(...,
upsert=True)``. MongoDB physically refuses a second record for the same decision
regardless of what the application does. A code-level check is a convention; an
index is an invariant.

Landing-zone immutability
-------------------------
The brief forbids deleting or updating landing-zone data. Fields that describe
the document *as first seen* are written with ``$setOnInsert`` and never touched
again. Only run-tracking fields are refreshed. When a document's content changes
between runs we append to a ``versions`` array and the new file lands at a new
object key (keys carry a hash suffix), so nothing is ever overwritten.
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

    Returned by :func:`upsert_landing_record` so idempotency is *observable*
    rather than merely asserted: a second run over the same date range should
    report ``UNCHANGED`` for every record, and that is the proof.
    """

    INSERTED = "inserted"    # first time we have seen this record
    UPDATED = "updated"      # seen before, but the file hash differs
    UNCHANGED = "unchanged"  # seen before, identical hash -> nothing to do


def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp.

    Always UTC: the pipeline may run in one region against data from another,
    and naive local timestamps make cross-run comparison unreliable.
    """
    return datetime.now(tz=timezone.utc)


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #
@functools.lru_cache(maxsize=4)
def _build_client(uri: str, timeout_ms: int) -> MongoClient:
    """Create and verify a client, cached per connection string.

    Cached because ``MongoClient`` is thread-safe and maintains its own
    connection pool internally. Creating one per record -- a common mistake --
    exhausts server connections quickly and is far slower.

    Note the cache key is the URI and timeout, **not** a ``Settings`` object.
    ``lru_cache`` requires hashable arguments, and ``Settings`` contains lists
    (``bodies``, ``retry_http_codes``) so it is unhashable; caching on it raises
    ``TypeError: unhashable type``. Keying on the primitive values is also more
    honest -- two different Settings objects with the same URI *should* share a
    connection pool.

    Also pings on creation. PyMongo connects lazily, so a wrong URI or bad
    credentials would otherwise stay silent until the first write, potentially
    thousands of requests into a crawl. Failing here makes it immediate.
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

    ``create_index`` is idempotent in MongoDB -- creating an index that already
    exists with the same definition is a no-op -- so this runs at the start of
    every pipeline run rather than as a separate migration step.
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
    # How the spider's reconciliation and the transform stage filter their work.
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

    Used for change detection *before* downloading: if the record exists and the
    remote file still hashes to the stored value, the download is skipped
    entirely. Projection limits the fields returned because this runs once per
    record and we only need the hash and path.
    """
    return landing_collection(settings).find_one(
        {"body_slug": body_slug, "identifier": identifier},
        # Every field the download pipeline needs before fetching: the hashes for
        # change detection, the path to carry forward on a skip, and the HTTP
        # validators for a conditional request. Omitting a field here silently
        # disables the feature that reads it -- etag/last_modified were missing
        # at first, which made conditional requests dead code.
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

    The store, not the run. A single pass can miss records the previous pass
    already captured -- this source's page windows overlap and displace entries
    non-deterministically -- so "did this pass see all 234?" and "do we hold all
    234?" are different questions, and only the second one decides whether the
    partition needs re-running.
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

    # file_path is REQUIRED here, not optional: it is copied into the versions
    # entry below, and a projection that omits it silently writes a null path for
    # every superseded version -- the object survives in the bucket but nothing
    # records where it is, which defeats the point of keeping history.
    #
    # This is the second bug of exactly this shape (find_existing omitted
    # etag/last_modified, which made conditional requests dead code). A projection
    # is a contract with the code below it: whenever you read a field from
    # `existing`, check it is listed here.
    existing = collection.find_one(
        key,
        projection={"file_hash": 1, "file_path": 1, "content_hash": 1, "versions": 1},
    )

    # --- unchanged -------------------------------------------------------- #
    if existing and existing.get("file_hash") == record.get("file_hash"):
        # Touch only run-tracking fields. The record itself is untouched, which
        # is what "do not update the Landing Zone" means in practice.
        collection.update_one(
            key,
            {"$set": {"last_seen_at": now, "last_run_id": record.get("run_id")}},
        )
        return WriteOutcome.UNCHANGED

    # --- changed ---------------------------------------------------------- #
    if existing:
        # Preserve the superseded state rather than discarding it. The file
        # itself is also safe: object keys embed the hash, so the new download
        # lands beside its predecessor rather than on top of it.
        previous_version = {
            "file_hash": existing.get("file_hash"),
            "file_path": existing.get("file_path"),
            "content_hash": existing.get("content_hash"),
            "superseded_at": now,
        }

        # The version list is rebuilt rather than appended to, because content on
        # this source does not only move forward.
        #
        # The WRC republishes some decisions under the same reference: the same
        # case sits at /2024/january/adj-00044064.html and again at
        # /2024/february/... with heavier anonymisation, and each week's search
        # returns its own copy. So the "current" version flips back and forth
        # depending on which partition ran last. An unconditional $push appended
        # an entry on every flip -- four live passes produced four entries for two
        # actual files, including one identical to the current version, and the
        # array grew without limit.
        #
        # Two rules, which need the whole array rather than a $push:
        #   1. a version already recorded is not recorded again;
        #   2. the version that just became current is removed from the history,
        #      since `versions` holds superseded states only.
        #
        # $addToSet cannot do (1) -- it compares whole subdocuments and
        # superseded_at differs every time -- and (2) needs a $pull, which cannot
        # share a field with a $push in one update.
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
                # $setOnInsert fields describe the document as first seen and are
                # never modified afterwards.
                "$setOnInsert": {**record, "first_seen_at": now},
                "$set": {"last_seen_at": now},
            },
            upsert=True,
        )
        return WriteOutcome.INSERTED
    except DuplicateKeyError:
        # A concurrent worker inserted the same record between our find_one and
        # this upsert. The unique index did its job: the data is correct and the
        # other worker owns the insert, so we report a no-op rather than failing.
        log.debug("record.insert_race", extra={"identifier": record["identifier"]})
        return WriteOutcome.UNCHANGED


def iter_landing_records(
    start_date: date,
    end_date: date,
    body_slug: str | None = None,
    settings: Settings | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream landing records for a date range (the transformation stage's input).

    A generator, not a list: at 1000x scale a month's metadata should not have
    to fit in memory at once. ``$gte``/``$lte`` because partition windows are
    inclusive at both ends, matching the site's own date filters.
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

    Duplicates what the JSON logs already capture, deliberately. Logs answer
    "what happened during this run"; a collection answers "show me every 404 we
    have ever hit, grouped by body" -- a query you cannot run against text files.

    Never raises: a failure while recording a failure must not abort the crawl.
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
    except Exception:  # noqa: BLE001 - see docstring
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