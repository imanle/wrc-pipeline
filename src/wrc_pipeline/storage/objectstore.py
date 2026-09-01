"""S3/MinIO object storage layer.

Owns every byte that leaves or enters the object store. Nothing else in the
project imports ``boto3`` directly, for the same reason nothing else imports
``pymongo``: the spider, the transformation stage and the Dagster assets must
agree on how keys are built and how hashes are computed, and swapping MinIO for
real S3 should be a change to ``.env``, not to code.

Content-addressed landing keys
------------------------------
Two requirements in the brief pull in opposite directions:

* requirement 9 -- don't re-download unchanged files; use the hash to detect
  changes;
* the tips section -- never delete or update anything in the Landing Zone.

A conventional key (``wrc/2024-01/ADJ-00054658.html``) cannot satisfy both: when
a decision is amended, the new version has to go *somewhere*, and writing it to
that key overwrites history.

So landing keys embed a prefix of the file's own SHA-256::

    wrc/2024-01/ADJ-00054658__a3f9c1d4.html     first version
    wrc/2024-01/ADJ-00054658__7be20f11.html     amended version, lands beside it

This buys two things at once:

1. The zone is append-only *by construction*, not by convention. There is no
   code path that can overwrite a landing object, because a different byte
   sequence produces a different key.
2. ``head_object`` becomes a sufficient change check. "This key exists" already
   implies "byte-identical content is already stored", so an unchanged document
   costs one HEAD request and no upload. We never have to download the stored
   copy to compare it.

The truncated prefix is a *disambiguator*, not an integrity mechanism: the full
64-character digest is written to MongoDB and to the object's own S3 metadata.
Its collision domain is one identifier within one partition -- a handful of
versions over the pipeline's lifetime -- not the whole corpus, which is why 32
bits is comfortable rather than merely convenient. ``hash_prefix_length`` is
configurable if a reviewer disagrees.

Curated keys are deliberately *not* content-addressed: the brief specifies
``identifier.ext``. That is safe because the curated zone is derived -- re-running
the transformation with better cleaning logic *should* replace its output.
"""

from __future__ import annotations

import functools
import hashlib
import mimetypes
import os
import posixpath
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO, Any
from urllib.parse import quote

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from ..logging_config import get_logger
from ..settings import Settings, get_settings

log = get_logger(__name__)

# Response codes MinIO/S3 use for "that object or bucket is not here". Checked by
# string because botocore models them as generic ClientErrors, not typed
# exceptions, and the set differs between head_* and get_* operations.
_NOT_FOUND_CODES = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}


class ObjectStoreError(RuntimeError):
    """Any object-store operation that failed for a reason we cannot retry.

    Deliberately narrow: transient errors are retried inside botocore (adaptive
    mode, ``max_attempts``), so anything reaching this exception is a real
    failure that belongs in the failure ledger with its URL and code.
    """


class UploadOutcome(str, Enum):
    """What an upload actually did.

    Mirrors :class:`~wrc_pipeline.storage.mongo.WriteOutcome` so idempotency is
    *observable* in the logs rather than merely asserted. A second run over the
    same date range should report ``SKIPPED_UNCHANGED`` for every document, and
    that log line is the proof the brief asks for.
    """

    UPLOADED = "uploaded"
    SKIPPED_UNCHANGED = "skipped_unchanged"


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Result of storing one document. Feeds straight into the metadata record."""

    bucket: str
    key: str
    file_hash: str  # SHA-256 of the bytes actually stored (requirement 8)
    # SHA-256 after per-request noise is stripped. This is what the key embeds
    # and what change detection compares, because the raw bytes of these pages
    # differ on every fetch (an ASP.NET render timer) while the document does
    # not. Equals file_hash when no normalisation applies.
    content_hash: str
    file_size: int
    content_type: str
    outcome: UploadOutcome

    @property
    def uri(self) -> str:
        """``s3://bucket/key`` -- what goes in ``file_path`` (requirement 7)."""
        return f"s3://{self.bucket}/{self.key}"


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def sha256_bytes(data: bytes) -> str:
    """SHA-256 of an in-memory payload.

    Used for HTML pages, which are tens of kilobytes and already fully resident
    in the Scrapy response body -- streaming them would be pointless ceremony.
    """
    return hashlib.sha256(data).hexdigest()


def sha256_fileobj(fileobj: IO[bytes], chunk_bytes: int = 65536) -> str:
    """SHA-256 of a binary stream, read in chunks and rewound afterwards.

    Chunked because the brief says to design for 1000x the evaluation volume: a
    single 300MB determination PDF must not become a 300MB allocation. The stream
    is seeked back to 0 so the caller can immediately upload from the same
    handle without having to remember to rewind it -- forgetting that produces a
    zero-byte object, which is a silent, plausible-looking failure.
    """
    digest = hashlib.sha256()
    for chunk in iter(lambda: fileobj.read(chunk_bytes), b""):
        digest.update(chunk)
    fileobj.seek(0)
    return digest.hexdigest()


def sha256_path(path: str | Path, chunk_bytes: int = 65536) -> str:
    """SHA-256 of a file on disk. Same chunking rationale as above."""
    with open(path, "rb") as handle:
        return sha256_fileobj(handle, chunk_bytes)


# --------------------------------------------------------------------------- #
# Keys
# --------------------------------------------------------------------------- #
def normalise_extension(ext: str | None) -> str:
    """Canonical form of a file extension: lowercase, dot-prefixed, or empty.

    Extensions arrive from two places -- the resolved document URL and the
    response ``Content-Type`` -- and only one of them is guaranteed to carry a
    leading dot. Normalising here keeps that inconsistency out of the key
    templates.
    """
    if not ext:
        return ""
    cleaned = ext.strip().lower()
    return cleaned if cleaned.startswith(".") else f".{cleaned}"


def content_type_for(ext: str, default: str = "application/octet-stream") -> str:
    """Best-effort MIME type for an extension.

    Stored on the object so a downstream consumer -- or a browser opening the
    MinIO console -- renders an HTML decision as a page instead of downloading
    it. HTML is special-cased with an explicit charset: the source pages contain
    non-ASCII party names, and ``mimetypes`` alone would omit the encoding.
    """
    normalised = normalise_extension(ext)
    if normalised in {".html", ".htm", ".aspx"}:
        return "text/html; charset=utf-8"
    guessed, _ = mimetypes.guess_type(f"file{normalised}")
    return guessed or default


def _render_key(template: str, **values: str) -> str:
    """Fill a key template and reject anything that is not a sane S3 key.

    Templates live in config so a reviewer can see the layout without reading
    code, which means a typo there is a runtime error rather than a syntax
    error. Both failure modes are converted into one precise message:

    * an unknown placeholder (``{body}`` instead of ``{body_slug}``) raises
      ``KeyError`` inside ``str.format``;
    * a value that is empty or contains ``..`` or ``//`` produces a key that
      technically uploads but is unreachable by prefix listing and confusing to
      anyone browsing the bucket.
    """
    try:
        key = template.format(**values)
    except KeyError as exc:
        raise ObjectStoreError(
            f"key template {template!r} references unknown placeholder {exc}; "
            f"available: {sorted(values)}"
        ) from exc

    if "//" in key or ".." in key.split("/") or key.startswith("/"):
        raise ObjectStoreError(f"refusing to build malformed object key: {key!r}")
    return posixpath.normpath(key)


def landing_key(
    body_slug: str,
    partition_key: str,
    identifier_safe: str,
    file_hash: str,
    ext: str,
    settings: Settings | None = None,
) -> str:
    """Build the content-addressed landing key for one document.

    Takes ``identifier_safe`` rather than the raw identifier: Labour Court and
    Equality Tribunal references embed forward slashes (``CD/12/572``), which
    would silently become extra key prefixes and break the
    one-object-per-identifier guarantee. ``Settings.scraping.safe_identifier()``
    produces the value this expects; the raw form stays in Mongo for fidelity.
    """
    cfg = (settings or get_settings()).s3
    if not file_hash:
        raise ObjectStoreError("landing keys are content-addressed; file_hash is required")

    return _render_key(
        cfg.landing_key_template,
        body_slug=body_slug,
        partition_key=partition_key,
        identifier=identifier_safe,
        hash_prefix=file_hash[: cfg.hash_prefix_length],
        ext=normalise_extension(ext),
    )


def curated_key(
    body_slug: str,
    identifier_safe: str,
    ext: str,
    settings: Settings | None = None,
) -> str:
    """Build the curated key: ``identifier.ext``, as the brief specifies.

    No hash suffix. The curated zone is derived output, so re-running the
    transformation with improved cleaning logic must *replace* the previous
    result rather than accumulate versions of it. The body slug is kept as a
    prefix so the bucket stays browsable once several sources exist.
    """
    return _render_key(
        (settings or get_settings()).s3.curated_key_template,
        body_slug=body_slug,
        identifier=identifier_safe,
        ext=normalise_extension(ext),
    )


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #
@functools.lru_cache(maxsize=4)
def _build_client(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    max_attempts: int,
) -> Any:
    """Create and verify an S3 client, cached per connection identity.

    Cached because a boto3 client holds its own connection pool and is safe to
    share; building one per document is a common and expensive mistake.

    The cache key is a tuple of primitives, **not** a ``Settings`` object.
    ``Settings`` is ``frozen=True`` so it looks hashable, but nested models hold
    lists (``bodies``, ``retry_http_codes``) and hashing raises ``TypeError`` --
    the same trap already hit in ``mongo.py``. Keying on the values is also more
    honest: two Settings objects pointing at the same endpoint *should* share a
    pool.

    Two MinIO-specific settings that are not optional:

    * ``addressing_style="path"`` -- the default virtual-host style resolves
      ``http://wrc-landing.localhost:9000``, which does not exist.
    * ``signature_version="s3v4"`` -- MinIO rejects the older v2 signature.

    ``mode="adaptive"`` adds client-side rate-limit backoff on top of plain
    retries, which matters when 48 partitions upload concurrently.

    Verifies with ``list_buckets`` on creation for the same reason ``mongo.py``
    pings: botocore connects lazily, so a wrong endpoint or bad credentials
    would otherwise surface only at the first upload -- potentially thousands of
    requests into a crawl.
    """
    client = boto3.session.Session().client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": max_attempts, "mode": "adaptive"},
        ),
    )
    try:
        client.list_buckets()
    except (ClientError, BotoCoreError) as exc:
        raise ObjectStoreError(
            f"Cannot reach object storage at {endpoint_url}. "
            f"Is `docker compose up -d` running?\n  {type(exc).__name__}: {exc}"
        ) from exc
    return client


def get_s3_client(settings: Settings | None = None) -> Any:
    """Return the process-wide S3 client for the configured endpoint."""
    cfg = (settings or get_settings()).s3
    return _build_client(
        cfg.endpoint_url, cfg.access_key, cfg.secret_key, cfg.region, cfg.max_attempts
    )


def _is_not_found(exc: ClientError) -> bool:
    """True if a ClientError means 'no such object/bucket' rather than a fault.

    botocore does not give these typed exceptions, and the code varies by
    operation (``head_object`` returns ``404``, ``get_object`` returns
    ``NoSuchKey``). Getting this wrong in either direction is bad: treat a fault
    as absence and we re-upload silently; treat absence as a fault and an
    ordinary first-time upload becomes a crash.
    """
    return str(exc.response.get("Error", {}).get("Code", "")) in _NOT_FOUND_CODES


# --------------------------------------------------------------------------- #
# Buckets
# --------------------------------------------------------------------------- #
def ensure_buckets(settings: Settings | None = None) -> None:
    """Create the landing and curated buckets if absent. Safe to call repeatedly.

    ``docker-compose`` already provisions both via the ``minio-init`` one-shot
    container, so in the normal path every call here is two HEAD requests. It
    exists anyway because the tests point at throwaway bucket names that
    ``minio-init`` knows nothing about, and because a pipeline that fails with
    ``NoSuchBucket`` after a fresh volume wipe is a bad first impression.
    """
    settings = settings or get_settings()
    client = get_s3_client(settings)

    for bucket in (settings.s3.landing_bucket, settings.s3.curated_bucket):
        try:
            client.head_bucket(Bucket=bucket)
            continue
        except ClientError as exc:
            if not _is_not_found(exc):
                raise ObjectStoreError(f"cannot access bucket {bucket!r}: {exc}") from exc

        try:
            # us-east-1 is the one region where a LocationConstraint is invalid
            # rather than required. MinIO reports itself as us-east-1 by default.
            if settings.s3.region and settings.s3.region != "us-east-1":
                client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": settings.s3.region},
                )
            else:
                client.create_bucket(Bucket=bucket)
            log.info("objectstore.bucket_created", extra={"bucket": bucket})
        except ClientError as exc:
            # Another worker won the race; the bucket exists, which is all we want.
            if exc.response.get("Error", {}).get("Code") in {
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            }:
                continue
            raise ObjectStoreError(f"cannot create bucket {bucket!r}: {exc}") from exc

    log.info("objectstore.buckets_ready")


# --------------------------------------------------------------------------- #
# Existence and reads
# --------------------------------------------------------------------------- #
def head_object(bucket: str, key: str, settings: Settings | None = None) -> dict[str, Any] | None:
    """Object metadata, or ``None`` if it does not exist.

    One HEAD is the cheapest possible "have we already stored this?" check, and
    because landing keys are content-addressed it is also a *complete* one.
    """
    try:
        return get_s3_client(settings).head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if _is_not_found(exc):
            return None
        raise ObjectStoreError(f"head_object failed for s3://{bucket}/{key}: {exc}") from exc


def object_exists(bucket: str, key: str, settings: Settings | None = None) -> bool:
    return head_object(bucket, key, settings) is not None


def get_bytes(bucket: str, key: str, settings: Settings | None = None) -> bytes:
    """Fetch an object into memory.

    The transformation stage's read path. In-memory is the right call there:
    BeautifulSoup needs the whole document anyway, and decisions are small. Use
    :func:`download_to_path` for anything that might not be.
    """
    try:
        response = get_s3_client(settings).get_object(Bucket=bucket, Key=key)
        with response["Body"] as stream:
            return stream.read()
    except ClientError as exc:
        raise ObjectStoreError(f"get_object failed for s3://{bucket}/{key}: {exc}") from exc


def download_to_path(
    bucket: str,
    key: str,
    destination: str | Path,
    settings: Settings | None = None,
) -> Path:
    """Stream an object to disk, creating parent directories as needed.

    ``download_fileobj`` handles ranged multipart downloads internally, so this
    stays constant-memory regardless of object size.
    """
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "wb") as handle:
            get_s3_client(settings).download_fileobj(bucket, key, handle)
    except (ClientError, BotoCoreError) as exc:
        raise ObjectStoreError(f"download failed for s3://{bucket}/{key}: {exc}") from exc
    return path


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #
def _object_metadata(
    file_hash: str,
    body_slug: str | None,
    partition_key: str | None,
    identifier: str | None,
    source_url: str | None,
) -> dict[str, str]:
    """User metadata to attach to an object.

    Costs nothing and makes each object self-describing: if the Mongo volume is
    lost, the bucket alone still says which decision each file is and what its
    full digest was. That is also why the *full* 64-char hash goes here while
    only a prefix goes in the key.

    S3 user metadata is transmitted as HTTP headers, so values must be
    header-safe ASCII. Document URLs on this site are not -- recon found literal
    spaces in paths such as ``/en/cases/2024/january/ir- sc- 00000787.html`` --
    hence the percent-encoding. Identifiers are passed in their ``_safe`` form
    for the same reason.
    """
    metadata = {"sha256": file_hash}
    if body_slug:
        metadata["body-slug"] = body_slug
    if partition_key:
        metadata["partition-key"] = partition_key
    if identifier:
        metadata["identifier"] = quote(identifier, safe="")
    if source_url:
        metadata["source-url"] = quote(source_url, safe=":/?&=%")
    return metadata


def _stream_length(stream: IO[bytes]) -> int | None:
    """Bytes remaining in a seekable stream, measured without consuming it.

    Returns ``None`` for a stream that cannot be seeked, where the only honest
    answer comes from the server after the fact.
    """
    if not getattr(stream, "seekable", lambda: False)():
        return None
    start = stream.tell()
    stream.seek(0, os.SEEK_END)
    length = stream.tell() - start
    stream.seek(start)
    return length


def put_object(
    bucket: str,
    key: str,
    data: bytes | IO[bytes],
    content_type: str,
    metadata: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> int:
    """Upload one object and return the number of bytes written.

    Two paths on purpose. ``bytes`` go through ``put_object`` -- a single request,
    which is what an HTML decision deserves. A file object goes through
    ``upload_fileobj``, which switches to multipart above botocore's threshold,
    so a large PDF is uploaded in constant memory instead of being buffered.
    """
    client = get_s3_client(settings)
    extra: dict[str, Any] = {"ContentType": content_type}
    if metadata:
        extra["Metadata"] = metadata

    try:
        if isinstance(data, bytes):
            client.put_object(Bucket=bucket, Key=key, Body=data, **extra)
            return len(data)

        # Size is measured BEFORE the upload. s3transfer closes the stream it is
        # handed, so a tell() afterwards raises "I/O operation on closed file" --
        # found by running the tests, not by reading the docs.
        size = _stream_length(data)
        client.upload_fileobj(data, bucket, key, ExtraArgs=extra)
        if size is not None:
            return size
        # Non-seekable stream: ask the server what it stored.
        head = head_object(bucket, key, settings)
        return int(head["ContentLength"]) if head else 0
    except (ClientError, BotoCoreError) as exc:
        raise ObjectStoreError(f"upload failed for s3://{bucket}/{key}: {exc}") from exc


def put_landing_document(
    body_slug: str,
    partition_key: str,
    identifier_safe: str,
    ext: str,
    data: bytes | IO[bytes],
    source_url: str | None = None,
    content_type: str | None = None,
    key_hash: str | None = None,
    settings: Settings | None = None,
) -> StoredObject:
    """Store one document in the landing zone, skipping unchanged content.

    Pass *key_hash* when the raw bytes contain per-request noise: the key is then
    derived from that normalised digest instead of the raw one, so a document
    whose only difference is a render timestamp maps to the same key and is
    skipped. The raw digest is still stored on the object and returned as
    ``file_hash``.

    The sequence is: hash the payload, derive the content-addressed key, HEAD it,
    and upload only if it is absent. Because the key *is* the hash, a hit proves
    byte equality -- there is nothing to compare and nothing to overwrite. That
    single fact is what makes the landing zone simultaneously idempotent
    (requirement 9) and append-only (the tips section).

    Note that a file-object *data* argument is consumed and closed by the upload
    (s3transfer owns the handle), so callers must not reuse it afterwards.

    Returns a :class:`StoredObject` whose ``outcome`` distinguishes the two
    cases, so ``PartitionCounters.skipped`` can be incremented from the caller
    and the second run of a range visibly reports skips rather than uploads.
    """
    settings = settings or get_settings()
    ext = normalise_extension(ext)

    if isinstance(data, bytes):
        file_hash = sha256_bytes(data)
    else:
        file_hash = sha256_fileobj(data, settings.s3.hash_chunk_bytes)

    content_hash = key_hash or file_hash
    key = landing_key(body_slug, partition_key, identifier_safe, content_hash, ext, settings)
    bucket = settings.s3.landing_bucket
    resolved_type = content_type or content_type_for(ext)

    existing = head_object(bucket, key, settings)
    if existing is not None:
        log.info(
            "objectstore.skipped_unchanged",
            extra={
                "body": body_slug,
                "partition_key": partition_key,
                "identifier": identifier_safe,
                "bucket": bucket,
                "key": key,
                "file_hash": file_hash,
            },
        )
        # Report the hash of what is ACTUALLY stored, taken from the object's own
        # metadata, not the hash of the bytes we just fetched. With normalisation
        # in play those differ -- the stored copy is the first version captured --
        # and reporting the fresh raw hash would make the metadata record
        # disagree with the file it points at, and would look like a change to
        # Mongo on every single run.
        stored_hash = (existing.get("Metadata") or {}).get("sha256") or file_hash
        return StoredObject(
            bucket=bucket,
            key=key,
            file_hash=stored_hash,
            content_hash=content_hash,
            file_size=int(existing.get("ContentLength", 0)),
            content_type=existing.get("ContentType", resolved_type),
            outcome=UploadOutcome.SKIPPED_UNCHANGED,
        )

    size = put_object(
        bucket,
        key,
        data,
        resolved_type,
        _object_metadata(file_hash, body_slug, partition_key, identifier_safe, source_url),
        settings,
    )
    log.info(
        "objectstore.uploaded",
        extra={
            "body": body_slug,
            "partition_key": partition_key,
            "identifier": identifier_safe,
            "bucket": bucket,
            "key": key,
            "file_hash": file_hash,
            "file_size": size,
            "zone": "landing",
        },
    )
    return StoredObject(
        bucket=bucket,
        key=key,
        file_hash=file_hash,
        content_hash=content_hash,
        file_size=size,
        content_type=resolved_type,
        outcome=UploadOutcome.UPLOADED,
    )


def put_curated_document(
    body_slug: str,
    identifier_safe: str,
    ext: str,
    data: bytes | IO[bytes],
    content_type: str | None = None,
    settings: Settings | None = None,
) -> StoredObject:
    """Store one transformed document as ``identifier.ext`` in the curated bucket.

    Always writes, never skips. The curated zone is derived, so the interesting
    case is precisely the one a skip would defeat: the transformation logic
    improved and the same input must now produce a different output at the same
    key. The recomputed hash is still returned and stored in the curated
    collection, which is what makes an improvement visible as a hash change.
    """
    settings = settings or get_settings()
    ext = normalise_extension(ext)

    if isinstance(data, bytes):
        file_hash = sha256_bytes(data)
    else:
        file_hash = sha256_fileobj(data, settings.s3.hash_chunk_bytes)

    key = curated_key(body_slug, identifier_safe, ext, settings)
    bucket = settings.s3.curated_bucket
    resolved_type = content_type or content_type_for(ext)

    size = put_object(
        bucket,
        key,
        data,
        resolved_type,
        _object_metadata(file_hash, body_slug, None, identifier_safe, None),
        settings,
    )
    log.info(
        "objectstore.uploaded",
        extra={
            "body": body_slug,
            "identifier": identifier_safe,
            "bucket": bucket,
            "key": key,
            "file_hash": file_hash,
            "file_size": size,
            "zone": "curated",
        },
    )
    return StoredObject(
        bucket=bucket,
        key=key,
        file_hash=file_hash,
        content_hash=file_hash,
        file_size=size,
        content_type=resolved_type,
        outcome=UploadOutcome.UPLOADED,
    )