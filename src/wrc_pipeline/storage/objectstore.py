"""S3/MinIO object storage layer.
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
    """


class UploadOutcome(str, Enum):
    """What an upload actually did.
    """

    UPLOADED = "uploaded"
    SKIPPED_UNCHANGED = "skipped_unchanged"


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Result of storing one document. Feeds straight into the metadata record."""

    bucket: str
    key: str
    file_hash: str  # SHA-256 of the bytes actually stored (requirement 8)
    content_hash: str
    file_size: int
    content_type: str
    outcome: UploadOutcome

    @property
    def uri(self) -> str:
        """``s3://bucket/key`` -- what goes in ``file_path``"""
        return f"s3://{self.bucket}/{self.key}"


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def sha256_bytes(data: bytes) -> str:
    """SHA-256 of an in-memory payload.
    """
    return hashlib.sha256(data).hexdigest()


def sha256_fileobj(fileobj: IO[bytes], chunk_bytes: int = 65536) -> str:
    """SHA-256 of a binary stream, read in chunks and rewound afterwards.
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
    if not ext:
        return ""
    cleaned = ext.strip().lower()
    return cleaned if cleaned.startswith(".") else f".{cleaned}"


def content_type_for(ext: str, default: str = "application/octet-stream") -> str:
    """Best-effort MIME type for an extension.
    """
    normalised = normalise_extension(ext)
    if normalised in {".html", ".htm", ".aspx"}:
        return "text/html; charset=utf-8"
    guessed, _ = mimetypes.guess_type(f"file{normalised}")
    return guessed or default


def _render_key(template: str, **values: str) -> str:
    """Fill a key template and reject anything that is not a sane S3 key.
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
    """
    return str(exc.response.get("Error", {}).get("Code", "")) in _NOT_FOUND_CODES


# --------------------------------------------------------------------------- #
# Buckets
# --------------------------------------------------------------------------- #
def ensure_buckets(settings: Settings | None = None) -> None:
    """Create the landing and curated buckets if absent. Safe to call repeatedly.
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
            if settings.s3.region and settings.s3.region != "us-east-1":
                client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": settings.s3.region},
                )
            else:
                client.create_bucket(Bucket=bucket)
            log.info("objectstore.bucket_created", extra={"bucket": bucket})
        except ClientError as exc:
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
    """
    client = get_s3_client(settings)
    extra: dict[str, Any] = {"ContentType": content_type}
    if metadata:
        extra["Metadata"] = metadata

    try:
        if isinstance(data, bytes):
            client.put_object(Bucket=bucket, Key=key, Body=data, **extra)
            return len(data)

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