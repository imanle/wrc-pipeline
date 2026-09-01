"""Tests for the object storage layer.

Split in two on purpose:

* **Pure tests** (hashing, key construction) need no network and always run.
  They cover the logic that has to be right for the landing zone to be
  append-only, which is the property hardest to verify by eye during a real run.
* **Live tests** run against MinIO when ``docker compose up -d`` is up and skip
  otherwise, matching ``test_mongo.py``. Set ``WRC_REQUIRE_MINIO=1`` to turn an
  unreachable endpoint into a failure, which is what CI should do.
"""

from __future__ import annotations

import io
import os
import uuid

import pytest

from wrc_pipeline.settings import load_settings
from wrc_pipeline.storage import objectstore as store
from wrc_pipeline.storage.objectstore import ObjectStoreError, UploadOutcome

# SHA-256 of the empty string -- a published constant, so this test would catch a
# hashing change that a self-referential "hash it twice" test would not.
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


@pytest.fixture(scope="module")
def settings():
    """Base settings, unchanged. Used by the pure tests for key templates."""
    return load_settings()


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def test_sha256_bytes_matches_known_digest():
    assert store.sha256_bytes(b"") == EMPTY_SHA256


def test_streamed_hash_equals_in_memory_hash():
    """The two hashing paths must agree, or an HTML and a PDF of identical bytes
    would land at different keys and idempotency would silently break."""
    payload = b"x" * 200_000  # several chunks at the 64KB default
    assert store.sha256_fileobj(io.BytesIO(payload), 65536) == store.sha256_bytes(payload)


def test_sha256_fileobj_rewinds_the_stream():
    """Callers upload from the same handle immediately after hashing it. If the
    stream were left at EOF the upload would silently write zero bytes."""
    stream = io.BytesIO(b"determination")
    store.sha256_fileobj(stream)
    assert stream.tell() == 0
    assert stream.read() == b"determination"


def test_sha256_path(tmp_path):
    path = tmp_path / "decision.pdf"
    path.write_bytes(b"")
    assert store.sha256_path(path) == EMPTY_SHA256


# --------------------------------------------------------------------------- #
# Extensions and content types
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw,expected",
    [("html", ".html"), (".HTML", ".html"), ("  .pdf ", ".pdf"), (None, ""), ("", "")],
)
def test_normalise_extension(raw, expected):
    assert store.normalise_extension(raw) == expected


def test_content_type_html_carries_charset():
    """Party names in the source contain non-ASCII characters; a bare text/html
    would leave the browser guessing the encoding."""
    assert store.content_type_for(".html") == "text/html; charset=utf-8"


def test_content_type_pdf_and_unknown():
    assert store.content_type_for(".pdf") == "application/pdf"
    assert store.content_type_for(".weird") == "application/octet-stream"


# --------------------------------------------------------------------------- #
# Key construction
# --------------------------------------------------------------------------- #
def test_landing_key_embeds_hash_prefix(settings):
    key = store.landing_key(
        "wrc", "2024-01", "ADJ-00054658", "a3f9c1d4" + "0" * 56, ".html", settings
    )
    assert key == "wrc/2024-01/ADJ-00054658__a3f9c1d4.html"


def test_landing_key_honours_configured_prefix_length(settings):
    """The prefix length is a decision, not a constant -- prove it is wired up."""
    long_prefix = settings.model_copy(
        update={"s3": settings.s3.model_copy(update={"hash_prefix_length": 16})}
    )
    key = store.landing_key("wrc", "2024-01", "ADJ-1", "a" * 64, ".html", long_prefix)
    assert key.endswith(f"ADJ-1__{'a' * 16}.html")


def test_changed_content_produces_a_different_landing_key(settings):
    """The append-only guarantee in one assertion: same document, different
    bytes, different key -- so the new version cannot overwrite the old one."""
    first = store.landing_key("wrc", "2024-01", "ADJ-1", "a" * 64, ".html", settings)
    second = store.landing_key("wrc", "2024-01", "ADJ-1", "b" * 64, ".html", settings)
    assert first != second


def test_landing_key_requires_a_hash(settings):
    with pytest.raises(ObjectStoreError, match="content-addressed"):
        store.landing_key("wrc", "2024-01", "ADJ-1", "", ".html", settings)


def test_curated_key_is_identifier_dot_ext(settings):
    """Requirement: curated files are named identifier.ext -- no hash suffix."""
    assert (
        store.curated_key("labour-court", "CD-12-572", ".html", settings)
        == "labour-court/CD-12-572.html"
    )


def test_key_templates_reject_traversal(settings):
    """A slug or identifier that escaped its prefix would write outside the
    zone's namespace. Sanitisation happens upstream; this is the backstop."""
    with pytest.raises(ObjectStoreError, match="malformed object key"):
        store.landing_key("..", "2024-01", "ADJ-1", "a" * 64, ".html", settings)


def test_unknown_placeholder_is_reported_precisely(settings):
    """A typo in a config-held template must fail with a message that names the
    bad placeholder, not with a bare KeyError three layers down."""
    broken = settings.model_copy(
        update={
            "s3": settings.s3.model_copy(
                update={"landing_key_template": "{body}/{identifier}{ext}"}
            )
        }
    )
    with pytest.raises(ObjectStoreError, match="unknown placeholder"):
        store.landing_key("wrc", "2024-01", "ADJ-1", "a" * 64, ".html", broken)


# --------------------------------------------------------------------------- #
# Live MinIO
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def live_settings():
    """Point every live test at throwaway buckets, never the real ones."""
    base = load_settings()
    suffix = uuid.uuid4().hex[:8]
    return base.model_copy(
        update={
            "s3": base.s3.model_copy(
                update={
                    "landing_bucket": f"test-landing-{suffix}",
                    "curated_bucket": f"test-curated-{suffix}",
                }
            )
        }
    )


@pytest.fixture(scope="module")
def live_s3(live_settings):
    """Skip the live tests if MinIO is unreachable, carrying the reason.

    The skip message includes the exception type. A broad ``except`` that reports
    every failure as "MinIO not available" already hid two unrelated bugs in the
    Mongo layer, so the type is part of the message here from the start.
    """
    try:
        store._build_client.cache_clear()
        client = store.get_s3_client(live_settings)
        store.ensure_buckets(live_settings)
    except Exception as exc:  # noqa: BLE001
        message = (
            f"MinIO not reachable at {live_settings.s3.endpoint_url}: "
            f"{type(exc).__name__}: {exc}. Run `docker compose up -d`."
        )
        if os.environ.get("WRC_REQUIRE_MINIO"):
            pytest.fail(message)
        pytest.skip(message)

    yield client

    for bucket in (live_settings.s3.landing_bucket, live_settings.s3.curated_bucket):
        listing = client.list_objects_v2(Bucket=bucket)
        for obj in listing.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])
        client.delete_bucket(Bucket=bucket)


def test_ensure_buckets_is_idempotent(live_s3, live_settings):
    """Called at the start of every run, so a second call must be a no-op."""
    store.ensure_buckets(live_settings)
    store.ensure_buckets(live_settings)


def test_second_upload_of_identical_content_is_skipped(live_s3, live_settings):
    """Requirement 9, proved at the storage layer: the same bytes twice means one
    upload and one skip."""
    payload = b"<html><body>ADJ-00054658</body></html>"
    first = store.put_landing_document(
        "wrc", "2024-01", "ADJ-00054658", ".html", payload, settings=live_settings
    )
    second = store.put_landing_document(
        "wrc", "2024-01", "ADJ-00054658", ".html", payload, settings=live_settings
    )

    assert first.outcome is UploadOutcome.UPLOADED
    assert second.outcome is UploadOutcome.SKIPPED_UNCHANGED
    assert first.key == second.key
    assert second.file_size == len(payload)


def test_changed_content_lands_beside_the_original(live_s3, live_settings):
    """The landing zone is append-only: an amended decision must not replace the
    version we first captured."""
    original = store.put_landing_document(
        "wrc", "2024-02", "ADJ-00099999", ".html", b"first version", settings=live_settings
    )
    amended = store.put_landing_document(
        "wrc", "2024-02", "ADJ-00099999", ".html", b"amended version", settings=live_settings
    )

    assert original.key != amended.key
    assert store.object_exists(original.bucket, original.key, live_settings)
    assert store.object_exists(amended.bucket, amended.key, live_settings)
    assert store.get_bytes(original.bucket, original.key, live_settings) == b"first version"


def test_full_hash_is_stored_in_object_metadata(live_s3, live_settings):
    """Only a prefix goes in the key, so the object itself must carry the full
    digest -- the bucket stays self-describing if the metadata store is lost."""
    stored = store.put_landing_document(
        "wrc",
        "2024-03",
        "ADJ-00011111",
        ".html",
        b"metadata check",
        source_url="https://www.workplacerelations.ie/en/cases/2024/january/ir- sc- 00000787.html",
        settings=live_settings,
    )
    head = store.head_object(stored.bucket, stored.key, live_settings)
    assert head["Metadata"]["sha256"] == stored.file_hash
    # The source URL contains literal spaces; it must have been encoded to
    # survive being transmitted as an HTTP header.
    assert " " not in head["Metadata"]["source-url"]


def test_file_object_upload_streams_and_hashes(live_s3, live_settings):
    """The PDF path: a file object routes through upload_fileobj rather than
    being buffered as bytes."""
    payload = b"%PDF-1.4 synthetic" + b"\x00" * 150_000
    stored = store.put_landing_document(
        "wrc", "2024-04", "ADJ-00022222", ".pdf", io.BytesIO(payload), settings=live_settings
    )
    assert stored.file_hash == store.sha256_bytes(payload)
    assert stored.file_size == len(payload)
    assert stored.content_type == "application/pdf"
    assert store.get_bytes(stored.bucket, stored.key, live_settings) == payload


def test_curated_upload_overwrites_at_a_stable_key(live_s3, live_settings):
    """The curated zone is derived: improved cleaning logic must replace the old
    output at the same identifier.ext key, not accumulate versions."""
    first = store.put_curated_document(
        "wrc", "ADJ-00033333", ".html", b"<p>clean v1</p>", settings=live_settings
    )
    second = store.put_curated_document(
        "wrc", "ADJ-00033333", ".html", b"<p>clean v2</p>", settings=live_settings
    )

    assert first.key == second.key
    assert first.file_hash != second.file_hash
    assert store.get_bytes(second.bucket, second.key, live_settings) == b"<p>clean v2</p>"


def test_key_hash_controls_the_key_not_the_reported_hash(live_s3, live_settings):
    """Bytes that differ only in per-request noise must map to one key, while
    file_hash still describes what was actually stored."""
    first = store.put_landing_document(
        "wrc", "2024-06", "ADJ-00055555", ".html", b"decision <!-- t: 1 -->",
        key_hash="c" * 64, settings=live_settings,
    )
    second = store.put_landing_document(
        "wrc", "2024-06", "ADJ-00055555", ".html", b"decision <!-- t: 2 -->",
        key_hash="c" * 64, settings=live_settings,
    )

    assert first.key == second.key
    assert second.outcome is UploadOutcome.SKIPPED_UNCHANGED
    # The skip reports the hash of the STORED copy (the first version), read from
    # the object's own metadata -- not the hash of the bytes just fetched.
    assert second.file_hash == first.file_hash
    assert second.file_hash == store.sha256_bytes(b"decision <!-- t: 1 -->")
    assert second.content_hash == "c" * 64


def test_without_key_hash_the_content_hash_is_the_file_hash(live_s3, live_settings):
    stored = store.put_landing_document(
        "wrc", "2024-07", "ADJ-00066666", ".html", b"plain", settings=live_settings
    )
    assert stored.content_hash == stored.file_hash


def test_missing_object_reads_raise_not_return_none(live_s3, live_settings):
    """Absence on a read is a real failure -- the metadata record claimed this
    file exists. Returning None here would let a corrupt record pass silently."""
    with pytest.raises(ObjectStoreError):
        store.get_bytes(live_settings.s3.landing_bucket, "wrc/nope/missing.html", live_settings)


def test_download_to_path_round_trip(live_s3, live_settings, tmp_path):
    stored = store.put_landing_document(
        "wrc", "2024-05", "ADJ-00044444", ".html", b"disk round trip", settings=live_settings
    )
    destination = tmp_path / "nested" / "out.html"
    store.download_to_path(stored.bucket, stored.key, destination, live_settings)
    assert destination.read_bytes() == b"disk round trip"
