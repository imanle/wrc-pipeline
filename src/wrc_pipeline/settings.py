"""Configuration loading and validation.

Design notes
------------
One mechanism, not two. ``config/config.yaml`` owns the *structure* and the
defaults; ``${VAR:-default}`` placeholders inside it are expanded from the
environment at load time. That means every single setting is overridable by an
env var without the YAML enumerating which ones are "env-backed", and secrets
never have to be committed.

Everything is then validated through Pydantic models, so a bad partition size or
a malformed date fails loudly at startup instead of three hours into a crawl.
"""

from __future__ import annotations

import functools
import math
import os
import re
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator

# Repo root = two levels above this file (src/wrc_pipeline/settings.py).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

# Matches ${NAME} and ${NAME:-default}. The default may itself be empty.
_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


class PartitionSize(str, Enum):
    """Supported date-partition granularities."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


# --------------------------------------------------------------------------- #
# Env expansion
# --------------------------------------------------------------------------- #
def _expand_str(value: str) -> str:
    """Replace every ``${VAR}`` / ``${VAR:-default}`` occurrence in *value*.

    Raises
    ------
    KeyError
        If a placeholder has no default and the variable is unset. Failing here
        is deliberate: a silently-empty connection string is far worse than a
        crash on line one.
    """

    def _replace(match: re.Match[str]) -> str:
        name = match.group("name")
        default = match.group("default")
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise KeyError(
            f"Config references ${{{name}}} but it is not set and has no default. "
            f"Add it to your .env file."
        )

    return _ENV_PATTERN.sub(_replace, value)


def _expand(node: Any) -> Any:
    """Recursively expand env placeholders through dicts, lists and strings."""
    if isinstance(node, dict):
        return {key: _expand(val) for key, val in node.items()}
    if isinstance(node, list):
        return [_expand(item) for item in node]
    if isinstance(node, str):
        return _expand_str(node)
    return node


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class MongoSettings(BaseModel):
    uri: str
    database: str
    landing_collection: str = "documents_landing"
    curated_collection: str = "documents_curated"
    runs_collection: str = "pipeline_runs"
    failures_collection: str = "pipeline_failures"
    server_selection_timeout_ms: int = 5000


class S3Settings(BaseModel):
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    landing_bucket: str
    curated_bucket: str
    landing_key_template: str
    curated_key_template: str
    hash_prefix_length: int = Field(default=8, ge=6, le=64)
    hash_chunk_bytes: int = Field(default=65536, gt=0)
    max_attempts: int = Field(default=5, gt=0)

    @model_validator(mode="after")
    def _distinct_buckets(self) -> "S3Settings":
        # The brief is explicit that the landing zone is never mutated. Sharing a
        # bucket between zones is the easiest way to violate that by accident.
        if self.landing_bucket == self.curated_bucket:
            raise ValueError("landing_bucket and curated_bucket must differ")
        return self


class PartitionSettings(BaseModel):
    size: PartitionSize
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def _ordered(self) -> "PartitionSettings":
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) must not be after end_date ({self.end_date})"
            )
        return self


class BodySettings(BaseModel):
    """One entry from the site's left-hand 'Body' facet.

    ``earliest_date`` comes from the WRC's own search guide and lets the spider
    skip partitions that provably cannot hold records (e.g. WRC adjudication
    decisions only exist from 2015-10-01). That both saves requests and removes
    the ambiguity between "empty month" and "broken date filter".
    """

    name: str
    slug: str
    value: str
    earliest_date: date | None = None
    identifier_pattern: str | None = None

    @field_validator("slug")
    @classmethod
    def _slug_is_key_safe(cls, value: str) -> str:
        # The slug becomes part of an S3 key, so keep it boring.
        if not re.fullmatch(r"[a-z0-9-]+", value):
            raise ValueError(f"slug must be lowercase alphanumeric/hyphen, got {value!r}")
        return value

    @field_validator("identifier_pattern")
    @classmethod
    def _pattern_compiles(cls, value: str | None) -> str | None:
        # Fail at startup, not on record 400.
        if value is not None:
            re.compile(value)
        return value

    def covers(self, window_end: date) -> bool:
        """True if this body could plausibly have records on or before *window_end*."""
        return self.earliest_date is None or window_end >= self.earliest_date

    def validate_identifier(self, identifier: str | None) -> bool:
        """Cheap guard against a silently-broken selector.

        ``re.match`` caches compiled patterns internally, so recompiling per call
        is free in practice. Kept as a method rather than a cached_property to
        avoid Pydantic model-attribute edge cases.
        """
        if not identifier:
            return False
        if self.identifier_pattern is None:
            return True
        return bool(re.match(self.identifier_pattern, identifier.strip()))


class ListingSelectors(BaseModel):
    """CSS selectors for one record on the search results page.

    Confirmed against live markup during recon. Held in config rather than
    hardcoded in the spider so a site redesign is a YAML edit, and so the
    selectors are visible to a reviewer without reading spider code.
    """

    record: str
    identifier: str
    identifier_crosscheck: str | None = None
    published_date: str
    description: str
    document_link: str
    document_link_fallback: str | None = None


class DocumentExtensions(BaseModel):
    """Which file extensions are treated as HTML vs binary (requirement 6)."""

    html: list[str] = Field(min_length=1)
    binary: list[str] = Field(min_length=1)

    def is_html(self, ext: str) -> bool:
        return ext.lower() in {e.lower() for e in self.html}

    def is_binary(self, ext: str) -> bool:
        return ext.lower() in {e.lower() for e in self.binary}


class ScrapingSettings(BaseModel):
    base_url: str
    search_path: str
    decisions_flag: int = 1
    date_display_format: str = "%d/%m/%Y"
    # Confirmed by recon: &pageNumber=2, 1-based, omitted for page 1.
    page_param: str = "pageNumber"
    result_count_pattern: str = (
        r"Shows\s+(\d+)\s+to\s+(\d+)\s+of\s+([\d,]+)\s+results"
    )
    max_pages_per_partition: int = Field(default=500, gt=0)
    page_size: int = Field(gt=0)
    user_agent: str
    concurrent_requests: int = Field(gt=0)
    concurrent_requests_per_domain: int = Field(gt=0)
    download_delay: float = Field(ge=0)
    download_timeout: int = Field(gt=0)
    autothrottle_enabled: bool
    autothrottle_start_delay: float
    autothrottle_max_delay: float
    autothrottle_target_concurrency: float = Field(gt=0)
    retry_times: int = Field(ge=0)
    retry_http_codes: list[int]
    robotstxt_obey: bool
    bodies: list[BodySettings] = Field(min_length=1)
    listing: ListingSelectors
    document_extensions: DocumentExtensions
    identifier_unsafe_chars: str = '/\\:*?"<>| '
    identifier_replacement: str = "-"

    def parse_result_count(self, text: str) -> int | None:
        """Extract the total from ``Shows 1 to 10 of 234 results``.

        Returns ``None`` when the phrase is absent, which the caller must treat
        as a hard failure rather than "zero results": without a trustworthy
        total there is no baseline to reconcile records_found against, and a
        silently-truncated partition is the failure mode this whole pipeline is
        designed to make impossible.
        """
        match = re.search(self.result_count_pattern, text)
        if not match:
            return None
        return int(match.group(3).replace(",", ""))

    def page_count(self, total_results: int) -> int:
        """Number of listing pages holding *total_results* records.

        The site fixes page size at 10 and exposes the total on page 1, so the
        full page range is knowable after a single request. Pages 2..N are then
        issued concurrently rather than discovered by following "next" links --
        this is the main throughput lever in the crawl (requirement 1).
        """
        if total_results <= 0:
            return 0
        pages = math.ceil(total_results / self.page_size)
        return min(pages, self.max_pages_per_partition)

    def format_input_date(self, value: date) -> str:
        """Render a date the way the site's ``from``/``to`` params expect it.

        The site uses unpadded day and month (``18/8/2026``). Built by hand
        rather than with ``strftime("%-d/%-m/%Y")`` because the ``%-`` no-pad
        modifier is a glibc/BSD extension that raises on Windows -- and this
        format is a hard requirement of the endpoint, not a cosmetic choice.
        """
        return f"{value.day}/{value.month}/{value.year}"

    def parse_display_date(self, text: str) -> date:
        """Parse a date as rendered in search results (zero-padded ``20/08/2026``)."""
        return datetime.strptime(text.strip(), self.date_display_format).date()

    def search_url(
        self,
        body: "BodySettings",
        start: date,
        end: date,
        page: int | None = None,
        keyword: str | None = None,
    ) -> str:
        """Build one search URL for a (body, partition[, page]) tuple.

        Centralised here so the spider, the tests and any future source adapter
        all agree on the contract, and so a change to the site's parameters is a
        one-line edit rather than a hunt through the spider.
        """
        params: dict[str, str] = {
            "decisions": str(self.decisions_flag),
            "from": self.format_input_date(start),
            "to": self.format_input_date(end),
            "body": body.value,
        }
        if keyword:
            params["q"] = keyword
        if page and page > 1:
            params[self.page_param] = str(page)

        base = f"{self.base_url.rstrip('/')}{self.search_path}"
        # safe="/" keeps the d/M/yyyy slashes readable rather than %2F-encoded;
        # the site accepts both, and readable URLs make the logs debuggable.
        return f"{base}?{urlencode(params, safe='/')}"

    def safe_identifier(self, identifier: str) -> str:
        """Filesystem/object-key-safe form of a reference.

        Labour Court and Equality Tribunal references embed forward slashes
        (``CD/12/572``, ``EE/2006/040``). The brief requires curated files be
        named ``identifier.ext``; left raw, those slashes would silently become
        nested object prefixes and break the one-file-per-identifier guarantee.
        The raw value is still persisted separately for fidelity.
        """
        cleaned = identifier.strip()
        for char in self.identifier_unsafe_chars:
            cleaned = cleaned.replace(char, self.identifier_replacement)
        # Collapse runs of the replacement char and trim it from the edges.
        while self.identifier_replacement * 2 in cleaned:
            cleaned = cleaned.replace(self.identifier_replacement * 2, self.identifier_replacement)
        return cleaned.strip(self.identifier_replacement)

    @model_validator(mode="after")
    def _unique_slugs(self) -> "ScrapingSettings":
        slugs = [body.slug for body in self.bodies]
        duplicates = {slug for slug in slugs if slugs.count(slug) > 1}
        if duplicates:
            raise ValueError(f"duplicate body slugs: {sorted(duplicates)}")
        return self

    def body_by_slug(self, slug: str) -> BodySettings:
        for body in self.bodies:
            if body.slug == slug:
                return body
        raise KeyError(f"unknown body slug: {slug!r}")


class TransformSettings(BaseModel):
    """HTML cleaning rules for the curated zone (transformation stage).

    Whitelist-first: ``content_selectors`` positively selects the decision body
    and the rest of the document is discarded, so page chrome the site adds in
    future is excluded without us having to enumerate it.
    """

    content_selectors: list[str] = Field(min_length=1)
    title_selector: str | None = None
    strip_selectors: list[str]
    preserve_selectors: list[str] = Field(default_factory=list)
    drop_empty_elements: bool = True
    normalise_whitespace: bool = True
    min_content_chars: int = Field(ge=0)


class LoggingSettings(BaseModel):
    level: str = "INFO"
    file: str | None = "logs/pipeline.jsonl"
    console: bool = True

    @field_validator("level")
    @classmethod
    def _known_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = value.upper()
        if upper not in allowed:
            raise ValueError(f"log level must be one of {sorted(allowed)}, got {value!r}")
        return upper


class Settings(BaseModel):
    """Root config object. Immutable once loaded."""

    model_config = {"frozen": True}

    mongo: MongoSettings
    s3: S3Settings
    partitions: PartitionSettings
    scraping: ScrapingSettings
    transform: TransformSettings
    logging: LoggingSettings


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def load_settings(config_path: str | Path | None = None) -> Settings:
    """Read .env, load the YAML, expand placeholders, validate.

    Parameters
    ----------
    config_path
        Overrides the default ``config/config.yaml``. Also honours the
        ``WRC_CONFIG_PATH`` env var, which is how the Dagster process and the
        Scrapy process are pointed at the same file in CI.
    """
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    path = Path(config_path or os.environ.get("WRC_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
    if not path.is_file():
        raise FileNotFoundError(f"config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    return Settings.model_validate(_expand(raw))


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide cached settings.

    Cached because Scrapy pipelines, middlewares and the spider all need config
    and re-reading the file per component would let them drift mid-run.
    """
    return load_settings()
