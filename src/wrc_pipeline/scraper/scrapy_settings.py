"""Scrapy engine settings, derived from ``config/config.yaml``.

Scrapy has its own settings system, entirely separate from the project's
Pydantic config. Rather than maintaining a second source of truth in a
``settings.py`` that Scrapy auto-discovers, this module *translates*: every value
below traces back to ``ScrapingSettings``, so a change to the YAML changes the
crawl and there is nothing to keep in sync by hand.

No local document storage
-------------------------
Documents must never be persisted to the machine running the crawl. Response
bodies live in memory only, and are streamed from there into MinIO by the item
pipeline. Scrapy has three separate features that would break that promise, so
all three are disabled *explicitly* rather than left at their defaults -- a
default is something the next person can flip on without realising what it
implies:

* ``HTTPCACHE_ENABLED`` writes every response body to ``.scrapy/httpcache/``.
  Off by default, but it is the first thing anyone enables while debugging a
  parser, and it would quietly create a complete local copy of every document.
* ``JOBDIR`` persists the pending request queue to disk for resumable crawls.
  Requests rather than responses, but still crawl state on local disk -- and it
  is unnecessary here, because a stateless GET means any partition can simply be
  re-run.
* ``FILES_STORE`` / ``FilesPipeline`` writes downloaded files to a local
  directory. Already rejected for other reasons (it keys objects by SHA-1 of the
  URL, cannot produce ``identifier.ext``, and does not write to MongoDB); this
  is the storage-side reason.

The trade-off of holding bodies in memory is that ``DOWNLOAD_MAXSIZE`` becomes a
real constraint rather than a formality, so it is set from config with a warning
threshold below it.

Note that ``logs/pipeline.jsonl`` is still written locally. That is crawl
metadata -- the structured logs requirement 10 asks for -- not document content.
"""

from __future__ import annotations

from typing import Any

from ..settings import Settings, get_settings

# Response bodies are held in memory, so these are memory limits, not disk ones.
# 64MB is far above any observed decision (all sampled documents are HTML under
# 200KB) while still bounding what a single malicious or malformed response can
# allocate.
DEFAULT_DOWNLOAD_MAXSIZE = 64 * 1024 * 1024
DEFAULT_DOWNLOAD_WARNSIZE = 8 * 1024 * 1024


def build_scrapy_settings(settings: Settings | None = None) -> dict[str, Any]:
    """Translate project config into a Scrapy settings dict.

    Returned as a plain dict so it can be handed to ``CrawlerProcess`` from the
    CLI runner *and* from a Dagster asset, without either needing a Scrapy
    project layout on disk.
    """
    cfg = (settings or get_settings()).scraping

    return {
        # --- identity ----------------------------------------------------- #
        # A descriptive UA with contact details. Politeness, but also
        # self-interest: an operator who can identify a crawler is far more
        # likely to throttle it than to ban it.
        "USER_AGENT": cfg.user_agent,
        "ROBOTSTXT_OBEY": cfg.robotstxt_obey,
        # --- throughput (requirement 1: fast without getting blocked) ----- #
        "CONCURRENT_REQUESTS": cfg.concurrent_requests,
        "CONCURRENT_REQUESTS_PER_DOMAIN": cfg.concurrent_requests_per_domain,
        "DOWNLOAD_DELAY": cfg.download_delay,
        "DOWNLOAD_TIMEOUT": cfg.download_timeout,
        # AutoThrottle adapts the delay to observed latency, which is strictly
        # better than a fixed delay: it backs off when the server slows down and
        # speeds up when it does not, so we are neither needlessly slow on a
        # healthy server nor hammering a struggling one.
        "AUTOTHROTTLE_ENABLED": cfg.autothrottle_enabled,
        "AUTOTHROTTLE_START_DELAY": cfg.autothrottle_start_delay,
        "AUTOTHROTTLE_MAX_DELAY": cfg.autothrottle_max_delay,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": cfg.autothrottle_target_concurrency,
        # --- retries ------------------------------------------------------ #
        # Note what is NOT in retry_http_codes: 404. A missing document is a
        # permanent failure, and retrying it four times wastes requests and
        # delays the run without changing the outcome. It goes to the failure
        # ledger instead.
        "RETRY_ENABLED": True,
        "RETRY_TIMES": cfg.retry_times,
        "RETRY_HTTP_CODES": cfg.retry_http_codes,
        # --- no local document storage ------------------------------------ #
        # See the module docstring. All three are set explicitly so that turning
        # any of them on is a visible decision rather than an accident.
        "HTTPCACHE_ENABLED": False,
        "JOBDIR": None,
        "FILES_STORE": None,
        "FEEDS": {},
        "DOWNLOAD_MAXSIZE": DEFAULT_DOWNLOAD_MAXSIZE,
        "DOWNLOAD_WARNSIZE": DEFAULT_DOWNLOAD_WARNSIZE,
        # --- item pipeline ------------------------------------------------ #
        # One stage. Splitting download / upload / metadata into three pipelines
        # would look tidier but would break the unit of work: a document is only
        # usefully "done" when its bytes and its metadata are both stored, and
        # three stages give three places for that to half-happen.
        "ITEM_PIPELINES": {
            "wrc_pipeline.scraper.pipelines.DocumentDownloadPipeline": 300,
        },
        # --- logging ------------------------------------------------------ #
        # Scrapy installs its own handlers and formatter on the root logger,
        # which would emit every line twice -- once plain, once as JSON. The
        # project's configure_logging() owns the root logger instead.
        "LOG_ENABLED": False,
        # Scrapy's own stats counters stay on: they are a useful independent
        # cross-check against PartitionCounters (response counts, retry counts,
        # exception types) and cost nothing.
        "STATS_DUMP": False,
        # --- correctness -------------------------------------------------- #
        # The in-memory dupefilter stays enabled as a within-run efficiency win.
        # It is NOT the idempotency mechanism -- it forgets between runs and is
        # URL-based, so it cannot see content changes. That guarantee lives in
        # the unique MongoDB index and the content-addressed object keys.
        "DUPEFILTER_DEBUG": False,
        # Items are Pydantic models, not dicts or scrapy.Item, so Scrapy must not
        # try to coerce them.
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "COOKIES_ENABLED": False,
    }