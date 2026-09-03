"""Scrapy engine settings, derived from ``config/config.yaml``."""

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
        "LOG_ENABLED": False,
        # Scrapy's own stats counters stay on: they are a useful independent
        # cross-check against PartitionCounters (response counts, retry counts,
        # exception types) and cost nothing.
        "STATS_DUMP": False,

        "DUPEFILTER_DEBUG": False,
        # Items are Pydantic models, not dicts or scrapy.Item, so Scrapy must not
        # try to coerce them.
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "COOKIES_ENABLED": False,
    }