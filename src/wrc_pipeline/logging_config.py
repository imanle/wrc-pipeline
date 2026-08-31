"""Structured JSON logging.

Requirement 10 asks for machine-readable logs carrying the current partition, the
body, found-vs-scraped counts, failed downloads with URLs and error codes, and a
per-run summary. That is a *shape* requirement, so the formatter is written by
hand (~40 lines) rather than pulled from a library: every field that appears in
the output is traceable to a line here.

Usage
-----
    log = get_logger(__name__)
    log.info("partition.start", extra={"body": "wrc", "partition_key": "2024-03"})

Anything passed in ``extra`` is promoted to a top-level JSON key, so downstream
tooling can filter with ``jq 'select(.body == "wrc")'``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Attributes present on every LogRecord. Anything outside this set arrived via
# `extra=` and therefore belongs in the JSON payload.
_RESERVED = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
    "levelname", "levelno", "lineno", "module", "msecs", "message", "msg", "name",
    "pathname", "process", "processName", "relativeCreated", "stack_info",
    "thread", "threadName", "taskName",
}

# One id per OS process, stamped on every line so a run's logs can be isolated
# even when several runs interleave in the same file.
RUN_ID = os.environ.get("WRC_RUN_ID") or uuid.uuid4().hex[:12]


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "logger": record.name,
            "run_id": RUN_ID,
        }

        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        # default=str so dates, Decimals and ObjectIds never crash a log call.
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(
    level: str = "INFO",
    log_file: str | Path | None = None,
    console: bool = True,
) -> None:
    """Install the JSON formatter on the root logger. Idempotent."""
    root = logging.getLogger()
    root.setLevel(level.upper())

    # Replace rather than append: Scrapy installs its own handlers, and without
    # this we would emit every line twice, once plain and once as JSON.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = JsonFormatter()

    if console:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(formatter)
        root.addHandler(stream)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # These are chatty at DEBUG and drown the signal we care about.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer", "pymongo"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class ContextLogger(logging.LoggerAdapter):
    """LoggerAdapter that *merges* bound context with per-call ``extra``.

    The stdlib's own adapter overwrites ``kwargs["extra"]`` with ``self.extra``,
    which silently discards every per-call field — the exact fields requirement
    10 asks for. This override merges instead, with the per-call value winning.

    ``bind()`` returns a new adapter carrying additional permanent context, so a
    spider can attach ``body`` and ``partition_key`` once and have every
    subsequent line inherit them.
    """

    def process(self, msg: Any, kwargs: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        call_extra = kwargs.get("extra") or {}
        kwargs["extra"] = {**(self.extra or {}), **call_extra}
        return msg, kwargs

    def bind(self, **context: Any) -> "ContextLogger":
        return ContextLogger(self.logger, {**(self.extra or {}), **context})


def get_logger(name: str, **context: Any) -> ContextLogger:
    """Return a logger; ``extra=`` keys land as top-level JSON fields."""
    return ContextLogger(logging.getLogger(name), dict(context))


class PartitionCounters:
    """Found-vs-scraped accounting for a single (body, partition) unit of work.

    Kept as a plain object rather than module globals so Dagster can run several
    partitions concurrently without the counts bleeding into each other.
    """

    __slots__ = ("body", "partition_key", "found", "scraped", "skipped", "failed", "failures")

    def __init__(self, body: str, partition_key: str) -> None:
        self.body = body
        self.partition_key = partition_key
        self.found = 0
        self.scraped = 0
        self.skipped = 0  # already present and unchanged -> idempotent no-op
        self.failed = 0
        self.failures: list[dict[str, Any]] = []

    def record_failure(
        self,
        url: str,
        reason: str,
        error_code: int | str | None = None,
        identifier: str | None = None,
    ) -> None:
        """Log one unrecoverable record. This is the 'every X is logged' ledger."""
        self.failed += 1
        entry = {
            "url": url,
            "reason": reason,
            "error_code": error_code,
            "identifier": identifier,
        }
        self.failures.append(entry)
        get_logger(__name__).error(
            "record.failed",
            extra={"body": self.body, "partition_key": self.partition_key, **entry},
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "partition_key": self.partition_key,
            "records_found": self.found,
            "records_scraped": self.scraped,
            "records_skipped_unchanged": self.skipped,
            "records_failed": self.failed,
            # Reconciliation check the brief asks for: found == scraped + skipped + failed.
            "reconciled": self.found == self.scraped + self.skipped + self.failed,
        }

    def emit_summary(self) -> None:
        get_logger(__name__).info("partition.summary", extra=self.as_dict())
