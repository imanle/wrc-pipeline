"""Structured JSON logging.
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

    __slots__ = (
        "body",
        "partition_key",
        "listings",
        "scraped",
        "skipped",
        "failed",
        "failures",
        "seen",
        "entries",
        "unidentified",
    )

    def __init__(self, body: str, partition_key: str) -> None:
        self.body = body
        self.partition_key = partition_key
        self.listings = 0
        self.scraped = 0
        self.skipped = 0  # already present and unchanged -> idempotent no-op
        self.failed = 0
        self.failures: list[dict[str, Any]] = []
        self.seen: set[str] = set()
        self.entries = 0
        self.unidentified = 0

    @property
    def found(self) -> int:
        """Alias for :attr:`listings`.
        """
        return self.listings

    @found.setter
    def found(self, value: int) -> None:
        self.listings = value

    def record_entry(self) -> None:
        self.entries += 1

    def record_unidentified(self) -> None:
        self.unidentified += 1

    def record_seen(self, identifier: str) -> None:
        self.seen.add(identifier)

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
        if identifier:
            self.seen.add(identifier)
        get_logger(__name__).error(
            "record.failed",
            extra={"body": self.body, "partition_key": self.partition_key, **entry},
        )

    def as_dict(self) -> dict[str, Any]:
        distinct = len(self.seen)
        operations = self.scraped + self.skipped + self.failed
        duplicate_listings = max(self.entries - self.unidentified - distinct, 0)
        listings_unaccounted = max(self.listings - distinct, 0)
        records_unaccounted = max(distinct - operations, 0)

        return {
            "body": self.body,
            "partition_key": self.partition_key,
            "records_found": self.listings,
            "listings_found": self.listings,
            "records_scraped": self.scraped,
            "records_skipped_unchanged": self.skipped,
            "records_failed": self.failed,
            "records_distinct": distinct,
            "listings_served": self.entries,
            "duplicate_listings": duplicate_listings,
            "listings_unidentified": self.unidentified,
            "listings_unaccounted": listings_unaccounted,
            "records_unaccounted": records_unaccounted,
            "listings_reconciled": listings_unaccounted == 0,
            "records_reconciled": records_unaccounted == 0,
            "reconciled": listings_unaccounted == 0 and records_unaccounted == 0,
        }

    def emit_summary(self) -> None:
        get_logger(__name__).info("partition.summary", extra=self.as_dict())