"""Dagster entry point.

    dagster dev -m wrc_pipeline.orchestration.definitions

Deliberately thin: the assets hold the logic, and this module only assembles
them. That keeps the code loadable and testable without a Dagster instance --
``tests/test_orchestration.py`` imports the assets directly.

No schedule is defined. The brief asks for ingestion and transformation as
separate tasks with dependency handling, which the asset graph provides; a
schedule would be a guess about how often the WRC publishes. A daily schedule
over the trailing week would be one line
(``build_schedule_from_partitioned_job``) and is noted in ARCHITECTURE.md as the
obvious next step rather than implemented on a guess.
"""

from __future__ import annotations

from dagster import Definitions, define_asset_job

from .assets import curated_documents, landing_documents

# One job over both assets, so a backfill runs ingest then transform for each
# partition in dependency order rather than needing two launches.
# The partition space is inferred from the selected assets -- passing
# partitions_def explicitly is deprecated in Dagster 1.13 and redundant, since
# both assets already share DOCUMENT_PARTITIONS.
ingest_and_transform = define_asset_job(
    name="ingest_and_transform",
    selection=[landing_documents, curated_documents],
    description="Scrape one body-week into the landing zone, then curate it.",
)

defs = Definitions(
    assets=[landing_documents, curated_documents],
    jobs=[ingest_and_transform],
)