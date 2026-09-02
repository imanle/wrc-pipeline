# WRC Decisions Pipeline

Scrapes documents and metadata from the Irish
[Workplace Relations](https://www.workplacerelations.ie/en/search/?decisions=1)
Decisions and Determinations database into a landing zone (MongoDB + object
storage), then transforms them into a curated zone.

---

## Prerequisites

- **Python 3.11+** (3.12 recommended). macOS ships 3.9 as its system Python,
  which is too old — `python3 --version` will tell you. Install a newer one with
  `brew install python@3.12`.
- Docker Desktop (running)

## Setup

```bash
# 1. Virtual environment — use an explicit 3.11+ interpreter, not bare `python3`
python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python --version                   # confirm 3.11+ before continuing

# 2. Dependencies
pip install --upgrade pip
pip install -e ".[dev]"

# 3. Environment file
cp .env.example .env               # edit if you need non-default ports

# 4. Storages (MongoDB + MinIO, with buckets created automatically)
docker compose up -d
docker compose ps                  # mongo + minio should be "healthy"
```

MinIO console: <http://localhost:9001> (`minioadmin` / `minioadmin`)
MongoDB: `mongodb://wrc:wrc_password@localhost:27017/?authSource=admin`


## Running the pipeline

Three ways in. All of them use the same spider and the same settings.

### 1. Scrape (command line)

```bash
# see the requests that would be made, fetch nothing
python -m wrc_pipeline.cli --dry-run \
  --start-date 2024-01-01 --end-date 2024-01-31 --bodies wrc

# scrape one body for one month
python -m wrc_pipeline.cli \
  --start-date 2024-01-01 --end-date 2024-01-31 --bodies wrc
```

Options: `--bodies` takes comma-separated slugs (`wrc`, `labour-court`,
`equality-tribunal`, `employment-appeals-tribunal`) and defaults to all four.
`--partition-size` takes `daily|weekly|monthly|quarterly|yearly`. Dates default
to `RUN_START_DATE` / `RUN_END_DATE` in `.env`.

**Exit code 0** means every partition is complete in the database. **1** means a
partition is short and should be re-run — re-running is safe and only fetches
what is missing.

### 2. Transform

```bash
python -m wrc_pipeline.transform --start-date 2024-01-01 --end-date 2024-01-31
```

Reads landing metadata for the range, cleans each HTML document down to the
decision itself, renames it to `identifier.ext`, and writes it to the curated
bucket with a fresh hash. PDFs and DOCs are copied unchanged. Re-running skips
anything already curated from the same source.

### 3. Orchestrated (Dagster)

```bash
export DAGSTER_HOME="$PWD/.dagster_home"
mkdir -p "$DAGSTER_HOME" && touch "$DAGSTER_HOME/dagster.yaml"
dagster dev -p 3000
```

Then <http://localhost:3000> → **Jobs** → `ingest_and_transform` → **Partitions**
→ **Materialize**.

Two assets, `landing_documents` → `curated_documents`, partitioned by
**(body x week)**. Pick one cell (e.g. body `wrc`, period `2024-01-01`) or a
range of weeks (`[2024-01-01...2024-01-29]`). Dagster runs the two stages in
order and retries a partition that comes back short.

Without `DAGSTER_HOME` set, Dagster uses a temporary directory and forgets every
run when it exits.

## Looking at the results

```bash
# how many records, by partition
python -c "
from wrc_pipeline.storage import mongo
from wrc_pipeline.settings import get_settings
c = mongo.landing_collection(get_settings())
print('records:', c.count_documents({}))
for p in sorted(c.distinct('partition_key')):
    print(' ', p, c.count_documents({'partition_key': p}))"

# stored documents
docker compose exec minio mc ls --recursive local/decisions-landing/ | wc -l

# the run summary
jq -c 'select(.event | endswith(".summary"))' logs/pipeline.jsonl | tail -3
```

MinIO console: <http://localhost:9001>. MongoDB Compass:
`mongodb://wrc:wrc_password@localhost:27017/?authSource=admin`, database
`decisions`.

Logs are JSON, one object per line, in `logs/pipeline.jsonl`.

## Known limitations

- **The site's pagination overlaps.** Its search pages sometimes serve the same
  record on two consecutive pages, which pushes other records off the results
  entirely. Measured while fetching one page at a time, so it is not caused by
  concurrency. The pipeline detects the shortfall and Dagster re-runs the
  partition; details and numbers are in `ARCHITECTURE.md`.
- **No PDFs in the tested range.** Every document sampled across three bodies
  and 2010-2024 was HTML. The pass-through branch for PDF/DOC files is
  implemented and unit-tested against synthetic files, but no live document has
  exercised it.
- **Conditional requests never fire against this source.** WRC sends no `ETag`
  or `Last-Modified`, so the `304 Not Modified` path is unit-tested only. Change
  detection falls back to comparing hashes, which is correct but re-downloads.
- **Two tribunals no longer publish.** The Equality Tribunal and Employment
  Appeals Tribunal stopped issuing decisions years ago, so recent partitions for
  them are correctly empty. There is no per-body end date, so those partitions
  are still crawled to discover that.

## Configuration

All tunables live in `config/config.yaml`. Every value supports
`${VAR:-default}` expansion, so any setting can be overridden by an environment
variable without editing the file. Secrets belong in `.env` (gitignored);
`.env.example` documents the full set.

## Teardown

```bash
docker compose down                # stop containers, keep data
docker compose down -v             # stop and delete all data
```

---

## Where things go

| | Landing (raw) | Curated (cleaned) |
|---|---|---|
| Files | `decisions-landing` bucket | `decisions-curated` bucket |
| Key | `wrc/2024-W05/ADJ-00054658__a3f9c1d4.html` | `wrc/ADJ-00054658.html` |
| Metadata | `decisions.documents_landing` | `decisions.documents_curated` |

Landing keys embed a hash of the file, so an amended decision lands beside the
old version instead of replacing it. Nothing in the landing zone is ever
overwritten. Also in MongoDB: `pipeline_runs` (one document per run, with totals)
and `pipeline_failures` (every record that could not be fetched, with its reason
and HTTP code).

## The source's URL contract (confirmed by recon)

```
https://www.workplacerelations.ie/en/search/
  ?decisions=1
  &from=<d/M/yyyy>      unpadded, e.g. 1/1/2024
  &to=<d/M/yyyy>        unpadded
  &body=<id>            15376=WRC  3=Labour Court  1=Equality Tribunal  2=EAT
  &pageNumber=<n>       1-based, omitted for page 1
```

Stateless GET: no session cookie and no ASP.NET `__VIEWSTATE` required, so every
`(body, partition, page)` tuple is an independent, retryable request. 10 results
per page; the total is exposed on page 1 as `Shows 1 to 10 of 234 results`.
