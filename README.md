# WRC Decisions Pipeline

Scrapes documents and metadata from the Irish
[Workplace Relations](https://www.workplacerelations.ie/en/search/?decisions=1)
Decisions and Determinations database into a landing zone (MongoDB + object
storage), then transforms them into a curated zone.

**Status: work in progress.** See "Build progress" below.

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

## Verify the setup

```bash
python -m pytest                   # unit tests
python -c "from wrc_pipeline.settings import load_settings; print(load_settings().mongo.database)"
```

> Use `python -m pytest` rather than bare `pytest`. A user-level pytest install
> elsewhere on `PATH` can shadow the one in the venv and run under the wrong
> interpreter; the `python -m` form always uses the active environment.

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

## Build progress

- [x] **Phase 0** — Recon: search endpoint, body IDs, pagination, HTML structure
- [x] **Phase 1** — Config, JSON logging, date partitioning, Docker storages, tests
- [ ] **Phase 2** — Storage clients (Mongo, S3) and item schema
- [ ] **Phase 3** — Scrapy spider: body x partition x page
- [ ] **Phase 4** — Download pipeline, hashing, idempotency
- [ ] **Phase 5** — Run summaries and reconciliation
- [ ] **Phase 6** — Transformation script (HTML cleaning, curated zone)
- [ ] **Phase 7** — Dagster orchestration
- [ ] **Phase 8** — ARCHITECTURE.md
- [ ] **Phase 9** — Full run at evaluation volume + idempotency proof

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
