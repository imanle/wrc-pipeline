# Architecture

## System overview

```text
Website
  ↓
Scrapy spider
  ↓
DocumentDownloadPipeline
  ↓
Landing Zone
├── MinIO: raw source documents
└── MongoDB: landing metadata
  ↓
Dagster
  ↓
Transformation runner
  ↓
Cleaner (HTML only)
  ↓
Curated Zone
├── MinIO: cleaned/derived documents
└── MongoDB: curated metadata
```

Dagster orchestrates the two stages over the same `(body × week)` partition space:

```text
landing_documents
        ↓
curated_documents


## Why weekly partitions

I started with monthly. A month of WRC results is 234 records, which is 24 pages
of 10. I fetch those pages at the same time, and records went missing.

I checked by hand, fetching one page at a time with a delay:

| Run | Pages | Entries served | Distinct decisions |
|---|---|---|---|
| Sequential, 0.5s delay | 5 | 46 | **40** |
| Normal crawl | 5 | 46 | **46** |

Same body, same three days, same stated total of 46. The second run proves 46
decisions exist. So in the first run, 6 entries were served twice and pushed 6
real decisions off the pages entirely.

This is the site, not my concurrency — it happened while fetching one page at a
time. The site's page windows shift between requests, and when two pages overlap,
whatever should have been in the gap is never sent.

Weekly cuts a month into 5 partitions of about 6 pages each, so there are fewer
page edges to lose records at. It does not fix the problem, because no client can
fix someone else's pagination. So I also made the pipeline **notice**.

## How I count, and why it took two tries

My first check was `found == scraped + skipped + failed`. It passed on a run that
had lost 6 records, because 6 duplicates and 6 missing records cancel out exactly.

Now I count three separate things:

- **listings_found** — the total the site advertises
- **listings_served** — entries actually delivered across the pages
- **records_distinct** — how many different decisions those were

And two checks instead of one:

- **listings** — did we see every decision the site advertised? If not, the
  problem is in fetching.
- **records** — did every decision we saw get stored? If not, the problem is in
  storage.

Splitting them tells you *where* to look. Duplicates are reported too, since they
explain a shortfall.

The run's final verdict asks a different question again: **is the partition
complete in the database?** A single pass can miss records an earlier pass already
stored. Failing on that would mean a complete partition fails forever.

## Retries and rate limiting

- **AutoThrottle** adjusts the delay based on how fast the server responds, so it
  backs off when the site slows down instead of using a fixed delay.
- **8 requests at a time** to one domain, with a 0.25s base delay.
- **Retries** on 429 and 5xx, four times. **Not on 404** — a missing document
  stays missing, and retrying wastes four requests. It goes in the failure log
  with its code instead.
- **Dagster retries a partition twice.** If the store is short, the run exits
  non-zero and Dagster runs it again. Re-running is cheap because nothing already
  stored is downloaded again, so it fills the gaps and stops.
- **A real, descriptive user agent** with contact details. An operator who can
  see who you are is more likely to throttle you than ban you.

## Deduplication

Requirement 9 asks for two things that cannot both be true: don't re-download
unchanged files, *and* use the file hash to detect changes. You cannot hash a file
you did not download.

Four things together:

1. **A unique index on (body, identifier)** in MongoDB. Duplicate records are
   impossible, not just unlikely.

2. **The file name contains its own hash**
   (`wrc/2024-W05/ADJ-00054658__a3f9c1d4.html`). Same bytes means same name, so
   "does this name exist?" already answers "have we got this exact file?" — one
   cheap check, no download, no comparison. And because different bytes mean a
   different name, an amended decision lands **beside** the old one instead of
   replacing it. The landing zone cannot be overwritten by accident.

3. **The comparison hash ignores noise.** Every page ends with
   `<!-- Elapsed time: 0.046756 -->`, and cached pages add
   `<!-- cached or not being index.aspx page -->`. Both change between requests.
   Hashing raw bytes made all 234 January documents look amended on the second
   run. So those two comments are stripped before hashing for comparison. The
   stored file is untouched and its real hash is still recorded.

4. **Conditional requests** (`If-None-Match` / `If-Modified-Since`) so the server
   can say "nothing changed" without sending the file. This is the only way to
   satisfy both halves of requirement 9. **WRC sends no ETag or Last-Modified**,
   so it never triggers here and the hash comparison does the work. It stays for
   sources that do support it.

## What I would change for 50+ sources

**Already fine:** bodies, URLs, selectors, date formats and identifier patterns
all live in `config.yaml`, so adding a body is a config change. The partitioning
logic and all storage code are source-agnostic.

**Would need to change:**

- **One config file per source.** `config.yaml` is already long for one site.
- **A source interface.** The spider currently knows this site's search URL and
  pagination. That should become a small class each source implements: build a
  URL, parse a listing page, find the total. Everything else stays as it is.
- **The failure ledger becomes the main tool.** With one source you can read the
  logs. With 50 you need "which sources broke this week, and how" as a query. The
  data is already in MongoDB; it needs a dashboard.
- **Per-source schedules and limits.** One rate limit for 50 sites is either too
  slow for most or too fast for some.
- **Alerts on the counts, not on crashes.** A source that quietly starts
  returning 30 records instead of 300 will not crash anything. The reconciliation
  numbers already catch it; something needs to be watching them.
