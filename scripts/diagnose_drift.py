"""Diagnose a partition that failed reconciliation.

Fetches every page of one (body, partition) search SEQUENTIALLY -- no concurrency,
so pagination drift cannot be introduced by our own request pattern -- and reports:

  * how many listing entries the site returns
  * how many DISTINCT references those entries represent
  * any reference that appears more than once, and on which page(s)

That distinguishes the two explanations for `records_missing`:

  A. Pagination drift -- the same reference on two different pages, with another
     reference appearing on none. A real loss; the stated total is right and we
     saw fewer records than exist.
  B. Displacement -- the same reference served twice occupies a slot a different
     decision would have filled. Confirmed on this source: two runs of
     2024-01-29..31 (stated total 46) returned 40 distinct with 6 repeats, and
     46 distinct with none. The 46 exist; overlap loses them.

Usage:
    python scripts/diagnose_drift.py --start 2024-01-29 --end 2024-01-31 --body wrc
"""

from __future__ import annotations

import argparse
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime

from parsel import Selector

from wrc_pipeline.settings import get_settings


def fetch(url: str, user_agent: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(request, timeout=45) as response:
        return response.read().decode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--body", default="wrc")
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    cfg = get_settings()
    scraping = cfg.scraping
    body = scraping.body_by_slug(args.body)
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    first_url = scraping.search_url(body, start, end, page=1)
    print(f"page 1: {first_url}")
    html = fetch(first_url, scraping.user_agent)

    total = scraping.parse_result_count(html)
    if total is None:
        print("could not parse the result count")
        return 1
    pages = scraping.page_count(total)
    print(f"site states: {total} results across {pages} page(s)\n")

    # reference -> list of pages it appeared on
    seen: dict[str, list[int]] = defaultdict(list)
    entries = 0

    for page in range(1, pages + 1):
        if page > 1:
            time.sleep(args.delay)  # sequential and polite
            html = fetch(scraping.search_url(body, start, end, page=page), scraping.user_agent)

        blocks = Selector(html).css(scraping.listing.record)
        for block in blocks:
            identifier = block.css(scraping.listing.identifier).get()
            if not identifier:
                continue
            entries += 1
            # Normalised, so the site's two renderings of one reference collapse.
            seen[scraping.safe_identifier(identifier.strip())].append(page)

        print(f"  page {page}: {len(blocks)} entries")

    distinct = len(seen)
    repeated = {ref: pages_ for ref, pages_ in seen.items() if len(pages_) > 1}

    print(f"\nstated total:      {total}")
    print(f"listing entries:   {entries}")
    print(f"distinct refs:     {distinct}")
    print(f"repeated refs:     {len(repeated)}")

    for ref, pages_ in sorted(repeated.items()):
        same_page = len(set(pages_)) == 1
        verdict = "SAME page (site lists it twice)" if same_page else "DIFFERENT pages (drift)"
        print(f"  {ref}: pages {pages_} -> {verdict}")

    print("\nverdict:")
    if distinct == total:
        print("  clean -- every advertised decision was served exactly once.")
        return 0

    if repeated:
        print(f"  {total - distinct} decision(s) missing: {len(repeated)} reference(s)")
        print("  were served more than once, displacing others. Re-run this")
        print("  partition; the pipeline is idempotent, so only the missing")
        print("  documents cost anything.")
    if entries < total:
        print(f"  {total - entries} advertised entr(y/ies) appeared on no page at all.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
