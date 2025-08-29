# meta-check

## Input Files Accepted
Meta title/description validator + internal link mapper. Works on any site. Accepts 1-, 2- or 3-column CSV:

- **1 columns**: infers which one:
    - `url`
        - if ≥60% rows look like URLs (absolute or slash-rooted).
    - `title`
        - if col1≈shorter text... avg < 120 chars
    - `description` 
        - if col1≈longer text... avg ≥ 120
- **2 columns**: infers which one:
    - `url, title`
        - if col1≈URL and col2≈shorter text.
    - `url, description`
        - if col1≈URL and longer text.
    - `title, description`
        - if col1 !≈URL and col2 !≈URL.

- **3 columns**: `URL,Title,Description`
- **No file or Empty file**
    - treats both as no input
    - can not output existance or comparison files


## Quick start

```bash
npm i

npm run dev:app
### or, for better logs
NO_VIZ=1 npm run dev:app

### Standard run(can be chosen in control panel as well)
set PLAYWRIGHT_HEADLESS=1 && node scripts/shard-run.js --base stage.next.ping.com --pathPrefix /en-us --bucketParts 1 --shardCap 1 --concurrency 1 --outDir ./dist


## Config Flag and Meanings
--base url
    - Base url
        - ex. https://stage.ping.com
--pathPrefix /ex.
    - Optional prefix (usually for region resstriction)
        - ex. /en-us
--concurrency N
    - How many browser tabs can be open at a time (per shard)
--shards N
    - How many shards to allow this program to use at a time
--maxShards
    - Auto runs max number of shards
MC_POLITE_DELAY_MS=N
    - Safe Mode Delay
        - ex. 500 (half a second delay)
--dropCache true
    - Drops the previous cache to not be used
--keepPageParam true
    - this keeps pages that end in ?page=
        - Warning: bloats run time and export files
```

## Output files
urls-final.txt
    - All urls found (trims duplicates)
site_catalog.csv
    - All of the crawled urls with meta data (titles and descriptions)
internal-links
    - Lists every link/button found in the site (what page they are on and where they link to)
discovery-tree.json
    - The tree of the full website
duplicate-titles.txt
    - Any duplicates of meta data titles found

### Existance files (only for input that contains url's)
url-existence.csv/json
    - Whether the urls in the input file exist or not
working-urls.txt
    - What urls from the input file work
non-working-urls.txt
    - What urls from the input file do not work

### Comparison files (Only for input that contains titles or descriptions)
comparison.csv
    - A row by row structured report with
        -url
        expected title and description
        actual title and description
        if they match (fuzze or not as well)
mismatches.txt
    - What urls did not match
not-found.txt
    - Input rows that were not found at all
ambiguous-fuzzy.txt
    - Rows where there were multiple matches or fuzzy matches
duplicate-titles.txt
    - List of titles that occur multiple times
extra-not-in-input.txt
    - Pages found that were not in input file

## Main File Overview
/scritps/shard-run.js
    - Role: Orchestrates run
        - Launches electron UI, gates Apply and Start button, builds seed buckets, spawns workers 'threads/shards', and merges outputs
    - Key functions:
        - Bucket sizing, spawns args, manages retries, top level logging
/src/run.js
    - Role: Brain of the worker. 
        - Desides input shape, chooses a mode according to the input file, fetches pages using Playwright, extracts internal links and calls report writers
    - Key functions:
        - Input sniffing, Discovery pipeline, Fetching, Reports
/src/discover/crawler
    - Role: Fallback crawler when there is no sitemap.
        - Extracts, normalizes same-site links and enqueues/de-duplicates
    - Key functions:
        - Helps the runner stat on same origin and prefix. 
/src/discover/frontier
    - Role: Bucket reader for multiple shards to collaborate.
    - Key functions:
        - Cursor file, exlusive locks per URL
/src/utils/telemetry.js
    - Role: Events/status updates for Electron UI
    - Key functions:
        - Updates mode steps, tree updates, per-thred phases, counters and preflight gating

### Less Messed With Files, but Their Feelings Matter Too

/src/index.js
    - Role: Imports and invokes run() using the orchestrators config
    - Key functions:
        - Wires new modes and flags into workers
/src/discover/sitemap.js
    - Role: Pulls and parses common sitemap endpoints, collects same-origin urls
/src/extract/links.js
    - Role: Extracts internal link candidates, normalizes and returns url, text and kind records
/src/io/reports.js
    - Role: Writes all reports
/src/io/csv.js
    - Role: Parses CSV/TSV/TXT 
src/cache/file-cache.js
    - Role: Read/write of the per-shard fetch cache (titles, descriptions, links, lastFetched timestamps)
src/utils/time.js
    - Role: Tells time
src/utils/log.js
    - Role: Helps organize logs, specifically for multi-threading
src/match/normalize.js
    - Role: normalizes the urls
src/match/matcher.js
    - Role: Title/Description matcher for comparison outputs

## Typical Data Flow
1. Resolves mode (run.js)
2. Discovers url (erither sitemap.js or crawler.js)
3. Normalize
4. Fetch Pages (links.js)
5. Emit reports (reports.js)
6. Shards merged by orchestrator
