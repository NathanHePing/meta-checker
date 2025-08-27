# meta-check

Meta title/description validator + internal link mapper. Works on any site. Accepts 2- or 3-column CSV:

- **2 columns**: `Title,Description` (discovers pages and matches by prefix → fuzzy)
- **3 columns**: `URL,Title,Description` (checks that URL directly)

## Quick start

```bash
npm i
# or: pnpm i / yarn

# 2-col CSV on a subtree:
node src/index.js --input input.csv --base https://stage.next.ping.com --pathPrefix /en-us

# 3-col CSV (URL,Title,Desc) whole site:
node src/index.js --input input-3col.csv --base https://www.example.com --pathPrefix ""

# Rebuild internal links and meta on every run, drop cache:
node src/index.js --input input.csv --base https://stage.next.ping.com --pathPrefix /en-us --rebuildLinks true --dropCache true
