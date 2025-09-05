## Windborne Constellation History Fetcher

This repo contains a robust fetcher that queries Windborne's live constellation API and extracts flight history from potentially messy/undocumented outputs.

- Source: `https://a.windbornesystems.com/treasure/00.json` is "now"; `01.json` is ~1 hour ago, up to `23.json`.
- The fetcher tolerates JSON5, stray characters, trailing commas, partial HTML wrappers, and varying data shapes.
- Outputs standardized records with fields: `id`, `time_iso` (UTC ISO8601), `lat`, `lon`, `alt_m` (meters), `source_hour`, `raw_index`.

### Setup

```bash
conda create -y -n wb-demo python=3.11
conda run -n wb-demo pip install -r requirements.txt
```

### Usage

Fetch the last 24 hours and write outputs under `out/`:

```bash
conda run -n wb-demo python fetch_constellation.py \
  --out-dir out \
  --hours 24 \
  --verbose
```

Options:
- `--hours`: number of hours back to fetch (1-24). Defaults to 24.
- `--timeout`: per-request timeout seconds (default 15.0).
- `--max-concurrency`: maximum parallel HTTP requests (default 8).
- `--save-raw`: also write raw responses to `out/raw/00.json`...`out/raw/23.json`.
- `--verbose`: print progress and fetch/parse errors to stderr.

### Outputs
- `out/constellation.ndjson`: one JSON object per line
- `out/constellation.csv`: CSV with header `id,time_iso,lat,lon,alt_m,source_hour,raw_index`

### Visualize on a world map (CARTO)

After generating `out/constellation.ndjson`, you can view the constellation on a CARTO basemap:

```bash
conda run -n wb-demo python fetch_constellation.py --out-dir out --hours 24 --verbose
```

Then open the local viewer:

```bash
npx --yes http-server -p 8080 | cat
# Visit http://localhost:8080/viewer/
```

If you prefer Python:

```bash
python -m http.server 8080
# Visit http://localhost:8080/viewer/
```

The viewer is in `viewer/` and reads `out/constellation.ndjson` directly from the repo.

### Library helper

The helper `utils.py` includes a small reader for the CSV output:

```python
from utils import read_constellation_csv
rows = read_constellation_csv("out/constellation.csv", balloon_id=None)
```

### Notes
- Records missing an explicit timestamp are assigned the snapshot hour's approximate time in UTC.
- Location and altitude fields are detected via flexible heuristics, including E7 lat/lon and common unit strings (m, km, ft).
- Duplicate records (same id, time, and 1e-6 rounded lat/lon) are de-duplicated across hours.
