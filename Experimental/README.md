# ioos_downloader

**Prototype for GSoC 2026 — Enhancing CrocoLakeTools with IOOS Data Sync from ERDDAP**

{Note that this is just experimental and decoupled from crocolaketools for now.}

This is a standalone prototype of the ERDDAP downloader layer being designed for [CrocoLakeTools](https://github.com/boom-lab/crocolaketools-public). It is intentionally decoupled from CrocoLakeTools so the design can be developed and tested in isolation before being ported back in.

## Architecture

Although its for layered but we can bring it within the three layer architecture. As this is just a prototype we can care less about the abstraction for now and for on testing and application.

```
scripts/download_ioos_gliders.py    <-- Layer 1: CLI argument parsing
        |
        v
ioos_downloader.DownloaderIOOSGliders   <-- Layer 3: dataset-specific
        |    - delayed-mode filter
        |    - parquet response format
        v
ioos_downloader.DownloaderIOOS          <-- Layer 2: generic ERDDAP
        |    - erddapy connection
        |    - dataset enumeration
        |    - info-endpoint timestamp comparison
        |    - URL construction
        v
ioos_downloader.Downloader              <-- Layer 4: shared HTTP primitives
             - _download_file (streaming + tqdm)
             - _is_already_downloaded
             - download_parallel (ThreadPoolExecutor)
             - unzip_file
```

The base `Downloader` class has the same method signatures as the one in CrocoLakeTools (`crocolaketools/downloader/downloader.py`), so porting the IOOS classes back upstream only requires:
1. Replacing the `output_dir` constructor argument with the standard `config` dict + `config.yaml` lookup.
2. Updating imports.

No method bodies change.

## Why erddapy, not gliderpy

Confirmed by reading both source trees:

| | erddapy | gliderpy |
|---|---|---|
| Server URL | configurable | hardcoded to `https://gliders.ioos.us/erddap` (servers.py) |
| Dataset filter | configurable | `search_for="glider"` hardcoded in `GliderDataFetcher.query()` |
| Delayed-mode | available | filtered out by default (`delayed=False`) |
| Parquet response | works via `get_download_url(response="parquet")` (no validation) | only what erddapy exposes |

gliderpy is used as a **structural reference**, not a runtime dependency.

## Parquet from ERDDAP

erddapy's `download_formats` allowlist does not include `parquet` — so `e.to_download("parquet")` raises `ValueError`. But `e.get_download_url(response="parquet")` does **not** validate against that allowlist; it just builds the URL. The IOOS Glider DAC natively serves parquet, so this works.

For ERDDAP servers that don't support parquet, override `response_format` to `"ncCF"`.

## Install

```bash
git clone https://github.com/mahi-anol/GSoC26-Enhancing-CrocoLakeTools-with-IOOS-Data-Sync-from-ERDDAP.git
cd Experimental
# create custom venv environment.
python -m venv .venv
# on linux
source .venv/bin/activate 
pip install -e .
```

## Run

First of all tryout the Error_Generator.py, look inside the script, I commented out some important details.
After generating the error, go through next steps to see my solution.

Dry-run against the live IOOS Glider DAC (lists what would be downloaded):

```bash
python scripts/download_ioos_gliders.py --dryrun --verbose
```

Actually download:

```bash
python scripts/download_ioos_gliders.py --output-dir ./data/ioos_gliders
```

Sync again — only new or updated datasets are downloaded (timestamp-based incremental sync):

```bash
python scripts/download_ioos_gliders.py --output-dir ./data/ioos_gliders
```

## Test

Unit tests (no network):

```bash
pytest tests/
```

Live integration test (hits real IOOS Glider DAC):

```bash
pytest -m live tests/
```

## Files

| File | Purpose |
|---|---|
| `ioos_downloader/downloader.py` | Base class. Shared HTTP/parallel primitives. |
| `ioos_downloader/downloaderIOOS.py` | Generic ERDDAP downloader. Reusable for any ERDDAP server. |
| `ioos_downloader/downloaderIOOSGliders.py` | IOOS Glider DAC subclass. Delayed-mode filter + parquet. |
| `scripts/download_ioos_gliders.py` | CLI script. |
| `tests/test_downloaderIOOSGliders.py` | Unit + live integration tests. |
