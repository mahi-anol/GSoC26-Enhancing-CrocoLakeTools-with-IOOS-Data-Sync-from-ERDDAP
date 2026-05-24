# ioos_downloader

Prototype for GSoC 2026 — downloading glider data from the [IOOS Glider DAC](https://gliders.ioos.us/erddap) as parquet files.

Decoupled from CrocoLakeTools for now so it can be tested standalone.

## Install

```bash
cd Experimental
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

## Usage

Dry run (see what would be downloaded):

```bash
python scripts/download_ioos_gliders.py --dryrun
```

Download delayed-mode glider datasets:

```bash
python scripts/download_ioos_gliders.py --output-dir ./data/ioos_gliders
```

Download everything (not just delayed-mode):

```bash
python scripts/download_ioos_gliders.py --output-dir ./data --all
```

## How it works

Multi-year glider datasets exceed this and return HTTP 413. The downloader splits the time range into 30-day chunks, downloads each as a separate parquet shard, then concatenates them into one file. On 413 it halves the window and retries automatically.

## Tests

```bash
pytest tests/                # unit tests, no network needed
pytest -m live tests/        # hits the real IOOS Glider DAC
```

## Files

| File | What it does |
|---|---|
| `ioos_downloader/downloader.py` | Base class — basic HTTP download with retries |
| `ioos_downloader/downloaderIOOS.py` | ERDDAP logic — list datasets, time-chunked download, shard concat |
| `ioos_downloader/downloaderIOOSGliders.py` | Glider-specific — sets server URL, filters to `-delayed` datasets |
| `scripts/download_ioos_gliders.py` | CLI entry point |
| `tests/test_downloaderIOOSGliders.py` | Tests |
