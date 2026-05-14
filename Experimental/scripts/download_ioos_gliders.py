#!/usr/bin/env python3

## @file download_ioos_gliders.py
#
# CLI for downloading delayed-mode glider data from the IOOS Glider DAC.
#
# Mirrors the structure of scripts/download_spraygliders.py in CrocoLakeTools:
#   - download_ioos_gliders() builds the downloader and calls download()
#   - main() parses CLI args and delegates
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 14 May 2026

##########################################################################
import argparse
import logging
import sys
from datetime import datetime

from ioos_downloader import DownloaderIOOSGliders

##########################################################################


def download_ioos_gliders(
    output_dir: str,
    delayed_only: bool = True,
    overwrite: bool = False,
    dryrun: bool = False,
    num_threads: int = 4,
    response_format: str = None,
    chunk_days: int = 365,
    min_chunk_days: int = 7,
    log_file: str = None,
) -> dict:
    """Download delayed-mode glider data from the IOOS Glider DAC."""
    downloader = DownloaderIOOSGliders(
        output_dir=output_dir,
        delayed_only=delayed_only,
        overwrite=overwrite,
        dryrun=dryrun,
        num_threads=num_threads,
        response_format=response_format,
        chunk_days=chunk_days,
        min_chunk_days=min_chunk_days,
        log_file=log_file,
    )
    return downloader.download()


# ---------------------------------------------------------------------- #


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Sync delayed-mode glider data from the IOOS Glider DAC "
            "(https://gliders.ioos.us/erddap) to a local directory. "
            "Multi-year datasets are downloaded as time-chunked parquet "
            "shards and concatenated into a single file per dataset."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="./data/ioos_gliders",
        help="Destination directory (default: ./data/ioos_gliders)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Sync all glider datasets, not just delayed-mode.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Re-download even if files already exist on disk.",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        default=False,
        help="Show what would be downloaded without actually downloading.",
    )
    parser.add_argument(
        "--num-threads",
        type=int,
        default=4,
        help="Parallel downloads across datasets (default: 4).",
    )
    parser.add_argument(
        "--format",
        default=None,
        help=(
            "ERDDAP response format. Default is 'parquet' "
            "(supported by IOOS Glider DAC). Use 'ncCF' for NetCDF."
        ),
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=30,
        help=(
            "Initial size of each time-window chunk in days (default: 30). "
            "Halved automatically on HTTP 413. Glider data is dense, so "
            "30 days is a reasonable starting point; reduce if you see "
            "many halving rounds in the log."
        ),
    )
    parser.add_argument(
        "--min-chunk-days",
        type=int,
        default=1,
        help="Smallest window the downloader will fall back to (default: 1).",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Path to a log file for the run (e.g. ioos_gliders.log).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose logging.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    result = download_ioos_gliders(
        output_dir=args.output_dir,
        delayed_only=not args.all,
        overwrite=args.overwrite,
        dryrun=args.dryrun,
        num_threads=args.num_threads,
        response_format=args.format,
        chunk_days=args.chunk_days,
        min_chunk_days=args.min_chunk_days,
        log_file=args.log_file,
    )

    print()
    print("=" * 60)
    print("Sync summary")
    print("=" * 60)
    print(f"  Downloaded:     {len(result['downloaded'])}")
    print(f"  Skipped:        {len(result['skipped'])}")
    print(f"  Failed:         {len(result['failed'])}")
    if result["failed"]:
        print("  Failed datasets:")
        for dsid, reason in result["failed"][:10]:
            print(f"    - {dsid}: {reason}")
        if len(result["failed"]) > 10:
            print(f"    ... and {len(result['failed']) - 10} more")
    print()


##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    try:
        main()
        print("download_ioos_gliders.py completed successfully")
    except KeyboardInterrupt:
        print("Interrupted by user.")
        sys.exit(130)
    print(datetime.now())