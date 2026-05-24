import argparse
import logging

from ioos_downloader import DownloaderIOOSGliders

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

parser = argparse.ArgumentParser(description="Download IOOS glider data from ERDDAP.")
parser.add_argument("--output-dir", default="./data/ioos_gliders")
parser.add_argument("--all", action="store_true", help="Include non-delayed datasets too.")
parser.add_argument("--overwrite", action="store_true")
parser.add_argument("--dryrun", action="store_true")
parser.add_argument("--chunk-days", type=int, default=30)

if __name__ == "__main__":
    args = parser.parse_args()
    d = DownloaderIOOSGliders(
        output_dir=args.output_dir,
        delayed_only=not args.all,
        overwrite=args.overwrite,
        dryrun=args.dryrun,
        chunk_days=args.chunk_days,
    )
    result = d.download()
    print(f"Downloaded: {len(result['downloaded'])}, Skipped: {len(result['skipped'])}, Failed: {len(result['failed'])}")
