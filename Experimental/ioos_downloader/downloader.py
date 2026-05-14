#!/usr/bin/env python3

## @file downloader.py
#
# Base class for IOOS ERDDAP downloaders.
#
# Stand-alone version of the CrocoLakeTools Downloader base class
# (crocolaketools/downloader/downloader.py), with the same public method
# signatures but no dependency on crocolakeloader or config.yaml.
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 12 May 2026

##########################################################################
import logging
import os
import shutil
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

##########################################################################

logger = logging.getLogger(__name__)


class Downloader:
    """Common facilities for HTTP-based dataset downloaders.

    Provides:
      _is_already_downloaded(local_path)
      _download_file(url, local_path)        (streaming, tqdm progress, retries)
      unzip_file(zip_path)
      download_parallel(url_path_pairs)      (ThreadPoolExecutor wrapper)
    """

    # ------------------------------------------------------------------ #
    # Constructor                                                        #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        output_dir,
        overwrite=False,
        dryrun=False,
        num_threads=4,
        read_timeout=600,
        connect_timeout=30,
        max_retries=2,
        log_file=None,
    ):
        """Constructor.

        Arguments
        ---------
        output_dir      : destination directory for downloaded files.
        overwrite       : if False (default) existing files are skipped.
        dryrun          : if True, log what would be downloaded but do nothing.
        num_threads     : default thread count for download_parallel().
        read_timeout    : per-chunk read timeout in seconds (default 600).
                          Larger than the requests default because ERDDAP can
                          take a long time to materialize parquet responses
                          before sending the first byte.
        connect_timeout : TCP connect timeout in seconds (default 30).
        max_retries     : number of retries on transient failures.
        log_file        : optional path to a log file. If provided, INFO+
                          messages are duplicated to it.
        """
        if output_dir is None:
            raise ValueError("output_dir is required.")

        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.num_threads = num_threads
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_retries = max_retries

        if log_file is not None:
            self._configure_logging(log_file)

    # ------------------------------------------------------------------ #
    # Logging                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _configure_logging(log_file):
        """Attach a FileHandler to the root logger if not already present."""
        root = logging.getLogger()
        if root.level == logging.NOTSET or root.level > logging.INFO:
            root.setLevel(logging.INFO)
        log_file = os.path.abspath(log_file)
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        # Don't add a duplicate handler if one already points at this file
        for h in root.handlers:
            if isinstance(h, logging.FileHandler) and h.baseFilename == log_file:
                return
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        root.addHandler(fh)
        logger.info("Logging to %s", log_file)

    # ------------------------------------------------------------------ #
    # Shared methods                                                     #
    # ------------------------------------------------------------------ #

    def _is_already_downloaded(self, local_path):
        """Return True if file exists on disk and overwrite is False."""
        return (not self.overwrite) and os.path.isfile(local_path)

    def _download_file(self, url, local_path):
        """Stream `url` to `local_path` with a tqdm progress bar and retries.

        Retries on transient errors (timeouts, 5xx, connection resets).
        Does NOT retry on 4xx (e.g. 413 Payload Too Large) -- those are
        the caller's responsibility to handle by adjusting the request.

        Raises
        ------
        requests.exceptions.RequestException
            Propagated from the last failed attempt.
        """
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                with requests.get(
                    url,
                    stream=True,
                    timeout=(self.connect_timeout, self.read_timeout),
                ) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    with open(local_path, "wb") as fh, tqdm(
                        desc=os.path.basename(local_path),
                        total=total_size,
                        unit="iB",
                        unit_scale=True,
                        unit_divisor=1024,
                        leave=False,
                    ) as bar:
                        for chunk in response.iter_content(chunk_size=8192):
                            size = fh.write(chunk)
                            bar.update(size)
                return  # success

            except requests.exceptions.HTTPError as exc:
                # Don't retry 4xx client errors (caller must change the request)
                status = exc.response.status_code if exc.response is not None else None
                if status is not None and 400 <= status < 500:
                    raise
                last_exc = exc
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                last_exc = exc

            # clean up partial file before retrying
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass

            if attempt < self.max_retries:
                backoff = 2 ** attempt
                logger.info(
                    "Retry %d/%d for %s after %ds: %s",
                    attempt + 1, self.max_retries, url, backoff, last_exc,
                )
                time.sleep(backoff)

        # All retries exhausted
        raise last_exc

    @staticmethod
    def unzip_file(zip_path):
        """Extract a zip archive next to itself and delete the zip."""
        extract_dir = os.path.dirname(zip_path)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        macosx_path = os.path.join(extract_dir, "__MACOSX")
        if os.path.exists(macosx_path) and os.path.isdir(macosx_path):
            shutil.rmtree(macosx_path)

        os.remove(zip_path)

    def download_parallel(self, url_path_pairs, num_threads=None):
        """Download a list of (url, local_path) pairs concurrently.

        Returns (completed, failed) tuple.
        """
        n_threads = num_threads or self.num_threads
        pairs = list(url_path_pairs)

        if self.dryrun:
            logger.info("[DRY RUN] would download %d file(s) with %d threads",
                        len(pairs), n_threads)
            for url, path in pairs:
                logger.info("[DRY RUN]   %s -> %s", url, path)
            return len(pairs), 0

        completed, failed = 0, 0
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            future_to_url = {
                executor.submit(self._safe_download, url, path): url
                for url, path in pairs
            }
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    if future.result():
                        completed += 1
                    else:
                        failed += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("Error downloading %s: %s", url, exc)

        logger.info("Download finished. Success: %d, Failed: %d", completed, failed)
        return completed, failed

    def _safe_download(self, url, local_path):
        """Wrapper around _download_file that catches errors and removes
        partial files. Returns True on success, False on failure.
        """
        if self._is_already_downloaded(local_path):
            return True
        try:
            self._download_file(url, local_path)
            return True
        except Exception as exc:
            logger.warning("Failed to download %s: %s", url, exc)
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            return False


##########################################################################
if __name__ == "__main__":
    Downloader(output_dir="./_downloads")