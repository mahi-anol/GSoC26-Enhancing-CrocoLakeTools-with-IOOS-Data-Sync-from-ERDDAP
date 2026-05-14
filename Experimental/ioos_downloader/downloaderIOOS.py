#!/usr/bin/env python3

## @file downloaderIOOS.py
#
# Generic ERDDAP downloader built on top of erddapy.
#
# Handles everything common to any ERDDAP server: connecting via erddapy,
# enumerating datasets, fetching the per-dataset info endpoint, and
# constructing download URLs.
#
# Dataset-specific subclasses (DownloaderIOOSGliders, future
# DownloaderIOOSAnimalTelemetry) override the server URL, response format,
# and dataset filter.
#
# Why time-chunked downloads:
# ---------------------------
# ERDDAP's /tabledap endpoint caps the size of a single response (typically
# ~100MB on the IOOS Glider DAC). Multi-year delayed-mode glider datasets
# exceed this and ERDDAP returns HTTP 413. The fix is to chunk the request
# by time: ask for one year at a time, retry with smaller windows on 413,
# and concatenate the parquet shards into one file at the end.
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 14 May 2026

##########################################################################
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Optional

import pandas as pd
import requests
from erddapy import ERDDAP

from .downloader import Downloader

##########################################################################

logger = logging.getLogger(__name__)


class DownloaderIOOS(Downloader):
    """Generic ERDDAP downloader with time-chunked, adaptive sizing.

    Sync flow per dataset:
      1. read time_coverage_start, time_coverage_end from info endpoint
      2. split [start, end] into windows of `chunk_days` (default 365)
      3. for each window:
           - request parquet with time>= / time<= constraints
           - on HTTP 413, halve the window and retry
           - if window < min_chunk_days, give up on this shard
      4. concatenate shards into {output_dir}/{dataset_id}.parquet
      5. remove the .shards subdirectory

    Skip logic uses the same time_coverage_end-vs-mtime comparison as before.
    """

    # Subclasses override:
    SERVER_URL = ""                    # e.g. "https://gliders.ioos.us/erddap"
    PROTOCOL = "tabledap"
    RESPONSE_FORMAT = "ncCF"
    FILE_EXTENSION = ".nc"

    # ------------------------------------------------------------------ #
    # Constructor                                                        #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        output_dir: str,
        server_url: Optional[str] = None,
        protocol: Optional[str] = None,
        response_format: Optional[str] = None,
        overwrite: bool = False,
        dryrun: bool = False,
        num_threads: int = 4,
        chunk_days: int = 30,
        min_chunk_days: int = 1,
        time_variable: str = "time",
        log_file: Optional[str] = None,
    ):
        """Constructor.

        Arguments
        ---------
        output_dir      : destination directory for downloaded files.
        server_url      : ERDDAP server URL. Defaults to class-level SERVER_URL.
        protocol        : 'tabledap' or 'griddap'.
        response_format : ERDDAP response format. 'parquet', 'ncCF', etc.
        overwrite       : if True, re-download files even if present.
        dryrun          : if True, only log what would be downloaded.
        num_threads     : parallel downloads (used across datasets only;
                          per-dataset shards download serially to keep
                          one dataset's progress understandable).
        chunk_days      : initial size of each time window (default 30).
                          Glider data is high-resolution (sub-second profiles),
                          so 30 days is the empirical sweet spot for IOOS DAC
                          parquet responses without hitting 413.
        min_chunk_days  : if a window has to be halved below this, give up
                          on the shard. Prevents infinite recursion.
        time_variable   : name of the time variable on the server. Almost
                          always 'time' for IOOS data.
        log_file        : optional path to a log file.
        """
        super().__init__(
            output_dir=output_dir,
            overwrite=overwrite,
            dryrun=dryrun,
            num_threads=num_threads,
            log_file=log_file,
        )
        self.server_url = server_url or self.SERVER_URL
        self.protocol = protocol or self.PROTOCOL
        self.response_format = response_format or self.RESPONSE_FORMAT

        if not self.server_url:
            raise ValueError("server_url is required (set class SERVER_URL or pass it in).")

        self.chunk_days = chunk_days
        self.min_chunk_days = min_chunk_days
        self.time_variable = time_variable

    # ------------------------------------------------------------------ #
    # Public interface                                                   #
    # ------------------------------------------------------------------ #

    def download(self) -> dict:
        """Run a full sync against the server."""
        logger.info("Querying ERDDAP server: %s", self.server_url)
        try:
            dataset_ids = self.list_dataset_ids()
        except Exception as exc:
            raise RuntimeError(
                f"Could not list datasets from {self.server_url}: {exc}"
            ) from exc

        logger.info("Server returned %d candidate datasets.", len(dataset_ids))
        dataset_ids = [d for d in dataset_ids if self._should_include(d)]
        logger.info("After filtering: %d datasets to sync.", len(dataset_ids))

        downloaded = []
        skipped = []
        failed = []

        for i, dataset_id in enumerate(dataset_ids, 1):
            local_path = os.path.join(
                self.output_dir, self._local_filename(dataset_id)
            )
            logger.info("[%d/%d] %s", i, len(dataset_ids), dataset_id)

            # Skip-if-current check
            if self._is_already_downloaded(local_path):
                server_end = self._get_time_coverage(dataset_id, "end")
                if server_end is not None:
                    local_mtime = datetime.fromtimestamp(
                        os.path.getmtime(local_path), tz=timezone.utc
                    )
                    if server_end <= local_mtime:
                        logger.info("  -> skipped (local is current)")
                        skipped.append((dataset_id, "local is current"))
                        continue
                    else:
                        logger.info(
                            "  -> server has newer data (%s > %s), re-downloading",
                            server_end, local_mtime,
                        )
                else:
                    logger.info("  -> skipped (file present, no server timestamp)")
                    skipped.append((dataset_id, "file present, no server timestamp"))
                    continue

            if self.dryrun:
                logger.info("  -> [DRY RUN] would download")
                continue

            # Actually fetch
            try:
                self.fetch_dataset(dataset_id, local_path)
                downloaded.append((dataset_id, local_path))
                logger.info("  -> downloaded")
            except Exception as exc:
                logger.warning("  -> failed: %s", exc)
                failed.append((dataset_id, str(exc)))

        logger.info(
            "Sync complete. downloaded=%d, skipped=%d, failed=%d",
            len(downloaded), len(skipped), len(failed),
        )
        return {
            "downloaded": downloaded,
            "skipped": skipped,
            "failed": failed,
        }

    def fetch_dataset(self, dataset_id: str, local_path: str) -> None:
        """Fetch a single dataset to local_path using time-chunked requests.

        For non-tabledap protocols or non-parquet responses, falls back to
        a single unconstrained request (the original behavior).
        """
        # Only do time-chunking for tabledap. griddap and file-endpoint
        # responses are single-shot.
        if self.protocol != "tabledap":
            url = self.get_download_url(dataset_id)
            self._download_file(url, local_path)
            return

        coverage_start = self._get_time_coverage(dataset_id, "start")
        coverage_end = self._get_time_coverage(dataset_id, "end")

        if coverage_start is None or coverage_end is None:
            # No time coverage metadata available -- fall back to single-shot
            logger.info(
                "  no time_coverage metadata for %s, trying single-shot download",
                dataset_id,
            )
            url = self.get_download_url(dataset_id)
            self._download_file(url, local_path)
            return

        shard_dir = os.path.join(self.output_dir, ".shards", dataset_id)
        os.makedirs(shard_dir, exist_ok=True)

        try:
            shards = self._download_time_chunks(
                dataset_id=dataset_id,
                t_start=coverage_start,
                t_end=coverage_end,
                shard_dir=shard_dir,
                window_days=self.chunk_days,
            )
            if not shards:
                raise RuntimeError(
                    f"No shards successfully downloaded for {dataset_id}"
                )
            self._concatenate_parquet_shards(shards, local_path)
        finally:
            # Clean up shard directory regardless of outcome
            if os.path.isdir(shard_dir):
                shutil.rmtree(shard_dir, ignore_errors=True)

    # ------------------------------------------------------------------ #
    # Time-chunked download                                              #
    # ------------------------------------------------------------------ #

    def _download_time_chunks(
        self, dataset_id, t_start, t_end, shard_dir, window_days,
    ):
        """Walk [t_start, t_end] in window_days chunks, downloading each
        chunk as a separate parquet shard. On 413, halve the window for
        just that shard. Returns list of shard paths in time order.
        """
        shards = []
        cursor = t_start
        chunk_idx = 0
        window = timedelta(days=window_days)

        while cursor < t_end:
            chunk_end = min(cursor + window, t_end)
            shard_paths = self._fetch_window_with_subdivision(
                dataset_id=dataset_id,
                window_start=cursor,
                window_end=chunk_end,
                window_days=window_days,
                shard_dir=shard_dir,
                chunk_idx_start=chunk_idx,
            )
            shards.extend(shard_paths)
            chunk_idx += len(shard_paths)
            cursor = chunk_end

        return shards

    def _fetch_window_with_subdivision(
        self, dataset_id, window_start, window_end,
        window_days, shard_dir, chunk_idx_start,
    ):
        """Try to download [window_start, window_end] as a single shard.
        On 413, halve the ACTUAL span (not the abstract target) and recurse.
        Returns list of shard paths.
        """
        url = self.get_download_url(
            dataset_id,
            constraints={
                f"{self.time_variable}>=": window_start.isoformat(),
                f"{self.time_variable}<=": window_end.isoformat(),
            },
        )
        shard_path = os.path.join(
            shard_dir, f"shard_{chunk_idx_start:04d}.parquet"
        )
        actual_span_days = max(1, (window_end - window_start).days)
        logger.info(
            "    shard %d: %s -> %s (%dd)",
            chunk_idx_start,
            window_start.date(), window_end.date(),
            actual_span_days,
        )
        try:
            self._download_file(url, shard_path)
            return [shard_path]
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status != 413:
                raise

        # 413 -- halve the ACTUAL window span.
        # We must use the realized window length, not the abstract target,
        # because the caller's target may be larger than the actual window
        # (e.g. when the dataset coverage is shorter than chunk_days).
        new_window_days = actual_span_days // 2
        if new_window_days < self.min_chunk_days:
            logger.warning(
                "    skipping window %s..%s for %s: still 413 at %d-day actual span",
                window_start.date(), window_end.date(),
                dataset_id, actual_span_days,
            )
            return []

        logger.info(
            "    got 413, retrying %s..%s with %d-day sub-chunks",
            window_start.date(), window_end.date(), new_window_days,
        )
        # Subdivide the window into smaller pieces
        sub_shards = []
        sub_cursor = window_start
        sub_idx = chunk_idx_start
        sub_window = timedelta(days=new_window_days)
        while sub_cursor < window_end:
            sub_end = min(sub_cursor + sub_window, window_end)
            new_shards = self._fetch_window_with_subdivision(
                dataset_id=dataset_id,
                window_start=sub_cursor,
                window_end=sub_end,
                window_days=new_window_days,
                shard_dir=shard_dir,
                chunk_idx_start=sub_idx,
            )
            sub_shards.extend(new_shards)
            sub_idx += len(new_shards) if new_shards else 1
            sub_cursor = sub_end
        return sub_shards

    @staticmethod
    def _concatenate_parquet_shards(shards, local_path):
        """Concatenate a list of parquet files into a single parquet file.

        Uses pyarrow for streaming concatenation when available.
        Falls back to pandas if not.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            # Fallback to pandas
            import pandas as pd
            logger.info("    concatenating %d shard(s) via pandas", len(shards))
            dfs = [pd.read_parquet(s) for s in shards]
            if not dfs:
                raise RuntimeError("no shards to concatenate")
            pd.concat(dfs, ignore_index=True).to_parquet(local_path)
            return

        logger.info("    concatenating %d shard(s) via pyarrow", len(shards))
        # Read schema from the first non-empty shard so we can stream-write
        first_schema = None
        for s in shards:
            try:
                first_schema = pq.read_schema(s)
                break
            except Exception:
                continue
        if first_schema is None:
            raise RuntimeError("could not read any shard's schema")

        with pq.ParquetWriter(local_path, first_schema) as writer:
            for s in shards:
                try:
                    table = pq.read_table(s)
                except Exception as exc:
                    logger.warning("    skipping unreadable shard %s: %s", s, exc)
                    continue
                # Cast to first schema in case of minor metadata drift
                if table.schema != first_schema:
                    try:
                        table = table.cast(first_schema)
                    except Exception:
                        pass
                writer.write_table(table)

    # ------------------------------------------------------------------ #
    # ERDDAP querying                                                    #
    # ------------------------------------------------------------------ #

    def list_dataset_ids(self) -> list:
        """Return all dataset IDs available on the server.

        Uses ERDDAP's `allDatasets` virtual dataset -- same approach as
        gliderpy's DatasetList.get_ids.
        """
        e = ERDDAP(server=self.server_url, protocol="tabledap")
        e.dataset_id = "allDatasets"
        df = e.to_pandas()
        ids = df["datasetID"].tolist()
        if "allDatasets" in ids:
            ids.remove("allDatasets")
        return ids

    def get_download_url(self, dataset_id: str, constraints: dict = None) -> str:
        """Construct the direct download URL for a dataset.

        Uses erddapy's get_download_url(). Note that erddapy's
        download_formats allowlist does NOT include 'parquet', but
        get_download_url() does not validate against it, so this works
        for parquet servers transparently.
        """
        e = ERDDAP(server=self.server_url, protocol=self.protocol)
        e.dataset_id = dataset_id
        if constraints:
            e.constraints = constraints
        return e.get_download_url(response=self.response_format)

    def _get_time_coverage(self, dataset_id: str, which: str) -> Optional[datetime]:
        """Fetch time_coverage_start or time_coverage_end from the info endpoint.

        Returns None if not available or unparseable.
        """
        assert which in ("start", "end")
        attr_name = f"time_coverage_{which}"
        info_url = f"{self.server_url}/info/{dataset_id}/index.csv"
        try:
            r = requests.get(info_url, timeout=(self.connect_timeout, 60))
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("info endpoint for %s failed: %s", dataset_id, exc)
            return None

        try:
            df = pd.read_csv(StringIO(r.text))
        except Exception as exc:
            logger.debug("could not parse info CSV for %s: %s", dataset_id, exc)
            return None

        mask = (
            (df.get("Attribute Name") == attr_name)
            & (df.get("Variable Name") == "NC_GLOBAL")
        )
        rows = df[mask]
        if rows.empty:
            return None
        value = str(rows.iloc[0]["Value"])
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            logger.debug("unparseable %s for %s: %r", attr_name, dataset_id, value)
            return None

    # Backwards-compat alias used by older code
    def _get_server_time_coverage_end(self, dataset_id: str):
        return self._get_time_coverage(dataset_id, "end")

    # ------------------------------------------------------------------ #
    # Overridable hooks                                                  #
    # ------------------------------------------------------------------ #

    def _should_include(self, dataset_id: str) -> bool:
        return dataset_id != "allDatasets"

    def _local_filename(self, dataset_id: str) -> str:
        return f"{dataset_id}{self.FILE_EXTENSION}"