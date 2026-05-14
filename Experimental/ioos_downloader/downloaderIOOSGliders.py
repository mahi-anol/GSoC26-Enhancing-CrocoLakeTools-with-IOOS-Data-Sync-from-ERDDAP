#!/usr/bin/env python3

## @file downloaderIOOSGliders.py
#
# Glider-specific ERDDAP downloader for the IOOS Glider DAC
# (https://gliders.ioos.us/erddap).
#
# Adds two things on top of DownloaderIOOS:
#   - server URL and parquet response format
#   - delayed-mode filter (dataset IDs ending in '-delayed')
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 14 May 2026

##########################################################################
from typing import Optional

from .downloaderIOOS import DownloaderIOOS
##########################################################################


class DownloaderIOOSGliders(DownloaderIOOS):
    """Downloader for delayed-mode glider datasets from the IOOS Glider DAC.

    The IOOS Glider DAC (https://gliders.ioos.us/erddap) hosts hundreds of
    individual glider deployment datasets. Delayed-mode datasets have IDs
    ending in '-delayed' and contain the quality-controlled science data
    that CrocoLake is interested in.

    Multi-year delayed-mode datasets often exceed ERDDAP's per-request
    size cap (HTTP 413). DownloaderIOOS handles that by chunking the
    request by time and concatenating shards into one parquet file.

    Typical usage
    -------------
    >>> d = DownloaderIOOSGliders(output_dir="./data/ioos_gliders")
    >>> result = d.download()
    >>> print(f"{len(result['downloaded'])} datasets downloaded")
    """

    SERVER_URL = "https://gliders.ioos.us/erddap"
    PROTOCOL = "tabledap"
    RESPONSE_FORMAT = "parquet"
    FILE_EXTENSION = ".parquet"

    # ------------------------------------------------------------------ #
    # Constructor                                                        #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        output_dir: str,
        delayed_only: bool = True,
        overwrite: bool = False,
        dryrun: bool = False,
        num_threads: int = 4,
        response_format: Optional[str] = None,
        chunk_days: int = 30,
        min_chunk_days: int = 1,
        log_file: Optional[str] = None,
    ):
        """Constructor.

        Arguments
        ---------
        output_dir      : destination directory for downloaded files.
        delayed_only    : if True (default), only sync '-delayed' datasets.
        overwrite       : if True, re-download files even if present.
        dryrun          : if True, only log what would be downloaded.
        num_threads     : parallel downloads across datasets.
        response_format : override response format (default: 'parquet').
        chunk_days      : initial time-window size for chunked downloads
                          (default 30). Halved on 413 down to min_chunk_days.
        min_chunk_days  : floor on the time-window halving (default 1).
        log_file        : optional log file path.
        """
        super().__init__(
            output_dir=output_dir,
            response_format=response_format,
            overwrite=overwrite,
            dryrun=dryrun,
            num_threads=num_threads,
            chunk_days=chunk_days,
            min_chunk_days=min_chunk_days,
            log_file=log_file,
        )
        self.delayed_only = delayed_only

    # ------------------------------------------------------------------ #
    # Overrides                                                          #
    # ------------------------------------------------------------------ #

    def _should_include(self, dataset_id: str) -> bool:
        """Include only delayed-mode datasets if delayed_only is True."""
        if not super()._should_include(dataset_id):
            return False
        if self.delayed_only:
            return dataset_id.endswith("-delayed")
        return True