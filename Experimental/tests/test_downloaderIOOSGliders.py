#!/usr/bin/env python3

"""Unit tests for DownloaderIOOS and DownloaderIOOSGliders.

All HTTP calls are mocked so the tests run offline. There is one
explicit live-server test marked @pytest.mark.live; run with
`pytest -m live` for integration testing.
"""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from ioos_downloader import (
    Downloader,
    DownloaderIOOS,
    DownloaderIOOSGliders,
)


# ====================================================================== #
# Fixtures                                                                #
# ====================================================================== #


@pytest.fixture
def tmp_output(tmp_path):
    return str(tmp_path / "downloads")


@pytest.fixture
def mock_info_csv_end_only():
    """CSV with only time_coverage_end (no start) -- triggers single-shot fallback."""
    return (
        "Row Type,Variable Name,Attribute Name,Data Type,Value\n"
        "attribute,NC_GLOBAL,time_coverage_end,String,2025-04-01T00:00:00Z\n"
        "attribute,NC_GLOBAL,title,String,Example dataset\n"
    )


@pytest.fixture
def mock_info_csv_full():
    """CSV with both time_coverage_start and time_coverage_end."""
    return (
        "Row Type,Variable Name,Attribute Name,Data Type,Value\n"
        "attribute,NC_GLOBAL,time_coverage_start,String,2024-01-01T00:00:00Z\n"
        "attribute,NC_GLOBAL,time_coverage_end,String,2024-04-01T00:00:00Z\n"
        "attribute,NC_GLOBAL,title,String,Example dataset\n"
    )


# ====================================================================== #
# Base Downloader                                                         #
# ====================================================================== #


class TestDownloaderBase:

    def test_requires_output_dir(self):
        with pytest.raises(ValueError):
            Downloader(output_dir=None)

    def test_creates_output_dir(self, tmp_path):
        out = str(tmp_path / "subdir" / "downloads")
        assert not os.path.exists(out)
        Downloader(output_dir=out)
        assert os.path.isdir(out)

    def test_is_already_downloaded_skips_existing(self, tmp_output):
        d = Downloader(output_dir=tmp_output, overwrite=False)
        local = os.path.join(tmp_output, "x.parquet")
        open(local, "w").close()
        assert d._is_already_downloaded(local) is True

    def test_is_already_downloaded_respects_overwrite(self, tmp_output):
        d = Downloader(output_dir=tmp_output, overwrite=True)
        local = os.path.join(tmp_output, "x.parquet")
        open(local, "w").close()
        assert d._is_already_downloaded(local) is False

    def test_is_already_downloaded_absent_file(self, tmp_output):
        d = Downloader(output_dir=tmp_output, overwrite=False)
        local = os.path.join(tmp_output, "does_not_exist.parquet")
        assert d._is_already_downloaded(local) is False

    def test_download_parallel_dryrun(self, tmp_output):
        d = Downloader(output_dir=tmp_output, dryrun=True)
        pairs = [("http://x/a", "/tmp/a"), ("http://x/b", "/tmp/b")]
        completed, failed = d.download_parallel(pairs)
        assert completed == 2
        assert failed == 0

    @patch("ioos_downloader.downloader.requests.get")
    def test_download_parallel_success(self, mock_get, tmp_output):
        resp = MagicMock()
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        resp.raise_for_status = MagicMock()
        resp.headers = {"content-length": "5"}
        resp.iter_content = MagicMock(return_value=[b"hello"])
        mock_get.return_value = resp

        d = Downloader(output_dir=tmp_output, num_threads=2, max_retries=0)
        pairs = [
            ("http://x/a", os.path.join(tmp_output, "a.parquet")),
            ("http://x/b", os.path.join(tmp_output, "b.parquet")),
        ]
        completed, failed = d.download_parallel(pairs)
        assert completed == 2
        assert failed == 0

    @patch("ioos_downloader.downloader.requests.get")
    def test_download_parallel_handles_failure_and_removes_partial(self, mock_get, tmp_output):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")

        d = Downloader(output_dir=tmp_output, num_threads=1, max_retries=0)
        pairs = [("http://x/a", os.path.join(tmp_output, "a.parquet"))]
        completed, failed = d.download_parallel(pairs)
        assert completed == 0
        assert failed == 1
        assert not os.path.exists(os.path.join(tmp_output, "a.parquet"))

    @patch("ioos_downloader.downloader.time.sleep", return_value=None)
    @patch("ioos_downloader.downloader.requests.get")
    def test_download_file_retries_on_timeout(self, mock_get, _mock_sleep, tmp_output):
        # First call raises Timeout, second succeeds
        good_resp = MagicMock()
        good_resp.__enter__ = MagicMock(return_value=good_resp)
        good_resp.__exit__ = MagicMock(return_value=False)
        good_resp.raise_for_status = MagicMock()
        good_resp.headers = {"content-length": "5"}
        good_resp.iter_content = MagicMock(return_value=[b"hello"])

        mock_get.side_effect = [
            requests.exceptions.Timeout("slow"),
            good_resp,
        ]

        d = Downloader(output_dir=tmp_output, max_retries=2)
        d._download_file("http://x/a", os.path.join(tmp_output, "a.parquet"))
        assert mock_get.call_count == 2

    @patch("ioos_downloader.downloader.time.sleep", return_value=None)
    @patch("ioos_downloader.downloader.requests.get")
    def test_download_file_does_not_retry_on_413(self, mock_get, _mock_sleep, tmp_output):
        # Build an HTTPError with a 413 status
        bad_resp = MagicMock()
        bad_resp.__enter__ = MagicMock(return_value=bad_resp)
        bad_resp.__exit__ = MagicMock(return_value=False)
        bad_resp.status_code = 413
        exc = requests.exceptions.HTTPError("413")
        exc.response = MagicMock(status_code=413)
        bad_resp.raise_for_status = MagicMock(side_effect=exc)
        mock_get.return_value = bad_resp

        d = Downloader(output_dir=tmp_output, max_retries=3)
        with pytest.raises(requests.exceptions.HTTPError):
            d._download_file("http://x/a", os.path.join(tmp_output, "a.parquet"))
        # No retries -- exactly one call
        assert mock_get.call_count == 1


# ====================================================================== #
# DownloaderIOOS                                                          #
# ====================================================================== #


class TestDownloaderIOOS:

    def test_requires_server_url(self, tmp_output):
        with pytest.raises(ValueError):
            DownloaderIOOS(output_dir=tmp_output)

    def test_accepts_explicit_server(self, tmp_output):
        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
        )
        assert d.server_url == "https://example.com/erddap"
        assert d.protocol == "tabledap"
        assert d.response_format == "ncCF"

    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_get_download_url_uses_erddapy(self, mock_erddap_cls, tmp_output):
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = (
            "https://example.com/erddap/tabledap/dataset_x.ncCF"
        )
        mock_erddap_cls.return_value = mock_e

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
        )
        url = d.get_download_url("dataset_x")
        assert url.endswith("dataset_x.ncCF")
        mock_e.get_download_url.assert_called_with(response="ncCF")

    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_get_download_url_passes_constraints(self, mock_erddap_cls, tmp_output):
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = "https://x/y.parquet?time>=2024"
        mock_erddap_cls.return_value = mock_e

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
            response_format="parquet",
        )
        d.get_download_url("dataset_x", constraints={"time>=": "2024-01-01"})
        assert mock_e.constraints == {"time>=": "2024-01-01"}

    @patch("ioos_downloader.downloaderIOOS.requests.get")
    def test_time_coverage_parsed_from_info(
        self, mock_get, tmp_output, mock_info_csv_full,
    ):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = mock_info_csv_full
        mock_get.return_value = mock_resp

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
        )
        t_start = d._get_time_coverage("dataset_x", "start")
        t_end = d._get_time_coverage("dataset_x", "end")
        assert t_start == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert t_end == datetime(2024, 4, 1, tzinfo=timezone.utc)

    @patch("ioos_downloader.downloaderIOOS.requests.get")
    def test_time_coverage_returns_none_on_http_error(self, mock_get, tmp_output):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")
        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
        )
        assert d._get_time_coverage("dataset_x", "end") is None


# ====================================================================== #
# Time chunking                                                           #
# ====================================================================== #


class TestTimeChunking:
    """Test the 413-halving and shard concatenation logic."""

    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._concatenate_parquet_shards")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_fetch_dataset_chunks_by_year(
        self, mock_erddap_cls, mock_cov, mock_dl, mock_concat, tmp_output,
    ):
        """A 600d coverage with chunk_days=200 should produce 3 shards."""
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = "https://x/y.parquet"
        mock_erddap_cls.return_value = mock_e

        # 600 days = exactly 3 windows of 200d
        mock_cov.side_effect = lambda dsid, which: (
            datetime(2022, 1, 1, tzinfo=timezone.utc) if which == "start"
            else datetime(2023, 8, 24, tzinfo=timezone.utc)  # 600 days later
        )

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
            response_format="parquet",
            chunk_days=200,
        )
        d.fetch_dataset("ds", os.path.join(tmp_output, "ds.parquet"))

        # 3 shards should have been downloaded
        assert mock_dl.call_count == 3
        called_with = mock_concat.call_args[0][0]
        assert len(called_with) == 3

    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._concatenate_parquet_shards")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_fetch_dataset_halves_on_413(
        self, mock_erddap_cls, mock_cov, mock_dl, mock_concat, tmp_output,
    ):
        """On 413, the window should be halved and the dataset retried."""
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = "https://x/y.parquet"
        mock_erddap_cls.return_value = mock_e

        # Use 200d coverage so the math is clean and doesn't depend on leap years:
        # chunk_days=200, coverage=200d -> 1 window, 413, halved to 100d -> 2 shards
        mock_cov.side_effect = lambda dsid, which: (
            datetime(2024, 1, 1, tzinfo=timezone.utc) if which == "start"
            else datetime(2024, 7, 19, tzinfo=timezone.utc)  # 200 days
        )

        # Track which call number we're on; only the first one (largest window) 413s.
        call_count = {"n": 0}
        exc_413 = requests.exceptions.HTTPError("413")
        exc_413.response = MagicMock(status_code=413)

        def fake_dl(url, path):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise exc_413
            return None
        mock_dl.side_effect = fake_dl

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
            response_format="parquet",
            chunk_days=200,
        )
        d.fetch_dataset("ds", os.path.join(tmp_output, "ds.parquet"))

        # 1 failing 200d attempt + 2 successful 100d attempts = 3 calls
        assert mock_dl.call_count == 3
        # Concat got 2 shards
        called_with = mock_concat.call_args[0][0]
        assert len(called_with) == 2

    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._concatenate_parquet_shards")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_halving_uses_actual_span_not_target(
        self, mock_erddap_cls, mock_cov, mock_dl, mock_concat, tmp_output,
    ):
        """Regression test for a bug where 413 on a short coverage range
        would 'halve' the abstract target (e.g. 365 -> 182) while the
        actual clipped window stayed at 81 days, causing an identical
        retry loop. The fix halves the actual span instead.
        """
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = "https://x/y.parquet"
        mock_erddap_cls.return_value = mock_e

        # Short coverage (81 days), but chunk_days=365 (much larger).
        # First attempt should be 81d, halved should be 40d.
        mock_cov.side_effect = lambda dsid, which: (
            datetime(2018, 12, 16, tzinfo=timezone.utc) if which == "start"
            else datetime(2019, 3, 7, tzinfo=timezone.utc)
        )

        attempts = []
        exc_413 = requests.exceptions.HTTPError("413")
        exc_413.response = MagicMock(status_code=413)

        def fake_dl(url, path):
            # Each call: record we got hit and 413 the first one, succeed the rest
            attempts.append(path)
            if len(attempts) == 1:
                raise exc_413
            return None
        mock_dl.side_effect = fake_dl

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
            response_format="parquet",
            chunk_days=365,    # much larger than the actual coverage
            min_chunk_days=1,
        )
        d.fetch_dataset("ds", os.path.join(tmp_output, "ds.parquet"))

        # 1 failing 81d attempt + 3 successful sub-shards (40d + 40d + 1d) = 4 calls
        # The key thing this test guards against is the OLD bug, where we would
        # have retried the SAME 81d window indefinitely instead of subdividing.
        assert mock_dl.call_count == 4
        # All sub-shards concatenated
        assert len(mock_concat.call_args[0][0]) == 3

    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
    @patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_fetch_dataset_falls_back_when_no_time_coverage(
        self, mock_erddap_cls, mock_cov, mock_dl, tmp_output,
    ):
        """If time_coverage_start is missing, do a single-shot download."""
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = "https://x/y.parquet"
        mock_erddap_cls.return_value = mock_e
        mock_cov.return_value = None

        d = DownloaderIOOS(
            output_dir=tmp_output,
            server_url="https://example.com/erddap",
            response_format="parquet",
        )
        d.fetch_dataset("ds", os.path.join(tmp_output, "ds.parquet"))
        # Exactly one (unconstrained) call
        assert mock_dl.call_count == 1

    def test_concatenate_parquet_shards_pyarrow(self, tmp_output):
        """End-to-end: write two small parquet shards and concatenate."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed")

        os.makedirs(tmp_output, exist_ok=True)
        shard1 = os.path.join(tmp_output, "s1.parquet")
        shard2 = os.path.join(tmp_output, "s2.parquet")
        out = os.path.join(tmp_output, "out.parquet")

        pq.write_table(
            pa.table({"time": [1, 2], "val": [10, 20]}), shard1,
        )
        pq.write_table(
            pa.table({"time": [3, 4], "val": [30, 40]}), shard2,
        )
        DownloaderIOOS._concatenate_parquet_shards([shard1, shard2], out)

        merged = pq.read_table(out).to_pandas()
        assert len(merged) == 4
        assert merged["time"].tolist() == [1, 2, 3, 4]
        assert merged["val"].tolist() == [10, 20, 30, 40]


# ====================================================================== #
# DownloaderIOOSGliders                                                   #
# ====================================================================== #


class TestDownloaderIOOSGliders:

    def test_defaults(self, tmp_output):
        d = DownloaderIOOSGliders(output_dir=tmp_output)
        assert d.server_url == "https://gliders.ioos.us/erddap"
        assert d.response_format == "parquet"
        assert d.delayed_only is True
        assert d.chunk_days == 30
        assert d.min_chunk_days == 1

    def test_delayed_only_filter(self, tmp_output):
        d = DownloaderIOOSGliders(output_dir=tmp_output, delayed_only=True)
        assert d._should_include("ru29-20220105T1430-delayed") is True
        assert d._should_include("ru29-20220105T1430") is False
        assert d._should_include("allDatasets") is False

    def test_all_mode(self, tmp_output):
        d = DownloaderIOOSGliders(output_dir=tmp_output, delayed_only=False)
        assert d._should_include("ru29-20220105T1430-delayed") is True
        assert d._should_include("ru29-20220105T1430") is True
        assert d._should_include("allDatasets") is False

    def test_local_filename_is_parquet(self, tmp_output):
        d = DownloaderIOOSGliders(output_dir=tmp_output)
        assert d._local_filename("ru29-delayed") == "ru29-delayed.parquet"

    @patch("ioos_downloader.downloaderIOOS.ERDDAP")
    def test_get_download_url_requests_parquet(self, mock_erddap_cls, tmp_output):
        mock_e = MagicMock()
        mock_e.get_download_url.return_value = (
            "https://gliders.ioos.us/erddap/tabledap/x.parquet"
        )
        mock_erddap_cls.return_value = mock_e

        d = DownloaderIOOSGliders(output_dir=tmp_output)
        url = d.get_download_url("x")
        mock_e.get_download_url.assert_called_with(response="parquet")
        assert "parquet" in url


# ====================================================================== #
# Live integration test                                                   #
# ====================================================================== #


@pytest.mark.live
def test_live_dataset_listing(tmp_path):
    """Hit the real IOOS Glider DAC and check we can list datasets.

    Run with: pytest -m live tests/
    """
    d = DownloaderIOOSGliders(output_dir=str(tmp_path), dryrun=True)
    ids = d.list_dataset_ids()
    assert len(ids) > 0
    delayed = [i for i in ids if i.endswith("-delayed")]
    assert len(delayed) > 0, "Expected at least one -delayed dataset"
    print(f"\nLive: {len(ids)} datasets, {len(delayed)} delayed-mode")