import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from ioos_downloader import Downloader, DownloaderIOOS, DownloaderIOOSGliders


@pytest.fixture
def out(tmp_path):
    return str(tmp_path / "dl")


def test_creates_output_dir(tmp_path):
    path = str(tmp_path / "new" / "dir")
    Downloader(output_dir=path)
    assert os.path.isdir(path)


def test_skip_existing(out):
    d = Downloader(output_dir=out, overwrite=False)
    f = os.path.join(out, "x.parquet")
    open(f, "w").close()
    assert d._is_already_downloaded(f) is True


def test_delayed_filter(out):
    d = DownloaderIOOSGliders(output_dir=out, delayed_only=True)
    assert d._should_include("abc-delayed") is True
    assert d._should_include("abc") is False


@patch("ioos_downloader.downloaderIOOS.requests.get")
def test_time_coverage_parsed(mock_get, out):
    csv = (
        "Row Type,Variable Name,Attribute Name,Data Type,Value\n"
        "attribute,NC_GLOBAL,time_coverage_start,String,2024-01-01T00:00:00Z\n"
        "attribute,NC_GLOBAL,time_coverage_end,String,2024-04-01T00:00:00Z\n"
    )
    mock_get.return_value = MagicMock(text=csv, raise_for_status=lambda: None)
    d = DownloaderIOOS(output_dir=out, server_url="https://example.com/erddap")
    assert d._get_time_coverage("ds", "start") == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert d._get_time_coverage("ds", "end") == datetime(2024, 4, 1, tzinfo=timezone.utc)


@patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._concat_shards")
@patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
@patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
@patch("ioos_downloader.downloaderIOOS.ERDDAP")
def test_halves_on_413(mock_erddap, mock_cov, mock_dl, mock_concat, out):
    mock_erddap.return_value.get_download_url.return_value = "https://x/y.parquet"
    mock_cov.side_effect = lambda d, w: (
        datetime(2024, 1, 1, tzinfo=timezone.utc) if w == "start"
        else datetime(2024, 7, 19, tzinfo=timezone.utc)  # 200 days
    )
    exc = requests.exceptions.HTTPError("413")
    exc.response = MagicMock(status_code=413)
    calls = {"n": 0}
    def fake_dl(url, path):
        calls["n"] += 1
        if calls["n"] == 1:
            raise exc
    mock_dl.side_effect = fake_dl

    d = DownloaderIOOS(output_dir=out, server_url="https://x.com/erddap", chunk_days=200)
    d._fetch("ds", os.path.join(out, "ds.parquet"))
    # 1 failed 200d attempt + 2 successful 100d sub-shards
    assert mock_dl.call_count == 3


@patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._download_file")
@patch("ioos_downloader.downloaderIOOS.DownloaderIOOS._get_time_coverage")
@patch("ioos_downloader.downloaderIOOS.ERDDAP")
def test_single_shot_when_no_coverage(mock_erddap, mock_cov, mock_dl, out):
    mock_erddap.return_value.get_download_url.return_value = "https://x/y.parquet"
    mock_cov.return_value = None
    d = DownloaderIOOS(output_dir=out, server_url="https://x.com/erddap")
    d._fetch("ds", os.path.join(out, "ds.parquet"))
    assert mock_dl.call_count == 1


@pytest.mark.live
def test_live_listing(tmp_path):
    d = DownloaderIOOSGliders(output_dir=str(tmp_path), dryrun=True)
    ids = d.list_dataset_ids()
    assert any(i.endswith("-delayed") for i in ids)
