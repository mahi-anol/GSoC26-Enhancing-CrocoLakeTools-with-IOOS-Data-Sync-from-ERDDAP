import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests
from erddapy import ERDDAP

from .downloader import Downloader

logger = logging.getLogger(__name__)


class DownloaderIOOS(Downloader):
    SERVER_URL = ""
    PROTOCOL = "tabledap"
    RESPONSE_FORMAT = "parquet"
    FILE_EXTENSION = ".parquet"

    def __init__(self, output_dir, server_url=None, chunk_days=30, min_chunk_days=1,
                 overwrite=False, dryrun=False):
        super().__init__(output_dir, overwrite=overwrite, dryrun=dryrun)
        self.server_url = server_url or self.SERVER_URL
        if not self.server_url:
            raise ValueError("server_url is required.")
        self.chunk_days = chunk_days
        self.min_chunk_days = min_chunk_days

    def download(self):
        ids = self.list_dataset_ids()
        ids = [d for d in ids if self._should_include(d)]
        logger.info("%d datasets to sync", len(ids))

        downloaded, skipped, failed = [], [], []
        for i, dsid in enumerate(ids, 1):
            path = os.path.join(self.output_dir, f"{dsid}{self.FILE_EXTENSION}")
            logger.info("[%d/%d] %s", i, len(ids), dsid)
            if self._is_already_downloaded(path):
                skipped.append(dsid)
                logger.info("skipped")
                continue
            if self.dryrun:
                logger.info("dry run")
                continue
            try:
                self._fetch(dsid, path)
                downloaded.append(dsid)
            except Exception as e:
                logger.warning("  failed: %s", e)
                failed.append((dsid, str(e)))

        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    def _fetch(self, dsid, local_path):
        t_start = self._get_time_coverage(dsid, "start")
        t_end = self._get_time_coverage(dsid, "end")

        if t_start is None or t_end is None:
            self._download_file(self._url(dsid), local_path)
            return

        shard_dir = os.path.join(self.output_dir, ".shards", dsid)
        os.makedirs(shard_dir, exist_ok=True)
        try:
            shards = self._download_chunks(dsid, t_start, t_end, shard_dir, self.chunk_days)
            if not shards:
                raise RuntimeError(f"no shards for {dsid}")
            self._concat_shards(shards, local_path)
        finally:
            shutil.rmtree(shard_dir, ignore_errors=True)

    def _download_chunks(self, dsid, t_start, t_end, shard_dir, window_days):
        shards, cursor, idx = [], t_start, 0
        while cursor < t_end:
            chunk_end = min(cursor + timedelta(days=window_days), t_end)
            new = self._try_window(dsid, cursor, chunk_end, window_days, shard_dir, idx)
            shards.extend(new)
            idx += len(new) or 1
            cursor = chunk_end
        return shards

    def _try_window(self, dsid, w_start, w_end, window_days, shard_dir, idx):
        url = self._url(dsid, {
            "time>=": w_start.isoformat(),
            "time<=": w_end.isoformat(),
        })
        path = os.path.join(shard_dir, f"shard_{idx:04d}.parquet")
        span = max(1, (w_end - w_start).days)
        try:
            self._download_file(url, path)
            return [path]
        except requests.exceptions.HTTPError as e:
            if e.response is None or e.response.status_code != 413:
                raise

        new_days = span // 2
        if new_days < self.min_chunk_days:
            logger.warning("skipping %s..%s: still 413 at %dd", w_start.date(), w_end.date(), span)
            return []

        sub, cursor, sub_idx = [], w_start, idx
        while cursor < w_end:
            end = min(cursor + timedelta(days=new_days), w_end)
            got = self._try_window(dsid, cursor, end, new_days, shard_dir, sub_idx)
            sub.extend(got)
            sub_idx += len(got) or 1
            cursor = end
        return sub

    @staticmethod
    def _concat_shards(shards, out_path):
        try:
            import pyarrow.parquet as pq
            first = pq.read_schema(shards[0])
            with pq.ParquetWriter(out_path, first) as w:
                for s in shards:
                    w.write_table(pq.read_table(s))
        except ImportError:
            pd.concat([pd.read_parquet(s) for s in shards]).to_parquet(out_path)

    def _url(self, dsid, constraints=None):
        e = ERDDAP(server=self.server_url, protocol=self.PROTOCOL)
        e.dataset_id = dsid
        if constraints:
            e.constraints = constraints
        return e.get_download_url(response=self.RESPONSE_FORMAT)

    def list_dataset_ids(self):
        e = ERDDAP(server=self.server_url, protocol="tabledap")
        e.dataset_id = "allDatasets"
        ids = e.to_pandas()["datasetID"].tolist()
        return [i for i in ids if i != "allDatasets"]

    def _get_time_coverage(self, dsid, which):
        url = f"{self.server_url}/info/{dsid}/index.csv"
        try:
            r = requests.get(url, timeout=(30, 60))
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
        except Exception:
            return None
        attr = f"time_coverage_{which}"
        rows = df[(df.get("Attribute Name") == attr) & (df.get("Variable Name") == "NC_GLOBAL")]
        if rows.empty:
            return None
        try:
            return datetime.fromisoformat(str(rows.iloc[0]["Value"]).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _should_include(self, dsid):
        return True
