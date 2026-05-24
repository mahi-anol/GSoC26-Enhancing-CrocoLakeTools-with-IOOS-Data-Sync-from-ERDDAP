import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

# Downloader Base class
class Downloader:
    def __init__(self, output_dir, overwrite=False, dryrun=False):
        if not output_dir:
            raise ValueError("output_dir is required.")
        self.output_dir = os.path.abspath(output_dir)
        self.overwrite = overwrite
        self.dryrun = dryrun
        os.makedirs(self.output_dir, exist_ok=True)

    def _is_already_downloaded(self, path):
        return (not self.overwrite) and os.path.isfile(path)

    def _download_file(self, url, local_path, retries=2):
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        for attempt in range(retries + 1):
            try:
                r = requests.get(url, stream=True, timeout=(30, 600))
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                return
            except requests.exceptions.HTTPError as e:
                if e.response is not None and 400 <= e.response.status_code < 500:
                    raise
                last = e
            except requests.exceptions.RequestException as e:
                last = e
            if attempt < retries:
                time.sleep(2 ** attempt)
        raise last
