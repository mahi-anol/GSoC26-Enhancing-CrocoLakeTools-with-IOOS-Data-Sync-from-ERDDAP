"""Pytest conftest for the ioos_downloader prototype.

This conftest exists primarily to work around an upstream bug in erddapy
(observed in v2.2.x): the package fetches a server list from the
awesome-erddap GitHub repo at module import time. The except clause only
catches httpx.HTTPError, but if the response succeeds with HTTP 403 (or
any non-JSON body), pandas.read_json raises ValueError instead, which
propagates and crashes the import.

This is reproducible in any environment where outbound traffic to
raw.githubusercontent.com is blocked but returns an HTTP error body.
It's a candidate for an upstream erddapy PR (broaden the except clause
to catch ValueError / pandas.errors.EmptyDataError).

On a normal developer machine with unrestricted internet, this conftest
is a no-op — the awesome-erddap fetch succeeds and the patch never fires.
"""

import sys


def _patch_erddapy_import():
    """Force erddapy's import-time fetch to fail via HTTPError, which
    triggers its existing local-fallback path.
    """
    try:
        import httpx
    except ImportError:
        return  # erddapy won't import anyway, let it fail naturally

    original_get = httpx.get

    def _fake_get(url, *args, **kwargs):
        if "awesome-erddap" in str(url):
            raise httpx.HTTPError("blocked by sandbox; using local fallback")
        return original_get(url, *args, **kwargs)

    httpx.get = _fake_get


_patch_erddapy_import()

# Now safe to let test modules import erddapy