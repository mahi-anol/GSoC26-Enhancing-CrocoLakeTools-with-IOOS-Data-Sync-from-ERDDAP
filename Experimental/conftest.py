"""
Workaround for an erddapy bug where a non-JSON response from awesome-erddap
raises ValueError instead of httpx.HTTPError, crashing the import.
This patch makes the request fail cleanly so erddapy falls back to its local list.
On a normal machine with unrestricted internet this is a no-op.
"""

import sys


def _patch_erddapy_import():
    try:
        import httpx
    except ImportError:
        return

    original_get = httpx.get

    def _fake_get(url, *args, **kwargs):
        if "awesome-erddap" in str(url):
            raise httpx.HTTPError("blocked by sandbox; using local fallback")
        return original_get(url, *args, **kwargs)

    httpx.get = _fake_get


_patch_erddapy_import()
