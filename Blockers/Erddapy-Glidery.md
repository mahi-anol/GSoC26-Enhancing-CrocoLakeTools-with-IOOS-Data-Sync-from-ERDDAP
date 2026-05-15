# Why erddapy, Not gliderpy - and What to Fix Upstream

**Project:** GSoC 2026 - Enhancing CrocoLakeTools with IOOS Data Sync from ERDDAP
**Date:** 14 May 2026
**Author:** Mahi Sarwar Anol

This document consists every problem I found in `gliderpy` and `erddapy` while building the prototype, with exact file/line references so they can be verified independently. It also contains some proposed upstream contributions I think will fix the issues, to `erddapy` (where the scope is reasonable).

---

## 1. gliderpy: three blockers for this project's scope

The gsoc project goal (proposal section 3.2) is to sync **delayed-mode** data from **any IOOS ERDDAP** server, not just gliders. `gliderpy` works against this goal in three places.

### 1.1 Server is hardcoded - non-glider servers crash at construction

**File:** `gliderpy/fetchers.py`
**Lines:** 29, 99

```python
# line 29
_server = "https://gliders.ioos.us/erddap"
```

```python
# lines 89–101 (GliderDataFetcher.__init__)
def __init__(
    self: "GliderDataFetcher",
    server: OptionalStr = _server,
) -> None:
    self.server = server
    self.fetcher = ERDDAP(
        server=server,
        protocol="tabledap",
    )
    self.fetcher.variables = server_vars[server]   # line 99
```

**Why this blocks the project:** line 99 reads `server_vars[server]`. Look at where that dict comes from:

**File:** `gliderpy/servers.py`
**Lines:** 3–11

```python
server_vars = {
    "https://gliders.ioos.us/erddap": [
        "latitude",
        "longitude",
        "pressure",
        "profile_id",
        "salinity",
        "temperature",
        "time",
    ],
}
```

`server_vars` only has **one entry**. Pass any other server URL to `GliderDataFetcher(server=...)` and it raises `KeyError` on construction. This means gliderpy cannot be used against the Animal Telemetry Network ERDDAP (`https://atn.ioos.us/erddap/`), Saildrones, or any other IOOS server - which the proposal explicitly targets as stretch goals.

### 1.2 Search query is hardcoded to `"glider"`

**File:** `gliderpy/fetchers.py`
**Lines:** 162–172 (inside `GliderDataFetcher.query()`)

```python
if self.datasets is None:
    url = self.fetcher.get_search_url(
        search_for="glider",      # line 164  — HARDCODED
        response="csv",
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
        min_time=min_time,
        max_time=max_time,
    )
```

`search_for="glider"` is fixed in the user-facing query path. Animal telemetry, moorings, drifters, surface vehicles - all of these need different search terms, and the main fetcher cannot be made to issue them without forking the package.

(`DatasetList` at line 216 does accept a `search_for` argument, but `DatasetList` only returns IDs - actual data fetching still goes through `GliderDataFetcher` which is glider-locked.)

### 1.3 Delayed-mode datasets are filtered out by default - the exact dataset class this project targets

**File:** `gliderpy/fetchers.py`

**First location - `GliderDataFetcher.query()`:**

```python
# line 133 (default value)
delayed: OptionalBool = False,
```

```python
# lines 186–189 (filter applied)
if not delayed:
    datasets = datasets.loc[
        ~datasets["Dataset ID"].str.endswith("delayed")
    ]
```

**Second location - `DatasetList.get_ids()`:**

```python
# line 217 (default value)
delayed: OptionalBool = False,
```

```python
# lines 250–255 (filter applied)
if not self.delayed:
    self.dataset_ids = [
        dataset_id
        for dataset_id in dataset_ids
        if not dataset_id.endswith("-delayed")
    ]
return self.dataset_ids
```

**Why this blocks the project:** the proposal's deliverable (proposal section 3.1, 4) is specifically to sync **delayed-mode** data - the quality-controlled science data CrocoLake actually consumes. gliderpy's default of `delayed=False` actively removes those datasets from results. Every call site in our downloader would need to pass `delayed=True` explicitly.

**bug I noticed in 1.3:** `DatasetList.get_ids()` line 256 returns `self.dataset_ids`, but `self.dataset_ids` is only assigned inside the `if not self.delayed:` branch. If someone constructs `DatasetList(delayed=True)` and calls `get_ids()`, the method raises `AttributeError` (no `self.dataset_ids`). Worth a separate gliderpy PR if I have time, but not blocking - we're not using gliderpy.

### 1.4 The verdict

gliderpy is built around a single use case: real-time glider data from one specific server. The three hardcoded assumptions - server URL, search term, and delayed-mode filtering - are exactly the assumptions this project needs to invert. Forking or patching around them would mean rewriting most of `GliderDataFetcher`, at which point we're not really using gliderpy any more.

`erddapy` exposes the same underlying ERDDAP machinery (URL building, info endpoint, dataset listing) without any of these constraints. That's why the prototype uses erddapy directly and treats gliderpy as a **structural reference** for the public API shape, nothing more.

---

## 2. erddapy: two real bugs found while building the prototype

Both of these are candidates for upstream PRs. Both have small, contained fixes.

### 2.1 Bug A - `parquet` is not in `download_formats`, but `get_download_url()` happily emits parquet URLs anyway

This is an inconsistency between two public methods, not a hard crash. But it forces users to know about an internal allowlist.

**File:** `erddapy/core/url.py`
**Lines:** 559-605

```python
# line 559
download_formats = [
    "asc",
    "csv",
    "csvp",
    ...
    "ncCF",
    ...
    "transparentPng",
]   # line 605 — NO 'parquet', NO 'parquetWMeta'
```

**Then `ERDDAP.download_file()` validates against this list:**

**File:** `erddapy/erddapy.py`
**Lines:** 560-568

```python
def download_file(
    self: ERDDAP,
    file_type: str,
) -> str:
    """Download the dataset to a file in a user specified format."""
    file_type = file_type.lstrip(".")
    if file_type not in download_formats:                                # line 566
        msg = f"Requested filetype {file_type} not available on ERDDAP"
        raise ValueError(msg)
    url = _sort_url(self.get_download_url(response=file_type))
```

So `e.download_file("parquet")` raises `ValueError`.

**But `ERDDAP.get_download_url()` does no such validation:**

**File:** `erddapy/erddapy.py`
**Lines:** 315–397

The only "validation" applied to `response` is `_clean_response()` at line 367, which is:

**File:** `erddapy/core/url.py`
**Lines:** 60–66

```python
def _clean_response(response: str) -> str:
    """Allow for `ext` or `.ext` format."""
    return response.lstrip(".")
```

Just strips a leading dot. The response value flows straight into the URL string at `url.py` line 529:

```python
url = f"{server}/{protocol}/{dataset_id}.{response}?"
```

**Net effect:**

- `e.download_file("parquet")` → `ValueError` (file_type not in allowlist)
- `e.get_download_url(response="parquet")` -> returns the URL just fine, server happily serves the parquet

Since ERDDAP servers have supported parquet since v2.22 (2023), and at least the IOOS Glider DAC serves it natively, this is a real inconvenience for downstream users who want a uniform interface.

#### Proposed upstream fix

Add two entries to the `download_formats` list at `erddapy/core/url.py:559`:

```python
download_formats = [
    "asc",
    ...
    "ncoJson",
    "odvTxt",
    "parquet",         # <-- add
    "parquetWMeta",    # <-- add
    "subset",
    ...
]
```

**Scope:** 2 lines plus a test. Tests live in `tests/test_url_builder.py` in the erddapy repo — should be straightforward to add a parametrized test that `download_file("parquet")` no longer raises.

**Risk:** very low. Older ERDDAP servers that don't support parquet will simply return an HTTP error when the URL is fetched - same as they'd return for any other unsupported format, and consistent with the current behavior for `"ncCF"` on servers that don't support netCDF-CF.

---

### 2.2 Bug B - `servers_list()` crashes erddapy's import in restricted-network environments

This one is more annoying because it breaks `import erddapy` entirely, not just one method.

**File:** `erddapy/servers/servers.py`
**Lines:** 19-43

```python
@functools.lru_cache(maxsize=128)
def servers_list() -> dict:
    """Download a new server list from awesome-erddap.

    First we try to load the latest list from GitHub.
    If that fails we fall back to the default one shipped with the package.
    """
    try:
        url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
        r = httpx.get(url, timeout=10)
        df_servers = pd.read_json(io.StringIO(r.text))    # line 30
    except httpx.HTTPError:                                # line 31 — CAUGHT
        path = Path(__file__).absolute().parent
        df_servers = pd.read_json(path.joinpath("erddaps.json"))
    df_servers = df_servers[df_servers["public"]]
    return {
        row["short_name"].lower(): Server(row["name"], row["url"])
        for k, row in df_servers.iterrows()
        if row["short_name"]
    }


servers = servers_list()   # line 43 — runs at module import time
```

**The bug:** the `except` clause at line 31 only catches `httpx.HTTPError`. But the call chain that can throw is:

1. `httpx.get(url)` - raises `httpx.HTTPError` (caught)
2. `r.text` - can't fail
3. `pd.read_json(io.StringIO(r.text))` - raises **`ValueError`** (or `pandas.errors.EmptyDataError`) if the response body isn't valid JSON

The second failure mode is the one that bites: when an outbound HTTP request to `raw.githubusercontent.com` is allowed but the response is a 403 HTML page, or a CDN error page, or any non-JSON content, `httpx.get()` returns a `Response` object with `status_code=403` and a non-JSON body. **No `httpx.HTTPError` is raised** (it would only be raised if you called `r.raise_for_status()`, which this code doesn't). Then `pd.read_json` raises `ValueError`, which is not caught.

Since line 43 calls `servers_list()` at module import time, the whole `erddapy` package fails to import. The local fallback `erddaps.json` that's literally sitting in the same directory is never used.

**I hit this immediately** when running our prototype in an environment with restricted egress (the GET returned a "Host not in allowlist" body with HTTP 200, not an HTTP error). The package wouldn't import until I worked around it locally.

#### Proposed upstream fix

Broaden the `except` clause:

```python
try:
    url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
    r = httpx.get(url, timeout=10)
    r.raise_for_status()                                  # <-- add
    df_servers = pd.read_json(io.StringIO(r.text))
except (httpx.HTTPError, ValueError):                     # <-- broaden
    path = Path(__file__).absolute().parent
    df_servers = pd.read_json(path.joinpath("erddaps.json"))
```

Two changes:

1. Call `r.raise_for_status()` so non-2xx responses become `httpx.HTTPError` (idiomatic).
2. Broaden the except to also catch `ValueError`, which covers the "got 200 but body wasn't JSON" case as well as `pandas.errors.EmptyDataError` (a subclass of `ValueError`).

**Scope:** 2-line change plus a test. Test would mock `httpx.get` to return a response with `status_code=200, text="<html>...</html>"` and assert the fallback file is loaded.

**Risk:** very low. The local fallback file already exists and is already supposed to be used; this just makes the existing fallback actually reachable in the situations where it was meant to fire.

---

## 3. Bonus diagnostic notes (not upstream PRs, but worth recording)

These are not erddapy bugs but rather behaviors that surprised me and are worth documenting so the project's design is grounded in observed reality, not assumptions.

### 3.1 `get_download_url` produces a malformed `?&` when `variables=None`

**File:** `erddapy/core/url.py`
**Lines:** 529–532

```python
url = f"{server}/{protocol}/{dataset_id}.{response}?"  # line 529 — '?' always appended
if variables:                                          # line 530 — only joined if truthy
    url += ",".join(variables)

if constraints:
    ...
    url += _constraints_url                            # line 554 — starts with '&'
```

When `variables` is `None` or empty, the URL becomes:

```
https://gliders.ioos.us/erddap/tabledap/amlr01-...-delayed.parquet?&time>=1544918400.0&time<=1547510400.0
```

The `?&` is malformed (empty variable list, then constraints starting with `&`). ERDDAP tolerates this and interprets it as "all variables," which is **the root cause of the HTTP 413 errors I was hitting**: the default request asks for every variable in the dataset (30-80 of them for a typical IOOS glider deployment) instead of the handful we actually want.

This isn't a true bug - ERDDAP accepts the URL - but it's a footgun. A cleaner emission would either:
- Skip the `?` when there are no variables and no constraints
- Or always require an explicit variable list

I handled it on our side by querying each dataset's info endpoint, intersecting with a curated CF variable list, and passing the result to `e.variables`. But it would be friendlier upstream behavior to either warn or omit the dangling `?`. Probably not worth a PR on its own.

### 3.2 Constraint format is opaque without reading the source

The `e.constraints = {"time>=": "2018-12-16T00:00:00+00:00"}` syntax is non-obvious - the operator (`>=`) is part of the **key**, not a separate field, and it's joined to the value with no separator (`&time>=1544918400.0`). The docstring at `erddapy.py:340-347` does show an example, but the convention isn't documented in a way that's easy to search for. Minor - flagging only because it slowed me down for ~10 minutes.

---

## 4. Summary table

| # | Package | Severity | What | Where | Fix |
|---|---------|----------|------|-------|-----|
| 1.1 | gliderpy | Blocker | Hardcoded server URL → KeyError on any other server | `fetchers.py:99`, `servers.py:3-11` | Not patching (we don't depend on gliderpy) |
| 1.2 | gliderpy | Blocker | Hardcoded `search_for="glider"` | `fetchers.py:164` | Not patching |
| 1.3 | gliderpy | Blocker | `delayed=False` default filters out what we want | `fetchers.py:133, 186-189, 217, 250-255` | Not patching |
| 1.3b | gliderpy | Latent bug | `DatasetList.get_ids()` returns unbound attr when `delayed=True` | `fetchers.py:256` | Could open a separate gliderpy PR; not blocking |
| 2.1 | erddapy | Inconsistency | `parquet` missing from `download_formats` | `url.py:559-605` | **Upstream PR: add 2 entries** |
| 2.2 | erddapy | Real bug | `servers_list()` crashes import when GitHub returns non-JSON | `servers/servers.py:30-31` | **Upstream PR: broaden except clause** |
| 3.1 | erddapy | Footgun | Malformed `?&` query when no variables | `url.py:529-532` | Handled in our wrapper; no PR planned |

---

## 5. What I plan to do during community bonding

1. **Open erddapy PR #1: add `parquet` and `parquetWMeta` to `download_formats`** (Bug A, section 2.1). Small, contained, includes a test.
2. **Open erddapy PR #2: fix the import-time crash in `servers_list()`** (Bug B, section 2.2). Small, contained, includes a test using a mocked httpx response.
3. **Maybe open a gliderpy issue** documenting 1.3b if I have spare time (low priority).

---

## Appendix - file paths and line numbers cited

| File | Lines | What's there |
|---|---|---|
| `gliderpy/fetchers.py` | 29 | `_server = "https://gliders.ioos.us/erddap"` |
| `gliderpy/fetchers.py` | 99 | `self.fetcher.variables = server_vars[server]` → KeyError for non-IOOS servers |
| `gliderpy/fetchers.py` | 133 | `delayed: OptionalBool = False` (default for `query()`) |
| `gliderpy/fetchers.py` | 164 | `search_for="glider"` (hardcoded) |
| `gliderpy/fetchers.py` | 186-189 | Filter applied: `~datasets["Dataset ID"].str.endswith("delayed")` |
| `gliderpy/fetchers.py` | 217 | `delayed: OptionalBool = False` (default for `DatasetList`) |
| `gliderpy/fetchers.py` | 250-255 | `DatasetList` filter applied |
| `gliderpy/fetchers.py` | 256 | Latent AttributeError bug |
| `gliderpy/servers.py` | 3-11 | `server_vars` with only one server |
| `erddapy/core/url.py` | 60-66 | `_clean_response` — does NOT validate format |
| `erddapy/core/url.py` | 441 | Module-level `get_download_url` definition |
| `erddapy/core/url.py` | 529-532 | URL build with `?&` malformation when `variables` empty |
| `erddapy/core/url.py` | 559-605 | `download_formats` list (no parquet) |
| `erddapy/erddapy.py` | 315-397 | `ERDDAP.get_download_url` method — no format validation |
| `erddapy/erddapy.py` | 367 | Only "validation": `_clean_response(response)` |
| `erddapy/erddapy.py` | 560-568 | `ERDDAP.download_file` — does validate at line 566 |
| `erddapy/servers/servers.py` | 19-43 | `servers_list()` with incomplete `except` |
| `erddapy/servers/servers.py` | 31 | `except httpx.HTTPError:` — misses ValueError from pd.read_json |
| `erddapy/servers/servers.py` | 43 | `servers = servers_list()` — runs at import time |
