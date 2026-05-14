"""ioos_downloader: ERDDAP-based dataset downloaders for IOOS data.

Prototype for the GSoC 2026 project
"Enhancing CrocoLakeTools with IOOS Data Sync from ERDDAP".

This prototype is intentionally decoupled from CrocoLakeTools so it can be
developed and tested in isolation. The class structure mirrors the
CrocoLakeTools Downloader / DownloaderIOOS / DownloaderIOOSGliders pattern
exactly, so porting back into the main package only requires changing the
constructor signature (output_dir -> config dict resolved via config.yaml)
and the imports.
"""

from .downloader import Downloader
from .downloaderIOOS import DownloaderIOOS
from .downloaderIOOSGliders import DownloaderIOOSGliders

__all__ = ["Downloader", "DownloaderIOOS", "DownloaderIOOSGliders"]
__version__ = "0.1.0"