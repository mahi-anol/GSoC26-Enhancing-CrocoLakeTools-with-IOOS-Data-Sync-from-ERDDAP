from .downloaderIOOS import DownloaderIOOS


# Glider Specific downloader.
class DownloaderIOOSGliders(DownloaderIOOS):
    SERVER_URL = "https://gliders.ioos.us/erddap"

    def __init__(self, output_dir, delayed_only=True, **kwargs):
        super().__init__(output_dir, **kwargs)
        self.delayed_only = delayed_only

    def _should_include(self, dsid):
        if self.delayed_only:
            return dsid.endswith("-delayed")
        return True
