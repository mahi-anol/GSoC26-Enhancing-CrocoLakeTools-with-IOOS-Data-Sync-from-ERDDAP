import requests
import time

server = "https://gliders.ioos.us/erddap"
ds_id = "amlr01-20181216T0641-delayed"  # For this ds id, we get and error.
# A workaround I found is, downloading temporary chunks. THen merging them to create the final perquet.
# For some files generates error.
# For some files it works. Usually when files are too big, direct download is causing error.
# Look at Experimental/amlr01-20181216T0641-delayed.parquet, it will generate.
"""
Error {
    code=413;
    message="Payload Too Large: Your query produced too much data.  Try to request less data. [memory]  The request needs more memory (44070 MB) than is ever safely available in this Java setup (7680 MB). (TableWriterAll.cumulativeTable)";
}

"""
url = f"{server}/tabledap/{ds_id}.parquet"

print(f"Downloading {ds_id}...")
start = time.time()

r = requests.get(url, stream=True, timeout=60)
total_bytes = 0
with open(f"{ds_id}.parquet", "wb") as f:
    for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
        total_bytes += len(chunk)

elapsed = time.time() - start
mb = total_bytes / (1024 * 1024)
speed = mb / elapsed

print(f"Size    : {mb:.2f} MB")
print(f"Time    : {elapsed:.1f} seconds")
print(f"Speed   : {speed:.2f} MB/s")
print(f"Estimated time for 851 datasets: {851 * elapsed / 60:.0f} minutes (single thread)")
print(f"Estimated time with 4 threads  : {851 * elapsed / 60 / 4:.0f} minutes")