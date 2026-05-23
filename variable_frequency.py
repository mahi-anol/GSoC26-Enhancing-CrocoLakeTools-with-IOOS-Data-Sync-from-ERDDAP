import requests
import pandas as pd
import json
import time
from collections import Counter
from io import StringIO

# Finding available datasets.
url = "https://gliders.ioos.us/erddap/tabledap/allDatasets.csv"
response_body = requests.get(url, timeout=60)
df = pd.read_csv(StringIO(response_body.text), skiprows=[1])
dataset_ids = [d for d in df["datasetID"].tolist() if d != "allDatasets"]
dataset_ids = [d for d in dataset_ids if d.endswith("-delayed")]

total_quantity_of_delayed_dataset=len(dataset_ids)

print(f"There are total {total_quantity_of_delayed_dataset} delayed datasets")

# Counting how many times a variable appeared over all the datasets, through iteration overall the dataset.

counter = Counter()
failed = []

for i, ds_id in enumerate(dataset_ids, 1):
    info_url = f"https://gliders.ioos.us/erddap/info/{ds_id}/index.csv"
    try:
        response_body = requests.get(info_url, timeout=30)
        response_body.raise_for_status()
        info_df = pd.read_csv(StringIO(response_body.text))
        variables = info_df[info_df["Row Type"] == "variable"]["Variable Name"].tolist()
        # print("Available Variables", variables)
        counter.update(set(variables))
    except Exception as exc:
        failed.append(ds_id)
        continue

    if i % 5 == 0 or i == len(dataset_ids):
        print(f"Inspection completed on {i} datasets out of {total_quantity_of_delayed_dataset}")

# Bulding json body
sorted_counts = dict(counter.most_common())

output = {
    "scope": "delayed-mode only",
    "total_datasets_processed": len(dataset_ids) - len(failed),
    "total_datasets_failed": len(failed),
    "unique_variables": len(sorted_counts),
    "variable_counts": sorted_counts,
}

# Writing Json
with open("variable_frequency_delayed.json", "w") as f:
    json.dump(output, f, indent=2)
