"""S2 bulk dataset downloader.

Mirrors `src/download.py` in the upstream repo. The only differences:
* Reads `S2_API_KEY` from the environment (upstream had a placeholder).
* Pins the release to `S2_BULK_RELEASE` (default `latest`) so re-runs
  are reproducible.
* Skips datasets we don't need (`abstracts`, `tldrs`, ...) to save disk.
"""

import json
import os
import sys
import time

import requests
from tqdm import tqdm

from .config import (
    CITATIONS_DIR, PAPERS_DIR, S2_API_KEY, S2_BULK_RELEASE, S2_DATASETS_BASE,
)


def _datasets_url(release, dataset):
    return f"{S2_DATASETS_BASE}/release/{release}/dataset/{dataset}"


def fetch_manifest(directory, dataset):
    """Fetch the dataset manifest JSON and store it in `directory/dataset.json`."""
    if not S2_API_KEY:
        raise RuntimeError(
            "S2_API_KEY is required for the bulk pipeline. Set the env var "
            "or use the Graph API path (`fetch_graph.py`) instead."
        )
    os.makedirs(directory, exist_ok=True)
    url = _datasets_url(S2_BULK_RELEASE, dataset)
    r = requests.get(url, headers={"x-api-key": S2_API_KEY}, timeout=60)
    r.raise_for_status()
    out = os.path.join(directory, f"{dataset}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(r.json(), f)
    return out


def download_file(url, filename, retries=5):
    if os.path.exists(filename):
        return
    delay = 2
    for attempt in range(retries):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(filename + ".part", "wb") as f, tqdm(
                    total=total, unit="iB", unit_scale=True, desc=os.path.basename(filename)
                ) as bar:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                        bar.update(len(chunk))
            os.rename(filename + ".part", filename)
            return
        except Exception as e:
            print(f"[{attempt+1}/{retries}] {filename}: {e}; retrying in {delay}s")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Failed to download {url}")


def download_dataset(directory, dataset):
    manifest = fetch_manifest(directory, dataset)
    with open(manifest, encoding="utf-8") as f:
        urls = json.load(f)["files"]
    for i, url in enumerate(urls, 1):
        out = os.path.join(directory, f"{dataset}_{i}.jsonl.gz")
        download_file(url, out)


def main():
    download_dataset(PAPERS_DIR, "papers")
    download_dataset(CITATIONS_DIR, "citations")


if __name__ == "__main__":
    sys.exit(main())
