"""Fetch S2 metadata + references + citations for every NLP paper.

Input: `data/acl_papers.jsonl` (from `fetch_acl.py`).
Output:
  data/raw/papers/<corpusId>.json     paper metadata (S2 fields-of-study, etc)
  data/raw/refs/<corpusId>.jsonl      its references (one ref per line)
  data/raw/cits/<corpusId>.jsonl      its citing papers (one cit per line)

Each line is a flat JSON record matching the column subset the
upstream Spark pipeline projects (`citingcorpusid`, `citedcorpusid`,
`contexts`, `intents`, plus the cited/citing paper's
`s2FieldsOfStudy` so the downstream analysis can run without a second
join).

Resumes via the presence of the per-paper output file - safe to
interrupt and re-run.
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from tqdm import tqdm

from .config import (
    DATA_DIR, GraphAPIConfig, S2_API_KEY, S2_GRAPH_BASE, YEAR_MAX, YEAR_MIN,
)


CFG = GraphAPIConfig()
RAW = os.path.join(DATA_DIR, "raw")
PAPERS_OUT = os.path.join(RAW, "papers")
REFS_OUT = os.path.join(RAW, "refs")
CITS_OUT = os.path.join(RAW, "cits")
SESSION = requests.Session()


def _headers():
    h = {"User-Agent": "nlp-citations-2025/1.0"}
    if S2_API_KEY:
        h["x-api-key"] = S2_API_KEY
    return h


_MIN_INTERVAL = (1.0 / CFG.rps_with_key) if S2_API_KEY else (1.0 / CFG.rps_without_key)
_LAST_CALL = [0.0]


def _politeness_sleep():
    now = time.monotonic()
    wait = _LAST_CALL[0] + _MIN_INTERVAL - now
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[0] = time.monotonic()


def _get(url, params=None):
    """GET with client-side rate-limit + exponential back-off on 429/5xx."""
    delay = 2.0
    for attempt in range(10):
        _politeness_sleep()
        try:
            r = SESSION.get(url, params=params, headers=_headers(),
                            timeout=CFG.request_timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
        except requests.RequestException:
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None


def _paged(url, fields, cap):
    out, offset = [], 0
    while offset < cap:
        page = _get(url, {
            "fields": fields,
            "offset": offset,
            "limit": min(CFG.page_size, cap - offset),
        })
        if not page:
            break
        data = page.get("data", [])
        if not data:
            break
        out.extend(data)
        if "next" not in page:
            break
        offset = page["next"]
    return out


def fetch_paper(acl_id):
    """Return the S2 paper dict for an ACL Anthology id, or None."""
    url = f"{S2_GRAPH_BASE}/paper/ACL:{quote(acl_id)}"
    return _get(url, {"fields": CFG.paper_fields})


def fetch_refs(corpus_id):
    url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/references"
    page = _paged(url, CFG.ref_fields, CFG.max_refs)
    return [
        {
            "citingcorpusid": str(corpus_id),
            "citedcorpusid": (str(d["citedPaper"]["corpusId"])
                              if d.get("citedPaper") and d["citedPaper"].get("corpusId")
                              else None),
            "cited_year": (d["citedPaper"] or {}).get("year"),
            "cited_s2fos": (d["citedPaper"] or {}).get("s2FieldsOfStudy"),
            "cited_externalids": (d["citedPaper"] or {}).get("externalIds"),
            "contexts": d.get("contexts"),
            "intents": d.get("intents"),
        }
        for d in page
    ]


def fetch_cits(corpus_id):
    url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/citations"
    page = _paged(url, CFG.cit_fields, CFG.max_cits)
    return [
        {
            "citingcorpusid": (str(d["citingPaper"]["corpusId"])
                               if d.get("citingPaper") and d["citingPaper"].get("corpusId")
                               else None),
            "citedcorpusid": str(corpus_id),
            "citing_year": (d["citingPaper"] or {}).get("year"),
            "citing_s2fos": (d["citingPaper"] or {}).get("s2FieldsOfStudy"),
            "citing_externalids": (d["citingPaper"] or {}).get("externalIds"),
            "contexts": d.get("contexts"),
            "intents": d.get("intents"),
        }
        for d in page
    ]


def _write_jsonl(path, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def process_one(acl_id, year, *, fetch_cits_too):
    """Fetch metadata, refs, and (optionally) citations for one ACL paper."""
    # Skip if already done.
    paper_path = os.path.join(PAPERS_OUT, f"{acl_id.replace('/', '_')}.json")
    if os.path.exists(paper_path):
        return "skip"

    paper = fetch_paper(acl_id)
    if not paper or not paper.get("corpusId"):
        return "miss"

    corpus_id = paper["corpusId"]
    paper["_acl_id"] = acl_id
    _write_json(paper_path, paper)

    refs = fetch_refs(corpus_id)
    _write_jsonl(os.path.join(REFS_OUT, f"{corpus_id}.jsonl"), refs)

    if fetch_cits_too:
        cits = fetch_cits(corpus_id)
        _write_jsonl(os.path.join(CITS_OUT, f"{corpus_id}.jsonl"), cits)

    return "ok"


def load_acl_papers(years=None):
    path = os.path.join(DATA_DIR, "acl_papers.jsonl")
    if not os.path.exists(path):
        sys.exit(f"Run `python -m src.fetch_acl` first; missing {path}")
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if years and row["year"] not in years:
                continue
            rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default=f"{YEAR_MIN}-{YEAR_MAX}",
                    help="Inclusive range, e.g. 2022-2025 or 2024")
    ap.add_argument("--workers", type=int,
                    default=8 if S2_API_KEY else 1,
                    help="Parallel S2 requests (use 1 without an API key).")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N papers (for sample / smoke runs).")
    ap.add_argument("--no-cits", action="store_true",
                    help="Skip incoming citations (faster).")
    args = ap.parse_args()

    if "-" in args.years:
        a, b = args.years.split("-", 1)
        years = set(range(int(a), int(b) + 1))
    else:
        years = {int(args.years)}

    for d in (PAPERS_OUT, REFS_OUT, CITS_OUT):
        os.makedirs(d, exist_ok=True)

    rows = load_acl_papers(years=years)
    if args.limit:
        rows = rows[: args.limit]
    print(f"Fetching {len(rows):,} ACL papers "
          f"(years={sorted(years)}, workers={args.workers}, "
          f"with_cits={not args.no_cits})")

    counts = {"ok": 0, "skip": 0, "miss": 0}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {
            pool.submit(process_one, r["acl_id"], r["year"],
                        fetch_cits_too=not args.no_cits): r
            for r in rows
        }
        for fut in tqdm(as_completed(futs), total=len(futs),
                        desc="paper fetch"):
            try:
                counts[fut.result()] += 1
            except Exception as e:
                counts.setdefault("err", 0)
                counts["err"] += 1
                tqdm.write(f"err: {e}")

    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    sys.exit(main())
