"""Fetch S2 metadata + references + citations for the NLP-paper window.

Two stages, runnable independently:

  python -m src.fetch_graph metadata
      Bulk-resolve every ACL Anthology paper id to S2 corpus metadata
      via POST /paper/batch (500 ids per call). Writes
      `data/raw/papers/<corpus_id>.json` and an index file
      `data/papers_index.jsonl`.

  python -m src.fetch_graph cites [--no-cits] [--workers N] [--limit N]
      For every focal paper from the metadata stage, fetch references
      (and, by default, citations) using the per-paper /references and
      /citations endpoints with limit=1000 and minimal fields. Writes
      `data/raw/refs/<corpus_id>.jsonl` and `data/raw/cits/<corpus_id>.jsonl`.

The split matches the S2 tutorial's two best practices:
  * Use bulk/batch endpoints "when requesting large data quantities."
  * "Avoid including more fields than you need" - both stages request
    only the columns the analysis actually consumes.

Resumes via the presence of per-paper output files; safe to interrupt
and re-run.
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
PAPERS_INDEX = os.path.join(DATA_DIR, "papers_index.jsonl")
ACL_LIST = os.path.join(DATA_DIR, "acl_papers.jsonl")

SESSION = requests.Session()
_MIN_INTERVAL = (1.0 / CFG.rps_with_key) if S2_API_KEY else (1.0 / CFG.rps_without_key)
_LAST_CALL = [0.0]


def _headers():
    h = {"User-Agent": "nlp-citations-2025/1.0",
         "Accept": "application/json"}
    if S2_API_KEY:
        h["x-api-key"] = S2_API_KEY
    return h


def _politeness_sleep():
    now = time.monotonic()
    wait = _LAST_CALL[0] + _MIN_INTERVAL - now
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[0] = time.monotonic()


def _request(method, url, *, params=None, json_body=None):
    """HTTP with client-side rate-limit + exponential back-off on 429/5xx.
    Returns the decoded JSON body, or None on terminal failure / 404."""
    delay = 2.0
    for attempt in range(10):
        _politeness_sleep()
        try:
            r = SESSION.request(method, url, params=params, json=json_body,
                                headers=_headers(),
                                timeout=CFG.request_timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                # Honour Retry-After when present.
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else delay
                time.sleep(wait)
                delay = min(delay * 2, 60)
                continue
            # 4xx other than 404/429 - log once and bail.
            tqdm.write(f"{method} {url} -> {r.status_code}: {r.text[:160]}")
            return None
        except requests.RequestException as e:
            tqdm.write(f"{method} {url} network error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None


# --- Stage 1: bulk metadata via POST /paper/batch ----------------------

def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _safe_basename(s):
    """Filesystem-safe version of an ACL id (e.g. 2024.naacl-long.1)."""
    return s.replace("/", "_")


def _load_acl_papers(years=None):
    if not os.path.exists(ACL_LIST):
        sys.exit(f"Run `python -m src.fetch_acl` first; missing {ACL_LIST}")
    rows = []
    with open(ACL_LIST, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if years and row["year"] not in years:
                continue
            rows.append(row)
    return rows


def _index_existing_papers():
    """Return {acl_id: corpusId} for already-fetched paper metadata."""
    out = {}
    if not os.path.isdir(PAPERS_OUT):
        return out
    for fn in os.listdir(PAPERS_OUT):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(PAPERS_OUT, fn), encoding="utf-8") as f:
                d = json.load(f)
            acl = (d.get("externalIds") or {}).get("ACL") or d.get("_acl_id")
            if acl and d.get("corpusId"):
                out[acl] = d["corpusId"]
        except Exception:
            continue
    return out


def fetch_metadata(years=None):
    """Phase A: POST /paper/batch on every ACL id, ~90 calls for 44k papers."""
    os.makedirs(PAPERS_OUT, exist_ok=True)
    rows = _load_acl_papers(years=years)
    done = _index_existing_papers()
    todo = [r for r in rows if r["acl_id"] not in done]
    print(f"Metadata stage: {len(rows):,} ACL papers, "
          f"{len(done):,} already cached, {len(todo):,} to fetch "
          f"({len(todo) // CFG.batch_size + 1} batch calls of "
          f"<= {CFG.batch_size} ids).")

    url = f"{S2_GRAPH_BASE}/paper/batch"
    pbar = tqdm(total=len(todo), desc="paper/batch", unit="paper")
    n_ok = n_miss = 0
    for start in range(0, len(todo), CFG.batch_size):
        chunk = todo[start:start + CFG.batch_size]
        ids = [f"ACL:{r['acl_id']}" for r in chunk]
        body = _request("POST", url,
                        params={"fields": CFG.paper_fields},
                        json_body={"ids": ids})
        if not body:
            pbar.update(len(chunk))
            n_miss += len(chunk)
            continue
        for acl_row, paper in zip(chunk, body):
            if not paper or not paper.get("corpusId"):
                n_miss += 1
                continue
            paper["_acl_id"] = acl_row["acl_id"]
            paper["_acl_year"] = acl_row["year"]
            out = os.path.join(PAPERS_OUT, f"{_safe_basename(acl_row['acl_id'])}.json")
            _write_json(out, paper)
            n_ok += 1
        pbar.update(len(chunk))
    pbar.close()

    # Refresh the index file so the cites stage doesn't have to scan
    # 44k JSON files.
    index = _index_existing_papers()
    with open(PAPERS_INDEX, "w", encoding="utf-8") as f:
        for acl, cid in sorted(index.items(), key=lambda x: x[1]):
            f.write(json.dumps({"acl_id": acl, "corpusid": cid}) + "\n")
    print(f"metadata stage: ok={n_ok:,} miss={n_miss:,}; index -> {PAPERS_INDEX}")


# --- Stage 2: per-paper /references and /citations --------------------

def _write_jsonl(path, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


def _paged(url, fields, cap):
    """Walk offset/limit pagination at the maximum page size."""
    out, offset = [], 0
    while offset < cap:
        page = _request("GET", url, params={
            "fields": fields,
            "offset": offset,
            "limit": min(CFG.page_size, cap - offset),
        })
        if not page:
            break
        data = page.get("data", [])
        out.extend(data)
        nxt = page.get("next")
        if nxt is None or not data:
            break
        offset = nxt
    return out


def _flatten_refs(corpus_id, page):
    return [{
        "citingcorpusid": str(corpus_id),
        "citedcorpusid": (str(d["citedPaper"]["corpusId"])
                          if d.get("citedPaper") and d["citedPaper"].get("corpusId")
                          else None),
        "cited_year": (d["citedPaper"] or {}).get("year"),
        "cited_s2fos": (d["citedPaper"] or {}).get("s2FieldsOfStudy"),
        "cited_externalids": (d["citedPaper"] or {}).get("externalIds"),
        "contexts": d.get("contexts"),
        "intents": d.get("intents"),
    } for d in page]


def _flatten_cits(corpus_id, page):
    return [{
        "citingcorpusid": (str(d["citingPaper"]["corpusId"])
                           if d.get("citingPaper") and d["citingPaper"].get("corpusId")
                           else None),
        "citedcorpusid": str(corpus_id),
        "citing_year": (d["citingPaper"] or {}).get("year"),
        "citing_s2fos": (d["citingPaper"] or {}).get("s2FieldsOfStudy"),
        "citing_externalids": (d["citingPaper"] or {}).get("externalIds"),
        "contexts": d.get("contexts"),
        "intents": d.get("intents"),
    } for d in page]


def _process_one(corpus_id, *, fetch_cits_too):
    refs_path = os.path.join(REFS_OUT, f"{corpus_id}.jsonl")
    cits_path = os.path.join(CITS_OUT, f"{corpus_id}.jsonl")
    refs_done = os.path.exists(refs_path)
    cits_done = (not fetch_cits_too) or os.path.exists(cits_path)
    if refs_done and cits_done:
        return "skip"

    if not refs_done:
        url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/references"
        page = _paged(url, CFG.ref_fields, CFG.max_refs)
        _write_jsonl(refs_path, _flatten_refs(corpus_id, page))
    if fetch_cits_too and not cits_done:
        url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/citations"
        page = _paged(url, CFG.cit_fields, CFG.max_cits)
        _write_jsonl(cits_path, _flatten_cits(corpus_id, page))
    return "ok"


def fetch_cites(*, workers, fetch_cits_too, limit=None):
    if not os.path.exists(PAPERS_INDEX):
        sys.exit(f"Run `python -m src.fetch_graph metadata` first; "
                 f"missing {PAPERS_INDEX}")
    os.makedirs(REFS_OUT, exist_ok=True)
    os.makedirs(CITS_OUT, exist_ok=True)

    ids = []
    with open(PAPERS_INDEX, encoding="utf-8") as f:
        for line in f:
            ids.append(json.loads(line)["corpusid"])
    if limit:
        ids = ids[:limit]

    print(f"Cites stage: {len(ids):,} papers, workers={workers}, "
          f"with_cits={fetch_cits_too}, page_size={CFG.page_size}.")

    counts = {"ok": 0, "skip": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_process_one, cid,
                             fetch_cits_too=fetch_cits_too): cid for cid in ids}
        for fut in tqdm(as_completed(futs), total=len(futs),
                        desc="refs+cits"):
            try:
                counts[fut.result()] += 1
            except Exception as e:
                counts["err"] += 1
                tqdm.write(f"err: {e}")
    print(json.dumps(counts, indent=2))


# --- CLI ---------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_meta = sub.add_parser("metadata",
        help="Stage 1: bulk-resolve ACL ids to S2 metadata via POST /paper/batch.")
    p_meta.add_argument("--years", default=f"{YEAR_MIN}-{YEAR_MAX}",
                        help="Inclusive range, e.g. 2022-2025 or 2024.")

    p_cites = sub.add_parser("cites",
        help="Stage 2: per-paper /references and /citations.")
    p_cites.add_argument("--workers", type=int,
                         default=4 if S2_API_KEY else 1,
                         help="Concurrent HTTP workers (the S2 1 req/s limit "
                              "is global, but parallel workers help overlap "
                              "I/O during 429 back-offs).")
    p_cites.add_argument("--no-cits", action="store_true",
                         help="Skip incoming citations (faster).")
    p_cites.add_argument("--limit", type=int, default=None,
                         help="Stop after N papers (for sample / smoke runs).")

    p_all = sub.add_parser("all", help="Run metadata then cites end-to-end.")
    p_all.add_argument("--years", default=f"{YEAR_MIN}-{YEAR_MAX}")
    p_all.add_argument("--workers", type=int, default=4 if S2_API_KEY else 1)
    p_all.add_argument("--no-cits", action="store_true")

    args = ap.parse_args()

    def _years(s):
        if "-" in s:
            a, b = s.split("-", 1)
            return set(range(int(a), int(b) + 1))
        return {int(s)}

    if args.cmd == "metadata":
        fetch_metadata(years=_years(args.years))
    elif args.cmd == "cites":
        fetch_cites(workers=args.workers, fetch_cits_too=not args.no_cits,
                    limit=args.limit)
    else:  # all
        fetch_metadata(years=_years(args.years))
        fetch_cites(workers=args.workers, fetch_cits_too=not args.no_cits)


if __name__ == "__main__":
    sys.exit(main())
