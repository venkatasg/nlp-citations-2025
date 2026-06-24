"""Fetch S2 metadata + references + citations for an experiment's
NLP-paper window.

Two stages, runnable independently:

  python -m src.fetch_graph metadata -e EXPERIMENT
      Bulk-resolve every ACL Anthology paper id (from the
      experiment's `acl_papers.jsonl`) to S2 corpus metadata via
      POST /paper/batch (500 ids per call). Writes
      `data/<experiment>/raw/papers/<acl_id>.json` and an index
      file `data/<experiment>/papers_index.jsonl`.

  python -m src.fetch_graph cites -e EXPERIMENT
                                  [--no-cits] [--workers N] [--limit N]
      For every focal paper from the metadata stage, fetch
      references (and, by default, citations) using the per-paper
      /references and /citations endpoints with limit=1000 and
      minimal fields. Writes
      `data/<experiment>/raw/refs/<corpus_id>.jsonl` and
      `data/<experiment>/raw/cits/<corpus_id>.jsonl`.

The split matches the S2 tutorial's two best practices:
  * Use bulk/batch endpoints "when requesting large data quantities."
  * "Avoid including more fields than you need" - both stages
    request only the columns the analysis actually consumes.

Resumes via the presence of per-paper output files; safe to
interrupt and re-run.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Optional

import requests
from tqdm import tqdm

from .config import (
    EXPERIMENTS,
    S2_API_KEY,
    S2_GRAPH_BASE,
    GraphAPIConfig,
    experiment_dirs,
)

CFG = GraphAPIConfig()
SESSION = requests.Session()
_MIN_INTERVAL = (1.0 / CFG.rps_with_key) if S2_API_KEY else (1.0 / CFG.rps_without_key)

# Module-global rate-limit state. The lock is held across the sleep
# so that concurrent workers serialise and the inter-request gap is
# guaranteed to be >= _MIN_INTERVAL across ALL endpoints (paper/batch,
# /references, /citations) and ALL threads. Without the lock,
# multiple workers can read _LAST_CALL simultaneously, both compute
# wait=0, and fire near-simultaneous requests, breaking the 1 RPS
# guarantee that S2 grants per API key.
_RATE_LOCK = threading.Lock()
_LAST_CALL = 0.0


def _headers():
    h = {"User-Agent": "nlp-citations-2025/1.0", "Accept": "application/json"}
    if S2_API_KEY:
        h["x-api-key"] = S2_API_KEY
    return h


def _politeness_sleep():
    """Block until at least _MIN_INTERVAL has elapsed since the last
    fired request. Thread-safe via _RATE_LOCK."""
    global _LAST_CALL
    with _RATE_LOCK:
        now = time.monotonic()
        wait = _LAST_CALL + _MIN_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL = time.monotonic()


def _request(method, url, *, params=None, json_body=None):
    """HTTP with client-side rate-limit + exponential back-off on 429/5xx.
    Returns the decoded JSON body, or None on terminal failure / 404."""
    delay = 2.0
    for attempt in range(10):
        _politeness_sleep()
        try:
            r = SESSION.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=_headers(),
                timeout=CFG.request_timeout,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else delay
                time.sleep(wait)
                delay = min(delay * 2, 60)
                continue
            tqdm.write(f"{method} {url} -> {r.status_code}: {r.text[:160]}")
            return None
        except requests.RequestException as e:
            tqdm.write(f"{method} {url} network error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None


# --- Stage 1: bulk metadata via POST /paper/batch ----------------------
#
# Lookup ladder. S2's ACL/DOI/URL aliases are populated unevenly,
# especially for recent (2024-2025) papers indexed only as arxiv
# preprints. We cascade through three batch lookups and (extension
# only) a title-match fallback.
#
#   Pass 1: ACL:<anthology_id>     - works for older / well-indexed papers
#   Pass 2: DOI:<10.18653/...>     - recovers papers whose ACL externalId
#                                    is unset on the S2 record
#   Pass 3: URL:<aclanthology url> - recovers the residual aclanthology
#                                    URL aliases
#   Pass 4: GET /paper/search/match
#                                  - extension only; recovers papers that
#                                    S2 indexed only as arxiv preprints
#                                    (no ACL/DOI/URL alias). Accept iff
#                                    matchScore >= TITLE_MATCH_MIN_SCORE
#                                    AND |s2_year - bib_year| <= 1.
#
# Anything still unresolved is logged with a reason code in
# data/<experiment>/papers_missing.jsonl.

# Empirically calibrated on a probe of arxiv-only ACL papers: the
# lowest matchScore for a true match was ~198 (long, distinctive title);
# generic single-noun-phrase queries score 49-115; "Attention Is All
# You Need" (off-window) scores 132. 150 keeps headroom under the
# observed true-match floor while excluding the ambiguous mid-band.
TITLE_MATCH_MIN_SCORE = 150.0
TITLE_MATCH_YEAR_TOLERANCE = 1

_LATEX_RE = re.compile(r"[{}\\]")


def _safe_basename(s):
    return s.replace("/", "_")


def _clean_title(t):
    """Strip latex-style braces / backslashes that ACL bib uses for
    capitalisation (e.g. `{LLM}s` -> `LLMs`) before sending to S2."""
    return _LATEX_RE.sub("", t or "").strip()


def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _write_jsonl(path, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


def _load_acl_papers(acl_list_path):
    if not os.path.exists(acl_list_path):
        sys.exit(f"Missing {acl_list_path}; run `python -m src.fetch_acl` first.")
    rows = []
    with open(acl_list_path, encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def _index_existing_papers(papers_dir):
    """Map ACL id -> corpusId for every cached metadata JSON.

    Falls back to the stamped `_acl_id` because S2 does not always set
    `externalIds.ACL` (e.g. for papers it indexed only as arxiv
    preprints, or papers resolved via DOI/URL/title-match)."""
    out = {}
    if not os.path.isdir(papers_dir):
        return out
    for fn in os.listdir(papers_dir):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(papers_dir, fn), encoding="utf-8") as f:
                d = json.load(f)
            acl = (d.get("externalIds") or {}).get("ACL") or d.get("_acl_id")
            if acl and d.get("corpusId"):
                out[acl] = d["corpusId"]
        except Exception:
            continue
    return out


@dataclass(frozen=True)
class LookupStrategy:
    """One pass of the bulk-metadata cascade. `id_fn` returns the
    prefixed S2 identifier for a row, or None if this strategy doesn't
    apply (e.g. DOI lookup for a row with no DOI in the bib)."""

    label: str
    id_fn: Callable[[dict], Optional[str]]


LOOKUP_STRATEGIES = [
    LookupStrategy("ACL", lambda r: f"ACL:{r['acl_id']}"),
    LookupStrategy("DOI", lambda r: f"DOI:{r['doi']}" if r.get("doi") else None),
    LookupStrategy("URL", lambda r: f"URL:{r['url']}" if r.get("url") else None),
]


def _persist_paper(paper, row, papers_dir, *, resolved_via, extra=None, index_fp=None):
    """Persist a resolved paper and (if `index_fp` is given) stream the
    acl_id -> corpusId mapping into the open index file so partial
    progress is durable across an interrupted run. The full index is
    rebuilt from disk at the end of `fetch_metadata` for dedupe + sort,
    so duplicate appends here are harmless."""
    paper["_acl_id"] = row["acl_id"]
    paper["_acl_year"] = row["year"]
    paper["_resolved_via"] = resolved_via
    if extra:
        paper.update(extra)
    out = os.path.join(papers_dir, f"{_safe_basename(row['acl_id'])}.json")
    _write_json(out, paper)
    if index_fp is not None:
        index_fp.write(
            json.dumps({"acl_id": row["acl_id"], "corpusid": paper["corpusId"]}) + "\n"
        )
        index_fp.flush()


def _run_batch_pass(experiment_name, strat, rows, papers_dir, *, index_fp=None):
    """POST /paper/batch using `strat.id_fn`; persist resolved papers;
    return rows still unresolved after this pass."""
    eligible = [(r, strat.id_fn(r)) for r in rows]
    with_id = [(r, i) for r, i in eligible if i]
    if not with_id:
        return rows, 0

    n_skipped = sum(1 for _, i in eligible if not i)
    url = f"{S2_GRAPH_BASE}/paper/batch"
    resolved = set()
    pbar = tqdm(
        total=len(with_id),
        desc=f"{experiment_name}/batch[{strat.label}]",
        unit="paper",
    )
    for start in range(0, len(with_id), CFG.batch_size):
        chunk = with_id[start : start + CFG.batch_size]
        ids = [i for _, i in chunk]
        body = _request(
            "POST", url, params={"fields": CFG.paper_fields}, json_body={"ids": ids}
        )
        if body:
            for (row, _), paper in zip(chunk, body):
                if paper and paper.get("corpusId"):
                    _persist_paper(
                        paper,
                        row,
                        papers_dir,
                        resolved_via=strat.label,
                        index_fp=index_fp,
                    )
                    resolved.add(row["acl_id"])
        pbar.update(len(chunk))
    pbar.close()

    return [r for r in rows if r["acl_id"] not in resolved], n_skipped


def _run_title_match_pass(experiment_name, rows, papers_dir, *, index_fp=None):
    """GET /paper/search/match per residual paper; accept top match iff
    matchScore >= TITLE_MATCH_MIN_SCORE and |s2_year - bib_year| <=
    TITLE_MATCH_YEAR_TOLERANCE. Returns (still_unresolved, rejections)
    where rejections is a list of (row, reason, details)."""
    url = f"{S2_GRAPH_BASE}/paper/search/match"
    resolved = set()
    rejections = []
    pbar = tqdm(total=len(rows), desc=f"{experiment_name}/title-match", unit="paper")
    for row in rows:
        title = _clean_title(row.get("title"))
        if not title:
            rejections.append((row, "no_title", None))
            pbar.update(1)
            continue
        body = _request("GET", url, params={"query": title, "fields": CFG.paper_fields})
        if not body or not body.get("data"):
            rejections.append((row, "title_404", None))
            pbar.update(1)
            continue
        d = body["data"][0]
        score = d.get("matchScore") or 0.0
        s2_year = d.get("year")
        if score < TITLE_MATCH_MIN_SCORE:
            rejections.append(
                (
                    row,
                    "low_match_score",
                    {"matchScore": score, "s2_title": d.get("title")},
                )
            )
            pbar.update(1)
            continue
        if s2_year is None or abs(s2_year - row["year"]) > TITLE_MATCH_YEAR_TOLERANCE:
            rejections.append(
                (
                    row,
                    "year_mismatch",
                    {
                        "matchScore": score,
                        "s2_year": s2_year,
                        "s2_title": d.get("title"),
                    },
                )
            )
            pbar.update(1)
            continue
        # The /search/match payload nests the result alongside scoring
        # metadata; strip matchScore back out of the persisted record
        # (it is not a paper field) and stamp our provenance.
        d.pop("matchScore", None)
        _persist_paper(
            d,
            row,
            papers_dir,
            resolved_via="title-match",
            extra={"_match_score": score},
            index_fp=index_fp,
        )
        resolved.add(row["acl_id"])
        pbar.update(1)
    pbar.close()

    return [r for r in rows if r["acl_id"] not in resolved], rejections


def _write_missing_log(unresolved, title_rejections, data_root):
    """Write data/<experiment>/papers_missing.jsonl with one row per
    unresolved paper. Rows that went through the title-match pass have
    a specific reason; rows that were never eligible for title-match
    (replication experiment) get reason=`no_match_via_id_lookup`."""
    rejection_index = {
        row["acl_id"]: (reason, details) for row, reason, details in title_rejections
    }
    path = os.path.join(data_root, "papers_missing.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for row in unresolved:
            reason, details = rejection_index.get(
                row["acl_id"], ("no_match_via_id_lookup", None)
            )
            f.write(
                json.dumps(
                    {
                        "acl_id": row["acl_id"],
                        "year": row["year"],
                        "title": row.get("title"),
                        "doi": row.get("doi"),
                        "reason": reason,
                        "details": details,
                    }
                )
                + "\n"
            )
    return path


def fetch_metadata(experiment_name):
    dirs = experiment_dirs(experiment_name)
    os.makedirs(dirs["papers_dir"], exist_ok=True)
    rows = _load_acl_papers(dirs["acl_list"])
    done = _index_existing_papers(dirs["papers_dir"])
    todo = [r for r in rows if r["acl_id"] not in done]

    print(
        f"[{experiment_name}] metadata: {len(rows):,} papers, "
        f"{len(done):,} cached, {len(todo):,} to resolve."
    )

    # Seed papers_index.jsonl from the already-cached set so the file
    # is non-empty + readable as soon as the first pass starts. Each
    # _persist_paper call inside the passes appends a new line as the
    # paper resolves, so an interrupted run leaves visible progress.
    keep = {r["acl_id"] for r in rows}
    seed_index = {acl: cid for acl, cid in done.items() if acl in keep}
    with open(dirs["papers_index"], "w", encoding="utf-8") as f:
        for acl, cid in sorted(seed_index.items(), key=lambda x: x[1]):
            f.write(json.dumps({"acl_id": acl, "corpusid": cid}) + "\n")

    remaining = todo
    pass_summary = []  # (label, n_in, n_resolved, n_skipped_no_id)
    title_rejections = []
    with open(dirs["papers_index"], "a", encoding="utf-8") as index_fp:
        for strat in LOOKUP_STRATEGIES:
            if not remaining:
                break
            n_in = len(remaining)
            remaining, n_skipped = _run_batch_pass(
                experiment_name,
                strat,
                remaining,
                dirs["papers_dir"],
                index_fp=index_fp,
            )
            pass_summary.append(
                (strat.label, n_in, n_in - len(remaining), n_skipped)
            )

        # Title-match fallback only for the extension experiment: the
        # 2023-2025 window has many papers indexed on S2 only via arxiv.
        # The replication window (1990-2022) is well covered by the
        # three id-lookup strategies and doesn't warrant per-paper
        # search calls.
        if experiment_name == "extension" and remaining:
            n_in = len(remaining)
            remaining, title_rejections = _run_title_match_pass(
                experiment_name,
                remaining,
                dirs["papers_dir"],
                index_fp=index_fp,
            )
            pass_summary.append(("title-match", n_in, n_in - len(remaining), 0))

    missing_path = _write_missing_log(remaining, title_rejections, dirs["data_root"])

    # Final rebuild: dedupe (the streaming appends can produce
    # duplicate lines if a paper is re-resolved across reruns) and
    # restrict to acl_ids that appear in the current acl_papers.jsonl.
    # Atomic via _write_jsonl's tmp-file replace.
    full_index = _index_existing_papers(dirs["papers_dir"])
    index = {acl: cid for acl, cid in full_index.items() if acl in keep}
    _write_jsonl(
        dirs["papers_index"],
        [
            {"acl_id": acl, "corpusid": cid}
            for acl, cid in sorted(index.items(), key=lambda x: x[1])
        ],
    )

    print(f"\n[{experiment_name}] metadata pass summary:")
    for label, n_in, n_ok, n_skip in pass_summary:
        skip_note = f" (skipped {n_skip} with no {label.lower()})" if n_skip else ""
        print(f"  {label:12s} in={n_in:>6,} resolved={n_ok:>6,}{skip_note}")
    print(
        f"  total resolved: {len(index):,} / {len(rows):,}; "
        f"unresolved: {len(remaining):,} -> {missing_path}"
    )
    print(f"  index -> {dirs['papers_index']}")


# --- Stage 2: per-paper /references and /citations --------------------


def _paged(url, fields, cap):
    out, offset = [], 0
    while offset < cap:
        page = _request(
            "GET",
            url,
            params={
                "fields": fields,
                "offset": offset,
                "limit": min(CFG.page_size, cap - offset),
            },
        )
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
    return [
        {
            "citingcorpusid": str(corpus_id),
            "citedcorpusid": (
                str(d["citedPaper"]["corpusId"])
                if d.get("citedPaper") and d["citedPaper"].get("corpusId")
                else None
            ),
            "cited_year": (d["citedPaper"] or {}).get("year"),
            "cited_s2fos": (d["citedPaper"] or {}).get("s2FieldsOfStudy"),
            "cited_externalids": (d["citedPaper"] or {}).get("externalIds"),
            "contexts": d.get("contexts"),
            "intents": d.get("intents"),
        }
        for d in page
    ]


def _flatten_cits(corpus_id, page):
    return [
        {
            "citingcorpusid": (
                str(d["citingPaper"]["corpusId"])
                if d.get("citingPaper") and d["citingPaper"].get("corpusId")
                else None
            ),
            "citedcorpusid": str(corpus_id),
            "citing_year": (d["citingPaper"] or {}).get("year"),
            "citing_s2fos": (d["citingPaper"] or {}).get("s2FieldsOfStudy"),
            "citing_externalids": (d["citingPaper"] or {}).get("externalIds"),
            "contexts": d.get("contexts"),
            "intents": d.get("intents"),
        }
        for d in page
    ]


def _process_one(corpus_id, refs_dir, cits_dir, *, fetch_cits_too):
    refs_path = os.path.join(refs_dir, f"{corpus_id}.jsonl")
    cits_path = os.path.join(cits_dir, f"{corpus_id}.jsonl")
    refs_done = os.path.exists(refs_path)
    cits_done = (not fetch_cits_too) or os.path.exists(cits_path)
    if refs_done and cits_done:
        return "skip"
    if not refs_done:
        url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/references"
        _write_jsonl(
            refs_path,
            _flatten_refs(corpus_id, _paged(url, CFG.ref_fields, CFG.max_refs)),
        )
    if fetch_cits_too and not cits_done:
        url = f"{S2_GRAPH_BASE}/paper/CorpusId:{corpus_id}/citations"
        _write_jsonl(
            cits_path,
            _flatten_cits(corpus_id, _paged(url, CFG.cit_fields, CFG.max_cits)),
        )
    return "ok"


def fetch_cites(experiment_name, *, workers, fetch_cits_too, limit=None):
    dirs = experiment_dirs(experiment_name)
    if not os.path.exists(dirs["papers_index"]):
        sys.exit(
            f"Run `python -m src.fetch_graph metadata -e "
            f"{experiment_name}` first; missing {dirs['papers_index']}."
        )
    os.makedirs(dirs["refs_dir"], exist_ok=True)
    os.makedirs(dirs["cits_dir"], exist_ok=True)

    ids = []
    with open(dirs["papers_index"], encoding="utf-8") as f:
        for line in f:
            ids.append(json.loads(line)["corpusid"])
    if limit:
        ids = ids[:limit]

    print(
        f"[{experiment_name}] cites: {len(ids):,} papers, "
        f"workers={workers}, with_cits={fetch_cits_too}, "
        f"page_size={CFG.page_size}."
    )

    counts = {"ok": 0, "skip": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                _process_one,
                cid,
                dirs["refs_dir"],
                dirs["cits_dir"],
                fetch_cits_too=fetch_cits_too,
            ): cid
            for cid in ids
        }
        for fut in tqdm(
            as_completed(futs), total=len(futs), desc=f"{experiment_name}/refs+cits"
        ):
            try:
                counts[fut.result()] += 1
            except Exception as e:
                counts["err"] += 1
                tqdm.write(f"err: {e}")
    print(f"[{experiment_name}] cites: {json.dumps(counts)}")


# --- CLI ---------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    def _add_exp(p):
        p.add_argument(
            "--experiment",
            "-e",
            required=True,
            choices=list(EXPERIMENTS),
            help="Which experiment's data dir to populate.",
        )

    p_meta = sub.add_parser(
        "metadata", help="Stage 1: bulk-resolve ACL ids via POST /paper/batch."
    )
    _add_exp(p_meta)

    p_cites = sub.add_parser(
        "cites", help="Stage 2: per-paper /references and /citations."
    )
    _add_exp(p_cites)
    p_cites.add_argument(
        "--workers",
        type=int,
        default=4 if S2_API_KEY else 1,
        help="Concurrent HTTP workers (1 RPS limit is "
        "global; parallel workers overlap I/O "
        "during back-offs).",
    )
    p_cites.add_argument(
        "--no-cits", action="store_true", help="Skip incoming citations (faster)."
    )
    p_cites.add_argument(
        "--limit", type=int, default=None, help="Stop after N papers (for smoke runs)."
    )

    p_all = sub.add_parser("all", help="Run metadata then cites end-to-end.")
    _add_exp(p_all)
    p_all.add_argument("--workers", type=int, default=4 if S2_API_KEY else 1)
    p_all.add_argument("--no-cits", action="store_true")

    args = ap.parse_args()

    if args.cmd == "metadata":
        fetch_metadata(args.experiment)
    elif args.cmd == "cites":
        fetch_cites(
            args.experiment,
            workers=args.workers,
            fetch_cits_too=not args.no_cits,
            limit=args.limit,
        )
    else:  # all
        fetch_metadata(args.experiment)
        fetch_cites(
            args.experiment, workers=args.workers, fetch_cits_too=not args.no_cits
        )


if __name__ == "__main__":
    sys.exit(main())
