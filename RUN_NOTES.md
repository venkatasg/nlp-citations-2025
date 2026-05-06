# Run notes — replication state

This document records what was actually run to produce the contents of
`outputs/sample/` and what is still required for a full replication.

## What ran in this commit

1. **`src/fetch_acl.py`** — downloaded
   `https://aclanthology.org/anthology+abstracts.bib.gz` (37.7 MB),
   parsed 122,133 entries, kept **43,960 ACL Anthology papers** with
   year ∈ {2022, 2023, 2024, 2025}. Wrote `data/acl_papers.jsonl`.

2. **`src/fetch_graph.py`** — fetched S2 metadata, references, and
   citations for **6 papers** (NAACL 2024 long papers 1–4) via the
   public S2 Graph API without an API key. Three smoke-test attempts
   exposed a bug in `fetch_acl.py` (bibtex keys were used instead of
   Anthology URL slugs); fixed and re-validated.

3. **`src/analysis_api.py`** — ran on the 6-paper sample, produced 9
   CSVs in `outputs/sample/`. Numbers are illustrative; **not the
   replicated study results**.

## Why the run is small

Without a Semantic Scholar API key the public Graph API throttles
unauthenticated clients to roughly one request per second per IP and
returns frequent 429s. Each NLP paper requires:

* 1 call to fetch metadata,
* ~1–10 calls to page through references,
* ~1–N calls to page through citations.

For ~44k NLP papers that's hundreds of thousands of requests. With an
API key (S2 grants ≈100 req/s) the full window completes in a few
hours; without one it would take days. A single session here cannot
cover the whole window without a key.

## To produce the headline numbers (CFDI, etc.) for 2022–2025

```bash
# 1. Ask for a free key: https://www.semanticscholar.org/product/api
export S2_API_KEY=...

# 2. Refresh ACL Anthology paper list (cheap; ~2 minutes).
python -m src.fetch_acl

# 3. Bulk-fetch the slice. ~3-6 h with a key.
python -m src.fetch_graph --years 2022-2025 --workers 16

# 4. Compute everything.
python -m src.analysis_api

# 5. Or, for the canonical Spark pipeline:
python -m src.download   # 650 GB
python -m src.preprocess
python -m src.analysis
```

## Files added in this branch

```
README.md                    (overview + how to run)
RUN_NOTES.md                 (this file)
requirements.txt
.gitignore
src/__init__.py
src/config.py                (year window 2022-2025; API config)
src/schemas.py               (PySpark schemas, mirror of upstream)
src/data.py                  (Spark loaders, mirror of upstream)
src/download.py              (S2 bulk download, mirror of upstream + auth fix)
src/preprocess.py            (Spark preprocessing + year filter)
src/analysis.py              (bulk analysis: general_stats, citation flows, CFDI)
src/fetch_acl.py             (ACL Anthology -> NLP paper-id list)
src/fetch_graph.py           (per-paper S2 Graph fetch with rate-limit + retry)
src/analysis_api.py          (Graph-API analysis; same CSV shape as bulk)
data/acl_papers.jsonl        (43,960 NLP papers 2022-2025)
outputs/sample/*.csv         (smoke-test CSVs from 6 papers)
outputs/sample/README.md     (what these CSVs are)
```

## Methodology fidelity to the original paper

| Definition | Wahle et al. 2023 | This repo |
|------------|-------------------|-----------|
| NLP paper | `papers.externalids.ACL is not null` | same |
| Field of paper | `s2fieldsofstudy`, drop `external` if `internal` exists | `filter_s2fos` (same logic) |
| CS sub-field | exploded `s2fieldsofstudy` for CS-tagged papers, dropping the "Computer Science" entry itself | same (`_select_fields(cs_only=True)`) |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | same (`cfdi`) |
| Self-citation | NLP→NLP / total NLP outgoing | same (`self_citations` / `general_stats`) |
| Year filter | `year > 1965` (then per-figure ranges) | `2022 ≤ year ≤ 2025` (`config.YEAR_RANGE`) |
| Tooling | S2 bulk dataset + PySpark | same bulk path; alt Graph-API path |

The only intentional methodological deviation from the upstream is the
year-window narrowing, which is the explicit goal of this replication.
