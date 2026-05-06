# Run notes — replication state

This document records what was actually run to produce the contents
of `outputs/sample/` and what is still required for a full
replication.

## What ran in this commit

1. **`src/fetch_acl.py`** — downloaded
   `https://aclanthology.org/anthology+abstracts.bib.gz` (37.7 MB),
   parsed 122,133 entries, kept **43,960 ACL Anthology papers** with
   year ∈ {2022, 2023, 2024, 2025}. Wrote `data/acl_papers.jsonl`.

2. **`src/fetch_graph.py`** — fetched S2 metadata, references, and
   citations for **6 papers** (NAACL 2024 long papers 1–4) via the
   public S2 Graph API without an API key (smoke test of the
   pipeline; not a real run).

3. **`src/analysis_api.py`** — ran on the 6-paper sample, produced 9
   CSVs in `outputs/sample/`. Numbers are illustrative; **not the
   replicated study results**.

## Why the run is small

Without a Semantic Scholar API key the public Graph API throttles
unauthenticated clients aggressively (1 RPS, frequent 429s). For
~44k NLP papers that's hundreds of thousands of requests. With an
API key (S2 grants 1 req/s + the bulk-batch endpoint) the full
window completes in ≈ 12–18 hours; without a key it would take
days.

## To produce the headline numbers (CFDI, etc.) for 2022–2025

The Graph-API pipeline follows S2's best-practices tutorial: bulk
endpoints when available, page size 1000, request only the fields
consumed by the analysis.

```bash
# 1. Ask for a free key: https://www.semanticscholar.org/product/api
export S2_API_KEY=...

# 2. Install deps.
pip install -r requirements.txt

# 3. Refresh ACL Anthology paper list (cheap; ~2 minutes, no API).
python -m src.fetch_acl

# 4. Stage 1 - bulk metadata via POST /paper/batch (~90 calls).
python -m src.fetch_graph metadata

# 5. Stage 2 - per-paper /references and /citations (~110k calls).
#    Workers > 1 helps overlap I/O during 429 back-offs.
python -m src.fetch_graph cites --workers 4

# 6. Compute every CSV.
python -m src.analysis_api
```

### Request budget summary

| Stage | Endpoint | Calls (≈) | Notes |
|-------|----------|-----------|-------|
| metadata | `POST /paper/batch` | 90 | 500 ids per call |
| references | `GET /paper/{id}/references` | 44,000 | limit=1000; 1 page covers virtually all ACL papers |
| citations | `GET /paper/{id}/citations` | 66,000 | limit=1000; tail of high-cite papers needs ≤ 50 pages |
| **total** | | **≈ 110,000** | + ~20 % for retries / back-fills |

At the documented 1 req/s with an API key, ~30 h serially; 12-18 h
with `--workers 4`.

## Files in this branch

```
README.md                    overview + how to run
RUN_NOTES.md                 this file
requirements.txt             requests, tqdm, pandas, numpy
.gitignore
src/__init__.py
src/config.py                YEAR_RANGE = (2022, 2025); GraphAPIConfig
src/fetch_acl.py             ACL Anthology -> NLP paper-id list
src/fetch_graph.py           POST /paper/batch + paged refs/cits
src/analysis_api.py          CFDI, citation flows, general stats
data/acl_papers.jsonl        43,960 NLP papers 2022-2025
outputs/sample/*.csv         smoke-test CSVs from 6 papers
outputs/sample/README.md     what those CSVs are
```

## Methodology fidelity to the original paper

| Definition | Wahle et al. 2023 | This repo |
|------------|-------------------|-----------|
| NLP paper | `papers.externalids.ACL is not null` | same |
| Field of paper | `s2fieldsofstudy`, drop `external` if `internal` exists | `filter_s2fos` (same logic) |
| CS sub-field | exploded `s2fieldsofstudy` for CS-tagged papers, dropping the "Computer Science" entry itself | same (`_select_fields(cs_only=True)`) |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | same (`cfdi`) |
| Self-citation | NLP→NLP / total NLP outgoing | same |
| Year filter | `year > 1965` (then per-figure ranges) | `2022 ≤ year ≤ 2025` (`config.YEAR_RANGE`) |
| Tooling | S2 bulk dataset + PySpark | S2 Graph API (`POST /paper/batch` + paged refs/cits) |

The only intentional methodological deviation is the year-window
narrowing, which is the explicit goal of this replication. The
tooling change (Graph API instead of bulk dataset) replaces a
650 GB / 24 h / Spark workflow with a ~1 GB / ~15 h / single-process
HTTP workflow over the same underlying data.
