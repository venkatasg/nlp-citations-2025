# Run notes — replication state

What's actually been run in this branch and what you need to run
yourself for the headline numbers.

## What's already in the repo

1. **Paper-id lists** for both experiments, from
   `https://aclanthology.org/anthology+abstracts.bib.gz`
   (37.7 MB; parsed locally - no API calls):
   * `data/replication/acl_papers.jsonl` — **78,846 ACL papers**
     with year ∈ [1990, 2022].
   * `data/extension/acl_papers.jsonl` — **35,499 ACL papers**
     with year ∈ [2023, 2025].

2. **Smoke-test artefacts** for the `extension` pipeline.
   `data/extension/raw/` contains S2 metadata + refs + cites for
   6 NAACL 2024 long papers (gitignored), and
   `outputs/extension/sample/` holds the CSVs they produce.
   These illustrate the schema; the numbers are not meaningful.

3. **Figure 3 + CFDI plots** generated from the smoke-test data
   (`outputs/extension/figures/`). Same caveat - illustrative only.

## What you need to run

For real numbers you need an S2 API key (free) and ~24 h per
experiment.

```bash
# 0. Free key from https://www.semanticscholar.org/product/api
export S2_API_KEY=...

# 1. Make sure both paper-id lists are current.
python -m src.fetch_acl

# 2a. Replication run.
python -m src.fetch_graph metadata -e replication
python -m src.fetch_graph cites    -e replication --workers 4
python -m src.analysis_api          -e replication
python -m src.plot figure3          -e replication
python -m src.plot cfdi             -e replication

# 2b. Extension run.
python -m src.fetch_graph metadata -e extension
python -m src.fetch_graph cites    -e extension --workers 4
python -m src.analysis_api          -e extension
python -m src.plot figure3          -e extension
python -m src.plot cfdi             -e extension
```

`fetch_graph` writes through a `.tmp` file per output, so any
interrupted run can simply be re-issued and it resumes where it
stopped. The metadata stage scans the per-experiment `papers/` dir
to detect what is already cached; the cites stage skips a paper
when both its refs and cites JSONL files already exist.

## Request budget summary (per the S2 tutorial's best practices)

| Experiment | Stage | Endpoint | Calls (≈) | Notes |
|------------|-------|----------|----------:|-------|
| replication | metadata | `POST /paper/batch` | 160 | 500 ids per call |
| replication | refs | `/paper/{id}/references` | 79,000 | limit=1000 |
| replication | cits | `/paper/{id}/citations` | 130,000 | limit=1000; tail of high-cite papers needs ≤ 50 pages |
| extension | metadata | `POST /paper/batch` | 70 | |
| extension | refs | `/paper/{id}/references` | 35,000 | |
| extension | cits | `/paper/{id}/citations` | 50,000 | |
| **total** | | | **≈ 295,000** | + ~20% retries / 2025 back-fills |

At the documented 1 req/s with an API key, ~80 h of wall-clock
serially; ~36-42 h with `--workers 4`. With the user-allocation
review unlock to a higher rate, proportionally faster.

## Files in this branch

```
README.md                                  overview + how to run
RUN_NOTES.md                               this file
requirements.txt                           requests, tqdm, pandas, numpy, matplotlib
.gitignore
src/__init__.py
src/config.py                              EXPERIMENTS registry, GraphAPIConfig
src/fetch_acl.py                           ACL Anthology -> NLP paper-id list (per experiment)
src/fetch_graph.py                         POST /paper/batch + paged refs/cites
src/analysis_api.py                        CFDI, citation flows, general stats
src/plot.py                                Figure 3 + CFDI per year
data/replication/acl_papers.jsonl          78,846 NLP papers 1990-2022
data/extension/acl_papers.jsonl            35,499 NLP papers 2023-2025
outputs/extension/sample/*.csv             smoke-test CSVs from 6 papers
outputs/extension/figures/*.png            smoke-test figures
```

## Methodology fidelity to the original paper

| Definition | Wahle et al. 2023 | This repo |
|------------|-------------------|-----------|
| NLP paper | `papers.externalids.ACL is not null` | same |
| Field of paper | `s2fieldsofstudy`, drop `external` if `internal` exists | `filter_s2fos` |
| CS sub-field | exploded `s2fieldsofstudy` for CS-tagged papers, drop the "Computer Science" entry | `_select_fields(cs_only=True)` |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | `cfdi` in `analysis_api.py` |
| Self-citation | NLP→NLP / total NLP outgoing | same |
| Figure 3 | % NLP <-> non-CS field citations, top 4 non-CS fields | `src/plot.py:figure3` (Linguistics, Mathematics, Psychology, Sociology) |
| Year filter | `year > 1965` (Figure 3 plotted 1990-2020) | replication: 1990-2022; extension: 2023-2025 |
| Tooling | S2 bulk dataset + PySpark | S2 Graph API (`POST /paper/batch` + paged refs/cits) |

The Graph-API tooling change replaces a 650 GB / 24 h / Spark
workflow with a ~3 GB / ~30 h / single-process HTTP workflow over
the same underlying data.
