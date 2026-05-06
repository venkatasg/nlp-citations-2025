# NLP Citations 2025

A replication of Wahle, Ruas, Abdalla, Gipp, Mohammad (2023), *"We are Who We Cite:
Bridges of Influence Between Natural Language Processing and Other Academic
Fields"* (EMNLP 2023, [paper](https://aclanthology.org/2023.emnlp-main.797),
[upstream code](https://github.com/jpwahle/emnlp23-citation-field-influence)),
re-run on **NLP papers published between 2022-01-01 and 2025-12-31**.

The original analysis cuts off at 2022. This repo re-uses the same data
sources (Semantic Scholar + ACL Anthology) and the same definitions
(NLP paper = paper with an ACL Anthology ID; field-of-study =
`s2FieldsOfStudy`; CFDI = `1 − Σ pᵢ²`) and reproduces the same outputs
(general stats, citation-flow tables, Citation Field Diversity Index)
for the new four-year window.

## What is replicated

For NLP papers in `[2022, 2025]`:

* General stats: # papers, # references, # incoming citations, %-self,
  median fields-of-study per cited / citing paper.
* Citation flows: NLP ↔ each non-CS field (per-year and aggregated).
* Citation flows: NLP ↔ each CS sub-field.
* CFDI per year (incoming and outgoing diversity).
* CFDI per non-CS field, per year (control comparison).

All output CSVs go to `outputs/`. Schemas mirror the upstream repo so
the original `plots_r.ipynb` plotting notebook can be re-pointed at
this repo without changes.

## Two pipelines

### A. Bulk pipeline (faithful to the upstream repo)

Mirrors Wahle et al.'s pipeline exactly: S2 bulk-dataset dump →
PySpark preprocessing → Spark joins → CSVs. Use this for the canonical
replication. **Resource cost: ~650 GB compressed download + 24 h+ of
Spark on a workstation.** The only meaningful change from upstream is
that all year-filters are clamped to 2022–2025 (`config.py:YEAR_RANGE`).

```bash
# 1. Get a Semantic Scholar API key, then download the latest release.
export S2_API_KEY=...
python -m src.download           # 108 GB papers + 534 GB citations

# 2. Convert to Parquet and project to the columns we need.
python -m src.preprocess

# 3. Run the full analysis. Outputs go to ./outputs/.
python -m src.analysis
```

### B. Graph-API pipeline (practical, no bulk download)

Uses the [Semantic Scholar Graph API](https://api.semanticscholar.org/api-docs/graph)
to fetch only the slice we need: NLP papers from 2022–2025 plus
their references and citations. Follows S2's
[best-practices tutorial](https://www.semanticscholar.org/product/api/tutorial)
— bulk endpoints when available, page size 1,000, only the fields the
analysis consumes.

```bash
export S2_API_KEY=...                          # required for any sized run
python -m src.fetch_acl                        # ACL Anthology -> NLP paper-id list

# Two stages, runnable independently:
python -m src.fetch_graph metadata             # POST /paper/batch (~90 calls for 44k papers)
python -m src.fetch_graph cites --workers 4    # per-paper /references and /citations
# ...or do both in one go:
python -m src.fetch_graph all --workers 4

python -m src.analysis_api                     # CSVs identical in shape to the bulk path
```

**Request budget for 2022–2025** (≈44 k NLP papers):

| Stage | Endpoint | Calls | Fields requested |
|-------|----------|-------|------------------|
| metadata | `POST /paper/batch` (500 ids/call) | ≈ 90 | `corpusId, externalIds, year, title, citationCount, referenceCount, s2FieldsOfStudy` |
| references | `GET /paper/{id}/references?limit=1000` | ≈ 44 k | `contexts, intents, citedPaper.{corpusId,year,s2FieldsOfStudy,externalIds}` |
| citations | `GET /paper/{id}/citations?limit=1000` | ≈ 66 k | `contexts, intents, citingPaper.{corpusId,year,s2FieldsOfStudy,externalIds}` |
| **total** | | **≈ 110 k** | |

At the documented 1 req/s with an API key, a full run takes ≈ 30 h
of wall-clock; in practice 2–4 workers running in parallel overlap
I/O during 429 back-offs and bring this down to ≈ 12–18 h.

## Methodology — how this matches the original

| Step | Upstream (Wahle et al.) | This repo |
|------|-------------------------|-----------|
| NLP paper definition | `papers.externalids.ACL` not null | same |
| Year filter | `year > 1965` (then per-figure ranges) | `2022 ≤ year ≤ 2025` |
| Field assignment | `s2fieldsofstudy`, dedup external if internal exists | same (`filter_s2fieldsofstudy`) |
| CS sub-field assignment | exploded `s2fieldsofstudy` where category=CS | same |
| Citation join key | `corpusid` | same |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | same |
| Self-citation | NLP→NLP / total NLP outgoing | same |
| Output layout | `outputs/*.csv` | same column names |

## Time window rationale

The original paper's last full year is 2022. Re-running for
**2022–2025** gives a four-year window where 2022 directly overlaps
the original's last reported year (allowing a sanity-check on
absolute counts after S2 has back-filled missing references) and
2023–2025 is genuinely new.

## Repo layout

```
src/
  config.py              # YEAR_RANGE, paths, API config
  schemas.py             # PySpark schemas (mirrors upstream)
  data.py                # Spark loaders / writers
  download.py            # bulk dataset download
  preprocess.py          # bulk -> parquet, project columns, year-filter
  analysis.py            # bulk analysis (Spark)
  fetch_acl.py           # ACL Anthology -> NLP paper-ID list
  fetch_graph.py         # S2 Graph API per-paper fetcher
  analysis_api.py        # analysis over Graph API output
citations/, papers/      # bulk dataset (gitignored)
data/                    # Graph API cache (gitignored)
outputs/                 # result CSVs
figures/                 # plots
```

## Citing

If you use this replication, please cite the original paper:

```bib
@inproceedings{wahle-etal-2023-cite,
    title = "We are Who We Cite: Bridges of Influence Between Natural Language Processing and Other Academic Fields",
    author = "Wahle, Jan Philip and Ruas, Terry and Abdalla, Mohamed and Gipp, Bela and Mohammad, Saif",
    booktitle = "Proceedings of the 2023 Conference on Empirical Methods in Natural Language Processing",
    year = "2023",
    url = "https://aclanthology.org/2023.emnlp-main.797",
}
```
