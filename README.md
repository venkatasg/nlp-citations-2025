# NLP Citations 2025

A replication of Wahle, Ruas, Abdalla, Gipp, Mohammad (2023),
*"We are Who We Cite: Bridges of Influence Between Natural Language
Processing and Other Academic Fields"*
([EMNLP 2023](https://aclanthology.org/2023.emnlp-main.797),
[upstream code](https://github.com/jpwahle/emnlp23-citation-field-influence)),
re-run on **NLP papers published between 2022-01-01 and 2025-12-31**.

The original analysis cuts off at 2022. This repo re-uses the same
data sources (Semantic Scholar + ACL Anthology) and the same
definitions (NLP paper = paper with an ACL Anthology ID;
field-of-study = `s2FieldsOfStudy`; CFDI = `1 − Σ pᵢ²`) and reproduces
the same outputs (general stats, citation-flow tables, Citation
Field Diversity Index) for the new four-year window.

The upstream repo runs over a 650 GB Semantic Scholar bulk dataset
in PySpark; this repo runs the same analysis directly off the
[Semantic Scholar Graph API](https://api.semanticscholar.org/api-docs/graph)
following the [S2 tutorial's best
practices](https://www.semanticscholar.org/product/api/tutorial)
(bulk endpoints when available, page size 1,000, only the fields the
analysis consumes). End-to-end this needs **≈ 1 GB of disk and a
free S2 API key**.

## What is replicated

For NLP papers in `[2022, 2025]`:

* General stats: # papers, # references, # incoming citations, %-self,
  median fields-of-study per cited / citing paper.
* Citation flows: NLP ↔ each non-CS field (per-year and aggregated).
* Citation flows: NLP ↔ each CS sub-field.
* CFDI per year (incoming and outgoing diversity).
* Window-aggregated CFDI (the headline number).

All output CSVs go to `outputs/`. Schemas mirror the upstream repo so
the original `plots_r.ipynb` plotting notebook can be re-pointed at
this repo without changes.

## Usage

```bash
# 1. Get a free S2 API key from
#    https://www.semanticscholar.org/product/api
export S2_API_KEY=...

# 2. Install deps.
pip install -r requirements.txt

# 3. NLP-paper list from ACL Anthology bibtex (no API; ~2 minutes).
python -m src.fetch_acl

# 4. Stage 1 - bulk metadata via POST /paper/batch (~90 calls).
python -m src.fetch_graph metadata

# 5. Stage 2 - per-paper /references and /citations (~110k calls).
#    Workers > 1 helps overlap I/O during 429 back-offs.
python -m src.fetch_graph cites --workers 4

# 6. Compute every CSV.
python -m src.analysis_api
```

`fetch_graph` is resumable: each per-paper output is written through
a `.tmp` file, so an interrupted run picks up where it left off.

### Request budget for 2022–2025 (≈ 44 k NLP papers)

| Stage | Endpoint | Calls | Fields requested |
|-------|----------|-------|------------------|
| metadata | `POST /paper/batch` (500 ids/call) | ≈ 90 | `corpusId, externalIds, year, title, citationCount, referenceCount, s2FieldsOfStudy` |
| references | `GET /paper/{id}/references?limit=1000` | ≈ 44 k | `contexts, intents, citedPaper.{corpusId,year,s2FieldsOfStudy,externalIds}` |
| citations | `GET /paper/{id}/citations?limit=1000` | ≈ 66 k | `contexts, intents, citingPaper.{corpusId,year,s2FieldsOfStudy,externalIds}` |
| **total** | | **≈ 110 k** | |

At the documented 1 req/s with an API key, ≈ 30 h serially; 12–18 h
with `--workers 4`.

## Methodology — fidelity to the original

| Definition | Wahle et al. 2023 | This repo |
|------------|-------------------|-----------|
| NLP paper | `papers.externalids.ACL` not null | same (ACL externalId on every record from `POST /paper/batch`) |
| Year filter | `year > 1965` (then per-figure ranges) | `2022 ≤ year ≤ 2025` (`config.YEAR_RANGE`) |
| Field assignment | `s2fieldsofstudy`, dedup `external` if `internal` exists | same (`filter_s2fos` in `analysis_api.py`) |
| CS sub-field assignment | exploded `s2fieldsofstudy` for CS-tagged papers, dropping the "Computer Science" entry itself | same (`_select_fields(cs_only=True)`) |
| Citation join key | `corpusid` | same |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | same (`cfdi`) |
| Self-citation | NLP→NLP / total NLP outgoing | same |
| Output layout | `outputs/*.csv` | same column names |

The only intentional methodological deviation is the year-window
narrowing, which is the explicit goal of this replication.

## Repo layout

```
src/
  config.py              # YEAR_RANGE, paths, GraphAPIConfig
  fetch_acl.py           # ACL Anthology -> NLP paper-id list
  fetch_graph.py         # S2 Graph API: metadata (batch) + refs/cites (paged)
  analysis_api.py        # CFDI, citation flows, general stats -> outputs/*.csv
data/
  acl_papers.jsonl       # NLP paper-id list (committed)
  papers_index.jsonl     # {acl_id, corpusid} index produced by stage 1
  raw/                   # per-paper JSON / JSONL (gitignored)
outputs/
  sample/                # smoke-test CSVs from a 6-paper run (committed)
RUN_NOTES.md             # what was actually run + replication checklist
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
