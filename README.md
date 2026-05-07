# NLP Citations 2025

Two experimental runs of Wahle, Ruas, Abdalla, Gipp, Mohammad (2023),
*"We are Who We Cite: Bridges of Influence Between Natural Language
Processing and Other Academic Fields"*
([EMNLP 2023](https://aclanthology.org/2023.emnlp-main.797),
[upstream code](https://github.com/jpwahle/emnlp23-citation-field-influence)):

* **`replication`** — reproduces the original paper's results,
  specifically Figure 3 (% citations from / to NLP across non-CS
  fields, 1990-2020) and the per-year CFDI numbers (e.g. *"declined
  from 0.58 in 1980 to 0.31 in 2022"*). Year window: **1990-2022**.
* **`extension`** — same pipeline, same metric definitions, applied
  to NLP papers from **2023-01-01 through 2025-12-31** — the slice
  the original paper does not cover.

Both experiments use the same data sources (Semantic Scholar Graph
API + ACL Anthology) and the same definitions (NLP paper = paper
with an ACL Anthology ID; field-of-study = `s2FieldsOfStudy`;
CFDI = `1 − Σ pᵢ²`). The only configured difference between the two
runs is the year window.

The upstream repo runs over a 650 GB Semantic Scholar bulk dataset
in PySpark; this repo runs the same analysis directly off the
[Graph API](https://api.semanticscholar.org/api-docs/graph)
following the
[S2 tutorial's best practices](https://www.semanticscholar.org/product/api/tutorial)
(bulk endpoints when available, page size 1,000, only the fields
the analysis consumes). End-to-end this needs **≈ 1–3 GB of disk
and a free S2 API key**.

## Quickstart

```bash
# 1. Get a free S2 API key from
#    https://www.semanticscholar.org/product/api
export S2_API_KEY=...

# 2. Install deps.
uv sync

# 3. Build NLP-paper lists for both experiments
#    (~2 minutes; no API calls).
python -m src.fetch_acl                        # writes data/{replication,extension}/acl_papers.jsonl

# 4. Run one or both experiments. (--experiment all runs both.)

# Replication (Wahle et al.; ~79k NLP papers from 1990-2022):
python -m src.fetch_graph metadata -e replication
python -m src.fetch_graph cites    -e replication --workers 4
python -m src.analysis_api          -e replication
python -m src.plot figure3          -e replication
python -m src.plot cfdi             -e replication

# Extension (the new 2023-2025 window; ~35k NLP papers):
python -m src.fetch_graph metadata -e extension
python -m src.fetch_graph cites    -e extension --workers 4
python -m src.analysis_api          -e extension
python -m src.plot figure3          -e extension
python -m src.plot cfdi             -e extension
```

`fetch_graph` is resumable: each per-paper output is written through
a `.tmp` file, so an interrupted run picks up where it left off.

### Request budget

| Experiment | NLP papers | Stage | Endpoint | Calls (≈) |
|------------|-----------:|-------|----------|----------:|
| `replication` | 78,846 | metadata | `POST /paper/batch` (500 ids/call) | 160 |
|              |        | references | `GET /paper/{id}/references?limit=1000` | 79 k |
|              |        | citations | `GET /paper/{id}/citations?limit=1000` | 130 k |
| **replication total** | | | | **≈ 210 k** |
| `extension`  | 35,499 | metadata | `POST /paper/batch` | 70 |
|              |        | references | `/references?limit=1000` | 35 k |
|              |        | citations | `/citations?limit=1000` | 50 k |
| **extension total** | | | | **≈ 85 k** |
| **both** | 114,345 | | | **≈ 295 k** |

At the documented 1 req/s with an API key the replication takes
≈ 60 h serially / 24-30 h with `--workers 4`; the extension takes
≈ 24 h serially / 10-12 h with `--workers 4`.

The rate limiter is a single global gate (`_politeness_sleep` in
`src/fetch_graph.py`) protected by a `threading.Lock`. With
multiple workers and across all S2 endpoints (`POST /paper/batch`,
`/references`, `/citations`), the effective request rate is
guaranteed `≤ 1 req/s`. Verified by `tests/test_rate_limit.py`:

```bash
python -m unittest tests.test_rate_limit -v
```

## Outputs

Each experiment writes to its own directory:

```
outputs/<experiment>/
  general_stats.csv                            per-year NLP papers / refs / cites / median FoS
  general_stats_papers_per_field.csv           paper count per S2 field
  nlp_self_citations.csv                       NLP -> NLP / total NLP outgoing per year
  citations_non_cs_fields_to_nlp_by_year.csv   NLP <-> each non-CS field (Figure 3 source)
  citations_cs_fields_to_nlp_by_year.csv       NLP <-> each CS sub-field
  nlp_papers_diversity.csv                     per-paper CFDI (in / out)
  cfdi_per_year_nlp.csv                        CFDI averaged per year
  cfdi_window_aggregated.csv                   window-level CFDI (headline number)
  nlp_field_distribution.csv                   total citations per field, in / out
  figure3_data.csv                             long-format data behind figure3.png
  figures/figure3.png                          (a) NLP -> non-CS, (b) non-CS -> NLP
  figures/cfdi_per_year.png                    CFDI per year line plot
```

## Methodology — fidelity to the original

| Definition | Wahle et al. 2023 | This repo |
|------------|-------------------|-----------|
| NLP paper | `papers.externalids.ACL` not null | same (ACL externalId on every record from `POST /paper/batch`) |
| Year filter | `year > 1965` (Figure 3 plotted 1990-2020) | `replication`: 1990-2022; `extension`: 2023-2025 |
| Field assignment | `s2fieldsofstudy`, dedup `external` if `internal` exists | same (`filter_s2fos` in `analysis_api.py`) |
| CS sub-field assignment | exploded `s2fieldsofstudy` for CS-tagged papers, dropping the "Computer Science" entry itself | same (`_select_fields(cs_only=True)`) |
| Figure 3 | % NLP <-> non-CS field citations / all NLP <-> non-CS, top 4 non-CS fields | same (`src/plot.py:figure3`) |
| CFDI | `1 − Σ pᵢ²` over field-of-study counts | same (`cfdi`) |
| Self-citation | NLP→NLP / total NLP outgoing | same |
| Citation join key | `corpusid` | same |

The `replication` experiment is intended to closely match Wahle et
al.'s headline results; the `extension` experiment runs the same
pipeline on the new four-year window.

## Repo layout

```
src/
  config.py        EXPERIMENTS registry, GraphAPIConfig, paths
  fetch_acl.py     ACL Anthology -> per-experiment NLP paper-id list
  fetch_graph.py   POST /paper/batch + paged refs/cites (per-experiment)
  analysis_api.py  CFDI, citation flows, general stats
  plot.py          Figure 3 + CFDI per year
data/<experiment>/
  acl_papers.jsonl
  papers_index.jsonl
  raw/{papers,refs,cits}/
outputs/<experiment>/
  *.csv, figures/*.png
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
