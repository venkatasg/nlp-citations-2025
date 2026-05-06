# Sample run

Outputs in this folder come from a 6-paper end-to-end smoke test of
the Graph-API pipeline. They demonstrate the schema and column
names produced by `src/analysis_api.py`. **Numbers in this folder
are not meaningful** — they are computed over a six-paper subset of
NAACL 2024 long papers.

To produce real numbers, run the full pipeline (see `../../README.md`):

```bash
export S2_API_KEY=...
python -m src.fetch_acl
python -m src.fetch_graph --years 2022-2025
python -m src.analysis_api
```

The full Graph-API run over all ~44k ACL papers from 2022–2025
takes a few hours with an API key (≈100 req/s) and ~1 GB of disk
for `data/raw/`.

## Files

| file | purpose | upstream analog |
|------|---------|-----------------|
| `general_stats_papers_per_field.csv` | paper count per S2 field | `count_papers_per_field` |
| `general_stats.csv` | per-year NLP papers / refs / cits / median fields | `comput_general_stats` |
| `nlp_self_citations.csv` | NLP→NLP / NLP→all per year | `compute_self_citations` |
| `citations_non_cs_fields_to_nlp_by_year.csv` | NLP ↔ each non-CS field | `compute_non_cs_fields_and_nlp_citation_counts` |
| `citations_cs_fields_to_nlp_by_year.csv` | NLP ↔ each CS sub-field | `compute_cs_subfield_to_nlp_citation_counts` |
| `nlp_papers_diversity.csv` | per-paper CFDI (in / out) | `compute_cfdi_nlp_by_citation_quantile` |
| `cfdi_per_year_nlp.csv` | CFDI averaged per year | aggregated from above |
| `cfdi_window_aggregated.csv` | window-level CFDI for NLP (in / out) | headline number |
| `nlp_field_distribution.csv` | total citations per field, in / out | input to CFDI |
