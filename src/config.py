"""Shared config for the replication.

Two experiments are registered:

  * `replication`  - reproduces Wahle et al. 2023 (NLP papers
                     1990-2022, the year range plotted in their
                     Figure 3 and over which they report CFDI).
  * `extension`    - same analysis on NLP papers from 2023-01-01 to
                     2025-12-31 (the new four-year window that the
                     original paper does not cover).

Each experiment has its own paper-id list, raw-data cache, and
output directory, so the two runs do not collide.
"""

import os
from dataclasses import dataclass

# Semantic Scholar Graph API.
S2_API_KEY = os.environ.get("S2_API_KEY", "")
S2_GRAPH_BASE = "https://api.semanticscholar.org/graph/v1"

# ACL Anthology bibtex (full corpus, used as the source of truth for
# "is this an NLP paper?" - we replicate Wahle et al.'s definition by
# treating any S2 record with a non-null externalIds.ACL as NLP).
ACL_BIB_URL = "https://aclanthology.org/anthology+abstracts.bib.gz"

# Local paths.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
OUTPUTS_DIR = os.path.join(ROOT, "outputs")


# --- Experiments -------------------------------------------------------

EXPERIMENTS = {
    "replication": {
        "label": "Replication of Wahle et al. 2023",
        "year_min": 1990,
        "year_max": 2022,
        # Figure 3 in the original paper plots the four non-CS fields
        # with the highest citation volume.
        "figure3_fields": ["Linguistics", "Mathematics", "Psychology", "Sociology"],
    },
    "extension": {
        "label": "Extension to 2023-2025",
        "year_min": 2023,
        "year_max": 2025,
        # Same field set as the replication so the two figures are
        # directly comparable.
        "figure3_fields": ["Linguistics", "Mathematics", "Psychology", "Sociology"],
    },
}


def experiment(name):
    if name not in EXPERIMENTS:
        raise SystemExit(f"Unknown experiment {name!r}; valid: {sorted(EXPERIMENTS)}")
    return EXPERIMENTS[name]


def experiment_dirs(name):
    """Return the per-experiment data + output paths."""
    base_data = os.path.join(DATA_DIR, name)
    base_out = os.path.join(OUTPUTS_DIR, name)
    return {
        "data_root": base_data,
        "raw_root": os.path.join(base_data, "raw"),
        "papers_dir": os.path.join(base_data, "raw", "papers"),
        "refs_dir": os.path.join(base_data, "raw", "refs"),
        "cits_dir": os.path.join(base_data, "raw", "cits"),
        "acl_list": os.path.join(base_data, "acl_papers.jsonl"),
        "papers_index": os.path.join(base_data, "papers_index.jsonl"),
        "outputs": base_out,
        "figures": os.path.join(base_out, "figures"),
    }


# --- Graph API tuning --------------------------------------------------


@dataclass(frozen=True)
class GraphAPIConfig:
    """Tuning for the Graph-API fetcher. Defaults follow the S2
    tutorial's best practices: bulk endpoints when available, page
    size = 1000, request only the fields the analysis consumes."""

    # Paper metadata (POST /paper/batch). The analysis only needs
    # corpusId, year, s2FieldsOfStudy, and the ACL externalId
    # (to confirm NLP membership).
    paper_fields: str = (
        "corpusId,externalIds,year,title,citationCount,referenceCount,s2FieldsOfStudy"
    )

    # Per-reference / per-citation fields. Nested paper expansion
    # avoids a second round-trip per neighbour.
    ref_fields: str = (
        "contexts,intents,"
        "citedPaper.corpusId,citedPaper.year,"
        "citedPaper.s2FieldsOfStudy,citedPaper.externalIds"
    )
    cit_fields: str = (
        "contexts,intents,"
        "citingPaper.corpusId,citingPaper.year,"
        "citingPaper.s2FieldsOfStudy,citingPaper.externalIds"
    )

    # POST /paper/batch accepts up to 500 ids per call (per S2 docs).
    batch_size: int = 500
    # Max page size for /references and /citations.
    page_size: int = 1000
    # Hard caps - papers above these need bulk-dataset access anyway.
    max_refs: int = 10000
    max_cits: int = 50000

    request_timeout: int = 60
    # Default rate per the tutorial: "1 request per second" with a key.
    rps_with_key: float = 1.0
    rps_without_key: float = 0.5  # share the unauthenticated quota
