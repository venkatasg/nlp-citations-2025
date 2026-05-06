"""Shared config for the replication.

The upstream paper (Wahle et al., EMNLP 2023) hard-codes year filters
per analysis function. We centralise them here so the same code paths
can be re-pointed at arbitrary windows. The default window is the
four years that this replication targets: 2022 through 2025
inclusive.
"""

import os
from dataclasses import dataclass

YEAR_MIN = 2022
YEAR_MAX = 2025
YEAR_RANGE = (YEAR_MIN, YEAR_MAX)

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


@dataclass(frozen=True)
class GraphAPIConfig:
    """Tuning for the Graph-API fetcher. Defaults follow the S2
    tutorial's best practices: bulk endpoints when available, page
    size = 1000, request only the fields the analysis consumes."""

    # Paper metadata (POST /paper/batch). The analysis only needs
    # corpusId, year, s2FieldsOfStudy, and the ACL externalId
    # (to confirm NLP membership).
    paper_fields: str = (
        "corpusId,externalIds,year,title,citationCount,referenceCount,"
        "s2FieldsOfStudy"
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
