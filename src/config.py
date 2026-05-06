"""Shared config for the replication.

The upstream repo hard-codes year filters per analysis function. We
centralise them here so the same code paths can be re-pointed at
arbitrary windows. The default window is the four years that this
replication targets: 2022 through 2025 inclusive.
"""

import os
from dataclasses import dataclass

YEAR_MIN = 2022
YEAR_MAX = 2025
YEAR_RANGE = (YEAR_MIN, YEAR_MAX)

# Semantic Scholar bulk + Graph API
S2_API_KEY = os.environ.get("S2_API_KEY", "")
S2_BULK_RELEASE = os.environ.get("S2_BULK_RELEASE", "latest")
S2_GRAPH_BASE = "https://api.semanticscholar.org/graph/v1"
S2_DATASETS_BASE = "https://api.semanticscholar.org/datasets/v1"

# ACL Anthology bibtex (full corpus, used as ground truth for NLP papers)
ACL_BIB_URL = "https://aclanthology.org/anthology+abstracts.bib.gz"

# Local paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAPERS_DIR = os.path.join(ROOT, "papers")
CITATIONS_DIR = os.path.join(ROOT, "citations")
DATA_DIR = os.path.join(ROOT, "data")
OUTPUTS_DIR = os.path.join(ROOT, "outputs")
FIGURES_DIR = os.path.join(ROOT, "figures")


@dataclass(frozen=True)
class GraphAPIConfig:
    """Per-paper fetch tuning for the Graph API path."""
    paper_fields: str = (
        "corpusId,externalIds,title,year,venue,publicationVenue,"
        "referenceCount,citationCount,influentialCitationCount,"
        "isOpenAccess,s2FieldsOfStudy,authors"
    )
    ref_fields: str = (
        "contexts,intents,citedPaper.corpusId,citedPaper.year,"
        "citedPaper.s2FieldsOfStudy,citedPaper.externalIds"
    )
    cit_fields: str = (
        "contexts,intents,citingPaper.corpusId,citingPaper.year,"
        "citingPaper.s2FieldsOfStudy,citingPaper.externalIds"
    )
    page_size: int = 1000        # max page size on the Graph API
    max_refs: int = 10000        # cap (NLP papers rarely exceed this)
    max_cits: int = 10000
    request_timeout: int = 30
    # Conservative client-side rate-limits. With an API key the server
    # allows ~100 RPS but Graph API frequently throttles bursty clients.
    rps_with_key: float = 50.0
    rps_without_key: float = 1.0


def get_spark_conf():
    """SparkConf identical to the upstream repo."""
    from pyspark import SparkConf
    conf = SparkConf()
    conf.set("spark.driver.port", "7070")
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.ui.reverseProxy", "true")
    conf.set("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
    conf.set("spark.sql.shuffle.partitions", "200")
    return conf
