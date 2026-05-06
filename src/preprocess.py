"""Convert the bulk dump to Parquet and project to the columns we use.

Mirrors the upstream `src/preprocess.py` and adds a year filter on
`papers` so downstream joins are restricted to the 2022-2025 window
(citations are kept whole because they need to match into both the
NLP and the cited/citing populations regardless of paper year).
"""

from pyspark.sql import functions as F

from .config import YEAR_MAX, YEAR_MIN
from .data import (
    get_spark_session, load_citations_data_original, load_papers_data_original,
    write_citations_data, write_papers_data,
)


def main():
    spark = get_spark_session()

    papers_df = load_papers_data_original(spark).select(
        "corpusid", "externalids", "title", "authors", "venue",
        "publicationvenueid", "year", "referencecount", "citationcount",
        "influentialcitationcount", "isopenaccess", "s2fieldsofstudy",
    )
    citations_df = load_citations_data_original(spark).select(
        "citingcorpusid", "citedcorpusid", "contexts", "intents",
    )

    # We need *all* papers (cited/citing population can be from any
    # year), but it's useful to materialise a subset of "focal" NLP
    # papers in [YEAR_MIN, YEAR_MAX]. Downstream analysis filters
    # again, so this is just an early prune for performance.
    focal_mask = (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
    nlp_focal = papers_df.filter(focal_mask & F.col("externalids.ACL").isNotNull())
    print(f"Focal NLP papers ({YEAR_MIN}-{YEAR_MAX}): {nlp_focal.count():,}")

    write_papers_data(papers_df)
    write_citations_data(citations_df)


if __name__ == "__main__":
    main()
