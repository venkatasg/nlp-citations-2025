"""PySpark schemas for the S2 bulk dataset.

Identical to the upstream repo's `schemas.py` so the rest of the
bulk pipeline can run unchanged.
"""

from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)


def _externalids_struct():
    return StructType([
        StructField("ACL", StringType(), True),
        StructField("DBLP", StringType(), True),
        StructField("ArXiv", StringType(), True),
        StructField("MAG", StringType(), True),
        StructField("CorpusId", StringType(), True),
        StructField("PubMed", StringType(), True),
        StructField("DOI", StringType(), True),
        StructField("PubMedCentral", StringType(), True),
    ])


def _authors_array():
    return ArrayType(StructType([
        StructField("authorId", StringType(), True),
        StructField("name", StringType(), True),
    ]))


def _s2fos_array():
    return ArrayType(StructType([
        StructField("category", StringType(), True),
        StructField("source", StringType(), True),
    ]))


def get_original_papers_schema():
    return StructType([
        StructField("corpusid", IntegerType(), True),
        StructField("externalids", _externalids_struct(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("authors", _authors_array(), True),
        StructField("venue", StringType(), True),
        StructField("publicationvenueid", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("referencecount", IntegerType(), True),
        StructField("citationcount", IntegerType(), True),
        StructField("influentialcitationcount", IntegerType(), True),
        StructField("isopenaccess", BooleanType(), True),
        StructField("s2fieldsofstudy", _s2fos_array(), True),
        StructField("publicationtypes", ArrayType(StringType()), True),
        StructField("publicationdate", TimestampType(), True),
        StructField("journal", StructType([
            StructField("name", StringType(), True),
            StructField("pages", StringType(), True),
            StructField("volume", StringType(), True),
        ]), True),
        StructField("updated", TimestampType(), True),
    ])


def get_truncated_papers_schema():
    return StructType([
        StructField("corpusid", IntegerType(), True),
        StructField("externalids", _externalids_struct(), True),
        StructField("title", StringType(), True),
        StructField("authors", _authors_array(), True),
        StructField("venue", StringType(), True),
        StructField("publicationvenueid", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("referencecount", IntegerType(), True),
        StructField("citationcount", IntegerType(), True),
        StructField("influentialcitationcount", IntegerType(), True),
        StructField("isopenaccess", BooleanType(), True),
        StructField("s2fieldsofstudy", _s2fos_array(), True),
    ])


def get_original_citations_schema():
    return StructType([
        StructField("citingcorpusid", StringType(), True),
        StructField("citedcorpusid", StringType(), True),
        StructField("isinfluential", BooleanType(), True),
        StructField("contexts", ArrayType(StringType()), True),
        StructField("intents", ArrayType(StringType()), True),
        StructField("updated", TimestampType(), True),
    ])


def get_truncated_citations_schema():
    return StructType([
        StructField("citingcorpusid", StringType(), True),
        StructField("citedcorpusid", StringType(), True),
        StructField("contexts", ArrayType(StringType()), True),
        StructField("intents", ArrayType(StringType()), True),
    ])
