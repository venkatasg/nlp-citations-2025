"""Spark loaders / writers (mirrors upstream repo)."""

from pyspark.sql import SparkSession

from .config import get_spark_conf
from .schemas import (
    get_original_citations_schema, get_original_papers_schema,
    get_truncated_citations_schema, get_truncated_papers_schema,
)


def get_spark_session():
    return (
        SparkSession.builder
        .appName("Citation Diversity Analysis 2022-2025")
        .config(conf=get_spark_conf())
        .getOrCreate()
    )


def load_papers_data_original(spark):
    return spark.read.json(
        "papers.jsonl/*.jsonl", schema=get_original_papers_schema()
    )


def load_citations_data_original(spark):
    return spark.read.json(
        "citations/*.jsonl", schema=get_original_citations_schema()
    )


def load_papers_data(spark):
    return spark.read.json(
        "papers.jsonl", schema=get_truncated_papers_schema()
    )


def load_citations_data(spark):
    return spark.read.json(
        "citations.jsonl", schema=get_truncated_citations_schema()
    )


def write_papers_data(df):
    df.write.json("papers.jsonl", mode="overwrite")


def write_citations_data(df):
    df.write.json("citations.jsonl", mode="overwrite")
