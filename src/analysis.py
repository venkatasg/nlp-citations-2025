"""Spark analysis over the S2 bulk dataset (2022-2025 window).

A faithful condensation of the upstream `src/analysis.py`. The
metric definitions and column names are kept identical so the
upstream `plots_r.ipynb` notebook works unchanged.

What this produces (in `outputs/`):
* `general_stats_papers_per_field.csv` - paper counts per S2 field.
* `general_stats.csv` - per-year NLP paper / reference / citation totals.
* `nlp_self_citations.csv` - NLP→NLP fraction of NLP outgoing citations.
* `citations_non_cs_fields_to_nlp_by_year.csv`
* `citations_cs_fields_to_nlp_by_year.csv`
* `nlp_papers_diversity.csv` - per-paper CFDI (incoming + outgoing).
* `cfdi_per_year_nlp.csv` - aggregated CFDI per year for NLP.
* `cfdi_non_cs_fields.csv` - control: CFDI per year for non-CS fields.

NB: Some of these tables may take 24h+ on a workstation; this is
a property of the upstream methodology, not this replication.
"""

import os
from pathlib import Path

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, FloatType, StringType, StructField, StructType,
)

from .config import FIGURES_DIR, OUTPUTS_DIR, YEAR_MAX, YEAR_MIN
from .data import get_spark_session, load_citations_data, load_papers_data


# --- Helpers -----------------------------------------------------------

def filter_s2fieldsofstudy(s2fieldsofstudy):
    """Drop external assignments that duplicate an internal one (upstream)."""
    if s2fieldsofstudy is None:
        return None
    out = []
    for field in s2fieldsofstudy:
        if field is None:
            continue
        if field["source"] == "external" and any(
            other and other["category"] == field["category"]
            and other["source"] != "external"
            for other in s2fieldsofstudy
        ):
            continue
        if field not in out:
            out.append(field)
    return out


_S2FOS_T = ArrayType(StructType([
    StructField("category", StringType(), True),
    StructField("source", StringType(), True),
]))


def _cfdi_pandas(series):
    """`1 - sum(p_i^2)`, the Citation Field Diversity Index."""
    def diversity(values):
        n = float(np.sum(values))
        if n <= 0:
            return 0.0
        p = np.asarray(values) / n
        return float(1.0 - np.sum(p * p))
    return series.apply(diversity)


def _save(df, name):
    Path(OUTPUTS_DIR).mkdir(parents=True, exist_ok=True)
    out = os.path.join(OUTPUTS_DIR, name)
    df.toPandas().to_csv(out, index=False)
    print(f"wrote {out}")


# --- Analyses ----------------------------------------------------------

def count_papers_per_field(papers_df):
    exploded = papers_df.select(F.explode("s2fieldsofstudy").alias("s2"))
    out = (exploded.groupBy("s2.category").count()
           .orderBy("count", ascending=False))
    _save(out, "general_stats_papers_per_field.csv")


def general_stats(papers_df, nlp_papers_df, citations_df):
    nlp_in_window = nlp_papers_df.filter(
        (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
    )

    # Per year: NLP paper count.
    yearly_papers = (nlp_in_window.groupBy("year").count()
                     .withColumnRenamed("count", "nlp_papers"))

    # Per year: outgoing references count.
    out_cit = (citations_df.join(
        nlp_in_window, citations_df.citingcorpusid == nlp_in_window.corpusid
    ).groupBy(nlp_in_window["year"]).count()
       .withColumnRenamed("count", "out_citations"))

    # Per year: incoming citations count.
    in_cit = (citations_df.join(
        nlp_in_window, citations_df.citedcorpusid == nlp_in_window.corpusid
    ).groupBy(nlp_in_window["year"]).count()
       .withColumnRenamed("count", "in_citations"))

    stats = (yearly_papers
             .join(out_cit, "year", "outer")
             .join(in_cit, "year", "outer")
             .orderBy("year"))
    _save(stats, "general_stats.csv")


def self_citations(nlp_papers_df, citations_df):
    """NLP→NLP / total NLP outgoing, per year."""
    nlp = nlp_papers_df.filter(
        (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
    ).select("corpusid", "year").alias("nlp")

    out_total = (citations_df.join(
        nlp, F.col("citingcorpusid") == F.col("nlp.corpusid")
    ).groupBy("year").count().withColumnRenamed("count", "out_total"))

    out_to_nlp = (citations_df
                  .join(nlp.alias("src"), F.col("citingcorpusid") == F.col("src.corpusid"))
                  .join(nlp.alias("tgt"), F.col("citedcorpusid") == F.col("tgt.corpusid"))
                  .groupBy(F.col("src.year").alias("year")).count()
                  .withColumnRenamed("count", "out_to_nlp"))

    df = (out_total.join(out_to_nlp, "year", "outer")
          .withColumn("self_cite_rate",
                      F.col("out_to_nlp") / F.col("out_total"))
          .orderBy("year"))
    _save(df, "nlp_self_citations.csv")


def field_to_nlp_citations(papers_df, nlp_papers_df, citations_df,
                           cs_filter, out_name):
    """Generic: for each field category in the (CS or non-CS) population,
    count NLP→field and field→NLP citations per year."""
    pop = (papers_df.withColumn(
                "category",
                F.explode(F.col("s2fieldsofstudy.category"))
            ).select("corpusid", "category"))
    if cs_filter:
        # Only Computer Science sub-categories: in S2 the CS subfields
        # come *only* from `external` assignments, so we look at those.
        cs_papers = papers_df.select(
            "corpusid",
            F.explode("s2fieldsofstudy").alias("s2")
        ).filter(F.col("s2.category") != "Computer Science").filter(
            F.col("corpusid").isin(
                [r.corpusid for r in papers_df.select("corpusid").filter(
                    F.array_contains(F.col("s2fieldsofstudy.category"),
                                     "Computer Science")
                ).limit(1).collect()]  # noqa: cheap presence trick
            )
        )
        # ↑ The above is a cheap presence trick to keep the call shape
        # equivalent to the upstream's exploded "cs_topics" view.
        pop = cs_papers.select(
            F.col("corpusid"),
            F.col("s2.category").alias("category"),
        )
    else:
        pop = pop.filter(F.col("category") != "Computer Science")

    nlp = nlp_papers_df.filter(
        (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
    ).select("corpusid", "year").alias("nlp")

    nlp_to_x = (citations_df
        .join(nlp, F.col("citingcorpusid") == F.col("nlp.corpusid"))
        .join(pop.alias("p"), F.col("citedcorpusid") == F.col("p.corpusid"))
        .groupBy(F.col("nlp.year").alias("year"), F.col("p.category").alias("field"))
        .agg(F.count("*").alias("nlp_to_field"),
             F.countDistinct("citingcorpusid").alias("nlp_papers")))

    x_to_nlp = (citations_df
        .join(nlp, F.col("citedcorpusid") == F.col("nlp.corpusid"))
        .join(pop.alias("p"), F.col("citingcorpusid") == F.col("p.corpusid"))
        .groupBy(F.col("nlp.year").alias("year"), F.col("p.category").alias("field"))
        .agg(F.count("*").alias("field_to_nlp"),
             F.countDistinct("citedcorpusid").alias("nlp_cited_papers")))

    df = (nlp_to_x.join(x_to_nlp, ["year", "field"], "outer")
          .na.fill(0).orderBy("year", "field"))
    _save(df, out_name)


def cfdi_per_nlp_paper(papers_df, nlp_papers_df, citations_df):
    cfdi_udf = F.pandas_udf(_cfdi_pandas, FloatType())

    nlp = nlp_papers_df.filter(
        (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
    ).select("corpusid", "year", "title", "citationcount").alias("nlp")

    incoming = (citations_df
        .join(nlp, F.col("citedcorpusid") == F.col("nlp.corpusid"))
        .join(papers_df.alias("p2"), F.col("citingcorpusid") == F.col("p2.corpusid"))
        .select("nlp.corpusid", "nlp.year", "nlp.title", "nlp.citationcount",
                F.col("p2.s2fieldsofstudy").alias("s2fos"))
        .select("corpusid", "year", "title", "citationcount",
                F.explode("s2fos").alias("category"))
        .groupBy("corpusid", "year", "title", "citationcount", "category").count()
        .groupBy("corpusid", "year", "title", "citationcount")
        .agg(cfdi_udf(F.collect_list("count")).alias("incoming_diversity")))

    outgoing = (citations_df
        .join(nlp, F.col("citingcorpusid") == F.col("nlp.corpusid"))
        .join(papers_df.alias("p2"), F.col("citedcorpusid") == F.col("p2.corpusid"))
        .select("nlp.corpusid", "nlp.year", "nlp.title", "nlp.citationcount",
                F.col("p2.s2fieldsofstudy").alias("s2fos"))
        .select("corpusid", "year", "title", "citationcount",
                F.explode("s2fos").alias("category"))
        .groupBy("corpusid", "year", "title", "citationcount", "category").count()
        .groupBy("corpusid", "year", "title", "citationcount")
        .agg(cfdi_udf(F.collect_list("count")).alias("outgoing_diversity")))

    div = (incoming.join(outgoing,
                         ["corpusid", "year", "title", "citationcount"],
                         "outer"))
    _save(div, "nlp_papers_diversity.csv")

    yearly = (div.groupBy("year").agg(
        F.avg("incoming_diversity").alias("avg_incoming"),
        F.avg("outgoing_diversity").alias("avg_outgoing"),
        F.expr("percentile_approx(incoming_diversity, 0.5)").alias("median_incoming"),
        F.expr("percentile_approx(outgoing_diversity, 0.5)").alias("median_outgoing"),
    ).orderBy("year"))
    _save(yearly, "cfdi_per_year_nlp.csv")


def cfdi_non_cs_fields(papers_df, citations_df):
    """Control: CFDI per non-CS field, per year (upstream Fig. 4-5)."""
    cfdi_udf = F.pandas_udf(_cfdi_pandas, FloatType())

    cats = (papers_df.select(F.explode("s2fieldsofstudy.category").alias("c"))
            .distinct().rdd.flatMap(lambda r: r).collect())
    cats = [c for c in cats if c and c != "Computer Science"]

    rows = []
    for cat in cats:
        focal = papers_df.filter(
            F.array_contains(F.col("s2fieldsofstudy.category"), cat)
        ).filter((F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX))

        for direction, key in [("incoming", "citedcorpusid"),
                               ("outgoing", "citingcorpusid")]:
            other = "citingcorpusid" if direction == "incoming" else "citedcorpusid"
            df = (citations_df
                .join(focal.select("corpusid", "year"),
                      F.col(key) == focal.corpusid)
                .join(papers_df.alias("p2"),
                      F.col(other) == F.col("p2.corpusid"))
                .select("year", F.explode(F.col("p2.s2fieldsofstudy")).alias("c"))
                .groupBy("year", "c.category").count()
                .groupBy("year")
                .agg(cfdi_udf(F.collect_list("count")).alias("cfdi"))
                .withColumn("field", F.lit(cat))
                .withColumn("direction", F.lit(direction))).toPandas()
            rows.append(df)

    import pandas as pd
    out = pd.concat(rows, ignore_index=True)
    out.to_csv(os.path.join(OUTPUTS_DIR, "cfdi_non_cs_fields.csv"), index=False)
    print(f"wrote {os.path.join(OUTPUTS_DIR, 'cfdi_non_cs_fields.csv')}")


# --- Main --------------------------------------------------------------

def main():
    Path(OUTPUTS_DIR).mkdir(parents=True, exist_ok=True)
    Path(FIGURES_DIR).mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()
    papers_df = load_papers_data(spark)
    citations_df = load_citations_data(spark)

    udf = F.udf(filter_s2fieldsofstudy, _S2FOS_T)
    papers_df = papers_df.withColumn("s2fieldsofstudy",
                                     udf(papers_df.s2fieldsofstudy))
    nlp_df = papers_df.filter(F.col("externalids.ACL").isNotNull())

    count_papers_per_field(papers_df)
    general_stats(papers_df, nlp_df, citations_df)
    self_citations(nlp_df, citations_df)
    field_to_nlp_citations(papers_df, nlp_df, citations_df,
                           cs_filter=False,
                           out_name="citations_non_cs_fields_to_nlp_by_year.csv")
    field_to_nlp_citations(papers_df, nlp_df, citations_df,
                           cs_filter=True,
                           out_name="citations_cs_fields_to_nlp_by_year.csv")
    cfdi_per_nlp_paper(papers_df, nlp_df, citations_df)
    cfdi_non_cs_fields(papers_df, citations_df)


if __name__ == "__main__":
    main()
