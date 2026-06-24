"""Reproduce Figure 3 of Wahle et al. 2023 from an experiment's output.

Figure 3 caption (verbatim from the paper):
    "The percentage of citations (a) from NLP to non-CS fields and
     (b) non-CS fields to NLP in relation to all non-CS citations
     from and to NLP."

X-axis: years; y-axis: % of all NLP <-> non-CS citations going
to/from each plotted field. The original paper plots the four
non-CS fields with the highest citation volume (Linguistics,
Mathematics, Psychology, Sociology); we plot the same set so
the replication and extension figures are directly comparable.

Usage:
    uv run -m src.plot figure3 -e replication
    uv run -m src.plot figure3 -e extension
    uv run -m src.plot cfdi    -e replication   # CFDI per year line plot
"""

import argparse
import os
import sys

import pandas as pd

from .config import EXPERIMENTS, experiment, experiment_dirs


def _load_field_to_nlp(experiment_name):
    """Load `citations_non_cs_fields_to_nlp_by_year.csv` for the experiment."""
    dirs = experiment_dirs(experiment_name)
    path = os.path.join(dirs["outputs"],
                        "citations_non_cs_fields_to_nlp_by_year.csv")
    if not os.path.exists(path):
        sys.exit(f"Missing {path}; run "
                 f"`uv run -m src.analysis_api -e {experiment_name}` first.")
    return pd.read_csv(path)


def _percent_per_year(df, value_col):
    """For each year, compute each field's share of the year's total."""
    totals = df.groupby("year")[value_col].sum()
    df = df.copy()
    df["pct"] = (df[value_col] / df["year"].map(totals)) * 100.0
    return df


def figure3(experiment_name, *, fields=None, save=True):
    """Two-panel plot: (a) NLP -> non-CS, (b) non-CS -> NLP, % per year."""
    import matplotlib.pyplot as plt  # imported lazily so the rest of the
                                     # pipeline doesn't require matplotlib.

    cfg = experiment(experiment_name)
    dirs = experiment_dirs(experiment_name)
    fields = fields or cfg["figure3_fields"]
    df = _load_field_to_nlp(experiment_name)

    # Restrict to non-CS rows (the CSV already excludes "Computer Science"
    # but be safe), and to the experiment's year range.
    df = df[(df["field"] != "Computer Science")
            & (df["year"] >= cfg["year_min"])
            & (df["year"] <= cfg["year_max"])]

    out = _percent_per_year(df, "nlp_to_field")
    inc = _percent_per_year(df, "field_to_nlp")

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, panel_df, title in [
        (axes[0], out, "(a) NLP → non-CS field"),
        (axes[1], inc, "(b) non-CS field → NLP"),
    ]:
        for f in fields:
            sub = (panel_df[panel_df["field"] == f]
                   .sort_values("year"))
            if sub.empty:
                continue
            ax.plot(sub["year"], sub["pct"], marker="o", label=f)
        ax.set_xlabel("year")
        ax.set_ylabel("% of NLP ↔ non-CS citations")
        ax.set_title(title)
        ax.grid(alpha=0.3)
        ax.legend(loc="best", fontsize=9)

    fig.suptitle(
        f"Figure 3 [{cfg['label']}]: NLP <-> non-CS field citation share",
        y=1.02,
    )
    fig.tight_layout()

    if save:
        os.makedirs(dirs["figures"], exist_ok=True)
        out_path = os.path.join(dirs["figures"], "figure3.png")
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        # Also save the underlying long-format table so reviewers
        # can verify the numbers without re-running the pipeline.
        long = pd.concat([
            out.assign(direction="nlp_to_field")
                .rename(columns={"nlp_to_field": "count"})
                [["year", "field", "direction", "count", "pct"]],
            inc.assign(direction="field_to_nlp")
                .rename(columns={"field_to_nlp": "count"})
                [["year", "field", "direction", "count", "pct"]],
        ])
        long.to_csv(os.path.join(dirs["outputs"], "figure3_data.csv"),
                    index=False)
        print(f"[{experiment_name}] wrote {out_path} and figure3_data.csv")
    return fig


def cfdi_plot(experiment_name, *, save=True):
    """Per-year average CFDI (incoming and outgoing) line plot."""
    import matplotlib.pyplot as plt

    cfg = experiment(experiment_name)
    dirs = experiment_dirs(experiment_name)
    path = os.path.join(dirs["outputs"], "cfdi_per_year_nlp.csv")
    if not os.path.exists(path):
        sys.exit(f"Missing {path}; run "
                 f"`uv run -m src.analysis_api -e {experiment_name}` first.")
    df = pd.read_csv(path)
    df = df[(df["year"] >= cfg["year_min"]) & (df["year"] <= cfg["year_max"])]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(df["year"], df["avg_outgoing"], marker="o", label="outgoing")
    ax.plot(df["year"], df["avg_incoming"], marker="s", label="incoming")
    ax.set_xlabel("year")
    ax.set_ylabel("CFDI (mean across NLP papers)")
    ax.set_title(f"CFDI per year [{cfg['label']}]")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()

    if save:
        os.makedirs(dirs["figures"], exist_ok=True)
        out_path = os.path.join(dirs["figures"], "cfdi_per_year.png")
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        print(f"[{experiment_name}] wrote {out_path}")
    return fig


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for cmd in ("figure3", "cfdi"):
        p = sub.add_parser(cmd)
        p.add_argument("--experiment", "-e", required=True,
                       choices=list(EXPERIMENTS) + ["all"])
    args = ap.parse_args()

    targets = list(EXPERIMENTS) if args.experiment == "all" else [args.experiment]
    for name in targets:
        if args.cmd == "figure3":
            figure3(name)
        else:
            cfdi_plot(name)


if __name__ == "__main__":
    main()
