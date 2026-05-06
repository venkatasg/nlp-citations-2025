"""Run the citation-flow / CFDI analysis for one experiment.

Usage:
    python -m src.analysis_api -e replication
    python -m src.analysis_api -e extension

Reads `data/<experiment>/raw/{papers,refs,cits}` and writes
`outputs/<experiment>/*.csv`. The metric definitions match Wahle et
al. (EMNLP 2023) exactly:

* NLP paper       <=> S2 record with non-null `externalIds.ACL`
* field of paper  <=> deduped `s2FieldsOfStudy` (drop `external` if
                       same category exists `internal`)
* CFDI            <=> 1 - sum(p_i^2) over field-of-study counts
* self-citation   <=> NLP -> NLP / total NLP outgoing citations
"""

import argparse
import json
import os
from collections import Counter, defaultdict
from glob import glob

import numpy as np
import pandas as pd
from tqdm import tqdm

from .config import EXPERIMENTS, experiment, experiment_dirs


# --- Helpers -----------------------------------------------------------

def filter_s2fos(s2fos):
    if not s2fos:
        return []
    out = []
    for f in s2fos:
        if f is None:
            continue
        if f.get("source") == "external" and any(
            o and o.get("category") == f["category"]
            and o.get("source") != "external"
            for o in s2fos
        ):
            continue
        if f not in out:
            out.append(f)
    return out


def categories(s2fos):
    return [f["category"] for f in filter_s2fos(s2fos)
            if f and f.get("category")]


def is_nlp(externalids):
    return bool(externalids and externalids.get("ACL"))


def cfdi(counts):
    if not counts:
        return float("nan")
    arr = np.asarray(list(counts), dtype=float)
    n = arr.sum()
    if n <= 0:
        return float("nan")
    p = arr / n
    return float(1.0 - np.sum(p * p))


# --- Experiment context ------------------------------------------------

class Ctx:
    """Holds per-experiment paths and the year window."""
    def __init__(self, name):
        cfg = experiment(name)
        self.name = name
        self.year_min = cfg["year_min"]
        self.year_max = cfg["year_max"]
        self.dirs = experiment_dirs(name)
        os.makedirs(self.dirs["outputs"], exist_ok=True)

    def papers_dir(self): return self.dirs["papers_dir"]
    def refs_dir(self):   return self.dirs["refs_dir"]
    def cits_dir(self):   return self.dirs["cits_dir"]

    def out(self, name):
        return os.path.join(self.dirs["outputs"], name)


# --- Loading -----------------------------------------------------------

def load_focal_papers(ctx):
    """Return {corpus_id: paper_meta_dict} for the experiment's focal set."""
    out = {}
    paths = sorted(glob(os.path.join(ctx.papers_dir(), "*.json")))
    if not paths:
        raise SystemExit(
            f"No paper metadata in {ctx.papers_dir()}. "
            f"Run `python -m src.fetch_graph metadata -e {ctx.name}`.")
    for p in tqdm(paths, desc=f"{ctx.name}/load"):
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        if not d.get("corpusId") or d.get("year") is None:
            continue
        if not (ctx.year_min <= d["year"] <= ctx.year_max):
            continue
        out[str(d["corpusId"])] = d
    return out


def iter_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


# --- Analyses ----------------------------------------------------------

def general_stats(ctx, focal):
    """Per-year NLP papers / out-refs / in-cits / median fields, plus
    NLP self-citation rate."""
    yearly = defaultdict(lambda: {
        "nlp_papers": 0, "out_citations": 0, "in_citations": 0,
        "out_to_nlp": 0,
        "fields_per_cited": [], "fields_per_citing": [],
    })

    for cid, p in focal.items():
        y = p["year"]
        yearly[y]["nlp_papers"] += 1

        ref_path = os.path.join(ctx.refs_dir(), f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                yearly[y]["out_citations"] += 1
                cited_cats = categories(r.get("cited_s2fos"))
                if cited_cats:
                    yearly[y]["fields_per_cited"].append(len(cited_cats))
                if is_nlp(r.get("cited_externalids")):
                    yearly[y]["out_to_nlp"] += 1

        cit_path = os.path.join(ctx.cits_dir(), f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for c in iter_jsonl(cit_path):
                yearly[y]["in_citations"] += 1
                citing_cats = categories(c.get("citing_s2fos"))
                if citing_cats:
                    yearly[y]["fields_per_citing"].append(len(citing_cats))

    rows, self_rows = [], []
    for y in sorted(yearly):
        s = yearly[y]
        rows.append({
            "year": y,
            "nlp_papers": s["nlp_papers"],
            "out_citations": s["out_citations"],
            "in_citations": s["in_citations"],
            "median_fields_per_cited": (np.median(s["fields_per_cited"])
                                        if s["fields_per_cited"] else None),
            "avg_fields_per_cited": (np.mean(s["fields_per_cited"])
                                     if s["fields_per_cited"] else None),
            "median_fields_per_citing": (np.median(s["fields_per_citing"])
                                         if s["fields_per_citing"] else None),
            "avg_fields_per_citing": (np.mean(s["fields_per_citing"])
                                      if s["fields_per_citing"] else None),
        })
        self_rows.append({
            "year": y,
            "out_total": s["out_citations"],
            "out_to_nlp": s["out_to_nlp"],
            "self_cite_rate": (s["out_to_nlp"] / s["out_citations"]
                               if s["out_citations"] else None),
        })

    pd.DataFrame(rows).to_csv(ctx.out("general_stats.csv"), index=False)
    pd.DataFrame(self_rows).to_csv(ctx.out("nlp_self_citations.csv"),
                                   index=False)


def papers_per_field(ctx, focal):
    counter = Counter()
    for cid in focal:
        for path in (os.path.join(ctx.refs_dir(), f"{cid}.jsonl"),
                     os.path.join(ctx.cits_dir(), f"{cid}.jsonl")):
            if not os.path.exists(path):
                continue
            for d in iter_jsonl(path):
                fos = d.get("cited_s2fos") or d.get("citing_s2fos")
                for c in categories(fos):
                    counter[c] += 1
    pd.DataFrame(counter.most_common(), columns=["category", "count"]).to_csv(
        ctx.out("general_stats_papers_per_field.csv"), index=False)


def _select_fields(s2fos, cs_only):
    cats = categories(s2fos)
    if cs_only:
        if "Computer Science" not in cats:
            return []
    return [c for c in cats if c != "Computer Science"]


def field_to_nlp(ctx, focal, *, cs_only, out_name):
    """Per (year, field) NLP <-> field citation counts."""
    table = defaultdict(lambda: {
        "nlp_to_field": 0, "field_to_nlp": 0,
        "nlp_papers": set(), "nlp_cited_papers": set(),
    })
    for cid, p in focal.items():
        y = p["year"]
        ref_path = os.path.join(ctx.refs_dir(), f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in _select_fields(r.get("cited_s2fos"), cs_only):
                    table[(y, c)]["nlp_to_field"] += 1
                    table[(y, c)]["nlp_papers"].add(cid)
        cit_path = os.path.join(ctx.cits_dir(), f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for r in iter_jsonl(cit_path):
                for c in _select_fields(r.get("citing_s2fos"), cs_only):
                    table[(y, c)]["field_to_nlp"] += 1
                    table[(y, c)]["nlp_cited_papers"].add(cid)

    rows = []
    for (y, c), v in table.items():
        rows.append({
            "year": y, "field": c,
            "nlp_to_field": v["nlp_to_field"],
            "field_to_nlp": v["field_to_nlp"],
            "nlp_papers": len(v["nlp_papers"]),
            "nlp_cited_papers": len(v["nlp_cited_papers"]),
        })
    (pd.DataFrame(rows).sort_values(["year", "field"])
        .to_csv(ctx.out(out_name), index=False))


def cfdi_per_paper(ctx, focal):
    rows = []
    for cid, p in focal.items():
        in_counter, out_counter = Counter(), Counter()
        ref_path = os.path.join(ctx.refs_dir(), f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in categories(r.get("cited_s2fos")):
                    out_counter[c] += 1
        cit_path = os.path.join(ctx.cits_dir(), f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for r in iter_jsonl(cit_path):
                for c in categories(r.get("citing_s2fos")):
                    in_counter[c] += 1
        rows.append({
            "corpusid": cid,
            "year": p["year"],
            "title": p.get("title"),
            "citationcount": p.get("citationCount"),
            "incoming_diversity": cfdi(in_counter.values()) if in_counter else None,
            "outgoing_diversity": cfdi(out_counter.values()) if out_counter else None,
            "incoming_n": sum(in_counter.values()),
            "outgoing_n": sum(out_counter.values()),
        })
    df = pd.DataFrame(rows)
    df.to_csv(ctx.out("nlp_papers_diversity.csv"), index=False)

    yearly = (df.groupby("year").agg(
        avg_incoming=("incoming_diversity", "mean"),
        avg_outgoing=("outgoing_diversity", "mean"),
        median_incoming=("incoming_diversity", "median"),
        median_outgoing=("outgoing_diversity", "median"),
        n_papers=("corpusid", "count"),
    ).reset_index())
    yearly.to_csv(ctx.out("cfdi_per_year_nlp.csv"), index=False)


def cfdi_aggregated(ctx, focal):
    """Window-level CFDI from aggregated field counts (the headline
    'CFDI = X' numbers in the paper)."""
    in_counter, out_counter = Counter(), Counter()
    for cid in focal:
        ref_path = os.path.join(ctx.refs_dir(), f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in categories(r.get("cited_s2fos")):
                    out_counter[c] += 1
        cit_path = os.path.join(ctx.cits_dir(), f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for r in iter_jsonl(cit_path):
                for c in categories(r.get("citing_s2fos")):
                    in_counter[c] += 1

    pd.DataFrame([
        {"direction": "outgoing", "cfdi": cfdi(out_counter.values()),
         "n": sum(out_counter.values()), "fields": len(out_counter)},
        {"direction": "incoming", "cfdi": cfdi(in_counter.values()),
         "n": sum(in_counter.values()), "fields": len(in_counter)},
    ]).to_csv(ctx.out("cfdi_window_aggregated.csv"), index=False)

    rows = []
    for c, n in out_counter.most_common():
        rows.append({"direction": "outgoing", "field": c, "count": n})
    for c, n in in_counter.most_common():
        rows.append({"direction": "incoming", "field": c, "count": n})
    pd.DataFrame(rows).to_csv(
        ctx.out("nlp_field_distribution.csv"), index=False)


# --- Main --------------------------------------------------------------

def run(experiment_name):
    ctx = Ctx(experiment_name)
    focal = load_focal_papers(ctx)
    print(f"[{ctx.name}] focal NLP papers in "
          f"[{ctx.year_min}, {ctx.year_max}]: {len(focal):,}")
    if not focal:
        raise SystemExit(f"No focal papers for {ctx.name}.")

    papers_per_field(ctx, focal)
    general_stats(ctx, focal)
    field_to_nlp(ctx, focal, cs_only=False,
                 out_name="citations_non_cs_fields_to_nlp_by_year.csv")
    field_to_nlp(ctx, focal, cs_only=True,
                 out_name="citations_cs_fields_to_nlp_by_year.csv")
    cfdi_per_paper(ctx, focal)
    cfdi_aggregated(ctx, focal)
    print(f"[{ctx.name}] wrote outputs to {ctx.dirs['outputs']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment", "-e", required=True,
                    choices=list(EXPERIMENTS) + ["all"])
    args = ap.parse_args()
    targets = list(EXPERIMENTS) if args.experiment == "all" else [args.experiment]
    for name in targets:
        run(name)


if __name__ == "__main__":
    main()
