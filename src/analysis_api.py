"""Analyse Graph-API output (`data/raw/`) and produce CSVs.

These CSVs have the same column names as the bulk-pipeline outputs in
`analysis.py`, so you can swap pipelines without touching downstream
plotting code.

The metric definitions match Wahle et al. exactly:
* NLP paper       <=> S2 record with non-null `externalIds.ACL`
* field of paper  <=> deduped `s2FieldsOfStudy`
                       (drop `external` if same category exists `internal`)
* CFDI            <=> 1 - sum(p_i^2) over field-of-study counts
* self-citation   <=> NLP -> NLP / total NLP outgoing citations
"""

import json
import os
from collections import Counter, defaultdict
from glob import glob

import numpy as np
import pandas as pd
from tqdm import tqdm

from .config import DATA_DIR, OUTPUTS_DIR, YEAR_MAX, YEAR_MIN

RAW = os.path.join(DATA_DIR, "raw")
PAPERS_DIR = os.path.join(RAW, "papers")
REFS_DIR = os.path.join(RAW, "refs")
CITS_DIR = os.path.join(RAW, "cits")


# --- Helpers (same as bulk path) ---------------------------------------

def filter_s2fos(s2fos):
    if not s2fos:
        return []
    out = []
    for f in s2fos:
        if f is None:
            continue
        if f.get("source") == "external" and any(
            o and o.get("category") == f["category"] and o.get("source") != "external"
            for o in s2fos
        ):
            continue
        if f not in out:
            out.append(f)
    return out


def categories(s2fos):
    return [f["category"] for f in filter_s2fos(s2fos) if f and f.get("category")]


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


# --- Loading -----------------------------------------------------------

def load_focal_papers():
    """Return {corpus_id: paper_meta_dict} for all NLP papers we fetched."""
    out = {}
    for p in tqdm(sorted(glob(os.path.join(PAPERS_DIR, "*.json"))),
                  desc="load NLP papers"):
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        if not d.get("corpusId"):
            continue
        if d.get("year") is None or not (YEAR_MIN <= d["year"] <= YEAR_MAX):
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

def general_stats(focal):
    """Per-year NLP papers / out-refs / in-cits / median fields."""
    yearly = defaultdict(lambda: {
        "nlp_papers": 0,
        "out_citations": 0,
        "in_citations": 0,
        "out_to_nlp": 0,
        "fields_per_cited": [],
        "fields_per_citing": [],
    })

    for cid, p in focal.items():
        y = p["year"]
        yearly[y]["nlp_papers"] += 1

        ref_path = os.path.join(REFS_DIR, f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                yearly[y]["out_citations"] += 1
                cited_cats = categories(r.get("cited_s2fos"))
                if cited_cats:
                    yearly[y]["fields_per_cited"].append(len(cited_cats))
                if is_nlp(r.get("cited_externalids")):
                    yearly[y]["out_to_nlp"] += 1

        cit_path = os.path.join(CITS_DIR, f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for c in iter_jsonl(cit_path):
                yearly[y]["in_citations"] += 1
                citing_cats = categories(c.get("citing_s2fos"))
                if citing_cats:
                    yearly[y]["fields_per_citing"].append(len(citing_cats))

    rows = []
    self_rows = []
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

    pd.DataFrame(rows).to_csv(_p("general_stats.csv"), index=False)
    pd.DataFrame(self_rows).to_csv(_p("nlp_self_citations.csv"), index=False)


def _p(name):
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    return os.path.join(OUTPUTS_DIR, name)


def papers_per_field(focal):
    """For comparability with `general_stats_papers_per_field.csv`,
    we count over all *cited* papers we observed (since we only
    know the FoS of papers we touched via S2)."""
    counter = Counter()
    for cid in focal:
        for path in (os.path.join(REFS_DIR, f"{cid}.jsonl"),
                     os.path.join(CITS_DIR, f"{cid}.jsonl")):
            if not os.path.exists(path):
                continue
            for d in iter_jsonl(path):
                fos = d.get("cited_s2fos") or d.get("citing_s2fos")
                for c in categories(fos):
                    counter[c] += 1
    df = (pd.DataFrame(counter.most_common(), columns=["category", "count"]))
    df.to_csv(_p("general_stats_papers_per_field.csv"), index=False)


def _select_fields(s2fos, cs_only):
    """Mirror the upstream split: non-CS view = top-level S2 categories
    other than 'Computer Science'; CS-subfield view = the same cats but
    only from papers also tagged 'Computer Science', with the
    'Computer Science' entry itself dropped (so we end up with the CS
    subfields only)."""
    cats = categories(s2fos)
    if cs_only:
        if "Computer Science" not in cats:
            return []
        return [c for c in cats if c != "Computer Science"]
    return [c for c in cats if c != "Computer Science"]


def field_to_nlp(focal, *, cs_only, out_name):
    """Per (year, field) NLP <-> field citation counts."""
    table = defaultdict(lambda: {
        "nlp_to_field": 0, "field_to_nlp": 0,
        "nlp_papers": set(), "nlp_cited_papers": set(),
    })
    for cid, p in focal.items():
        y = p["year"]
        ref_path = os.path.join(REFS_DIR, f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in _select_fields(r.get("cited_s2fos"), cs_only):
                    table[(y, c)]["nlp_to_field"] += 1
                    table[(y, c)]["nlp_papers"].add(cid)
        cit_path = os.path.join(CITS_DIR, f"{cid}.jsonl")
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
    pd.DataFrame(rows).sort_values(["year", "field"]).to_csv(_p(out_name), index=False)


def cfdi_per_paper(focal):
    """Per-paper incoming and outgoing CFDI."""
    rows = []
    for cid, p in focal.items():
        in_counter = Counter()
        out_counter = Counter()
        ref_path = os.path.join(REFS_DIR, f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in categories(r.get("cited_s2fos")):
                    out_counter[c] += 1
        cit_path = os.path.join(CITS_DIR, f"{cid}.jsonl")
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
    df.to_csv(_p("nlp_papers_diversity.csv"), index=False)

    yearly = (df.groupby("year").agg(
        avg_incoming=("incoming_diversity", "mean"),
        avg_outgoing=("outgoing_diversity", "mean"),
        median_incoming=("incoming_diversity", "median"),
        median_outgoing=("outgoing_diversity", "median"),
        n_papers=("corpusid", "count"),
    ).reset_index())
    yearly.to_csv(_p("cfdi_per_year_nlp.csv"), index=False)


def cfdi_aggregated(focal):
    """Whole-window CFDI computed from the *aggregated* field counts
    (matches the headline 'CFDI = X' numbers in the paper)."""
    in_counter = Counter()
    out_counter = Counter()
    for cid in focal:
        ref_path = os.path.join(REFS_DIR, f"{cid}.jsonl")
        if os.path.exists(ref_path):
            for r in iter_jsonl(ref_path):
                for c in categories(r.get("cited_s2fos")):
                    out_counter[c] += 1
        cit_path = os.path.join(CITS_DIR, f"{cid}.jsonl")
        if os.path.exists(cit_path):
            for r in iter_jsonl(cit_path):
                for c in categories(r.get("citing_s2fos")):
                    in_counter[c] += 1

    pd.DataFrame([
        {"direction": "outgoing", "cfdi": cfdi(out_counter.values()),
         "n": sum(out_counter.values()), "fields": len(out_counter)},
        {"direction": "incoming", "cfdi": cfdi(in_counter.values()),
         "n": sum(in_counter.values()), "fields": len(in_counter)},
    ]).to_csv(_p("cfdi_window_aggregated.csv"), index=False)

    rows = []
    for c, n in out_counter.most_common():
        rows.append({"direction": "outgoing", "field": c, "count": n})
    for c, n in in_counter.most_common():
        rows.append({"direction": "incoming", "field": c, "count": n})
    pd.DataFrame(rows).to_csv(_p("nlp_field_distribution.csv"), index=False)


# --- Main --------------------------------------------------------------

def main():
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    focal = load_focal_papers()
    print(f"NLP focal papers loaded: {len(focal):,}")
    if not focal:
        raise SystemExit("No NLP papers found - run fetch_acl + fetch_graph first.")

    papers_per_field(focal)
    general_stats(focal)
    field_to_nlp(focal, cs_only=False,
                 out_name="citations_non_cs_fields_to_nlp_by_year.csv")
    field_to_nlp(focal, cs_only=True,
                 out_name="citations_cs_fields_to_nlp_by_year.csv")
    cfdi_per_paper(focal)
    cfdi_aggregated(focal)
    print(f"Wrote outputs to {OUTPUTS_DIR}")


if __name__ == "__main__":
    main()
