"""Build the list of NLP papers from ACL Anthology metadata.

Wahle et al. defined "NLP paper" as any S2 record whose
`externalids.ACL` is non-null. We replicate that exactly by reading
ACL Anthology's bibtex dump and emitting one row per Anthology entry
(Anthology IDs are exactly what S2 stores under `externalIds.ACL`).

Per-experiment output: writes
`data/<experiment>/acl_papers.jsonl` filtered to that experiment's
year window.
"""

import argparse
import gzip
import json
import os
import re
import sys

import requests
from tqdm import tqdm

from .config import ACL_BIB_URL, DATA_DIR, EXPERIMENTS, experiment, experiment_dirs


def _download_bib(target):
    if os.path.exists(target):
        return target
    os.makedirs(os.path.dirname(target), exist_ok=True)
    print(f"Downloading {ACL_BIB_URL} -> {target}")
    with requests.get(ACL_BIB_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with (
            open(target, "wb") as f,
            tqdm(
                total=total, unit="iB", unit_scale=True, desc="anthology.bib.gz"
            ) as bar,
        ):
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                bar.update(len(chunk))
    return target


def parse_bib(stream):
    """Yield dicts for each @entry. Robust to nested braces in fields."""
    text = stream.read().decode("utf-8", errors="replace")
    # The dump is large (~150 MB). We scan with a hand-rolled parser
    # because bibtexparser is slow for files this size.
    i, n = 0, len(text)
    while i < n:
        if text[i] != "@":
            i += 1
            continue
        # parse entry type
        j = text.find("{", i)
        if j < 0:
            return
        etype = text[i + 1 : j].strip().lower()
        i = j + 1
        # parse key
        k = text.find(",", i)
        if k < 0:
            return
        key = text[i:k].strip()
        i = k + 1
        # parse fields up to matching closing brace
        depth = 1
        start = i
        while i < n and depth > 0:
            c = text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        body = text[start:i]
        i += 1  # consume the closing brace
        # split body into fields
        fields = {}
        for m in re.finditer(
            r"(\w+)\s*=\s*(\{(?:[^{}]|\{[^{}]*\})*\}|\"[^\"]*\")",
            body,
        ):
            name = m.group(1).lower()
            val = m.group(2)
            if val.startswith("{") and val.endswith("}"):
                val = val[1:-1]
            elif val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            fields[name] = val.strip()
        if etype in {"comment", "string", "preamble"}:
            continue
        yield {"type": etype, "key": key, **fields}


def _year_of(entry):
    y = entry.get("year")
    if not y:
        return None
    m = re.search(r"\d{4}", y)
    return int(m.group(0)) if m else None


_URL_RE = re.compile(r"aclanthology\.org/([^/]+)")


def _acl_id_of(entry):
    """The Anthology paper id (URL slug) is what S2 stores under
    `externalIds.ACL`. Bib keys are author-year-title style and are NOT
    interchangeable with the Anthology id."""
    url = entry.get("url", "")
    m = _URL_RE.search(url)
    return m.group(1) if m else None


# Front matter (prefaces, journal volume headers, program-chairs reports)
# does not have a real S2 record or citation graph and would otherwise
# inflate the unresolved-papers count. Detect by:
#   - acl_id ending in `.0` (Anthology convention for volume headers,
#     e.g. `2025.tal-1.0` -> "Traitement Automatique des Langues, Volume 66")
#   - title that is exactly or starts with a frontmatter phrase, after
#     stripping latex-style braces / backslashes.
_FRONTMATTER_PREFIXES = (
    "preface",
    "proceedings of",
    "front matter",
    "foreword",
    "program chairs report",
)
_FRONTMATTER_EXACT = {"preface", "foreword", "front matter"}
# After stripping latex `{}\` markers, also drop any non-alphanumeric
# characters so accent residues (`Pr'eface` <- `Pr{\'e}face`) and curly
# apostrophes (`Chairs’`) do not block a prefix match.
_NORMALISE_RE = re.compile(r"[^a-z0-9 ]+")
_LATEX_RE = re.compile(r"[{}\\]")


def _normalise_title(title):
    if not title:
        return ""
    t = _LATEX_RE.sub("", title).lower()
    t = _NORMALISE_RE.sub("", t)
    return " ".join(t.split())


def _is_frontmatter(acl_id, title):
    if acl_id.endswith(".0"):
        return True
    t = _normalise_title(title)
    if not t:
        return False
    if t in _FRONTMATTER_EXACT:
        return True
    return t.startswith(_FRONTMATTER_PREFIXES)


def build_paper_list(experiment_name):
    cfg = experiment(experiment_name)
    dirs = experiment_dirs(experiment_name)
    os.makedirs(dirs["data_root"], exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    bib_gz = os.path.join(DATA_DIR, "anthology+abstracts.bib.gz")
    _download_bib(bib_gz)

    ymin, ymax = cfg["year_min"], cfg["year_max"]
    n_total = n_kept = n_frontmatter = 0
    with (
        gzip.open(bib_gz, "rb") as gz,
        open(dirs["acl_list"], "w", encoding="utf-8") as out,
    ):
        for entry in parse_bib(gz):
            n_total += 1
            year = _year_of(entry)
            if year is None or not (ymin <= year <= ymax):
                continue
            if entry["type"] in {"proceedings"}:
                continue
            acl_id = _acl_id_of(entry)
            if not acl_id:
                continue
            if _is_frontmatter(acl_id, entry.get("title")):
                n_frontmatter += 1
                continue
            row = {
                "acl_id": acl_id,  # URL slug; matches S2 externalIds.ACL
                "bib_key": entry["key"],
                "year": year,
                "title": entry.get("title"),
                "venue": entry.get("booktitle") or entry.get("journal"),
                "doi": entry.get("doi"),
                "url": entry.get("url"),
            }
            out.write(json.dumps(row) + "\n")
            n_kept += 1

    print(
        f"[{experiment_name}] parsed {n_total:,} entries; "
        f"kept {n_kept:,} in {ymin}-{ymax} "
        f"(skipped {n_frontmatter:,} frontmatter)"
    )
    print(f"[{experiment_name}] wrote {dirs['acl_list']}")
    return n_kept


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--experiment",
        "-e",
        choices=list(EXPERIMENTS) + ["all"],
        default="all",
        help="Experiment to build the paper list for; 'all' (default) builds both.",
    )
    args = ap.parse_args()
    targets = list(EXPERIMENTS) if args.experiment == "all" else [args.experiment]
    for name in targets:
        build_paper_list(name)


if __name__ == "__main__":
    sys.exit(main())
