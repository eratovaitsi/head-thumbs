"""
VOC Head Thombo XML Parser — transcript-only, batch loop
=========================================================
Parses PAGE XML transcripts of Dutch colonial Ceylon (Sri Lanka) Head Thombo
(census) documents into structured CSV/Excel.  No image required.

Structural detection uses only the XML transcript:
  - Y coordinate    → row order (top to bottom)
  - TextLine height → household head = tallest content line after village row
  - X coordinate    → indentation:
        head   ≈ leftmost
        family ≈ same indent zone as head
        child  ≈ indented more than head by CHILD_INDENT_DELTA pixels
  - Age matching    → right-margin-only TextLines are paired to the nearest
                      content line by Y proximity (fixes ages on separate lines)
  - Text content    → relationship words, gent., ditto, caste, group labels

Usage:
    python3 thombo_parser.py file.xml
    python3 thombo_parser.py folder/
    python3 thombo_parser.py file1.xml file2.xml --output my_output

    # Push output to GitHub after parsing:
    python3 thombo_parser.py folder/ --github https://github.com/user/repo.git

Dependencies: pip3 install pandas openpyxl gitpython
"""

import re
import sys
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

# ── PAGE XML namespace ────────────────────────────────────────────────────────
NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

# ── Layout thresholds ─────────────────────────────────────────────────────────
RIGHT_MARGIN_X     = 3500   # x_min >= this → right-margin age column
MIN_LINE_HEIGHT    = 60     # ignore TextLines shorter than this (noise)
CHILD_INDENT_DELTA = 150    # pixels more indented than head → child row
AGE_Y_THRESHOLD    = 150    # max Y distance to match an age line to content row

# ── Caste titles (extend when you have the full list) ────────────────────────
KNOWN_CASTES = {
    "bellale", "salagama", "karave", "durave", "wahumpura",
    "panna", "bathgama", "rada", "hunu", "berava", "oli",
    "navandanna", "kumbal", "paduvansinnala", "paduvanse",
}

# ── Dutch relationship words → English ───────────────────────────────────────
RELATIONSHIP_MAP = {
    "broeder":    "brother",   "broeders":   "brother",
    "zuster":     "sister",
    "zoon":       "son",       "zeon":       "son",    "zoonen": "sons",
    "dochter":    "daughter",  "dogters":    "daughters", "dogters,": "daughters",
    "vrouw":      "wife",      "krouw":      "wife",
    "man":        "husband",
    "vader":      "father",    "moeder":     "mother",
    "weduwe":     "widow",     "weduwenaar": "widower",
    "neef":       "nephew/cousin", "nicht":  "niece/cousin",
    "oom":        "uncle",     "tante":      "aunt",
    "schoonzoon": "son-in-law",
}

# ── Token classification sets ─────────────────────────────────────────────────
GROUP_WORDS   = {"zoonen","dogters","dogters,","zegnen","zegenen",
                 "kinderen","zegnen;","zoonen;","zegnen,"}
NAMED_MARKERS = {"gent.","gent:","gent","genaamd","gen.","gen:"}
DITTO_MARKS   = {",,", '""', '"', "''", "„"}
IGNORE_WORDS  = (
    {"als", "do", "--", "-", "&", "zyn"}
    | NAMED_MARKERS
    | DITTO_MARKS
    | set(RELATIONSHIP_MAP.keys())
    | GROUP_WORDS
)

RANK_RE = re.compile(r'^(\d+)\.?$')
RANK_L  = re.compile(r'^[lL]$')


# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — Parse XML
#
# Two passes:
#   Pass A: collect ALL TextLines into two buckets —
#           - content lines  (x_first < RIGHT_MARGIN_X)
#           - age-only lines (all words in right margin, typically just a number)
#   Pass B: match each age-only line to the nearest content line by Y distance
#           and attach the age value to that content row.
# ═════════════════════════════════════════════════════════════════════════════

def _get_text(word_el):
    """Return text from a Word element, supporting Unicode and PlainText tags."""
    for tag in (f"{{{NS}}}Unicode", f"{{{NS}}}PlainText"):
        el = word_el.find(f".//{tag}")
        if el is not None and (el.text or "").strip():
            return el.text.strip()
    return ""


def parse_xml(xml_path: str):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    page = root.find(f".//{{{NS}}}Page")
    img  = page.attrib.get("imageFilename", Path(xml_path).stem)

    content_rows = []   # rows with actual text content
    age_lines    = []   # right-margin-only rows (just a number)

    for tl in root.iter(f"{{{NS}}}TextLine"):
        lc = tl.find(f"{{{NS}}}Coords")
        if lc is None:
            continue

        pts    = [(int(p.split(",")[0]), int(p.split(",")[1]))
                  for p in lc.attrib["points"].split()]
        ys     = [p[1] for p in pts]
        height = max(ys) - min(ys)

        if height < MIN_LINE_HEIGHT:
            continue

        y_mid = (min(ys) + max(ys)) // 2

        words = []
        for w in tl.iter(f"{{{NS}}}Word"):
            wc   = w.find(f"{{{NS}}}Coords")
            text = _get_text(w)
            if wc is None or not text:
                continue
            wpts = [(int(p.split(",")[0]), int(p.split(",")[1]))
                    for p in wc.attrib["points"].split()]
            wxs  = [p[0] for p in wpts]
            words.append({"text": text, "x_min": min(wxs),
                          "x_mid": (min(wxs) + max(wxs)) // 2})

        if not words:
            continue

        words.sort(key=lambda w: w["x_mid"])
        all_in_margin = all(w["x_min"] >= RIGHT_MARGIN_X for w in words)

        if all_in_margin:
            # This TextLine is a pure right-margin age/number row
            raw = words[0]["text"].rstrip(".")
            if re.fullmatch(r'\d+', raw) or raw == "-":
                age_lines.append({"y_mid": y_mid, "age_raw": raw})
        else:
            # Normal content row — split off any trailing margin word
            if words[-1]["x_min"] >= RIGHT_MARGIN_X:
                raw = words[-1]["text"].rstrip(".")
                if re.fullmatch(r'\d+', raw) or raw == "-":
                    inline_age = raw if re.fullmatch(r'\d+', raw) else ""
                    words = words[:-1]
                else:
                    inline_age = ""
            else:
                inline_age = ""

            if not words:
                continue

            content_rows.append({
                "y_mid":       y_mid,
                "line_height": height,
                "x_first":     words[0]["x_min"],
                "words":       words,
                "age":         inline_age,
                "absent":      not bool(inline_age),
            })

    content_rows.sort(key=lambda r: r["y_mid"])

    # ── Match age-only lines to nearest content row by Y proximity ────────────
    for al in age_lines:
        if not content_rows:
            break
        best = min(content_rows,
                   key=lambda r: abs(r["y_mid"] - al["y_mid"]))
        if abs(best["y_mid"] - al["y_mid"]) <= AGE_Y_THRESHOLD:
            # Only attach if the content row has no age yet
            if not best["age"]:
                age_val      = al["age_raw"] if re.fullmatch(r'\d+', al["age_raw"]) else ""
                best["age"]  = age_val
                best["absent"] = not bool(age_val)

    return img, content_rows


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 — Token helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_named_as(tokens):
    for i, t in enumerate(tokens):
        if t.lower().rstrip(".:") in {m.rstrip(".:") for m in NAMED_MARKERS}:
            alias = " ".join(tokens[i + 1:]).strip(" .,;:")
            return tokens[:i], alias
    return tokens, ""


def get_relationship(tokens):
    for t in tokens:
        r = RELATIONSHIP_MAP.get(t.lower().rstrip(".,;:"))
        if r:
            return r
    return ""


def get_caste(tokens):
    for t in tokens:
        if t.lower().rstrip(".,;:") in KNOWN_CASTES:
            return t.lower().rstrip(".,;:").title()
    return ""


def is_group_row(tokens):
    content = [t for t in tokens
               if t.lower().rstrip(".,;:") not in {"als"}
               and not RANK_RE.match(t) and not RANK_L.match(t)]
    return bool(content) and all(
        t.lower().rstrip(".,;:") in GROUP_WORDS for t in content)


def build_name(tokens):
    """
    Assemble person name.
    - Strips: ignore words, relationship words, group words,
              caste words, rank numbers, ditto marks.
    - Keeps:  personal names + non-caste title words (e.g. Colomboge, Don).
    - Flags illegible entries as [illegible].
    """
    parts, illegible = [], False
    for t in tokens:
        raw     = t.strip()
        cleaned = raw.lower().rstrip(".,;:")
        if "onleesbaar" in cleaned:
            illegible = True
            continue
        if raw in DITTO_MARKS:
            continue
        if cleaned in IGNORE_WORDS:
            continue
        if cleaned in KNOWN_CASTES:
            continue
        if RANK_RE.match(raw) or RANK_L.match(raw):
            continue
        parts.append(raw)
    name = " ".join(parts).strip(" .,;:-_")
    if illegible:
        return (name + " [illegible]").strip() if name else "[illegible]"
    return name


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 — Interpret rows → person records
#
# Head detection (transcript-only):
#   Pre-pass finds the tallest TextLine after the village row.
#   In the original document the head's name is written in larger script;
#   the TextLine bounding-box height is a reliable proxy for that.
#
# Hierarchy classification:
#   head   → pre-identified tallest line
#   child  → x_first > head_x + CHILD_INDENT_DELTA
#   family → everything else
# ═════════════════════════════════════════════════════════════════════════════

def find_head_index(rows):
    village_seen = False
    best_idx, best_h = None, 0
    for i, row in enumerate(rows):
        tokens = [w["text"] for w in row["words"]]
        if tokens and tokens[0].lower() in ("torp", "dorp"):
            village_seen = True
            continue
        if not village_seen or not row["words"]:
            continue
        if is_group_row(tokens):
            continue
        if row["line_height"] > best_h:
            best_h   = row["line_height"]
            best_idx = i
    return best_idx


def parse_rows(rows, img_file):
    head_idx   = find_head_index(rows)
    records    = []
    village    = ""
    hh_rank    = ""
    head_name  = ""
    head_x     = 0
    caste      = ""
    found_head = False

    for i, row in enumerate(rows):
        tokens  = [w["text"] for w in row["words"]]
        age     = row["age"]
        absent  = row["absent"]
        x_first = row["x_first"]

        if not tokens:
            continue

        # Village row
        if tokens[0].lower() in ("torp", "dorp"):
            village    = " ".join(tokens[1:]).strip(" .,;:-")
            found_head = False
            hh_rank = head_name = caste = ""
            head_x  = 0
            continue

        # Structural group label rows — skip
        if is_group_row(tokens):
            continue

        # Ditto row → "also named", no alias carried over
        is_ditto = tokens[0] in DITTO_MARKS
        if is_ditto:
            tokens   = [t for t in tokens if t not in DITTO_MARKS]
            named_as = "gent."
        else:
            tokens, named_as = get_named_as(tokens)

        # Strip leading child-count / rank number
        rank_token = ""
        if tokens and (RANK_RE.match(tokens[0]) or RANK_L.match(tokens[0])):
            rank_token = tokens[0].rstrip(".")
            tokens     = tokens[1:]

        relationship = get_relationship(tokens)

        # Extract caste BEFORE building name so it is removed from person_name
        found_caste = get_caste(tokens)
        if found_caste:
            caste  = found_caste
            tokens = [t for t in tokens
                      if t.lower().rstrip(".,;:") != found_caste.lower()]

        person_name = build_name(tokens)
        if not person_name:
            continue

        # Classify
        if i == head_idx:
            hierarchy  = "head"
            hh_rank    = rank_token
            head_name  = person_name
            head_x     = x_first
            found_head = True
        elif found_head and x_first > head_x + CHILD_INDENT_DELTA:
            hierarchy = "child"
        else:
            hierarchy = "family"

        records.append({
            "source_file":    img_file,
            "village":        village,
            "household_rank": hh_rank,
            "head_name":      head_name,
            "caste":          caste,
            "person_name":    person_name,
            "hierarchy":      hierarchy,
            "relationship":   relationship,
            "named_as":       named_as,
            "age":            age,
            "absent":         absent,
        })

    return records


# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 — Save output files
# ═════════════════════════════════════════════════════════════════════════════

def save_outputs(df, base_path):
    out  = Path(base_path)
    csv  = out.with_suffix(".csv")
    xlsx = out.with_suffix(".xlsx")
    df.to_csv(csv, index=False, encoding="utf-8-sig")
    print(f"  CSV   → {csv}")
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Census")
        ws = writer.sheets["Census"]
        for col in ws.columns:
            w = max((len(str(c.value)) for c in col if c.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(w + 2, 45)
    print(f"  Excel → {xlsx}")
    return csv, xlsx


# ═════════════════════════════════════════════════════════════════════════════
# STEP 5 — GitHub push (optional)
# ═════════════════════════════════════════════════════════════════════════════

def push_to_github(files: list, repo_url: str, branch: str = "main",
                   commit_msg: str = "Update census output"):
    """
    Push output files + the parser script to a GitHub repository.

    Requirements:
      pip3 install gitpython
      Authenticated with GitHub (brew install gh && gh auth login)

    files    : list of Path objects to commit (CSV + Excel)
    repo_url : HTTPS or SSH URL  e.g. https://github.com/username/repo.git
    branch   : branch name (default: main)
    """
    try:
        from git import Repo, InvalidGitRepositoryError
    except ImportError:
        print("\nERROR: gitpython not installed. Run: pip3 install gitpython")
        return

    import shutil

    # Always include the parser script itself in the commit
    script_path = Path(__file__).resolve()
    all_files   = list(files) + [script_path]
    repo_dir    = Path(files[0]).parent

    # Open existing repo or initialise one
    try:
        repo = Repo(repo_dir)
        print(f"  Using existing git repo at {repo_dir}")
    except InvalidGitRepositoryError:
        print(f"  Initialising git repo in {repo_dir} ...")
        repo = Repo.init(repo_dir)

    # Set / update remote URL
    if "origin" not in [r.name for r in repo.remotes]:
        repo.create_remote("origin", repo_url)
    else:
        repo.remotes.origin.set_url(repo_url)

    # Pull latest to avoid rejection for being behind
    print(f"  Pulling latest from {branch} ...")
    try:
        repo.remotes.origin.pull(branch)
    except Exception as e:
        print(f"  Pull skipped ({e}) -- continuing")

    # Copy files into repo dir if they live elsewhere, then stage
    staged = []
    for f in all_files:
        f    = Path(f)
        dest = repo_dir / f.name
        if f.resolve() != dest.resolve():
            shutil.copy2(f, dest)
        staged.append(str(dest.resolve()))

    repo.index.add(staged)

    # Commit
    repo.index.commit(commit_msg)
    print(f"  Committed: {commit_msg}")

    # Push
    print(f"  Pushing to {repo_url} ({branch}) ...")
    repo.remotes.origin.push(refspec=f"HEAD:{branch}")
    print("  Push complete. Check your repo on GitHub.")

def run(inputs, output_base, github_url=None, branch="main"):
    xml_files = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            xml_files.extend(sorted(p.glob("*.xml")))
        elif p.suffix.lower() == ".xml" and p.exists():
            xml_files.append(p)
        else:
            print(f"WARNING: skipping '{inp}'")

    if not xml_files:
        print("ERROR: no XML files found."); sys.exit(1)

    print(f"\nProcessing {len(xml_files)} file(s):\n")
    all_records = []
    for xf in xml_files:
        img, rows = parse_xml(str(xf))
        recs      = parse_rows(rows, img)
        print(f"  {xf.name:50s} → {len(recs):>3} records")
        all_records.extend(recs)

    if not all_records:
        print("WARNING: no records extracted."); sys.exit(0)

    df = pd.DataFrame(all_records, columns=[
        "source_file", "village", "household_rank", "head_name", "caste",
        "person_name", "hierarchy", "relationship", "named_as", "age", "absent"
    ])

    print(f"\nTotal: {len(df)} records across {len(xml_files)} file(s)\n")
    print(df.to_string(index=False))
    print()

    csv_path, xlsx_path = save_outputs(df, output_base)

    if github_url:
        print(f"\nPushing to GitHub: {github_url}")
        push_to_github([csv_path, xlsx_path], github_url, branch=branch)

    print("\nDone.")


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description=(
            "Parse VOC Head Thombo PAGE XML → CSV / Excel (no image needed)\n\n"
            "Examples:\n"
            "  python3 thombo_parser.py page1.xml\n"
            "  python3 thombo_parser.py my_transcripts/\n"
            "  python3 thombo_parser.py my_transcripts/ --github https://github.com/you/repo.git\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("inputs",   nargs="+",
                    help="XML file(s) or folder(s) containing XML files")
    ap.add_argument("--output", "-o", default="thombo_output",
                    help="Output base name (no extension). Default: thombo_output")
    ap.add_argument("--github", "-g", default=None,
                    help="GitHub repo URL to push output files to after parsing")
    ap.add_argument("--branch", "-b", default="main",
                    help="Git branch to push to. Default: main")
    args = ap.parse_args()
    run(args.inputs, args.output, github_url=args.github, branch=args.branch)


if __name__ == "__main__":
    main()
