import sys
import re
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

MIN_LINE_HEIGHT = 60

KNOWN_CASTES = {
    "bellale","salagama","karave","durave","wahumpura","panna","bathgama",
    "rada","hunu","berava","oli","navandanna","kumbal",
    "paduvansinnala","paduvanse",
}

RELATIONSHIP_MAP = {
    "broeder":"brother","broeders":"brother","zuster":"sister",
    "zoon":"son","zeon":"son","zoonen":"sons",
    "dochter":"daughter","dogters":"daughters","dogters,":"daughters",
    "vrouw":"wife","krouw":"wife","man":"husband",
    "vader":"father","moeder":"mother",
    "weduwe":"widow","weduwenaar":"widower",
    "neef":"nephew/cousin","nicht":"niece/cousin",
    "oom":"uncle","tante":"aunt","schoonzoon":"son-in-law",
}

GROUP_WORDS = {
    "zoonen","dogters","dogters,","zegnen","zegenen",
    "kinderen","zegnen;","zoonen;","zegnen,"
}

NAMED_MARKERS = {"gent.","gent:","gent","genaamd","gen.","gen:"}
DITTO_MARKS = {",,", '""', '"', "''"}

IGNORE_WORDS = (
    {"als", "do", "--", "-", "&", "zyn"}
    | NAMED_MARKERS
    | DITTO_MARKS
    | set(RELATIONSHIP_MAP.keys())
    | GROUP_WORDS
)

RANK_RE = re.compile(r'^(\d+)\.?$')
RANK_L = re.compile(r'^[lL]$')


# ─────────────────────────────────────────────────────────────
# XML parsing (WITH Y + X)
# ─────────────────────────────────────────────────────────────

def parse_xml(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    page = root.find(f".//{{{NS}}}Page")
    img = page.attrib.get("imageFilename", Path(xml_path).stem)

    rows = []

    for tl in root.iter(f"{{{NS}}}TextLine"):
        lc = tl.find(f"{{{NS}}}Coords")
        if lc is None:
            continue

        pts = [(int(p.split(",")[0]), int(p.split(",")[1]))
               for p in lc.attrib["points"].split()]
        ys = [p[1] for p in pts]

        height = max(ys) - min(ys)
        if height < MIN_LINE_HEIGHT:
            continue

        y_mid = sum(ys) // len(ys)

        words = []

        for w in tl.iter(f"{{{NS}}}Word"):
            wc = w.find(f"{{{NS}}}Coords")

            wt = (
                w.find(f".//{{{NS}}}Unicode")
                or w.find(f".//{{{NS}}}PlainText")
            )

            text = (wt.text or "").strip() if wt is not None else ""

            if wc is None or not text:
                continue

            wpts = [(int(p.split(",")[0]), int(p.split(",")[1]))
                    for p in wc.attrib["points"].split()]
            wxs = [p[0] for p in wpts]

            words.append({
                "text": text,
                "x_min": min(wxs),
            })

        if not words:
            continue

        rows.append({
            "words": words,
            "y_mid": y_mid
        })

    return img, rows


# ─────────────────────────────────────────────────────────────
# BLOCK SPLITTING
# ─────────────────────────────────────────────────────────────

def split_into_blocks(rows, gap_threshold=250):

    if not rows:
        return []

    rows_sorted = sorted(rows, key=lambda r: r["y_mid"])

    blocks = []
    current = [rows_sorted[0]]

    for prev, curr in zip(rows_sorted, rows_sorted[1:]):
        if abs(curr["y_mid"] - prev["y_mid"]) > gap_threshold:
            blocks.append(current)
            current = []

        current.append(curr)

    blocks.append(current)
    return blocks


# ─────────────────────────────────────────────────────────────
# COLUMN SPLIT + AGE DETECTION (FIXED)
# ─────────────────────────────────────────────────────────────

def split_people_and_ages(rows):

    all_x = [w["x_min"] for r in rows for w in r["words"]]
    if not all_x:
        return [], []

    # ✅ FIXED: real left/right split
    split_x = (min(all_x) + max(all_x)) / 2

    people = []
    ages = []

    for row in rows:
        words = row["words"]
        text = " ".join(w["text"] for w in words).strip()
        avg_x = sum(w["x_min"] for w in words) / len(words)

        # ✅ FIXED: robust number extraction
        cleaned = re.sub(r"[^\d]", "", text)

        if cleaned.isdigit() and avg_x > split_x:
            ages.append({
                "age": cleaned,
                "y": row["y_mid"],
                "x": avg_x
            })
        else:
            people.append({
                "row": row,
                "y": row["y_mid"],
                "x": avg_x
            })

    return people, ages


# ─────────────────────────────────────────────────────────────
# MATCHING
# ─────────────────────────────────────────────────────────────

def match_ages(people, ages, max_dist=200):

    results = []

    for p in people:
        best_age = ""
        best_score = float("inf")

        for a in ages:
            dy = abs(p["y"] - a["y"])
            dx = abs(p["x"] - a["x"])

            score = dy + dx * 0.5

            if score < best_score:
                best_score = score
                best_age = a["age"]

        if best_score > max_dist:
            best_age = ""

        results.append(best_age)

    return results


# ─────────────────────────────────────────────────────────────

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


def build_name(tokens):
    parts = []

    for t in tokens:
        raw = t.strip()
        cleaned = raw.lower().rstrip(".,;:")

        if raw in DITTO_MARKS:
            continue
        if cleaned in IGNORE_WORDS:
            continue
        if RANK_RE.match(raw) or RANK_L.match(raw):
            continue

        parts.append(raw)

    return " ".join(parts).strip()


# ─────────────────────────────────────────────────────────────
# PARSING
# ─────────────────────────────────────────────────────────────

def parse_rows(rows, img_file):

    records = []

    village = ""
    caste = ""

    blocks = split_into_blocks(rows)

    for block in blocks:

        people, ages = split_people_and_ages(block)
        matched_ages = match_ages(people, ages)

        for i, p in enumerate(people):

            words = p["row"]["words"]
            token_texts = [w["text"] for w in words]

            if not token_texts:
                continue

            if token_texts[0].lower() in ("torp", "dorp"):
                village = " ".join(token_texts[1:])
                continue

            caste_found = get_caste(token_texts)
            if caste_found:
                caste = caste_found

            relationship = get_relationship(token_texts)
            person_name = build_name(token_texts)

            if not person_name:
                continue

            age = matched_ages[i] if i < len(matched_ages) else ""

            records.append({
                "source_file": img_file,
                "village": village,
                "person_name": person_name,
                "relationship": relationship,
                "caste": caste,
                "age": age,
                "absent": not bool(age),
            })

    return records


# ─────────────────────────────────────────────────────────────

def run(inputs, output):
    xml_files = []

    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            xml_files.extend(p.glob("*.xml"))
        elif p.suffix == ".xml":
            xml_files.append(p)

    all_records = []

    for f in xml_files:
        img, rows = parse_xml(f)
        all_records.extend(parse_rows(rows, img))

    df = pd.DataFrame(all_records)

    df.to_csv(output + ".csv", index=False)
    df.to_excel(output + ".xlsx", index=False)

    print(df.head(20))
    print("\nDone.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--output", default="thombo_output")

    args = parser.parse_args()
    run(args.inputs, args.output)


if __name__ == "__main__":
    main()
