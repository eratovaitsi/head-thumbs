

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
# XML parsing
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

        y_mid = (min(ys) + max(ys)) // 2   # 🔥 IMPORTANT ADDED

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
            "y_mid": y_mid   # 🔥 IMPORTANT ADDED
        })

    return img, rows


# ─────────────────────────────────────────────────────────────
# AGE + PERSON EXTRACTION
# ─────────────────────────────────────────────────────────────

def extract_people_and_ages(rows):
    people = []
    ages = []

    for row in rows:
        words = row["words"]
        text = " ".join(w["text"] for w in words).strip()

        if re.fullmatch(r"\d+", text):
            ages.append({
                "age": text,
                "y_mid": row["y_mid"]
            })
        else:
            people.append({
                "words": words,
                "y_mid": row["y_mid"]
            })

    return people, ages


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
# 🔥 Y-COORDINATE MATCHING (NEW CORE FIX)
# ─────────────────────────────────────────────────────────────

def match_ages(people, ages):
    used = set()
    result = []

    for p in people:

        px = sum(w["x_min"] for w in p["words"]) / len(p["words"])
        py = p["y_mid"]

        best_age = ""
        best_dist = 10**18
        best_idx = -1

        for i, a in enumerate(ages):
            if i in used:
                continue

            ax = 0  # ages usually don't have reliable x structure in your data
            ay = a["y_mid"]

            # 2D Manhattan distance (stable for OCR layouts)
            dist = abs(px - ax) + abs(py - ay)

            if dist < best_dist:
                best_dist = dist
                best_age = a["age"]
                best_idx = i

        if best_idx != -1:
            used.add(best_idx)

        result.append((p, best_age))

    return result

# ─────────────────────────────────────────────────────────────
# PARSING
# ─────────────────────────────────────────────────────────────

def parse_rows(rows, img_file):

    records = []

    village = ""
    caste = ""

    people, ages = extract_people_and_ages(rows)
    matched = match_ages(people, ages)

    for p, age in matched:

        words = p["words"]
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
# RUN
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
