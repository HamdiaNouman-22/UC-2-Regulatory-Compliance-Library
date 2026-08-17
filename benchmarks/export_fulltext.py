"""Read a crawl workbook the way `promote.py` does, and write what Excel cannot show.

WHY THIS EXISTS

A workbook cell tops out at 32,767 characters, so `ExcelRepo` keeps anything
longer in a `<workbook>.fulltext.json` sidecar and leaves a 32k preview plus a
marker in the cell. That is correct for the database path — `promote.py`
rehydrates from the sidecar — but it means a PERSON reading the workbook sees a
document that stops mid-sentence. GOSI made it obvious: a 92,995-character
instrument displayed as 32,028 characters, ending inside Article 26.

This exports the full values as files you can actually open:

    <out>/<Regulator>/0001_<title>.html    the document HTML, browser-openable
    <out>/<Regulator>/index.md            one row per document, with lengths

    python benchmarks/export_fulltext.py GOSI-SI
    python benchmarks/export_fulltext.py GOSI-SI GOSI-Saned MOE
    python benchmarks/export_fulltext.py --all

It reads through `resolve_overflow()`, so a value is full length here whether it
fitted in the cell or not. Rows with no HTML are listed with `0` rather than
skipped — an empty document is a finding, not a gap in the report.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

CRAWL_DIR = REPO_ROOT / "output" / "formfill" / "_orch_runs" / "crawl"
DEFAULT_OUT = REPO_ROOT / "output" / "fulltext"


def _slug(s: str, n: int = 60) -> str:
    s = re.sub(r"[^\w\s-]", "", str(s or "untitled")).strip()
    return (re.sub(r"[\s_]+", "-", s).lower() or "untitled")[:n]


def export(label: str, out_root: Path) -> dict:
    import pandas as pd
    from dynamic_crawler.formfill.excel_repo import resolve_overflow

    wb = CRAWL_DIR / f"{label}.xlsx"
    if not wb.exists():
        print(f"  {label:<12} no workbook at {wb}")
        return {}

    sidecar = {}
    sc = wb.with_suffix(".fulltext.json")
    if sc.exists():
        sidecar = json.loads(sc.read_text(encoding="utf-8"))

    df = pd.read_excel(wb, sheet_name="regulations")
    rows = df.where(df.notna(), None).to_dict("records")

    out_dir = out_root / label
    out_dir.mkdir(parents=True, exist_ok=True)

    index = [f"# {label} — full document text",
             "",
             f"Workbook: `{wb.relative_to(REPO_ROOT)}`",
             f"Sidecar : `{sc.relative_to(REPO_ROOT)}`"
             + ("" if sc.exists() else "  (none — nothing overflowed)"),
             "",
             "| # | title | chars in Excel | chars in full | file |",
             "|---|---|---|---|---|"]

    written = truncated_in_excel = empty = 0
    for i, r in enumerate(rows, 1):
        title = str(r.get("title") or "untitled")
        cell = r.get("document_html")
        cell_len = len(cell) if isinstance(cell, str) else 0
        full = resolve_overflow(cell, sidecar)
        full = full if isinstance(full, str) else ""

        if not full:
            empty += 1
            index.append(f"| {i} | {title} | 0 | **0 — NO HTML** | — |")
            continue

        name = f"{i:04d}_{_slug(title)}.html"
        (out_dir / name).write_text(full, encoding="utf-8")
        written += 1
        if len(full) > cell_len:
            truncated_in_excel += 1
        flag = f"**{len(full):,}**" if len(full) > cell_len else f"{len(full):,}"
        index.append(f"| {i} | {title} | {cell_len:,} | {flag} | [{name}]({name}) |")

    index += ["",
              f"- {written} document(s) written",
              f"- {truncated_in_excel} were TRUNCATED in the workbook and are "
              f"restored here in full",
              f"- {empty} row(s) carry no HTML at all"]
    (out_dir / "index.md").write_text("\n".join(index) + "\n", encoding="utf-8")

    print(f"  {label:<12} {written:>3} file(s), {truncated_in_excel} restored from "
          f"sidecar, {empty} empty -> {out_dir.relative_to(REPO_ROOT)}")
    return {"written": written, "restored": truncated_in_excel, "empty": empty}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("labels", nargs="*", help="workbook names, e.g. GOSI-SI MOE")
    ap.add_argument("--all", action="store_true", help="every crawl workbook")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    a = ap.parse_args()

    labels = a.labels
    if a.all or not labels:
        labels = sorted(p.stem for p in CRAWL_DIR.glob("*.xlsx")
                        if "superseded" not in p.stem and not p.stem.startswith("~"))

    out_root = Path(a.out)
    print(f"exporting {len(labels)} workbook(s) -> {out_root}")
    for label in labels:
        export(label, out_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
