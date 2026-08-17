"""
pages_to_excel.py — read any engine's pages.json into one readable workbook.

    # one run
    venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_faqs

    # several runs merged into one workbook, with a Summary sheet
    venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_* \
        --out output/site_runners/CMA_all.xlsx

Works for generic_crawler, formfill and the site_runners alike, because all three
emit the same pages.json schema.

    # include the raw HTML as two extra columns
    venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_guides --with-html

    Summary    one row per run — counts, empties, date fill rates
    Documents  one row per attached file
    Pages      one row per page/article, with a text preview

HTML is EXCLUDED unless --with-html. Excel truncates a cell at 32,767 characters
and the markup is usually several times the text, so it would quietly lose the
tail of the longer records and multiply the file size. pages.json always keeps
the full HTML; the workbook is for reading, not for round-tripping.

Merging matters more than it looks: reviewing nine tabs means nine files to open
and no way to see that one of them came back empty. The Summary sheet is the
whole crawl on one screen.
"""
import sys
from pathlib import Path
import json
import pandas as pd

CELL = 32000          # Excel's hard limit is 32767 characters per cell


def load_run(pj: Path):
    d = json.loads(pj.read_text(encoding="utf-8"))
    # The run's name: the tab label if the engine recorded one, else the folder.
    name = d.get("tab") or d.get("name") or pj.parent.name
    return name, d


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    out_arg = ""
    with_html = "--with-html" in sys.argv
    for i, a in enumerate(sys.argv):
        if a == "--out" and i + 1 < len(sys.argv):
            out_arg = sys.argv[i + 1]
    if not args:
        print(__doc__)
        return 1

    runs = []
    for a in args:
        src = Path(a)
        if src.suffix == ".json":
            cands = [src]
        elif (src / "pages.json").exists():
            cands = [src / "pages.json"]
        else:
            # a parent folder: pick up every run beneath it
            cands = sorted(src.glob("*/pages.json"))
        for pj in cands:
            if pj.exists():
                runs.append((pj, *load_run(pj)))
    if not runs:
        print(f"no pages.json found under: {', '.join(args)}")
        return 1

    summary, pages, docs, regs, nested = [], [], [], [], []
    for pj, name, d in runs:
        ps, ds = d.get("pages", []), d.get("documents", [])
        # Registers are entity tables, not pages — one sheet per concern, with
        # the register's own columns flattened out so it reads like the site.
        for reg in d.get("registers", []):
            for row in reg.get("rows", []):
                regs.append({"run": name, "register": reg.get("register", ""),
                             "key": row.get("key", ""),
                             "section_path": row.get("section_path", ""),
                             "group": row.get("group", ""),
                             **{k: v for k, v in (row.get("fields") or {}).items()}})
                for nt in row.get("nested", []):
                    ncols = nt.get("cols") or []
                    for nr in nt.get("rows", []):
                        nested.append({
                            "run": name, "register": reg.get("register", ""),
                            "parent_key": row.get("key", ""),
                            **{(ncols[i] if i < len(ncols) else f"col{i}"): v
                               for i, v in enumerate(nr)}})
        empty = sum(1 for r in ps if not r.get("text_len") and not r.get("n_pdfs"))
        summary.append({
            "run": name,
            "records": len(ps),
            "documents": len(ds),
            "register_rows": sum(len(r.get("rows", []))
                                 for r in d.get("registers", [])),
            # An empty record carries nothing at all — no text, no file. This is
            # the column to scan first.
            "empty_records": empty,
            "with_published_date": sum(1 for r in ps if r.get("published_date")),
            "with_last_updated": sum(1 for r in ps if r.get("last_updated_date")),
            "unique_urls": len({r.get("url", "") for r in ps}),
            "max_depth": max([r.get("depth", 0) for r in ps] or [0]),
            "shape": d.get("shape", ""),
            "engine": d.get("engine", ""),
            "source": str(pj.parent),
        })
        for r in ps:
            # HTML is off by default. Excel truncates a cell at 32,767
            # characters and the markup is usually several times the text, so
            # including it silently loses the tail of the longer records and
            # multiplies the file size. pages.json always has it intact.
            html = {"html": (r.get("html") or "")[:CELL],
                    "html_len": len(r.get("html") or "")} if with_html else {}
            pages.append({
                "run": name,
                "section_path": r.get("section_path", ""),
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "text_len": r.get("text_len", 0),
                "n_pdfs": r.get("n_pdfs", 0),
                "published_date": r.get("published_date", ""),
                "last_updated_date": r.get("last_updated_date", ""),
                # CMA's "Last modified date:" is a SITE stamp, not a document
                # revision date — 36 Implementing Regulations all share one
                # value. Kept in its own column so nobody mistakes it for the
                # document's own date.
                "page_last_modified": r.get("page_last_modified", ""),
                "expiry_date": r.get("expiry_date", ""),
                "record_type": r.get("record_type", ""),
                "html_file": r.get("html_file", ""),
                "page_title": r.get("page_title", ""),
                "pdf_links": (r.get("pdf_links") or "")[:CELL],
                "text_preview": (r.get("text") or "")[:CELL],
                **html,
            })
        for x in ds:
            docs.append({"run": name, **x})

    out = Path(out_arg) if out_arg else runs[0][0].parent / "results.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        if len(runs) > 1:
            pd.DataFrame(summary).to_excel(xw, sheet_name="Summary", index=False)
        if docs:
            pd.DataFrame(docs).to_excel(xw, sheet_name="Documents", index=False)
        if regs:
            pd.DataFrame(regs).to_excel(xw, sheet_name="Registers", index=False)
        if nested:
            # Accounting Offices: the accountants inside each office row.
            pd.DataFrame(nested).to_excel(xw, sheet_name="Register_nested", index=False)
        if pages or not regs:
            pd.DataFrame(pages).to_excel(xw, sheet_name="Pages", index=False)

    print(f"{len(runs)} run(s), {len(pages)} pages, {len(docs)} documents, "
          f"{len(regs)} register rows -> {out}")
    for s in summary:
        flag = "  <-- EMPTY RECORDS" if s["empty_records"] else ""
        print(f"   {s['run'][:44]:<46} {s['records']:>5} pages  "
              f"{s['documents']:>4} docs{flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
