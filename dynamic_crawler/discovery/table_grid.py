"""Table-grid discovery strategy (e.g. SAMA Circulars' DataTables grid with a
"Show All" dropdown, see crawler/sama_circulars_crawler.py).

Not implemented in Phase 1 -- this pilot covers the sidebar_tree strategy only
(SAMA Finance Sector). This stub exists so config schema validation and any
future engine dispatch can reference discovery.strategy="table_grid" without a
rewrite once table-grid support is actually built.
"""


class TableGridNotImplemented(NotImplementedError):
    pass


def collect_table_rows(*args, **kwargs):
    raise TableGridNotImplemented(
        "discovery.strategy=table_grid has no engine implementation yet "
        "(Phase 1 pilot covers sidebar_tree only, e.g. SAMA Finance Sector)."
    )
