"""Sandboxed runner for an auto-generated adapter.

Invoked ONLY as a subprocess by sandbox.py (never imported by the live
pipeline). It:
  1. statically analyzes the generated source (AST) and REJECTS it before
     execution if it imports anything outside a curated allowlist or uses a
     dangerous builtin (open/eval/exec/compile/__import__/input),
  2. executes the module with those dangerous builtins removed from its
     namespace as defense-in-depth,
  3. instantiates its RegulatorAdapter subclass with a CountingFetcher,
  4. runs .crawl(limit) and writes the resulting documents to --out as JSON.

Static analysis (rather than a runtime __import__ hook) is used deliberately:
it inspects only the adapter's OWN imports, so it can't be tripped by trusted
libraries' internal/transitive imports, yet still blocks os/subprocess/socket/
requests etc. at the adapter level. Combined with subprocess isolation and the
network being forced through the rate-limited Fetcher, this is the sandbox.

Any exception is serialized to --out as {"error": ...} with a traceback so the
orchestrator can feed it into the refine loop.

Usage:
    python -m dynamic_crawler.auto.runner <adapter.py> --out results.json [--limit N]
                                          [--backend requests|selenium] [--max-fetches N]
"""

import argparse
import ast
import builtins
import importlib.util
import inspect
import json
import sys
import traceback
from dataclasses import asdict

# Top-level modules the generated adapter may import.
_ALLOWED_ROOTS = {
    "bs4", "re", "datetime", "urllib", "html", "collections", "itertools",
    "functools", "string", "math", "json", "typing", "dataclasses", "unicodedata",
    "dynamic_crawler", "models",
}
# Fully-qualified names allowed within otherwise-restricted packages.
_ALLOWED_DC = {
    "dynamic_crawler", "dynamic_crawler.fetcher", "dynamic_crawler.urlnorm",
    "dynamic_crawler.auto.adapter_base",
}
_ALLOWED_MODELS = {"models", "models.models"}
_BLOCKED_BUILTIN_CALLS = {"eval", "exec", "compile", "__import__", "open", "input"}
_STRIP_FROM_NAMESPACE = ("open", "eval", "exec", "compile", "input")


class SandboxViolation(Exception):
    pass


def _check_module_allowed(name: str):
    root = name.split(".")[0]
    if root not in _ALLOWED_ROOTS:
        raise SandboxViolation(f"Adapter imports disallowed module {name!r} (sandbox policy)")
    if root == "dynamic_crawler" and name not in _ALLOWED_DC:
        raise SandboxViolation(f"Adapter may not import {name!r} (only fetcher/urlnorm/adapter_base)")
    if root == "models" and name not in _ALLOWED_MODELS:
        raise SandboxViolation(f"Adapter may not import {name!r}")


def _static_check(source: str, path: str):
    tree = ast.parse(source, filename=path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _check_module_allowed(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                raise SandboxViolation("Relative imports are not allowed in a generated adapter")
            if node.module:
                _check_module_allowed(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in _BLOCKED_BUILTIN_CALLS:
                raise SandboxViolation(f"Adapter uses disallowed builtin {node.func.id!r}")
        elif isinstance(node, ast.Attribute) and node.attr.startswith("__") and node.attr.endswith("__"):
            # block dunder attribute tricks like ().__class__.__bases__... to reach os
            if node.attr in ("__globals__", "__builtins__", "__subclasses__", "__bases__", "__mro__"):
                raise SandboxViolation(f"Adapter uses disallowed attribute {node.attr!r}")


def _load_adapter_module(path: str):
    source = open(path, "r", encoding="utf-8").read()  # runner is trusted; adapter is the untrusted input
    _static_check(source, path)

    spec = importlib.util.spec_from_file_location("_generated_adapter", path)
    module = importlib.util.module_from_spec(spec)
    safe_builtins = {k: v for k, v in vars(builtins).items() if k not in _STRIP_FROM_NAMESPACE}
    module.__dict__["__builtins__"] = safe_builtins
    spec.loader.exec_module(module)
    return module


def _find_adapter_class(module):
    from dynamic_crawler.auto.adapter_base import RegulatorAdapter
    candidates = [
        obj for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, RegulatorAdapter) and obj is not RegulatorAdapter
        and obj.__module__ == module.__name__
    ]
    if not candidates:
        raise SandboxViolation("No RegulatorAdapter subclass defined in the generated module")
    if len(candidates) > 1:
        raise SandboxViolation(f"Expected exactly one RegulatorAdapter subclass, found {len(candidates)}")
    return candidates[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("adapter_path")
    parser.add_argument("--out", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--backend", default="requests")
    parser.add_argument("--max-fetches", type=int, default=1500)
    parser.add_argument("--request-delay", type=float, default=1.0)
    args = parser.parse_args()

    result = {"documents": [], "fetch_count": 0}
    try:
        module = _load_adapter_module(args.adapter_path)
        adapter_cls = _find_adapter_class(module)

        from dynamic_crawler.auto.counting_fetcher import CountingFetcher
        fetch_cfg = {
            "backend": args.backend,
            "request_delay_seconds": args.request_delay,
            "max_retries": 3,
            "retry_backoff_seconds": 2,
            "timeout_seconds": 30,
        }
        fetcher = CountingFetcher(fetch_cfg, max_fetches=args.max_fetches)
        try:
            adapter = adapter_cls(fetcher)
            docs = adapter.crawl(limit=args.limit)
        finally:
            fetcher.close()

        serialized = []
        for d in docs:
            serialized.append(asdict(d) if hasattr(d, "__dataclass_fields__") else dict(d))
        result["documents"] = serialized
        result["fetch_count"] = fetcher.count
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        result["traceback"] = traceback.format_exc()

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    sys.exit(1 if "error" in result else 0)


if __name__ == "__main__":
    main()
