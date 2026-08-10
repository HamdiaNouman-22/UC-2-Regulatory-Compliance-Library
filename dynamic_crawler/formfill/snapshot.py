"""SNAPSHOT STORE — one saved page, and the clock that decides when to ask again.

Why this exists: SIMAH is behind a Cloudflare firewall rule (a 1020-class "Sorry,
you have been blocked" — not a challenge, so there is nothing to solve). A full run
of its form is only TWO loads of one URL, so request volume never tripped the rule.
ITERATION did: every selector fix, and every `verify --runs 3`, was more live
traffic for no new information.

So the live site is touched by exactly one thing — `capture()` — and everything
else replays the saved page. The rules below are the ones that keep a blocked site
from getting more blocked:

  * ONE navigation per attempt. No retries. Retrying a block earns a longer block.
  * A blocked attempt backs off: 6h, 24h, 72h, 7d, 14d, then 14d forever. Written
    to `next_attempt_after` so the wait is inspectable, not implicit.
  * A success resets the backoff and refreshes the page.
  * A challenge page is NEVER saved. That is how the block ended up stored as
    1,054 characters of "law" the first time.

FRESH / AGING / STALE
    fresh   captured within max_age_days           -> serve it
    aging   older than that, refresh attempts due  -> serve it, flagged
    stale   older than the grace period AND every  -> refuse; a caller must not
            refresh since has been blocked            publish it as current

The staleness cliff is the honest half. A snapshot replayed forever would tell
change detection "unchanged" while the law moved on — a silent false negative in
the one system whose job is noticing change.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Where snapshots live. Under output/, which .gitignore already excludes: a
# snapshot is a copy of someone else's page, and it is regenerable by whoever can
# reach the site. Committing one is a deliberate act, not a side effect.
DEFAULT_DIR = Path("output/snapshots")

# Backoff after a blocked attempt, by consecutive-block count. The first step is
# long enough that a rate rule would have expired; the last is long enough that we
# are no longer part of the site's traffic pattern at all.
BACKOFF_HOURS = (6, 24, 72, 24 * 7, 24 * 14)

DEFAULT_MAX_AGE_DAYS = 30      # a law does not change weekly; past this, refresh
DEFAULT_GRACE_DAYS = 90        # past this with no successful refresh, refuse


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _parse(ts: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(ts) if ts else None
    except ValueError:
        return None


class SnapshotStore:
    """The saved page for one form, plus its manifest."""

    def __init__(self, name: str, directory: str | Path | None = None,
                 max_age_days: int = DEFAULT_MAX_AGE_DAYS,
                 grace_days: int = DEFAULT_GRACE_DAYS):
        self.name = name
        self.dir = Path(directory or DEFAULT_DIR)
        self.html_path = self.dir / f"{name}.html"
        self.manifest_path = self.dir / f"{name}.manifest.json"
        self.max_age_days = max_age_days
        self.grace_days = grace_days

    # ---- reading ---------------------------------------------------------- #

    def exists(self) -> bool:
        return self.html_path.exists()

    def manifest(self) -> dict:
        try:
            return json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def html(self) -> str:
        return self.html_path.read_text(encoding="utf-8")

    def age_days(self) -> float | None:
        captured = _parse(self.manifest().get("captured_at"))
        return None if captured is None else (_now() - captured).total_seconds() / 86400

    def state(self) -> str:
        """fresh | aging | stale | missing — the word a caller acts on."""
        if not self.exists():
            return "missing"
        age = self.age_days()
        if age is None:
            return "aging"                 # captured, but undated: treat as due
        if age <= self.max_age_days:
            return "fresh"
        return "stale" if age > self.grace_days else "aging"

    # ---- the clock -------------------------------------------------------- #

    def may_attempt(self) -> tuple[bool, str]:
        """Is a live attempt allowed right now, and if not, why not?

        This is the whole safety mechanism. Nobody decides when to poke a blocked
        site by hand — a human under deadline always decides 'now'.
        """
        m = self.manifest()
        after = _parse(m.get("next_attempt_after"))
        if after is None:
            return True, "no attempt on record"
        now = _now()
        if now >= after:
            return True, f"due since {_iso(after)}"
        hours = (after - now).total_seconds() / 3600
        # The same field holds two very different waits, and saying "blocked 0x in a
        # row" after a SUCCESS reads as a fault when it is the opposite.
        if m.get("last_attempt_result") == "blocked":
            return False, (
                f"blocked {m.get('consecutive_blocks', 0)}x in a row; backoff until "
                f"{_iso(after)} (in {hours:.1f}h). --force only if something actually "
                f"changed — a different network, or the site allowlisted us.")
        return False, (
            f"snapshot is fresh; next refresh due {_iso(after)} (in {hours / 24:.1f} "
            f"days). Nothing to do — replay it with `run --snapshot`.")

    def next_backoff_hours(self) -> int:
        n = int(self.manifest().get("consecutive_blocks", 0))
        return BACKOFF_HOURS[min(n, len(BACKOFF_HOURS) - 1)]

    # ---- writing ---------------------------------------------------------- #

    def save(self, html: str, url: str) -> dict:
        """Record a successful capture. Returns the manifest, including whether the
        page CHANGED — which is the monitoring signal this whole mechanism is for."""
        self.dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256((html or "").encode("utf-8")).hexdigest()
        previous = self.manifest()
        changed = bool(previous.get("sha256")) and previous["sha256"] != digest

        self.html_path.write_text(html, encoding="utf-8")
        m = {
            "name": self.name,
            "url": url,
            "captured_at": _iso(_now()),
            "sha256": digest,
            "bytes": len((html or "").encode("utf-8")),
            "last_attempt_at": _iso(_now()),
            "last_attempt_result": "ok",
            "consecutive_blocks": 0,          # success resets the backoff
            "next_attempt_after": _iso(_now() + timedelta(days=self.max_age_days)),
            "changed_on_last_capture": changed,
            "previous_sha256": previous.get("sha256", ""),
            "history": (previous.get("history") or [])[-19:] + [
                {"at": _iso(_now()), "result": "ok", "sha256": digest,
                 "changed": changed}],
        }
        self.manifest_path.write_text(json.dumps(m, indent=2), encoding="utf-8")
        return m

    def record_block(self, reason: str) -> dict:
        """Record a blocked attempt and push the next one further out. The HTML is
        NOT written: a challenge page must never become the stored document."""
        self.dir.mkdir(parents=True, exist_ok=True)
        m = self.manifest()
        blocks = int(m.get("consecutive_blocks", 0)) + 1
        hours = BACKOFF_HOURS[min(blocks - 1, len(BACKOFF_HOURS) - 1)]
        m.update({
            "name": self.name,
            "last_attempt_at": _iso(_now()),
            "last_attempt_result": "blocked",
            "last_block_reason": reason,
            "consecutive_blocks": blocks,
            "next_attempt_after": _iso(_now() + timedelta(hours=hours)),
            "history": (m.get("history") or [])[-19:] + [
                {"at": _iso(_now()), "result": "blocked", "reason": reason,
                 "backoff_hours": hours}],
        })
        self.manifest_path.write_text(json.dumps(m, indent=2), encoding="utf-8")
        return m

    def record_failure(self, reason: str) -> dict:
        """A load that failed WITHOUT being a block — a timeout, DNS, an empty
        render. Recorded, but it does not earn the block backoff: the site did not
        refuse us, so the next scheduled run may try again normally."""
        self.dir.mkdir(parents=True, exist_ok=True)
        m = self.manifest()
        # Present even on a first-ever failure, so a reader can index it. A failure
        # deliberately does NOT increment it.
        m.setdefault("consecutive_blocks", 0)
        m.update({
            "name": self.name,
            "last_attempt_at": _iso(_now()),
            "last_attempt_result": "failed",
            "last_failure_reason": reason,
            "history": (m.get("history") or [])[-19:] + [
                {"at": _iso(_now()), "result": "failed", "reason": reason}],
        })
        self.manifest_path.write_text(json.dumps(m, indent=2), encoding="utf-8")
        return m

    # ---- reporting -------------------------------------------------------- #

    def describe(self) -> str:
        if not self.exists():
            m = self.manifest()
            tail = (f" — last attempt {m.get('last_attempt_result')} at "
                    f"{m.get('last_attempt_at')}" if m.get("last_attempt_at") else "")
            return f"{self.name}: NO SNAPSHOT{tail}"
        m = self.manifest()
        age = self.age_days()
        bits = [f"{self.name}: {self.state().upper()}"]
        if age is not None:
            bits.append(f"captured {age:.1f}d ago ({m.get('captured_at')})")
        bits.append(f"{m.get('bytes', 0)} bytes")
        if m.get("consecutive_blocks"):
            bits.append(f"{m['consecutive_blocks']} blocked attempt(s) since")
        if m.get("next_attempt_after"):
            bits.append(f"next attempt after {m['next_attempt_after']}")
        return " | ".join(bits)


def capture(hints: dict, store: SnapshotStore, headed: bool = True,
            force: bool = False, wait_ms: int = 1200,
            user_data_dir: str | Path | None = None) -> dict:
    """ONE navigation to the live site. The only function here that leaves the machine.

    Headed and persistent by default, because the hypothesis worth testing is
    bot-fingerprinting: stock headless Chromium advertises `navigator.webdriver`
    and a headless UA, and a Cloudflare rule can be set to BLOCK rather than
    challenge on that. A real profile in a real window is the cheapest way to find
    out, and it costs one request.

    Returns {"result": ok|blocked|failed|refused, ...}. Never raises for a block —
    a block is data, and the caller needs the manifest, not a traceback.
    """
    from playwright.sync_api import sync_playwright

    from dynamic_crawler.formfill.runner import _blocked, _expand

    url = hints["seed_url"]
    allowed, why = store.may_attempt()
    if not allowed and not force:
        return {"result": "refused", "reason": why, "url": url}

    expand_sel = hints.get("expand_selector") or ""
    profile = Path(user_data_dir or (store.dir / f"{store.name}.profile"))
    profile.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # A persistent context keeps cookies between attempts — including any
        # Cloudflare clearance cookie, which is the difference between passing once
        # and passing every time.
        ctx = pw.chromium.launch_persistent_context(
            str(profile), headless=not headed, locale="en-US",
            viewport={"width": 1440, "height": 900},
            args=["--disable-blink-features=AutomationControlled"])
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        try:
            # ONE goto. No retry loop anywhere in this function, by design.
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
            except Exception as e:
                store.record_failure(str(e)[:200])
                return {"result": "failed", "reason": str(e)[:200], "url": url}

            page.wait_for_timeout(wait_ms)
            # SIMAH renders its articles late: wait for the page to stop growing
            # rather than for a fixed time (see runner._load).
            settled = -1
            for _ in range(12):
                try:
                    size = page.evaluate(
                        "()=>document.body ? document.body.innerText.length : 0")
                except Exception:
                    break
                if size == settled:
                    break
                settled = size
                page.wait_for_timeout(400)

            reason = _blocked(page)
            if reason:
                m = store.record_block(reason)
                return {"result": "blocked", "reason": reason, "url": url,
                        "next_attempt_after": m.get("next_attempt_after"),
                        "consecutive_blocks": m.get("consecutive_blocks")}

            # Expand before saving: captured closed, SIMAH's saved HTML is 17 lines
            # reading "Article-N" and no law at all.
            clicked = _expand(page, expand_sel) if expand_sel else 0
            html = page.content()
            text_len = len(page.evaluate(
                "()=>document.body ? document.body.innerText : ''") or "")
            m = store.save(html, url)
            return {"result": "ok", "url": url, "expanded": clicked,
                    "text_len": text_len, "bytes": m["bytes"],
                    "sha256": m["sha256"], "changed": m["changed_on_last_capture"],
                    "path": str(store.html_path)}
        finally:
            try:
                ctx.close()
            except Exception:
                pass


__all__ = ["SnapshotStore", "capture", "DEFAULT_DIR", "BACKOFF_HOURS",
           "DEFAULT_MAX_AGE_DAYS", "DEFAULT_GRACE_DAYS"]
