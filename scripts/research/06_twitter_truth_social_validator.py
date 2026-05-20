"""Strategy 06 / Phase 0 -- Truth Social signal validator.

Hypothesis (from research/strategies/06_twitter_truth_social.md):
  Posts from a curated allowlist (Trump on Truth Social being the
  primary high-impact source for Polymarket political markets) move
  prices for 30s-several minutes after publication. A subscriber that
  catches the post early can fill against stale quotes.

This validator answers the literal Phase 0 question from the spec:
  "Pull 20 historical Trump posts from past 30 days... For each:
   Polymarket price 5 min before vs 30 min after. Pass if average
   post-tweet drift > 3% in predictable direction."

Because the X (Twitter) historical archive is paywalled, this
validator focuses on the Truth Social side, which is the dominant
high-impact source per the spec. Truth Social has community-maintained
public archives (trumpstruth.org, factbase.com); we try to scrape one
and fall back gracefully.

What we measure: for each Trump post, hourly Polymarket price
movements in the political/world tag in a window [-1h, +2h] around
the post. Specifically the |drift| from the hour before to the hour
after publication, aggregated across markets active at that time.

This is NOT a direction test (Phase 0 doesn't include an LLM
classifier — that's Phase 1+). It's an INFORMATION-CONTENT test:
do Polymarket political markets move ABNORMALLY around Trump posts
relative to a random hour? If yes, an LLM that classifies direction
extracts that movement. If no, the signal doesn't exist regardless
of classifier quality.

Pass criteria (per research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  n_posts >= 20 AND mean_post_drift_pp >= 1.5 *
          mean_baseline_drift_pp AND mean_post_drift_pp >= 2.0
  YELLOW: positive uplift but below the 1.5x bar
  RED:    no uplift, or post archive unreachable, or < 5 posts

Usage:
  python scripts/research/06_twitter_truth_social_validator.py
"""
from __future__ import annotations

import json
import random
import re
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

# ── Endpoints ───────────────────────────────────────────────────────────────

TRUTHSTRUTH_URL    = "https://trumpstruth.org/"
GAMMA_EVENTS_URL   = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_URL   = "https://clob.polymarket.com/prices-history"

# ── Parameters ──────────────────────────────────────────────────────────────

LOOKBACK_DAYS         = 30       # widen since we use interval=max (no API cap)
MAX_POSTS             = 60
PRE_WINDOW_HOURS      = 1
POST_WINDOW_HOURS     = 2
BASELINE_SAMPLES      = 200      # random 1h windows for control
HARD_RUNTIME_SECS     = 270
SLEEP_BETWEEN_REQS    = 0.4
PAGE_SIZE             = 100
MAX_GAMMA_PAGES       = 4
MAX_MARKETS_PER_POST  = 10        # cap price-history fetches per post

# Narrow tags + only consider markets that pre-existed the lookback
# AND extend past it, so each post has history on both sides.
CATEGORY_TAGS         = ("politics", "elections")

USER_AGENT = "Mozilla/5.0 (Constantine validator; +https://github.com/)"


# ── Data classes ────────────────────────────────────────────────────────────

@dataclass
class TruthPost:
    post_id:   str
    text:      str
    ts_utc:    datetime


@dataclass
class PriceDriftSample:
    market_slug:    str
    post_ts:        datetime | None      # None for baseline samples
    pre_price:      float
    post_price:     float
    drift_pp:       float                # signed (post - pre) * 100
    abs_drift_pp:   float


@dataclass
class ValidationResult:
    n_posts_parsed:        int = 0
    n_posts_with_markets:  int = 0
    post_drifts:           list[float] = field(default_factory=list)
    baseline_drifts:       list[float] = field(default_factory=list)
    samples_post:          list[PriceDriftSample] = field(default_factory=list)
    samples_baseline:      list[PriceDriftSample] = field(default_factory=list)

    @property
    def mean_post_abs(self) -> float:
        return statistics.mean(self.post_drifts) if self.post_drifts else 0.0

    @property
    def mean_baseline_abs(self) -> float:
        return statistics.mean(self.baseline_drifts) if self.baseline_drifts else 0.0

    @property
    def uplift_ratio(self) -> float:
        b = self.mean_baseline_abs
        if b <= 0:
            return 0.0 if self.mean_post_abs <= 0 else float("inf")
        return self.mean_post_abs / b


# ── Truth Social scrape ─────────────────────────────────────────────────────

_TIMESTAMP_PATTERNS = [
    # ISO-ish: 2026-05-12T14:23:00Z or "May 12, 2026 14:23"
    re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?Z?)"),
    re.compile(
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},\s*\d{4}"
        r"(?:\s+\d{1,2}:\d{2}(?:\s*[apAP][mM])?)?)"
    ),
]

_POST_BLOCK_RE = re.compile(
    r'<article[^>]*data-(?:post-)?id="(\d+)"[^>]*>(.*?)</article>',
    re.DOTALL | re.IGNORECASE,
)

# Looser fallback: any block containing both a recognised date and
# meaningful text. Used if the structured <article> selector misses.
_GENERIC_POST_RE = re.compile(
    r'(<[^>]+class="[^"]*(?:status|post|truth)[^"]*"[^>]*>.*?</[^>]+>)',
    re.DOTALL | re.IGNORECASE,
)


def _parse_timestamp(raw: str) -> datetime | None:
    raw = raw.strip()
    # ISO-8601 first
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        pass
    # "May 12, 2026 14:23"
    for fmt in ("%b %d, %Y %H:%M", "%b %d, %Y %I:%M %p",
                "%B %d, %Y %H:%M", "%B %d, %Y %I:%M %p",
                "%b %d, %Y", "%B %d, %Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def fetch_truth_social_posts() -> list[TruthPost]:
    """Scrape recent Trump Truth Social posts from trumpstruth.org.
    Returns list of (post_id, text, utc-timestamp). Empty list on
    failure. Output is best-effort: timestamps may be approximate."""
    try:
        r = requests.get(TRUTHSTRUTH_URL, timeout=20,
                         headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        html = r.text
    except (requests.RequestException, ValueError) as exc:
        print(f"  trumpstruth.org fetch failed: {exc}", file=sys.stderr)
        return []

    posts: list[TruthPost] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    seen_ids: set[str] = set()

    # Primary: structured <article data-post-id=...>
    for match in _POST_BLOCK_RE.finditer(html):
        post_id = match.group(1)
        block = match.group(2)
        if post_id in seen_ids:
            continue
        # First timestamp in the block
        ts_dt = None
        for pat in _TIMESTAMP_PATTERNS:
            tm = pat.search(block)
            if tm:
                ts_dt = _parse_timestamp(tm.group(1))
                if ts_dt:
                    break
        if not ts_dt or ts_dt < cutoff:
            continue
        # Strip HTML tags for text
        text = re.sub(r"<[^>]+>", " ", block)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) < 5:
            continue
        seen_ids.add(post_id)
        posts.append(TruthPost(post_id=post_id, text=text[:500], ts_utc=ts_dt))
        if len(posts) >= MAX_POSTS:
            break

    if not posts:
        # Fallback: generic block hunt
        for match in _GENERIC_POST_RE.finditer(html):
            block = match.group(1)
            ts_dt = None
            for pat in _TIMESTAMP_PATTERNS:
                tm = pat.search(block)
                if tm:
                    ts_dt = _parse_timestamp(tm.group(1))
                    if ts_dt:
                        break
            if not ts_dt or ts_dt < cutoff:
                continue
            text = re.sub(r"<[^>]+>", " ", block)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) < 10:
                continue
            posts.append(TruthPost(
                post_id=f"fb_{len(posts)}",
                text=text[:500],
                ts_utc=ts_dt,
            ))
            if len(posts) >= MAX_POSTS:
                break

    posts.sort(key=lambda p: p.ts_utc, reverse=True)
    print(f"parsed {len(posts)} truth social posts in last {LOOKBACK_DAYS}d")
    return posts


# ── Polymarket markets + price history ──────────────────────────────────────

def fetch_political_markets() -> list[dict]:
    out: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 7)
    for tag in CATEGORY_TAGS:
        for page in range(MAX_GAMMA_PAGES):
            params = {
                "limit":     str(PAGE_SIZE),
                "offset":    str(page * PAGE_SIZE),
                "tag_slug":  tag,
                "order":     "endDate",
                "ascending": "false",
            }
            try:
                r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
                r.raise_for_status()
                events = r.json()
            except (requests.RequestException, ValueError):
                break
            if not events:
                break
            for e in events:
                for m in e.get("markets", []):
                    m.setdefault("_event_title", e.get("title", ""))
                    out.append(m)
            time.sleep(SLEEP_BETWEEN_REQS)
        # Dedupe across tag iterations
    seen: set[str] = set()
    deduped: list[dict] = []
    for m in out:
        mid = str(m.get("id") or m.get("conditionId") or m.get("slug"))
        if mid in seen:
            continue
        seen.add(mid)
        deduped.append(m)
    print(f"fetched {len(deduped)} unique political markets across "
          f"{len(CATEGORY_TAGS)} tags")
    return deduped


def fetch_price_history(token_id: str, around_ts: datetime,
                        hours_back: int, hours_forward: int) -> list[tuple[datetime, float]]:
    """Use interval=max — the only form that returns reliable history
    for long-lived markets. Caller filters to the desired window after
    receiving the full series. (around_ts/hours_back/hours_forward are
    kept on the signature for upstream caller compatibility.)"""
    del around_ts, hours_back, hours_forward  # silence linter
    params = {"market": token_id, "interval": "max"}
    try:
        r = requests.get(CLOB_HISTORY_URL, params=params, timeout=15)
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError):
        return []
    history = payload.get("history") or payload.get("h") or []
    out: list[tuple[datetime, float]] = []
    for pt in history:
        ts = pt.get("t") or pt.get("timestamp")
        px = pt.get("p") or pt.get("price")
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            out.append((dt, float(px)))
        except (TypeError, ValueError, OSError):
            continue
    out.sort(key=lambda x: x[0])
    return out


def _bisect_price(history: list[tuple[datetime, float]],
                  target: datetime) -> float | None:
    """Find price at the closest hour <= target, or None."""
    if not history:
        return None
    # Linear scan since lists are tiny (3 buckets)
    last = None
    for ts, px in history:
        if ts <= target:
            last = px
        else:
            break
    return last


def measure_drift(history: list[tuple[datetime, float]],
                  centre_ts: datetime) -> tuple[float, float] | None:
    """Returns (pre_price, post_price). None if cannot be computed."""
    pre = _bisect_price(history, centre_ts - timedelta(hours=PRE_WINDOW_HOURS))
    post = _bisect_price(history, centre_ts + timedelta(hours=POST_WINDOW_HOURS))
    if pre is None or post is None:
        return None
    if pre <= 0.02 or pre >= 0.98:
        return None
    return pre, post


def first_token_id(m: dict) -> str | None:
    raw = m.get("clobTokenIds") or "[]"
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return None
    if not isinstance(ids, list) or not ids:
        return None
    return str(ids[0]) if ids[0] not in (0, "0") else None


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    posts = fetch_truth_social_posts()
    if not posts:
        print()
        print("=" * 64)
        print("STRATEGY 06 / PHASE 0 -- TRUTH SOCIAL VALIDATOR")
        print("=" * 64)
        print("[RED] KILL: could not parse any Trump Truth Social posts from")
        print("           trumpstruth.org (HTML structure may have changed,")
        print("           network may be blocked, or the archive is offline).")
        print("           Phase 0 requires a working post archive; the strategy")
        print("           cannot be validated empirically without it.")
        print()
        print("Recovery: inspect raw HTML from trumpstruth.org and update")
        print("the _POST_BLOCK_RE / _TIMESTAMP_PATTERNS regexes in this file.")
        return 3

    if time.time() - started > HARD_RUNTIME_SECS:
        print("  runtime cap hit after post scrape", file=sys.stderr)
        return 1

    markets = fetch_political_markets()
    if not markets:
        print("[RED] KILL: no political markets returned from Gamma API.")
        return 3

    # Pre-filter to markets that have token ids and a parseable endDate
    candidate_markets = []
    for m in markets:
        tid = first_token_id(m)
        if not tid:
            continue
        end_raw = m.get("endDate", "")
        try:
            m["_end_dt"] = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        except (TypeError, AttributeError, ValueError):
            continue
        m["_tid"] = tid
        candidate_markets.append(m)
    print(f"using {len(candidate_markets)} markets with valid token ids + endDate")

    res = ValidationResult()
    res.n_posts_parsed = len(posts)

    # Pick a random subset of markets per post (no domain-matching since
    # we'd need an LLM; we treat each market as potentially impacted and
    # let the aggregate average smooth out non-impacted ones).
    rng = random.Random(20260513)

    for i, post in enumerate(posts):
        if time.time() - started > HARD_RUNTIME_SECS:
            print(f"  runtime cap hit at post {i}/{len(posts)}", file=sys.stderr)
            break

        # Per-post candidate filter: market must extend past post time by
        # the post-window, so we have data on both sides of the post.
        post_eligible = [
            m for m in candidate_markets
            if m["_end_dt"] > post.ts_utc + timedelta(hours=POST_WINDOW_HOURS + 1)
        ]
        if not post_eligible:
            continue

        # Sample N markets that were active around the post time
        sample = rng.sample(
            post_eligible,
            min(MAX_MARKETS_PER_POST, len(post_eligible)),
        )
        had_any = False
        for m in sample:
            history = fetch_price_history(
                m["_tid"], post.ts_utc,
                hours_back=PRE_WINDOW_HOURS,
                hours_forward=POST_WINDOW_HOURS,
            )
            pair = measure_drift(history, post.ts_utc)
            if not pair:
                continue
            pre, post_p = pair
            drift = (post_p - pre) * 100.0
            abs_drift = abs(drift)
            res.post_drifts.append(abs_drift)
            had_any = True
            if len(res.samples_post) < 25:
                res.samples_post.append(PriceDriftSample(
                    market_slug=m.get("slug", ""),
                    post_ts=post.ts_utc,
                    pre_price=pre,
                    post_price=post_p,
                    drift_pp=drift,
                    abs_drift_pp=abs_drift,
                ))
            time.sleep(SLEEP_BETWEEN_REQS / 2)
        if had_any:
            res.n_posts_with_markets += 1

        if (i + 1) % 5 == 0:
            elapsed = time.time() - started
            print(f"  processed {i+1}/{len(posts)} posts "
                  f"(drifts={len(res.post_drifts)} elapsed={elapsed:.0f}s)",
                  flush=True)
        time.sleep(SLEEP_BETWEEN_REQS)

    # Baseline: random hours during the lookback period on a random
    # sample of the same markets — same measurement, no post anchor
    now = datetime.now(timezone.utc)
    for _ in range(BASELINE_SAMPLES):
        if time.time() - started > HARD_RUNTIME_SECS:
            break
        if not candidate_markets:
            break
        m = rng.choice(candidate_markets)
        # Random hour in the lookback window
        offset_h = rng.uniform(PRE_WINDOW_HOURS + 1,
                               LOOKBACK_DAYS * 24 - POST_WINDOW_HOURS - 1)
        centre = now - timedelta(hours=offset_h)
        history = fetch_price_history(
            m["_tid"], centre,
            hours_back=PRE_WINDOW_HOURS,
            hours_forward=POST_WINDOW_HOURS,
        )
        pair = measure_drift(history, centre)
        if not pair:
            continue
        pre, post_p = pair
        drift = (post_p - pre) * 100.0
        abs_drift = abs(drift)
        res.baseline_drifts.append(abs_drift)
        if len(res.samples_baseline) < 25:
            res.samples_baseline.append(PriceDriftSample(
                market_slug=m.get("slug", ""),
                post_ts=None,
                pre_price=pre,
                post_price=post_p,
                drift_pp=drift,
                abs_drift_pp=abs_drift,
            ))
        time.sleep(SLEEP_BETWEEN_REQS / 2)

    # Artefacts
    csv_path = out_dir / "06_twitter_truth_social_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("group,market_slug,post_ts,pre_price,post_price,drift_pp\n")
        for s in res.samples_post:
            f.write(f"post,{s.market_slug},{s.post_ts.isoformat() if s.post_ts else ''},"
                    f"{s.pre_price:.4f},{s.post_price:.4f},{s.drift_pp:+.2f}\n")
        for s in res.samples_baseline:
            f.write(f"baseline,{s.market_slug},,"
                    f"{s.pre_price:.4f},{s.post_price:.4f},{s.drift_pp:+.2f}\n")

    summary = {
        "n_posts_parsed":        res.n_posts_parsed,
        "n_posts_with_markets":  res.n_posts_with_markets,
        "n_post_drifts":         len(res.post_drifts),
        "n_baseline_drifts":     len(res.baseline_drifts),
        "mean_post_abs_drift_pp":     round(res.mean_post_abs, 4),
        "mean_baseline_abs_drift_pp": round(res.mean_baseline_abs, 4),
        "uplift_ratio":          round(res.uplift_ratio, 4),
        "lookback_days":         LOOKBACK_DAYS,
        "pre_window_h":          PRE_WINDOW_HOURS,
        "post_window_h":         POST_WINDOW_HOURS,
    }
    json_path = out_dir / "06_twitter_truth_social_phase0.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 06 / PHASE 0 -- TRUTH SOCIAL VALIDATOR")
    print("=" * 64)
    print(f"Posts parsed (last {LOOKBACK_DAYS}d):  {res.n_posts_parsed}")
    print(f"Posts with market drift data:    {res.n_posts_with_markets}")
    print(f"Post drift samples:              {len(res.post_drifts)}")
    print(f"Baseline drift samples:          {len(res.baseline_drifts)}")
    print(f"Mean |drift| post-tweet [-{PRE_WINDOW_HOURS}h..+{POST_WINDOW_HOURS}h]: "
          f"{res.mean_post_abs:.2f}pp")
    print(f"Mean |drift| baseline 1h windows: {res.mean_baseline_abs:.2f}pp")
    print(f"Uplift ratio (post / baseline):  {res.uplift_ratio:.2f}x")
    print()

    if len(res.post_drifts) < 5:
        print("[RED] KILL: too few post drift samples to draw conclusions.")
        print("            Likely cause: post archive or Polymarket price-history")
        print("            unavailable for the post timestamps in this window.")
        return 3

    if res.n_posts_parsed >= 20 and res.uplift_ratio >= 1.5 and res.mean_post_abs >= 2.0:
        print("[GREEN] GREEN-LIGHT Strategy 06 to Phase 1:")
        print(f"        n_posts {res.n_posts_parsed} >= 20, "
              f"uplift {res.uplift_ratio:.2f}x >= 1.5x, "
              f"mean post |drift| {res.mean_post_abs:.2f}pp >= 2.0pp.")
        return 0
    if res.uplift_ratio > 1.0 and res.mean_post_abs > res.mean_baseline_abs:
        print("[YELLOW] MARGINAL: some uplift but below promotion bar.")
        print("         Consider extending lookback window or filtering to")
        print("         markets thematically related to post content (needs LLM).")
        return 1
    print("[RED] KILL: post-tweet drift not distinguishable from baseline.")
    print("           Either Polymarket has already priced in Trump posts at")
    print("           this 1h granularity, or our random market sample missed")
    print("           the impacted markets (Phase 1 LLM matching may help).")
    return 3


if __name__ == "__main__":
    sys.exit(main())
