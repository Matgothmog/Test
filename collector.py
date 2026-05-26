"""Hourly Hyperliquid portfolio snapshot collector.

Designed to run inside GitHub Actions on a cron schedule. Each run:
  1. Fetches the current HL leaderboard (live, top wallets)
  2. For each tracked wallet, calls portfolio endpoint
  3. Appends new (addr, bucket, t, equity) rows to data/portfolios.parquet
  4. Dedupes on (addr, bucket, t); writes back

Persistence is via git commits made by the Actions runner.

Failure modes:
  - HL API 500: retried with backoff, then skipped (logged)
  - Wallet missing portfolio data: skipped
  - No new rows after dedup: exits without committing
"""
from __future__ import annotations
import os, sys, time, json
import requests
import pandas as pd
from pathlib import Path

INFO_URL = "https://api.hyperliquid.xyz/info"
LB_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

# Tracked-wallet selection rule.
# The wallets.json was seeded externally (by the all-regime-consistency selector)
# with ~794 hand-picked consistently-winning wallets. The leaderboard daily refresh
# below only ADDS new wallets that newly meet criteria — existing entries are never
# dropped (avoids survivorship bias).
TOP_N_ALLTIME = 150              # additive seed from daily leaderboard top-N
MIN_WEEK_VLM = 1_000_000         # require recent activity ≥$1M weekly volume
MAX_TRACKED = 1500               # hard cap on tracked universe (~12 min runtime at 1500)

DATA_DIR = Path(__file__).parent / "data"
OUT_PATH = DATA_DIR / "portfolios.parquet"
WALLETS_PATH = DATA_DIR / "wallets.json"  # cached wallet list, refreshed daily

USER_AGENT = "hl-portfolio-collector/1.0 (GitHub Actions)"


def info(payload: dict, retries: int = 6, backoff: float = 1.5) -> dict | list:
    for attempt in range(retries):
        try:
            r = requests.post(INFO_URL, json=payload, timeout=20,
                              headers={"Content-Type": "application/json",
                                       "User-Agent": USER_AGENT})
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(backoff * (1.6 ** attempt), 15.0))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(min(backoff * (1.6 ** attempt), 15.0))
    raise RuntimeError("info exhausted retries")


def _fetch_topN() -> list[str]:
    """Return today's top-N leaderboard addresses (active-only)."""
    r = requests.get(LB_URL, timeout=30, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    rows = r.json()["leaderboardRows"]
    enriched = []
    for row in rows:
        addr = row["ethAddress"].lower()
        all_pnl, week_vlm = 0.0, 0.0
        for window, m in row.get("windowPerformances", []):
            if window == "allTime":
                all_pnl = float(m.get("pnl", 0))
            elif window == "week":
                week_vlm = float(m.get("vlm", 0))
        if week_vlm >= MIN_WEEK_VLM:
            enriched.append((addr, all_pnl, week_vlm))
    enriched.sort(key=lambda x: x[1], reverse=True)
    return [a for a, _, _ in enriched[:TOP_N_ALLTIME]]


def _load_existing() -> dict:
    """Load cached wallet state. Handles both the new schema and the legacy [addrs] schema."""
    if not WALLETS_PATH.exists():
        return {"ts": 0.0, "wallets": []}
    raw = json.loads(WALLETS_PATH.read_text())
    if "wallets" in raw:
        return raw
    # Legacy schema: {ts, addrs: [...]} — migrate
    ts = raw.get("ts", 0.0)
    return {"ts": ts, "wallets": [{"addr": a, "first_seen": ts, "last_in_topN": ts}
                                  for a in raw.get("addrs", [])]}


def refresh_wallet_list() -> dict:
    """Merge today's top-N into the existing tracked set (ADDITIVE — never drop).

    Returns the updated state dict written to wallets.json.
    """
    now = time.time()
    existing = _load_existing()
    by_addr = {w["addr"]: w for w in existing["wallets"]}
    top_now = _fetch_topN()
    top_set = set(top_now)

    # Update last_in_topN for any tracked wallet that's in today's top-N
    n_already = 0
    for addr in top_now:
        if addr in by_addr:
            by_addr[addr]["last_in_topN"] = now
            n_already += 1

    # Add new top-N entries, respecting MAX_TRACKED
    n_added, n_skipped_cap = 0, 0
    for addr in top_now:
        if addr in by_addr:
            continue
        if len(by_addr) >= MAX_TRACKED:
            n_skipped_cap += 1
            continue
        by_addr[addr] = {"addr": addr, "first_seen": now, "last_in_topN": now}
        n_added += 1

    n_total = len(by_addr)
    n_in_top = sum(1 for w in by_addr.values() if w["addr"] in top_set)
    n_historical = n_total - n_in_top
    state = {
        "ts": now, "wallets": list(by_addr.values()),
        "summary": {"n_total": n_total, "n_in_topN_today": n_in_top,
                    "n_historical_only": n_historical,
                    "n_added_this_refresh": n_added,
                    "n_skipped_at_cap": n_skipped_cap,
                    "cap": MAX_TRACKED, "top_n_target": TOP_N_ALLTIME},
    }
    print(f"[wallets] refresh: total={n_total} (in_top={n_in_top}, "
          f"historical={n_historical}, added={n_added}, skipped_at_cap={n_skipped_cap})")
    return state


def load_wallet_list(max_age_h: int = 24) -> list[str]:
    """Return list of wallet addresses to poll. Refreshes from leaderboard at most once per max_age_h."""
    existing = _load_existing()
    age_h = (time.time() - existing.get("ts", 0)) / 3600
    if existing["wallets"] and age_h < max_age_h:
        addrs = [w["addr"] for w in existing["wallets"]]
        print(f"[wallets] using cached list ({len(addrs)} addrs, age {age_h:.1f}h)")
        return addrs
    state = refresh_wallet_list()
    WALLETS_PATH.write_text(json.dumps(state, indent=2))
    return [w["addr"] for w in state["wallets"]]


def collect_one(addr: str) -> list[dict]:
    """Return new equity rows for one wallet. Empty list on failure."""
    try:
        p = info({"type": "portfolio", "user": addr})
    except Exception as e:
        print(f"  {addr[:10]}: FAIL {str(e)[:80]}", flush=True)
        return []
    rows = []
    for window, data in p:
        av = data.get("accountValueHistory", []) or []
        for entry in av:
            try:
                t, v = entry
                rows.append({"addr": addr, "bucket": window,
                             "t": int(t), "equity": float(v)})
            except (ValueError, TypeError):
                continue
    return rows


def main() -> int:
    DATA_DIR.mkdir(exist_ok=True)
    wallets = load_wallet_list()
    if not wallets:
        print("ERROR: empty wallet list", file=sys.stderr)
        return 1

    print(f"[collect] pulling portfolios for {len(wallets)} wallets...")
    t0 = time.time()
    all_new = []
    n_ok = n_fail = 0
    for i, addr in enumerate(wallets):
        rows = collect_one(addr)
        if rows:
            n_ok += 1
            all_new.extend(rows)
        else:
            n_fail += 1
        if (i + 1) % 25 == 0:
            print(f"  [{i+1}/{len(wallets)}] ok={n_ok} fail={n_fail} rows={len(all_new)}", flush=True)
        time.sleep(0.1)
    elapsed = time.time() - t0
    print(f"[collect] done in {elapsed:.1f}s — ok={n_ok}, fail={n_fail}, raw_rows={len(all_new)}")

    if not all_new:
        print("[collect] no rows returned; not writing")
        return 0

    new_df = pd.DataFrame(all_new)

    # Merge with existing parquet, dedupe
    if OUT_PATH.exists():
        existing = pd.read_parquet(OUT_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        existing = pd.DataFrame()
        combined = new_df
    before = len(combined)
    combined = combined.drop_duplicates(["addr", "bucket", "t"]).sort_values(["addr", "bucket", "t"])
    after = len(combined)
    added = after - len(existing)
    print(f"[write] existing={len(existing):,}, after_merge_dedupe={after:,}, added={added:,}")

    if added <= 0:
        print("[write] no new unique rows — skipping commit")
        return 0

    combined.to_parquet(OUT_PATH, index=False)
    # Write a tiny status file so GH UI can show last-run summary
    status = {
        "last_run_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "n_wallets_polled": len(wallets),
        "n_wallets_ok": n_ok,
        "n_wallets_fail": n_fail,
        "total_rows": after,
        "rows_added_this_run": added,
        "elapsed_sec": round(elapsed, 1),
    }
    (DATA_DIR / "status.json").write_text(json.dumps(status, indent=2))
    print(f"[write] saved {OUT_PATH} ({OUT_PATH.stat().st_size/1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
