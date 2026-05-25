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

# Tracked-wallet selection rule
TOP_N_ALLTIME = 150
MIN_WEEK_VLM = 1_000_000  # require recent activity ≥$1M weekly volume

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


def refresh_wallet_list() -> list[str]:
    """Re-fetch leaderboard and pick top wallets by allTime PnL, filtered to active."""
    r = requests.get(LB_URL, timeout=30, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    rows = r.json()["leaderboardRows"]
    enriched = []
    for row in rows:
        addr = row["ethAddress"].lower()
        all_pnl = 0.0
        week_vlm = 0.0
        for window, m in row.get("windowPerformances", []):
            if window == "allTime":
                all_pnl = float(m.get("pnl", 0))
            elif window == "week":
                week_vlm = float(m.get("vlm", 0))
        if week_vlm >= MIN_WEEK_VLM:
            enriched.append({"addr": addr, "all_pnl": all_pnl, "week_vlm": week_vlm})
    enriched.sort(key=lambda r: r["all_pnl"], reverse=True)
    selected = [e["addr"] for e in enriched[:TOP_N_ALLTIME]]
    return selected


def load_wallet_list(max_age_h: int = 24) -> list[str]:
    """Use cached list if fresh, else refresh."""
    if WALLETS_PATH.exists():
        meta = json.loads(WALLETS_PATH.read_text())
        age_h = (time.time() - meta.get("ts", 0)) / 3600
        if age_h < max_age_h and meta.get("addrs"):
            print(f"[wallets] using cached list ({len(meta['addrs'])} addrs, age {age_h:.1f}h)")
            return meta["addrs"]
    print("[wallets] refreshing from leaderboard...")
    addrs = refresh_wallet_list()
    WALLETS_PATH.write_text(json.dumps({"ts": time.time(), "addrs": addrs}, indent=2))
    print(f"[wallets] selected {len(addrs)} wallets")
    return addrs


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
