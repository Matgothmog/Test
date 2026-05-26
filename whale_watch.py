"""Whale #12 watcher — snapshot their full position book every cron run.

Runs alongside the main 794-wallet portfolio collector but at a higher cadence
(every 15min). Each run appends a row PER POSITION to data/whale12_positions.parquet,
giving us forward time-series of:
  - which positions are open
  - when each was opened/closed/flipped
  - position size evolution (scale-ups / scale-downs)
  - unrealized PnL trajectory per position

Critical for validating: (a) does the whale really hold losers long-term? (b) what's
their actual book size over time? (c) when do they open new positions vs rebalance?
"""
from __future__ import annotations
import os, sys, time, json
import requests
import pandas as pd
from pathlib import Path

INFO_URL = "https://api.hyperliquid.xyz/info"
WHALE = "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05"
DATA_DIR = Path(__file__).parent / "data"
OUT = DATA_DIR / "whale12_positions.parquet"
STATUS = DATA_DIR / "whale12_status.json"

UA = "hl-whale-watcher/1.0 (GitHub Actions)"


def info(payload: dict, retries: int = 6, backoff: float = 1.5):
    for attempt in range(retries):
        try:
            r = requests.post(INFO_URL, json=payload, timeout=30,
                              headers={"Content-Type": "application/json", "User-Agent": UA})
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(backoff * (1.6 ** attempt), 15.0)); continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == retries - 1: raise
            time.sleep(min(backoff * (1.6 ** attempt), 15.0))
    raise RuntimeError("info() exhausted")


def snap_positions() -> tuple[pd.DataFrame, dict]:
    now = int(time.time() * 1000)
    cs = info({"type": "clearinghouseState", "user": WHALE})
    acc_val = float(cs["marginSummary"]["accountValue"])
    withdrawable = float(cs.get("withdrawable", 0))

    # Also pull current funding rates per coin so we know whether whale is
    # receiving or paying funding on each leg right now
    funding_by_coin = {}
    try:
        meta_ctxs = info({"type": "metaAndAssetCtxs"})
        meta = meta_ctxs[0]
        ctxs = meta_ctxs[1]
        for u, ctx in zip(meta["universe"], ctxs):
            try:
                funding_by_coin[u["name"]] = float(ctx.get("funding", 0) or 0)
            except (TypeError, ValueError):
                continue
    except Exception:
        pass  # funding columns will be NaN

    rows = []
    for p in cs.get("assetPositions", []):
        pp = p.get("position", {})
        try:
            sz = float(pp.get("szi", 0))
        except (TypeError, ValueError):
            continue
        if abs(sz) < 1e-9: continue
        try:
            entry_px = float(pp.get("entryPx", 0) or 0)
            upnl = float(pp.get("unrealizedPnl", 0) or 0)
            pos_val = float(pp.get("positionValue", 0) or 0)
            margin = float(pp.get("marginUsed", 0) or 0)
            roe = float(pp.get("returnOnEquity", 0) or 0)
        except (TypeError, ValueError):
            continue
        coin = pp.get("coin", "")
        direction = 1 if sz > 0 else -1
        # Funding: positive rate = longs pay shorts
        # Whale's per-hour funding $: -rate * pos_val * direction
        # (long with positive rate => negative income; short with positive rate => positive income)
        fund_rate = funding_by_coin.get(coin)
        if fund_rate is not None:
            fund_per_hour_usd = -fund_rate * pos_val * direction
            fund_per_day_usd = fund_per_hour_usd * 24
            fund_apr_for_whale_pct = -fund_rate * direction * 24 * 365 * 100
        else:
            fund_per_hour_usd = fund_per_day_usd = fund_apr_for_whale_pct = None
        rows.append({
            "snap_t": now, "coin": coin,
            "direction": direction, "size_abs": abs(sz),
            "entry_px": entry_px, "position_value": pos_val,
            "margin_used": margin, "unrealized_pnl": upnl,
            "return_on_equity": roe,
            "liq_px": pp.get("liquidationPx") or None,
            "funding_rate_per_hour": fund_rate,
            "funding_per_hour_usd": fund_per_hour_usd,    # >0 = whale receives
            "funding_per_day_usd": fund_per_day_usd,
            "funding_apr_for_whale_pct": fund_apr_for_whale_pct,
        })
    df = pd.DataFrame(rows)
    status = {
        "snap_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "snap_t_ms": now,
        "account_value": acc_val,
        "withdrawable": withdrawable,
        "n_positions": len(df),
        "n_long": int((df["direction"] > 0).sum()) if not df.empty else 0,
        "n_short": int((df["direction"] < 0).sum()) if not df.empty else 0,
        "total_unrealized_pnl": float(df["unrealized_pnl"].sum()) if not df.empty else 0,
        # Funding aggregates (per current rates × current notional)
        "total_funding_per_day_usd": (float(df["funding_per_day_usd"].sum())
                                       if not df.empty and df["funding_per_day_usd"].notna().any() else None),
        "n_positions_receiving_funding": (int((df["funding_per_day_usd"] > 0).sum())
                                           if not df.empty and df["funding_per_day_usd"].notna().any() else None),
        "n_positions_paying_funding": (int((df["funding_per_day_usd"] < 0).sum())
                                        if not df.empty and df["funding_per_day_usd"].notna().any() else None),
        "biggest_winner": (df.loc[df["unrealized_pnl"].idxmax()].to_dict()
                            if not df.empty and (df["unrealized_pnl"] > 0).any() else None),
        "biggest_loser": (df.loc[df["unrealized_pnl"].idxmin()].to_dict()
                          if not df.empty and (df["unrealized_pnl"] < 0).any() else None),
        "worst_funding_drain": (df.loc[df["funding_per_day_usd"].idxmin()].to_dict()
                                 if not df.empty and df["funding_per_day_usd"].notna().any()
                                 and (df["funding_per_day_usd"] < 0).any() else None),
        "best_funding_income": (df.loc[df["funding_per_day_usd"].idxmax()].to_dict()
                                 if not df.empty and df["funding_per_day_usd"].notna().any()
                                 and (df["funding_per_day_usd"] > 0).any() else None),
    }
    return df, status


def main():
    DATA_DIR.mkdir(exist_ok=True)
    df_new, status = snap_positions()
    print(f"[whale-watch] snap: {status['n_positions']} positions, "
          f"acct=${status['account_value']:,.0f}, "
          f"sum uPnL=${status['total_unrealized_pnl']:+,.0f}", flush=True)

    if df_new.empty:
        print("[whale-watch] no positions returned — exiting without write"); return

    if OUT.exists():
        existing = pd.read_parquet(OUT)
        combined = pd.concat([existing, df_new], ignore_index=True)
        before = len(combined)
        combined = combined.drop_duplicates(["snap_t", "coin"]).sort_values(["snap_t", "coin"])
        after = len(combined)
        added = after - len(existing)
    else:
        combined = df_new
        added = len(df_new)

    combined.to_parquet(OUT, index=False)
    print(f"[whale-watch] parquet now {len(combined):,} rows ({added:+} from this run)")

    STATUS.write_text(json.dumps(status, indent=2, default=str))
    print(f"[whale-watch] wrote status to {STATUS}")


if __name__ == "__main__":
    main()
