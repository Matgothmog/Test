# HL Portfolio Snapshot Collector

Polls the Hyperliquid `portfolio` endpoint for the top ~150 active wallets
every hour, appends new equity points to `data/portfolios.parquet`, and commits
the result back to the repo. Designed to run on GitHub Actions free tier.

## Why
We need ≥4 weeks of higher-resolution per-wallet equity history to test
"streak-weighted" copy-trading signals — the HL API only exposes ~30 days of
limited-resolution `accountValueHistory` per call, so we must accumulate it forward.

## What it costs
$0. Runs on GitHub Actions:
  - Public repo: unlimited free minutes
  - Private repo: 2,000 free min/month — we use ~22 (one ~45s run/hour × 30 days)
  - Storage: parquet grows ~5–10 MB / 4 weeks, well under any limit

## Layout

    .
    ├── collector.py                # main script (one HTTP-pull pass per run)
    ├── .github/workflows/collect.yml   # hourly cron
    ├── data/
    │   ├── portfolios.parquet      # accumulated equity timeseries (created on first run)
    │   ├── wallets.json            # cached wallet list (refreshed daily inside collector)
    │   └── status.json             # last-run summary for quick monitoring
    └── README.md

## Setup (5 minutes, all in your browser)

1. **Create a new repo on GitHub.** Public is fine (cheaper, free unlimited
   minutes). Name it e.g. `hl-portfolio-snapshots`.

2. **Push these files.** Either:
   - Clone the new empty repo locally, copy the contents of this
     `cloud_collector/` directory into it, `git add -A && git commit -m "init"
     && git push`, OR
   - Use the GitHub web UI: "Add file → Upload files" and drag the 4 files in
     (collector.py, .gitignore, README.md, and the .github/ directory). Commit
     directly to main.

3. **Enable Actions.** Go to the repo's **Actions** tab. GitHub will show
   "Workflows aren't being run on this repository — I understand my workflows,
   go ahead and enable them." Click it.

4. **Trigger the first run manually.** In the Actions tab, click
   "hourly-portfolio-snapshot" → "Run workflow" → "Run workflow". The first
   run takes ~1 minute; once it completes successfully, the cron takes over.

5. **(Optional) Failure alerts via Discord.** In repo Settings → Secrets and
   variables → Actions → New repository secret. Name `DISCORD_WEBHOOK`, value =
   your Discord webhook URL. The workflow will ping it on failure. (Without
   this, GitHub still emails the repo owner on failure.)

## Monitoring

- **Run history**: Actions tab shows every run with green / red status and
  duration. Click any run for full logs.
- **Email alerts**: GitHub emails the repo owner on workflow failure by
  default (configurable at Settings → Notifications).
- **Last-run summary**: `data/status.json` is rewritten each successful run
  with `last_run_utc`, `n_wallets_ok`, `n_wallets_fail`, `rows_added_this_run`,
  `total_rows`. View at `https://github.com/<you>/<repo>/blob/main/data/status.json`.
- **Optional Discord**: see step 5.

## Cost / time sanity check

| | |
|---|---|
| API calls per run | ~150 wallets × 1 endpoint = 150 |
| API calls per day | 150 × 24 runs = 3,600 |
| Wall time per run | ~45 seconds |
| GitHub Actions minutes per month | ~22 (vs 2,000 free quota) |
| Storage growth | ~150–300 KB per day, ~5–10 MB per 4 weeks |
| Total cost | $0 |

## Pulling data back to local for analysis

```bash
git clone https://github.com/<you>/hl-portfolio-snapshots.git
cd hl-portfolio-snapshots
python -c "import pandas as pd; df = pd.read_parquet('data/portfolios.parquet'); print(df.shape); print(df.head())"
```

Or just download `data/portfolios.parquet` from the web UI.

## Local sanity test before deploying

```bash
cd cloud_collector/
python collector.py
ls -la data/
cat data/status.json
```

Should produce `data/portfolios.parquet` and `data/status.json` after ~45s.
If this works locally, the workflow will work in GitHub Actions.

## Tuning

- **Track more/fewer wallets**: edit `TOP_N_ALLTIME` and `MIN_WEEK_VLM` in
  `collector.py`. Keep total wallets ≤ ~300 to stay within the 8-minute job
  timeout in the workflow.
- **Change cadence**: edit `cron` in `.github/workflows/collect.yml`. Format
  is standard cron. The free quota easily supports every 30 min (still <50
  min/month for runtime).
- **Pause the collector**: comment out the `schedule:` block in the workflow
  YAML; manual `workflow_dispatch` still works.
