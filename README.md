# substudy

Local-first workflow for English study from downloaded short videos.

## What this adds

- Multi-account source config in `config/sources.toml`
- One CLI for:
  - downloading media/subtitles/metadata with `yt-dlp`
  - reverse-incremental backfill for older posts
  - maintaining a master ledger (`SQLite` + `CSV`)
  - incremental ASR subtitle generation
  - download run/error tracking for retry-safe operations
- macOS automation with two launchd jobs:
  - daily incremental run
  - weekly full-ledger run

## Layout

- `scripts/substudy.py`: main CLI (`sync`, `backfill`, `ledger`, `asr`, `downloads`)
- `scripts/run_daily_sync.sh`: daily incremental wrapper
- `scripts/run_weekly_full_sync.sh`: weekly full wrapper (includes `brew upgrade yt-dlp`)
- `scripts/install_launchd.sh`: install/update launchd jobs
- `config/sources.example.toml`: example config
- `data/master_ledger.sqlite`: generated ledger DB
- `data/master_ledger.csv`: generated ledger CSV

## Quick start

1. Copy config and edit:

```bash
cp config/sources.example.toml config/sources.toml
```

2. Dry-run daily sync:

```bash
python3 scripts/substudy.py sync --dry-run
```

3. Run daily sync (incremental ledger):

```bash
python3 scripts/substudy.py sync
```

4. Run older-post backfill (one window) incrementally:

```bash
python3 scripts/substudy.py backfill
```

Set `backfill_enabled = true` globally or per source before using it.

5. Run ASR incrementally:

```bash
python3 scripts/substudy.py asr
```

6. Inspect recent download logs and pending failures:

```bash
python3 scripts/substudy.py downloads --since-hours 24
```

7. Rebuild ledger fully when needed:

```bash
python3 scripts/substudy.py sync --full-ledger
```

## Settings that matter

- `playlist_end`
  - Limits daily fetch scope to recent top-N items to avoid full remote traversal.
- `backfill_enabled`, `backfill_window`, `backfill_windows_per_run`
  - Backfill older ranges progressively (`playlist_start`/`playlist_end` windows).
  - When a source reaches the tail, it is automatically marked `completed` and stops running.
- `backfill_start`
  - Optional explicit initial cursor. If unset, starts from `playlist_end + 1`.
- `break_on_existing`, `break_per_input`, `lazy_playlist`
  - Optional listing controls. Keep `break_on_existing=false` for TikTok profiles unless you have verified ordering safety.
- `media_archive` / `subs_archive`
  - If missing, `sync` bootstraps archive IDs from existing local files to avoid repeated re-fetch attempts.
- `asr_*`
  - ASR is primary subtitle pipeline.
  - TikTok subtitles (`subs_dir`) are supplemental.

## Download Logging + Retry

- `download_runs` table:
  - One row per sync stage execution (`media`, `subs`, `meta`) with status and counts.
- `download_state` table:
  - Per-video latest status for each stage.
  - Failed `meta` items are auto-retried in later `sync` runs with exponential backoff.

The current retry scope is metadata stage (`meta`), which has explicit per-video targets.
Metadata retries now honor `next_retry_at` (backoff), so failed IDs are not re-hit on every run.

## Reverse Incremental Backfill

- `backfill` keeps per-source cursor state in `backfill_state`.
- Each run processes `backfill_windows_per_run` windows of size `backfill_window`.
- End detection:
  - A window with 0 discovered IDs marks completion.
  - A window with discovered IDs `< backfill_window` also marks completion.
- Control options:
  - `python3 scripts/substudy.py backfill --windows 3`
  - `python3 scripts/substudy.py backfill --reset`

## Daily + weekly automation (macOS)

Install both jobs with defaults:

- daily: `06:30`
- weekly full: `Sunday 07:00` (`Weekday=0`) + `yt-dlp` Homebrew upgrade

```bash
./scripts/install_launchd.sh
```

Custom schedule:

```bash
./scripts/install_launchd.sh <daily_hour> <daily_minute> <weekly_weekday> <weekly_hour> <weekly_minute> [label_prefix]
```

Example:

```bash
./scripts/install_launchd.sh 6 30 0 7 0 com.bakemocho.substudy
```

Check jobs:

```bash
launchctl list | rg substudy
```

## Add more creators

Add another `[[sources]]` block in `config/sources.toml`.
Each source can point to its own directory under `/Users/bakemocho/Audio/tiktok/english`.
