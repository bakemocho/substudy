# Technical Guide

This document is for developer/ops-oriented details that are too internal for the public README.

## Release checks

Run privacy check before sharing/publishing:

```bash
make privacy-check
```

GitHub Actions also runs this check on pull requests and pushes to `master`
via `.github/workflows/privacy-check.yml`.

## Project layout

- `scripts/substudy.py`: main CLI (`sync`, `backfill`, `ledger`, `asr`, `loudness`, `dict-index`, `downloads`, `web`)
- `scripts/web/index.html`: study UI shell
- `scripts/web/app.js`: feed/subtitle/bookmark interactions + shortcuts
- `scripts/web/styles.css`: TikTok-style vertical feed design
- `scripts/run_daily_sync.sh`: daily incremental wrapper (`sync`/`backfill`/`ledger`/`loudness`/`asr`)
- `scripts/run_weekly_full_sync.sh`: weekly full wrapper (includes `brew upgrade yt-dlp` + `loudness`)
- `scripts/install_launchd.sh`: install/update launchd jobs
- `docs/subtitle-translation.md`: 字幕和訳の手順書/要件
- `docs/roadmap.md`: implementation roadmap and status
- `docs/extension-ingest-mvp.md`: browser extension ingest migration plan
- `config/sources.example.toml`: example config
- `data/master_ledger.sqlite`: generated ledger DB
- `data/master_ledger.csv`: generated ledger CSV

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
- `cookies_file` / `cookies_from_browser`
  - `cookies_file` is preferred for scheduled runs to avoid macOS keychain password popups.
  - If both are set, `cookies_file` is used first; browser cookies are fallback only.
  - To fully disable browser access, set `cookies_from_browser = ""`.
  - Refresh command example:
    `yt-dlp --cookies-from-browser chrome --cookies ~/.local/share/substudy/cookies.txt "https://www.tiktok.com"`
- `media_archive` / `subs_archive`
  - If missing, `sync` bootstraps archive IDs from existing local files to avoid repeated re-fetch attempts.
- `asr_*`
  - ASR is primary subtitle pipeline.
  - `asr_command` can use local Whisper CLI (no OpenAI API key required).
  - TikTok subtitles (`subs_dir`) are supplemental.

## Download logging and retry

- `download_runs` table:
  - One row per sync stage execution (`media`, `subs`, `meta`) with status and counts.
- `download_state` table:
  - Per-video latest status for each stage.
- Failed `meta` and `subs` items are auto-retried in later `sync` runs with exponential backoff.
- Subtitle stage now runs on explicit per-video targets (new media IDs and due retries) instead of re-scanning the full profile feed.
- Retries honor `next_retry_at` (backoff), so failed IDs are not re-hit on every run.

## Reverse incremental backfill

- `backfill` keeps per-source cursor state in `backfill_state`.
- Each run processes `backfill_windows_per_run` windows of size `backfill_window`.
- End detection:
  - A window with 0 discovered IDs marks completion.
  - A window with discovered IDs `< backfill_window` also marks completion.
- Control options:
  - `python3 scripts/substudy.py backfill --windows 3`
  - `python3 scripts/substudy.py backfill --reset`

## Daily and weekly automation (macOS)

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
./scripts/install_launchd.sh 6 30 0 7 0 com.substudy
```

Check jobs:

```bash
launchctl list | rg substudy
```

## Dictionary source prep (EIJIRO)

Use EIJIRO as source data with `cp932` decoding, then keep a UTF-8 normalized copy for editor-friendly access.

- source (example): `~/path/to/EIJIRO-1449.TXT`
- normalized copy (local artifact): `data/eijiro-1449.utf8.txt`
- note: `data/` is gitignored in this repo.

Generate/update normalized copy:

```bash
python3 - <<'PY'
from pathlib import Path
src = Path('~/path/to/EIJIRO-1449.TXT').expanduser()
dst = Path('data/eijiro-1449.utf8.txt')
text = src.read_bytes().decode('cp932')
text = text.replace('\r\n', '\n').replace('\r', '\n').replace('\x85', '\n')
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(text, encoding='utf-8', newline='\n')
print(dst)
PY
```

## Add more creators

Add another `[[sources]]` block in `config/sources.toml`.
Each source can point to its own directory under your local `base_data_dir` (for example `~/substudy-data`).

Example:

```toml
[global]
base_data_dir = "~/substudy-data"

[[sources]]
id = "creator_a"
data_dir = "creator_a"
```
