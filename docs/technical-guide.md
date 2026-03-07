# Technical Guide

This document is for developer/ops-oriented details that are too internal for the public README.

## Release checks

Run privacy check before sharing/publishing:

```bash
make privacy-check
```

GitHub Actions also runs this check on pull requests and pushes to `master`
via `.github/workflows/privacy-check.yml`.

## Playwright verification policy

Use Playwright for UI regressions where query params, dropdown filters, and
fallback behavior can break in browser-only flows.

### 1) Run in an isolated temp workspace

- Do not add Playwright dependencies to this repository unless explicitly planned.
- Use a throwaway directory (example: `/tmp/substudy-pw`) for one-shot checks.
- Use a local npm cache path to avoid permission issues:
  `NPM_CONFIG_CACHE=/tmp/substudy-npm-cache`.

Example setup:

```bash
mkdir -p /tmp/substudy-pw
cd /tmp/substudy-pw
NPM_CONFIG_CACHE=/tmp/substudy-npm-cache npm init -y
NPM_CONFIG_CACHE=/tmp/substudy-npm-cache npm install -D @playwright/test
```

### 2) Always test against a dedicated local server

- Start `substudy web` from the current working tree on a dedicated port
  (example: `8890`) to avoid stale behavior from older processes.
- When backend logic changed, restart the server before browser validation.

Example:

```bash
python3 scripts/substudy.py web --host 127.0.0.1 --port 8890
```

### 3) Verify both reproduction and fixed behavior

- If the issue report includes a failing URL, check it first as a reproduction.
- Run assertions for the intended behavior on the patched server.
- For source filtering fixes, assert that sources without playable media are not
  present in `#sourceSelect`.

### 4) Keep evidence and clean up

- In the task report, include: tested URL, selected query params, expected vs.
  observed option list, and pass/fail result.
- Stop temporary server processes after validation.
- Do not commit temporary Playwright files or caches under `/tmp`.

## Project layout

- `scripts/substudy.py`: main CLI (`sync`, `backfill`, `ledger`, `asr`, `loudness`, `dict-index`, `downloads`, `web`)
- `scripts/web/index.html`: study UI shell
- `scripts/web/app.js`: feed/subtitle/bookmark interactions + shortcuts
- `scripts/web/styles.css`: TikTok-style vertical feed design
- `scripts/run_daily_sync.sh`: daily queue wrapper (`sync/backfill --execution-mode queue` producer + `queue-worker` lanes + incremental ledger)
- `scripts/run_weekly_full_sync.sh`: weekly queue wrapper (includes `brew upgrade yt-dlp`, queue producer/worker flow, full ledger)
- `scripts/install_launchd.sh`: install/update launchd jobs
- `docs/subtitle-translation.md`: 字幕和訳の手順書/要件
- `docs/local-translation-quality-lab.md`: ローカル翻訳品質の仮説検証ログ
- `docs/roadmap.md`: implementation roadmap and status
- `docs/extension-ingest-mvp.md`: browser extension ingest migration plan
- `config/sources.example.toml`: example config
- `data/master_ledger.sqlite`: generated ledger DB
- `data/master_ledger.csv`: generated ledger CSV

## Local translation quality hardening (opt-in)

Implemented pipeline:

- `translate-local` baseline stages:
  - `draft` (`20b`)
  - `refine` (`120b`)
  - `global` (`120b`)
- Optional quality loop (new):
  - rule-based `quality-gate` (`json_fragment`, `english_heavy`, `unchanged`)
  - `quality-audit` (LLM)
  - `quality-repair` (LLM targeted patch)
  - bounded rounds (`audit -> repair -> re-audit`)

Key options:

- `--quality-loop-max-rounds` (`0` disables loop)
- `--quality-enforce` (treat threshold miss as failure)
- `--quality-json-fragment-threshold`
- `--quality-english-heavy-threshold`
- `--quality-unchanged-threshold`
- `--quality-audit-model`, `--quality-repair-model`
- `--quality-audit-max-tokens`, `--quality-repair-max-tokens`

Operator note:

- For unstable 120b endpoints, keep `--limit` small and use conservative params:
  - `temperature=0`, `top_p=1`
  - larger token ceilings with smaller `chunk-size`

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
- `video_format`
  - Primary media download format.
  - If the primary file has no audio stream, `sync` keeps that video, fetches an audio donor with `-f download`, and muxes audio locally via `ffmpeg` (`-shortest`) so output duration follows the primary video length.
- `ytdlp_impersonate`
  - Optional yt-dlp impersonation target for request hardening (`chrome`, `edge`, `safari`, `firefox`, `tor`, or `auto`).
  - When set, substudy checks available targets from `yt-dlp --list-impersonate-targets` and applies `--impersonate`.
  - If requested target is unavailable, substudy warns once and falls back (or skips impersonation when none are available).
  - `auto` picks first available target from preferred order: `chrome -> edge -> safari -> firefox -> tor`.
- `asr_*`
  - ASR is primary subtitle pipeline.
  - `asr_command` can use local Whisper CLI (no OpenAI API key required).
  - TikTok subtitles (`subs_dir`) are supplemental.

## Download logging and retry

- `download_runs` table:
  - One row per sync stage execution (`media`, `subs`, `meta`) with status and counts.
- `download_state` table:
  - Per-video latest status for each stage.
- Failed `media`, `subs`, and `meta` items are auto-retried in later `sync` runs with exponential backoff.
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

Install launchd jobs with defaults:

- producer daily: `06:30`
- producer weekly full: `Sunday 07:00` (`Weekday=0`) + `yt-dlp` Homebrew upgrade
- worker media: every `300s` (`StartInterval`)
- worker pipeline: every `300s` (`StartInterval`)

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

Optional install-time overrides:

- Disable worker jobs:
  - `SUBSTUDY_ENABLE_WORKER_JOBS=0 ./scripts/install_launchd.sh`
- Change worker intervals:
  - `SUBSTUDY_MEDIA_WORKER_INTERVAL_SEC=180`
  - `SUBSTUDY_PIPELINE_WORKER_INTERVAL_SEC=120`
- Change queue-worker lease/poll:
  - `SUBSTUDY_QUEUE_WORKER_LEASE_SEC`
  - `SUBSTUDY_QUEUE_WORKER_POLL_SEC`
  - `SUBSTUDY_QUEUE_WORKER_MAX_ATTEMPTS`

Check jobs:

```bash
launchctl list | rg substudy
```

Manual producer/worker operation:

```bash
# producer: discovery + enqueue only (lock protected)
python3 scripts/substudy.py sync --config config/sources.toml --execution-mode queue --skip-ledger
python3 scripts/substudy.py backfill --config config/sources.toml --execution-mode queue --skip-ledger

# worker: media lane
python3 scripts/substudy.py queue-worker --config config/sources.toml --stage media

# worker: pipeline lane
python3 scripts/substudy.py queue-worker --config config/sources.toml --stage subs --stage meta --stage asr --stage loudness --stage translate --translate-target-lang ja-local --translate-source-track auto
```

Notes:

- `sync/backfill --execution-mode queue` use a shared producer lock (`data/locks/producer.lock`).
- If another producer is running, new producer runs exit immediately with lock-busy error.
- `queue-worker` has no producer lock and is safe to run multiple instances in parallel.
- launchd worker jobs set PATH explicitly (`/opt/homebrew/bin:/usr/local/bin:...`) so `ffmpeg/ffprobe` are discoverable.

Metered-link safeguards (`run_daily_sync.sh` / `run_weekly_full_sync.sh`):

- The scripts auto-detect likely metered links (e.g. iPhone USB, Bluetooth PAN, hotspot gateway `172.20.10.1`, hotspot-like SSID) and pass:
  - Also treats `ipconfig getsummary <iface>` with `IsExpensive : TRUE` as metered.
  - `--metered-media-mode updates-only`
  - `--metered-min-archive-ids <N>`
  - `--metered-playlist-end <N>`
- In `updates-only` mode:
  - sources with too few archived media IDs are skipped for media download
  - seeded sources download only recent update range (forced `break-on-existing` + capped playlist window)
  - `backfill` historical windows are skipped entirely
- Overrides:
  - `SUBSTUDY_METERED_LINK_MODE=auto|on|off`
  - `SUBSTUDY_METERED_MIN_ARCHIVE_IDS` (default: `200`)
  - `SUBSTUDY_METERED_PLAYLIST_END` (default: `40`)

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
