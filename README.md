# substudy

Local-first workflow for English study from downloaded short videos.

## What this adds

- Multi-account source config in `config/sources.toml`
- One CLI for:
  - downloading media/subtitles/metadata with `yt-dlp`
  - reverse-incremental backfill for older posts
  - maintaining a master ledger (`SQLite` + `CSV`)
  - incremental ASR subtitle generation
  - per-video loudness analysis for inter-video volume normalization
  - EIJIRO dictionary indexing for subtitle word hover lookup
  - download run/error tracking for retry-safe operations
  - local TikTok-style study web UI (`web`) with favorites/bookmarks/notes
- macOS automation with two launchd jobs:
  - daily incremental run
  - weekly full-ledger run

## Layout

- `scripts/substudy.py`: main CLI (`sync`, `backfill`, `ledger`, `asr`, `loudness`, `dict-index`, `downloads`, `web`)
- `scripts/web/index.html`: study UI shell
- `scripts/web/app.js`: feed/subtitle/bookmark interactions + shortcuts
- `scripts/web/styles.css`: TikTok-style vertical feed design
- `scripts/run_daily_sync.sh`: daily incremental wrapper (`sync`/`backfill`/`ledger`/`loudness`/`asr`)
- `scripts/run_weekly_full_sync.sh`: weekly full wrapper (includes `brew upgrade yt-dlp` + `loudness`)
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

6. Analyze loudness (inter-video normalization gain):

```bash
python3 scripts/substudy.py loudness
```

7. Index dictionary entries (for subtitle word hover):

```bash
python3 scripts/substudy.py dict-index --dictionary-path data/eijiro-1449.utf8.txt
```

8. Inspect recent download logs and pending failures:

```bash
python3 scripts/substudy.py downloads --since-hours 24
```

9. Rebuild ledger fully when needed:

```bash
python3 scripts/substudy.py sync --full-ledger
```

10. Launch local study web UI:

```bash
python3 scripts/substudy.py web --host 127.0.0.1 --port 8876
```

Open `http://127.0.0.1:8876`.

## Web study features (MVP)

- 9:16 vertical video feed with up/down navigation (`↑/↓`, `J/K`, wheel, swipe)
- Auto-advance on video end with 3-second countdown and cancel button
- Continuous playback toggle (`A`)
- Inter-video volume normalization toggle (`N`)
- Subtitle overlay + selectable track
- Hover English words in subtitle overlay to open dictionary popup
- Hover dictionary debug/emulation (CLI):
  - reproduce lookup terms and ranking for a sentence/word pair
  - useful for tuning collocation ordering without manual UI retries
  - example:
    ```bash
    python3 scripts/dict_hover_emulator.py \
      --db data/master_ledger.sqlite \
      --sentence "So last month I kind of made a fool of myself by um" \
      --word made
    ```
- Subtitle bookmarks:
  - save current subtitle (`B`)
  - save playback range (`R` start, `T` save)
  - bookmark memo per entry
- Video favorite toggle (`F`)
- Video memo save

Learning tables added to the same SQLite ledger:

- `video_favorites(source_id, video_id, created_at)`
- `subtitle_bookmarks(id, source_id, video_id, track, start_ms, end_ms, text, note, created_at)`
- `video_notes(id, source_id, video_id, note, created_at, updated_at)`

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

## Dictionary source prep (EIJIRO)

Use EIJIRO as source data with `cp932` decoding, then keep a UTF-8 normalized copy for editor-friendly access.

- source: `/Users/bakemocho/Library/Mobile Documents/com~apple~CloudDocs/Foundation/dict/EIJIRO-1449.TXT`
- normalized copy (local artifact): `data/eijiro-1449.utf8.txt`
- note: `data/` is gitignored in this repo.

Generate/update normalized copy:

```bash
python3 - <<'PY'
from pathlib import Path
src = Path('/Users/bakemocho/Library/Mobile Documents/com~apple~CloudDocs/Foundation/dict/EIJIRO-1449.TXT')
dst = Path('data/eijiro-1449.utf8.txt')
text = src.read_bytes().decode('cp932')
text = text.replace('\r\n', '\n').replace('\r', '\n').replace('\x85', '\n')
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(text, encoding='utf-8', newline='\n')
print(dst)
PY
```

## Roadmap: Dictionary + NLP + Translation (planned)

Planned next steps for grammar-aware subtitle study UX.

### 0) EIJIRO dictionary ingestion

- Current status: initial implementation done (`dict-index` + hover lookup backend).
- Target source: `/Users/bakemocho/Library/Mobile Documents/com~apple~CloudDocs/Foundation/dict/EIJIRO-1449.TXT`
- Encoding note (verified): strict decode with `cp932` (`windows-31j`) succeeds for the full file.
- `shift_jis` fails on vendor extension bytes (example: `0xFB..`), so ingestion should not assume strict Shift_JIS.
- To improve usability, keep a normalized UTF-8 artifact in this project (or SQLite-only import) and treat the original as source-of-truth input.
- Build a one-time dictionary index pipeline (planned command: `substudy.py dict-index`):
  - Parse line-oriented entries from EIJIRO.
  - Normalize keys (lowercase, lemma-like form, punctuation cleanup).
  - Save to SQLite (`dict_entries` + `dict_entries_fts` for fast lookup).
  - Optionally export UTF-8 TSV/JSONL for editor-friendly inspection.

### 1) Subtitle NLP cache (token/POS/dependency)

- Reuse structure ideas from:
  - `/Users/bakemocho/gitwork_bk/vseg/avseg/inorder_dependency.py`
  - `/Users/bakemocho/gitwork_bk/vseg/avseg/chunkify_segments.py`
- English parsing target: spaCy `en_core_web_sm` (token, lemma, POS, dependency head).
- Persist per-cue analysis in SQLite cache (planned table: `subtitle_nlp_cache`):
  - `source_id`, `video_id`, `track`, `start_ms`, `end_ms`, `text_hash`
  - `tokens_json`, `deps_json`, `importance_json`, `analyzed_at`

### 2) Study UI enhancements

- POS-based color coding for subtitle tokens.
- Importance-based emphasis (frequency/learning priority).
- Dependency arrows rendered above subtitle text (graphical syntax guidance).
- Token click/hover opens dictionary panel with EIJIRO candidates.

### 3) LLM translation track (EN -> JA)

- Add optional translated subtitle track in addition to original English.
- Cache translation results in SQLite (planned table: `subtitle_translations`):
  - key: cue identity + `text_hash` + model/version
  - value: translated text, metadata, timestamps
- UI toggle between original and translated subtitle track.

### 4) Automation rollout

- Daily/weekly jobs already run `sync/backfill/ledger/loudness/asr`.
- After manual validation, optionally add `nlp` and `translate` stages as opt-in automation.
- Keep translation stage disable-by-default until quality and token cost are measured.
- `bep-eng.txt` is a reading dictionary and is currently out of scope for this pipeline.

## Add more creators

Add another `[[sources]]` block in `config/sources.toml`.
Each source can point to its own directory under `/Users/bakemocho/Audio/tiktok/english`.
