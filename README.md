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
- `docs/subtitle-translation.md`: 字幕和訳の手順書/要件
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

## Roadmap (updated)

Current priority is subtitle-first study UX: fast cue navigation + EN/JA parallel reading.

### Immediate: playback-card UX polish + dictionary continuity

Goal: reduce visual/interaction friction during continuous study.

- Add progress timer in video card (bottom-right overlay):
  - circular pie-style timer on top of video card
  - base color: semi-transparent milky gray/white
  - fill grows clockwise from 12 o'clock based on playback progress ratio
  - near video end, filled semi-transparent area should cover the full circle
  - keep timer in sync with pause/seek/replay
- Add viewport snap behavior for video card:
  - card snaps to top/bottom edges while scrolling (sticky "click" feeling)
  - apply slight resistance/hysteresis when leaving snapped state
  - avoid scroll-chain conflict with subtitle reel / bookmark panel internal scroll
- Expand hover dictionary targets:
  - enable same hover dictionary in English words inside subtitle reel rows
  - enable same hover dictionary in English words inside saved bookmark text
  - keep existing subtitle-overlay hover behavior unchanged
- Keep reel focus stable while using hover dictionary:
  - when pointer is on subtitle-reel word/popup/bridge, pause reel auto-follow scroll
  - resume auto-follow only after hover session ends
- Place dictionary popup near hover source card:
  - overlay words -> popup in video card (current behavior)
  - reel words -> popup near reel card word
  - bookmark words -> popup near bookmark card word
  - avoid routing every popup to video card when source is outside overlay
- Add hover bridge between word and popup:
  - fill the gap with transparent hover-hit region so pointer can travel without losing context
  - prevent accidental re-target to words behind the gap while moving to popup
  - keep popup hide timing stable (no flicker while crossing)
- Define recursive dictionary tree interaction rules (graph-mode draft):
  - treat first popup as `lv.1` node (source word), then nested popup lookups as `lv.2+` child nodes
  - model is tree-only (parent -> child lookup path), no collocation/semantic edge expansion here
  - click on a lookup term pins the node (popup stays visible even when pointer leaves)
  - outside click or `Esc` clears the full tree and dictionary loop state
  - when hovering a node, freeze that node position; other nodes re-layout toward stable positions

Acceptance:

- Timer draw direction/fill matches actual video progress for play/pause/seek.
- Card snap feels deterministic: easy to snap, slightly resistant to unsnap.
- Hover dictionary opens from overlay/reel/bookmark consistently.
- While using hover dictionary in subtitle reel, active cue auto-scroll does not yank the view.
- Popup appears adjacent to the hover source region (overlay/reel/bookmark), not always in video card.
- Moving pointer from word to popup does not collapse/switch lookup unexpectedly.
- Click-pin / outside-click / `Esc` rules coexist with existing hover-hide behavior.
- In graph-mode, hovered node stays fixed while non-hovered nodes can settle around it.

Status (2026-02-10):

- Implemented:
  - video-card progress timer
  - viewport snap + release resistance
  - transparent hover bridge between word and popup
  - hover dictionary hooks in subtitle reel and saved bookmarks
- Needs polish:
  - reel hover session should temporarily freeze auto-follow scrolling
  - popup placement should anchor to source card (reel/bookmark) instead of video-card fixed placement
  - recursive tree graph-mode (pin + freeze + re-layout) is still design/implementation pending
  - hover dictionary behavior in non-overlay cards (subtitle reel/bookmark) still has UX tuning room
  - keep tuning hover stability/intent detection before marking this block fully done

### 0) Done / baseline

- EIJIRO dictionary indexing (`dict-index`) + hover lookup + collocation ranking.
- Hover dictionary UX improvements (copy, loop, scroll isolation, prefetch).
- Translation runbook + translation log table (`translation_runs`) in SQLite.
- EN/JA subtitle tracks can be added by file naming (`video_id.<lang>.<ext>`) and reflected by `ledger`.

### 1) Next: translated-availability filter (prerequisite for parallel cards)

Goal: enable stable study-mode feed slicing before EN/JA card expansion.

- Add feed filter mode:
  - `all`
  - `ja_only` (和訳済みのみ)
  - `ja_missing` (未和訳のみ)
- Source of truth:
  - subtitle track existence (`video_id.ja.*`)
  - optional `translation_runs` status as supplemental signal
- Keep behavior deterministic in UI:
  - persist selected filter in URL/query + local storage
  - apply filter without breaking shuffle/history behavior

Acceptance:

- Filtered feed count is consistent across reloads.
- `ja_only` mode never shows videos without JA track.
- Missing JA data degrades gracefully (no crash).

### 2) Next: subtitle reel in player side panel

Goal: repurpose the current "字幕ブックマーク" area as a subtitle reel panel.

- Replace panel default content with reel list centered on current cue.
- Reel row should show:
  - line 1: English cue
  - line 2: Japanese cue (if available)
- Keep cue click behavior: jump video to cue `start_ms`.
- Keep bookmark feature, but move it behind each reel row (row action) instead of occupying panel as primary view.

Acceptance:

- During playback, active cue stays centered and updates smoothly.
- Reel click seeks accurately.
- Existing bookmark APIs remain usable (no data migration required).

### 3) Next: EN/JA parallel subtitle mapping

Goal: stable 1:1 view for bilingual cues.

- Add client-side cue aligner:
  - primary key: `start_ms/end_ms`
  - fallback: nearest timestamp within threshold
- Rendering rules:
  - JA missing -> show EN only
  - EN missing (rare) -> show JA with marker
- Add panel toggle:
  - `EN only`
  - `EN + JA` (default for study mode)

Acceptance:

- For typical tracks, most cues align without visible mismatch.
- No crash/blank panel when one language track is missing.

### 4) Next: similar-scene suggestion via cue embeddings

Goal: let users discover semantically similar scenes from current subtitle context.

- Build embeddings for subtitle cue windows (current cue + surrounding cues).
- Index vectors locally (FAISS or equivalent local ANN index).
- Add `Similar Scenes` action from current cue / subtitle reel row.
- Retrieval rules:
  - exclude near-duplicate neighbors in same video/time neighborhood
  - return diverse results across videos/sources when possible
- Result card should show:
  - EN cue text
  - JA cue text (if available)
  - source/video and timestamp
  - click-to-jump behavior

Acceptance:

- Query from current cue returns relevant top-k candidates quickly.
- Results are not dominated by same-video adjacent cues.
- Jump from suggestion lands on correct timestamp and updates subtitle state.

### 5) Next: hover dictionary expansion (bookmark + recursive tree graph)

Goal: make hover dictionary a reusable learning surface.

- Add dictionary bookmark action in hover popup.
  - bookmark unit: term/definition + video context (source/video/track/time/cue text)
  - persist in dedicated table (`dictionary_bookmarks`)
- Add recursive hover inside dictionary popup.
  - hovering English words in definitions opens next-level popup lookup
  - build parent->child tree nodes by actual lookup path (`lv.1`, `lv.2`, ...)
  - keep depth guard + visited-term guard to prevent infinite loop
- Graph-mode rendering policy:
  - tree visualization only (no collocation edges)
  - hovered node is temporarily fixed in place
  - non-hovered nodes can continue force-layout relaxation
  - click-pinned node remains fixed/visible until explicit clear
- Prerequisite alignment with Immediate block:
  - finalize popup placement/focus policy across overlay/reel/bookmark contexts
  - finalize reel auto-follow freeze/resume policy during dictionary hover sessions
  - finalize click-pin / outside-click / `Esc` coexistence rules
  - treat these as mandatory before enabling recursive graph-mode by default
- Keep MVP storage model simple:
  - subtitle JSON migration is not required for this step
  - can be implemented with existing subtitle files + SQLite metadata

Acceptance:

- Popup bookmark toggle is responsive and persists after reload.
- Popup context policy is deterministic before recursive hover is enabled globally.
- Recursive tree graph can grow to `lv.2+` via lookup-in-lookup without UI lock/conflict.
- Hovered node freeze + surrounding-node re-layout behavior is visually stable.
- click-pin / outside-click / `Esc` clear behavior is deterministic.
- Existing subtitle hover behavior remains intact.

### 6) Next: reel linguistics overlay (NLP + dependencies)

Goal: treat subtitle reel as grammar visualization surface.

- Add English token/POS layer for each reel row.
- Add dependency edges (head -> dependent) as overlay arrows on reel lines.
- Color-code tokens by POS category.
- Add EN/JA alignment color hints:
  - same color group for likely aligned EN token span and JA phrase span
  - fallback to neutral color when alignment confidence is low
- Keep interactions readable:
  - overlay can be toggled on/off
  - hover dictionary and dependency overlay do not conflict

Acceptance:

- Dependency arrows render without blocking line selection/click.
- POS colors are consistent across videos and tracks.
- EN/JA alignment coloring remains stable when seeking/scrolling.

### 7) Quality/performance pass for reel + dictionary coexistence

- Ensure wheel/keyboard focus rules are unambiguous between:
  - reel scrolling
  - hover dictionary popup
  - video seek / next-prev
- Keep dictionary lookup latency low with prefetch + cache hit monitoring.
- Add lightweight regression checks for:
  - cue sync drift
  - popup interaction conflicts
  - keybind conflicts

### 8) Translation pipeline hardening

- Add helper command for translation target extraction (video-present subtitle set).
- Add DB write helper for `translation_runs` (active/superseded transaction pattern).
- Keep translation automation opt-in until quality/cost metrics stabilize.

### Backlog

- Unified subtitle JSON model (optional, only when metadata pressure grows).
- Word importance ranking by frequency/learning priority.
- Extended grammar visualization beyond reel view.
- `bep-eng.txt` integration remains out of scope for current roadmap.

## Add more creators

Add another `[[sources]]` block in `config/sources.toml`.
Each source can point to its own directory under `/Users/bakemocho/Audio/tiktok/english`.
