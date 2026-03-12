# substudy

Local-first workflow for English study from downloaded short videos.

## Public entry point

This repository provides a local study tool focused on:

- short-video feed playback (vertical, keyboard-friendly)
- subtitle-centered learning (EN/JA reel, bookmarks, notes)
- hover dictionary lookup for subtitle words
- local data pipeline (sync/ledger/ASR/loudness) for your own study corpus

If you are new here, start with:

1. local-only files initialization (`make init-local`)
2. command list (`make help`)
3. basic sync (`make sync`)
4. web UI launch (`python3 scripts/substudy.py web --host 127.0.0.1 --port 8876`)

## Quick start

1. Create local config:

```bash
make init-local
```

2. Run incremental sync:

```bash
python3 scripts/substudy.py sync
```

3. (Optional) run ASR and loudness:

```bash
python3 scripts/substudy.py asr
python3 scripts/substudy.py loudness
```

4. Launch local study UI:

```bash
python3 scripts/substudy.py web --host 127.0.0.1 --port 8876
```

Open `http://127.0.0.1:8876`.

## Task-based command guide

Use `make` for common operations first. Use `python3 scripts/substudy.py ...` when you need detailed flags.

Show the common `make` targets:

```bash
make help
```

### Local bootstrap

- Initialize local-only files without overwriting existing values:
  - `make init-local`

### Daily operation

- Run normal daily flow (queue producer + workers + ledger/report):
  - `make daily`
- Run daily flow for one source:
  - `make daily-source SOURCE=<source_id>`
- Run one sync manually:
  - `make sync`
  - require latest `yt-dlp`: `YTDLP_REQUIRE_LATEST=1 make sync`
  - weak/auto network profile: `python3 scripts/substudy.py sync --config config/sources.toml --network-profile auto`
- Run only metadata fetch (no media, no subtitles):
  - `make sync-meta-only`
  - alias: `make sync-meta-missing`
- Run subtitle fetch for missing subtitles (no media, no metadata):
  - `make sync-subs-missing`
  - upstream JA only: `make sync-subs-ja-missing`
- Note:
  - Daily/weekly automation currently excludes queue-based `translate` processing (temporary quality hold).

### Queue failure handling and retry

- See only unresolved queue items:
  - `make queue-status-unresolved`
- See full queue report (including recovered history):
  - `make queue-status`
- Preview known recoverable failures (no DB write):
  - `make queue-recover-known-dry`
- Apply known recovery + show unresolved in one step:
  - `make queue-heal`
- Manually requeue with custom filters:
  - `make queue-requeue QUEUE_REQUEUE_ARGS="--stage translate --status dead --error-contains 'tuple indices must be integers or slices, not str'"`

### Pipeline stages

- Backfill historical videos:
  - `make backfill`
- Retry/generate ASR:
  - `make asr`
- Compute loudness normalization:
  - `make loudness`
- Local subtitle translation:
  - `make translate-local`
  - process all available targets: `make translate-local-all`

### Ledger and reporting

- Incremental ledger update (recommended):
  - `make ledger-inc`
- Full ledger rebuild:
  - `make ledger-full`
- Download-stage report:
  - `make downloads`

### Study UI and dictionary/LLM tools

- Launch web UI:
  - `python3 scripts/substudy.py web --host 127.0.0.1 --port 8876`
- Build dictionary index:
  - `python3 scripts/substudy.py dict-index --dictionary-path data/eijiro-1449.utf8.txt`
- Export/import/curate dictionary bookmarks:
  - `python3 scripts/substudy.py dict-bookmarks-export --entry-status missing --format jsonl`
  - `python3 scripts/substudy.py dict-bookmarks-import --input exports/dictionary_bookmarks_missing_*.jsonl --on-duplicate upsert`
  - `python3 scripts/substudy.py dict-bookmarks-curate --preset review_cards --format jsonl --limit 200`
- LLM helper scripts:
  - `scripts/run_llm_pipeline.sh missing-export --limit 200`
  - `scripts/run_llm_pipeline.sh missing-import`
  - `scripts/run_llm_pipeline.sh review-cards-export --limit 200`

### Notifications and checks

- `yt-dlp` freshness/update:
  - `make ytdlp-check`
  - `make ytdlp-update`
  - strict mode for sync/backfill: `YTDLP_REQUIRE_LATEST=1 make sync-subs-ja-missing`
- macOS launchd install/update:
  - `make install-launchd`
  - custom schedule/label: `make install-launchd LAUNCHD_ARGS="6 30 0 7 0 com.substudy"`
- Local notifications:
  - `python3 scripts/substudy.py notify --kind all`
  - `python3 scripts/substudy.py notify-install-macos --interval-minutes 90 --kind all`
  - `python3 scripts/substudy.py notify-uninstall-macos`
- Sanity checks:
  - `make privacy-check`
  - `make test`

Notification note (macOS):

- Click-to-open notification requires `terminal-notifier` (`brew install terminal-notifier`).
- Without it, fallback uses AppleScript notification (toast only, no click action).

## Web study features (MVP)

- `Study` / `Ops` view split:
  - `Study`: player, subtitle reel, review queue, notes, bookmarks
  - `Ops`: source processing monitor, download/import status, source target manager
- 9:16 vertical feed with up/down navigation (`↑/↓`, `J/K`, wheel, swipe)
- always-visible transport controls (`前へ / 再生 / 次へ`) + collapsible detailed actions
- auto-advance with 3-second countdown and cancel
- continuous playback toggle (`A`)
- inter-video volume normalization toggle (`N`)
- compact playback settings drawer in the top toolbar
- subtitle overlay + selectable track
- hover dictionary popup on English subtitle words
- subtitle bookmarks:
  - save current subtitle (`B`)
  - save playback range (`R` start, `T` save)
- video favorite toggle (`F`) and per-video memo
- floating status toast for action/result feedback
- workspace panels:
  - review queue with `review_hints` join (`one_line_hint_ja/en`)
  - EN/JA mismatch warning from `translation_qa` (`qa_result=check`)
  - missing-entry status (`LLM補完待ち / 補完済み / 要再確認`)
  - import result monitor (`inserted/updated/skipped/errors`)
  - recent artifact browser with `open` / `download`
  - source target manager (投稿監視/いいね欄監視の追加・有効/無効管理)

SQLite learning tables:

- `video_favorites(source_id, video_id, created_at)`
- `subtitle_bookmarks(id, source_id, video_id, track, start_ms, end_ms, text, note, created_at)`
- `video_notes(id, source_id, video_id, note, created_at, updated_at)`

## Documentation map

User-facing / publishing:

- `LEGAL.md`
- `THIRD_PARTY_NOTICES.md`

Technical / developer-facing:

- `docs/technical-guide.md`
- `docs/roadmap.md`
- `docs/extension-ingest-mvp.md`
- `docs/subtitle-translation.md`
- `docs/llm-work-items.md`

## Legal and public sharing

If you plan to make this repository public or share it on social platforms:

- read `LEGAL.md` first
- keep the "personal/local study tool" scope explicit
- do not imply that platform terms or content rights are automatically cleared
- include `THIRD_PARTY_NOTICES.md` in your release context

Recommended one-line disclosure:

`Local study tool. Use only with content/permissions you own and in compliance with platform terms and local law.`
