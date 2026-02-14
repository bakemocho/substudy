# substudy

Local-first workflow for English study from downloaded short videos.

## Public entry point

This repository provides a local study tool focused on:

- short-video feed playback (vertical, keyboard-friendly)
- subtitle-centered learning (EN/JA reel, bookmarks, notes)
- hover dictionary lookup for subtitle words
- local data pipeline (sync/ledger/ASR/loudness) for your own study corpus

If you are new here, start with:

1. `config/sources.example.toml` -> `config/sources.toml`
2. basic sync (`sync`)
3. web UI launch (`web`)

## Quick start

1. Create local config:

```bash
cp config/sources.example.toml config/sources.toml
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

## Core commands

- `python3 scripts/substudy.py sync`
- `python3 scripts/substudy.py backfill`
- `python3 scripts/substudy.py ledger`
- `python3 scripts/substudy.py asr`
- `python3 scripts/substudy.py loudness`
- `python3 scripts/substudy.py dict-index --dictionary-path data/eijiro-1449.utf8.txt`
- `python3 scripts/substudy.py dict-bookmarks-export --entry-status missing --format jsonl`
- `python3 scripts/substudy.py dict-bookmarks-import --input exports/dictionary_bookmarks_missing_*.jsonl --on-duplicate upsert`
- `python3 scripts/substudy.py dict-bookmarks-curate --preset frequent_terms --format csv --limit 200`
- `python3 scripts/substudy.py downloads --since-hours 24`
- `python3 scripts/substudy.py web --host 127.0.0.1 --port 8876`

## Web study features (MVP)

- 9:16 vertical feed with up/down navigation (`↑/↓`, `J/K`, wheel, swipe)
- auto-advance with 3-second countdown and cancel
- continuous playback toggle (`A`)
- inter-video volume normalization toggle (`N`)
- subtitle overlay + selectable track
- hover dictionary popup on English subtitle words
- subtitle bookmarks:
  - save current subtitle (`B`)
  - save playback range (`R` start, `T` save)
- video favorite toggle (`F`) and per-video memo

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

## Legal and public sharing

If you plan to make this repository public or share it on social platforms:

- read `LEGAL.md` first
- keep the "personal/local study tool" scope explicit
- do not imply that platform terms or content rights are automatically cleared
- include `THIRD_PARTY_NOTICES.md` in your release context

Recommended one-line disclosure:

`Local study tool. Use only with content/permissions you own and in compliance with platform terms and local law.`
