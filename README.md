# substudy

Local-first workflow for English study with downloaded short videos and subtitles.

## What this adds

- Multi-account source config in `config/sources.toml`
- One CLI for:
  - downloading media/subtitles/metadata with `yt-dlp`
  - maintaining a master ledger (`SQLite` + `CSV`)
- Optional macOS daily automation via `launchd`

## Layout

- `scripts/substudy.py`: main CLI (`sync` and `ledger`)
- `scripts/run_daily_sync.sh`: daily sync wrapper
- `scripts/install_launchd.sh`: install/update launchd schedule
- `config/sources.example.toml`: example config
- `data/master_ledger.sqlite`: generated ledger DB
- `data/master_ledger.csv`: generated ledger CSV

## Quick start

1. Copy config and edit if needed:

```bash
cp config/sources.example.toml config/sources.toml
```

2. Dry-run the sync commands:

```bash
python3 scripts/substudy.py sync --dry-run
```

3. Run sync:

```bash
python3 scripts/substudy.py sync
```

By default, `sync` updates the ledger incrementally (new/missing IDs only), not full file re-scan.
It is designed for daily runs.

4. Rebuild ledger only:

```bash
python3 scripts/substudy.py ledger
```

5. Force a full ledger rebuild when needed:

```bash
python3 scripts/substudy.py sync --full-ledger
```

or

```bash
python3 scripts/substudy.py ledger
```

6. Run explicit incremental ledger update only:

```bash
python3 scripts/substudy.py ledger --incremental
```

## Daily automation (macOS)

Install a 06:30 daily job:

```bash
./scripts/install_launchd.sh 6 30
```

This writes a plist under `~/Library/LaunchAgents`, then bootstraps it with `launchctl`.

## Add more creators

Add another `[[sources]]` entry to `config/sources.toml`.
Each source can point to a different directory structure, so existing folders do not need migration.

## Remote listing optimization flags

These are configurable per source (or in `[global]`):

- `break_on_existing`
- `break_per_input`
- `lazy_playlist`

For safety, defaults are `false`.  
If you enable `break_on_existing`, verify your source feed ordering first (for some feeds, pinned/old posts at the top can make it stop too early).
