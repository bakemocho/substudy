# Browser Extension Ingest MVP Plan

Goal: make data ingestion less dependent on `yt-dlp`, improve UX for local users, and keep existing study pipeline compatible.

This plan adds a browser extension as a new ingest path, without breaking current CLI/web features.

## Scope and non-goals

In scope:

- capture subtitle/metadata/media URL information from pages the user is already viewing
- save captured data in a canonical local format
- import captured data into existing SQLite/ledger pipeline
- keep `yt-dlp` as fallback during migration

Out of scope (MVP):

- bypassing DRM, paywalls, or technical protections
- server-side scraping service
- full replacement of all `yt-dlp` features in one step

## Architecture (target)

1. Browser Extension (MV3)
- content script reads page context
- background/service worker handles capture workflow
- popup UI provides capture status and options

2. Local Bridge (host app)
- extension sends captured payloads to local endpoint (localhost)
- host validates and stores capture artifacts

3. Existing pipeline integration
- host writes normalized files/records so `ledger`, `web`, and study features continue to work

## Canonical ingest format

Define one canonical JSON payload (versioned), for example:

```json
{
  "schema_version": "ingest.v1",
  "source_id": "storiesofcz",
  "video_id": "7603444228575890709",
  "title": "...",
  "description": "...",
  "uploader": "...",
  "upload_date": "2026-02-05",
  "duration_sec": 31.2,
  "webpage_url": "https://...",
  "media_candidates": [],
  "subtitle_tracks": [],
  "captured_at": "2026-02-11T00:00:00Z"
}
```

The host converts this into the same logical model currently consumed by `ledger`.

## Phased delivery

### 0) Prep: ingest abstraction in core

- split current ingest logic into provider interface:
  - `yt_dlp_provider`
  - `extension_provider` (new)
- keep downstream stages (`ledger`, `asr`, `loudness`, `web`) provider-agnostic

Acceptance:
- pipeline runs unchanged with `yt_dlp_provider`
- provider can be selected by config/CLI flag

### 1) Extension POC: metadata + subtitle capture (no auto media download)

- MV3 extension that captures:
  - page URL, id-like signals, title/description/uploader
  - subtitle cues/tracks if accessible from page/runtime APIs
- save payload to local host endpoint

Acceptance:
- capture from supported pages appears in local ingest queue
- `ledger --incremental` imports those records

### 2) Local host bridge and queue

- add local endpoint (loopback-only)
- add queue table and processing states (`queued/processed/error`)
- add retry/backoff and error visibility

Acceptance:
- extension submissions are durable and retryable
- failed items are inspectable from CLI

### 3) Media acquisition strategy

- first, consume direct media URLs if provided by extension capture
- if absent:
  - keep item as metadata/subtitle-only
  - optionally fallback to `yt-dlp` per-source/per-item policy

Acceptance:
- web UI handles metadata/subtitle-only entries gracefully
- fallback path is explicit and auditable

### 4) UX and distribution baseline

- extension popup:
  - source selector
  - capture button
  - latest status
- docs for local install/update
- signed package plan (store/private distribution)

Acceptance:
- first-time setup fits one short guide
- local users can capture and study without CLI-heavy flow

### 5) De-risking and eventual `yt-dlp` minimization

- per-source ingest mode:
  - `extension_only`
  - `extension_preferred`
  - `ytdlp_only` (legacy)
- metrics:
  - capture success rate
  - media availability rate
  - ingest latency

Exit criteria for “yt-dlp optional by default”:
- extension pipeline meets reliability target on main sources
- fallback usage is low and intentional

## Security and privacy requirements

- loopback-only local bridge; no remote ingest server by default
- explicit user action for capture (no hidden background scraping)
- minimal stored secrets; no cloud sync by default
- logs redact local absolute paths and sensitive headers

## Legal and compliance requirements

- keep `LEGAL.md` policy in scope for extension path as well
- do not claim extension usage bypasses platform terms
- document supported “allowed-use” scenarios only

## Implementation checklist (immediate next tasks)

1. define `ingest.v1` JSON schema and validation
2. create provider interface in `scripts/substudy.py`
3. add local ingest queue table + CLI inspect command
4. scaffold MV3 extension with one supported capture flow
5. import captured payload into existing ledger flow
6. document setup in `docs/technical-guide.md`
