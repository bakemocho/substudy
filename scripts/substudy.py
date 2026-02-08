#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "tomllib is required (Python 3.11+)."
    ) from exc

INFO_SUFFIX = ".info.json"
DEFAULT_CONFIG = Path("config/sources.toml")
DEFAULT_LEDGER_DB = Path("data/master_ledger.sqlite")
DEFAULT_LEDGER_CSV = Path("data/master_ledger.csv")
DEFAULT_TIKTOK_VIDEO_URL = "https://www.tiktok.com/@{handle}/video/{id}"
DEFAULT_PLAYLIST_END = 200
DEFAULT_ASR_EXTS = ["srt", "vtt"]


@dataclass
class GlobalConfig:
    ledger_db: Path
    ledger_csv: Path


@dataclass
class SourceConfig:
    id: str
    platform: str
    url: str
    enabled: bool
    data_dir: Path
    media_dir: Path
    subs_dir: Path
    meta_dir: Path
    media_archive: Path
    subs_archive: Path
    urls_file: Path
    handle: str | None
    video_url_template: str | None
    video_id_regex: str
    ytdlp_bin: str
    cookies_browser: str
    video_format: str
    sub_langs: str
    sub_format: str
    sleep_interval: int
    max_sleep_interval: int
    retry_sleep: int
    playlist_end: int | None
    break_on_existing: bool
    break_per_input: bool
    lazy_playlist: bool
    backfill_enabled: bool
    backfill_start: int | None
    backfill_window: int
    backfill_windows_per_run: int
    asr_enabled: bool
    asr_dir: Path
    asr_command: list[str]
    asr_max_per_run: int
    asr_timeout_sec: int
    asr_prefer_exts: list[str]
    media_output_template: str
    subs_output_template: str
    meta_output_template: str


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def resolve_path(base: Path, raw_value: str | None, default: str) -> Path:
    value = raw_value if raw_value not in (None, "") else default
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def resolve_source_root(
    base_data_dir: Path | None,
    data_dir_value: str | None,
    source_id: str,
    cwd: Path,
) -> Path:
    if data_dir_value not in (None, ""):
        candidate = Path(data_dir_value).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        if base_data_dir is not None:
            return (base_data_dir / candidate).resolve()
        return (cwd / candidate).resolve()
    if base_data_dir is not None:
        return (base_data_dir / source_id).resolve()
    return (cwd / source_id).resolve()


def infer_tiktok_handle(url: str) -> str | None:
    match = re.search(r"tiktok\.com/@([^/?]+)", url)
    if match:
        return match.group(1)
    return None


def parse_command_spec(raw_value: Any) -> list[str]:
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, str):
        return shlex.split(raw_value)
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value]
    raise ValueError("asr_command must be a string or list of strings.")


def parse_optional_positive_int(raw_value: Any, fallback: int | None) -> int | None:
    if raw_value in (None, ""):
        return fallback
    parsed = int(raw_value)
    if parsed <= 0:
        return None
    return parsed


def parse_ext_list(raw_value: Any) -> list[str]:
    if raw_value in (None, ""):
        return list(DEFAULT_ASR_EXTS)
    if isinstance(raw_value, str):
        candidates = [raw_value]
    elif isinstance(raw_value, list):
        candidates = [str(item) for item in raw_value]
    else:
        raise ValueError("asr_prefer_exts must be a string or list of strings.")

    exts: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.strip().lower().lstrip(".")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        exts.append(normalized)
    return exts or list(DEFAULT_ASR_EXTS)


def load_config(config_path: Path) -> tuple[GlobalConfig, list[SourceConfig]]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    global_raw = raw.get("global", {})

    cwd = Path.cwd()
    base_data_dir_raw = global_raw.get("base_data_dir")
    base_data_dir = None
    if base_data_dir_raw not in (None, ""):
        candidate = Path(base_data_dir_raw).expanduser()
        base_data_dir = candidate if candidate.is_absolute() else (cwd / candidate).resolve()

    global_config = GlobalConfig(
        ledger_db=resolve_path(cwd, global_raw.get("ledger_db"), str(DEFAULT_LEDGER_DB)),
        ledger_csv=resolve_path(cwd, global_raw.get("ledger_csv"), str(DEFAULT_LEDGER_CSV)),
    )
    global_playlist_end = parse_optional_positive_int(
        global_raw.get("playlist_end"),
        DEFAULT_PLAYLIST_END,
    )
    global_backfill_enabled = bool(global_raw.get("backfill_enabled", False))
    global_backfill_start = parse_optional_positive_int(global_raw.get("backfill_start"), None)
    global_backfill_window = parse_optional_positive_int(
        global_raw.get("backfill_window"),
        global_playlist_end or DEFAULT_PLAYLIST_END,
    )
    if global_backfill_window is None:
        global_backfill_window = DEFAULT_PLAYLIST_END
    global_backfill_windows_per_run = int(global_raw.get("backfill_windows_per_run", 1))
    if global_backfill_windows_per_run <= 0:
        global_backfill_windows_per_run = 1
    global_asr_command = parse_command_spec(global_raw.get("asr_command"))
    global_asr_max_per_run = int(global_raw.get("asr_max_per_run", 20))
    global_asr_timeout_sec = int(global_raw.get("asr_timeout_sec", 0))
    global_asr_prefer_exts = parse_ext_list(global_raw.get("asr_prefer_exts"))
    global_asr_enabled = bool(global_raw.get("asr_enabled", False))

    sources_raw = raw.get("sources", [])
    if not sources_raw:
        raise ValueError("No [[sources]] entries found in config.")

    sources: list[SourceConfig] = []
    for source_raw in sources_raw:
        source_id = str(source_raw["id"])
        platform = str(source_raw.get("platform", "tiktok"))
        url = str(source_raw["url"])

        data_dir = resolve_source_root(
            base_data_dir=base_data_dir,
            data_dir_value=source_raw.get("data_dir"),
            source_id=source_id,
            cwd=cwd,
        )
        source_playlist_end = parse_optional_positive_int(
            source_raw.get("playlist_end"),
            global_playlist_end,
        )
        source_backfill_start = parse_optional_positive_int(
            source_raw.get("backfill_start"),
            global_backfill_start,
        )
        source_backfill_window = parse_optional_positive_int(
            source_raw.get("backfill_window"),
            global_backfill_window,
        )
        if source_backfill_window is None:
            source_backfill_window = source_playlist_end or DEFAULT_PLAYLIST_END
        source_backfill_windows_per_run = int(
            source_raw.get("backfill_windows_per_run", global_backfill_windows_per_run)
        )
        if source_backfill_windows_per_run <= 0:
            source_backfill_windows_per_run = 1

        source = SourceConfig(
            id=source_id,
            platform=platform,
            url=url,
            enabled=bool(source_raw.get("enabled", True)),
            data_dir=data_dir,
            media_dir=resolve_path(data_dir, source_raw.get("media_dir"), "media"),
            subs_dir=resolve_path(data_dir, source_raw.get("subs_dir"), "subs"),
            meta_dir=resolve_path(data_dir, source_raw.get("meta_dir"), "meta"),
            media_archive=resolve_path(data_dir, source_raw.get("media_archive"), "archives/media.archive.txt"),
            subs_archive=resolve_path(data_dir, source_raw.get("subs_archive"), "archives/subs.archive.txt"),
            urls_file=resolve_path(data_dir, source_raw.get("urls_file"), "archives/urls.txt"),
            handle=source_raw.get("handle") or infer_tiktok_handle(url),
            video_url_template=source_raw.get("video_url_template"),
            video_id_regex=str(source_raw.get("video_id_regex", r"_(\d{10,})_")),
            ytdlp_bin=str(source_raw.get("ytdlp_bin", global_raw.get("ytdlp_bin", "yt-dlp"))),
            cookies_browser=str(
                source_raw.get("cookies_from_browser", global_raw.get("cookies_from_browser", "chrome"))
            ),
            video_format=str(source_raw.get("video_format", global_raw.get("video_format", "bv*+ba/best"))),
            sub_langs=str(source_raw.get("sub_langs", global_raw.get("sub_langs", "en.*,en,und"))),
            sub_format=str(source_raw.get("sub_format", global_raw.get("sub_format", "vtt/ttml/best"))),
            sleep_interval=int(source_raw.get("sleep_interval", global_raw.get("sleep_interval", 2))),
            max_sleep_interval=int(
                source_raw.get("max_sleep_interval", global_raw.get("max_sleep_interval", 6))
            ),
            retry_sleep=int(source_raw.get("retry_sleep", global_raw.get("retry_sleep", 5))),
            playlist_end=source_playlist_end,
            break_on_existing=bool(
                source_raw.get("break_on_existing", global_raw.get("break_on_existing", False))
            ),
            break_per_input=bool(
                source_raw.get("break_per_input", global_raw.get("break_per_input", False))
            ),
            lazy_playlist=bool(
                source_raw.get("lazy_playlist", global_raw.get("lazy_playlist", False))
            ),
            backfill_enabled=bool(
                source_raw.get("backfill_enabled", global_backfill_enabled)
            ),
            backfill_start=source_backfill_start,
            backfill_window=source_backfill_window,
            backfill_windows_per_run=source_backfill_windows_per_run,
            asr_enabled=bool(source_raw.get("asr_enabled", global_asr_enabled)),
            asr_dir=resolve_path(data_dir, source_raw.get("asr_dir"), "asr"),
            asr_command=parse_command_spec(source_raw.get("asr_command", global_asr_command)),
            asr_max_per_run=int(source_raw.get("asr_max_per_run", global_asr_max_per_run)),
            asr_timeout_sec=int(source_raw.get("asr_timeout_sec", global_asr_timeout_sec)),
            asr_prefer_exts=parse_ext_list(source_raw.get("asr_prefer_exts", global_asr_prefer_exts)),
            media_output_template=str(
                source_raw.get(
                    "media_output_template",
                    "%(upload_date>%Y-%m-%d)s_%(id)s_%(title).200B.%(ext)s",
                )
            ),
            subs_output_template=str(
                source_raw.get("subs_output_template", "%(id)s.%(language)s.%(ext)s")
            ),
            meta_output_template=str(source_raw.get("meta_output_template", "%(id)s.%(ext)s")),
        )
        sources.append(source)

    return global_config, sources


def select_sources(all_sources: list[SourceConfig], selected_ids: list[str] | None) -> list[SourceConfig]:
    if not selected_ids:
        enabled_sources = [source for source in all_sources if source.enabled]
        if not enabled_sources:
            raise ValueError("No enabled sources in config.")
        return enabled_sources

    by_id = {source.id: source for source in all_sources}
    selected: list[SourceConfig] = []
    for source_id in selected_ids:
        source = by_id.get(source_id)
        if source is None:
            raise ValueError(f"Unknown source id: {source_id}")
        if source not in selected:
            selected.append(source)
    return selected


def run_command(command: list[str], dry_run: bool, raise_on_error: bool = True) -> int:
    print("$", shlex.join(command))
    if dry_run:
        return 0
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0 and raise_on_error:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}")
    return completed.returncode


def read_archive_ids(archive_path: Path) -> list[str]:
    if not archive_path.exists():
        return []

    ids: list[str] = []
    seen: set[str] = set()
    for raw_line in archive_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        candidate = parts[1] if len(parts) >= 2 else parts[0]
        if candidate and candidate not in seen:
            ids.append(candidate)
            seen.add(candidate)
    return ids


def list_meta_ids(meta_dir: Path) -> set[str]:
    if not meta_dir.exists():
        return set()
    ids: set[str] = set()
    for path in meta_dir.glob(f"*{INFO_SUFFIX}"):
        ids.add(path.name[: -len(INFO_SUFFIX)])
    return ids


def build_video_url(source: SourceConfig, video_id: str) -> str:
    if source.video_url_template:
        return source.video_url_template.format(
            id=video_id,
            handle=source.handle or "",
            source_id=source.id,
            source_url=source.url,
        )
    if source.platform.lower() == "tiktok":
        if not source.handle:
            raise ValueError(
                f"Cannot infer TikTok handle for source '{source.id}'. "
                "Set `handle` or `video_url_template` in config."
            )
        return DEFAULT_TIKTOK_VIDEO_URL.format(handle=source.handle, id=video_id)
    raise ValueError(
        f"No video URL template for source '{source.id}'. Set `video_url_template` in config."
    )


def write_urls_file(path: Path, urls: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for url in urls:
            file.write(url)
            file.write("\n")


def detect_archive_extractor(source: SourceConfig) -> str:
    for candidate in [source.media_archive, source.subs_archive]:
        if not candidate.exists():
            continue
        for raw_line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2 and parts[0]:
                return parts[0]
    return source.platform.lower()


def write_archive_ids(path: Path, extractor: str, ids: set[str], dry_run: bool) -> None:
    if not ids:
        return
    if dry_run:
        print(f"[archive] dry-run: bootstrap {path} with {len(ids)} IDs")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for video_id in sorted(ids):
            file.write(f"{extractor} {video_id}\n")
    print(f"[archive] bootstrapped {path} with {len(ids)} IDs")


def bootstrap_missing_archives(source: SourceConfig, dry_run: bool) -> None:
    extractor = detect_archive_extractor(source)

    if not source.media_archive.exists():
        media_ids = set(scan_media_files(source))
        write_archive_ids(source.media_archive, extractor, media_ids, dry_run=dry_run)

    if not source.subs_archive.exists():
        subtitle_ids = set(scan_subtitles(source))
        write_archive_ids(source.subs_archive, extractor, subtitle_ids, dry_run=dry_run)


def sync_source(
    source: SourceConfig,
    dry_run: bool,
    skip_media: bool,
    skip_subs: bool,
    skip_meta: bool,
    connection: sqlite3.Connection | None = None,
    playlist_start: int | None = None,
    playlist_end: int | None = None,
    metadata_candidate_ids: list[str] | None = None,
    run_label: str = "sync",
) -> None:
    print(f"\n=== {run_label}: {source.id} ===")

    source.media_dir.mkdir(parents=True, exist_ok=True)
    source.subs_dir.mkdir(parents=True, exist_ok=True)
    source.meta_dir.mkdir(parents=True, exist_ok=True)
    source.media_archive.parent.mkdir(parents=True, exist_ok=True)
    source.subs_archive.parent.mkdir(parents=True, exist_ok=True)

    bootstrap_missing_archives(source, dry_run=dry_run)

    retry_flags = [
        "--ignore-errors",
        "--retries",
        "infinite",
        "--fragment-retries",
        "infinite",
        "--sleep-interval",
        str(source.sleep_interval),
        "--max-sleep-interval",
        str(source.max_sleep_interval),
        "--retry-sleep",
        str(source.retry_sleep),
    ]
    discovery_flags: list[str] = []
    if source.break_on_existing:
        discovery_flags.append("--break-on-existing")
    if source.break_per_input:
        discovery_flags.append("--break-per-input")
    if source.lazy_playlist:
        discovery_flags.append("--lazy-playlist")
    if playlist_start is not None:
        discovery_flags.extend(["--playlist-start", str(playlist_start)])
    effective_playlist_end = playlist_end if playlist_end is not None else source.playlist_end
    if effective_playlist_end is not None:
        discovery_flags.extend(["--playlist-end", str(effective_playlist_end)])

    def safe_video_url(video_id: str) -> str | None:
        try:
            return build_video_url(source, video_id)
        except ValueError:
            return None

    media_before_ids = set(read_archive_ids(source.media_archive))
    if not skip_media:
        media_command = [
            source.ytdlp_bin,
            "--cookies-from-browser",
            source.cookies_browser,
            "--download-archive",
            str(source.media_archive),
            "--continue",
            "--no-overwrites",
            *retry_flags,
            *discovery_flags,
            "-f",
            source.video_format,
            "-o",
            str(source.media_dir / source.media_output_template),
            "--no-playlist",
            source.url,
        ]
        media_started_at = now_utc_iso()
        media_run_id: int | None = None
        if connection is not None and not dry_run:
            media_run_id = begin_download_run(
                connection=connection,
                source_id=source.id,
                stage="media",
                command=media_command,
                target_count=None,
                started_at=media_started_at,
            )
            connection.commit()

        media_exit_code = 0
        media_error: str | None = None
        try:
            media_exit_code = run_command(media_command, dry_run=dry_run, raise_on_error=False)
        except Exception as exc:
            media_exit_code = 1
            media_error = str(exc)
            print(f"[sync] {source.id} media command failed: {exc}", file=sys.stderr)

        media_after_ids = set(read_archive_ids(source.media_archive))
        new_media_ids = sorted(media_after_ids - media_before_ids)
        if connection is not None and not dry_run:
            for video_id in new_media_ids:
                upsert_download_state(
                    connection=connection,
                    source_id=source.id,
                    stage="media",
                    video_id=video_id,
                    status="success",
                    run_id=media_run_id,
                    attempt_at=media_started_at,
                    url=safe_video_url(video_id),
                    last_error=None,
                    retry_count=0,
                    next_retry_at=None,
                )

            finish_download_run(
                connection=connection,
                run_id=media_run_id if media_run_id is not None else -1,
                status="success" if media_exit_code == 0 else "error",
                finished_at=now_utc_iso(),
                exit_code=media_exit_code,
                success_count=len(new_media_ids),
                failure_count=None if media_exit_code != 0 else 0,
                error_message=media_error or (
                    None if media_exit_code == 0 else f"command exit code {media_exit_code}"
                ),
            )
            connection.commit()

        print(f"media new IDs: {len(new_media_ids)}")
    else:
        print("skip media")

    subs_before_ids = set(read_archive_ids(source.subs_archive))
    if not skip_subs:
        subs_command = [
            source.ytdlp_bin,
            "--cookies-from-browser",
            source.cookies_browser,
            "--download-archive",
            str(source.subs_archive),
            "--continue",
            "--no-overwrites",
            *retry_flags,
            *discovery_flags,
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            source.sub_langs,
            "--sub-format",
            source.sub_format,
            "-o",
            str(source.subs_dir / source.subs_output_template),
            "--no-playlist",
            source.url,
        ]
        subs_started_at = now_utc_iso()
        subs_run_id: int | None = None
        if connection is not None and not dry_run:
            subs_run_id = begin_download_run(
                connection=connection,
                source_id=source.id,
                stage="subs",
                command=subs_command,
                target_count=None,
                started_at=subs_started_at,
            )
            connection.commit()

        subs_exit_code = 0
        subs_error: str | None = None
        try:
            subs_exit_code = run_command(subs_command, dry_run=dry_run, raise_on_error=False)
        except Exception as exc:
            subs_exit_code = 1
            subs_error = str(exc)
            print(f"[sync] {source.id} subs command failed: {exc}", file=sys.stderr)

        subs_after_ids = set(read_archive_ids(source.subs_archive))
        new_subs_ids = sorted(subs_after_ids - subs_before_ids)
        if connection is not None and not dry_run:
            for video_id in new_subs_ids:
                upsert_download_state(
                    connection=connection,
                    source_id=source.id,
                    stage="subs",
                    video_id=video_id,
                    status="success",
                    run_id=subs_run_id,
                    attempt_at=subs_started_at,
                    url=safe_video_url(video_id),
                    last_error=None,
                    retry_count=0,
                    next_retry_at=None,
                )

            finish_download_run(
                connection=connection,
                run_id=subs_run_id if subs_run_id is not None else -1,
                status="success" if subs_exit_code == 0 else "error",
                finished_at=now_utc_iso(),
                exit_code=subs_exit_code,
                success_count=len(new_subs_ids),
                failure_count=None if subs_exit_code != 0 else 0,
                error_message=subs_error or (
                    None if subs_exit_code == 0 else f"command exit code {subs_exit_code}"
                ),
            )
            connection.commit()

        print(f"subtitle new IDs: {len(new_subs_ids)}")
    else:
        print("skip subtitles")

    if skip_meta:
        print("skip metadata")
        return

    if metadata_candidate_ids is not None:
        archive_ids = []
        seen_archive_ids: set[str] = set()
        for video_id in metadata_candidate_ids:
            if video_id in seen_archive_ids:
                continue
            archive_ids.append(video_id)
            seen_archive_ids.add(video_id)
    else:
        media_archive_ids = read_archive_ids(source.media_archive)
        subs_archive_ids = read_archive_ids(source.subs_archive)
        local_media_ids = sorted(scan_media_files(source))
        local_sub_ids = sorted(scan_subtitles(source))
        archive_ids = []
        seen_archive_ids = set()
        for video_id in [*media_archive_ids, *subs_archive_ids, *local_media_ids, *local_sub_ids]:
            if video_id in seen_archive_ids:
                continue
            archive_ids.append(video_id)
            seen_archive_ids.add(video_id)

    if not archive_ids:
        if metadata_candidate_ids is None:
            print(
                f"no archived IDs found in {source.media_archive} or {source.subs_archive}; "
                "skip metadata"
            )
        else:
            print("no metadata candidates for this run; skip metadata")
        return

    existing_meta_ids = list_meta_ids(source.meta_dir)
    missing_ids = [video_id for video_id in archive_ids if video_id not in existing_meta_ids]
    deferred_missing_ids: list[str] = []
    missing_retryable_ids = list(missing_ids)
    if connection is not None and not dry_run:
        missing_retryable_ids, deferred_missing_ids = split_retryable_ids(
            connection=connection,
            source_id=source.id,
            stage="meta",
            candidate_ids=missing_ids,
        )

    retry_ids: list[str] = []
    if connection is not None and not dry_run:
        retry_ids = get_due_retry_ids(connection, source.id, "meta")

    metadata_target_ids: list[str] = []
    seen_target_ids: set[str] = set()
    for video_id in [*missing_retryable_ids, *retry_ids]:
        if video_id in seen_target_ids:
            continue
        seen_target_ids.add(video_id)
        metadata_target_ids.append(video_id)

    if not metadata_target_ids:
        print("metadata already up to date")
        return

    urls = [build_video_url(source, video_id) for video_id in metadata_target_ids]
    if dry_run:
        print(
            f"metadata target IDs (dry-run): {len(metadata_target_ids)} "
            f"(new={len(missing_retryable_ids)}, retry={len(retry_ids)}, "
            f"deferred={len(deferred_missing_ids)})"
        )
    else:
        write_urls_file(source.urls_file, urls)

    metadata_command = [
        source.ytdlp_bin,
        "--cookies-from-browser",
        source.cookies_browser,
        "--skip-download",
        "--write-info-json",
        "--write-description",
        "--continue",
        "--no-overwrites",
        *retry_flags,
        "-o",
        str(source.meta_dir / source.meta_output_template),
        "--no-playlist",
        "-a",
        str(source.urls_file),
    ]
    meta_started_at = now_utc_iso()
    meta_run_id: int | None = None
    if connection is not None and not dry_run:
        meta_run_id = begin_download_run(
            connection=connection,
            source_id=source.id,
            stage="meta",
            command=metadata_command,
            target_count=len(metadata_target_ids),
            started_at=meta_started_at,
        )
        connection.commit()

    meta_exit_code = 0
    meta_error: str | None = None
    try:
        meta_exit_code = run_command(metadata_command, dry_run=dry_run, raise_on_error=False)
    except Exception as exc:
        meta_exit_code = 1
        meta_error = str(exc)
        print(f"[sync] {source.id} metadata command failed: {exc}", file=sys.stderr)

    if dry_run:
        return

    post_meta_ids = list_meta_ids(source.meta_dir)
    success_meta_ids = [video_id for video_id in metadata_target_ids if video_id in post_meta_ids]
    failed_meta_ids = [video_id for video_id in metadata_target_ids if video_id not in post_meta_ids]

    if connection is not None:
        for video_id in success_meta_ids:
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id=video_id,
                status="success",
                run_id=meta_run_id,
                attempt_at=meta_started_at,
                url=safe_video_url(video_id),
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )

        for video_id in failed_meta_ids:
            current = connection.execute(
                """
                SELECT retry_count
                FROM download_state
                WHERE source_id = ? AND stage = ? AND video_id = ?
                """,
                (source.id, "meta", video_id),
            ).fetchone()
            next_retry_count = (int(current[0]) if current else 0) + 1
            failure_reason = meta_error or (
                f"command exit code {meta_exit_code}"
                if meta_exit_code != 0
                else "metadata file missing after download attempt"
            )
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id=video_id,
                status="error",
                run_id=meta_run_id,
                attempt_at=meta_started_at,
                url=safe_video_url(video_id),
                last_error=failure_reason,
                retry_count=next_retry_count,
                next_retry_at=schedule_next_retry_iso(next_retry_count),
            )

        finish_download_run(
            connection=connection,
            run_id=meta_run_id if meta_run_id is not None else -1,
            status="success" if not failed_meta_ids else "error",
            finished_at=now_utc_iso(),
            exit_code=meta_exit_code,
            success_count=len(success_meta_ids),
            failure_count=len(failed_meta_ids),
            error_message=None if not failed_meta_ids else (
                meta_error or (
                    f"command exit code {meta_exit_code}"
                    if meta_exit_code != 0
                    else "some metadata items are still missing"
                )
            ),
        )
        connection.commit()

    print(
        f"metadata targets={len(metadata_target_ids)} "
        f"success={len(success_meta_ids)} failed={len(failed_meta_ids)} "
        f"(new={len(missing_retryable_ids)}, retry={len(retry_ids)}, "
        f"deferred={len(deferred_missing_ids)})"
    )


def normalize_upload_date(raw_value: Any) -> str | None:
    if raw_value is None:
        return None
    raw_str = str(raw_value)
    if re.fullmatch(r"\d{8}", raw_str):
        return f"{raw_str[0:4]}-{raw_str[4:6]}-{raw_str[6:8]}"
    return raw_str


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_meta_records(meta_dir: Path) -> dict[str, tuple[Path, dict[str, Any]]]:
    records: dict[str, tuple[Path, dict[str, Any]]] = {}
    if not meta_dir.exists():
        return records
    for info_path in meta_dir.glob(f"*{INFO_SUFFIX}"):
        video_id = info_path.name[: -len(INFO_SUFFIX)]
        try:
            with info_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"warning: failed to parse {info_path}: {exc}", file=sys.stderr)
            continue
        records[video_id] = (info_path, data)
    return records


def load_meta_record_by_id(source: SourceConfig, video_id: str) -> tuple[Path | None, dict[str, Any]]:
    info_path = source.meta_dir / f"{video_id}{INFO_SUFFIX}"
    if not info_path.exists():
        return None, {}
    try:
        with info_path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"warning: failed to parse {info_path}: {exc}", file=sys.stderr)
        return None, {}
    return info_path, data


def extract_video_id_from_media(file_name: str, id_regex: re.Pattern[str]) -> str | None:
    match = id_regex.search(file_name)
    if match:
        return match.group(1)
    fallback = re.search(r"(\d{10,})", file_name)
    if fallback:
        return fallback.group(1)
    return None


def scan_media_files(source: SourceConfig) -> dict[str, Path]:
    media: dict[str, Path] = {}
    if not source.media_dir.exists():
        return media

    id_regex = re.compile(source.video_id_regex)
    for media_path in source.media_dir.iterdir():
        if not media_path.is_file():
            continue
        video_id = extract_video_id_from_media(media_path.name, id_regex)
        if not video_id:
            continue
        current = media.get(video_id)
        if current is None:
            media[video_id] = media_path
            continue
        try:
            current_size = current.stat().st_size
            candidate_size = media_path.stat().st_size
        except OSError:
            media[video_id] = media_path
            continue
        if candidate_size > current_size:
            media[video_id] = media_path
    return media


def find_media_file_for_video(source: SourceConfig, video_id: str) -> Path | None:
    if not source.media_dir.exists():
        return None
    candidates = [path for path in source.media_dir.glob(f"*{video_id}*") if path.is_file()]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    best = candidates[0]
    best_size = -1
    for candidate in candidates:
        try:
            size = candidate.stat().st_size
        except OSError:
            size = -1
        if size > best_size:
            best = candidate
            best_size = size
    return best


def scan_subtitles(source: SourceConfig) -> dict[str, list[tuple[str, Path, str]]]:
    subtitles: dict[str, list[tuple[str, Path, str]]] = {}
    if not source.subs_dir.exists():
        return subtitles
    for subtitle_path in source.subs_dir.iterdir():
        if not subtitle_path.is_file():
            continue
        parts = subtitle_path.name.split(".")
        if len(parts) < 2:
            continue
        video_id = parts[0]
        if not video_id.isdigit():
            continue
        language = ".".join(parts[1:-1]) if len(parts) > 2 else ""
        extension = parts[-1]
        subtitles.setdefault(video_id, []).append((language, subtitle_path, extension))
    return subtitles


def scan_subtitles_for_video(source: SourceConfig, video_id: str) -> list[tuple[str, Path, str]]:
    if not source.subs_dir.exists():
        return []
    results: list[tuple[str, Path, str]] = []
    for subtitle_path in source.subs_dir.glob(f"{video_id}.*"):
        if not subtitle_path.is_file():
            continue
        parts = subtitle_path.name.split(".")
        if len(parts) < 2:
            continue
        language = ".".join(parts[1:-1]) if len(parts) > 2 else ""
        extension = parts[-1]
        results.append((language, subtitle_path, extension))
    return results


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            url TEXT NOT NULL,
            data_dir TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS videos (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            uploader TEXT,
            uploader_id TEXT,
            title TEXT,
            description TEXT,
            upload_date TEXT,
            duration REAL,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            repost_count INTEGER,
            save_count INTEGER,
            webpage_url TEXT,
            media_path TEXT,
            media_ext TEXT,
            media_size INTEGER,
            meta_path TEXT,
            description_path TEXT,
            has_media INTEGER NOT NULL DEFAULT 0,
            has_subtitles INTEGER NOT NULL DEFAULT 0,
            subtitle_count INTEGER NOT NULL DEFAULT 0,
            subtitle_langs TEXT,
            synced_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id),
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE TABLE IF NOT EXISTS subtitles (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            language TEXT,
            subtitle_path TEXT NOT NULL,
            ext TEXT,
            PRIMARY KEY (source_id, video_id, subtitle_path),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS asr_runs (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            status TEXT NOT NULL,
            output_path TEXT,
            artifact_dir TEXT,
            engine TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id)
        );

        CREATE INDEX IF NOT EXISTS idx_videos_upload_date ON videos(upload_date);
        CREATE INDEX IF NOT EXISTS idx_subtitles_source_video ON subtitles(source_id, video_id);
        CREATE INDEX IF NOT EXISTS idx_asr_runs_status ON asr_runs(status, updated_at);

        CREATE TABLE IF NOT EXISTS download_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            command TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            exit_code INTEGER,
            target_count INTEGER,
            success_count INTEGER,
            failure_count INTEGER,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS download_state (
            source_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            video_id TEXT NOT NULL,
            url TEXT,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_attempt_at TEXT,
            next_retry_at TEXT,
            last_run_id INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, stage, video_id),
            FOREIGN KEY (last_run_id) REFERENCES download_runs(run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_download_runs_time ON download_runs(started_at, source_id, stage);
        CREATE INDEX IF NOT EXISTS idx_download_state_retry ON download_state(stage, status, next_retry_at);

        CREATE TABLE IF NOT EXISTS backfill_state (
            source_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            next_start INTEGER NOT NULL,
            window_size INTEGER NOT NULL,
            last_window_start INTEGER,
            last_window_end INTEGER,
            last_seen_count INTEGER,
            last_run_at TEXT,
            completed_at TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_backfill_state_status ON backfill_state(status, updated_at);
        """
    )


def export_csv(connection: sqlite3.Connection, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    query = """
    SELECT
        v.source_id,
        v.video_id,
        v.upload_date,
        v.uploader,
        v.title,
        v.duration,
        v.view_count,
        v.like_count,
        v.comment_count,
        v.repost_count,
        v.save_count,
        v.webpage_url,
        v.has_media,
        v.has_subtitles,
        v.subtitle_count,
        v.subtitle_langs,
        CASE
            WHEN a.status = 'success' AND a.output_path IS NOT NULL THEN 1
            ELSE 0
        END AS has_asr_subtitles,
        a.status AS asr_status,
        a.output_path AS asr_output_path,
        a.updated_at AS asr_updated_at,
        v.media_path,
        v.meta_path,
        v.description_path,
        v.synced_at
    FROM videos v
    LEFT JOIN asr_runs a
        ON a.source_id = v.source_id AND a.video_id = v.video_id
    ORDER BY v.source_id, v.upload_date DESC, v.video_id DESC;
    """
    cursor = connection.execute(query)
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"[ledger] csv rows: {len(rows)} -> {csv_path}")


def upsert_source(connection: sqlite3.Connection, source: SourceConfig, updated_at: str) -> None:
    connection.execute(
        """
        INSERT INTO sources(source_id, platform, url, data_dir, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            platform = excluded.platform,
            url = excluded.url,
            data_dir = excluded.data_dir,
            updated_at = excluded.updated_at
        """,
        (source.id, source.platform, source.url, str(source.data_dir), updated_at),
    )


def upsert_video_and_subtitles(
    connection: sqlite3.Connection,
    source: SourceConfig,
    video_id: str,
    meta_path: Path | None,
    meta_data: dict[str, Any],
    media_path: Path | None,
    subtitle_records: list[tuple[str, Path, str]],
    synced_at: str,
) -> None:
    media_path_value = str(media_path) if media_path else None
    media_ext = media_path.suffix.lstrip(".") if media_path else None
    media_size = None
    if media_path is not None:
        try:
            media_size = media_path.stat().st_size
        except OSError:
            media_size = None

    description_path = source.meta_dir / f"{video_id}.description"
    description_path_value = str(description_path) if description_path.exists() else None
    description = meta_data.get("description")
    if description is None and description_path.exists():
        try:
            description = description_path.read_text(encoding="utf-8", errors="ignore").strip()
        except OSError:
            description = None

    subtitle_langs = sorted({language for language, _, _ in subtitle_records if language})
    subtitle_langs_value = ",".join(subtitle_langs) if subtitle_langs else None

    webpage_url = meta_data.get("webpage_url")
    if not webpage_url:
        try:
            webpage_url = build_video_url(source, video_id)
        except ValueError:
            webpage_url = None

    connection.execute(
        """
        INSERT INTO videos (
            source_id,
            video_id,
            uploader,
            uploader_id,
            title,
            description,
            upload_date,
            duration,
            view_count,
            like_count,
            comment_count,
            repost_count,
            save_count,
            webpage_url,
            media_path,
            media_ext,
            media_size,
            meta_path,
            description_path,
            has_media,
            has_subtitles,
            subtitle_count,
            subtitle_langs,
            synced_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, video_id) DO UPDATE SET
            uploader = excluded.uploader,
            uploader_id = excluded.uploader_id,
            title = excluded.title,
            description = excluded.description,
            upload_date = excluded.upload_date,
            duration = excluded.duration,
            view_count = excluded.view_count,
            like_count = excluded.like_count,
            comment_count = excluded.comment_count,
            repost_count = excluded.repost_count,
            save_count = excluded.save_count,
            webpage_url = excluded.webpage_url,
            media_path = excluded.media_path,
            media_ext = excluded.media_ext,
            media_size = excluded.media_size,
            meta_path = excluded.meta_path,
            description_path = excluded.description_path,
            has_media = excluded.has_media,
            has_subtitles = excluded.has_subtitles,
            subtitle_count = excluded.subtitle_count,
            subtitle_langs = excluded.subtitle_langs,
            synced_at = excluded.synced_at
        """,
        (
            source.id,
            video_id,
            meta_data.get("uploader"),
            meta_data.get("uploader_id"),
            meta_data.get("title"),
            description,
            normalize_upload_date(meta_data.get("upload_date")),
            safe_float(meta_data.get("duration")),
            safe_int(meta_data.get("view_count")),
            safe_int(meta_data.get("like_count")),
            safe_int(meta_data.get("comment_count")),
            safe_int(meta_data.get("repost_count")),
            safe_int(meta_data.get("save_count")),
            webpage_url,
            media_path_value,
            media_ext,
            media_size,
            str(meta_path) if meta_path else None,
            description_path_value,
            int(media_path is not None),
            int(len(subtitle_records) > 0),
            len(subtitle_records),
            subtitle_langs_value,
            synced_at,
        ),
    )

    connection.execute(
        "DELETE FROM subtitles WHERE source_id = ? AND video_id = ?",
        (source.id, video_id),
    )
    for language, subtitle_path, extension in subtitle_records:
        connection.execute(
            """
            INSERT INTO subtitles (
                source_id,
                video_id,
                language,
                subtitle_path,
                ext
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (source.id, video_id, language, str(subtitle_path), extension),
        )


def rebuild_source_full(connection: sqlite3.Connection, source: SourceConfig, synced_at: str) -> None:
    connection.execute("DELETE FROM subtitles WHERE source_id = ?", (source.id,))
    connection.execute("DELETE FROM videos WHERE source_id = ?", (source.id,))

    meta_records = load_meta_records(source.meta_dir)
    media_files = scan_media_files(source)
    subtitle_files = scan_subtitles(source)

    all_video_ids = sorted(set(meta_records) | set(media_files) | set(subtitle_files))
    subtitle_file_count = sum(len(files) for files in subtitle_files.values())

    for video_id in all_video_ids:
        meta_path: Path | None = None
        meta_data: dict[str, Any] = {}
        if video_id in meta_records:
            meta_path, meta_data = meta_records[video_id]
        media_path = media_files.get(video_id)
        subtitle_records = subtitle_files.get(video_id, [])
        upsert_video_and_subtitles(
            connection=connection,
            source=source,
            video_id=video_id,
            meta_path=meta_path,
            meta_data=meta_data,
            media_path=media_path,
            subtitle_records=subtitle_records,
            synced_at=synced_at,
        )

    print(
        f"[ledger] {source.id}: full videos={len(all_video_ids)} "
        f"media_files={len(media_files)} subtitle_files={subtitle_file_count}"
    )


def rebuild_source_incremental(connection: sqlite3.Connection, source: SourceConfig, synced_at: str) -> None:
    existing_ids = {
        row[0]
        for row in connection.execute(
            "SELECT video_id FROM videos WHERE source_id = ?",
            (source.id,),
        ).fetchall()
    }

    if not existing_ids:
        print(f"[ledger] {source.id}: initial import -> full scan")
        rebuild_source_full(connection, source, synced_at)
        return

    media_archive_ids = set(read_archive_ids(source.media_archive))
    subs_archive_ids = set(read_archive_ids(source.subs_archive))
    missing_from_db_ids = (media_archive_ids | subs_archive_ids) - existing_ids

    db_no_meta_ids = {
        row[0]
        for row in connection.execute(
            """
            SELECT video_id
            FROM videos
            WHERE source_id = ?
              AND meta_path IS NULL
            """,
            (source.id,),
        ).fetchall()
    }
    expected_meta_ids = media_archive_ids | subs_archive_ids
    no_meta_ids = db_no_meta_ids & expected_meta_ids

    no_media_ids = {
        row[0]
        for row in connection.execute(
            "SELECT video_id FROM videos WHERE source_id = ? AND has_media = 0",
            (source.id,),
        ).fetchall()
    }
    no_subtitle_ids = {
        row[0]
        for row in connection.execute(
            "SELECT video_id FROM videos WHERE source_id = ? AND has_subtitles = 0",
            (source.id,),
        ).fetchall()
    }
    media_backfill_ids = media_archive_ids & no_media_ids
    subtitle_backfill_ids = subs_archive_ids & no_subtitle_ids

    candidate_ids = missing_from_db_ids | no_meta_ids | media_backfill_ids | subtitle_backfill_ids
    if not candidate_ids:
        print(f"[ledger] {source.id}: incremental up to date")
        return

    updated = 0
    with_meta = 0
    with_media = 0
    with_subtitles = 0

    for video_id in sorted(candidate_ids):
        meta_path, meta_data = load_meta_record_by_id(source, video_id)
        media_path = find_media_file_for_video(source, video_id)
        subtitle_records = scan_subtitles_for_video(source, video_id)
        upsert_video_and_subtitles(
            connection=connection,
            source=source,
            video_id=video_id,
            meta_path=meta_path,
            meta_data=meta_data,
            media_path=media_path,
            subtitle_records=subtitle_records,
            synced_at=synced_at,
        )
        updated += 1
        if meta_path:
            with_meta += 1
        if media_path:
            with_media += 1
        if subtitle_records:
            with_subtitles += 1

    print(
        f"[ledger] {source.id}: incremental updated={updated} "
        f"new_from_archive={len(missing_from_db_ids)} no_meta={len(no_meta_ids)} "
        f"media_backfill={len(media_backfill_ids)} subtitle_backfill={len(subtitle_backfill_ids)} "
        f"meta={with_meta} media={with_media} subs={with_subtitles}"
    )


def build_ledger(
    sources: list[SourceConfig],
    db_path: Path,
    csv_path: Path,
    incremental: bool = False,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path))
    create_schema(connection)
    synced_at = now_utc_iso()

    with connection:
        for source in sources:
            upsert_source(connection, source, synced_at)
            if incremental:
                rebuild_source_incremental(connection, source, synced_at)
            else:
                rebuild_source_full(connection, source, synced_at)
            connection.execute(
                """
                DELETE FROM asr_runs
                WHERE source_id = ?
                  AND video_id NOT IN (
                    SELECT video_id FROM videos WHERE source_id = ?
                  )
                """,
                (source.id, source.id),
            )
            connection.execute(
                """
                DELETE FROM download_state
                WHERE source_id = ?
                  AND video_id NOT IN (
                    SELECT video_id FROM videos WHERE source_id = ?
                  )
                """,
                (source.id, source.id),
            )

    export_csv(connection, csv_path)
    connection.close()
    mode = "incremental" if incremental else "full"
    print(f"[ledger] sqlite ({mode}) -> {db_path}")


def schedule_next_retry_iso(retry_count: int) -> str:
    # Exponential backoff with 5m base, capped at 24h.
    delay_seconds = min(300 * (2 ** max(0, retry_count - 1)), 86400)
    retry_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=delay_seconds)
    return retry_at.replace(microsecond=0).isoformat()


def begin_download_run(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    command: list[str] | None,
    target_count: int | None,
    started_at: str,
) -> int:
    command_text = shlex.join(command) if command else None
    cursor = connection.execute(
        """
        INSERT INTO download_runs (
            source_id,
            stage,
            status,
            command,
            started_at,
            target_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_id, stage, "running", command_text, started_at, target_count),
    )
    run_id = cursor.lastrowid
    if run_id is None:
        raise RuntimeError("Failed to create download run record")
    return int(run_id)


def finish_download_run(
    connection: sqlite3.Connection,
    run_id: int,
    status: str,
    finished_at: str,
    exit_code: int | None,
    success_count: int | None,
    failure_count: int | None,
    error_message: str | None,
) -> None:
    connection.execute(
        """
        UPDATE download_runs
        SET status = ?,
            finished_at = ?,
            exit_code = ?,
            success_count = ?,
            failure_count = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        (
            status,
            finished_at,
            exit_code,
            success_count,
            failure_count,
            error_message,
            run_id,
        ),
    )


def upsert_download_state(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    video_id: str,
    status: str,
    run_id: int | None,
    attempt_at: str,
    url: str | None = None,
    last_error: str | None = None,
    retry_count: int | None = None,
    next_retry_at: str | None = None,
) -> None:
    current = connection.execute(
        """
        SELECT retry_count
        FROM download_state
        WHERE source_id = ? AND stage = ? AND video_id = ?
        """,
        (source_id, stage, video_id),
    ).fetchone()
    if retry_count is None:
        retry_count = int(current[0]) if current else 0

    connection.execute(
        """
        INSERT INTO download_state (
            source_id,
            stage,
            video_id,
            url,
            status,
            retry_count,
            last_error,
            last_attempt_at,
            next_retry_at,
            last_run_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, stage, video_id) DO UPDATE SET
            url = excluded.url,
            status = excluded.status,
            retry_count = excluded.retry_count,
            last_error = excluded.last_error,
            last_attempt_at = excluded.last_attempt_at,
            next_retry_at = excluded.next_retry_at,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            stage,
            video_id,
            url,
            status,
            retry_count,
            last_error,
            attempt_at,
            next_retry_at,
            run_id,
            attempt_at,
        ),
    )


def split_retryable_ids(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    candidate_ids: list[str],
    now_iso: str | None = None,
) -> tuple[list[str], list[str]]:
    if not candidate_ids:
        return [], []
    now_value = now_iso or now_utc_iso()

    retryable: list[str] = []
    deferred: list[str] = []
    for video_id in candidate_ids:
        row = connection.execute(
            """
            SELECT status, next_retry_at
            FROM download_state
            WHERE source_id = ? AND stage = ? AND video_id = ?
            """,
            (source_id, stage, video_id),
        ).fetchone()
        if row is None:
            retryable.append(video_id)
            continue

        status = str(row[0])
        next_retry_at = row[1]
        if status != "error":
            retryable.append(video_id)
            continue

        if next_retry_at in (None, "") or str(next_retry_at) <= now_value:
            retryable.append(video_id)
        else:
            deferred.append(video_id)

    return retryable, deferred


def get_due_retry_ids(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    limit: int = 200,
) -> list[str]:
    now_iso = now_utc_iso()
    rows = connection.execute(
        """
        SELECT video_id
        FROM download_state
        WHERE source_id = ?
          AND stage = ?
          AND status = 'error'
          AND (next_retry_at IS NULL OR next_retry_at <= ?)
        ORDER BY updated_at ASC
        LIMIT ?
        """,
        (source_id, stage, now_iso, limit),
    ).fetchall()
    return [str(row[0]) for row in rows]


def get_default_backfill_start(source: SourceConfig) -> int:
    if source.backfill_start is not None:
        return source.backfill_start
    if source.playlist_end is not None:
        return source.playlist_end + 1
    return DEFAULT_PLAYLIST_END + 1


def discover_playlist_window_ids(
    source: SourceConfig,
    playlist_start: int,
    playlist_end: int,
    dry_run: bool,
) -> list[str]:
    command = [
        source.ytdlp_bin,
        "--cookies-from-browser",
        source.cookies_browser,
        "--flat-playlist",
        "--print",
        "%(id)s",
        "--ignore-errors",
        "--playlist-start",
        str(playlist_start),
        "--playlist-end",
        str(playlist_end),
        source.url,
    ]
    print("$", shlex.join(command))
    if dry_run:
        return []

    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip()
        if not message:
            message = f"exit code {completed.returncode}"
        raise RuntimeError(message)

    ids: list[str] = []
    seen: set[str] = set()
    for raw_line in completed.stdout.splitlines():
        video_id = raw_line.strip()
        if not video_id or video_id.startswith("["):
            continue
        if video_id in seen:
            continue
        seen.add(video_id)
        ids.append(video_id)
    return ids


def ensure_backfill_state(
    connection: sqlite3.Connection,
    source: SourceConfig,
    reset: bool = False,
) -> tuple[str, int, int]:
    now_iso = now_utc_iso()
    default_start = get_default_backfill_start(source)
    default_window = max(1, source.backfill_window)

    if reset:
        connection.execute(
            "DELETE FROM backfill_state WHERE source_id = ?",
            (source.id,),
        )

    connection.execute(
        """
        INSERT INTO backfill_state (
            source_id,
            status,
            next_start,
            window_size,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            window_size = excluded.window_size,
            updated_at = excluded.updated_at
        """,
        (
            source.id,
            "active",
            default_start,
            default_window,
            now_iso,
        ),
    )
    row = connection.execute(
        """
        SELECT status, next_start, window_size
        FROM backfill_state
        WHERE source_id = ?
        """,
        (source.id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to initialize backfill state")
    return str(row[0]), int(row[1]), int(row[2])


def update_backfill_state(
    connection: sqlite3.Connection,
    source_id: str,
    status: str,
    next_start: int,
    window_size: int,
    last_window_start: int | None,
    last_window_end: int | None,
    last_seen_count: int | None,
    completed_at: str | None,
) -> None:
    now_iso = now_utc_iso()
    connection.execute(
        """
        INSERT INTO backfill_state (
            source_id,
            status,
            next_start,
            window_size,
            last_window_start,
            last_window_end,
            last_seen_count,
            last_run_at,
            completed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            status = excluded.status,
            next_start = excluded.next_start,
            window_size = excluded.window_size,
            last_window_start = excluded.last_window_start,
            last_window_end = excluded.last_window_end,
            last_seen_count = excluded.last_seen_count,
            last_run_at = excluded.last_run_at,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            status,
            next_start,
            window_size,
            last_window_start,
            last_window_end,
            last_seen_count,
            now_iso,
            completed_at,
            now_iso,
        ),
    )


def render_asr_command(command_template: list[str], replacements: dict[str, str]) -> list[str]:
    rendered: list[str] = []
    for token in command_template:
        try:
            rendered.append(token.format(**replacements))
        except KeyError as exc:
            missing_key = exc.args[0]
            raise ValueError(f"Unknown ASR command placeholder: {missing_key}") from exc
    return rendered


def upsert_asr_run(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
    status: str,
    attempts: int,
    engine: str,
    output_path: str | None = None,
    artifact_dir: str | None = None,
    last_error: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    updated_at = now_utc_iso()
    connection.execute(
        """
        INSERT INTO asr_runs (
            source_id,
            video_id,
            status,
            output_path,
            artifact_dir,
            engine,
            attempts,
            last_error,
            started_at,
            finished_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, video_id) DO UPDATE SET
            status = excluded.status,
            output_path = excluded.output_path,
            artifact_dir = excluded.artifact_dir,
            engine = excluded.engine,
            attempts = excluded.attempts,
            last_error = excluded.last_error,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            video_id,
            status,
            output_path,
            artifact_dir,
            engine,
            attempts,
            last_error,
            started_at,
            finished_at,
            updated_at,
        ),
    )


def pick_asr_subtitle_file(artifact_dir: Path, prefer_exts: list[str]) -> Path | None:
    if not artifact_dir.exists():
        return None
    ext_rank = {
        f".{ext.lower().lstrip('.')}": index
        for index, ext in enumerate(prefer_exts)
    }
    ranked: list[tuple[int, float, int, Path]] = []
    for candidate in artifact_dir.rglob("*"):
        if not candidate.is_file():
            continue
        suffix = candidate.suffix.lower()
        if suffix not in ext_rank:
            continue
        try:
            stat = candidate.stat()
        except OSError:
            continue
        ranked.append((ext_rank[suffix], -stat.st_mtime, -stat.st_size, candidate))

    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], item[2]))
    return ranked[0][3]


def run_asr(
    sources: list[SourceConfig],
    db_path: Path,
    csv_path: Path,
    dry_run: bool = False,
    force: bool = False,
    max_per_source_override: int | None = None,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path))
    create_schema(connection)

    for source in sources:
        if not source.asr_enabled:
            print(f"[asr] {source.id}: disabled")
            continue
        if not source.asr_command:
            print(f"[asr] {source.id}: missing asr_command, skip")
            continue

        rows = connection.execute(
            """
            SELECT
                v.video_id,
                v.media_path,
                COALESCE(a.status, ''),
                a.output_path,
                COALESCE(a.attempts, 0)
            FROM videos v
            LEFT JOIN asr_runs a
                ON a.source_id = v.source_id
               AND a.video_id = v.video_id
            WHERE v.source_id = ?
              AND v.has_media = 1
            ORDER BY v.upload_date DESC, v.video_id DESC
            """,
            (source.id,),
        ).fetchall()
        if not rows:
            print(f"[asr] {source.id}: no videos with media in ledger")
            continue

        candidates: list[tuple[str, Path, str | None, int]] = []
        for video_id, media_path_value, status, output_path_value, attempts in rows:
            if media_path_value in (None, ""):
                continue
            media_path = Path(str(media_path_value))
            if not media_path.exists():
                continue

            has_valid_output = False
            if output_path_value not in (None, ""):
                has_valid_output = Path(str(output_path_value)).exists()

            should_run = force or not (status == "success" and has_valid_output)
            if not should_run:
                continue
            candidates.append((str(video_id), media_path, output_path_value, int(attempts)))

        if not candidates:
            print(f"[asr] {source.id}: up to date")
            continue

        max_per_source = source.asr_max_per_run
        if max_per_source_override is not None:
            max_per_source = max_per_source_override
        if max_per_source > 0:
            candidates = candidates[:max_per_source]

        print(f"[asr] {source.id}: queued={len(candidates)}")
        source.asr_dir.mkdir(parents=True, exist_ok=True)
        final_dir = source.asr_dir / "final"
        final_dir.mkdir(parents=True, exist_ok=True)

        for video_id, media_path, previous_output_path, attempts in candidates:
            artifact_dir = source.asr_dir / video_id
            work_dir = artifact_dir / "work"
            replacements = {
                "input_path": str(media_path),
                "media_path": str(media_path),
                "video_id": video_id,
                "source_id": source.id,
                "work_dir": str(work_dir),
                "artifact_dir": str(artifact_dir),
                "asr_dir": str(source.asr_dir),
            }
            command = render_asr_command(source.asr_command, replacements)
            print("$", shlex.join(command))

            if dry_run:
                continue

            artifact_dir.mkdir(parents=True, exist_ok=True)
            work_dir.mkdir(parents=True, exist_ok=True)
            run_attempt = attempts + 1
            started_at = now_utc_iso()

            upsert_asr_run(
                connection=connection,
                source_id=source.id,
                video_id=video_id,
                status="running",
                attempts=run_attempt,
                engine="command",
                output_path=str(previous_output_path) if previous_output_path else None,
                artifact_dir=str(artifact_dir),
                last_error=None,
                started_at=started_at,
                finished_at=None,
            )
            connection.commit()

            timeout = source.asr_timeout_sec if source.asr_timeout_sec > 0 else None
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                finished_at = now_utc_iso()
                upsert_asr_run(
                    connection=connection,
                    source_id=source.id,
                    video_id=video_id,
                    status="error",
                    attempts=run_attempt,
                    engine="command",
                    output_path=str(previous_output_path) if previous_output_path else None,
                    artifact_dir=str(artifact_dir),
                    last_error=f"timeout after {source.asr_timeout_sec}s",
                    started_at=started_at,
                    finished_at=finished_at,
                )
                connection.commit()
                print(f"[asr] {source.id}/{video_id}: timeout")
                continue

            if completed.returncode != 0:
                finished_at = now_utc_iso()
                upsert_asr_run(
                    connection=connection,
                    source_id=source.id,
                    video_id=video_id,
                    status="error",
                    attempts=run_attempt,
                    engine="command",
                    output_path=str(previous_output_path) if previous_output_path else None,
                    artifact_dir=str(artifact_dir),
                    last_error=f"command exit code {completed.returncode}",
                    started_at=started_at,
                    finished_at=finished_at,
                )
                connection.commit()
                print(f"[asr] {source.id}/{video_id}: failed ({completed.returncode})")
                continue

            subtitle_candidate = pick_asr_subtitle_file(artifact_dir, source.asr_prefer_exts)
            if subtitle_candidate is None:
                finished_at = now_utc_iso()
                upsert_asr_run(
                    connection=connection,
                    source_id=source.id,
                    video_id=video_id,
                    status="error",
                    attempts=run_attempt,
                    engine="command",
                    output_path=str(previous_output_path) if previous_output_path else None,
                    artifact_dir=str(artifact_dir),
                    last_error=(
                        "ASR command succeeded but no subtitle file "
                        f"found in {artifact_dir}"
                    ),
                    started_at=started_at,
                    finished_at=finished_at,
                )
                connection.commit()
                print(f"[asr] {source.id}/{video_id}: no subtitle artifacts")
                continue

            final_output_path = final_dir / f"{video_id}.asr{subtitle_candidate.suffix.lower()}"
            shutil.copy2(subtitle_candidate, final_output_path)

            finished_at = now_utc_iso()
            upsert_asr_run(
                connection=connection,
                source_id=source.id,
                video_id=video_id,
                status="success",
                attempts=run_attempt,
                engine="command",
                output_path=str(final_output_path),
                artifact_dir=str(artifact_dir),
                last_error=None,
                started_at=started_at,
                finished_at=finished_at,
            )
            connection.commit()
            print(f"[asr] {source.id}/{video_id}: {final_output_path}")

    export_csv(connection, csv_path)
    connection.close()
    mode = "dry-run" if dry_run else "run"
    print(f"[asr] completed ({mode}) -> {db_path}")


def run_backfill(
    sources: list[SourceConfig],
    db_path: Path,
    csv_path: Path,
    dry_run: bool = False,
    skip_media: bool = False,
    skip_subs: bool = False,
    skip_meta: bool = False,
    skip_ledger: bool = False,
    full_ledger: bool = False,
    windows_override: int | None = None,
    reset: bool = False,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path))
    create_schema(connection)
    any_work = False

    try:
        for source in sources:
            if not source.backfill_enabled:
                print(f"[backfill] {source.id}: disabled")
                continue

            window_size = max(1, source.backfill_window)
            windows_per_run = source.backfill_windows_per_run
            if windows_override is not None:
                windows_per_run = max(1, windows_override)

            if dry_run:
                next_start = get_default_backfill_start(source)
                if source.backfill_start is not None:
                    next_start = source.backfill_start
                print(
                    f"\n=== backfill: {source.id} ===\n"
                    f"[backfill] dry-run window={window_size} windows_per_run={windows_per_run} "
                    f"next_start={next_start}"
                )
            else:
                state_status, next_start, stored_window_size = ensure_backfill_state(
                    connection=connection,
                    source=source,
                    reset=reset,
                )
                window_size = max(1, stored_window_size)
                if state_status == "completed":
                    print(
                        f"[backfill] {source.id}: completed "
                        f"(next_start={next_start}, window={window_size})"
                    )
                    continue
                print(
                    f"\n=== backfill: {source.id} ===\n"
                    f"[backfill] window={window_size} windows_per_run={windows_per_run} "
                    f"next_start={next_start}"
                )

            for window_index in range(windows_per_run):
                playlist_start = next_start
                playlist_end = playlist_start + window_size - 1
                print(
                    f"[backfill] {source.id}: "
                    f"step={window_index + 1}/{windows_per_run} range={playlist_start}-{playlist_end}"
                )

                try:
                    window_ids = discover_playlist_window_ids(
                        source=source,
                        playlist_start=playlist_start,
                        playlist_end=playlist_end,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    print(
                        f"[backfill] {source.id}: discovery failed for "
                        f"{playlist_start}-{playlist_end}: {exc}",
                        file=sys.stderr,
                    )
                    if not dry_run:
                        update_backfill_state(
                            connection=connection,
                            source_id=source.id,
                            status="active",
                            next_start=playlist_start,
                            window_size=window_size,
                            last_window_start=playlist_start,
                            last_window_end=playlist_end,
                            last_seen_count=None,
                            completed_at=None,
                        )
                        connection.commit()
                    break

                if dry_run:
                    print(
                        f"[backfill] {source.id}: dry-run skips sync for "
                        f"range={playlist_start}-{playlist_end}"
                    )
                    next_start = playlist_end + 1
                    continue

                seen_count = len(window_ids)
                if seen_count == 0:
                    update_backfill_state(
                        connection=connection,
                        source_id=source.id,
                        status="completed",
                        next_start=playlist_start,
                        window_size=window_size,
                        last_window_start=playlist_start,
                        last_window_end=playlist_end,
                        last_seen_count=0,
                        completed_at=now_utc_iso(),
                    )
                    connection.commit()
                    print(
                        f"[backfill] {source.id}: reached tail "
                        f"at range={playlist_start}-{playlist_end}, completed"
                    )
                    break

                any_work = True
                sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=skip_media,
                    skip_subs=skip_subs,
                    skip_meta=skip_meta,
                    connection=connection,
                    playlist_start=playlist_start,
                    playlist_end=playlist_end,
                    metadata_candidate_ids=window_ids,
                    run_label="backfill",
                )

                reached_tail = seen_count < window_size
                next_start = playlist_end + 1
                update_backfill_state(
                    connection=connection,
                    source_id=source.id,
                    status="completed" if reached_tail else "active",
                    next_start=next_start,
                    window_size=window_size,
                    last_window_start=playlist_start,
                    last_window_end=playlist_end,
                    last_seen_count=seen_count,
                    completed_at=now_utc_iso() if reached_tail else None,
                )
                connection.commit()

                if reached_tail:
                    print(
                        f"[backfill] {source.id}: reached tail "
                        f"(seen={seen_count} < window={window_size})"
                    )
                    break
    finally:
        connection.close()

    if not skip_ledger and not dry_run and any_work:
        build_ledger(
            sources,
            db_path,
            csv_path,
            incremental=not full_ledger,
        )
    elif dry_run and not skip_ledger:
        print("dry-run: skip ledger rebuild")
    elif not any_work and not dry_run:
        print("[backfill] no new window work this run")


def show_download_report(
    sources: list[SourceConfig],
    db_path: Path,
    since_hours: int = 24,
    limit: int = 20,
) -> None:
    if not db_path.exists():
        print(f"[downloads] no ledger DB: {db_path}")
        return

    connection = sqlite3.connect(str(db_path))
    create_schema(connection)
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=since_hours)
    since_iso = since.replace(microsecond=0).isoformat()

    for source in sources:
        print(f"\n=== downloads: {source.id} (last {since_hours}h) ===")
        run_rows = connection.execute(
            """
            SELECT
                started_at,
                stage,
                status,
                COALESCE(target_count, 0),
                COALESCE(success_count, 0),
                COALESCE(failure_count, 0),
                COALESCE(exit_code, 0),
                COALESCE(error_message, '')
            FROM download_runs
            WHERE source_id = ?
              AND started_at >= ?
            ORDER BY started_at DESC, run_id DESC
            LIMIT ?
            """,
            (source.id, since_iso, limit),
        ).fetchall()
        if not run_rows:
            print("no run records in range")
        else:
            for (
                started_at,
                stage,
                status,
                target_count,
                success_count,
                failure_count,
                exit_code,
                error_message,
            ) in run_rows:
                print(
                    f"{started_at} stage={stage} status={status} "
                    f"target={target_count} success={success_count} "
                    f"failed={failure_count} exit={exit_code}"
                )
                if error_message:
                    print(f"  error: {error_message}")

        failure_rows = connection.execute(
            """
            SELECT
                stage,
                video_id,
                retry_count,
                COALESCE(last_error, ''),
                COALESCE(next_retry_at, '')
            FROM download_state
            WHERE source_id = ?
              AND status = 'error'
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (source.id, limit),
        ).fetchall()
        if not failure_rows:
            print("no pending failures")
        else:
            print("pending failures:")
            for stage, video_id, retry_count, last_error, next_retry_at in failure_rows:
                print(
                    f"  stage={stage} video_id={video_id} retry_count={retry_count} "
                    f"next_retry_at={next_retry_at}"
                )
                if last_error:
                    print(f"    reason: {last_error}")

    connection.close()


def resolve_output_path(path_value: Path | None, default_path: Path) -> Path:
    if path_value is None:
        return default_path
    expanded = path_value.expanduser()
    if expanded.is_absolute():
        return expanded
    return (Path.cwd() / expanded).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Substudy sync and ledger tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Download updates and refresh ledger (incremental by default)",
    )
    sync_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    sync_parser.add_argument("--source", action="append", dest="sources")
    sync_parser.add_argument("--dry-run", action="store_true")
    sync_parser.add_argument("--skip-media", action="store_true")
    sync_parser.add_argument("--skip-subs", action="store_true")
    sync_parser.add_argument("--skip-meta", action="store_true")
    sync_parser.add_argument("--skip-ledger", action="store_true")
    sync_parser.add_argument(
        "--full-ledger",
        action="store_true",
        help="Run a full ledger rebuild (scan all files) instead of incremental update.",
    )
    sync_parser.add_argument("--ledger-db", type=Path)
    sync_parser.add_argument("--ledger-csv", type=Path)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Fetch older playlist windows incrementally and advance per-source cursors",
    )
    backfill_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    backfill_parser.add_argument("--source", action="append", dest="sources")
    backfill_parser.add_argument("--dry-run", action="store_true")
    backfill_parser.add_argument("--skip-media", action="store_true")
    backfill_parser.add_argument("--skip-subs", action="store_true")
    backfill_parser.add_argument("--skip-meta", action="store_true")
    backfill_parser.add_argument("--skip-ledger", action="store_true")
    backfill_parser.add_argument(
        "--full-ledger",
        action="store_true",
        help="Run a full ledger rebuild after backfill instead of incremental update.",
    )
    backfill_parser.add_argument(
        "--windows",
        type=int,
        help="Override how many windows each source processes in this run.",
    )
    backfill_parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset saved backfill cursor before running.",
    )
    backfill_parser.add_argument("--ledger-db", type=Path)
    backfill_parser.add_argument("--ledger-csv", type=Path)

    ledger_parser = subparsers.add_parser("ledger", help="Rebuild ledger only")
    ledger_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ledger_parser.add_argument("--source", action="append", dest="sources")
    ledger_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Update ledger incrementally from archive deltas/unresolved rows.",
    )
    ledger_parser.add_argument("--ledger-db", type=Path)
    ledger_parser.add_argument("--ledger-csv", type=Path)

    asr_parser = subparsers.add_parser(
        "asr",
        help="Generate ASR subtitles incrementally from local media files",
    )
    asr_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    asr_parser.add_argument("--source", action="append", dest="sources")
    asr_parser.add_argument("--dry-run", action="store_true")
    asr_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run ASR even when successful artifacts already exist.",
    )
    asr_parser.add_argument(
        "--max-per-source",
        type=int,
        help="Override per-source ASR batch size for this run.",
    )
    asr_parser.add_argument("--ledger-db", type=Path)
    asr_parser.add_argument("--ledger-csv", type=Path)

    downloads_parser = subparsers.add_parser(
        "downloads",
        help="Show recent download run logs and pending failures",
    )
    downloads_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    downloads_parser.add_argument("--source", action="append", dest="sources")
    downloads_parser.add_argument(
        "--since-hours",
        type=int,
        default=24,
        help="Lookback window for download_runs.",
    )
    downloads_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max rows per source for runs/failures.",
    )
    downloads_parser.add_argument("--ledger-db", type=Path)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        global_config, all_sources = load_config(args.config)
        sources = select_sources(all_sources, args.sources)
    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    ledger_db_path = resolve_output_path(getattr(args, "ledger_db", None), global_config.ledger_db)
    ledger_csv_path = resolve_output_path(getattr(args, "ledger_csv", None), global_config.ledger_csv)

    if args.command == "sync":
        sync_connection: sqlite3.Connection | None = None
        if not args.dry_run:
            ledger_db_path.parent.mkdir(parents=True, exist_ok=True)
            sync_connection = sqlite3.connect(str(ledger_db_path))
            create_schema(sync_connection)
        try:
            for source in sources:
                sync_source(
                    source=source,
                    dry_run=args.dry_run,
                    skip_media=args.skip_media,
                    skip_subs=args.skip_subs,
                    skip_meta=args.skip_meta,
                    connection=sync_connection,
                )
        finally:
            if sync_connection is not None:
                sync_connection.close()

        if not args.skip_ledger and not args.dry_run:
            build_ledger(
                sources,
                ledger_db_path,
                ledger_csv_path,
                incremental=not args.full_ledger,
            )
        elif args.dry_run and not args.skip_ledger:
            print("dry-run: skip ledger rebuild")
        return 0

    if args.command == "backfill":
        run_backfill(
            sources=sources,
            db_path=ledger_db_path,
            csv_path=ledger_csv_path,
            dry_run=args.dry_run,
            skip_media=args.skip_media,
            skip_subs=args.skip_subs,
            skip_meta=args.skip_meta,
            skip_ledger=args.skip_ledger,
            full_ledger=args.full_ledger,
            windows_override=args.windows,
            reset=args.reset,
        )
        return 0

    if args.command == "ledger":
        build_ledger(
            sources,
            ledger_db_path,
            ledger_csv_path,
            incremental=args.incremental,
        )
        return 0

    if args.command == "asr":
        run_asr(
            sources=sources,
            db_path=ledger_db_path,
            csv_path=ledger_csv_path,
            dry_run=args.dry_run,
            force=args.force,
            max_per_source_override=args.max_per_source,
        )
        return 0

    if args.command == "downloads":
        show_download_report(
            sources=sources,
            db_path=ledger_db_path,
            since_hours=max(1, args.since_hours),
            limit=max(1, args.limit),
        )
        return 0

    print("error: unsupported command", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
