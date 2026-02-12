#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import math
import mimetypes
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import zlib
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

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
DEFAULT_LOUDNESS_TARGET_LUFS = -16.0
DEFAULT_LOUDNESS_MAX_BOOST_DB = 6.0
DEFAULT_LOUDNESS_MAX_CUT_DB = 12.0
DEFAULT_LOUDNESS_LIMIT = 300
DEFAULT_LOUDNESS_FFMPEG_BIN = "ffmpeg"
DEFAULT_DICT_SOURCE_NAME = "eijiro-1449"
DEFAULT_DICT_ENCODING = "utf-8"
DEFAULT_DICT_PATH = Path("data/eijiro-1449.utf8.txt")
DEFAULT_DICT_LOOKUP_LIMIT = 8
DICT_INDEX_BATCH_SIZE = 2000
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 8876
WEB_STATIC_DIR = Path(__file__).resolve().parent / "web"
MISSING_DICT_ENTRY_ID_BASE = 3_000_000_000


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
    cookies_browser: str | None
    cookies_file: Path | None
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


def parse_optional_path(base: Path, raw_value: Any) -> Path | None:
    if raw_value in (None, ""):
        return None
    candidate = Path(str(raw_value)).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
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
    global_cookies_browser_raw = global_raw.get("cookies_from_browser", "chrome")
    global_cookies_browser = (
        str(global_cookies_browser_raw)
        if global_cookies_browser_raw not in (None, "")
        else None
    )
    global_cookies_file = parse_optional_path(cwd, global_raw.get("cookies_file"))

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
        source_cookies_browser_raw = source_raw.get(
            "cookies_from_browser",
            global_cookies_browser,
        )
        source_cookies_browser = (
            str(source_cookies_browser_raw)
            if source_cookies_browser_raw not in (None, "")
            else None
        )
        source_cookies_file_raw = source_raw.get("cookies_file")
        source_cookies_file = (
            parse_optional_path(data_dir, source_cookies_file_raw)
            if source_cookies_file_raw not in (None, "")
            else global_cookies_file
        )

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
            cookies_browser=source_cookies_browser,
            cookies_file=source_cookies_file,
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


def resolve_cookie_flags(source: SourceConfig) -> list[str]:
    if source.cookies_file is not None:
        if source.cookies_file.exists():
            return ["--cookies", str(source.cookies_file)]
        print(
            f"[cookies] {source.id}: configured file not found ({source.cookies_file}); "
            "falling back to browser cookies",
            file=sys.stderr,
        )
    if source.cookies_browser:
        return ["--cookies-from-browser", source.cookies_browser]
    return []


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
    cookie_flags = resolve_cookie_flags(source)
    if connection is not None and not dry_run:
        recovered_any = False
        for stage in ("media", "subs", "meta"):
            recovered = recover_interrupted_download_runs(
                connection=connection,
                source_id=source.id,
                stage=stage,
            )
            if recovered:
                recovered_any = True
                print(f"[sync] {source.id} {stage}: recovered {recovered} interrupted runs")
        if recovered_any:
            connection.commit()

    def safe_video_url(video_id: str) -> str | None:
        try:
            return build_video_url(source, video_id)
        except ValueError:
            return None

    media_before_ids = set(read_archive_ids(source.media_archive))
    new_media_ids: list[str] = []
    if not skip_media:
        media_command = [
            source.ytdlp_bin,
            *cookie_flags,
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
        media_audio_fallback_failures: dict[str, str] = {}
        media_audio_fallback_repaired = 0
        ffprobe_bin = shutil.which("ffprobe")
        if (
            not dry_run
            and media_exit_code == 0
            and new_media_ids
            and ffprobe_bin is not None
        ):
            for video_id in new_media_ids:
                media_path = find_media_file_for_video(source, video_id)
                if media_path is None or not media_path.exists():
                    media_audio_fallback_failures[video_id] = "media file missing after download"
                    continue

                has_audio_stream, probe_error = detect_audio_stream(
                    media_path=media_path,
                    ffprobe_bin=ffprobe_bin,
                )
                if has_audio_stream is True:
                    continue
                if has_audio_stream is None:
                    print(
                        f"[media] {source.id}/{video_id}: ffprobe warning ({probe_error}); "
                        "skip audio fallback",
                        file=sys.stderr,
                    )
                    continue

                video_url = safe_video_url(video_id)
                if video_url is None:
                    media_audio_fallback_failures[video_id] = "cannot build video URL for fallback"
                    continue

                fallback_command = [
                    source.ytdlp_bin,
                    *cookie_flags,
                    "--continue",
                    "--force-overwrites",
                    *retry_flags,
                    "-f",
                    "download",
                    "-o",
                    str(media_path),
                    "--no-playlist",
                    video_url,
                ]
                fallback_exit_code = run_command(
                    fallback_command,
                    dry_run=False,
                    raise_on_error=False,
                )
                if fallback_exit_code != 0:
                    media_audio_fallback_failures[video_id] = (
                        f"audio fallback command exit code {fallback_exit_code}"
                    )
                    print(
                        f"[media] {source.id}/{video_id}: audio fallback failed "
                        f"({fallback_exit_code})",
                        file=sys.stderr,
                    )
                    continue

                has_audio_after, probe_error_after = detect_audio_stream(
                    media_path=media_path,
                    ffprobe_bin=ffprobe_bin,
                )
                if has_audio_after is True:
                    media_audio_fallback_repaired += 1
                    print(
                        f"[media] {source.id}/{video_id}: recovered audio stream "
                        "(format=download)"
                    )
                    continue

                if has_audio_after is False:
                    media_audio_fallback_failures[video_id] = (
                        "audio fallback still has no audio stream"
                    )
                else:
                    media_audio_fallback_failures[video_id] = (
                        f"audio fallback ffprobe warning: {probe_error_after or 'unknown'}"
                    )
                print(
                    f"[media] {source.id}/{video_id}: audio fallback did not produce audio",
                    file=sys.stderr,
                )
        elif (
            not dry_run
            and media_exit_code == 0
            and new_media_ids
            and ffprobe_bin is None
        ):
            print(
                "[media] ffprobe not found; skipping audio-stream validation for new media",
                file=sys.stderr,
            )

        if connection is not None and not dry_run:
            for video_id in new_media_ids:
                fallback_error = media_audio_fallback_failures.get(video_id)
                upsert_download_state(
                    connection=connection,
                    source_id=source.id,
                    stage="media",
                    video_id=video_id,
                    status="success" if fallback_error is None else "error",
                    run_id=media_run_id,
                    attempt_at=media_started_at,
                    url=safe_video_url(video_id),
                    last_error=fallback_error,
                    retry_count=0 if fallback_error is None else 1,
                    next_retry_at=(
                        None
                        if fallback_error is None
                        else schedule_next_retry_iso(1)
                    ),
                )

            media_failed_count = len(media_audio_fallback_failures)
            media_success_count = len(new_media_ids) - media_failed_count
            media_stage_error = (
                media_error
                or (
                    None
                    if media_exit_code == 0 and media_failed_count == 0
                    else (
                        f"command exit code {media_exit_code}"
                        if media_exit_code != 0
                        else "some media files have no audio after fallback"
                    )
                )
            )
            finish_download_run(
                connection=connection,
                run_id=media_run_id if media_run_id is not None else -1,
                status=(
                    "success"
                    if media_exit_code == 0 and media_failed_count == 0
                    else "error"
                ),
                finished_at=now_utc_iso(),
                exit_code=media_exit_code,
                success_count=media_success_count if media_exit_code == 0 else len(new_media_ids),
                failure_count=(
                    media_failed_count
                    if media_exit_code == 0
                    else None
                ),
                error_message=media_stage_error,
            )
            connection.commit()

        if media_audio_fallback_repaired > 0:
            print(
                f"[media] {source.id}: audio fallback repaired="
                f"{media_audio_fallback_repaired}"
            )
        if media_audio_fallback_failures:
            print(
                f"[media] {source.id}: audio fallback failed={len(media_audio_fallback_failures)}",
                file=sys.stderr,
            )
        print(f"media new IDs: {len(new_media_ids)}")
    else:
        print("skip media")

    subs_before_ids = set(read_archive_ids(source.subs_archive))
    if not skip_subs:
        if metadata_candidate_ids is not None:
            subtitle_candidate_ids = []
            seen_subtitle_candidates: set[str] = set()
            for video_id in metadata_candidate_ids:
                if video_id in seen_subtitle_candidates:
                    continue
                seen_subtitle_candidates.add(video_id)
                subtitle_candidate_ids.append(video_id)
        else:
            subtitle_candidate_ids = list(new_media_ids)

        local_sub_ids = set(scan_subtitles(source).keys())
        missing_sub_ids = [
            video_id
            for video_id in subtitle_candidate_ids
            if video_id not in subs_before_ids and video_id not in local_sub_ids
        ]

        deferred_sub_ids: list[str] = []
        missing_retryable_sub_ids = list(missing_sub_ids)
        if connection is not None and not dry_run:
            missing_retryable_sub_ids, deferred_sub_ids = split_retryable_ids(
                connection=connection,
                source_id=source.id,
                stage="subs",
                candidate_ids=missing_sub_ids,
            )

        retry_sub_ids: list[str] = []
        if connection is not None and not dry_run:
            retry_sub_ids = get_due_retry_ids(connection, source.id, "subs")

        subtitle_target_ids: list[str] = []
        seen_target_ids: set[str] = set()
        for video_id in [*missing_retryable_sub_ids, *retry_sub_ids]:
            if video_id in seen_target_ids:
                continue
            seen_target_ids.add(video_id)
            subtitle_target_ids.append(video_id)

        if not subtitle_target_ids:
            if connection is not None and not dry_run:
                subs_started_at = now_utc_iso()
                subs_run_id = begin_download_run(
                    connection=connection,
                    source_id=source.id,
                    stage="subs",
                    command=None,
                    target_count=0,
                    started_at=subs_started_at,
                )
                finish_download_run(
                    connection=connection,
                    run_id=subs_run_id,
                    status="success",
                    finished_at=now_utc_iso(),
                    exit_code=0,
                    success_count=0,
                    failure_count=0,
                    error_message=None,
                )
                connection.commit()
            print(
                f"subtitle targets=0 success=0 failed=0 "
                f"(new={len(missing_retryable_sub_ids)}, retry={len(retry_sub_ids)}, "
                f"deferred={len(deferred_sub_ids)})"
            )
        else:
            subtitle_urls = [build_video_url(source, video_id) for video_id in subtitle_target_ids]
            if dry_run:
                print(
                    f"subtitle targets (dry-run): {len(subtitle_target_ids)} "
                    f"(new={len(missing_retryable_sub_ids)}, retry={len(retry_sub_ids)}, "
                    f"deferred={len(deferred_sub_ids)})"
                )
            else:
                write_urls_file(source.urls_file, subtitle_urls)

            subs_command = [
                source.ytdlp_bin,
                *cookie_flags,
                "--download-archive",
                str(source.subs_archive),
                "--continue",
                "--no-overwrites",
                *retry_flags,
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
                "-a",
                str(source.urls_file),
            ]
            subs_started_at = now_utc_iso()
            subs_run_id: int | None = None
            if connection is not None and not dry_run:
                subs_run_id = begin_download_run(
                    connection=connection,
                    source_id=source.id,
                    stage="subs",
                    command=subs_command,
                    target_count=len(subtitle_target_ids),
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

            if not dry_run:
                subs_after_ids = set(read_archive_ids(source.subs_archive))
                local_sub_after_ids = set(scan_subtitles(source).keys())
                success_sub_ids = [
                    video_id
                    for video_id in subtitle_target_ids
                    if video_id in subs_after_ids or video_id in local_sub_after_ids
                ]
                failed_sub_ids = [
                    video_id
                    for video_id in subtitle_target_ids
                    if video_id not in subs_after_ids and video_id not in local_sub_after_ids
                ]

                if connection is not None:
                    for video_id in success_sub_ids:
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

                    for video_id in failed_sub_ids:
                        current = connection.execute(
                            """
                            SELECT retry_count
                            FROM download_state
                            WHERE source_id = ? AND stage = ? AND video_id = ?
                            """,
                            (source.id, "subs", video_id),
                        ).fetchone()
                        next_retry_count = (int(current[0]) if current else 0) + 1
                        failure_reason = subs_error or (
                            f"command exit code {subs_exit_code}"
                            if subs_exit_code != 0
                            else "subtitle file missing after download attempt"
                        )
                        upsert_download_state(
                            connection=connection,
                            source_id=source.id,
                            stage="subs",
                            video_id=video_id,
                            status="error",
                            run_id=subs_run_id,
                            attempt_at=subs_started_at,
                            url=safe_video_url(video_id),
                            last_error=failure_reason,
                            retry_count=next_retry_count,
                            next_retry_at=schedule_next_retry_iso(next_retry_count),
                        )

                    finish_download_run(
                        connection=connection,
                        run_id=subs_run_id if subs_run_id is not None else -1,
                        status="success" if not failed_sub_ids else "error",
                        finished_at=now_utc_iso(),
                        exit_code=subs_exit_code,
                        success_count=len(success_sub_ids),
                        failure_count=len(failed_sub_ids),
                        error_message=None if not failed_sub_ids else (
                            subs_error or (
                                f"command exit code {subs_exit_code}"
                                if subs_exit_code != 0
                                else "some subtitle items are still missing"
                            )
                        ),
                    )
                    connection.commit()

                print(
                    f"subtitle targets={len(subtitle_target_ids)} "
                    f"success={len(success_sub_ids)} failed={len(failed_sub_ids)} "
                    f"(new={len(missing_retryable_sub_ids)}, retry={len(retry_sub_ids)}, "
                    f"deferred={len(deferred_sub_ids)})"
                )
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
        *cookie_flags,
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


def normalize_dictionary_term(raw_value: str) -> str:
    value = str(raw_value or "")
    if not value:
        return ""
    value = (
        value
        .replace("’", "'")
        .replace("‘", "'")
        .replace("`", "'")
        .replace('"', " ")
        .replace("“", " ")
        .replace("”", " ")
        .replace("‐", "-")
        .replace("‑", "-")
        .replace("–", "-")
        .replace("—", "-")
    )
    value = re.sub(r"[,:;!?()\[\]{}<>]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    value = value.strip("\"'()[]{}<>")
    value = re.sub(r"^[^a-z0-9]+|[^a-z0-9]+$", "", value)
    return value


def strip_eijiro_head_annotations(raw_head: str) -> str:
    head = str(raw_head or "").strip()
    if not head:
        return ""
    while True:
        updated = re.sub(r"\s+\{[^{}]+\}\s*$", "", head).strip()
        if updated == head:
            break
        head = updated
    return head


def parse_eijiro_line(raw_line: str, line_no: int) -> dict[str, Any] | None:
    line = raw_line.strip()
    if not line:
        return None
    while line.startswith("■"):
        line = line[1:].strip()
    if not line:
        return None
    if " : " not in line:
        return None
    raw_head, raw_definition = line.split(" : ", 1)
    head = strip_eijiro_head_annotations(raw_head) or raw_head.strip()
    definition = raw_definition.strip()
    if not head or not definition:
        return None
    term_norm = normalize_dictionary_term(head)
    if not term_norm:
        return None
    return {
        "term": head,
        "term_norm": term_norm,
        "definition": definition,
        "line_no": line_no,
    }


def dictionary_lookup_variants(term_norm: str) -> list[str]:
    base = normalize_dictionary_term(term_norm)
    if not base:
        return []

    variants: list[str] = []
    seen: set[str] = set()

    def add_variant(value: str) -> None:
        normalized = normalize_dictionary_term(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        variants.append(normalized)

    add_variant(base)
    if base.endswith("'s"):
        add_variant(base[:-2])
    if base.endswith("ies") and len(base) > 4:
        add_variant(base[:-3] + "y")
    if base.endswith("ing") and len(base) > 5:
        stem = base[:-3]
        add_variant(stem)
        add_variant(stem + "e")
        if len(stem) > 2 and stem[-1] == stem[-2]:
            add_variant(stem[:-1])
    if base.endswith("ed") and len(base) > 4:
        stem = base[:-2]
        add_variant(stem)
        add_variant(stem + "e")
        if len(stem) > 2 and stem[-1] == stem[-2]:
            add_variant(stem[:-1])
    if base.endswith("es") and len(base) > 4:
        add_variant(base[:-2])
    if base.endswith("s") and len(base) > 3:
        add_variant(base[:-1])
    if "-" in base:
        add_variant(base.replace("-", " "))

    return variants


def escape_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


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
            audio_lufs REAL,
            audio_gain_db REAL,
            audio_loudness_analyzed_at TEXT,
            audio_loudness_error TEXT,
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

        CREATE TABLE IF NOT EXISTS video_favorites (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS subtitle_bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            track TEXT,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            text TEXT,
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS video_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (source_id, video_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS dictionary_bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            track TEXT NOT NULL DEFAULT '',
            cue_start_ms INTEGER NOT NULL,
            cue_end_ms INTEGER NOT NULL,
            cue_text TEXT,
            dict_entry_id INTEGER NOT NULL,
            dict_source_name TEXT,
            lookup_term TEXT,
            term TEXT NOT NULL,
            term_norm TEXT NOT NULL,
            definition TEXT NOT NULL,
            missing_entry INTEGER NOT NULL DEFAULT 0,
            lookup_path_json TEXT NOT NULL DEFAULT '',
            lookup_path_label TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (source_id, video_id, track, cue_start_ms, cue_end_ms, dict_entry_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE INDEX IF NOT EXISTS idx_video_favorites_created_at
            ON video_favorites(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_subtitle_bookmarks_lookup
            ON subtitle_bookmarks(source_id, video_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_notes_lookup
            ON video_notes(source_id, video_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_dictionary_bookmarks_lookup
            ON dictionary_bookmarks(source_id, video_id, updated_at DESC);
        """
    )
    ensure_videos_loudness_columns(connection)
    ensure_dictionary_schema(connection)
    ensure_translation_runs_table(connection)
    ensure_dictionary_bookmarks_schema(connection)


def ensure_videos_loudness_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(videos)").fetchall()
    existing_columns = {str(row[1]) for row in rows}
    required_columns = {
        "audio_lufs": "REAL",
        "audio_gain_db": "REAL",
        "audio_loudness_analyzed_at": "TEXT",
        "audio_loudness_error": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE videos ADD COLUMN {column_name} {column_type}"
        )


def ensure_dictionary_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS dict_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            term TEXT NOT NULL,
            term_norm TEXT NOT NULL,
            definition TEXT NOT NULL,
            line_no INTEGER,
            created_at TEXT NOT NULL,
            UNIQUE (source_name, term_norm, definition)
        );

        CREATE INDEX IF NOT EXISTS idx_dict_entries_term_norm
            ON dict_entries(term_norm);
        CREATE INDEX IF NOT EXISTS idx_dict_entries_source_term_norm
            ON dict_entries(source_name, term_norm);
        """
    )
    try:
        connection.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS dict_entries_fts USING fts5(
                term_norm,
                term,
                definition,
                content='dict_entries',
                content_rowid='id'
            );
            """
        )
    except sqlite3.OperationalError:
        # Some SQLite builds do not have FTS5 enabled.
        return


def ensure_translation_runs_table(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS translation_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id       TEXT NOT NULL,
            video_id        TEXT NOT NULL,
            source_lang     TEXT NOT NULL DEFAULT 'en',
            target_lang     TEXT NOT NULL DEFAULT 'ja',
            source_path     TEXT NOT NULL,
            output_path     TEXT NOT NULL,
            cue_count       INTEGER,
            cue_match       INTEGER,
            agent           TEXT,
            method          TEXT,
            method_version  TEXT,
            summary         TEXT,
            status          TEXT NOT NULL DEFAULT 'active',
            started_at      TEXT,
            finished_at     TEXT,
            created_at      TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );
        CREATE INDEX IF NOT EXISTS idx_translation_runs_video
            ON translation_runs(source_id, video_id, target_lang, status);
        """
    )


def ensure_dictionary_bookmarks_schema(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(dictionary_bookmarks)").fetchall()
    if not rows:
        return
    existing_columns = {str(row[1]) for row in rows}
    if "missing_entry" not in existing_columns:
        connection.execute(
            """
            ALTER TABLE dictionary_bookmarks
            ADD COLUMN missing_entry INTEGER NOT NULL DEFAULT 0
            """
        )
    if "lookup_path_json" not in existing_columns:
        connection.execute(
            """
            ALTER TABLE dictionary_bookmarks
            ADD COLUMN lookup_path_json TEXT NOT NULL DEFAULT ''
            """
        )
    if "lookup_path_label" not in existing_columns:
        connection.execute(
            """
            ALTER TABLE dictionary_bookmarks
            ADD COLUMN lookup_path_label TEXT NOT NULL DEFAULT ''
            """
        )


def make_missing_dict_entry_id(term_norm: str) -> int:
    normalized = normalize_dictionary_term(term_norm)
    digest = zlib.crc32(normalized.encode("utf-8")) & 0xFFFFFFFF
    return MISSING_DICT_ENTRY_ID_BASE + int(digest)


def normalize_dictionary_lookup_path(raw_value: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for raw_item in raw_value:
        term = ""
        term_norm = ""
        source = ""
        node_id: int | None = None
        parent_id: int | None = None
        level: int | None = None
        if isinstance(raw_item, dict):
            term = str(raw_item.get("term") or "").strip()
            term_norm = normalize_dictionary_term(raw_item.get("term_norm") or term)
            source = str(raw_item.get("source") or "").strip()
            try:
                parsed_node_id = int(raw_item.get("node_id"))
            except (TypeError, ValueError):
                parsed_node_id = 0
            if parsed_node_id > 0:
                node_id = parsed_node_id
            try:
                parsed_parent_id = int(raw_item.get("parent_id"))
            except (TypeError, ValueError):
                parsed_parent_id = 0
            if parsed_parent_id > 0:
                parent_id = parsed_parent_id
            try:
                parsed_level = int(raw_item.get("level"))
            except (TypeError, ValueError):
                parsed_level = 0
            if parsed_level > 0:
                level = parsed_level
        elif raw_item not in (None, ""):
            term = str(raw_item).strip()
            term_norm = normalize_dictionary_term(term)
        if not term and term_norm:
            term = term_norm
        if not term and not term_norm:
            continue
        entry: dict[str, Any] = {
            "level": level if level and level > 0 else len(normalized) + 1,
            "term": term,
            "term_norm": term_norm,
            "source": source,
        }
        if node_id is not None:
            entry["node_id"] = node_id
        if parent_id is not None:
            entry["parent_id"] = parent_id
        normalized.append(entry)
        if len(normalized) >= 24:
            break
    return normalized


def build_dictionary_lookup_path_label(path: list[dict[str, Any]]) -> str:
    labels: list[str] = []
    for item in path:
        if not isinstance(item, dict):
            continue
        term = str(item.get("term") or "").strip()
        if term:
            labels.append(term)
    return " > ".join(labels)


def rebuild_dictionary_fts(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'dict_entries_fts'
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return False
    try:
        connection.execute(
            "INSERT INTO dict_entries_fts(dict_entries_fts) VALUES ('rebuild')"
        )
    except sqlite3.OperationalError:
        return False
    return True


def lookup_dictionary_entries(
    connection: sqlite3.Connection,
    term: str,
    limit: int = DEFAULT_DICT_LOOKUP_LIMIT,
    exact_only: bool = False,
    fts_mode: str = "all",
) -> dict[str, Any]:
    def read_field(
        row: sqlite3.Row | tuple[Any, ...],
        index: int,
        name: str,
    ) -> Any:
        if isinstance(row, sqlite3.Row):
            return row[name]
        return row[index]

    normalized = normalize_dictionary_term(term)
    if not normalized:
        return {
            "term": term,
            "normalized": "",
            "results": [],
        }

    safe_limit = max(1, min(20, int(limit)))
    safe_fts_mode = str(fts_mode or "all").strip().lower()
    if safe_fts_mode not in {"all", "term", "off"}:
        safe_fts_mode = "all"
    variants = dictionary_lookup_variants(normalized)
    if not variants:
        return {
            "term": term,
            "normalized": normalized,
            "results": [],
        }

    placeholders = ",".join("?" for _ in variants)
    exact_rows = connection.execute(
        f"""
        SELECT id, source_name, term, term_norm, definition
        FROM dict_entries
        WHERE term_norm IN ({placeholders})
        ORDER BY
            CASE
                WHEN term_norm = ? THEN 0
                ELSE 1
            END,
            LENGTH(term_norm) ASC,
            id ASC
        LIMIT ?
        """,
        (*variants, normalized, safe_limit),
    ).fetchall()

    selected_rows: list[sqlite3.Row | tuple[Any, ...]] = list(exact_rows)
    seen_ids = {int(read_field(row, 0, "id")) for row in exact_rows}

    if exact_only:
        results: list[dict[str, Any]] = []
        for row in selected_rows:
            results.append(
                {
                    "id": int(read_field(row, 0, "id")),
                    "source_name": str(read_field(row, 1, "source_name")),
                    "term": str(read_field(row, 2, "term")),
                    "term_norm": str(read_field(row, 3, "term_norm")),
                    "definition": str(read_field(row, 4, "definition")),
                }
            )
        return {
            "term": term,
            "normalized": normalized,
            "results": results,
        }

    if len(selected_rows) < safe_limit:
        remaining = safe_limit - len(selected_rows)
        prefix_pattern = f"{escape_like_pattern(normalized)}%"
        prefix_rows = connection.execute(
            """
            SELECT id, source_name, term, term_norm, definition
            FROM dict_entries
            WHERE term_norm LIKE ? ESCAPE '\\'
            ORDER BY LENGTH(term_norm) ASC, id ASC
            LIMIT ?
            """,
            (prefix_pattern, remaining * 3),
        ).fetchall()
        for row in prefix_rows:
            row_id = int(read_field(row, 0, "id"))
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            selected_rows.append(row)
            if len(selected_rows) >= safe_limit:
                break

    if len(selected_rows) < safe_limit and safe_fts_mode != "off":
        fts_table_exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'dict_entries_fts'
            LIMIT 1
            """
        ).fetchone() is not None
        if fts_table_exists:
            remaining = safe_limit - len(selected_rows)
            safe_fts_term = normalized.replace('"', ' ').strip()
            if safe_fts_term:
                fts_match_expr = safe_fts_term
                if safe_fts_mode == "term":
                    term_tokens = [token for token in safe_fts_term.split() if token]
                    column_terms: list[str] = []
                    for token in term_tokens:
                        cleaned_token = re.sub(r"[^a-z0-9-]+", "", token)
                        if not cleaned_token:
                            continue
                        column_terms.append(
                            f"(term_norm:{cleaned_token} OR term:{cleaned_token})"
                        )
                    if not column_terms:
                        fts_match_expr = ""
                    else:
                        fts_match_expr = " ".join(column_terms)
                if not fts_match_expr:
                    fts_rows: list[sqlite3.Row | tuple[Any, ...]] = []
                else:
                    try:
                        fts_rows = connection.execute(
                            """
                            SELECT de.id, de.source_name, de.term, de.term_norm, de.definition
                            FROM dict_entries_fts fts
                            JOIN dict_entries de
                              ON de.id = fts.rowid
                            WHERE fts.dict_entries_fts MATCH ?
                            ORDER BY LENGTH(de.term_norm) ASC, de.id ASC
                            LIMIT ?
                            """,
                            (fts_match_expr, remaining * 4),
                        ).fetchall()
                    except sqlite3.OperationalError:
                        fts_rows = []
                for row in fts_rows:
                    row_id = int(read_field(row, 0, "id"))
                    if row_id in seen_ids:
                        continue
                    seen_ids.add(row_id)
                    selected_rows.append(row)
                    if len(selected_rows) >= safe_limit:
                        break

    results: list[dict[str, Any]] = []
    for row in selected_rows:
        results.append(
            {
                "id": int(read_field(row, 0, "id")),
                "source_name": str(read_field(row, 1, "source_name")),
                "term": str(read_field(row, 2, "term")),
                "term_norm": str(read_field(row, 3, "term_norm")),
                "definition": str(read_field(row, 4, "definition")),
            }
        )

    return {
        "term": term,
        "normalized": normalized,
        "results": results,
    }


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
        v.audio_lufs,
        v.audio_gain_db,
        v.audio_loudness_analyzed_at,
        v.audio_loudness_error,
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


def prune_source_videos_full_rebuild(
    connection: sqlite3.Connection,
    source_id: str,
    keep_video_ids: list[str],
) -> None:
    """Delete only stale rows after full rebuild without wiping per-video analysis columns."""
    connection.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS tmp_keep_video_ids (
            video_id TEXT PRIMARY KEY
        )
        """
    )
    connection.execute("DELETE FROM tmp_keep_video_ids")
    if keep_video_ids:
        connection.executemany(
            "INSERT OR IGNORE INTO tmp_keep_video_ids(video_id) VALUES (?)",
            ((video_id,) for video_id in keep_video_ids),
        )

    # Keep side tables consistent when full rebuild drops videos that no longer exist.
    for table_name in ("video_favorites", "subtitle_bookmarks", "video_notes"):
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE source_id = ?
              AND video_id NOT IN (
                SELECT video_id FROM tmp_keep_video_ids
              )
            """,
            (source_id,),
        )

    connection.execute(
        """
        DELETE FROM videos
        WHERE source_id = ?
          AND video_id NOT IN (
            SELECT video_id FROM tmp_keep_video_ids
          )
        """,
        (source_id,),
    )


def rebuild_source_full(connection: sqlite3.Connection, source: SourceConfig, synced_at: str) -> None:
    connection.execute("DELETE FROM subtitles WHERE source_id = ?", (source.id,))

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

    prune_source_videos_full_rebuild(connection, source.id, all_video_ids)

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

    # Keep incremental mode aware of subtitle file add/remove updates for existing videos.
    # This allows translation subtitle drops (e.g., *.ja.vtt) to appear without full rebuild.
    subtitle_files = scan_subtitles(source)
    db_subtitle_paths_by_video: dict[str, set[str]] = {}
    for row in connection.execute(
        """
        SELECT video_id, subtitle_path
        FROM subtitles
        WHERE source_id = ?
        """,
        (source.id,),
    ).fetchall():
        video_id = str(row[0])
        subtitle_path = str(row[1])
        db_subtitle_paths_by_video.setdefault(video_id, set()).add(subtitle_path)

    subtitle_changed_ids: set[str] = set()
    subtitle_video_ids = set(subtitle_files) | set(db_subtitle_paths_by_video)
    for video_id in subtitle_video_ids:
        scanned_paths = {
            str(path)
            for _, path, _ in subtitle_files.get(video_id, [])
        }
        db_paths = db_subtitle_paths_by_video.get(video_id, set())
        if scanned_paths != db_paths:
            subtitle_changed_ids.add(video_id)

    candidate_ids = (
        missing_from_db_ids
        | no_meta_ids
        | media_backfill_ids
        | subtitle_backfill_ids
        | subtitle_changed_ids
    )
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
        f"subtitle_changed={len(subtitle_changed_ids)} "
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


def recover_interrupted_download_runs(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    finished_at: str | None = None,
) -> int:
    finished_value = finished_at or now_utc_iso()
    cursor = connection.execute(
        """
        UPDATE download_runs
        SET status = 'error',
            finished_at = COALESCE(finished_at, ?),
            exit_code = COALESCE(exit_code, 130),
            error_message = CASE
                WHEN error_message IS NULL OR error_message = '' THEN 'interrupted previous run'
                ELSE error_message
            END
        WHERE source_id = ?
          AND stage = ?
          AND status = 'running'
        """,
        (
            finished_value,
            source_id,
            stage,
        ),
    )
    return cursor.rowcount


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
    cookie_flags = resolve_cookie_flags(source)
    command = [
        source.ytdlp_bin,
        *cookie_flags,
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


def write_empty_srt(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


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
    ffprobe_bin = shutil.which("ffprobe")

    for source in sources:
        if not source.asr_enabled:
            print(f"[asr] {source.id}: disabled")
            continue
        if not source.asr_command:
            print(f"[asr] {source.id}: missing asr_command, skip")
            continue

        interrupted_finished_at = now_utc_iso()
        marked_rows = connection.execute(
            """
            UPDATE asr_runs
            SET status = 'error',
                last_error = CASE
                    WHEN last_error IS NULL OR last_error = '' THEN 'interrupted previous run'
                    ELSE last_error
                END,
                finished_at = COALESCE(finished_at, ?),
                updated_at = ?
            WHERE source_id = ?
              AND status = 'running'
            """,
            (
                interrupted_finished_at,
                interrupted_finished_at,
                source.id,
            ),
        ).rowcount
        if marked_rows:
            connection.commit()
            print(
                f"[asr] {source.id}: recovered {marked_rows} interrupted running records"
            )

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
            if ffprobe_bin:
                has_audio_stream, probe_error = detect_audio_stream(
                    media_path=media_path,
                    ffprobe_bin=ffprobe_bin,
                )
                if has_audio_stream is False:
                    final_output_path = final_dir / f"{video_id}.asr.srt"
                    write_empty_srt(final_output_path)
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
                    print(
                        f"[asr] {source.id}/{video_id}: no audio stream "
                        f"(wrote {final_output_path})"
                    )
                    continue
                if has_audio_stream is None and probe_error:
                    print(
                        f"[asr] {source.id}/{video_id}: "
                        f"ffprobe warning ({probe_error}); continuing",
                        file=sys.stderr,
                    )
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


def extract_loudnorm_stats(output_text: str) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    best_payload: dict[str, Any] | None = None
    for match in re.finditer(r"\{", output_text):
        chunk = output_text[match.start() :]
        try:
            payload, _ = decoder.raw_decode(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "input_i" in payload:
            best_payload = payload
    return best_payload


def parse_finite_float(raw_value: Any) -> float | None:
    parsed = safe_float(raw_value)
    if parsed is None or not math.isfinite(parsed):
        return None
    return parsed


def analyze_media_loudness(
    media_path: Path,
    ffmpeg_bin: str,
    target_lufs: float,
) -> tuple[float | None, str | None]:
    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-nostats",
        "-i",
        str(media_path),
        "-vn",
        "-sn",
        "-dn",
        "-af",
        f"loudnorm=I={target_lufs:.1f}:TP=-1.5:LRA=11:print_format=json",
        "-f",
        "null",
        "-",
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    merged_output = f"{completed.stderr}\n{completed.stdout}".strip()
    stats = extract_loudnorm_stats(merged_output)
    if stats is None:
        if completed.returncode != 0:
            message = merged_output.splitlines()[-1].strip() if merged_output else ""
            return None, message or f"ffmpeg exited with code {completed.returncode}"
        return None, "loudnorm JSON payload not found"

    input_lufs = parse_finite_float(stats.get("input_i"))
    if input_lufs is None:
        return None, "invalid input_i in loudnorm output"
    return input_lufs, None


def detect_audio_stream(
    media_path: Path,
    ffprobe_bin: str,
) -> tuple[bool | None, str | None]:
    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index",
        "-of",
        "csv=p=0",
        str(media_path),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        merged_output = f"{completed.stderr}\n{completed.stdout}".strip()
        message = merged_output.splitlines()[-1].strip() if merged_output else ""
        return None, message or f"ffprobe exited with code {completed.returncode}"
    return bool(completed.stdout.strip()), None


def run_loudness(
    sources: list[SourceConfig],
    db_path: Path,
    target_lufs: float = DEFAULT_LOUDNESS_TARGET_LUFS,
    max_boost_db: float = DEFAULT_LOUDNESS_MAX_BOOST_DB,
    max_cut_db: float = DEFAULT_LOUDNESS_MAX_CUT_DB,
    limit: int = DEFAULT_LOUDNESS_LIMIT,
    force: bool = False,
    ffmpeg_bin: str = DEFAULT_LOUDNESS_FFMPEG_BIN,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg_candidate = Path(ffmpeg_bin).expanduser()
    has_explicit_path = ffmpeg_candidate.is_absolute() or "/" in ffmpeg_bin or "\\" in ffmpeg_bin
    if has_explicit_path:
        if not ffmpeg_candidate.exists():
            raise RuntimeError(f"ffmpeg binary not found: {ffmpeg_candidate}")
    elif shutil.which(ffmpeg_bin) is None:
        raise RuntimeError(
            f"ffmpeg binary '{ffmpeg_bin}' not found in PATH. "
            "Install ffmpeg or pass --ffmpeg-bin."
        )
    ffprobe_bin = "ffprobe"
    if has_explicit_path:
        sibling_ffprobe = ffmpeg_candidate.with_name("ffprobe")
        if sibling_ffprobe.exists():
            ffprobe_bin = str(sibling_ffprobe)
    has_ffprobe = shutil.which(ffprobe_bin) is not None or (
        Path(ffprobe_bin).expanduser().is_absolute() and Path(ffprobe_bin).exists()
    )

    connection = sqlite3.connect(str(db_path))
    create_schema(connection)

    safe_limit = max(1, int(limit))
    safe_boost = max(0.0, float(max_boost_db))
    safe_cut = max(0.0, float(max_cut_db))

    total_candidates = 0
    total_success = 0
    total_failed = 0
    total_missing = 0

    try:
        for source in sources:
            where_clauses = [
                "source_id = ?",
                "has_media = 1",
                "media_path IS NOT NULL",
            ]
            params: list[Any] = [source.id]
            if not force:
                where_clauses.append(
                    "("
                    "audio_loudness_analyzed_at IS NULL "
                    "OR audio_loudness_analyzed_at = '' "
                    "OR audio_gain_db IS NULL"
                    ")"
                )

            rows = connection.execute(
                f"""
                SELECT video_id, media_path
                FROM videos
                WHERE {" AND ".join(where_clauses)}
                ORDER BY COALESCE(upload_date, '') DESC, video_id DESC
                LIMIT ?
                """,
                (*params, safe_limit),
            ).fetchall()

            if not rows:
                print(f"[loudness] {source.id}: up to date")
                continue

            print(
                f"[loudness] {source.id}: queued={len(rows)} "
                f"target={target_lufs:.1f}LUFS max_boost={safe_boost:.1f}dB max_cut={safe_cut:.1f}dB"
            )

            source_success = 0
            source_failed = 0
            source_missing = 0

            for index, (video_id_value, media_path_value) in enumerate(rows, start=1):
                video_id = str(video_id_value)
                media_path = Path(str(media_path_value))
                analyzed_at = now_utc_iso()

                if not media_path.exists() or not media_path.is_file():
                    connection.execute(
                        """
                        UPDATE videos
                        SET audio_lufs = NULL,
                            audio_gain_db = NULL,
                            audio_loudness_analyzed_at = ?,
                            audio_loudness_error = ?
                        WHERE source_id = ?
                          AND video_id = ?
                        """,
                        (analyzed_at, "media file missing", source.id, video_id),
                    )
                    source_missing += 1
                    print(f"[loudness] {source.id}/{video_id}: media file missing")
                    connection.commit()
                    continue

                if has_ffprobe:
                    has_audio_stream, probe_error = detect_audio_stream(
                        media_path=media_path,
                        ffprobe_bin=ffprobe_bin,
                    )
                    if has_audio_stream is False:
                        connection.execute(
                            """
                            UPDATE videos
                            SET audio_lufs = NULL,
                                audio_gain_db = 0.0,
                                audio_loudness_analyzed_at = ?,
                                audio_loudness_error = ''
                            WHERE source_id = ?
                              AND video_id = ?
                            """,
                            (analyzed_at, source.id, video_id),
                        )
                        source_success += 1
                        print(
                            f"[loudness] {source.id}/{video_id}: "
                            "no audio stream (gain=+0.00dB)"
                        )
                        connection.commit()
                        continue
                    if has_audio_stream is None and probe_error:
                        print(
                            f"[loudness] {source.id}/{video_id}: "
                            f"ffprobe warning ({probe_error}); fallback to loudnorm",
                            file=sys.stderr,
                        )

                input_lufs, error = analyze_media_loudness(
                    media_path=media_path,
                    ffmpeg_bin=ffmpeg_bin,
                    target_lufs=target_lufs,
                )
                if input_lufs is None:
                    connection.execute(
                        """
                        UPDATE videos
                        SET audio_lufs = NULL,
                            audio_gain_db = NULL,
                            audio_loudness_analyzed_at = ?,
                            audio_loudness_error = ?
                        WHERE source_id = ?
                          AND video_id = ?
                        """,
                        (analyzed_at, error or "loudness analysis failed", source.id, video_id),
                    )
                    source_failed += 1
                    print(
                        f"[loudness] {source.id}/{video_id}: failed "
                        f"({error or 'unknown error'})"
                    )
                    connection.commit()
                    continue

                raw_gain_db = target_lufs - input_lufs
                clipped_gain_db = max(-safe_cut, min(safe_boost, raw_gain_db))
                connection.execute(
                    """
                    UPDATE videos
                    SET audio_lufs = ?,
                        audio_gain_db = ?,
                        audio_loudness_analyzed_at = ?,
                        audio_loudness_error = ''
                    WHERE source_id = ?
                      AND video_id = ?
                    """,
                    (
                        input_lufs,
                        clipped_gain_db,
                        analyzed_at,
                        source.id,
                        video_id,
                    ),
                )
                source_success += 1
                print(
                    f"[loudness] {source.id}/{video_id}: "
                    f"LUFS={input_lufs:.2f} gain={clipped_gain_db:+.2f}dB "
                    f"({index}/{len(rows)})"
                )
                connection.commit()

            total_candidates += len(rows)
            total_success += source_success
            total_failed += source_failed
            total_missing += source_missing
            print(
                f"[loudness] {source.id}: ok={source_success} "
                f"failed={source_failed} missing={source_missing}"
            )
    finally:
        connection.close()

    print(
        f"[loudness] completed candidates={total_candidates} "
        f"ok={total_success} failed={total_failed} missing={total_missing} "
        f"db={db_path}"
    )


def run_dict_index(
    db_path: Path,
    dictionary_path: Path,
    source_name: str = DEFAULT_DICT_SOURCE_NAME,
    encoding: str = DEFAULT_DICT_ENCODING,
    clear_existing: bool = True,
    max_lines: int | None = None,
) -> None:
    if not dictionary_path.exists():
        raise FileNotFoundError(f"Dictionary file not found: {dictionary_path}")
    if not dictionary_path.is_file():
        raise ValueError(f"Dictionary path is not a file: {dictionary_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    create_schema(connection)

    max_lines_value = None
    if max_lines is not None and max_lines > 0:
        max_lines_value = int(max_lines)

    total_lines = 0
    parsed_entries = 0
    inserted_entries = 0
    skipped_lines = 0
    duplicate_entries = 0
    now_iso = now_utc_iso()
    batch: list[tuple[Any, ...]] = []

    def flush_batch() -> tuple[int, int]:
        nonlocal batch
        if not batch:
            return 0, 0
        before_changes = connection.total_changes
        connection.executemany(
            """
            INSERT OR IGNORE INTO dict_entries (
                source_name,
                term,
                term_norm,
                definition,
                line_no,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        after_changes = connection.total_changes
        inserted = max(0, after_changes - before_changes)
        duplicates = max(0, len(batch) - inserted)
        batch = []
        return inserted, duplicates

    try:
        if clear_existing:
            connection.execute(
                "DELETE FROM dict_entries WHERE source_name = ?",
                (source_name,),
            )
            connection.commit()

        with dictionary_path.open("r", encoding=encoding, errors="strict", newline=None) as file:
            for line_no, raw_line in enumerate(file, start=1):
                total_lines += 1
                if max_lines_value is not None and total_lines > max_lines_value:
                    break
                parsed = parse_eijiro_line(raw_line, line_no)
                if parsed is None:
                    skipped_lines += 1
                    continue
                parsed_entries += 1
                batch.append(
                    (
                        source_name,
                        parsed["term"],
                        parsed["term_norm"],
                        parsed["definition"],
                        int(parsed["line_no"]),
                        now_iso,
                    )
                )
                if len(batch) >= DICT_INDEX_BATCH_SIZE:
                    inserted, duplicates = flush_batch()
                    inserted_entries += inserted
                    duplicate_entries += duplicates
                    connection.commit()

        inserted, duplicates = flush_batch()
        inserted_entries += inserted
        duplicate_entries += duplicates

        fts_rebuilt = rebuild_dictionary_fts(connection)
        connection.commit()
    finally:
        connection.close()

    print(
        f"[dict-index] source={source_name} path={dictionary_path} encoding={encoding} "
        f"lines={total_lines} parsed={parsed_entries} inserted={inserted_entries} "
        f"duplicates={duplicate_entries} skipped={skipped_lines}"
    )
    if max_lines_value is not None:
        print(f"[dict-index] max_lines applied: {max_lines_value}")
    print(f"[dict-index] fts_rebuilt={fts_rebuilt} db={db_path}")


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


def clamp_int(
    raw_value: str | None,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    if raw_value in (None, ""):
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def parse_bool_flag(raw_value: str | None, default: bool = False) -> bool:
    if raw_value in (None, ""):
        return default
    normalized = str(raw_value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def encode_path_token(path: Path) -> str:
    payload = str(path).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_path_token(token: str) -> Path | None:
    if not token:
        return None
    padded = token + ("=" * ((4 - len(token) % 4) % 4))
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        return Path(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def parse_subtitle_timestamp_ms(raw_value: str) -> int | None:
    raw = raw_value.strip().replace(",", ".")
    if not raw:
        return None

    parts = raw.split(":")
    if len(parts) == 2:
        hours_str = "0"
        minutes_str, seconds_str = parts
    elif len(parts) == 3:
        hours_str, minutes_str, seconds_str = parts
    else:
        return None

    try:
        hours = int(hours_str)
        minutes = int(minutes_str)
        seconds = float(seconds_str)
    except ValueError:
        return None

    total_ms = int(round(((hours * 3600) + (minutes * 60) + seconds) * 1000))
    return max(0, total_ms)


def parse_subtitle_cues(subtitle_path: Path) -> list[dict[str, Any]]:
    try:
        raw_text = subtitle_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []

    lines = raw_text.replace("\ufeff", "").splitlines()
    cues: list[dict[str, Any]] = []
    index = 0

    while index < len(lines):
        line = lines[index].strip()
        if not line or line.upper() == "WEBVTT":
            index += 1
            continue

        if line.isdigit() and index + 1 < len(lines) and "-->" in lines[index + 1]:
            index += 1
            line = lines[index].strip()

        if "-->" not in line:
            index += 1
            continue

        start_raw, end_raw = [part.strip() for part in line.split("-->", 1)]
        start_token = start_raw.split()[0] if start_raw else ""
        end_token = end_raw.split()[0] if end_raw else ""
        start_ms = parse_subtitle_timestamp_ms(start_token)
        end_ms = parse_subtitle_timestamp_ms(end_token)
        index += 1

        cue_lines: list[str] = []
        while index < len(lines):
            cue_line = lines[index].strip()
            if not cue_line:
                index += 1
                break
            cue_lines.append(cue_line)
            index += 1

        if start_ms is None or end_ms is None:
            continue
        if end_ms < start_ms:
            start_ms, end_ms = end_ms, start_ms

        cue_text = re.sub(r"<[^>]+>", "", " ".join(cue_lines)).strip()
        if not cue_text:
            continue
        cues.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "text": cue_text,
            }
        )

    return cues


def format_ms_to_clock(total_ms: int) -> str:
    safe_ms = max(0, int(total_ms))
    total_seconds = safe_ms // 1000
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def collect_video_tracks(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
) -> list[dict[str, str]]:
    tracks: list[dict[str, str]] = []
    seen_paths: set[str] = set()

    asr_row = connection.execute(
        """
        SELECT output_path
        FROM asr_runs
        WHERE source_id = ?
          AND video_id = ?
          AND status = 'success'
          AND output_path IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (source_id, video_id),
    ).fetchone()
    if asr_row is not None:
        asr_path = Path(str(asr_row[0]))
        asr_key = str(asr_path)
        if asr_path.exists() and asr_path.is_file() and asr_key not in seen_paths:
            seen_paths.add(asr_key)
            tracks.append(
                {
                    "track_id": f"asr:{encode_path_token(asr_path)}",
                    "kind": "asr",
                    "label": "ASR",
                    "path": asr_key,
                }
            )

    subtitle_rows = connection.execute(
        """
        SELECT language, subtitle_path, ext
        FROM subtitles
        WHERE source_id = ?
          AND video_id = ?
        ORDER BY language COLLATE NOCASE ASC, subtitle_path ASC
        """,
        (source_id, video_id),
    ).fetchall()
    for language, subtitle_path_value, ext in subtitle_rows:
        subtitle_path = Path(str(subtitle_path_value))
        subtitle_key = str(subtitle_path)
        if not subtitle_path.exists() or not subtitle_path.is_file():
            continue
        if subtitle_key in seen_paths:
            continue
        seen_paths.add(subtitle_key)
        language_label = str(language).strip() if language not in (None, "") else ""
        ext_label = str(ext).upper() if ext not in (None, "") else subtitle_path.suffix.lstrip(".").upper()
        label = language_label or f"TikTok ({ext_label or 'SUB'})"
        tracks.append(
            {
                "track_id": f"subtitle:{encode_path_token(subtitle_path)}",
                "kind": "subtitle",
                "label": label,
                "path": subtitle_key,
            }
        )

    return tracks


def get_track_for_video(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
    track_id: str | None,
) -> dict[str, str] | None:
    tracks = collect_video_tracks(connection, source_id, video_id)
    if not tracks:
        return None
    if not track_id:
        return tracks[0]
    for track in tracks:
        if track["track_id"] == track_id:
            return track
    return None


def serialize_bookmark_row(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        source_id = str(row["source_id"])
        video_id = str(row["video_id"])
        bookmark_id = int(row["id"])
        track = row["track"]
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        text_value = row["text"]
        note_value = row["note"]
        created_at = str(row["created_at"])
    else:
        (
            bookmark_id,
            source_id,
            video_id,
            track,
            start_ms,
            end_ms,
            text_value,
            note_value,
            created_at,
        ) = row

    return {
        "id": int(bookmark_id),
        "source_id": str(source_id),
        "video_id": str(video_id),
        "track": None if track in (None, "") else str(track),
        "start_ms": int(start_ms),
        "end_ms": int(end_ms),
        "start_label": format_ms_to_clock(int(start_ms)),
        "end_label": format_ms_to_clock(int(end_ms)),
        "text": "" if text_value in (None, "") else str(text_value),
        "note": "" if note_value in (None, "") else str(note_value),
        "created_at": str(created_at),
    }


def serialize_dictionary_bookmark_row(
    row: sqlite3.Row | tuple[Any, ...],
) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        bookmark_id = int(row["id"])
        source_id = str(row["source_id"])
        video_id = str(row["video_id"])
        track = str(row["track"] or "")
        cue_start_ms = int(row["cue_start_ms"])
        cue_end_ms = int(row["cue_end_ms"])
        cue_text = row["cue_text"]
        dict_entry_id = int(row["dict_entry_id"])
        dict_source_name = row["dict_source_name"]
        lookup_term = row["lookup_term"]
        term = str(row["term"])
        term_norm = str(row["term_norm"])
        definition = str(row["definition"])
        missing_entry = int(row["missing_entry"])
        lookup_path_json = row["lookup_path_json"]
        lookup_path_label = row["lookup_path_label"]
        created_at = str(row["created_at"])
        updated_at = str(row["updated_at"])
    else:
        (
            bookmark_id,
            source_id,
            video_id,
            track,
            cue_start_ms,
            cue_end_ms,
            cue_text,
            dict_entry_id,
            dict_source_name,
            lookup_term,
            term,
            term_norm,
            definition,
            missing_entry,
            lookup_path_json,
            lookup_path_label,
            created_at,
            updated_at,
        ) = row

    path_text = "" if lookup_path_json in (None, "") else str(lookup_path_json)
    lookup_path: list[dict[str, Any]] = []
    if path_text:
        try:
            parsed_path = json.loads(path_text)
        except json.JSONDecodeError:
            parsed_path = []
        if isinstance(parsed_path, list):
            lookup_path = normalize_dictionary_lookup_path(parsed_path)
    path_label = "" if lookup_path_label in (None, "") else str(lookup_path_label)
    if not path_label and lookup_path:
        path_label = build_dictionary_lookup_path_label(lookup_path)

    return {
        "id": int(bookmark_id),
        "source_id": str(source_id),
        "video_id": str(video_id),
        "track": str(track or ""),
        "cue_start_ms": int(cue_start_ms),
        "cue_end_ms": int(cue_end_ms),
        "cue_text": "" if cue_text in (None, "") else str(cue_text),
        "dict_entry_id": int(dict_entry_id),
        "dict_source_name": "" if dict_source_name in (None, "") else str(dict_source_name),
        "lookup_term": "" if lookup_term in (None, "") else str(lookup_term),
        "term": str(term),
        "term_norm": str(term_norm),
        "definition": str(definition),
        "missing_entry": bool(int(missing_entry)),
        "lookup_path": lookup_path,
        "lookup_path_label": path_label,
        "created_at": str(created_at),
        "updated_at": str(updated_at),
    }


def build_web_handler(
    db_path: Path,
    static_dir: Path,
    allowed_source_ids: set[str],
) -> type[BaseHTTPRequestHandler]:
    static_root = static_dir.resolve()

    class SubstudyWebHandler(BaseHTTPRequestHandler):
        server_version = "SubstudyWeb/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write(
                f"[web] {self.address_string()} - {fmt % args}\n"
            )

        def _open_connection(self) -> sqlite3.Connection:
            connection = sqlite3.connect(str(db_path))
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            return connection

        def _read_json_body(self) -> dict[str, Any]:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                content_length = int(raw_length)
            except ValueError:
                content_length = 0
            if content_length <= 0:
                return {}
            raw_body = self.rfile.read(content_length)
            if not raw_body:
                return {}
            try:
                parsed = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid JSON body: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("JSON body must be an object.")
            return parsed

        def _send_json(self, payload: Any, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error_json(self, status: int, message: str) -> None:
            self._send_json(
                {
                    "error": message,
                },
                status=status,
            )

        def _serve_static_file(self, relative_path: str) -> None:
            target_path = (static_root / relative_path).resolve()
            if not str(target_path).startswith(str(static_root)):
                self._send_error_json(403, "Access denied.")
                return
            if not target_path.exists() or not target_path.is_file():
                self._send_error_json(404, "File not found.")
                return
            try:
                payload = target_path.read_bytes()
            except OSError:
                self._send_error_json(500, "Failed to read static file.")
                return

            content_type = mimetypes.guess_type(str(target_path))[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8" if content_type.startswith("text/") else content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _serve_media_file(self, token: str) -> None:
            media_path = decode_path_token(token)
            if media_path is None or not media_path.exists() or not media_path.is_file():
                self._send_error_json(404, "Media file not found.")
                return

            try:
                stat = media_path.stat()
            except OSError:
                self._send_error_json(500, "Failed to inspect media file.")
                return

            file_size = stat.st_size
            start = 0
            end = file_size - 1
            status_code = 200

            range_header = self.headers.get("Range", "").strip()
            if range_header:
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header)
                if not match:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{file_size}")
                    self.end_headers()
                    return
                start_token, end_token = match.groups()
                try:
                    if start_token and end_token:
                        start = int(start_token)
                        end = int(end_token)
                    elif start_token:
                        start = int(start_token)
                        end = file_size - 1
                    elif end_token:
                        suffix_size = int(end_token)
                        start = max(0, file_size - suffix_size)
                        end = file_size - 1
                except ValueError:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{file_size}")
                    self.end_headers()
                    return

                if start < 0 or end < start or start >= file_size:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{file_size}")
                    self.end_headers()
                    return
                end = min(end, file_size - 1)
                status_code = 206

            content_length = (end - start) + 1
            content_type = mimetypes.guess_type(str(media_path))[0] or "application/octet-stream"

            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(content_length))
            if status_code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.end_headers()

            try:
                with media_path.open("rb") as file:
                    file.seek(start)
                    remaining = content_length
                    while remaining > 0:
                        chunk = file.read(min(1024 * 128, remaining))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        remaining -= len(chunk)
            except OSError:
                return

        def _is_source_allowed(self, source_id: str) -> bool:
            if not allowed_source_ids:
                return True
            return source_id in allowed_source_ids

        def _normalize_source(self, source_id: Any) -> str | None:
            if source_id in (None, ""):
                return None
            normalized = str(source_id).strip()
            if not normalized:
                return None
            return normalized

        def _fetch_bookmark_by_id(
            self,
            connection: sqlite3.Connection,
            bookmark_id: int,
        ) -> sqlite3.Row | None:
            return connection.execute(
                """
                SELECT
                    id,
                    source_id,
                    video_id,
                    track,
                    start_ms,
                    end_ms,
                    text,
                    note,
                    created_at
                FROM subtitle_bookmarks
                WHERE id = ?
                """,
                (bookmark_id,),
            ).fetchone()

        def _fetch_dictionary_bookmark_by_composite(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
            track: str,
            cue_start_ms: int,
            cue_end_ms: int,
            dict_entry_id: int,
        ) -> sqlite3.Row | None:
            return connection.execute(
                """
                SELECT
                    id,
                    source_id,
                    video_id,
                    track,
                    cue_start_ms,
                    cue_end_ms,
                    cue_text,
                    dict_entry_id,
                    dict_source_name,
                    lookup_term,
                    term,
                    term_norm,
                    definition,
                    missing_entry,
                    lookup_path_json,
                    lookup_path_label,
                    created_at,
                    updated_at
                FROM dictionary_bookmarks
                WHERE source_id = ?
                  AND video_id = ?
                  AND track = ?
                  AND cue_start_ms = ?
                  AND cue_end_ms = ?
                  AND dict_entry_id = ?
                LIMIT 1
                """,
                (
                    source_id,
                    video_id,
                    track,
                    cue_start_ms,
                    cue_end_ms,
                    dict_entry_id,
                ),
            ).fetchone()

        def _handle_api_feed(self, query: dict[str, list[str]]) -> None:
            source_filter = self._normalize_source(query.get("source_id", [None])[0])
            if source_filter and not self._is_source_allowed(source_filter):
                self._send_error_json(403, "Source is not allowed.")
                return
            translation_filter = str(query.get("translation_filter", ["all"])[0] or "all").strip().lower()
            if translation_filter not in {"all", "ja_only", "ja_missing"}:
                translation_filter = "all"

            limit = clamp_int(query.get("limit", [None])[0], default=180, minimum=1, maximum=1000)
            offset = clamp_int(query.get("offset", [None])[0], default=0, minimum=0, maximum=20000)

            def ja_subtitle_exists_clause(video_alias: str) -> str:
                return f"""
                EXISTS (
                    SELECT 1
                    FROM subtitles sja
                    WHERE sja.source_id = {video_alias}.source_id
                      AND sja.video_id = {video_alias}.video_id
                      AND (
                        LOWER(COALESCE(sja.language, '')) = 'ja'
                        OR LOWER(COALESCE(sja.language, '')) LIKE 'ja-%'
                        OR LOWER(COALESCE(sja.subtitle_path, '')) LIKE '%.ja.%'
                      )
                )
                """

            where_clauses = [
                "v.has_media = 1",
                "v.media_path IS NOT NULL",
            ]
            params: list[Any] = []
            if allowed_source_ids:
                placeholders = ",".join("?" for _ in sorted(allowed_source_ids))
                where_clauses.append(f"v.source_id IN ({placeholders})")
                params.extend(sorted(allowed_source_ids))
            if source_filter:
                where_clauses.append("v.source_id = ?")
                params.append(source_filter)
            if translation_filter == "ja_only":
                where_clauses.append(ja_subtitle_exists_clause("v"))
            elif translation_filter == "ja_missing":
                where_clauses.append(f"NOT ({ja_subtitle_exists_clause('v')})")

            with self._open_connection() as connection:
                rows = connection.execute(
                    f"""
                    SELECT
                        v.source_id,
                        v.video_id,
                        v.title,
                        v.description,
                        v.uploader,
                        v.upload_date,
                        v.duration,
                        v.webpage_url,
                        v.media_path,
                        COALESCE(vf.created_at, '') AS favorite_created_at,
                        CASE
                            WHEN vf.video_id IS NULL THEN 0
                            ELSE 1
                        END AS is_favorite,
                        COALESCE(vn.note, '') AS video_note,
                        v.audio_lufs,
                        v.audio_gain_db
                    FROM videos v
                    LEFT JOIN video_favorites vf
                      ON vf.source_id = v.source_id
                     AND vf.video_id = v.video_id
                    LEFT JOIN video_notes vn
                      ON vn.source_id = v.source_id
                     AND vn.video_id = v.video_id
                    WHERE {" AND ".join(where_clauses)}
                    ORDER BY
                        COALESCE(v.upload_date, '') DESC,
                        v.video_id DESC
                    LIMIT ?
                    OFFSET ?
                    """,
                    (*params, limit, offset),
                ).fetchall()

                source_where_clauses = [
                    "has_media = 1",
                    "media_path IS NOT NULL",
                ]
                source_params: list[Any] = []
                if allowed_source_ids:
                    placeholders = ",".join("?" for _ in sorted(allowed_source_ids))
                    source_where_clauses.append(f"source_id IN ({placeholders})")
                    source_params.extend(sorted(allowed_source_ids))
                if translation_filter == "ja_only":
                    source_where_clauses.append(ja_subtitle_exists_clause("videos"))
                elif translation_filter == "ja_missing":
                    source_where_clauses.append(f"NOT ({ja_subtitle_exists_clause('videos')})")
                source_rows = connection.execute(
                    f"""
                    SELECT DISTINCT source_id
                    FROM videos
                    WHERE {" AND ".join(source_where_clauses)}
                    ORDER BY source_id ASC
                    """,
                    tuple(source_params),
                ).fetchall()
                source_ids = [str(row["source_id"]) for row in source_rows]

                videos: list[dict[str, Any]] = []
                for row in rows:
                    media_path_value = row["media_path"]
                    if media_path_value in (None, ""):
                        continue
                    media_path = Path(str(media_path_value))
                    if not media_path.exists() or not media_path.is_file():
                        continue

                    source_id = str(row["source_id"])
                    video_id = str(row["video_id"])
                    tracks = collect_video_tracks(connection, source_id, video_id)
                    public_tracks = [
                        {
                            "track_id": track["track_id"],
                            "kind": track["kind"],
                            "label": track["label"],
                        }
                        for track in tracks
                    ]
                    has_ja_track = any(
                        (
                            str(track["kind"]).strip().lower() == "subtitle"
                            and (
                                str(track["label"]).strip().lower() == "ja"
                                or str(track["label"]).strip().lower().startswith("ja-")
                            )
                        )
                        for track in public_tracks
                    )
                    if translation_filter == "ja_only" and not has_ja_track:
                        continue
                    if translation_filter == "ja_missing" and has_ja_track:
                        continue
                    videos.append(
                        {
                            "source_id": source_id,
                            "video_id": video_id,
                            "title": "" if row["title"] in (None, "") else str(row["title"]),
                            "description": (
                                ""
                                if row["description"] in (None, "")
                                else str(row["description"])
                            ),
                            "uploader": "" if row["uploader"] in (None, "") else str(row["uploader"]),
                            "upload_date": "" if row["upload_date"] in (None, "") else str(row["upload_date"]),
                            "duration": safe_float(row["duration"]),
                            "webpage_url": (
                                ""
                                if row["webpage_url"] in (None, "")
                                else str(row["webpage_url"])
                            ),
                            "media_url": f"/media/{encode_path_token(media_path)}",
                            "is_favorite": bool(row["is_favorite"]),
                            "favorite_created_at": (
                                ""
                                if row["favorite_created_at"] in (None, "")
                                else str(row["favorite_created_at"])
                            ),
                            "note": "" if row["video_note"] in (None, "") else str(row["video_note"]),
                            "audio_lufs": safe_float(row["audio_lufs"]),
                            "audio_gain_db": safe_float(row["audio_gain_db"]),
                            "tracks": public_tracks,
                            "default_track": public_tracks[0]["track_id"] if public_tracks else None,
                        }
                    )

            self._send_json(
                {
                    "videos": videos,
                    "count": len(videos),
                    "sources": source_ids,
                    "translation_filter": translation_filter,
                }
            )

        def _handle_api_subtitles(self, query: dict[str, list[str]]) -> None:
            source_id = self._normalize_source(query.get("source_id", [None])[0])
            video_id = self._normalize_source(query.get("video_id", [None])[0])
            track_id = self._normalize_source(query.get("track", [None])[0])

            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return

            with self._open_connection() as connection:
                track = get_track_for_video(connection, source_id, video_id, track_id)
                if track is None:
                    self._send_json(
                        {
                            "source_id": source_id,
                            "video_id": video_id,
                            "track_id": None,
                            "label": "",
                            "kind": "",
                            "cues": [],
                        }
                    )
                    return

                track_path = Path(track["path"])
                cues = parse_subtitle_cues(track_path)
                self._send_json(
                    {
                        "source_id": source_id,
                        "video_id": video_id,
                        "track_id": track["track_id"],
                        "label": track["label"],
                        "kind": track["kind"],
                        "cues": cues,
                    }
                )

        def _handle_api_dictionary_lookup(self, query: dict[str, list[str]]) -> None:
            raw_term = query.get("term", [None])[0]
            if raw_term in (None, ""):
                self._send_error_json(400, "term is required.")
                return
            term = str(raw_term).strip()
            if not term:
                self._send_error_json(400, "term is required.")
                return
            limit = clamp_int(
                query.get("limit", [None])[0],
                default=DEFAULT_DICT_LOOKUP_LIMIT,
                minimum=1,
                maximum=20,
            )
            exact_only = parse_bool_flag(query.get("exact_only", [None])[0], default=False)
            fts_mode = str(query.get("fts_mode", ["all"])[0] or "all")

            with self._open_connection() as connection:
                payload = lookup_dictionary_entries(
                    connection,
                    term,
                    limit=limit,
                    exact_only=exact_only,
                    fts_mode=fts_mode,
                )
            self._send_json(payload)

        def _handle_api_dictionary_lookup_batch(self, query: dict[str, list[str]]) -> None:
            raw_terms = query.get("term", [])
            if not raw_terms:
                self._send_error_json(400, "term is required.")
                return
            cleaned_terms: list[str] = []
            seen_terms: set[str] = set()
            for raw_term in raw_terms:
                value = str(raw_term).strip()
                if not value:
                    continue
                if value in seen_terms:
                    continue
                seen_terms.add(value)
                cleaned_terms.append(value)
                if len(cleaned_terms) >= 24:
                    break
            if not cleaned_terms:
                self._send_error_json(400, "term is required.")
                return

            limit = clamp_int(
                query.get("limit", [None])[0],
                default=DEFAULT_DICT_LOOKUP_LIMIT,
                minimum=1,
                maximum=20,
            )
            exact_only = parse_bool_flag(query.get("exact_only", [None])[0], default=False)
            fts_mode = str(query.get("fts_mode", ["all"])[0] or "all")

            items: list[dict[str, Any]] = []
            with self._open_connection() as connection:
                for term in cleaned_terms:
                    items.append(
                        lookup_dictionary_entries(
                            connection,
                            term,
                            limit=limit,
                            exact_only=exact_only,
                            fts_mode=fts_mode,
                        )
                    )
            self._send_json({"items": items})

        def _handle_api_bookmarks_get(self, query: dict[str, list[str]]) -> None:
            source_id = self._normalize_source(query.get("source_id", [None])[0])
            video_id = self._normalize_source(query.get("video_id", [None])[0])
            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return
            limit = clamp_int(query.get("limit", [None])[0], default=200, minimum=1, maximum=1000)

            with self._open_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT
                        id,
                        source_id,
                        video_id,
                        track,
                        start_ms,
                        end_ms,
                        text,
                        note,
                        created_at
                    FROM subtitle_bookmarks
                    WHERE source_id = ?
                      AND video_id = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (source_id, video_id, limit),
                ).fetchall()
            bookmarks = [serialize_bookmark_row(row) for row in rows]
            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "bookmarks": bookmarks,
                }
            )

        def _handle_api_dictionary_bookmarks_get(self, query: dict[str, list[str]]) -> None:
            source_id = self._normalize_source(query.get("source_id", [None])[0])
            video_id = self._normalize_source(query.get("video_id", [None])[0])
            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return

            track_filter = self._normalize_source(query.get("track", [None])[0])
            if track_filter is None:
                track_filter = ""
            limit = clamp_int(query.get("limit", [None])[0], default=1200, minimum=1, maximum=5000)

            where_clauses = [
                "source_id = ?",
                "video_id = ?",
            ]
            params: list[Any] = [source_id, video_id]
            if track_filter:
                where_clauses.append("track = ?")
                params.append(track_filter)
            params.append(limit)

            with self._open_connection() as connection:
                rows = connection.execute(
                    f"""
                    SELECT
                        id,
                        source_id,
                        video_id,
                        track,
                        cue_start_ms,
                        cue_end_ms,
                        cue_text,
                        dict_entry_id,
                        dict_source_name,
                        lookup_term,
                        term,
                        term_norm,
                        definition,
                        missing_entry,
                        lookup_path_json,
                        lookup_path_label,
                        created_at,
                        updated_at
                    FROM dictionary_bookmarks
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY updated_at DESC, id DESC
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
            bookmarks = [serialize_dictionary_bookmark_row(row) for row in rows]
            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "track": track_filter,
                    "bookmarks": bookmarks,
                }
            )

        def _handle_api_toggle_dictionary_bookmark(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return

            source_id = self._normalize_source(payload.get("source_id"))
            video_id = self._normalize_source(payload.get("video_id"))
            track = self._normalize_source(payload.get("track")) or ""
            cue_text = "" if payload.get("cue_text") in (None, "") else str(payload.get("cue_text"))
            lookup_term = "" if payload.get("lookup_term") in (None, "") else str(payload.get("lookup_term"))
            term = "" if payload.get("term") in (None, "") else str(payload.get("term")).strip()
            term_norm_value = "" if payload.get("term_norm") in (None, "") else str(payload.get("term_norm"))
            term_norm = normalize_dictionary_term(term_norm_value or term)
            definition = "" if payload.get("definition") in (None, "") else str(payload.get("definition")).strip()
            dict_source_name = (
                "" if payload.get("dict_source_name") in (None, "") else str(payload.get("dict_source_name")).strip()
            )
            lookup_path = normalize_dictionary_lookup_path(payload.get("lookup_path"))
            lookup_path_label = (
                "" if payload.get("lookup_path_label") in (None, "") else str(payload.get("lookup_path_label")).strip()
            )
            missing_entry_raw = payload.get("missing_entry")
            if isinstance(missing_entry_raw, str):
                missing_entry = missing_entry_raw.strip().lower() in {"1", "true", "yes", "on"}
            else:
                missing_entry = bool(missing_entry_raw)

            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return

            if not term:
                self._send_error_json(400, "term is required.")
                return
            if not term_norm:
                self._send_error_json(400, "term_norm is required.")
                return
            if missing_entry:
                if not definition:
                    definition = "辞書エントリが見つかりません。"
                if not lookup_term:
                    lookup_term = term
            if not definition:
                self._send_error_json(400, "definition is required.")
                return

            try:
                cue_start_ms = int(payload.get("cue_start_ms"))
                cue_end_ms = int(payload.get("cue_end_ms"))
            except (TypeError, ValueError):
                self._send_error_json(
                    400,
                    "cue_start_ms and cue_end_ms must be integers.",
                )
                return
            if missing_entry:
                dict_entry_id = make_missing_dict_entry_id(term_norm)
            else:
                try:
                    dict_entry_id = int(payload.get("dict_entry_id"))
                except (TypeError, ValueError):
                    self._send_error_json(400, "dict_entry_id must be an integer.")
                    return
                if dict_entry_id <= 0:
                    self._send_error_json(400, "dict_entry_id must be a positive integer.")
                    return
            if cue_end_ms < cue_start_ms:
                cue_start_ms, cue_end_ms = cue_end_ms, cue_start_ms
            cue_start_ms = max(0, cue_start_ms)
            cue_end_ms = max(cue_start_ms, cue_end_ms)
            if not lookup_path:
                base_term = lookup_term or term
                base_norm = normalize_dictionary_term(base_term)
                if base_term or base_norm:
                    lookup_path = [
                        {
                            "level": 1,
                            "term": base_term or base_norm,
                            "term_norm": base_norm,
                            "source": "dictionary",
                        }
                    ]
            if not lookup_path_label:
                lookup_path_label = build_dictionary_lookup_path_label(lookup_path)
            lookup_path_json = ""
            if lookup_path:
                lookup_path_json = json.dumps(
                    lookup_path,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

            with self._open_connection() as connection:
                if not self._validate_video_exists(connection, source_id, video_id):
                    self._send_error_json(404, "Video not found.")
                    return
                if track:
                    valid_track = get_track_for_video(connection, source_id, video_id, track)
                    if valid_track is None:
                        self._send_error_json(400, "track is invalid for this video.")
                        return

                existing = self._fetch_dictionary_bookmark_by_composite(
                    connection,
                    source_id,
                    video_id,
                    track,
                    cue_start_ms,
                    cue_end_ms,
                    dict_entry_id,
                )
                if existing is None:
                    now_iso = now_utc_iso()
                    cursor = connection.execute(
                        """
                        INSERT INTO dictionary_bookmarks (
                            source_id,
                            video_id,
                            track,
                            cue_start_ms,
                            cue_end_ms,
                            cue_text,
                            dict_entry_id,
                            dict_source_name,
                            lookup_term,
                            term,
                            term_norm,
                            definition,
                            missing_entry,
                            lookup_path_json,
                            lookup_path_label,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_id,
                            video_id,
                            track,
                            cue_start_ms,
                            cue_end_ms,
                            cue_text,
                            dict_entry_id,
                            dict_source_name,
                            lookup_term,
                            term,
                            term_norm,
                            definition,
                            int(missing_entry),
                            lookup_path_json,
                            lookup_path_label,
                            now_iso,
                            now_iso,
                        ),
                    )
                    bookmark_id = cursor.lastrowid
                    if bookmark_id is None:
                        self._send_error_json(500, "Failed to create dictionary bookmark.")
                        return
                    connection.commit()
                    bookmark = connection.execute(
                        """
                        SELECT
                            id,
                            source_id,
                            video_id,
                            track,
                            cue_start_ms,
                            cue_end_ms,
                            cue_text,
                            dict_entry_id,
                            dict_source_name,
                            lookup_term,
                            term,
                            term_norm,
                            definition,
                            missing_entry,
                            lookup_path_json,
                            lookup_path_label,
                            created_at,
                            updated_at
                        FROM dictionary_bookmarks
                        WHERE id = ?
                        """,
                        (int(bookmark_id),),
                    ).fetchone()
                    if bookmark is None:
                        self._send_error_json(500, "Failed to read dictionary bookmark.")
                        return
                    self._send_json(
                        {
                            "status": "saved",
                            "bookmark": serialize_dictionary_bookmark_row(bookmark),
                        }
                    )
                    return

                connection.execute(
                    "DELETE FROM dictionary_bookmarks WHERE id = ?",
                    (int(existing["id"]),),
                )
                connection.commit()
                self._send_json(
                    {
                        "status": "removed",
                        "bookmark": serialize_dictionary_bookmark_row(existing),
                    }
                )

        def _validate_video_exists(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
        ) -> bool:
            row = connection.execute(
                """
                SELECT 1
                FROM videos
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            return row is not None

        def _handle_api_toggle_favorite(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return
            source_id = self._normalize_source(payload.get("source_id"))
            video_id = self._normalize_source(payload.get("video_id"))
            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return

            with self._open_connection() as connection:
                if not self._validate_video_exists(connection, source_id, video_id):
                    self._send_error_json(404, "Video not found.")
                    return

                existing = connection.execute(
                    """
                    SELECT 1
                    FROM video_favorites
                    WHERE source_id = ?
                      AND video_id = ?
                    LIMIT 1
                    """,
                    (source_id, video_id),
                ).fetchone()
                if existing is None:
                    created_at = now_utc_iso()
                    connection.execute(
                        """
                        INSERT INTO video_favorites(source_id, video_id, created_at)
                        VALUES (?, ?, ?)
                        """,
                        (source_id, video_id, created_at),
                    )
                    favorited = True
                else:
                    connection.execute(
                        """
                        DELETE FROM video_favorites
                        WHERE source_id = ?
                          AND video_id = ?
                        """,
                        (source_id, video_id),
                    )
                    favorited = False
                    created_at = ""
                connection.commit()

            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "is_favorite": favorited,
                    "created_at": created_at,
                }
            )

        def _handle_api_upsert_video_note(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return
            source_id = self._normalize_source(payload.get("source_id"))
            video_id = self._normalize_source(payload.get("video_id"))
            note = "" if payload.get("note") in (None, "") else str(payload.get("note"))

            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return

            with self._open_connection() as connection:
                if not self._validate_video_exists(connection, source_id, video_id):
                    self._send_error_json(404, "Video not found.")
                    return

                now_iso = now_utc_iso()
                if note.strip():
                    connection.execute(
                        """
                        INSERT INTO video_notes (
                            source_id,
                            video_id,
                            note,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(source_id, video_id) DO UPDATE SET
                            note = excluded.note,
                            updated_at = excluded.updated_at
                        """,
                        (source_id, video_id, note, now_iso, now_iso),
                    )
                else:
                    connection.execute(
                        """
                        DELETE FROM video_notes
                        WHERE source_id = ?
                          AND video_id = ?
                        """,
                        (source_id, video_id),
                    )
                connection.commit()

                row = connection.execute(
                    """
                    SELECT note, created_at, updated_at
                    FROM video_notes
                    WHERE source_id = ?
                      AND video_id = ?
                    """,
                    (source_id, video_id),
                ).fetchone()
            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "note": "" if row is None else str(row["note"]),
                    "created_at": "" if row is None else str(row["created_at"]),
                    "updated_at": "" if row is None else str(row["updated_at"]),
                }
            )

        def _handle_api_create_bookmark(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return
            source_id = self._normalize_source(payload.get("source_id"))
            video_id = self._normalize_source(payload.get("video_id"))
            track = self._normalize_source(payload.get("track"))
            text_value = "" if payload.get("text") in (None, "") else str(payload.get("text"))
            note_value = "" if payload.get("note") in (None, "") else str(payload.get("note"))

            if source_id is None or video_id is None:
                self._send_error_json(400, "source_id and video_id are required.")
                return
            if not self._is_source_allowed(source_id):
                self._send_error_json(403, "Source is not allowed.")
                return
            try:
                start_ms = int(payload.get("start_ms"))
                end_ms = int(payload.get("end_ms"))
            except (TypeError, ValueError):
                self._send_error_json(400, "start_ms and end_ms must be integers.")
                return
            if end_ms < start_ms:
                start_ms, end_ms = end_ms, start_ms
            start_ms = max(0, start_ms)
            end_ms = max(start_ms, end_ms)

            with self._open_connection() as connection:
                if not self._validate_video_exists(connection, source_id, video_id):
                    self._send_error_json(404, "Video not found.")
                    return
                if track:
                    valid_track = get_track_for_video(connection, source_id, video_id, track)
                    if valid_track is None:
                        self._send_error_json(400, "track is invalid for this video.")
                        return

                created_at = now_utc_iso()
                cursor = connection.execute(
                    """
                    INSERT INTO subtitle_bookmarks (
                        source_id,
                        video_id,
                        track,
                        start_ms,
                        end_ms,
                        text,
                        note,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        video_id,
                        track,
                        start_ms,
                        end_ms,
                        text_value,
                        note_value,
                        created_at,
                    ),
                )
                bookmark_id = cursor.lastrowid
                if bookmark_id is None:
                    self._send_error_json(500, "Failed to create bookmark.")
                    return
                connection.commit()
                row = self._fetch_bookmark_by_id(connection, int(bookmark_id))
            if row is None:
                self._send_error_json(500, "Failed to read created bookmark.")
                return
            self._send_json(
                {
                    "bookmark": serialize_bookmark_row(row),
                },
                status=201,
            )

        def _handle_api_update_bookmark_note(self, bookmark_id: int) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return
            note_value = "" if payload.get("note") in (None, "") else str(payload.get("note"))
            with self._open_connection() as connection:
                existing = self._fetch_bookmark_by_id(connection, bookmark_id)
                if existing is None:
                    self._send_error_json(404, "Bookmark not found.")
                    return
                source_id = str(existing["source_id"])
                if not self._is_source_allowed(source_id):
                    self._send_error_json(403, "Source is not allowed.")
                    return
                connection.execute(
                    """
                    UPDATE subtitle_bookmarks
                    SET note = ?
                    WHERE id = ?
                    """,
                    (note_value, bookmark_id),
                )
                connection.commit()
                updated = self._fetch_bookmark_by_id(connection, bookmark_id)
            if updated is None:
                self._send_error_json(500, "Failed to update bookmark.")
                return
            self._send_json(
                {
                    "bookmark": serialize_bookmark_row(updated),
                }
            )

        def _handle_api_delete_bookmark(self, bookmark_id: int) -> None:
            with self._open_connection() as connection:
                existing = self._fetch_bookmark_by_id(connection, bookmark_id)
                if existing is None:
                    self._send_error_json(404, "Bookmark not found.")
                    return
                source_id = str(existing["source_id"])
                if not self._is_source_allowed(source_id):
                    self._send_error_json(403, "Source is not allowed.")
                    return

                connection.execute(
                    "DELETE FROM subtitle_bookmarks WHERE id = ?",
                    (bookmark_id,),
                )
                connection.commit()
            self._send_json(
                {
                    "deleted": True,
                    "id": bookmark_id,
                }
            )

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            try:
                if path in ("/", "/index.html"):
                    self._serve_static_file("index.html")
                    return
                if path == "/app.js":
                    self._serve_static_file("app.js")
                    return
                if path == "/styles.css":
                    self._serve_static_file("styles.css")
                    return
                if path.startswith("/vendor/"):
                    self._serve_static_file(path.lstrip("/"))
                    return
                if path.startswith("/media/"):
                    token = path[len("/media/") :]
                    self._serve_media_file(token)
                    return
                if path == "/api/feed":
                    self._handle_api_feed(query)
                    return
                if path == "/api/subtitles":
                    self._handle_api_subtitles(query)
                    return
                if path == "/api/dictionary":
                    self._handle_api_dictionary_lookup(query)
                    return
                if path == "/api/dictionary/batch":
                    self._handle_api_dictionary_lookup_batch(query)
                    return
                if path == "/api/bookmarks":
                    self._handle_api_bookmarks_get(query)
                    return
                if path == "/api/dictionary-bookmarks":
                    self._handle_api_dictionary_bookmarks_get(query)
                    return
                self._send_error_json(404, "Not found.")
            except BrokenPipeError:
                return
            except Exception as exc:  # pragma: no cover - defensive
                self._send_error_json(500, f"Unexpected server error: {exc}")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/api/favorites/toggle":
                    self._handle_api_toggle_favorite()
                    return
                if path == "/api/video-note":
                    self._handle_api_upsert_video_note()
                    return
                if path == "/api/bookmarks":
                    self._handle_api_create_bookmark()
                    return
                if path == "/api/dictionary-bookmarks/toggle":
                    self._handle_api_toggle_dictionary_bookmark()
                    return
                note_match = re.fullmatch(r"/api/bookmarks/(\d+)/note", path)
                if note_match:
                    self._handle_api_update_bookmark_note(int(note_match.group(1)))
                    return
                self._send_error_json(404, "Not found.")
            except BrokenPipeError:
                return
            except Exception as exc:  # pragma: no cover - defensive
                self._send_error_json(500, f"Unexpected server error: {exc}")

        def do_DELETE(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                match = re.fullmatch(r"/api/bookmarks/(\d+)", path)
                if match:
                    self._handle_api_delete_bookmark(int(match.group(1)))
                    return
                self._send_error_json(404, "Not found.")
            except BrokenPipeError:
                return
            except Exception as exc:  # pragma: no cover - defensive
                self._send_error_json(500, f"Unexpected server error: {exc}")

    return SubstudyWebHandler


def run_web_ui(
    db_path: Path,
    source_ids: list[str],
    host: str,
    port: int,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as bootstrap_connection:
        create_schema(bootstrap_connection)
        bootstrap_connection.commit()

    if not WEB_STATIC_DIR.exists():
        raise FileNotFoundError(f"Web static directory not found: {WEB_STATIC_DIR}")

    handler_cls = build_web_handler(
        db_path=db_path,
        static_dir=WEB_STATIC_DIR,
        allowed_source_ids=set(source_ids),
    )
    server = ThreadingHTTPServer((host, port), handler_cls)
    print(f"[web] serving on http://{host}:{port}")
    print(f"[web] sources: {', '.join(sorted(source_ids))}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[web] stopped")
    finally:
        server.server_close()


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

    loudness_parser = subparsers.add_parser(
        "loudness",
        help="Analyze per-video loudness and store normalization gain",
    )
    loudness_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    loudness_parser.add_argument("--source", action="append", dest="sources")
    loudness_parser.add_argument("--ledger-db", type=Path)
    loudness_parser.add_argument(
        "--target-lufs",
        type=float,
        default=DEFAULT_LOUDNESS_TARGET_LUFS,
        help="Target integrated loudness in LUFS (default: -16.0)",
    )
    loudness_parser.add_argument(
        "--max-boost-db",
        type=float,
        default=DEFAULT_LOUDNESS_MAX_BOOST_DB,
        help="Maximum positive gain per video (default: 6.0)",
    )
    loudness_parser.add_argument(
        "--max-cut-db",
        type=float,
        default=DEFAULT_LOUDNESS_MAX_CUT_DB,
        help="Maximum attenuation per video (default: 12.0)",
    )
    loudness_parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LOUDNESS_LIMIT,
        help="Maximum videos to analyze per source in one run (default: 300)",
    )
    loudness_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-analyze videos even when gain has already been computed.",
    )
    loudness_parser.add_argument(
        "--ffmpeg-bin",
        default=DEFAULT_LOUDNESS_FFMPEG_BIN,
        help="ffmpeg binary path/name (default: ffmpeg)",
    )

    dict_index_parser = subparsers.add_parser(
        "dict-index",
        help="Index EIJIRO dictionary entries into SQLite for hover lookup",
    )
    dict_index_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    dict_index_parser.add_argument("--source", action="append", dest="sources")
    dict_index_parser.add_argument("--ledger-db", type=Path)
    dict_index_parser.add_argument(
        "--dictionary-path",
        type=Path,
        default=DEFAULT_DICT_PATH,
        help="Dictionary file path (default: data/eijiro-1449.utf8.txt)",
    )
    dict_index_parser.add_argument(
        "--encoding",
        default=DEFAULT_DICT_ENCODING,
        help="Dictionary file encoding (default: utf-8)",
    )
    dict_index_parser.add_argument(
        "--source-name",
        default=DEFAULT_DICT_SOURCE_NAME,
        help="Logical dictionary source label stored in DB (default: eijiro-1449)",
    )
    dict_index_parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Append/update without deleting existing entries for the same source.",
    )
    dict_index_parser.add_argument(
        "--max-lines",
        type=int,
        default=0,
        help="Optional line cap for quick trial runs (0 = no cap).",
    )

    web_parser = subparsers.add_parser(
        "web",
        help="Run local TikTok-style study web UI",
    )
    web_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    web_parser.add_argument("--source", action="append", dest="sources")
    web_parser.add_argument("--ledger-db", type=Path)
    web_parser.add_argument("--host", default=DEFAULT_WEB_HOST)
    web_parser.add_argument("--port", type=int, default=DEFAULT_WEB_PORT)

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

    if args.command == "loudness":
        try:
            run_loudness(
                sources=sources,
                db_path=ledger_db_path,
                target_lufs=float(args.target_lufs),
                max_boost_db=max(0.0, float(args.max_boost_db)),
                max_cut_db=max(0.0, float(args.max_cut_db)),
                limit=max(1, int(args.limit)),
                force=bool(args.force),
                ffmpeg_bin=str(args.ffmpeg_bin),
            )
            return 0
        except KeyboardInterrupt:
            print(
                "[loudness] interrupted by user. "
                "Processed rows are already committed.",
                file=sys.stderr,
            )
            return 130

    if args.command == "dict-index":
        dictionary_path = resolve_output_path(
            args.dictionary_path,
            DEFAULT_DICT_PATH,
        )
        run_dict_index(
            db_path=ledger_db_path,
            dictionary_path=dictionary_path,
            source_name=str(args.source_name),
            encoding=str(args.encoding),
            clear_existing=not bool(args.no_clear),
            max_lines=None if int(args.max_lines) <= 0 else int(args.max_lines),
        )
        return 0

    if args.command == "web":
        run_web_ui(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            host=str(args.host),
            port=max(1, min(65535, int(args.port))),
        )
        return 0

    print("error: unsupported command", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
