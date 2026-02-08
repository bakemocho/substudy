#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shlex
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
    break_on_existing: bool
    break_per_input: bool
    lazy_playlist: bool
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
            break_on_existing=bool(
                source_raw.get("break_on_existing", global_raw.get("break_on_existing", False))
            ),
            break_per_input=bool(
                source_raw.get("break_per_input", global_raw.get("break_per_input", False))
            ),
            lazy_playlist=bool(
                source_raw.get("lazy_playlist", global_raw.get("lazy_playlist", False))
            ),
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


def run_command(command: list[str], dry_run: bool) -> None:
    print("$", shlex.join(command))
    if dry_run:
        return
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}")


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


def sync_source(
    source: SourceConfig,
    dry_run: bool,
    skip_media: bool,
    skip_subs: bool,
    skip_meta: bool,
) -> None:
    print(f"\n=== sync: {source.id} ===")

    source.media_dir.mkdir(parents=True, exist_ok=True)
    source.subs_dir.mkdir(parents=True, exist_ok=True)
    source.meta_dir.mkdir(parents=True, exist_ok=True)
    source.media_archive.parent.mkdir(parents=True, exist_ok=True)
    source.subs_archive.parent.mkdir(parents=True, exist_ok=True)

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
        run_command(media_command, dry_run=dry_run)
    else:
        print("skip media")

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
        run_command(subs_command, dry_run=dry_run)
    else:
        print("skip subtitles")

    if skip_meta:
        print("skip metadata")
        return

    media_archive_ids = read_archive_ids(source.media_archive)
    subs_archive_ids = read_archive_ids(source.subs_archive)
    archive_ids: list[str] = []
    seen_archive_ids: set[str] = set()
    for video_id in [*media_archive_ids, *subs_archive_ids]:
        if video_id in seen_archive_ids:
            continue
        archive_ids.append(video_id)
        seen_archive_ids.add(video_id)

    if not archive_ids:
        print(
            f"no archived IDs found in {source.media_archive} or {source.subs_archive}; "
            "skip metadata"
        )
        return

    existing_meta_ids = list_meta_ids(source.meta_dir)
    missing_ids = [video_id for video_id in archive_ids if video_id not in existing_meta_ids]
    if not missing_ids:
        print("metadata already up to date")
        return

    urls = [build_video_url(source, video_id) for video_id in missing_ids]
    if dry_run:
        print(f"metadata target IDs (dry-run): {len(missing_ids)}")
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
    run_command(metadata_command, dry_run=dry_run)
    if not dry_run:
        print(f"metadata target IDs: {len(missing_ids)}")


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

        CREATE INDEX IF NOT EXISTS idx_videos_upload_date ON videos(upload_date);
        CREATE INDEX IF NOT EXISTS idx_subtitles_source_video ON subtitles(source_id, video_id);
        """
    )


def export_csv(connection: sqlite3.Connection, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    query = """
    SELECT
        source_id,
        video_id,
        upload_date,
        uploader,
        title,
        duration,
        view_count,
        like_count,
        comment_count,
        repost_count,
        save_count,
        webpage_url,
        has_media,
        has_subtitles,
        subtitle_count,
        subtitle_langs,
        media_path,
        meta_path,
        description_path,
        synced_at
    FROM videos
    ORDER BY source_id, upload_date DESC, video_id DESC;
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

    export_csv(connection, csv_path)
    connection.close()
    mode = "incremental" if incremental else "full"
    print(f"[ledger] sqlite ({mode}) -> {db_path}")


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

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        global_config, all_sources = load_config(args.config)
        sources = select_sources(all_sources, args.sources)
    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    ledger_db_path = resolve_output_path(args.ledger_db, global_config.ledger_db)
    ledger_csv_path = resolve_output_path(args.ledger_csv, global_config.ledger_csv)

    if args.command == "sync":
        for source in sources:
            sync_source(
                source=source,
                dry_run=args.dry_run,
                skip_media=args.skip_media,
                skip_subs=args.skip_subs,
                skip_meta=args.skip_meta,
            )
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

    if args.command == "ledger":
        build_ledger(
            sources,
            ledger_db_path,
            ledger_csv_path,
            incremental=args.incremental,
        )
        return 0

    print("error: unsupported command", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
