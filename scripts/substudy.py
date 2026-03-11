#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import math
import mimetypes
import os
import plistlib
import random
import re
import signal
import shlex
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import zlib
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterable, cast
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import parse_qs, urlencode, urlparse

try:
    import tomllib
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "tomllib is required (Python 3.11+)."
    ) from exc

try:
    import fcntl  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

INFO_SUFFIX = ".info.json"
DEFAULT_CONFIG = Path("config/sources.toml")
DEFAULT_LEDGER_DB = Path("data/master_ledger.sqlite")
DEFAULT_LEDGER_CSV = Path("data/master_ledger.csv")
DEFAULT_TIKTOK_VIDEO_URL = "https://www.tiktok.com/@{handle}/video/{id}"
DEFAULT_TIKTOK_PROFILE_URL = "https://www.tiktok.com/@{handle}"
DEFAULT_TIKTOK_LIKED_URL = "https://www.tiktok.com/@{handle}/liked"
SOURCE_WATCH_KIND_POSTS = "posts"
SOURCE_WATCH_KIND_LIKES = "likes"
DEFAULT_MANAGED_TARGETS_FILE_NAME = "source_targets.json"
MANAGED_TARGETS_FORMAT_VERSION = 1
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
DEFAULT_DICT_BOOKMARK_EXPORT_DIR = Path("exports")
DEFAULT_NOTIFY_WEB_URL_BASE = f"http://{DEFAULT_WEB_HOST}:{DEFAULT_WEB_PORT}"
DEFAULT_NOTIFY_LLM_LOOKBACK_HOURS = 24
DEFAULT_NOTIFY_MACOS_LABEL = "com.substudy.notify"
DEFAULT_NOTIFY_INTERVAL_MINUTES = 90
DEFAULT_NOTIFY_COOLDOWN_MINUTES = 15
DEFAULT_LOCAL_LLM_ENDPOINT = "http://127.0.0.1:11435/v1/chat/completions"
DEFAULT_LOCAL_TRANSLATE_DRAFT_MODEL = "gpt-oss:20b"
DEFAULT_LOCAL_TRANSLATE_REFINE_MODEL = "gpt-oss:120b"
DEFAULT_LOCAL_TRANSLATE_GLOBAL_MODEL = "gpt-oss:120b"
DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MODEL = "gpt-oss:120b"
DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MODEL = "gpt-oss:120b"
DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MAX_TOKENS = 900
DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MAX_TOKENS = 900
DEFAULT_LOCAL_TRANSLATE_QUALITY_LOOP_MAX_ROUNDS = 0
DEFAULT_LOCAL_TRANSLATE_QUALITY_JSON_FRAGMENT_THRESHOLD = 0.0
DEFAULT_LOCAL_TRANSLATE_QUALITY_ENGLISH_HEAVY_THRESHOLD = 0.10
DEFAULT_LOCAL_TRANSLATE_QUALITY_UNCHANGED_THRESHOLD = 0.15
DEFAULT_NETWORK_PROBE_URL = "https://www.tiktok.com/robots.txt"
DEFAULT_NETWORK_PROBE_TIMEOUT_SEC = 8
DEFAULT_NETWORK_PROBE_BYTES = 131072
DEFAULT_WEAK_NET_MIN_KBPS = 900.0
DEFAULT_WEAK_NET_MAX_RTT_MS = 900.0
DEFAULT_METERED_MEDIA_MODE = "off"
DEFAULT_METERED_MIN_ARCHIVE_IDS = 200
DEFAULT_METERED_PLAYLIST_END = 40
DEFAULT_YTDLP_IMPERSONATE = ""
DEFAULT_PRODUCER_LOCK_FILE_NAME = "producer.lock"
DEFAULT_QUEUE_LEASE_SEC = 1800
DEFAULT_QUEUE_POLL_SEC = 3.0
DEFAULT_QUEUE_MAX_ATTEMPTS = 8
DEFAULT_QUEUE_STAGES = ("media", "subs", "meta", "asr", "loudness", "translate")
DEFAULT_YTDLP_URL_BATCH_SIZE = 5
DEFAULT_SLEEP_REQUESTS_MIN_SEC = 5.0
DEFAULT_SLEEP_REQUESTS_JITTER_RATIO = 0.35
DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_CHANCE = 0.18
DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_MIN_SEC = 1.0
DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_MAX_SEC = 2.5
AUTO_TAG_COMPARATORS = ("gte", "gt", "lte", "lt", "eq")
AUTO_TAG_METRIC_KEYS = frozenset(
    {
        "total_videos",
        "complete_count",
        "pending_total",
        "english_subtitles_ready",
        "english_subtitles_missing",
        "source_text_ready",
        "source_text_missing",
        "ja_subtitles_ready",
        "ja_subtitles_missing",
        "ja_subtitles_ready_playable",
        "ja_subtitles_missing_playable",
        "claude_subtitles_ready",
        "claude_subtitles_missing",
        "claude_subtitles_ready_playable",
        "claude_subtitles_missing_playable",
        "meta_ready",
        "meta_missing",
        "media_ready",
        "media_missing",
        "asr_ready",
        "asr_pending",
        "loudness_ready",
        "loudness_pending",
        "complete_ratio",
        "pending_ratio",
        "english_subtitles_ready_ratio",
        "english_subtitles_missing_ratio",
        "source_text_ready_ratio",
        "source_text_missing_ratio",
        "ja_subtitles_ready_ratio",
        "ja_subtitles_missing_ratio",
        "ja_subtitles_ready_playable_ratio",
        "ja_subtitles_missing_playable_ratio",
        "claude_subtitles_ready_ratio",
        "claude_subtitles_missing_ratio",
        "claude_subtitles_ready_playable_ratio",
        "claude_subtitles_missing_playable_ratio",
        "meta_ready_ratio",
        "meta_missing_ratio",
        "media_ready_ratio",
        "media_missing_ratio",
        "asr_ready_ratio",
        "asr_pending_ratio",
        "loudness_ready_ratio",
        "loudness_pending_ratio",
    }
)
_SOURCE_ACCESS_UNSET = object()
RE_TRANSLATION_ASCII = re.compile(r"[A-Za-z]")
RE_TRANSLATION_JA = re.compile(r"[ぁ-んァ-ヶ一-龯々ー]")
RE_RETRY_TIKTOK_BLOCKED = re.compile(
    r"(your ip address is blocked from accessing this post|"
    r"blocked from accessing this post|"
    r"status(?:\s*code)?\s*[:=]?\s*10204)",
    re.IGNORECASE,
)
RE_RETRY_TIKTOK_WEBPAGE_TRANSIENT = re.compile(
    r"(unable to extract universal data for rehydration|"
    r"unable to extract webpage video data|"
    r"unable to extract challenge data|"
    r"unexpected response from webpage request)",
    re.IGNORECASE,
)
RE_RETRY_MISSING_ARTIFACT = re.compile(
    r"(subtitle file missing after download attempt|did not write a terminal download_state row)",
    re.IGNORECASE,
)
RE_TIKTOK_ERROR_VIDEO_ID = re.compile(
    r"ERROR:\s*\[TikTok\]\s*(?P<video_id>\d{10,})\s*:\s*(?P<message>.+)",
    re.IGNORECASE,
)

_YTDLP_IMPERSONATE_TARGETS_CACHE: dict[str, list[str]] = {}
_YTDLP_IMPERSONATE_WARNED_KEYS: set[tuple[str, str]] = set()


@dataclass
class GlobalConfig:
    ledger_db: Path
    ledger_csv: Path
    source_order: str
    auto_tag_rules: list[AutoTagRule]


@dataclass(frozen=True)
class AutoTagRule:
    tag: str
    metric: str
    comparator: str
    threshold: float
    min_total_videos: int = 1


@dataclass
class SourceConfig:
    id: str
    platform: str
    url: str
    tags: list[str]
    watch_kind: str
    target_handle: str | None
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
    ytdlp_impersonate: str | None
    cookies_browser: str | None
    cookies_file: Path | None
    video_format: str
    sub_langs: str
    sub_format: str
    sleep_interval: int
    max_sleep_interval: int
    retry_sleep: int
    sleep_requests: float
    media_discovery_interval_hours: float
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
    origin: str
    subtitle_existing_match_langs: str | None = None
    subtitle_existing_origin_kind: str | None = None
    subtitle_download_archive_enabled: bool = True


@dataclass
class SubtitleCueBlock:
    cue_id: int
    block_index: int
    header_lines: list[str]
    timing_line: str
    text_lines: list[str]
    start_ms: int
    end_ms: int


@dataclass
class ParsedSubtitleDocument:
    path: Path
    format_hint: str
    blocks: list[dict[str, Any]]
    cues: list[SubtitleCueBlock]


@dataclass
class TranslationStageMetrics:
    stage_name: str
    model: str
    input_cue_count: int
    changed_cue_count: int = 0
    request_count: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    elapsed_ms: int = 0
    status: str = "completed"
    error_message: str = ""
    started_at: str = ""
    finished_at: str = ""


@dataclass
class TranslationQualityReport:
    total_cues: int
    json_fragment_cues: int = 0
    english_heavy_cues: int = 0
    unchanged_cues: int = 0
    empty_cues: int = 0
    bad_cue_ids: list[int] = field(default_factory=list)

    def json_fragment_rate(self) -> float:
        if self.total_cues <= 0:
            return 0.0
        return float(self.json_fragment_cues) / float(self.total_cues)

    def english_heavy_rate(self) -> float:
        if self.total_cues <= 0:
            return 0.0
        return float(self.english_heavy_cues) / float(self.total_cues)

    def unchanged_rate(self) -> float:
        if self.total_cues <= 0:
            return 0.0
        return float(self.unchanged_cues) / float(self.total_cues)


@dataclass
class NetworkProfileDecision:
    profile: str
    reason: str
    probe_url: str
    rtt_ms: float | None = None
    kbps: float | None = None
    bytes_read: int = 0


class ProducerLockAcquisitionError(RuntimeError):
    pass


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def get_queue_producer_lock_path(db_path: Path) -> Path:
    safe_db_path = Path(db_path).expanduser().resolve()
    return safe_db_path.parent / "locks" / DEFAULT_PRODUCER_LOCK_FILE_NAME


@contextmanager
def queue_producer_lock(lock_path: Path, enabled: bool = True) -> Iterable[None]:
    if not enabled:
        yield
        return
    if fcntl is None:  # pragma: no cover
        print(
            "[producer-lock] warning: fcntl unavailable; lock disabled",
            file=sys.stderr,
        )
        yield
        return

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+", encoding="utf-8")
    acquired = False
    holder = f"pid={os.getpid()} host={socket.gethostname()} started_at={now_utc_iso()}"
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.seek(0)
            existing_holder = lock_file.read().strip()
            detail = f" holder={existing_holder}" if existing_holder else ""
            raise ProducerLockAcquisitionError(
                f"producer lock busy: {lock_path}{detail}"
            ) from exc

        acquired = True
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"{holder}\n")
        lock_file.flush()
        print(f"[producer-lock] acquired {lock_path} ({holder})")
        yield
    finally:
        if acquired:
            try:
                lock_file.seek(0)
                lock_file.truncate()
                lock_file.flush()
            except OSError:
                pass
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            print(f"[producer-lock] released {lock_path}")
        lock_file.close()


def parse_iso_datetime_utc(raw_value: Any) -> dt.datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def build_media_audio_fallback_format_selector(primary_video_format: str) -> str:
    normalized = str(primary_video_format or "").strip()
    if not normalized:
        normalized = "best"
    # Prefer TikTok's dedicated "download" format when available, otherwise
    # require an audio-capable format before falling back to the primary selector.
    return f"download/best*[acodec!=none]/{normalized}"


def build_media_audio_fallback_format_candidates(
    primary_video_format: str,
    preferred_format: str | None = None,
) -> list[str]:
    candidates = [
        str(preferred_format or "").strip(),
        build_media_audio_fallback_format_selector(primary_video_format),
        "best*[acodec!=none][format_id*=h264]/best*[acodec!=none]",
        "best*[acodec!=none]/best",
    ]
    unique_candidates: list[str] = []
    seen_candidates: set[str] = set()
    for candidate in candidates:
        value = str(candidate or "").strip()
        if not value or value in seen_candidates:
            continue
        seen_candidates.add(value)
        unique_candidates.append(value)
    return unique_candidates


def compute_review_priority_score(
    bookmark_count: int,
    video_count: int,
    missing_count: int,
    last_seen_at: str,
    now_utc: dt.datetime | None = None,
) -> float:
    safe_bookmark_count = max(0, int(bookmark_count))
    safe_video_count = max(0, int(video_count))
    safe_missing_count = max(0, int(missing_count))
    reencounter_count = max(0, safe_bookmark_count - 1)
    now_value = now_utc or dt.datetime.now(dt.timezone.utc)
    last_seen_dt = parse_iso_datetime_utc(last_seen_at)
    if last_seen_dt is None:
        days_since_last = 30.0
    else:
        days_since_last = max(0.0, (now_value - last_seen_dt).total_seconds() / 86400.0)

    # Review priority heuristic:
    # - repeated encounters raise priority
    # - cross-video spread raises priority
    # - missing dictionary entries get extra urgency
    # - older items slowly regain priority over time
    score = (
        (reencounter_count * 1.2)
        + ((safe_video_count - 1) * 0.8)
        + (safe_missing_count * 1.5)
        + (min(30.0, days_since_last) * 0.1)
    )
    return round(score, 3)


def resolve_path(base: Path, raw_value: str | None, default: str) -> Path:
    value = raw_value if raw_value not in (None, "") else default
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def is_path_like_command(command: str) -> bool:
    return "/" in command or "\\" in command


def find_executable_command(command: str) -> str | None:
    value = str(command or "").strip()
    if not value:
        return None
    candidate = Path(value).expanduser()
    if candidate.is_absolute() or is_path_like_command(value):
        if candidate.exists():
            return str(candidate)
        return None
    return shutil.which(value)


def resolve_executable_command(command: str) -> str:
    resolved = find_executable_command(command)
    if resolved is not None:
        return resolved
    value = str(command or "").strip()
    if is_path_like_command(value):
        return str(Path(value).expanduser())
    return value


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


def parse_non_negative_float(raw_value: Any, fallback: float) -> float:
    if raw_value in (None, ""):
        return fallback
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(parsed) or parsed < 0:
        return fallback
    return parsed


def parse_config_finite_float(raw_value: Any, field_name: str) -> float:
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite number.") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be a finite number.")
    return parsed


def normalize_source_order_mode(raw_value: Any, fallback: str = "random") -> str:
    text = str(raw_value or "").strip().lower()
    if text in {"config", "random"}:
        return text
    return fallback


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


def merge_ytdlp_sub_langs(primary_raw: Any, extra_raw: Any) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for raw_value in (primary_raw, extra_raw):
        text = str(raw_value or "").strip()
        if not text:
            continue
        for part in text.split(","):
            token = str(part or "").strip()
            if not token:
                continue
            dedupe_key = token.casefold()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            merged.append(token)
    return ",".join(merged)


def split_ytdlp_sub_langs(raw_value: Any) -> list[str]:
    return merge_ytdlp_sub_langs(raw_value, "").split(",") if str(raw_value or "").strip() else []


def subtitle_language_matches_sub_langs(language: str | None, sub_langs_raw: Any) -> bool:
    normalized_language = str(language or "").strip()
    if not normalized_language:
        return False
    candidate_languages = [normalized_language]
    if "." in normalized_language:
        parts = [part.strip() for part in normalized_language.split(".") if str(part).strip()]
        for index in range(1, len(parts)):
            candidate = ".".join(parts[index:])
            if candidate and candidate not in candidate_languages:
                candidate_languages.append(candidate)
    for token in split_ytdlp_sub_langs(sub_langs_raw):
        if token.casefold() == "all":
            return True
        for candidate_language in candidate_languages:
            try:
                if re.fullmatch(token, candidate_language, flags=re.IGNORECASE):
                    return True
            except re.error:
                if candidate_language.casefold() == token.casefold():
                    return True
    return False


def subtitle_label_tokens(value: Any) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").strip().lower())
        if token
    }


def subtitle_path_label_hints(subtitle_path: str | None) -> list[str]:
    name = Path(str(subtitle_path or "").strip()).name.lower()
    if not name:
        return []
    parts = [part.strip() for part in name.split(".") if part.strip()]
    if len(parts) <= 2:
        return []
    return parts[1:-1]


def is_japanese_subtitle_label(language: str | None, subtitle_path: str | None = None) -> bool:
    normalized = str(language or "").strip().lower()
    path_hints = subtitle_path_label_hints(subtitle_path)
    if not normalized and not path_hints:
        return False
    if (
        normalized in {"ja", "jp", "jpn"}
        or normalized.startswith("ja-")
        or normalized.startswith("jp-")
        or normalized.startswith("jpn-")
        or "japanese" in normalized
    ):
        return True
    label_tokens = subtitle_label_tokens(normalized)
    if label_tokens & {"ja", "jp", "jpn", "japanese"}:
        return True
    path_tokens = {
        token
        for hint in path_hints
        for token in subtitle_label_tokens(hint)
    }
    return bool(path_tokens & {"ja", "jp", "jpn", "japanese"})


def classify_ja_subtitle_variant(language: str | None, subtitle_path: str | None = None) -> str:
    normalized = str(language or "").strip().lower()
    path_hints = subtitle_path_label_hints(subtitle_path)
    if (
        normalized == "ja-asr-local"
        or normalized.startswith("ja-asr-local-")
        or any(hint == "ja-asr-local" or hint.startswith("ja-asr-local-") for hint in path_hints)
    ):
        return "ja-asr-local"
    if (
        normalized == "ja-local"
        or normalized.startswith("ja-local-")
        or any(hint == "ja-local" or hint.startswith("ja-local-") for hint in path_hints)
    ):
        return "ja-local"
    if is_japanese_subtitle_label(language, subtitle_path):
        return "ja"
    return ""


def resolve_managed_targets_path(config_path: Path) -> Path:
    candidate = config_path.expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return (candidate.parent / DEFAULT_MANAGED_TARGETS_FILE_NAME).resolve()


def normalize_source_watch_kind(raw_value: Any) -> str:
    normalized = str(raw_value or "").strip().lower()
    if normalized in {
        SOURCE_WATCH_KIND_LIKES,
        "liked",
        "likes_feed",
        "liked_feed",
        "favorite",
        "favorites",
    }:
        return SOURCE_WATCH_KIND_LIKES
    return SOURCE_WATCH_KIND_POSTS


def normalize_source_tags(raw_value: Any) -> list[str]:
    if raw_value in (None, ""):
        return []
    candidates: list[str] = []
    if isinstance(raw_value, str):
        candidates.extend(re.split(r"[,;\n]", raw_value))
    elif isinstance(raw_value, list):
        for item in raw_value:
            if item in (None, ""):
                continue
            candidates.extend(re.split(r"[,;\n]", str(item)))
    else:
        candidates.append(str(raw_value))

    normalized_tags: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        tag = " ".join(str(candidate or "").strip().split())
        if not tag:
            continue
        dedupe_key = tag.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized_tags.append(tag)
    return normalized_tags


def normalize_auto_tag_metric(raw_value: Any) -> str:
    return str(raw_value or "").strip().lower()


def parse_auto_tag_rules(raw_value: Any) -> list[AutoTagRule]:
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, dict):
        candidates = [raw_value]
    elif isinstance(raw_value, list):
        candidates = raw_value
    else:
        raise ValueError("`auto_tags` must be a list of tables.")

    rules: list[AutoTagRule] = []
    for index, item in enumerate(candidates, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"`auto_tags[{index}]` must be a table.")
        if not parse_bool_like(item.get("enabled"), default=True):
            continue
        tag = " ".join(str(item.get("tag", "")).strip().split())
        if not tag:
            raise ValueError(f"`auto_tags[{index}].tag` is required.")
        metric = normalize_auto_tag_metric(item.get("metric"))
        if metric not in AUTO_TAG_METRIC_KEYS:
            supported_metrics = ", ".join(sorted(AUTO_TAG_METRIC_KEYS))
            raise ValueError(
                f"`auto_tags[{index}].metric` must be one of: {supported_metrics}"
            )
        comparisons = [
            (name, item.get(name))
            for name in AUTO_TAG_COMPARATORS
            if item.get(name) not in (None, "")
        ]
        if len(comparisons) != 1:
            raise ValueError(
                f"`auto_tags[{index}]` requires exactly one of "
                f"{', '.join(AUTO_TAG_COMPARATORS)}."
            )
        comparator, raw_threshold = comparisons[0]
        threshold = parse_config_finite_float(raw_threshold, f"`auto_tags[{index}].{comparator}`")
        min_total_videos = parse_optional_positive_int(item.get("min_total_videos"), 1) or 1
        rules.append(
            AutoTagRule(
                tag=tag,
                metric=metric,
                comparator=comparator,
                threshold=threshold,
                min_total_videos=max(1, int(min_total_videos)),
            )
        )
    return rules


def normalize_tiktok_handle(raw_value: Any) -> str | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    if "tiktok.com/" in text.lower():
        inferred = infer_tiktok_handle(text)
        if inferred:
            return inferred
    normalized = text.lstrip("@").strip()
    normalized = normalized.split("/", 1)[0].strip()
    if not normalized:
        return None
    return normalized


def build_tiktok_source_url(handle: str, watch_kind: str) -> str:
    if watch_kind == SOURCE_WATCH_KIND_LIKES:
        return DEFAULT_TIKTOK_LIKED_URL.format(handle=handle)
    return DEFAULT_TIKTOK_PROFILE_URL.format(handle=handle)


def load_managed_source_overrides(config_path: Path) -> list[dict[str, Any]]:
    managed_path = resolve_managed_targets_path(config_path)
    if not managed_path.exists():
        return []
    try:
        payload = json.loads(managed_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[config] warning: failed to parse managed targets file ({managed_path}): {exc}", file=sys.stderr)
        return []
    if not isinstance(payload, dict):
        return []
    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list):
        return []

    entries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_item in raw_targets:
        if not isinstance(raw_item, dict):
            continue
        source_id = str(raw_item.get("id", "")).strip()
        if not source_id or source_id in seen_ids:
            continue
        seen_ids.add(source_id)
        platform = str(raw_item.get("platform", "tiktok") or "tiktok").strip().lower()
        watch_kind = normalize_source_watch_kind(raw_item.get("watch_kind"))
        target_handle = normalize_tiktok_handle(
            raw_item.get("target_handle")
            if raw_item.get("target_handle") not in (None, "")
            else raw_item.get("handle")
        )
        url_raw = str(raw_item.get("url", "")).strip()
        if not url_raw and platform == "tiktok" and target_handle:
            url_raw = build_tiktok_source_url(target_handle, watch_kind)
        if not url_raw:
            print(
                f"[config] warning: skip managed target '{source_id}' because url/target_handle is missing",
                file=sys.stderr,
            )
            continue

        normalized: dict[str, Any] = {
            "id": source_id,
            "platform": platform,
            "url": url_raw,
            "watch_kind": watch_kind,
            "enabled": parse_bool_like(raw_item.get("enabled"), default=True),
        }
        if "tags" in raw_item:
            normalized["tags"] = normalize_source_tags(raw_item.get("tags"))
        if target_handle:
            normalized["target_handle"] = target_handle
            if watch_kind == SOURCE_WATCH_KIND_POSTS:
                normalized["handle"] = target_handle
        data_dir = str(raw_item.get("data_dir", "")).strip()
        if data_dir:
            normalized["data_dir"] = data_dir
        video_url_template = str(raw_item.get("video_url_template", "")).strip()
        if video_url_template:
            normalized["video_url_template"] = video_url_template
        entries.append(normalized)
    return entries


def merge_source_rows_with_managed_overrides(
    base_rows: list[dict[str, Any]],
    managed_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    merged_by_id: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    origin_by_id: dict[str, str] = {}

    for raw_row in base_rows:
        if not isinstance(raw_row, dict):
            continue
        source_id = str(raw_row.get("id", "")).strip()
        if not source_id:
            continue
        normalized = dict(raw_row)
        normalized["id"] = source_id
        if source_id not in merged_by_id:
            order.append(source_id)
        merged_by_id[source_id] = normalized
        origin_by_id[source_id] = "config"

    for managed_row in managed_rows:
        source_id = str(managed_row.get("id", "")).strip()
        if not source_id:
            continue
        if source_id in merged_by_id:
            merged = dict(merged_by_id[source_id])
            merged.update(managed_row)
            merged_by_id[source_id] = merged
            origin_by_id[source_id] = "managed_override"
            continue
        merged_by_id[source_id] = dict(managed_row)
        order.append(source_id)
        origin_by_id[source_id] = "managed"

    merged_rows = [merged_by_id[source_id] for source_id in order if source_id in merged_by_id]
    return merged_rows, origin_by_id


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

    global_source_order = normalize_source_order_mode(
        global_raw.get("source_order", "random"),
        fallback="random",
    )
    raw_auto_tag_rules: list[Any] = []
    if "auto_tags" in global_raw:
        global_auto_tags = global_raw.get("auto_tags")
        if isinstance(global_auto_tags, list):
            raw_auto_tag_rules.extend(global_auto_tags)
        elif global_auto_tags not in (None, ""):
            raise ValueError("`global.auto_tags` must be a list of tables.")
    root_auto_tags = raw.get("auto_tags")
    if isinstance(root_auto_tags, list):
        raw_auto_tag_rules.extend(root_auto_tags)
    elif root_auto_tags not in (None, ""):
        raise ValueError("`auto_tags` must be a list of tables.")
    auto_tag_rules = parse_auto_tag_rules(raw_auto_tag_rules)
    global_media_discovery_interval_hours = parse_non_negative_float(
        global_raw.get("media_discovery_interval_hours", 24.0),
        24.0,
    )

    global_config = GlobalConfig(
        ledger_db=resolve_path(cwd, global_raw.get("ledger_db"), str(DEFAULT_LEDGER_DB)),
        ledger_csv=resolve_path(cwd, global_raw.get("ledger_csv"), str(DEFAULT_LEDGER_CSV)),
        source_order=global_source_order,
        auto_tag_rules=auto_tag_rules,
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
    global_ytdlp_impersonate = normalize_ytdlp_impersonate(
        global_raw.get("ytdlp_impersonate", DEFAULT_YTDLP_IMPERSONATE)
    )
    global_cookies_file = parse_optional_path(cwd, global_raw.get("cookies_file"))

    sources_raw_base = raw.get("sources", [])
    if sources_raw_base is None:
        sources_raw_base = []
    if not isinstance(sources_raw_base, list):
        raise ValueError("`sources` must be a list of tables.")
    managed_overrides = load_managed_source_overrides(config_path)
    sources_raw, source_origin_by_id = merge_source_rows_with_managed_overrides(
        [row for row in sources_raw_base if isinstance(row, dict)],
        managed_overrides,
    )
    if not sources_raw:
        raise ValueError("No sources found in config (including managed targets).")

    sources: list[SourceConfig] = []
    for source_raw in sources_raw:
        source_id = str(source_raw.get("id", "")).strip()
        if not source_id:
            raise ValueError("Each source requires a non-empty `id`.")
        platform = str(source_raw.get("platform", "tiktok") or "tiktok").strip().lower()
        watch_kind = normalize_source_watch_kind(source_raw.get("watch_kind"))
        source_url_raw = str(source_raw.get("url", "")).strip()
        target_handle = normalize_tiktok_handle(
            source_raw.get("target_handle")
            if source_raw.get("target_handle") not in (None, "")
            else source_raw.get("handle")
        )
        if not target_handle and source_url_raw:
            target_handle = normalize_tiktok_handle(infer_tiktok_handle(source_url_raw))
        url = source_url_raw
        if not url and platform == "tiktok" and target_handle:
            url = build_tiktok_source_url(target_handle, watch_kind)
        if not url:
            raise ValueError(
                f"Source '{source_id}' is missing `url`. "
                "Set `url` directly, or set `target_handle` (and optional `watch_kind`)."
            )

        configured_handle = normalize_tiktok_handle(source_raw.get("handle"))
        if configured_handle:
            effective_handle = configured_handle
        elif target_handle:
            effective_handle = target_handle
        else:
            effective_handle = None

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
        merged_sub_langs = merge_ytdlp_sub_langs(
            source_raw.get("sub_langs", global_raw.get("sub_langs", "en.*,en,und")),
            source_raw.get(
                "upstream_sub_langs",
                global_raw.get("upstream_sub_langs", "ja.*,ja,jp.*,jpn.*"),
            ),
        ) or "en.*,en,und"

        source = SourceConfig(
            id=source_id,
            platform=platform,
            url=url,
            tags=normalize_source_tags(source_raw.get("tags")),
            watch_kind=watch_kind,
            target_handle=target_handle,
            enabled=parse_bool_like(source_raw.get("enabled"), default=True),
            data_dir=data_dir,
            media_dir=resolve_path(data_dir, source_raw.get("media_dir"), "media"),
            subs_dir=resolve_path(data_dir, source_raw.get("subs_dir"), "subs"),
            meta_dir=resolve_path(data_dir, source_raw.get("meta_dir"), "meta"),
            media_archive=resolve_path(data_dir, source_raw.get("media_archive"), "archives/media.archive.txt"),
            subs_archive=resolve_path(data_dir, source_raw.get("subs_archive"), "archives/subs.archive.txt"),
            urls_file=resolve_path(data_dir, source_raw.get("urls_file"), "archives/urls.txt"),
            handle=effective_handle,
            video_url_template=source_raw.get("video_url_template"),
            video_id_regex=str(source_raw.get("video_id_regex", r"_(\d{10,})_")),
            ytdlp_bin=resolve_executable_command(
                str(source_raw.get("ytdlp_bin", global_raw.get("ytdlp_bin", "yt-dlp")))
            ),
            ytdlp_impersonate=normalize_ytdlp_impersonate(
                source_raw.get("ytdlp_impersonate", global_ytdlp_impersonate)
            ),
            cookies_browser=source_cookies_browser,
            cookies_file=source_cookies_file,
            video_format=str(source_raw.get("video_format", global_raw.get("video_format", "bv*+ba/best"))),
            sub_langs=merged_sub_langs,
            sub_format=str(source_raw.get("sub_format", global_raw.get("sub_format", "vtt/ttml/best"))),
            sleep_interval=int(source_raw.get("sleep_interval", global_raw.get("sleep_interval", 2))),
            max_sleep_interval=int(
                source_raw.get("max_sleep_interval", global_raw.get("max_sleep_interval", 6))
            ),
            retry_sleep=int(source_raw.get("retry_sleep", global_raw.get("retry_sleep", 5))),
            sleep_requests=parse_non_negative_float(
                source_raw.get("sleep_requests", global_raw.get("sleep_requests", 1.0)),
                1.0,
            ),
            media_discovery_interval_hours=parse_non_negative_float(
                source_raw.get(
                    "media_discovery_interval_hours",
                    global_media_discovery_interval_hours,
                ),
                global_media_discovery_interval_hours,
            ),
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
            origin=source_origin_by_id.get(source_id, "config"),
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


def apply_upstream_sub_langs_override(
    sources: list[SourceConfig],
    override_raw: Any,
) -> list[SourceConfig]:
    override_sub_langs = merge_ytdlp_sub_langs(override_raw, "")
    if not override_sub_langs:
        return list(sources)

    return [
        replace(
            source,
            sub_langs=override_sub_langs,
            subtitle_existing_match_langs=override_sub_langs,
            subtitle_existing_origin_kind="upstream",
            subtitle_download_archive_enabled=False,
        )
        for source in sources
    ]


def order_sources_for_run(
    sources: list[SourceConfig],
    mode: str,
    command_name: str,
) -> list[SourceConfig]:
    normalized_mode = normalize_source_order_mode(mode, fallback="random")
    if normalized_mode != "random" or len(sources) <= 1:
        return list(sources)

    ordered = list(sources)
    random.shuffle(ordered)
    preview_ids = ", ".join(source.id for source in ordered[:8])
    if len(ordered) > 8:
        preview_ids += ", ..."
    print(f"[{command_name}] source order=random ({preview_ids})")
    return ordered


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


def normalize_ytdlp_impersonate(raw_value: Any) -> str | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    if value.lower() in {"off", "none", "false", "0"}:
        return None
    return value


def list_ytdlp_impersonate_targets(ytdlp_bin: str) -> list[str]:
    cached = _YTDLP_IMPERSONATE_TARGETS_CACHE.get(ytdlp_bin)
    if cached is not None:
        return list(cached)

    command = [str(ytdlp_bin), "--list-impersonate-targets"]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        _YTDLP_IMPERSONATE_TARGETS_CACHE[ytdlp_bin] = []
        return []

    output = f"{completed.stdout}\n{completed.stderr}"
    targets: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("["):
            continue
        if line.lower().startswith("client"):
            continue
        if line.startswith("-"):
            continue
        parts = line.split()
        if not parts:
            continue
        target = parts[0].strip().lower()
        if not target:
            continue
        if target in targets:
            continue
        targets.append(target)

    _YTDLP_IMPERSONATE_TARGETS_CACHE[ytdlp_bin] = list(targets)
    return targets


def _warn_impersonate_once(
    ytdlp_bin: str,
    impersonate_value: str,
    message: str,
) -> None:
    warn_key = (ytdlp_bin, impersonate_value)
    if warn_key in _YTDLP_IMPERSONATE_WARNED_KEYS:
        return
    _YTDLP_IMPERSONATE_WARNED_KEYS.add(warn_key)
    print(f"[yt-dlp] {message}", file=sys.stderr)


def resolve_impersonate_flags(source: SourceConfig) -> list[str]:
    configured = source.ytdlp_impersonate
    if not configured:
        return []

    available_targets = list_ytdlp_impersonate_targets(source.ytdlp_bin)
    if not available_targets:
        _warn_impersonate_once(
            source.ytdlp_bin,
            configured,
            (
                f"{source.id}: --impersonate '{configured}' requested, but no targets are available "
                f"for {source.ytdlp_bin}; skipping"
            ),
        )
        return []

    configured_stripped = str(configured).strip()
    configured_lower = configured_stripped.lower()
    if configured_lower == "auto":
        for preferred in ("chrome", "edge", "safari", "firefox", "tor"):
            if preferred in available_targets:
                return ["--impersonate", preferred]
        return ["--impersonate", available_targets[0]]

    requested_target = configured_lower.split(":", 1)[0]
    if requested_target in available_targets:
        return ["--impersonate", configured_stripped]

    fallback_target = available_targets[0]
    _warn_impersonate_once(
        source.ytdlp_bin,
        configured_stripped,
        (
            f"{source.id}: --impersonate '{configured_stripped}' unavailable; "
            f"falling back to '{fallback_target}'"
        ),
    )
    return ["--impersonate", fallback_target]


def run_command(command: list[str], dry_run: bool, raise_on_error: bool = True) -> int:
    print("$", shlex.join(command))
    if dry_run:
        return 0
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0 and raise_on_error:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}")
    return completed.returncode


def _stream_text_pipe(
    pipe: Any,
    target: Any,
    chunks: list[str],
) -> None:
    if pipe is None:
        return
    try:
        while True:
            chunk = pipe.readline()
            if chunk == "":
                break
            chunks.append(chunk)
            target.write(chunk)
            target.flush()
    except (OSError, ValueError):
        return
    finally:
        try:
            pipe.close()
        except (OSError, ValueError):
            pass


def _terminate_process_group(
    completed: subprocess.Popen[str],
    wait_timeout_sec: float = 2.0,
) -> None:
    try:
        if completed.poll() is not None:
            return
    except Exception:
        return

    terminated = False
    if hasattr(os, "killpg"):
        try:
            os.killpg(os.getpgid(completed.pid), signal.SIGTERM)
            terminated = True
        except ProcessLookupError:
            return
        except Exception:
            terminated = False

    if not terminated:
        try:
            completed.terminate()
        except ProcessLookupError:
            return
        except Exception:
            pass

    try:
        completed.wait(timeout=wait_timeout_sec)
        return
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        return

    killed = False
    if hasattr(os, "killpg"):
        try:
            os.killpg(os.getpgid(completed.pid), signal.SIGKILL)
            killed = True
        except ProcessLookupError:
            return
        except Exception:
            killed = False

    if not killed:
        try:
            completed.kill()
        except ProcessLookupError:
            return
        except Exception:
            return

    try:
        completed.wait(timeout=wait_timeout_sec)
    except Exception:
        return


def run_command_with_output(command: list[str], dry_run: bool) -> tuple[int, str]:
    print("$", shlex.join(command))
    if dry_run:
        return (0, "")

    completed = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    stdout_thread = threading.Thread(
        target=_stream_text_pipe,
        kwargs={
            "pipe": completed.stdout,
            "target": sys.stdout,
            "chunks": stdout_chunks,
        },
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_text_pipe,
        kwargs={
            "pipe": completed.stderr,
            "target": sys.stderr,
            "chunks": stderr_chunks,
        },
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    try:
        return_code = completed.wait()
        stdout_thread.join()
        stderr_thread.join()
    except BaseException:
        _terminate_process_group(completed)
        try:
            if completed.stdout is not None:
                completed.stdout.close()
        except (OSError, ValueError):
            pass
        try:
            if completed.stderr is not None:
                completed.stderr.close()
        except (OSError, ValueError):
            pass
        stdout_thread.join(timeout=2.0)
        stderr_thread.join(timeout=2.0)
        raise
    stdout_text = "".join(stdout_chunks)
    stderr_text = "".join(stderr_chunks)
    combined_output = "\n".join(
        part.strip()
        for part in (stderr_text, stdout_text)
        if str(part or "").strip()
    )
    return (return_code, combined_output)


def summarize_command_failure(output_text: str | None, exit_code: int) -> str:
    lines = [
        line.strip()
        for line in str(output_text or "").splitlines()
        if line.strip()
    ]
    for line in reversed(lines):
        lowered = line.lower()
        if lowered.startswith("error:") or "blocked" in lowered or "forbidden" in lowered:
            return line[:4000]
    if lines:
        return lines[-1][:4000]
    return f"command exit code {exit_code}"


def probe_network_quality(
    probe_url: str,
    timeout_sec: int,
    probe_bytes: int,
) -> tuple[float, float, int]:
    safe_url = str(probe_url).strip()
    if not safe_url:
        raise ValueError("probe_url is required")
    safe_timeout = max(2, int(timeout_sec))
    safe_bytes = max(1024, int(probe_bytes))
    request = urllib_request.Request(
        safe_url,
        headers={
            "User-Agent": "substudy-network-probe/1.0",
            "Range": f"bytes=0-{safe_bytes - 1}",
            "Cache-Control": "no-cache",
        },
        method="GET",
    )
    started = time.perf_counter()
    with urllib_request.urlopen(request, timeout=safe_timeout) as response:
        first_byte_at = time.perf_counter()
        payload = response.read(safe_bytes)
    finished = time.perf_counter()
    bytes_read = len(payload)
    rtt_ms = max(0.0, (first_byte_at - started) * 1000.0)
    transfer_sec = max(0.001, finished - first_byte_at)
    kbps = (bytes_read * 8.0) / 1000.0 / transfer_sec
    return (rtt_ms, kbps, bytes_read)


def decide_network_profile(
    profile_mode: str,
    probe_url: str,
    timeout_sec: int,
    probe_bytes: int,
    weak_net_min_kbps: float,
    weak_net_max_rtt_ms: float,
    probe_func: Callable[[str, int, int], tuple[float, float, int]] | None = None,
) -> NetworkProfileDecision:
    mode = str(profile_mode or "normal").strip().lower()
    if mode not in {"normal", "weak", "auto"}:
        mode = "normal"
    safe_probe_url = str(probe_url or DEFAULT_NETWORK_PROBE_URL).strip() or DEFAULT_NETWORK_PROBE_URL
    if mode == "normal":
        return NetworkProfileDecision(
            profile="normal",
            reason="manual profile=normal",
            probe_url=safe_probe_url,
        )
    if mode == "weak":
        return NetworkProfileDecision(
            profile="weak",
            reason="manual profile=weak",
            probe_url=safe_probe_url,
        )

    checker = probe_func or probe_network_quality
    safe_min_kbps = max(50.0, float(weak_net_min_kbps))
    safe_max_rtt_ms = max(50.0, float(weak_net_max_rtt_ms))
    try:
        rtt_ms, kbps, bytes_read = checker(
            safe_probe_url,
            max(2, int(timeout_sec)),
            max(1024, int(probe_bytes)),
        )
    except Exception as exc:
        return NetworkProfileDecision(
            profile="weak",
            reason=f"probe failed ({exc}); fallback=weak",
            probe_url=safe_probe_url,
        )

    weak_by_rtt = rtt_ms > safe_max_rtt_ms
    weak_by_kbps = kbps < safe_min_kbps
    weak_by_empty_payload = bytes_read <= 0
    if weak_by_rtt or weak_by_kbps or weak_by_empty_payload:
        reason_parts: list[str] = []
        if weak_by_rtt:
            reason_parts.append(f"rtt_ms={rtt_ms:.1f}>{safe_max_rtt_ms:.1f}")
        if weak_by_kbps:
            reason_parts.append(f"kbps={kbps:.1f}<{safe_min_kbps:.1f}")
        if weak_by_empty_payload:
            reason_parts.append("bytes=0")
        return NetworkProfileDecision(
            profile="weak",
            reason="auto probe: " + ", ".join(reason_parts),
            probe_url=safe_probe_url,
            rtt_ms=rtt_ms,
            kbps=kbps,
            bytes_read=max(0, int(bytes_read)),
        )
    return NetworkProfileDecision(
        profile="normal",
        reason=f"auto probe: rtt_ms={rtt_ms:.1f}, kbps={kbps:.1f}",
        probe_url=safe_probe_url,
        rtt_ms=rtt_ms,
        kbps=kbps,
        bytes_read=max(0, int(bytes_read)),
    )


def resolve_skip_media_with_network_profile(
    command_name: str,
    explicit_skip_media: bool,
    network_profile: str,
    network_probe_url: str,
    network_probe_timeout_sec: int,
    network_probe_bytes: int,
    weak_net_min_kbps: float,
    weak_net_max_rtt_ms: float,
) -> tuple[bool, NetworkProfileDecision | None]:
    if explicit_skip_media:
        print(f"[{command_name}] skip-media enabled by explicit flag.")
        return (True, None)
    decision = decide_network_profile(
        profile_mode=network_profile,
        probe_url=network_probe_url,
        timeout_sec=network_probe_timeout_sec,
        probe_bytes=network_probe_bytes,
        weak_net_min_kbps=weak_net_min_kbps,
        weak_net_max_rtt_ms=weak_net_max_rtt_ms,
    )
    print(
        f"[{command_name}] network profile={decision.profile} "
        f"probe_url={decision.probe_url} reason={decision.reason}"
    )
    if decision.profile == "weak":
        print(
            f"[{command_name}] weak network detected: force --skip-media "
            "(download metadata + subtitles only)."
        )
        return (True, decision)
    return (False, decision)


def normalize_metered_media_mode(raw_mode: str | None, fallback: str = DEFAULT_METERED_MEDIA_MODE) -> str:
    mode = str(raw_mode or "").strip().lower()
    if mode in {"off", "updates-only"}:
        return mode
    return str(fallback or DEFAULT_METERED_MEDIA_MODE).strip().lower() or DEFAULT_METERED_MEDIA_MODE


def resolve_metered_media_policy(
    source_id: str,
    mode: str,
    media_archive_count: int,
    configured_playlist_end: int | None,
    min_archive_ids: int,
    metered_playlist_end: int,
) -> tuple[bool, bool, int | None, str]:
    normalized_mode = normalize_metered_media_mode(mode, DEFAULT_METERED_MEDIA_MODE)
    safe_archive_count = max(0, int(media_archive_count))
    safe_min_archive_ids = max(0, int(min_archive_ids))
    safe_metered_playlist_end = max(1, int(metered_playlist_end))
    effective_playlist_end = configured_playlist_end

    if normalized_mode != "updates-only":
        return (False, False, effective_playlist_end, "metered mode off")

    if safe_archive_count < safe_min_archive_ids:
        return (
            True,
            False,
            effective_playlist_end,
            f"{source_id}: skip media (archive_count={safe_archive_count} < min={safe_min_archive_ids})",
        )

    if effective_playlist_end is None or effective_playlist_end > safe_metered_playlist_end:
        effective_playlist_end = safe_metered_playlist_end

    return (
        False,
        True,
        effective_playlist_end,
        f"{source_id}: updates-only discovery (archive_count={safe_archive_count}, "
        f"playlist_end={effective_playlist_end}, break_on_existing=on)",
    )


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


def build_ytdlp_retry_flags(
    source: SourceConfig,
    include_ignore_errors: bool = True,
) -> list[str]:
    flags: list[str] = []
    if include_ignore_errors:
        flags.append("--ignore-errors")
    flags.extend(
        [
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
    )
    if source.sleep_requests > 0:
        flags.extend(
            [
                "--sleep-requests",
                format_ytdlp_sleep_seconds(
                    compute_effective_sleep_requests_seconds(source)
                ),
            ]
        )
    return flags


def format_ytdlp_sleep_seconds(seconds: float) -> str:
    normalized = max(0.0, float(seconds))
    rendered = f"{normalized:.2f}".rstrip("0").rstrip(".")
    if "." not in rendered:
        rendered = f"{rendered}.0"
    return rendered


def compute_effective_sleep_requests_seconds(source: SourceConfig) -> float:
    base_seconds = max(0.0, float(source.sleep_requests))
    if base_seconds <= 0:
        return 0.0

    lower_bound = max(
        DEFAULT_SLEEP_REQUESTS_MIN_SEC,
        base_seconds * (1.0 - DEFAULT_SLEEP_REQUESTS_JITTER_RATIO),
    )
    upper_bound = max(lower_bound, base_seconds * (1.0 + DEFAULT_SLEEP_REQUESTS_JITTER_RATIO))
    effective_seconds = random.uniform(lower_bound, upper_bound)

    if random.random() < DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_CHANCE:
        effective_seconds += random.uniform(
            DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_MIN_SEC,
            DEFAULT_SLEEP_REQUESTS_LONG_PAUSE_MAX_SEC,
        )

    return round(effective_seconds, 2)


def chunk_items(items: list[Any], batch_size: int) -> list[list[Any]]:
    safe_batch_size = max(1, int(batch_size))
    return [
        list(items[index:index + safe_batch_size])
        for index in range(0, len(items), safe_batch_size)
    ]


def apply_stage_target_limit(target_ids: list[str], limit: int | None) -> list[str]:
    if limit is None:
        return list(target_ids)
    safe_limit = max(0, int(limit))
    if safe_limit <= 0:
        return []
    return list(target_ids[:safe_limit])


def build_run_local_urls_file(source: SourceConfig) -> Path:
    run_token = f"{int(time.time())}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    return source.media_archive.parent / "tmp" / f"urls.{run_token}.txt"


def normalize_optional_stage_limit(raw_value: Any) -> int | None:
    if raw_value in (None, ""):
        return None
    try:
        normalized = int(raw_value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


@dataclass
class SyncSourceRunResult:
    new_media_ids: list[str] = field(default_factory=list)


@dataclass
class ChunkedYtdlpStagePlan:
    source: SourceConfig
    stage: str
    active_urls_file: Path
    build_command: Callable[[], list[str]]
    command_template: list[str] | None
    url_chunks: list[list[tuple[str, str]]]
    target_ids: list[str]
    resolved_target_ids: list[str]
    unresolved_target_ids: list[str]
    started_at: str
    run_id: int | None
    payload: dict[str, Any] = field(default_factory=dict)
    chunk_index: int = 0
    exit_code: int = 0
    error: str | None = None
    blocked_error: str | None = None


def attempted_chunk_target_ids(plan: ChunkedYtdlpStagePlan) -> list[str]:
    attempted_ids: list[str] = []
    seen_ids: set[str] = set()
    attempted_chunk_count = max(0, min(plan.chunk_index, len(plan.url_chunks)))
    for chunk_pairs in plan.url_chunks[:attempted_chunk_count]:
        for video_id, _url in chunk_pairs:
            if video_id in seen_ids:
                continue
            seen_ids.add(video_id)
            attempted_ids.append(video_id)
    return attempted_ids


def extract_tiktok_transient_retry_video_ids(
    output_text: str | None,
    candidate_video_ids: Iterable[str],
) -> list[str]:
    candidate_set = {
        str(video_id or "").strip()
        for video_id in candidate_video_ids
        if str(video_id or "").strip()
    }
    if not candidate_set:
        return []
    retry_ids: list[str] = []
    seen_ids: set[str] = set()
    for line in str(output_text or "").splitlines():
        match = RE_TIKTOK_ERROR_VIDEO_ID.search(line)
        if match is None:
            continue
        video_id = str(match.group("video_id") or "").strip()
        if video_id not in candidate_set or video_id in seen_ids:
            continue
        message = str(match.group("message") or "").strip()
        if not RE_RETRY_TIKTOK_WEBPAGE_TRANSIENT.search(message):
            continue
        seen_ids.add(video_id)
        retry_ids.append(video_id)
    return retry_ids


def extract_tiktok_error_video_ids(
    output_text: str | None,
    candidate_video_ids: Iterable[str],
) -> set[str]:
    candidate_set = {
        str(video_id or "").strip()
        for video_id in candidate_video_ids
        if str(video_id or "").strip()
    }
    if not candidate_set:
        return set()
    error_ids: set[str] = set()
    for line in str(output_text or "").splitlines():
        match = RE_TIKTOK_ERROR_VIDEO_ID.search(line)
        if match is None:
            continue
        video_id = str(match.group("video_id") or "").strip()
        if video_id in candidate_set:
            error_ids.add(video_id)
    return error_ids


def run_interleaved_chunked_ytdlp_stage_plans(
    plans: list[ChunkedYtdlpStagePlan],
    dry_run: bool,
) -> None:
    active_plans = [
        plan
        for plan in plans
        if plan.command_template is not None and plan.url_chunks
    ]
    while True:
        progress_made = False
        for plan in active_plans:
            if plan.error is not None or plan.chunk_index >= len(plan.url_chunks):
                continue
            progress_made = True
            chunk_pairs = plan.url_chunks[plan.chunk_index]
            chunk_urls = [url for _, url in chunk_pairs]
            write_urls_file(plan.active_urls_file, chunk_urls)
            if plan.chunk_index == 0:
                command = [*cast(list[str], plan.command_template), "-a", str(plan.active_urls_file)]
            else:
                command = [*plan.build_command(), "-a", str(plan.active_urls_file)]
            print(
                f"[{plan.stage}] {plan.source.id}: chunk "
                f"{plan.chunk_index + 1}/{len(plan.url_chunks)} "
                f"targets={len(chunk_pairs)}"
            )
            try:
                plan.exit_code, output_text = run_command_with_output(
                    command,
                    dry_run=dry_run,
                )
                transient_retry_ids = extract_tiktok_transient_retry_video_ids(
                    output_text,
                    [video_id for video_id, _url in chunk_pairs],
                )
                if transient_retry_ids:
                    original_error_ids = extract_tiktok_error_video_ids(
                        output_text,
                        [video_id for video_id, _url in chunk_pairs],
                    )
                    retry_id_set = set(transient_retry_ids)
                    retry_pairs = [
                        (video_id, url)
                        for video_id, url in chunk_pairs
                        if video_id in retry_id_set
                    ]
                    if retry_pairs:
                        print(
                            f"[{plan.stage}] {plan.source.id}: retry transient TikTok webpage errors "
                            f"targets={len(retry_pairs)}"
                        )
                        write_urls_file(
                            plan.active_urls_file,
                            [url for _video_id, url in retry_pairs],
                        )
                        retry_command = [*plan.build_command(), "-a", str(plan.active_urls_file)]
                        retry_exit_code, retry_output_text = run_command_with_output(
                            retry_command,
                            dry_run=dry_run,
                        )
                        if retry_exit_code == 0:
                            if plan.exit_code != 0 and original_error_ids.issubset(retry_id_set):
                                plan.exit_code = 0
                                output_text = retry_output_text
                        else:
                            plan.exit_code = retry_exit_code
                            output_text = retry_output_text
                            plan.error = summarize_command_failure(
                                retry_output_text,
                                retry_exit_code,
                            )
                if plan.exit_code != 0:
                    plan.error = summarize_command_failure(output_text, plan.exit_code)
            except Exception as exc:
                plan.exit_code = 1
                plan.error = str(exc)
                print(
                    f"[sync] {plan.source.id} {plan.stage} command failed: {exc}",
                    file=sys.stderr,
                )
            finally:
                plan.chunk_index += 1
        if not progress_made:
            break


def prepare_subtitle_download_plan(
    *,
    source: SourceConfig,
    dry_run: bool,
    connection: sqlite3.Connection | None,
    metadata_candidate_ids: list[str] | None,
    new_media_ids: list[str],
    strict_candidate_scope: bool,
    active_urls_file: Path,
    cookie_flags: list[str],
    impersonate_flags: list[str],
    safe_video_url: Callable[[str], str | None],
    source_cooldown_active: bool,
    source_cooldown_until: str | None,
    limit: int | None = None,
    suppress_skip_log: bool = False,
) -> ChunkedYtdlpStagePlan | None:
    subs_before_ids = (
        set(read_archive_ids(source.subs_archive))
        if source.subtitle_download_archive_enabled
        else set()
    )
    local_sub_ids = scan_existing_subtitle_ids(
        source,
        match_langs=source.subtitle_existing_match_langs,
        origin_kind=source.subtitle_existing_origin_kind,
    )
    if source_cooldown_active:
        if not suppress_skip_log:
            print(
                f"skip subtitles (source network cooldown until {source_cooldown_until or 'unknown'})"
            )
        return None

    bootstrap_missing_sub_ids: list[str] = []
    if (
        connection is not None
        and not dry_run
        and not (strict_candidate_scope and metadata_candidate_ids is not None)
    ):
        bootstrap_limit = (
            max(1, int(limit))
            if limit is not None and int(limit) > 0
            else 200
        )
        bootstrap_missing_sub_ids = get_subtitle_missing_bootstrap_ids(
            connection=connection,
            source_id=source.id,
            existing_sub_ids=local_sub_ids,
            require_db_missing_subtitles=not (
                source.subtitle_existing_match_langs or source.subtitle_existing_origin_kind
            ),
            limit=bootstrap_limit,
        )

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
    for video_id in bootstrap_missing_sub_ids:
        if video_id in subtitle_candidate_ids:
            continue
        subtitle_candidate_ids.append(video_id)

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
    if (
        connection is not None
        and not dry_run
        and not (strict_candidate_scope and metadata_candidate_ids is not None)
    ):
        retry_sub_ids = get_due_retry_ids(connection, source.id, "subs")
    existing_retry_sub_ids = [
        video_id
        for video_id in retry_sub_ids
        if video_id in subs_before_ids or video_id in local_sub_ids
    ]
    retry_sub_ids = [
        video_id
        for video_id in retry_sub_ids
        if video_id not in subs_before_ids and video_id not in local_sub_ids
    ]

    subtitle_target_ids: list[str] = []
    seen_target_ids: set[str] = set()
    for video_id in [*missing_retryable_sub_ids, *retry_sub_ids]:
        if video_id in seen_target_ids:
            continue
        seen_target_ids.add(video_id)
        subtitle_target_ids.append(video_id)
    subtitle_target_ids = apply_stage_target_limit(subtitle_target_ids, limit)

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
            for video_id in existing_retry_sub_ids:
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
            f"bootstrap={len(bootstrap_missing_sub_ids)}, "
            f"deferred={len(deferred_sub_ids)}, "
            f"skipped_existing={len(existing_retry_sub_ids)})"
        )
        return None

    subtitle_url_pairs: list[tuple[str, str]] = []
    unresolved_subtitle_target_ids: list[str] = []
    for video_id in subtitle_target_ids:
        video_url = safe_video_url(video_id)
        if not video_url:
            unresolved_subtitle_target_ids.append(video_id)
            continue
        subtitle_url_pairs.append((video_id, video_url))
    resolved_subtitle_target_ids = [video_id for video_id, _ in subtitle_url_pairs]
    subtitle_url_chunks = chunk_items(
        subtitle_url_pairs,
        DEFAULT_YTDLP_URL_BATCH_SIZE,
    )

    if dry_run:
        print(
            f"subtitle targets (dry-run): {len(subtitle_target_ids)} "
            f"(resolved={len(resolved_subtitle_target_ids)}, unresolved={len(unresolved_subtitle_target_ids)}, "
            f"new={len(missing_retryable_sub_ids)}, retry={len(retry_sub_ids)}, "
            f"bootstrap={len(bootstrap_missing_sub_ids)}, "
            f"deferred={len(deferred_sub_ids)})"
        )

    def build_subs_command() -> list[str]:
        command = [
            source.ytdlp_bin,
            *impersonate_flags,
            *cookie_flags,
            "--continue",
            "--no-overwrites",
            *build_ytdlp_retry_flags(source, include_ignore_errors=True),
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
        ]
        if source.subtitle_download_archive_enabled:
            command[1:1] = [
                "--download-archive",
                str(source.subs_archive),
            ]
        return command

    subs_command_template: list[str] | None = None
    if resolved_subtitle_target_ids:
        subs_command_template = build_subs_command()

    subs_started_at = now_utc_iso()
    subs_run_id: int | None = None
    if connection is not None and not dry_run:
        subs_run_id = begin_download_run(
            connection=connection,
            source_id=source.id,
            stage="subs",
            command=(
                [*subs_command_template, "-a", str(active_urls_file)]
                if subs_command_template is not None
                else None
            ),
            target_count=len(resolved_subtitle_target_ids),
            started_at=subs_started_at,
        )
        connection.commit()

    return ChunkedYtdlpStagePlan(
        source=source,
        stage="subs",
        active_urls_file=active_urls_file,
        build_command=build_subs_command,
        command_template=subs_command_template,
        url_chunks=subtitle_url_chunks,
        target_ids=subtitle_target_ids,
        resolved_target_ids=resolved_subtitle_target_ids,
        unresolved_target_ids=unresolved_subtitle_target_ids,
        started_at=subs_started_at,
        run_id=subs_run_id,
        payload={
            "new_count": len(missing_retryable_sub_ids),
            "retry_count": len(retry_sub_ids),
            "bootstrap_count": len(bootstrap_missing_sub_ids),
            "deferred_count": len(deferred_sub_ids),
            "existing_retry_ids": list(existing_retry_sub_ids),
        },
    )


def finalize_subtitle_download_plan(
    plan: ChunkedYtdlpStagePlan,
    *,
    dry_run: bool,
    connection: sqlite3.Connection | None,
    safe_video_url: Callable[[str], str | None],
    activate_source_network_cooldown: Callable[[str | None, str | None], None] | None = None,
) -> None:
    if dry_run:
        return

    source = plan.source
    attempted_resolved_target_ids = attempted_chunk_target_ids(plan)
    attempted_resolved_target_id_set = set(attempted_resolved_target_ids)
    unattempted_resolved_target_ids = [
        video_id
        for video_id in plan.resolved_target_ids
        if video_id not in attempted_resolved_target_id_set
    ]
    subs_after_ids = (
        set(read_archive_ids(source.subs_archive))
        if source.subtitle_download_archive_enabled
        else set()
    )
    local_sub_after_ids = scan_existing_subtitle_ids(
        source,
        match_langs=source.subtitle_existing_match_langs,
        origin_kind=source.subtitle_existing_origin_kind,
    )
    success_sub_ids = [
        video_id
        for video_id in attempted_resolved_target_ids
        if video_id in subs_after_ids or video_id in local_sub_after_ids
    ]
    failed_sub_ids = [
        video_id
        for video_id in attempted_resolved_target_ids
        if video_id not in subs_after_ids and video_id not in local_sub_after_ids
    ]
    failed_sub_ids.extend(plan.unresolved_target_ids)

    existing_retry_sub_ids = [
        str(video_id)
        for video_id in cast(list[Any], plan.payload.get("existing_retry_ids") or [])
    ]
    if connection is not None:
        for video_id in success_sub_ids:
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id=video_id,
                status="success",
                run_id=plan.run_id,
                attempt_at=plan.started_at,
                url=safe_video_url(video_id),
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )
        for video_id in existing_retry_sub_ids:
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id=video_id,
                status="success",
                run_id=plan.run_id,
                attempt_at=plan.started_at,
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
            if video_id in plan.unresolved_target_ids:
                failure_reason = "cannot build video URL for subtitle download target"
            else:
                failure_reason = plan.error or (
                    f"command exit code {plan.exit_code}"
                    if plan.exit_code != 0
                    else "subtitle file missing after download attempt"
                )
            next_retry_at = schedule_next_retry_iso(
                next_retry_count,
                error_message=failure_reason,
            )
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id=video_id,
                status="error",
                run_id=plan.run_id,
                attempt_at=plan.started_at,
                url=safe_video_url(video_id),
                last_error=failure_reason,
                retry_count=next_retry_count,
                next_retry_at=next_retry_at,
            )
            blocked_until = extend_source_network_cooldown(
                connection=connection,
                source_id=source.id,
                error_message=failure_reason,
                blocked_until=next_retry_at,
                blocked_at=plan.started_at,
            )
            if blocked_until:
                plan.blocked_error = failure_reason
                if activate_source_network_cooldown is not None:
                    activate_source_network_cooldown(
                        blocked_until,
                        failure_reason,
                    )

        subs_stage_error = None if not failed_sub_ids else (
            "cannot build URL for some subtitle targets"
            if plan.unresolved_target_ids and plan.error is None and plan.exit_code == 0
            else (
                plan.error or (
                    f"command exit code {plan.exit_code}"
                    if plan.exit_code != 0
                    else "some subtitle items are still missing"
                )
            )
        )
        subs_finished_at = now_utc_iso()
        subs_succeeded = not failed_sub_ids
        if plan.command_template is not None:
            record_source_network_access(
                connection=connection,
                source=source,
                request_at=subs_finished_at,
                succeeded=subs_succeeded,
                clear_cooldown=subs_succeeded,
                last_error=(
                    None
                    if subs_succeeded
                    else (
                        _SOURCE_ACCESS_UNSET
                        if plan.blocked_error
                        else subs_stage_error
                    )
                ),
            )
        finish_download_run(
            connection=connection,
            run_id=plan.run_id if plan.run_id is not None else -1,
            status="success" if subs_succeeded else "error",
            finished_at=subs_finished_at,
            exit_code=plan.exit_code,
            success_count=len(success_sub_ids),
            failure_count=len(failed_sub_ids),
            error_message=subs_stage_error,
        )
        connection.commit()

    print(
        f"subtitle targets={len(plan.target_ids)} "
        f"success={len(success_sub_ids)} failed={len(failed_sub_ids)} "
        f"(resolved={len(plan.resolved_target_ids)}, unresolved={len(plan.unresolved_target_ids)}, "
        f"attempted={len(attempted_resolved_target_ids)}, "
        f"not_attempted={len(unattempted_resolved_target_ids)}, "
        f"new={int(plan.payload.get('new_count', 0))}, retry={int(plan.payload.get('retry_count', 0))}, "
        f"bootstrap={int(plan.payload.get('bootstrap_count', 0))}, "
        f"deferred={int(plan.payload.get('deferred_count', 0))}, "
        f"skipped_existing={len(existing_retry_sub_ids)})"
    )


def prepare_metadata_download_plan(
    *,
    source: SourceConfig,
    dry_run: bool,
    connection: sqlite3.Connection | None,
    metadata_candidate_ids: list[str] | None,
    strict_candidate_scope: bool,
    active_urls_file: Path,
    cookie_flags: list[str],
    impersonate_flags: list[str],
    safe_video_url: Callable[[str], str | None],
    source_cooldown_active: bool,
    source_cooldown_until: str | None,
    limit: int | None = None,
    suppress_skip_log: bool = False,
) -> ChunkedYtdlpStagePlan | None:
    if source_cooldown_active:
        if not suppress_skip_log:
            print(
                f"skip metadata (source network cooldown until {source_cooldown_until or 'unknown'})"
            )
        return None

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
        return None

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
    if (
        connection is not None
        and not dry_run
        and not (strict_candidate_scope and metadata_candidate_ids is not None)
    ):
        retry_ids = get_due_retry_ids(connection, source.id, "meta")

    metadata_target_ids: list[str] = []
    seen_target_ids: set[str] = set()
    for video_id in [*missing_retryable_ids, *retry_ids]:
        if video_id in seen_target_ids:
            continue
        seen_target_ids.add(video_id)
        metadata_target_ids.append(video_id)
    metadata_target_ids = apply_stage_target_limit(metadata_target_ids, limit)

    if not metadata_target_ids:
        print("metadata already up to date")
        return None

    metadata_url_pairs: list[tuple[str, str]] = []
    unresolved_metadata_target_ids: list[str] = []
    for video_id in metadata_target_ids:
        video_url = safe_video_url(video_id)
        if not video_url:
            unresolved_metadata_target_ids.append(video_id)
            continue
        metadata_url_pairs.append((video_id, video_url))
    resolved_metadata_target_ids = [video_id for video_id, _ in metadata_url_pairs]
    metadata_url_chunks = chunk_items(
        metadata_url_pairs,
        DEFAULT_YTDLP_URL_BATCH_SIZE,
    )
    if dry_run:
        print(
            f"metadata target IDs (dry-run): {len(metadata_target_ids)} "
            f"(resolved={len(resolved_metadata_target_ids)}, unresolved={len(unresolved_metadata_target_ids)}, "
            f"new={len(missing_retryable_ids)}, retry={len(retry_ids)}, "
            f"deferred={len(deferred_missing_ids)})"
        )

    def build_metadata_command() -> list[str]:
        return [
            source.ytdlp_bin,
            *impersonate_flags,
            *cookie_flags,
            "--skip-download",
            "--write-info-json",
            "--write-description",
            "--continue",
            "--no-overwrites",
            *build_ytdlp_retry_flags(source, include_ignore_errors=True),
            "-o",
            str(source.meta_dir / source.meta_output_template),
            "--no-playlist",
        ]

    metadata_command_template: list[str] | None = None
    if resolved_metadata_target_ids:
        metadata_command_template = build_metadata_command()
    meta_started_at = now_utc_iso()
    meta_run_id: int | None = None
    if connection is not None and not dry_run:
        meta_run_id = begin_download_run(
            connection=connection,
            source_id=source.id,
            stage="meta",
            command=(
                [*metadata_command_template, "-a", str(active_urls_file)]
                if metadata_command_template is not None
                else None
            ),
            target_count=len(resolved_metadata_target_ids),
            started_at=meta_started_at,
        )
        connection.commit()

    return ChunkedYtdlpStagePlan(
        source=source,
        stage="meta",
        active_urls_file=active_urls_file,
        build_command=build_metadata_command,
        command_template=metadata_command_template,
        url_chunks=metadata_url_chunks,
        target_ids=metadata_target_ids,
        resolved_target_ids=resolved_metadata_target_ids,
        unresolved_target_ids=unresolved_metadata_target_ids,
        started_at=meta_started_at,
        run_id=meta_run_id,
        payload={
            "new_count": len(missing_retryable_ids),
            "retry_count": len(retry_ids),
            "deferred_count": len(deferred_missing_ids),
        },
    )


def finalize_metadata_download_plan(
    plan: ChunkedYtdlpStagePlan,
    *,
    dry_run: bool,
    connection: sqlite3.Connection | None,
    safe_video_url: Callable[[str], str | None],
    activate_source_network_cooldown: Callable[[str | None, str | None], None] | None = None,
) -> None:
    if dry_run:
        return

    source = plan.source
    attempted_resolved_target_ids = attempted_chunk_target_ids(plan)
    attempted_resolved_target_id_set = set(attempted_resolved_target_ids)
    unattempted_resolved_target_ids = [
        video_id
        for video_id in plan.resolved_target_ids
        if video_id not in attempted_resolved_target_id_set
    ]
    post_meta_ids = list_meta_ids(source.meta_dir)
    success_meta_ids = [
        video_id for video_id in attempted_resolved_target_ids if video_id in post_meta_ids
    ]
    failed_meta_ids = [
        video_id for video_id in attempted_resolved_target_ids if video_id not in post_meta_ids
    ]
    failed_meta_ids.extend(plan.unresolved_target_ids)

    if connection is not None:
        for video_id in success_meta_ids:
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id=video_id,
                status="success",
                run_id=plan.run_id,
                attempt_at=plan.started_at,
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
            if video_id in plan.unresolved_target_ids:
                failure_reason = "cannot build video URL for metadata download target"
            else:
                failure_reason = plan.error or (
                    f"command exit code {plan.exit_code}"
                    if plan.exit_code != 0
                    else "metadata file missing after download attempt"
                )
            next_retry_at = schedule_next_retry_iso(
                next_retry_count,
                error_message=failure_reason,
            )
            upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id=video_id,
                status="error",
                run_id=plan.run_id,
                attempt_at=plan.started_at,
                url=safe_video_url(video_id),
                last_error=failure_reason,
                retry_count=next_retry_count,
                next_retry_at=next_retry_at,
            )
            blocked_until = extend_source_network_cooldown(
                connection=connection,
                source_id=source.id,
                error_message=failure_reason,
                blocked_until=next_retry_at,
                blocked_at=plan.started_at,
            )
            if blocked_until:
                plan.blocked_error = failure_reason
                if activate_source_network_cooldown is not None:
                    activate_source_network_cooldown(
                        blocked_until,
                        failure_reason,
                    )

        meta_stage_error = None if not failed_meta_ids else (
            "cannot build URL for some metadata targets"
            if plan.unresolved_target_ids and plan.error is None and plan.exit_code == 0
            else (
                plan.error or (
                    f"command exit code {plan.exit_code}"
                    if plan.exit_code != 0
                    else "some metadata items are still missing"
                )
            )
        )
        meta_finished_at = now_utc_iso()
        meta_succeeded = not failed_meta_ids
        if plan.command_template is not None:
            record_source_network_access(
                connection=connection,
                source=source,
                request_at=meta_finished_at,
                succeeded=meta_succeeded,
                clear_cooldown=meta_succeeded,
                last_error=(
                    None
                    if meta_succeeded
                    else (
                        _SOURCE_ACCESS_UNSET
                        if plan.blocked_error
                        else meta_stage_error
                    )
                ),
            )
        finish_download_run(
            connection=connection,
            run_id=plan.run_id if plan.run_id is not None else -1,
            status="success" if meta_succeeded else "error",
            finished_at=meta_finished_at,
            exit_code=plan.exit_code,
            success_count=len(success_meta_ids),
            failure_count=len(failed_meta_ids),
            error_message=meta_stage_error,
        )
        connection.commit()

    print(
        f"metadata targets={len(plan.target_ids)} "
        f"success={len(success_meta_ids)} failed={len(failed_meta_ids)} "
        f"(resolved={len(plan.resolved_target_ids)}, unresolved={len(plan.unresolved_target_ids)}, "
        f"attempted={len(attempted_resolved_target_ids)}, "
        f"not_attempted={len(unattempted_resolved_target_ids)}, "
        f"new={int(plan.payload.get('new_count', 0))}, retry={int(plan.payload.get('retry_count', 0))}, "
        f"deferred={int(plan.payload.get('deferred_count', 0))})"
    )


def sync_source(
    source: SourceConfig,
    dry_run: bool,
    skip_media: bool,
    skip_subs: bool,
    skip_meta: bool,
    connection: sqlite3.Connection | None = None,
    playlist_start: int | None = None,
    playlist_end: int | None = None,
    media_candidate_ids: list[str] | None = None,
    metadata_candidate_ids: list[str] | None = None,
    run_label: str = "sync",
    respect_media_discovery_interval: bool = True,
    respect_source_cooldown: bool = True,
    metered_media_mode: str = DEFAULT_METERED_MEDIA_MODE,
    metered_min_archive_ids: int = DEFAULT_METERED_MIN_ARCHIVE_IDS,
    metered_playlist_end: int = DEFAULT_METERED_PLAYLIST_END,
    urls_file_override: Path | None = None,
    strict_candidate_scope: bool = False,
    stage_limit: int | None = None,
    suppress_skip_subs_log: bool = False,
    suppress_skip_meta_log: bool = False,
) -> SyncSourceRunResult:
    print(f"\n=== {run_label}: {source.id} ===")

    source.media_dir.mkdir(parents=True, exist_ok=True)
    source.subs_dir.mkdir(parents=True, exist_ok=True)
    source.meta_dir.mkdir(parents=True, exist_ok=True)
    source.media_archive.parent.mkdir(parents=True, exist_ok=True)
    source.subs_archive.parent.mkdir(parents=True, exist_ok=True)

    bootstrap_missing_archives(source, dry_run=dry_run)

    source_cooldown_active = False
    source_cooldown_remaining_hours = 0.0
    source_cooldown_until: str | None = None
    source_cooldown_reason: str | None = None
    if (
        respect_source_cooldown
        and connection is not None
        and not dry_run
    ):
        (
            source_cooldown_active,
            source_cooldown_remaining_hours,
            source_cooldown_until,
            source_cooldown_reason,
        ) = get_source_network_cooldown_state(
            connection=connection,
            source_id=source.id,
        )
        if source_cooldown_active:
            print(
                f"[network-cooldown] {source.id}: active until="
                f"{source_cooldown_until or 'unknown'} "
                f"remaining={source_cooldown_remaining_hours:.2f}h"
            )
            if source_cooldown_reason:
                print(f"[network-cooldown] {source.id}: reason={source_cooldown_reason}")

    def activate_source_network_cooldown(
        blocked_until: str | None,
        reason: str | None,
    ) -> None:
        nonlocal source_cooldown_active
        nonlocal source_cooldown_remaining_hours
        nonlocal source_cooldown_until
        nonlocal source_cooldown_reason
        if not blocked_until:
            return
        blocked_until_dt = parse_iso_datetime_utc(blocked_until)
        source_cooldown_active = True
        source_cooldown_until = blocked_until
        source_cooldown_reason = reason
        if blocked_until_dt is None:
            source_cooldown_remaining_hours = 0.0
            return
        remaining_seconds = max(
            0.0,
            (
                blocked_until_dt
                - dt.datetime.now(dt.timezone.utc)
            ).total_seconds(),
        )
        source_cooldown_remaining_hours = remaining_seconds / 3600.0

    media_before_ids = set(read_archive_ids(source.media_archive))
    configured_playlist_end = playlist_end if playlist_end is not None else source.playlist_end
    metered_skip_media = False
    metered_force_break_on_existing = False
    metered_reason = ""
    effective_playlist_end = configured_playlist_end
    if media_candidate_ids is None and playlist_start is None:
        (
            metered_skip_media,
            metered_force_break_on_existing,
            effective_playlist_end,
            metered_reason,
        ) = resolve_metered_media_policy(
            source_id=source.id,
            mode=metered_media_mode,
            media_archive_count=len(media_before_ids),
            configured_playlist_end=configured_playlist_end,
            min_archive_ids=max(0, int(metered_min_archive_ids)),
            metered_playlist_end=max(1, int(metered_playlist_end)),
        )
        if normalize_metered_media_mode(metered_media_mode, DEFAULT_METERED_MEDIA_MODE) == "updates-only":
            print(f"[media] {metered_reason}")

    retry_flags = build_ytdlp_retry_flags(source, include_ignore_errors=True)
    discovery_flags: list[str] = []
    if source.break_on_existing or metered_force_break_on_existing:
        discovery_flags.append("--break-on-existing")
    if source.break_per_input:
        discovery_flags.append("--break-per-input")
    if source.lazy_playlist:
        discovery_flags.append("--lazy-playlist")
    if playlist_start is not None:
        discovery_flags.extend(["--playlist-start", str(playlist_start)])
    if effective_playlist_end is not None:
        discovery_flags.extend(["--playlist-end", str(effective_playlist_end)])
    cookie_flags = resolve_cookie_flags(source)
    impersonate_flags = resolve_impersonate_flags(source)
    if urls_file_override is not None:
        active_urls_file = urls_file_override
    else:
        active_urls_file = build_run_local_urls_file(source)

    def safe_video_url(video_id: str) -> str | None:
        try:
            return build_video_url(source, video_id)
        except ValueError:
            return None

    new_media_ids: list[str] = []
    normalized_media_candidate_ids: list[str] = []
    if media_candidate_ids is not None:
        seen_media_candidate_ids: set[str] = set()
        for video_id in media_candidate_ids:
            video_id_value = str(video_id or "").strip()
            if not video_id_value or video_id_value in seen_media_candidate_ids:
                continue
            seen_media_candidate_ids.add(video_id_value)
            normalized_media_candidate_ids.append(video_id_value)

    if not skip_media and not metered_skip_media and not source_cooldown_active:
        media_discovery_state_key = f"media_discovery_last_attempt:{source.id}"
        run_media_discovery = media_candidate_ids is None
        media_discovery_remaining_hours: float | None = None
        media_discovery_last_attempt: str | None = None
        if (
            connection is not None
            and not dry_run
            and respect_media_discovery_interval
            and source.media_discovery_interval_hours > 0
            and media_candidate_ids is None
        ):
            media_discovery_last_attempt = get_app_state_value(
                connection,
                media_discovery_state_key,
                default="",
            ).strip()
            if media_discovery_last_attempt:
                last_attempt_dt = parse_iso_datetime_utc(media_discovery_last_attempt)
                if last_attempt_dt is not None:
                    now_dt = dt.datetime.now(dt.timezone.utc)
                    elapsed_hours = max(0.0, (now_dt - last_attempt_dt).total_seconds() / 3600.0)
                    if elapsed_hours < source.media_discovery_interval_hours:
                        run_media_discovery = False
                        media_discovery_remaining_hours = (
                            source.media_discovery_interval_hours - elapsed_hours
                        )

        media_command: list[str] | None = None
        if run_media_discovery:
            media_command = [
                source.ytdlp_bin,
                *impersonate_flags,
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
        elif media_candidate_ids is not None:
            new_media_ids = list(normalized_media_candidate_ids)
            print(
                f"[media] {source.id}: target mode "
                f"video_ids={len(new_media_ids)}"
            )
        else:
            wait_label = (
                f"{media_discovery_remaining_hours:.2f}h"
                if media_discovery_remaining_hours is not None
                else "n/a"
            )
            print(
                f"[media] {source.id}: discovery deferred "
                f"(last_attempt={media_discovery_last_attempt or 'unknown'}, "
                f"remaining={wait_label}, "
                f"interval={source.media_discovery_interval_hours:.2f}h)"
            )
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
        media_blocked_error: str | None = None
        if media_command is not None:
            try:
                media_exit_code, media_output = run_command_with_output(
                    media_command,
                    dry_run=dry_run,
                )
                if media_exit_code != 0:
                    media_error = summarize_command_failure(
                        media_output,
                        media_exit_code,
                    )
            except Exception as exc:
                media_exit_code = 1
                media_error = str(exc)
                print(f"[sync] {source.id} media command failed: {exc}", file=sys.stderr)
            if connection is not None and not dry_run:
                if media_error:
                    blocked_until = extend_source_network_cooldown(
                        connection=connection,
                        source_id=source.id,
                        error_message=media_error,
                        blocked_until=schedule_next_retry_iso(
                            1,
                            error_message=media_error,
                        ),
                        blocked_at=media_started_at,
                    )
                    if blocked_until:
                        media_blocked_error = media_error
                        activate_source_network_cooldown(
                            blocked_until=blocked_until,
                            reason=media_error,
                        )
                set_app_state_value(
                    connection,
                    media_discovery_state_key,
                    media_started_at,
                )
                connection.commit()

        if media_candidate_ids is None:
            media_after_ids = set(read_archive_ids(source.media_archive))
            new_media_ids = sorted(media_after_ids - media_before_ids)

        retry_media_ids: list[str] = []
        bootstrap_no_audio_media_ids: list[str] = []
        if connection is not None and not dry_run and media_candidate_ids is None:
            retry_media_ids = get_due_retry_ids(connection, source.id, "media")
            bootstrap_no_audio_media_ids = get_media_no_audio_bootstrap_ids(
                connection=connection,
                source_id=source.id,
            )

        media_audio_target_ids: list[str] = []
        seen_media_targets: set[str] = set()
        for video_id in [*new_media_ids, *retry_media_ids, *bootstrap_no_audio_media_ids]:
            if video_id in seen_media_targets:
                continue
            seen_media_targets.add(video_id)
            media_audio_target_ids.append(video_id)

        media_audio_fallback_failures: dict[str, str] = {}
        media_audio_fallback_repaired = 0
        repaired_media_ids: set[str] = set()
        evaluated_media_ids: list[str] = []
        seen_evaluated_media_ids: set[str] = set()
        for video_id in new_media_ids:
            seen_evaluated_media_ids.add(video_id)
            evaluated_media_ids.append(video_id)

        ffprobe_bin = find_executable_command("ffprobe")
        ffmpeg_bin = find_executable_command("ffmpeg")
        media_fallback_work_dir = source.media_dir / ".audio_fallback"
        media_audio_preferred_format: str | None = None
        if connection is not None and not dry_run:
            media_audio_preferred_format = get_media_fallback_preferred_format(
                connection=connection,
                source_id=source.id,
            )
        media_audio_fallback_format = build_media_audio_fallback_format_selector(
            source.video_format
        )
        media_audio_fallback_format_candidates = build_media_audio_fallback_format_candidates(
            source.video_format,
            preferred_format=media_audio_preferred_format,
        )

        def run_media_primary_download(
            video_id: str,
            output_path: Path | None,
        ) -> tuple[Path | None, str | None]:
            video_url = safe_video_url(video_id)
            if video_url is None:
                return None, "cannot build video URL for primary download"

            primary_command = [
                source.ytdlp_bin,
                *impersonate_flags,
                *cookie_flags,
                "--continue",
                "--force-overwrites",
                *retry_flags,
                "-f",
                source.video_format,
                "-o",
                (
                    str(output_path)
                    if output_path is not None
                    else str(source.media_dir / source.media_output_template)
                ),
                "--no-playlist",
                video_url,
            ]
            try:
                primary_exit_code, primary_output = run_command_with_output(
                    primary_command,
                    dry_run=False,
                )
            except Exception as exc:
                return None, f"primary download command exception: {exc}"

            if primary_exit_code != 0:
                return None, summarize_command_failure(primary_output, primary_exit_code)

            refreshed_media_path = find_media_file_for_video(source, video_id)
            if refreshed_media_path is None or not refreshed_media_path.exists():
                return None, "media file missing after primary download"
            return refreshed_media_path, None

        def run_media_audio_fallback_merge(
            video_id: str,
            media_path: Path,
        ) -> tuple[Path | None, str | None, str | None, str | None]:
            if ffmpeg_bin is None:
                return None, "ffmpeg not found for audio fallback merge", None, None

            video_url = safe_video_url(video_id)
            if video_url is None:
                return None, "cannot build video URL for audio fallback", None, None

            media_fallback_work_dir.mkdir(parents=True, exist_ok=True)
            donor_media_path = media_fallback_work_dir / f"{video_id}.donor.mp4"
            merged_temp_path = media_fallback_work_dir / f"{video_id}.merged.tmp.mp4"
            merged_output_path = (
                media_path
                if media_path.suffix.lower() == ".mp4"
                else media_path.with_suffix(".mp4")
            )

            donor_source = "download"
            donor_format = media_audio_fallback_format
            cleanup_donor_file = False
            selected_download_format: str | None = None
            try:
                donor_is_usable = False
                try:
                    donor_is_usable = donor_media_path.exists() and donor_media_path.stat().st_size > 0
                except OSError:
                    donor_is_usable = False

                if donor_is_usable and ffprobe_bin is not None:
                    cached_has_audio, cached_probe_error = detect_audio_stream(
                        media_path=donor_media_path,
                        ffprobe_bin=ffprobe_bin,
                    )
                    if cached_has_audio is False:
                        donor_is_usable = False
                        try:
                            donor_media_path.unlink()
                        except OSError:
                            pass
                    elif cached_has_audio is None:
                        print(
                            f"[media] {source.id}/{video_id}: donor ffprobe warning "
                            f"({cached_probe_error}); use cached donor",
                            file=sys.stderr,
                        )

                if donor_is_usable:
                    donor_source = "cache"
                    donor_format = "cached"
                else:
                    download_errors: list[str] = []
                    selected_download_format = None
                    for candidate_format in media_audio_fallback_format_candidates:
                        if donor_media_path.exists():
                            try:
                                donor_media_path.unlink()
                            except OSError:
                                pass

                        fallback_command = [
                            source.ytdlp_bin,
                            *impersonate_flags,
                            *cookie_flags,
                            "--continue",
                            "--force-overwrites",
                            *retry_flags,
                            "-f",
                            candidate_format,
                            "-o",
                            str(donor_media_path),
                            "--no-playlist",
                            video_url,
                        ]
                        try:
                            fallback_exit_code, fallback_output = run_command_with_output(
                                fallback_command,
                                dry_run=False,
                            )
                        except Exception as exc:
                            download_errors.append(
                                f"{candidate_format}: command exception ({exc})"
                            )
                            continue

                        if fallback_exit_code != 0:
                            download_errors.append(
                                f"{candidate_format}: "
                                f"{summarize_command_failure(fallback_output, fallback_exit_code)}"
                            )
                            continue
                        if not donor_media_path.exists():
                            download_errors.append(
                                f"{candidate_format}: donor file missing after download"
                            )
                            continue
                        try:
                            donor_size = donor_media_path.stat().st_size
                        except OSError:
                            donor_size = 0
                        if donor_size <= 0:
                            download_errors.append(
                                f"{candidate_format}: donor file is empty"
                            )
                            try:
                                donor_media_path.unlink()
                            except OSError:
                                pass
                            continue

                        if ffprobe_bin is not None:
                            donor_has_audio, donor_probe_error = detect_audio_stream(
                                media_path=donor_media_path,
                                ffprobe_bin=ffprobe_bin,
                            )
                            if donor_has_audio is False:
                                download_errors.append(
                                    f"{candidate_format}: donor has no audio stream"
                                )
                                try:
                                    donor_media_path.unlink()
                                except OSError:
                                    pass
                                continue
                            if donor_has_audio is None:
                                print(
                                    f"[media] {source.id}/{video_id}: donor ffprobe warning "
                                    f"({donor_probe_error}); continue with downloaded donor",
                                    file=sys.stderr,
                                )

                        selected_download_format = candidate_format
                        break

                    if selected_download_format is None:
                        cleanup_donor_file = True
                        if download_errors:
                            detail = "; ".join(download_errors[-3:])
                            return (
                                None,
                                f"audio fallback donor has no usable audio ({detail})",
                                None,
                                None,
                            )
                        return None, "audio fallback donor has no usable audio", None, None

                    donor_source = "download"
                    donor_format = selected_download_format

                def summarize_ffmpeg_failure(
                    completed: subprocess.CompletedProcess[str],
                ) -> str:
                    merged_output = f"{completed.stderr}\n{completed.stdout}".strip()
                    lines = [line.strip() for line in merged_output.splitlines() if line.strip()]
                    if lines:
                        return " | ".join(lines[-3:])
                    return f"ffmpeg exited with code {completed.returncode}"

                merge_command_copy = [
                    ffmpeg_bin,
                    "-y",
                    "-i",
                    str(media_path),
                    "-i",
                    str(donor_media_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "aac",
                    "-shortest",
                    "-movflags",
                    "+faststart",
                    str(merged_temp_path),
                ]
                completed_copy = subprocess.run(
                    merge_command_copy,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                merge_mode = "copy"
                if completed_copy.returncode != 0:
                    merge_command_reencode = [
                        ffmpeg_bin,
                        "-y",
                        "-i",
                        str(media_path),
                        "-i",
                        str(donor_media_path),
                        "-map",
                        "0:v:0",
                        "-map",
                        "1:a:0",
                        "-c:v",
                        "libx264",
                        "-preset",
                        "veryfast",
                        "-crf",
                        "23",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
                        "-shortest",
                        "-movflags",
                        "+faststart",
                        str(merged_temp_path),
                    ]
                    completed_reencode = subprocess.run(
                        merge_command_reencode,
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    if completed_reencode.returncode != 0:
                        copy_message = summarize_ffmpeg_failure(completed_copy)
                        reencode_message = summarize_ffmpeg_failure(completed_reencode)
                        return (
                            None,
                            "ffmpeg merge failed "
                            f"(copy={copy_message}; reencode={reencode_message})",
                            None,
                            None,
                        )
                    merge_mode = "reencode"

                try:
                    if merged_output_path != media_path and merged_output_path.exists():
                        merged_output_path.unlink()
                    merged_temp_path.replace(merged_output_path)
                    if merged_output_path != media_path and media_path.exists():
                        media_path.unlink()
                except OSError as exc:
                    return None, f"failed to replace merged media file: {exc}", None, None

                if not merged_output_path.exists():
                    return None, "merged media file missing after audio fallback merge", None, None
                cleanup_donor_file = True
                return (
                    merged_output_path,
                    None,
                    f"{merge_mode}/{donor_source}:{donor_format}",
                    selected_download_format,
                )
            finally:
                if cleanup_donor_file and donor_media_path.exists():
                    try:
                        donor_media_path.unlink()
                    except OSError:
                        pass
                if merged_temp_path.exists():
                    try:
                        merged_temp_path.unlink()
                    except OSError:
                        pass

        if (
            connection is not None
            and not dry_run
            and media_run_id is not None
            and media_audio_target_ids
        ):
            update_download_run_progress(
                connection=connection,
                run_id=media_run_id,
                target_count=len(media_audio_target_ids),
                success_count=0,
                failure_count=0,
                error_message=f"audio_fallback 0/{len(media_audio_target_ids)}",
            )
            connection.commit()

        if not dry_run and media_audio_target_ids and ffprobe_bin is not None:
            for video_id in media_audio_target_ids:
                if video_id not in seen_evaluated_media_ids:
                    seen_evaluated_media_ids.add(video_id)
                    evaluated_media_ids.append(video_id)

                progress_note = ""
                try:
                    media_path = find_media_file_for_video(source, video_id)
                    if media_path is None or not media_path.exists():
                        media_path, primary_error = run_media_primary_download(video_id, None)
                        if primary_error is not None:
                            media_audio_fallback_failures[video_id] = primary_error
                            progress_note = "primary_failed"
                            print(
                                f"[media] {source.id}/{video_id}: primary download failed "
                                f"({primary_error})",
                                file=sys.stderr,
                            )
                            continue

                    has_audio_stream, probe_error = detect_audio_stream(
                        media_path=media_path,
                        ffprobe_bin=ffprobe_bin,
                    )
                    if has_audio_stream is True:
                        progress_note = "already_has_audio"
                        continue
                    if has_audio_stream is None:
                        progress_note = "ffprobe_warning"
                        print(
                            f"[media] {source.id}/{video_id}: ffprobe warning ({probe_error}); "
                            "skip audio fallback",
                            file=sys.stderr,
                        )
                        continue

                    (
                        media_path_after,
                        fallback_error,
                        fallback_merge_mode,
                        fallback_selected_format,
                    ) = run_media_audio_fallback_merge(video_id, media_path)
                    if fallback_error is not None:
                        media_audio_fallback_failures[video_id] = fallback_error
                        progress_note = "fallback_failed"
                        print(
                            f"[media] {source.id}/{video_id}: audio fallback failed "
                            f"({fallback_error})",
                            file=sys.stderr,
                        )
                        continue

                    has_audio_after, probe_error_after = detect_audio_stream(
                        media_path=media_path_after,
                        ffprobe_bin=ffprobe_bin,
                    )
                    if has_audio_after is True:
                        media_audio_fallback_repaired += 1
                        repaired_media_ids.add(video_id)
                        if (
                            connection is not None
                            and fallback_selected_format is not None
                        ):
                            record_media_fallback_preferred_format(
                                connection=connection,
                                source_id=source.id,
                                preferred_format=fallback_selected_format,
                            )
                            connection.commit()
                        progress_note = "recovered"
                        print(
                            f"[media] {source.id}/{video_id}: recovered audio stream "
                            f"(format={media_audio_fallback_format}+mux/"
                            f"{fallback_merge_mode or 'unknown'})"
                        )
                        continue

                    if has_audio_after is False:
                        media_audio_fallback_failures[video_id] = (
                            "audio fallback still has no audio stream"
                        )
                        progress_note = "fallback_no_audio"
                    else:
                        media_audio_fallback_failures[video_id] = (
                            f"audio fallback ffprobe warning: {probe_error_after or 'unknown'}"
                        )
                        progress_note = "fallback_probe_warning"
                    print(
                        f"[media] {source.id}/{video_id}: audio fallback did not produce audio",
                        file=sys.stderr,
                    )
                finally:
                    if connection is not None and media_run_id is not None:
                        processed_count = len(evaluated_media_ids)
                        failed_count = len(media_audio_fallback_failures)
                        success_count = max(0, processed_count - failed_count)
                        progress_label = (
                            f"audio_fallback {processed_count}/{len(media_audio_target_ids)}"
                        )
                        if progress_note:
                            progress_label = f"{progress_label} ({progress_note})"
                        update_download_run_progress(
                            connection=connection,
                            run_id=media_run_id,
                            target_count=len(media_audio_target_ids),
                            success_count=success_count,
                            failure_count=failed_count,
                            error_message=progress_label,
                        )
                        connection.commit()
        elif not dry_run and media_audio_target_ids and ffprobe_bin is None:
            print(
                "[media] ffprobe not found; skipping audio-stream validation for media retries",
                file=sys.stderr,
            )
            if connection is not None and media_run_id is not None:
                update_download_run_progress(
                    connection=connection,
                    run_id=media_run_id,
                    target_count=len(media_audio_target_ids),
                    success_count=0,
                    failure_count=0,
                    error_message="audio_fallback unavailable (ffprobe missing)",
                )
                connection.commit()

        if connection is not None and not dry_run:
            for video_id in evaluated_media_ids:
                fallback_error = media_audio_fallback_failures.get(video_id)
                if fallback_error is None:
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
                    continue

                current = connection.execute(
                    """
                    SELECT retry_count
                    FROM download_state
                    WHERE source_id = ? AND stage = ? AND video_id = ?
                    """,
                    (source.id, "media", video_id),
                ).fetchone()
                next_retry_count = (int(current[0]) if current else 0) + 1
                next_retry_at = schedule_next_retry_iso(
                    next_retry_count,
                    error_message=fallback_error,
                )
                upsert_download_state(
                    connection=connection,
                    source_id=source.id,
                    stage="media",
                    video_id=video_id,
                    status="error",
                    run_id=media_run_id,
                    attempt_at=media_started_at,
                    url=safe_video_url(video_id),
                    last_error=fallback_error,
                    retry_count=next_retry_count,
                    next_retry_at=next_retry_at,
                )
                blocked_until = extend_source_network_cooldown(
                    connection=connection,
                    source_id=source.id,
                    error_message=fallback_error,
                    blocked_until=next_retry_at,
                    blocked_at=media_started_at,
                )
                if blocked_until:
                    media_blocked_error = fallback_error
                    activate_source_network_cooldown(
                        blocked_until=blocked_until,
                        reason=fallback_error,
                    )

            if repaired_media_ids:
                repaired_at = now_utc_iso()
                for video_id in repaired_media_ids:
                    connection.execute(
                        """
                        UPDATE videos
                        SET audio_lufs = NULL,
                            audio_gain_db = NULL,
                            audio_loudness_analyzed_at = NULL,
                            audio_loudness_error = NULL,
                            synced_at = ?
                        WHERE source_id = ?
                          AND video_id = ?
                        """,
                        (repaired_at, source.id, video_id),
                    )

            media_failed_count = sum(
                1 for video_id in evaluated_media_ids
                if video_id in media_audio_fallback_failures
            )
            media_success_count = len(evaluated_media_ids) - media_failed_count
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
            media_finished_at = now_utc_iso()
            media_succeeded = media_exit_code == 0 and media_failed_count == 0
            if media_command is not None or media_audio_target_ids:
                record_source_network_access(
                    connection=connection,
                    source=source,
                    request_at=media_finished_at,
                    succeeded=media_succeeded,
                    clear_cooldown=media_succeeded,
                    last_error=(
                        None
                        if media_succeeded
                        else (
                            _SOURCE_ACCESS_UNSET
                            if media_blocked_error
                            else media_stage_error
                        )
                    ),
                )
            finish_download_run(
                connection=connection,
                run_id=media_run_id if media_run_id is not None else -1,
                status=(
                    "success"
                    if media_exit_code == 0 and media_failed_count == 0
                    else "error"
                ),
                finished_at=media_finished_at,
                exit_code=media_exit_code,
                success_count=media_success_count,
                failure_count=media_failed_count,
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
        print(
            f"media new IDs: {len(new_media_ids)} "
            f"(retry={len(retry_media_ids)}, bootstrap={len(bootstrap_no_audio_media_ids)})"
        )
    else:
        if skip_media:
            print("skip media")
        elif source_cooldown_active:
            print(
                f"skip media (source network cooldown until {source_cooldown_until or 'unknown'})"
            )
        else:
            print(
                f"skip media (metered updates-only; archive_count<{max(0, int(metered_min_archive_ids))})"
            )

    if not skip_subs:
        subs_plan = prepare_subtitle_download_plan(
            source=source,
            dry_run=dry_run,
            connection=connection,
            metadata_candidate_ids=metadata_candidate_ids,
            new_media_ids=new_media_ids,
            strict_candidate_scope=strict_candidate_scope,
            active_urls_file=active_urls_file,
            cookie_flags=cookie_flags,
            impersonate_flags=impersonate_flags,
            safe_video_url=safe_video_url,
            source_cooldown_active=source_cooldown_active,
            source_cooldown_until=source_cooldown_until,
            limit=stage_limit,
            suppress_skip_log=suppress_skip_subs_log,
        )
        if subs_plan is not None:
            run_interleaved_chunked_ytdlp_stage_plans([subs_plan], dry_run=dry_run)
            finalize_subtitle_download_plan(
                subs_plan,
                dry_run=dry_run,
                connection=connection,
                safe_video_url=safe_video_url,
                activate_source_network_cooldown=activate_source_network_cooldown,
            )
    elif not suppress_skip_subs_log:
        print("skip subtitles")

    if skip_meta:
        if not suppress_skip_meta_log:
            print("skip metadata")
        return SyncSourceRunResult(new_media_ids=new_media_ids)

    meta_plan = prepare_metadata_download_plan(
        source=source,
        dry_run=dry_run,
        connection=connection,
        metadata_candidate_ids=metadata_candidate_ids,
        strict_candidate_scope=strict_candidate_scope,
        active_urls_file=active_urls_file,
        cookie_flags=cookie_flags,
        impersonate_flags=impersonate_flags,
        safe_video_url=safe_video_url,
        source_cooldown_active=source_cooldown_active,
        source_cooldown_until=source_cooldown_until,
        limit=stage_limit,
        suppress_skip_log=suppress_skip_meta_log,
    )
    if meta_plan is not None:
        run_interleaved_chunked_ytdlp_stage_plans([meta_plan], dry_run=dry_run)
        finalize_metadata_download_plan(
            meta_plan,
            dry_run=dry_run,
            connection=connection,
            safe_video_url=safe_video_url,
            activate_source_network_cooldown=activate_source_network_cooldown,
        )
    return SyncSourceRunResult(new_media_ids=new_media_ids)


def run_legacy_sync_sources(
    *,
    sources: list[SourceConfig],
    dry_run: bool,
    skip_media: bool,
    skip_subs: bool,
    skip_meta: bool,
    connection: sqlite3.Connection | None,
    metered_media_mode: str,
    metered_min_archive_ids: int,
    metered_playlist_end: int,
    limit: int | None = None,
) -> None:
    source_results: dict[str, SyncSourceRunResult] = {}
    for source in sources:
        source_results[source.id] = sync_source(
            source=source,
            dry_run=dry_run,
            skip_media=skip_media,
            skip_subs=True,
            skip_meta=True,
            connection=connection,
            metered_media_mode=metered_media_mode,
            metered_min_archive_ids=metered_min_archive_ids,
            metered_playlist_end=metered_playlist_end,
            suppress_skip_subs_log=not skip_subs,
            suppress_skip_meta_log=not skip_meta,
        )

    if not skip_subs:
        subtitle_plans: list[ChunkedYtdlpStagePlan] = []
        remaining_subs_limit = None if limit is None else max(0, int(limit))
        for source in sources:
            if remaining_subs_limit == 0:
                break
            source_cooldown_active = False
            source_cooldown_until: str | None = None
            if connection is not None and not dry_run:
                (
                    source_cooldown_active,
                    _remaining_hours,
                    source_cooldown_until,
                    _source_cooldown_reason,
                ) = get_source_network_cooldown_state(
                    connection=connection,
                    source_id=source.id,
                )

            def safe_video_url(video_id: str, *, _source: SourceConfig = source) -> str | None:
                try:
                    return build_video_url(_source, video_id)
                except ValueError:
                    return None

            subtitle_plan = prepare_subtitle_download_plan(
                source=source,
                dry_run=dry_run,
                connection=connection,
                metadata_candidate_ids=None,
                new_media_ids=source_results.get(source.id, SyncSourceRunResult()).new_media_ids,
                strict_candidate_scope=False,
                active_urls_file=build_run_local_urls_file(source),
                cookie_flags=resolve_cookie_flags(source),
                impersonate_flags=resolve_impersonate_flags(source),
                safe_video_url=safe_video_url,
                source_cooldown_active=source_cooldown_active,
                source_cooldown_until=source_cooldown_until,
                limit=remaining_subs_limit,
            )
            if subtitle_plan is not None:
                subtitle_plans.append(subtitle_plan)
                if remaining_subs_limit is not None:
                    remaining_subs_limit = max(0, remaining_subs_limit - len(subtitle_plan.target_ids))

        run_interleaved_chunked_ytdlp_stage_plans(subtitle_plans, dry_run=dry_run)
        for plan in subtitle_plans:
            def safe_video_url(video_id: str, *, _source: SourceConfig = plan.source) -> str | None:
                try:
                    return build_video_url(_source, video_id)
                except ValueError:
                    return None

            finalize_subtitle_download_plan(
                plan,
                dry_run=dry_run,
                connection=connection,
                safe_video_url=safe_video_url,
            )

    if not skip_meta:
        metadata_plans: list[ChunkedYtdlpStagePlan] = []
        remaining_meta_limit = None if limit is None else max(0, int(limit))
        for source in sources:
            if remaining_meta_limit == 0:
                break
            source_cooldown_active = False
            source_cooldown_until: str | None = None
            if connection is not None and not dry_run:
                (
                    source_cooldown_active,
                    _remaining_hours,
                    source_cooldown_until,
                    _source_cooldown_reason,
                ) = get_source_network_cooldown_state(
                    connection=connection,
                    source_id=source.id,
                )

            def safe_video_url(video_id: str, *, _source: SourceConfig = source) -> str | None:
                try:
                    return build_video_url(_source, video_id)
                except ValueError:
                    return None

            metadata_plan = prepare_metadata_download_plan(
                source=source,
                dry_run=dry_run,
                connection=connection,
                metadata_candidate_ids=None,
                strict_candidate_scope=False,
                active_urls_file=build_run_local_urls_file(source),
                cookie_flags=resolve_cookie_flags(source),
                impersonate_flags=resolve_impersonate_flags(source),
                safe_video_url=safe_video_url,
                source_cooldown_active=source_cooldown_active,
                source_cooldown_until=source_cooldown_until,
                limit=remaining_meta_limit,
            )
            if metadata_plan is not None:
                metadata_plans.append(metadata_plan)
                if remaining_meta_limit is not None:
                    remaining_meta_limit = max(0, remaining_meta_limit - len(metadata_plan.target_ids))

        run_interleaved_chunked_ytdlp_stage_plans(metadata_plans, dry_run=dry_run)
        for plan in metadata_plans:
            def safe_video_url(video_id: str, *, _source: SourceConfig = plan.source) -> str | None:
                try:
                    return build_video_url(_source, video_id)
                except ValueError:
                    return None

            finalize_metadata_download_plan(
                plan,
                dry_run=dry_run,
                connection=connection,
                safe_video_url=safe_video_url,
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


def scan_existing_subtitle_ids(
    source: SourceConfig,
    *,
    match_langs: str | None = None,
    origin_kind: str | None = None,
) -> set[str]:
    subtitles = scan_subtitles(source)
    if not match_langs and not origin_kind:
        return set(subtitles.keys())

    expected_origin_kind = (
        normalize_subtitle_origin_kind(origin_kind)
        if origin_kind not in (None, "")
        else None
    )
    matching_ids: set[str] = set()
    for video_id, tracks in subtitles.items():
        for language, subtitle_path, _extension in tracks:
            detected_origin_kind, _origin_detail = classify_subtitle_origin(
                language,
                subtitle_path,
            )
            if (
                expected_origin_kind is not None
                and normalize_subtitle_origin_kind(detected_origin_kind) != expected_origin_kind
            ):
                continue
            if match_langs and not subtitle_language_matches_sub_langs(language, match_langs):
                continue
            matching_ids.add(video_id)
            break
    return matching_ids


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


def normalize_subtitle_origin_kind(value: Any, fallback: str = "upstream") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"upstream", "generated"}:
        return normalized
    safe_fallback = str(fallback or "upstream").strip().lower()
    return safe_fallback if safe_fallback in {"upstream", "generated"} else "upstream"


def build_translation_output_origin_lookup(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
) -> dict[str, tuple[str, str]]:
    rows = connection.execute(
        """
        SELECT output_path, agent, method_version
        FROM translation_runs
        WHERE source_id = ?
          AND video_id = ?
          AND output_path IS NOT NULL
          AND output_path <> ''
        ORDER BY run_id DESC
        """,
        (source_id, video_id),
    ).fetchall()
    lookup: dict[str, tuple[str, str]] = {}
    for row in rows:
        if isinstance(row, sqlite3.Row):
            output_path_value = row["output_path"]
            agent_value = row["agent"]
            method_version_value = row["method_version"]
        else:
            output_path_value = row[0] if len(row) > 0 else None
            agent_value = row[1] if len(row) > 1 else None
            method_version_value = row[2] if len(row) > 2 else None
        output_path = str(output_path_value or "").strip()
        if not output_path or output_path in lookup:
            continue
        agent = str(agent_value or "").strip().lower()
        method_version = str(method_version_value or "").strip().lower()
        if "source-track=asr" in method_version:
            origin_detail = "translate-local-asr" if agent == "local-llm" else "generated-asr"
        elif agent == "local-llm":
            origin_detail = "translate-local"
        elif agent:
            origin_detail = f"generated:{agent}"
        else:
            origin_detail = "generated"
        lookup[output_path] = ("generated", origin_detail)
    return lookup


def classify_subtitle_origin(
    language: str | None,
    subtitle_path: Path,
    translation_output_origin_lookup: dict[str, tuple[str, str]] | None = None,
) -> tuple[str, str]:
    subtitle_key = str(subtitle_path)
    if translation_output_origin_lookup and subtitle_key in translation_output_origin_lookup:
        return translation_output_origin_lookup[subtitle_key]

    normalized_language = str(language or "").strip().lower()
    path_value = subtitle_key.strip().lower()
    if (
        normalized_language == "ja-asr-local"
        or normalized_language.startswith("ja-asr-local-")
        or ".ja-asr-local." in path_value
    ):
        return ("generated", "translate-local-asr")
    if (
        normalized_language == "ja-local"
        or normalized_language.startswith("ja-local-")
        or ".ja-local." in path_value
    ):
        return ("generated", "translate-local")
    return ("upstream", "tiktok")


def format_subtitle_track_origin_label(origin_kind: str, origin_detail: str) -> str:
    safe_kind = normalize_subtitle_origin_kind(origin_kind, "upstream")
    safe_detail = str(origin_detail or "").strip().lower()
    if safe_kind == "generated":
        if safe_detail == "translate-local-asr":
            return "Generated/ASR"
        if safe_detail == "translate-local":
            return "Generated"
        return "Generated"
    return "Upstream"


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
            origin_kind TEXT NOT NULL DEFAULT 'upstream',
            origin_detail TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS media_fallback_format_state (
            source_id TEXT PRIMARY KEY,
            preferred_format TEXT NOT NULL,
            success_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_media_fallback_format_state_updated_at
            ON media_fallback_format_state(updated_at);

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

        CREATE TABLE IF NOT EXISTS work_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            video_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            priority INTEGER NOT NULL DEFAULT 100,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT,
            lease_owner TEXT,
            lease_token TEXT,
            lease_expires_at TEXT,
            last_error TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            UNIQUE (source_id, stage, video_id),
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_work_items_status_retry
            ON work_items(status, next_retry_at, priority, updated_at);
        CREATE INDEX IF NOT EXISTS idx_work_items_source_stage
            ON work_items(source_id, stage, status, updated_at);

        CREATE TABLE IF NOT EXISTS source_poll_state (
            source_id TEXT PRIMARY KEY,
            last_poll_at TEXT,
            next_poll_at TEXT,
            poll_interval_hours REAL NOT NULL DEFAULT 24,
            last_poll_status TEXT,
            last_error TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_source_poll_state_next
            ON source_poll_state(next_poll_at, updated_at);

        CREATE TABLE IF NOT EXISTS source_access_state (
            source_id TEXT PRIMARY KEY,
            blocked_until TEXT,
            last_blocked_at TEXT,
            last_error TEXT,
            last_request_at TEXT,
            next_request_not_before TEXT,
            last_success_at TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_source_access_state_blocked_until
            ON source_access_state(blocked_until, updated_at);

        CREATE INDEX IF NOT EXISTS idx_source_access_state_next_request
            ON source_access_state(next_request_not_before, updated_at);

        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id TEXT PRIMARY KEY,
            host TEXT,
            pid INTEGER,
            started_at TEXT NOT NULL,
            last_heartbeat_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS video_favorites (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS video_dislikes (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS video_not_interested (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_id, video_id),
            FOREIGN KEY (source_id, video_id) REFERENCES videos(source_id, video_id)
        );

        CREATE TABLE IF NOT EXISTS video_playback_stats (
            source_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            impression_count INTEGER NOT NULL DEFAULT 0,
            play_count INTEGER NOT NULL DEFAULT 0,
            total_watch_seconds REAL NOT NULL DEFAULT 0,
            completed_count INTEGER NOT NULL DEFAULT 0,
            fast_skip_count INTEGER NOT NULL DEFAULT 0,
            shallow_skip_count INTEGER NOT NULL DEFAULT 0,
            last_served_at TEXT,
            last_played_at TEXT,
            last_completed_at TEXT,
            last_position_seconds REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_video_dislikes_created_at
            ON video_dislikes(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_not_interested_created_at
            ON video_not_interested(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_playback_stats_last_played
            ON video_playback_stats(last_played_at DESC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_subtitle_bookmarks_lookup
            ON subtitle_bookmarks(source_id, video_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_video_notes_lookup
            ON video_notes(source_id, video_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_dictionary_bookmarks_lookup
            ON dictionary_bookmarks(source_id, video_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS app_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    ensure_videos_loudness_columns(connection)
    ensure_source_access_state_columns(connection)
    ensure_video_playback_stats_columns(connection)
    ensure_dictionary_schema(connection)
    ensure_translation_runs_table(connection)
    ensure_translation_stage_runs_table(connection)
    ensure_subtitles_origin_columns(connection)
    ensure_dictionary_bookmarks_schema(connection)
    ensure_dictionary_import_runs_table(connection)


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


def ensure_source_access_state_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(source_access_state)").fetchall()
    if not rows:
        return
    existing_columns = {str(row[1]) for row in rows}
    required_columns = {
        "last_request_at": "TEXT",
        "next_request_not_before": "TEXT",
        "last_success_at": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE source_access_state ADD COLUMN {column_name} {column_type}"
        )


def ensure_video_playback_stats_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(video_playback_stats)").fetchall()
    if not rows:
        return
    existing_columns = {str(row[1]) for row in rows}
    required_columns = {
        "impression_count": "INTEGER NOT NULL DEFAULT 0",
        "fast_skip_count": "INTEGER NOT NULL DEFAULT 0",
        "shallow_skip_count": "INTEGER NOT NULL DEFAULT 0",
        "last_served_at": "TEXT",
        "last_completed_at": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE video_playback_stats ADD COLUMN {column_name} {column_type}"
        )


def ensure_subtitles_origin_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(subtitles)").fetchall()
    if not rows:
        return
    existing_columns = {str(row[1]) for row in rows}
    required_columns = {
        "origin_kind": "TEXT NOT NULL DEFAULT 'upstream'",
        "origin_detail": "TEXT NOT NULL DEFAULT ''",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE subtitles ADD COLUMN {column_name} {column_type}"
        )

    connection.execute(
        """
        UPDATE subtitles
        SET origin_kind = 'generated',
            origin_detail = 'translate-local'
        WHERE (
                LOWER(COALESCE(language, '')) = 'ja-local'
             OR LOWER(COALESCE(language, '')) LIKE 'ja-local-%'
             OR LOWER(COALESCE(subtitle_path, '')) LIKE '%.ja-local.%'
        )
        """
    )
    connection.execute(
        """
        UPDATE subtitles
        SET origin_kind = 'generated',
            origin_detail = 'translate-local-asr'
        WHERE (
                LOWER(COALESCE(language, '')) = 'ja-asr-local'
             OR LOWER(COALESCE(language, '')) LIKE 'ja-asr-local-%'
             OR LOWER(COALESCE(subtitle_path, '')) LIKE '%.ja-asr-local.%'
        )
        """
    )
    connection.execute(
        """
        UPDATE subtitles
        SET origin_kind = 'generated',
            origin_detail = (
                SELECT CASE
                    WHEN LOWER(COALESCE(tr.method_version, '')) LIKE '%source-track=asr%'
                    THEN CASE
                        WHEN LOWER(COALESCE(tr.agent, '')) = 'local-llm' THEN 'translate-local-asr'
                        ELSE 'generated-asr'
                    END
                    ELSE CASE
                        WHEN LOWER(COALESCE(tr.agent, '')) = 'local-llm' THEN 'translate-local'
                        WHEN TRIM(COALESCE(tr.agent, '')) != '' THEN 'generated:' || LOWER(TRIM(tr.agent))
                        ELSE 'generated'
                    END
                END
                FROM translation_runs tr
                WHERE tr.source_id = subtitles.source_id
                  AND tr.video_id = subtitles.video_id
                  AND tr.output_path = subtitles.subtitle_path
                ORDER BY tr.run_id DESC
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM translation_runs tr
            WHERE tr.source_id = subtitles.source_id
              AND tr.video_id = subtitles.video_id
              AND tr.output_path = subtitles.subtitle_path
        )
        """
    )
    connection.execute(
        """
        UPDATE subtitles
        SET origin_kind = 'upstream'
        WHERE TRIM(COALESCE(origin_kind, '')) = ''
        """
    )
    connection.execute(
        """
        UPDATE subtitles
        SET origin_detail = 'tiktok'
        WHERE LOWER(COALESCE(origin_kind, '')) = 'upstream'
          AND TRIM(COALESCE(origin_detail, '')) = ''
        """
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


def ensure_translation_stage_runs_table(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS translation_stage_runs (
            stage_run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            translation_run_id INTEGER,
            source_id         TEXT NOT NULL,
            video_id          TEXT NOT NULL,
            stage_name        TEXT NOT NULL,
            model             TEXT NOT NULL,
            input_cue_count   INTEGER NOT NULL DEFAULT 0,
            changed_cue_count INTEGER NOT NULL DEFAULT 0,
            request_count     INTEGER NOT NULL DEFAULT 0,
            prompt_tokens     INTEGER,
            completion_tokens INTEGER,
            total_tokens      INTEGER,
            elapsed_ms        INTEGER,
            status            TEXT NOT NULL DEFAULT 'completed',
            error_message     TEXT,
            started_at        TEXT,
            finished_at       TEXT,
            created_at        TEXT NOT NULL,
            FOREIGN KEY (translation_run_id) REFERENCES translation_runs(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_translation_stage_runs_lookup
            ON translation_stage_runs(source_id, video_id, stage_name, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_translation_stage_runs_run_id
            ON translation_stage_runs(translation_run_id, stage_run_id);
        """
    )


def record_translation_run(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
    source_path: Path,
    output_path: Path,
    cue_count: int,
    cue_match: bool,
    agent: str,
    method: str,
    method_version: str,
    summary: str,
    source_lang: str = "en",
    target_lang: str = "ja",
    status: str = "active",
    started_at: str | None = None,
    finished_at: str | None = None,
) -> int:
    safe_source_id = str(source_id).strip()
    safe_video_id = str(video_id).strip()
    safe_source_lang = str(source_lang or "en").strip().lower() or "en"
    safe_target_lang = str(target_lang or "ja").strip().lower() or "ja"
    safe_status = str(status or "active").strip().lower() or "active"
    now_iso = now_utc_iso()
    started_iso = str(started_at or now_iso)
    finished_iso = str(finished_at or now_iso)
    created_iso = now_iso

    if safe_status == "active":
        connection.execute(
            """
            UPDATE translation_runs
            SET status = 'superseded'
            WHERE source_id = ?
              AND video_id = ?
              AND target_lang = ?
              AND status = 'active'
            """,
            (safe_source_id, safe_video_id, safe_target_lang),
        )

    cursor = connection.execute(
        """
        INSERT INTO translation_runs (
            source_id,
            video_id,
            source_lang,
            target_lang,
            source_path,
            output_path,
            cue_count,
            cue_match,
            agent,
            method,
            method_version,
            summary,
            status,
            started_at,
            finished_at,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            safe_source_id,
            safe_video_id,
            safe_source_lang,
            safe_target_lang,
            str(source_path),
            str(output_path),
            max(0, int(cue_count)),
            1 if cue_match else 0,
            str(agent or "").strip(),
            str(method or "").strip(),
            str(method_version or "").strip(),
            str(summary or "").strip(),
            safe_status,
            started_iso,
            finished_iso,
            created_iso,
        ),
    )
    return int(cursor.lastrowid)


def record_translation_stage_metrics(
    connection: sqlite3.Connection,
    translation_run_id: int,
    source_id: str,
    video_id: str,
    stage_metrics: TranslationStageMetrics,
) -> None:
    created_iso = now_utc_iso()
    connection.execute(
        """
        INSERT INTO translation_stage_runs (
            translation_run_id,
            source_id,
            video_id,
            stage_name,
            model,
            input_cue_count,
            changed_cue_count,
            request_count,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            elapsed_ms,
            status,
            error_message,
            started_at,
            finished_at,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(translation_run_id),
            str(source_id),
            str(video_id),
            str(stage_metrics.stage_name),
            str(stage_metrics.model),
            max(0, int(stage_metrics.input_cue_count)),
            max(0, int(stage_metrics.changed_cue_count)),
            max(0, int(stage_metrics.request_count)),
            max(0, int(stage_metrics.prompt_tokens)),
            max(0, int(stage_metrics.completion_tokens)),
            max(0, int(stage_metrics.total_tokens)),
            max(0, int(stage_metrics.elapsed_ms)),
            str(stage_metrics.status or "completed"),
            str(stage_metrics.error_message or ""),
            str(stage_metrics.started_at or ""),
            str(stage_metrics.finished_at or ""),
            created_iso,
        ),
    )


def ensure_dictionary_import_runs_table(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS dictionary_import_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_scope TEXT NOT NULL DEFAULT '',
            input_path TEXT NOT NULL,
            input_format TEXT NOT NULL,
            on_duplicate TEXT NOT NULL,
            dry_run INTEGER NOT NULL DEFAULT 0,
            row_count INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'completed',
            error_message TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dictionary_import_runs_time
            ON dictionary_import_runs(finished_at DESC, run_id DESC);
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


def get_media_fallback_preferred_format(
    connection: sqlite3.Connection,
    source_id: str,
) -> str | None:
    row = connection.execute(
        """
        SELECT preferred_format
        FROM media_fallback_format_state
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    if row is None:
        return None
    value = str(row[0] or "").strip()
    return value or None


def record_media_fallback_preferred_format(
    connection: sqlite3.Connection,
    source_id: str,
    preferred_format: str,
    updated_at: str | None = None,
) -> None:
    value = str(preferred_format or "").strip()
    if not value:
        return
    timestamp = updated_at or now_utc_iso()
    connection.execute(
        """
        INSERT INTO media_fallback_format_state (
            source_id,
            preferred_format,
            success_count,
            updated_at
        ) VALUES (?, ?, 1, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            preferred_format = excluded.preferred_format,
            success_count = CASE
                WHEN media_fallback_format_state.preferred_format = excluded.preferred_format
                THEN media_fallback_format_state.success_count + 1
                ELSE 1
            END,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            value,
            timestamp,
        ),
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
    translation_output_origin_lookup = build_translation_output_origin_lookup(
        connection=connection,
        source_id=source.id,
        video_id=video_id,
    )
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
        origin_kind, origin_detail = classify_subtitle_origin(
            language=language,
            subtitle_path=subtitle_path,
            translation_output_origin_lookup=translation_output_origin_lookup,
        )
        connection.execute(
            """
            INSERT INTO subtitles (
                source_id,
                video_id,
                language,
                subtitle_path,
                origin_kind,
                origin_detail,
                ext
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source.id,
                video_id,
                language,
                str(subtitle_path),
                origin_kind,
                origin_detail,
                extension,
            ),
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
    for table_name in (
        "video_favorites",
        "video_dislikes",
        "video_not_interested",
        "video_playback_stats",
        "subtitle_bookmarks",
        "video_notes",
    ):
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

    connection = sqlite3.connect(str(db_path), timeout=30)
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


def is_blocked_or_forbidden_error(error_message: str | None) -> bool:
    text = str(error_message or "").strip()
    if not text:
        return False
    if RE_RETRY_TIKTOK_BLOCKED.search(text) is not None:
        return True
    lowered = text.lower()
    return (
        "tiktok" in lowered
        and "403" in lowered
        and "forbidden" in lowered
    )


def is_tiktok_transient_webpage_error(error_message: str | None) -> bool:
    text = str(error_message or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return (
        "tiktok" in lowered
        and RE_RETRY_TIKTOK_WEBPAGE_TRANSIENT.search(text) is not None
    )


def is_missing_artifact_error(error_message: str | None) -> bool:
    text = str(error_message or "").strip()
    if not text:
        return False
    return RE_RETRY_MISSING_ARTIFACT.search(text) is not None


def is_source_network_cooldown_error(error_message: str | None) -> bool:
    return is_blocked_or_forbidden_error(error_message)


def schedule_next_retry_iso(
    retry_count: int,
    error_message: str | None = None,
) -> str:
    if is_blocked_or_forbidden_error(error_message):
        # More conservative cool-off for likely IP blocks / hard 403s.
        blocked_backoff_seconds = (21600, 43200, 86400, 129600, 172800)
        index = min(max(0, retry_count - 1), len(blocked_backoff_seconds) - 1)
        delay_seconds = blocked_backoff_seconds[index]
    elif is_tiktok_transient_webpage_error(error_message):
        # TikTok webpage extractor misses often recover after a shorter source-level cool-off.
        transient_backoff_seconds = (1800, 3600, 7200, 14400, 21600)
        index = min(max(0, retry_count - 1), len(transient_backoff_seconds) - 1)
        delay_seconds = transient_backoff_seconds[index]
    elif is_missing_artifact_error(error_message):
        # Structural misses (missing subtitle/meta artifacts) should not spin quickly.
        missing_artifact_backoff_seconds = (1800, 7200, 21600, 43200, 86400)
        index = min(max(0, retry_count - 1), len(missing_artifact_backoff_seconds) - 1)
        delay_seconds = missing_artifact_backoff_seconds[index]
    else:
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


def update_download_run_progress(
    connection: sqlite3.Connection,
    run_id: int,
    target_count: int | None = None,
    success_count: int | None = None,
    failure_count: int | None = None,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE download_runs
        SET target_count = COALESCE(?, target_count),
            success_count = COALESCE(?, success_count),
            failure_count = COALESCE(?, failure_count),
            error_message = COALESCE(?, error_message)
        WHERE run_id = ?
          AND status = 'running'
        """,
        (
            target_count,
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


def mark_media_retry_state(
    connection: sqlite3.Connection,
    source: SourceConfig,
    video_id: str,
    reason: str,
    run_id: int | None = None,
    attempt_at: str | None = None,
) -> tuple[int, str]:
    attempt_value = attempt_at or now_utc_iso()
    current = connection.execute(
        """
        SELECT status, retry_count, next_retry_at
        FROM download_state
        WHERE source_id = ? AND stage = 'media' AND video_id = ?
        """,
        (source.id, video_id),
    ).fetchone()
    current_status = str(current[0]) if current else ""
    current_retry_count = int(current[1]) if current else 0
    current_next_retry = (
        str(current[2])
        if current is not None and current[2] not in (None, "")
        else None
    )

    if (
        current_status == "error"
        and current_next_retry is not None
        and current_next_retry > attempt_value
    ):
        next_retry_count = current_retry_count
        next_retry_at = current_next_retry
    else:
        next_retry_count = current_retry_count + 1
        next_retry_at = schedule_next_retry_iso(
            next_retry_count,
            error_message=reason,
        )

    video_url: str | None = None
    try:
        video_url = build_video_url(source, video_id)
    except ValueError:
        video_url = None

    upsert_download_state(
        connection=connection,
        source_id=source.id,
        stage="media",
        video_id=video_id,
        status="error",
        run_id=run_id,
        attempt_at=attempt_value,
        url=video_url,
        last_error=reason,
        retry_count=next_retry_count,
        next_retry_at=next_retry_at,
    )
    extend_source_network_cooldown(
        connection=connection,
        source_id=source.id,
        error_message=reason,
        blocked_until=next_retry_at,
        blocked_at=attempt_value,
    )
    return next_retry_count, next_retry_at


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


def compute_next_poll_at_iso(
    poll_interval_hours: float,
    from_dt: dt.datetime | None = None,
) -> str:
    safe_hours = max(0.0, float(poll_interval_hours))
    base_dt = from_dt or dt.datetime.now(dt.timezone.utc)
    next_dt = base_dt + dt.timedelta(hours=safe_hours)
    return next_dt.replace(microsecond=0).isoformat()


def get_source_network_cooldown_state(
    connection: sqlite3.Connection,
    source_id: str,
    now_dt: dt.datetime | None = None,
) -> tuple[bool, float, str | None, str | None]:
    now_value = now_dt or dt.datetime.now(dt.timezone.utc)
    row = connection.execute(
        """
        SELECT blocked_until, COALESCE(last_error, '')
        FROM source_access_state
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    if row is None or row[0] in (None, ""):
        return False, 0.0, None, None

    blocked_until_text = str(row[0])
    blocked_until_dt = parse_iso_datetime_utc(blocked_until_text)
    last_error = str(row[1] or "").strip() or None
    if last_error and is_tiktok_transient_webpage_error(last_error):
        upsert_source_access_state(
            connection=connection,
            source_id=source_id,
            blocked_until=None,
            updated_at=now_value.replace(microsecond=0).isoformat(),
        )
        connection.commit()
        return False, 0.0, None, last_error
    if blocked_until_dt is None or now_value >= blocked_until_dt:
        return False, 0.0, blocked_until_text, last_error

    remaining_hours = max(0.0, (blocked_until_dt - now_value).total_seconds() / 3600.0)
    return True, remaining_hours, blocked_until_text, last_error


def get_source_network_spacing_state(
    connection: sqlite3.Connection,
    source_id: str,
    now_dt: dt.datetime | None = None,
) -> tuple[bool, float, str | None]:
    now_value = now_dt or dt.datetime.now(dt.timezone.utc)
    row = connection.execute(
        """
        SELECT next_request_not_before
        FROM source_access_state
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    if row is None or row[0] in (None, ""):
        return False, 0.0, None

    next_request_text = str(row[0])
    next_request_dt = parse_iso_datetime_utc(next_request_text)
    if next_request_dt is None or now_value >= next_request_dt:
        return False, 0.0, next_request_text

    remaining_seconds = max(0.0, (next_request_dt - now_value).total_seconds())
    return True, remaining_seconds, next_request_text


def compute_source_network_min_interval_seconds(source: SourceConfig) -> int:
    return max(0, int(source.sleep_interval))


def compute_source_next_request_not_before_iso(
    source: SourceConfig,
    from_dt: dt.datetime | None = None,
) -> str | None:
    delay_seconds = compute_source_network_min_interval_seconds(source)
    if delay_seconds <= 0:
        return None
    base_dt = from_dt or dt.datetime.now(dt.timezone.utc)
    next_dt = base_dt + dt.timedelta(seconds=delay_seconds)
    return next_dt.replace(microsecond=0).isoformat()


def upsert_source_access_state(
    connection: sqlite3.Connection,
    source_id: str,
    blocked_until: Any = _SOURCE_ACCESS_UNSET,
    last_blocked_at: Any = _SOURCE_ACCESS_UNSET,
    last_error: Any = _SOURCE_ACCESS_UNSET,
    last_request_at: Any = _SOURCE_ACCESS_UNSET,
    next_request_not_before: Any = _SOURCE_ACCESS_UNSET,
    last_success_at: Any = _SOURCE_ACCESS_UNSET,
    updated_at: str | None = None,
) -> None:
    updated_at_value = str(updated_at or now_utc_iso())
    current = connection.execute(
        """
        SELECT
            blocked_until,
            last_blocked_at,
            last_error,
            last_request_at,
            next_request_not_before,
            last_success_at
        FROM source_access_state
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    current_values = (
        current if current is not None else (None, None, None, None, None, None)
    )
    blocked_until_value = current_values[0] if blocked_until is _SOURCE_ACCESS_UNSET else blocked_until
    last_blocked_at_value = (
        current_values[1] if last_blocked_at is _SOURCE_ACCESS_UNSET else last_blocked_at
    )
    last_error_value = current_values[2] if last_error is _SOURCE_ACCESS_UNSET else last_error
    last_request_at_value = (
        current_values[3] if last_request_at is _SOURCE_ACCESS_UNSET else last_request_at
    )
    next_request_not_before_value = (
        current_values[4]
        if next_request_not_before is _SOURCE_ACCESS_UNSET
        else next_request_not_before
    )
    last_success_at_value = (
        current_values[5] if last_success_at is _SOURCE_ACCESS_UNSET else last_success_at
    )
    connection.execute(
        """
        INSERT INTO source_access_state (
            source_id,
            blocked_until,
            last_blocked_at,
            last_error,
            last_request_at,
            next_request_not_before,
            last_success_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            blocked_until = excluded.blocked_until,
            last_blocked_at = excluded.last_blocked_at,
            last_error = excluded.last_error,
            last_request_at = excluded.last_request_at,
            next_request_not_before = excluded.next_request_not_before,
            last_success_at = excluded.last_success_at,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            blocked_until_value,
            last_blocked_at_value,
            last_error_value,
            last_request_at_value,
            next_request_not_before_value,
            last_success_at_value,
            updated_at_value,
        ),
    )


def extend_source_network_cooldown(
    connection: sqlite3.Connection,
    source_id: str,
    error_message: str | None,
    blocked_until: str | None = None,
    blocked_at: str | None = None,
) -> str | None:
    if not is_source_network_cooldown_error(error_message):
        return None

    blocked_at_value = str(blocked_at or now_utc_iso())
    next_blocked_until = str(
        blocked_until
        or schedule_next_retry_iso(
            1,
            error_message=error_message,
        )
    )
    current = connection.execute(
        """
        SELECT blocked_until
        FROM source_access_state
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()
    if current is not None and current[0] not in (None, ""):
        current_blocked_until = str(current[0])
        if current_blocked_until > next_blocked_until:
            next_blocked_until = current_blocked_until

    upsert_source_access_state(
        connection=connection,
        source_id=source_id,
        blocked_until=next_blocked_until,
        last_blocked_at=blocked_at_value,
        last_error=error_message,
        updated_at=blocked_at_value,
    )
    return next_blocked_until


def record_source_network_access(
    connection: sqlite3.Connection,
    source: SourceConfig,
    request_at: str | None = None,
    succeeded: bool = False,
    clear_cooldown: bool = False,
    last_error: str | None | object = _SOURCE_ACCESS_UNSET,
) -> str | None:
    request_at_value = str(request_at or now_utc_iso())
    request_dt = parse_iso_datetime_utc(request_at_value)
    if request_dt is None:
        request_dt = dt.datetime.now(dt.timezone.utc)
        request_at_value = request_dt.replace(microsecond=0).isoformat()
    next_request_not_before = compute_source_next_request_not_before_iso(
        source,
        from_dt=request_dt,
    )
    blocked_until: Any = _SOURCE_ACCESS_UNSET
    if clear_cooldown:
        blocked_until = None
    success_value: Any = _SOURCE_ACCESS_UNSET
    if succeeded:
        success_value = request_at_value
    upsert_source_access_state(
        connection=connection,
        source_id=source.id,
        blocked_until=blocked_until,
        last_error=last_error,
        last_request_at=request_at_value,
        next_request_not_before=next_request_not_before,
        last_success_at=success_value,
        updated_at=request_at_value,
    )
    return next_request_not_before


def should_poll_source_discovery(
    connection: sqlite3.Connection,
    source: SourceConfig,
    now_dt: dt.datetime | None = None,
) -> tuple[bool, float, str | None]:
    safe_interval_hours = max(0.0, float(source.media_discovery_interval_hours))
    if safe_interval_hours <= 0:
        return True, 0.0, None

    now_value = now_dt or dt.datetime.now(dt.timezone.utc)
    row = connection.execute(
        """
        SELECT next_poll_at
        FROM source_poll_state
        WHERE source_id = ?
        """,
        (source.id,),
    ).fetchone()
    if row is None or row[0] in (None, ""):
        return True, 0.0, None

    next_poll_text = str(row[0])
    next_poll_dt = parse_iso_datetime_utc(next_poll_text)
    if next_poll_dt is None:
        return True, 0.0, None
    if now_value >= next_poll_dt:
        return True, 0.0, next_poll_text

    remaining_hours = max(0.0, (next_poll_dt - now_value).total_seconds() / 3600.0)
    return False, remaining_hours, next_poll_text


def upsert_source_poll_state(
    connection: sqlite3.Connection,
    source_id: str,
    poll_interval_hours: float,
    last_poll_status: str,
    last_error: str | None,
    last_poll_at: str | None,
    next_poll_at: str | None,
    updated_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO source_poll_state (
            source_id,
            last_poll_at,
            next_poll_at,
            poll_interval_hours,
            last_poll_status,
            last_error,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            last_poll_at = excluded.last_poll_at,
            next_poll_at = excluded.next_poll_at,
            poll_interval_hours = excluded.poll_interval_hours,
            last_poll_status = excluded.last_poll_status,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        """,
        (
            source_id,
            last_poll_at,
            next_poll_at,
            float(max(0.0, poll_interval_hours)),
            last_poll_status,
            last_error,
            updated_at,
        ),
    )


def enqueue_work_item(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    video_id: str,
    now_iso: str,
    priority: int = 100,
    payload_json: str | None = None,
) -> str:
    row = connection.execute(
        """
        SELECT status, priority
        FROM work_items
        WHERE source_id = ? AND stage = ? AND video_id = ?
        """,
        (source_id, stage, video_id),
    ).fetchone()
    safe_priority = int(priority)
    if row is None:
        connection.execute(
            """
            INSERT INTO work_items (
                source_id,
                stage,
                video_id,
                status,
                priority,
                attempt_count,
                next_retry_at,
                lease_owner,
                lease_token,
                lease_expires_at,
                last_error,
                payload_json,
                created_at,
                updated_at,
                started_at,
                finished_at
            ) VALUES (?, ?, ?, 'queued', ?, 0, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, NULL, NULL)
            """,
            (
                source_id,
                stage,
                video_id,
                safe_priority,
                payload_json,
                now_iso,
                now_iso,
            ),
        )
        return "inserted"

    current_status = str(row[0] or "")
    current_priority = int(row[1] or safe_priority)
    next_priority = min(current_priority, safe_priority)

    if current_status in {"queued", "leased"}:
        if next_priority != current_priority:
            connection.execute(
                """
                UPDATE work_items
                SET priority = ?, updated_at = ?
                WHERE source_id = ? AND stage = ? AND video_id = ?
                """,
                (next_priority, now_iso, source_id, stage, video_id),
            )
            return "updated"
        return "kept"

    if current_status == "success":
        return "kept"

    connection.execute(
        """
        UPDATE work_items
        SET status = 'queued',
            priority = ?,
            next_retry_at = NULL,
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error = NULL,
            payload_json = COALESCE(?, payload_json),
            updated_at = ?,
            started_at = NULL,
            finished_at = NULL
        WHERE source_id = ? AND stage = ? AND video_id = ?
        """,
        (
            next_priority,
            payload_json,
            now_iso,
            source_id,
            stage,
            video_id,
        ),
    )
    return "requeued"


def enqueue_media_work_items(
    connection: sqlite3.Connection,
    source_id: str,
    video_ids: list[str],
    now_iso: str,
    source_slot: int = 0,
    source_stride: int = 1,
    priority_base: int = 0,
) -> tuple[int, int, int, int]:
    inserted = 0
    requeued = 0
    updated = 0
    kept = 0
    normalized_stride = max(1, int(source_stride))
    normalized_slot = max(0, int(source_slot)) % normalized_stride
    normalized_base = max(0, int(priority_base))
    for index, video_id in enumerate(video_ids):
        priority = normalized_base + normalized_slot + (index * normalized_stride)
        action = enqueue_work_item(
            connection=connection,
            source_id=source_id,
            stage="media",
            video_id=video_id,
            now_iso=now_iso,
            priority=priority,
            payload_json=None,
        )
        if action == "inserted":
            inserted += 1
        elif action == "requeued":
            requeued += 1
        elif action == "updated":
            updated += 1
        else:
            kept += 1
    return inserted, requeued, updated, kept


def enqueue_source_media_discovery(
    connection: sqlite3.Connection,
    source: SourceConfig,
    dry_run: bool,
    run_label: str,
    playlist_start: int | None = None,
    playlist_end: int | None = None,
    enforce_poll_interval: bool = True,
    source_slot: int = 0,
    source_stride: int = 1,
) -> tuple[int, int, int]:
    now_dt = dt.datetime.now(dt.timezone.utc)
    now_iso = now_dt.replace(microsecond=0).isoformat()
    interval_hours = max(0.0, float(source.media_discovery_interval_hours))

    cooldown_active, cooldown_remaining_hours, cooldown_until, cooldown_reason = (
        get_source_network_cooldown_state(
            connection=connection,
            source_id=source.id,
            now_dt=now_dt,
        )
    )
    if cooldown_active:
        print(
            f"[{run_label}] {source.id}: network cooldown active "
            f"(blocked_until={cooldown_until or 'unknown'}, "
            f"remaining={cooldown_remaining_hours:.2f}h)"
        )
        if cooldown_reason:
            print(f"[{run_label}] {source.id}: cooldown reason={cooldown_reason}")
        return 0, 0, 0

    should_poll = True
    remaining_hours = 0.0
    next_poll_at = None
    if enforce_poll_interval:
        should_poll, remaining_hours, next_poll_at = should_poll_source_discovery(
            connection=connection,
            source=source,
            now_dt=now_dt,
        )

    if not should_poll:
        print(
            f"[{run_label}] {source.id}: media discovery deferred "
            f"(next_poll_at={next_poll_at or 'unknown'}, remaining={remaining_hours:.2f}h)"
        )
        return 0, 0, 0

    playlist_start_value = playlist_start if playlist_start is not None else 1
    if playlist_end is not None:
        playlist_end_value = playlist_end
    elif source.playlist_end is not None:
        playlist_end_value = source.playlist_end
    else:
        playlist_end_value = DEFAULT_PLAYLIST_END

    discovered_ids: list[str] = []
    try:
        discovered_ids = discover_playlist_window_ids(
            source=source,
            playlist_start=playlist_start_value,
            playlist_end=playlist_end_value,
            dry_run=dry_run,
        )
    except Exception as exc:
        next_poll_iso = compute_next_poll_at_iso(
            poll_interval_hours=min(interval_hours, 1.0) if interval_hours > 0 else 1.0,
            from_dt=now_dt,
        )
        if not dry_run:
            blocked_until = extend_source_network_cooldown(
                connection=connection,
                source_id=source.id,
                error_message=str(exc),
                blocked_until=schedule_next_retry_iso(
                    1,
                    error_message=str(exc),
                ),
                blocked_at=now_iso,
            )
            if blocked_until and blocked_until > next_poll_iso:
                next_poll_iso = blocked_until
            record_source_network_access(
                connection=connection,
                source=source,
                request_at=now_utc_iso(),
                succeeded=False,
                last_error=(
                    _SOURCE_ACCESS_UNSET
                    if blocked_until
                    else str(exc)
                ),
            )
            upsert_source_poll_state(
                connection=connection,
                source_id=source.id,
                poll_interval_hours=interval_hours if interval_hours > 0 else 24.0,
                last_poll_status="error",
                last_error=str(exc),
                last_poll_at=now_iso,
                next_poll_at=next_poll_iso,
                updated_at=now_iso,
            )
            connection.commit()
        raise

    if dry_run:
        print(
            f"[{run_label}] {source.id}: discovery dry-run ids={len(discovered_ids)} "
            f"range={playlist_start_value}-{playlist_end_value}"
        )
        return len(discovered_ids), 0, 0

    inserted, requeued, updated, kept = enqueue_media_work_items(
        connection=connection,
        source_id=source.id,
        video_ids=discovered_ids,
        now_iso=now_iso,
        source_slot=source_slot,
        source_stride=source_stride,
    )
    next_poll_iso = compute_next_poll_at_iso(
        poll_interval_hours=interval_hours if interval_hours > 0 else 24.0,
        from_dt=now_dt,
    )
    upsert_source_poll_state(
        connection=connection,
        source_id=source.id,
        poll_interval_hours=interval_hours if interval_hours > 0 else 24.0,
        last_poll_status="success",
        last_error=None,
        last_poll_at=now_iso,
        next_poll_at=next_poll_iso,
        updated_at=now_iso,
    )
    record_source_network_access(
        connection=connection,
        source=source,
        request_at=now_utc_iso(),
        succeeded=True,
        clear_cooldown=True,
        last_error=None,
    )
    connection.commit()

    print(
        f"[{run_label}] {source.id}: queued media ids={len(discovered_ids)} "
        f"(inserted={inserted}, requeued={requeued}, reprioritized={updated}, kept={kept}) "
        f"range={playlist_start_value}-{playlist_end_value}"
    )
    return len(discovered_ids), inserted, requeued


def normalize_queue_stages(raw_stages: list[str] | None) -> list[str]:
    if not raw_stages:
        return list(DEFAULT_QUEUE_STAGES)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_stage in raw_stages:
        stage = str(raw_stage or "").strip().lower()
        if stage not in DEFAULT_QUEUE_STAGES or stage in seen:
            continue
        seen.add(stage)
        normalized.append(stage)
    return normalized or list(DEFAULT_QUEUE_STAGES)


def compute_lease_expires_at_iso(
    lease_seconds: int,
    from_dt: dt.datetime | None = None,
) -> str:
    safe_lease_seconds = max(30, int(lease_seconds))
    base_dt = from_dt or dt.datetime.now(dt.timezone.utc)
    lease_dt = base_dt + dt.timedelta(seconds=safe_lease_seconds)
    return lease_dt.replace(microsecond=0).isoformat()


def upsert_worker_heartbeat(
    connection: sqlite3.Connection,
    worker_id: str,
    started_at: str,
    heartbeat_at: str,
    host: str | None = None,
    pid: int | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO worker_heartbeats (
            worker_id,
            host,
            pid,
            started_at,
            last_heartbeat_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
            host = excluded.host,
            pid = excluded.pid,
            started_at = excluded.started_at,
            last_heartbeat_at = excluded.last_heartbeat_at
        """,
        (
            worker_id,
            str(host or socket.gethostname()),
            int(pid if pid is not None else os.getpid()),
            started_at,
            heartbeat_at,
        ),
    )


def requeue_expired_work_item_leases(
    connection: sqlite3.Connection,
    now_iso: str | None = None,
) -> int:
    now_value = now_iso or now_utc_iso()
    updated = connection.execute(
        """
        UPDATE work_items
        SET status = 'queued',
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            updated_at = ?
        WHERE status = 'leased'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at <= ?
        """,
        (now_value, now_value),
    ).rowcount
    return int(updated or 0)


def extend_work_item_lease(
    connection: sqlite3.Connection,
    work_item_id: int,
    lease_token: str,
    lease_seconds: int = DEFAULT_QUEUE_LEASE_SEC,
    now_dt: dt.datetime | None = None,
) -> tuple[bool, str | None]:
    now_value = now_dt or dt.datetime.now(dt.timezone.utc)
    now_iso = now_value.replace(microsecond=0).isoformat()
    next_lease_expires_at = compute_lease_expires_at_iso(
        lease_seconds=lease_seconds,
        from_dt=now_value,
    )
    updated = connection.execute(
        """
        UPDATE work_items
        SET lease_expires_at = ?,
            updated_at = ?
        WHERE id = ?
          AND status = 'leased'
          AND lease_token = ?
        """,
        (
            next_lease_expires_at,
            now_iso,
            int(work_item_id),
            lease_token,
        ),
    ).rowcount
    connection.commit()
    return (updated == 1, next_lease_expires_at if updated == 1 else None)


def lease_keepalive_loop(
    db_path: Path,
    work_item_id: int,
    lease_token: str,
    lease_seconds: int,
    poll_interval_sec: float,
    stop_event: threading.Event,
    worker_id: str,
) -> None:
    safe_interval_sec = max(5.0, float(poll_interval_sec))
    while not stop_event.wait(safe_interval_sec):
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(str(db_path), timeout=30)
            connection.execute("PRAGMA journal_mode=WAL")
            extended, next_expires_at = extend_work_item_lease(
                connection=connection,
                work_item_id=work_item_id,
                lease_token=lease_token,
                lease_seconds=lease_seconds,
            )
            if not extended:
                print(
                    f"[queue-worker] lease keepalive stopped worker_id={worker_id} "
                    f"id={work_item_id} (lease lost)",
                    file=sys.stderr,
                )
                return
            print(
                f"[queue-worker] lease extended worker_id={worker_id} "
                f"id={work_item_id} next_expires_at={next_expires_at}"
            )
        except Exception as exc:
            print(
                f"[queue-worker] lease keepalive warning worker_id={worker_id} "
                f"id={work_item_id} ({exc})",
                file=sys.stderr,
            )
        finally:
            if connection is not None:
                connection.close()


def lease_next_work_item(
    connection: sqlite3.Connection,
    worker_id: str,
    stages: list[str] | None,
    lease_seconds: int = DEFAULT_QUEUE_LEASE_SEC,
    now_dt: dt.datetime | None = None,
    max_scan_attempts: int = 12,
    avoid_source_id: str | None = None,
) -> dict[str, Any] | None:
    normalized_stages = normalize_queue_stages(stages)
    if not normalized_stages:
        return None
    placeholders = ",".join("?" for _ in normalized_stages)

    for _ in range(max(1, int(max_scan_attempts))):
        now_value = now_dt or dt.datetime.now(dt.timezone.utc)
        now_iso = now_value.replace(microsecond=0).isoformat()
        lease_expires_at = compute_lease_expires_at_iso(
            lease_seconds=lease_seconds,
            from_dt=now_value,
        )
        lease_token = uuid.uuid4().hex

        connection.execute("BEGIN IMMEDIATE")
        candidate_params: list[Any] = [*normalized_stages, now_iso, now_iso, now_iso]
        order_by = "priority ASC, updated_at ASC, id ASC"
        if avoid_source_id:
            order_by = (
                "CASE WHEN source_id = ? THEN 1 ELSE 0 END ASC, "
                "priority ASC, updated_at ASC, id ASC"
            )
            candidate_params.append(str(avoid_source_id))

        row = connection.execute(
            f"""
            WITH eligible AS (
                SELECT
                    id,
                    source_id,
                    stage,
                    video_id,
                    priority,
                    attempt_count,
                    COALESCE(payload_json, '') AS payload_json,
                    updated_at
                FROM work_items
                WHERE stage IN ({placeholders})
                  AND status IN ('queued', 'error')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                  AND (
                      stage NOT IN ('media', 'subs', 'meta')
                      OR NOT EXISTS (
                          SELECT 1
                          FROM source_access_state AS access
                          WHERE access.source_id = work_items.source_id
                            AND (
                                (
                                    access.blocked_until IS NOT NULL
                                    AND access.blocked_until > ?
                                )
                                OR (
                                    access.next_request_not_before IS NOT NULL
                                    AND access.next_request_not_before > ?
                                )
                            )
                      )
                  )
            ),
            source_head AS (
                SELECT
                    eligible.id,
                    eligible.source_id,
                    eligible.stage,
                    eligible.video_id,
                    eligible.priority,
                    eligible.attempt_count,
                    eligible.payload_json,
                    eligible.updated_at
                FROM eligible
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM eligible AS other
                    WHERE other.source_id = eligible.source_id
                      AND (
                          other.priority < eligible.priority
                          OR (
                              other.priority = eligible.priority
                              AND other.updated_at < eligible.updated_at
                          )
                          OR (
                              other.priority = eligible.priority
                              AND other.updated_at = eligible.updated_at
                              AND other.id < eligible.id
                          )
                      )
                )
            )
            SELECT
                id,
                source_id,
                stage,
                video_id,
                priority,
                attempt_count,
                payload_json
            FROM source_head
            ORDER BY {order_by}
            LIMIT 1
            """,
            candidate_params,
        ).fetchone()
        if row is None:
            connection.commit()
            return None

        item_id = int(row[0])
        updated = connection.execute(
            """
            UPDATE work_items
            SET status = 'leased',
                lease_owner = ?,
                lease_token = ?,
                lease_expires_at = ?,
                updated_at = ?,
                started_at = COALESCE(started_at, ?)
            WHERE id = ?
              AND status IN ('queued', 'error')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            """,
            (
                worker_id,
                lease_token,
                lease_expires_at,
                now_iso,
                now_iso,
                item_id,
                now_iso,
            ),
        ).rowcount
        if updated == 1:
            connection.commit()
            return {
                "id": item_id,
                "source_id": str(row[1]),
                "stage": str(row[2]),
                "video_id": str(row[3]),
                "priority": int(row[4] or 0),
                "attempt_count": int(row[5] or 0),
                "payload_json": str(row[6] or ""),
                "lease_token": lease_token,
                "lease_expires_at": lease_expires_at,
            }
        connection.rollback()
    return None


def complete_work_item_success(
    connection: sqlite3.Connection,
    work_item_id: int,
    lease_token: str,
    finished_at: str | None = None,
) -> bool:
    finished_value = finished_at or now_utc_iso()
    updated = connection.execute(
        """
        UPDATE work_items
        SET status = 'success',
            attempt_count = attempt_count + 1,
            next_retry_at = NULL,
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error = NULL,
            updated_at = ?,
            finished_at = ?
        WHERE id = ?
          AND lease_token = ?
        """,
        (finished_value, finished_value, int(work_item_id), lease_token),
    ).rowcount
    connection.commit()
    return updated == 1


def enqueue_downstream_work_items(
    connection: sqlite3.Connection,
    source: SourceConfig | None,
    source_id: str,
    stage: str,
    video_id: str,
    now_iso: str,
    base_priority: int = 100,
) -> tuple[int, int, int, int]:
    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage == "media":
        next_stages = ["subs", "meta", "loudness"]
        if source is not None and source.asr_enabled and bool(source.asr_command):
            next_stages.append("asr")
    elif normalized_stage in {"subs", "asr"}:
        next_stages = ["translate"]
    else:
        return (0, 0, 0, 0)
    inserted = 0
    requeued = 0
    updated = 0
    kept = 0
    for offset, next_stage in enumerate(next_stages, start=1):
        action = enqueue_work_item(
            connection=connection,
            source_id=source_id,
            stage=next_stage,
            video_id=video_id,
            now_iso=now_iso,
            priority=max(0, int(base_priority) + offset),
        )
        if action == "inserted":
            inserted += 1
        elif action == "requeued":
            requeued += 1
        elif action == "updated":
            updated += 1
        else:
            kept += 1
    return (inserted, requeued, updated, kept)


def fail_work_item_lease(
    connection: sqlite3.Connection,
    work_item_id: int,
    lease_token: str,
    error_message: str,
    max_attempts: int = DEFAULT_QUEUE_MAX_ATTEMPTS,
    finished_at: str | None = None,
) -> tuple[str, int, str | None]:
    safe_error = str(error_message or "").strip() or "work item failed"
    safe_error = safe_error[:4000]
    finished_value = finished_at or now_utc_iso()
    row = connection.execute(
        """
        SELECT attempt_count
        FROM work_items
        WHERE id = ?
          AND lease_token = ?
        """,
        (int(work_item_id), lease_token),
    ).fetchone()
    if row is None:
        return ("lost", 0, None)

    next_attempt_count = int(row[0] or 0) + 1
    if next_attempt_count >= max(1, int(max_attempts)):
        next_status = "dead"
        next_retry_at = None
    else:
        next_status = "error"
        next_retry_at = schedule_next_retry_iso(
            next_attempt_count,
            error_message=safe_error,
        )

    updated = connection.execute(
        """
        UPDATE work_items
        SET status = ?,
            attempt_count = ?,
            next_retry_at = ?,
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error = ?,
            updated_at = ?,
            finished_at = ?
        WHERE id = ?
          AND lease_token = ?
        """,
        (
            next_status,
            next_attempt_count,
            next_retry_at,
            safe_error,
            finished_value,
            finished_value,
            int(work_item_id),
            lease_token,
        ),
    ).rowcount
    connection.commit()
    if updated != 1:
        return ("lost", next_attempt_count, next_retry_at)
    return (next_status, next_attempt_count, next_retry_at)


def get_download_state_status(
    connection: sqlite3.Connection,
    source_id: str,
    stage: str,
    video_id: str,
) -> tuple[str | None, str | None]:
    row = connection.execute(
        """
        SELECT status, COALESCE(last_error, '')
        FROM download_state
        WHERE source_id = ?
          AND stage = ?
          AND video_id = ?
        """,
        (source_id, stage, video_id),
    ).fetchone()
    if row is None:
        return (None, None)
    status = str(row[0] or "")
    error_text = str(row[1] or "")
    return (status, error_text)


def build_worker_urls_temp_path(
    source: SourceConfig,
    worker_id: str,
    work_item_id: int,
) -> Path:
    worker_token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(worker_id or "worker"))
    return source.media_archive.parent / "tmp" / (
        f"urls.{worker_token}.{os.getpid()}.{int(work_item_id)}.txt"
    )


def process_leased_work_item(
    connection: sqlite3.Connection,
    source_by_id: dict[str, SourceConfig],
    work_item: dict[str, Any],
    worker_id: str,
    db_path: Path,
    translate_target_lang: str = "ja-local",
    translate_source_track: str = "auto",
    translate_timeout_sec: int = 60,
    dry_run: bool = False,
) -> tuple[bool, str | None]:
    source_id = str(work_item.get("source_id") or "")
    stage = str(work_item.get("stage") or "").strip().lower()
    video_id = str(work_item.get("video_id") or "")
    item_id = int(work_item.get("id") or 0)

    source = source_by_id.get(source_id)
    if source is None:
        return (False, f"unknown source_id for work item: {source_id}")

    if not video_id:
        return (False, "work item has empty video_id")

    if stage not in {"media", "subs", "meta", "asr", "loudness", "translate"}:
        return (False, f"unsupported stage: {stage}")

    if dry_run:
        print(
            f"[queue-worker] dry-run processed "
            f"id={item_id} source={source_id} stage={stage} video_id={video_id}"
        )
        return (True, None)

    run_label = f"queue-worker:{worker_id}:{stage}:{item_id}"
    urls_temp_path = build_worker_urls_temp_path(
        source=source,
        worker_id=worker_id,
        work_item_id=item_id,
    )

    try:
        if stage == "media":
            sync_source(
                source=source,
                dry_run=False,
                skip_media=False,
                skip_subs=True,
                skip_meta=True,
                connection=connection,
                media_candidate_ids=[video_id],
                metadata_candidate_ids=None,
                run_label=run_label,
                respect_media_discovery_interval=False,
                respect_source_cooldown=False,
                urls_file_override=urls_temp_path,
                strict_candidate_scope=True,
            )
        elif stage == "subs":
            sync_source(
                source=source,
                dry_run=False,
                skip_media=True,
                skip_subs=False,
                skip_meta=True,
                connection=connection,
                metadata_candidate_ids=[video_id],
                run_label=run_label,
                respect_media_discovery_interval=False,
                respect_source_cooldown=False,
                urls_file_override=urls_temp_path,
                strict_candidate_scope=True,
            )
        elif stage == "meta":
            sync_source(
                source=source,
                dry_run=False,
                skip_media=True,
                skip_subs=True,
                skip_meta=False,
                connection=connection,
                metadata_candidate_ids=[video_id],
                run_label=run_label,
                respect_media_discovery_interval=False,
                respect_source_cooldown=False,
                urls_file_override=urls_temp_path,
                strict_candidate_scope=True,
            )
        elif stage == "asr":
            asr_ok, asr_error = run_asr_for_video(
                connection=connection,
                source=source,
                video_id=video_id,
                dry_run=False,
                force=False,
                ffprobe_bin=find_executable_command("ffprobe"),
            )
            if not asr_ok:
                return (False, asr_error or f"asr failed ({source.id}/{video_id})")
        elif stage == "loudness":
            loudness_ok, loudness_error = run_loudness_for_video(
                connection=connection,
                source=source,
                video_id=video_id,
                target_lufs=DEFAULT_LOUDNESS_TARGET_LUFS,
                max_boost_db=DEFAULT_LOUDNESS_MAX_BOOST_DB,
                max_cut_db=DEFAULT_LOUDNESS_MAX_CUT_DB,
                ffmpeg_bin=DEFAULT_LOUDNESS_FFMPEG_BIN,
            )
            if not loudness_ok:
                return (
                    False,
                    loudness_error or f"loudness failed ({source.id}/{video_id})",
                )
        else:
            translate_ok, translate_error = run_translate_local_for_video(
                connection=connection,
                db_path=db_path,
                source=source,
                video_id=video_id,
                target_lang=translate_target_lang,
                source_track=translate_source_track,
                timeout_sec=translate_timeout_sec,
                dry_run=False,
            )
            if not translate_ok:
                return (
                    False,
                    translate_error or f"translate failed ({source.id}/{video_id})",
                )
    finally:
        if urls_temp_path.exists():
            try:
                urls_temp_path.unlink()
            except OSError:
                pass

    if stage in {"asr", "loudness", "translate"}:
        return (True, None)

    status, last_error = get_download_state_status(
        connection=connection,
        source_id=source_id,
        stage=stage,
        video_id=video_id,
    )
    if status == "success":
        return (True, None)
    if status == "error":
        return (False, last_error or f"{stage} download_state status=error")

    if stage == "media":
        media_path = find_media_file_for_video(source, video_id)
        if media_path is not None and media_path.exists():
            return (True, None)

    return (False, f"{stage} did not write a terminal download_state row")


def run_queue_worker(
    sources: list[SourceConfig],
    db_path: Path,
    stages: list[str] | None = None,
    worker_id: str | None = None,
    lease_seconds: int = DEFAULT_QUEUE_LEASE_SEC,
    poll_interval_sec: float = DEFAULT_QUEUE_POLL_SEC,
    max_items: int = 0,
    once: bool = False,
    dry_run: bool = False,
    max_attempts: int = DEFAULT_QUEUE_MAX_ATTEMPTS,
    enqueue_downstream: bool = True,
    translate_target_lang: str = "ja-local",
    translate_source_track: str = "auto",
    translate_timeout_sec: int = 60,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.row_factory = sqlite3.Row
    create_schema(connection)

    source_by_id = {source.id: source for source in sources}
    normalized_stages = normalize_queue_stages(stages)
    safe_lease_seconds = max(30, int(lease_seconds))
    safe_poll_interval_sec = max(0.2, float(poll_interval_sec))
    safe_max_items = max(0, int(max_items))
    safe_max_attempts = max(1, int(max_attempts))

    worker_id_value = str(worker_id or "").strip()
    if not worker_id_value:
        worker_id_value = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    started_at = now_utc_iso()
    processed = 0
    success_count = 0
    failure_count = 0
    last_leased_source_id: str | None = None
    print(
        f"[queue-worker] start worker_id={worker_id_value} "
        f"stages={','.join(normalized_stages)} "
        f"lease={safe_lease_seconds}s poll={safe_poll_interval_sec:.1f}s "
        f"max_items={safe_max_items or 'unbounded'} once={bool(once)}"
    )

    try:
        while True:
            heartbeat_at = now_utc_iso()
            upsert_worker_heartbeat(
                connection=connection,
                worker_id=worker_id_value,
                started_at=started_at,
                heartbeat_at=heartbeat_at,
            )
            reclaimed = requeue_expired_work_item_leases(
                connection=connection,
                now_iso=heartbeat_at,
            )
            connection.commit()
            if reclaimed > 0:
                print(
                    f"[queue-worker] reclaimed expired leases={reclaimed}"
                )

            leased_item = lease_next_work_item(
                connection=connection,
                worker_id=worker_id_value,
                stages=normalized_stages,
                lease_seconds=safe_lease_seconds,
                avoid_source_id=last_leased_source_id,
            )
            if leased_item is None:
                if once:
                    break
                if safe_max_items > 0 and processed >= safe_max_items:
                    break
                time.sleep(safe_poll_interval_sec)
                continue

            processed += 1
            item_id = int(leased_item["id"])
            source_id = str(leased_item["source_id"])
            stage = str(leased_item["stage"])
            video_id = str(leased_item["video_id"])
            lease_token = str(leased_item["lease_token"])
            last_leased_source_id = source_id
            print(
                f"[queue-worker] leased id={item_id} source={source_id} "
                f"stage={stage} video_id={video_id}"
            )

            ok = False
            failure_reason: str | None = None
            keepalive_stop_event = threading.Event()
            keepalive_thread: threading.Thread | None = None
            if not dry_run:
                keepalive_interval_sec = max(10.0, min(float(safe_lease_seconds) / 3.0, 120.0))
                keepalive_thread = threading.Thread(
                    target=lease_keepalive_loop,
                    kwargs={
                        "db_path": db_path,
                        "work_item_id": item_id,
                        "lease_token": lease_token,
                        "lease_seconds": safe_lease_seconds,
                        "poll_interval_sec": keepalive_interval_sec,
                        "stop_event": keepalive_stop_event,
                        "worker_id": worker_id_value,
                    },
                )
                keepalive_thread.start()
            try:
                ok, failure_reason = process_leased_work_item(
                    connection=connection,
                    source_by_id=source_by_id,
                    work_item=leased_item,
                    worker_id=worker_id_value,
                    db_path=db_path,
                    translate_target_lang=translate_target_lang,
                    translate_source_track=translate_source_track,
                    translate_timeout_sec=translate_timeout_sec,
                    dry_run=dry_run,
                )
            except Exception as exc:
                ok = False
                failure_reason = f"work item execution exception: {exc}"
            finally:
                keepalive_stop_event.set()
                if keepalive_thread is not None:
                    keepalive_thread.join()

            if ok:
                if complete_work_item_success(
                    connection=connection,
                    work_item_id=item_id,
                    lease_token=lease_token,
                ):
                    downstream_summary = ""
                    if enqueue_downstream:
                        (
                            ds_inserted,
                            ds_requeued,
                            ds_updated,
                            ds_kept,
                        ) = enqueue_downstream_work_items(
                            connection=connection,
                            source=source_by_id.get(source_id),
                            source_id=source_id,
                            stage=stage,
                            video_id=video_id,
                            now_iso=now_utc_iso(),
                            base_priority=int(leased_item.get("priority") or 100),
                        )
                        connection.commit()
                        if ds_inserted or ds_requeued or ds_updated:
                            downstream_summary = (
                                " downstream="
                                f"(inserted={ds_inserted}, requeued={ds_requeued}, "
                                f"updated={ds_updated}, kept={ds_kept})"
                            )
                    success_count += 1
                    print(
                        f"[queue-worker] success id={item_id} "
                        f"source={source_id} stage={stage} video_id={video_id}"
                        f"{downstream_summary}"
                    )
                else:
                    failure_count += 1
                    print(
                        f"[queue-worker] lost lease before success update id={item_id}",
                        file=sys.stderr,
                    )
            else:
                failure_count += 1
                next_status, attempt_count, next_retry_at = fail_work_item_lease(
                    connection=connection,
                    work_item_id=item_id,
                    lease_token=lease_token,
                    error_message=failure_reason or "unknown worker error",
                    max_attempts=safe_max_attempts,
                )
                retry_label = next_retry_at or "-"
                print(
                    f"[queue-worker] failed id={item_id} source={source_id} "
                    f"stage={stage} video_id={video_id} status={next_status} "
                    f"attempt={attempt_count} next_retry_at={retry_label}",
                    file=sys.stderr,
                )

            if once:
                break
            if safe_max_items > 0 and processed >= safe_max_items:
                break
    finally:
        connection.close()

    print(
        f"[queue-worker] done processed={processed} "
        f"success={success_count} failed={failure_count}"
    )


def get_media_no_audio_bootstrap_ids(
    connection: sqlite3.Connection,
    source_id: str,
    limit: int = 200,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT v.video_id
        FROM videos v
        LEFT JOIN download_state d
          ON d.source_id = v.source_id
         AND d.stage = 'media'
         AND d.video_id = v.video_id
        WHERE v.source_id = ?
          AND v.has_media = 1
          AND COALESCE(v.media_path, '') != ''
          AND v.audio_lufs IS NULL
          AND ABS(COALESCE(v.audio_gain_db, 0.0)) < 0.000001
          AND COALESCE(v.audio_loudness_analyzed_at, '') != ''
          AND COALESCE(v.audio_loudness_error, '') = ''
          AND COALESCE(d.status, '') != 'error'
        ORDER BY COALESCE(v.audio_loudness_analyzed_at, '') DESC, v.video_id DESC
        LIMIT ?
        """,
        (source_id, limit),
    ).fetchall()
    return [str(row[0]) for row in rows]


def get_subtitle_missing_bootstrap_ids(
    connection: sqlite3.Connection,
    source_id: str,
    existing_sub_ids: set[str] | None = None,
    require_db_missing_subtitles: bool = True,
    limit: int = 200,
) -> list[str]:
    safe_limit = max(0, int(limit))
    if safe_limit <= 0:
        return []

    params: list[Any] = [source_id]
    query = """
        SELECT v.video_id
        FROM videos v
        LEFT JOIN download_state d
          ON d.source_id = v.source_id
         AND d.stage = 'subs'
         AND d.video_id = v.video_id
        WHERE v.source_id = ?
          AND v.has_media = 1
          AND COALESCE(v.media_path, '') != ''
          AND COALESCE(d.status, '') != 'error'
    """
    if require_db_missing_subtitles:
        query += "\n          AND COALESCE(v.has_subtitles, 0) = 0"
    query += "\n        ORDER BY COALESCE(v.upload_date, '') DESC, v.video_id DESC"
    rows = connection.execute(query, tuple(params)).fetchall()

    filtered_ids: list[str] = []
    existing = existing_sub_ids or set()
    for row in rows:
        video_id = str(row[0])
        if video_id in existing:
            continue
        filtered_ids.append(video_id)
        if len(filtered_ids) >= safe_limit:
            break
    return filtered_ids


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
    impersonate_flags = resolve_impersonate_flags(source)
    retry_flags = build_ytdlp_retry_flags(source, include_ignore_errors=False)
    command = [
        source.ytdlp_bin,
        *impersonate_flags,
        *cookie_flags,
        *retry_flags,
        "--flat-playlist",
        "--print",
        "%(id)s",
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


def run_asr_for_video(
    connection: sqlite3.Connection,
    source: SourceConfig,
    video_id: str,
    dry_run: bool = False,
    force: bool = False,
    ffprobe_bin: str | None = None,
) -> tuple[bool, str | None]:
    if not source.asr_enabled:
        return (False, f"{source.id}: asr disabled")
    if not source.asr_command:
        return (False, f"{source.id}: missing asr_command")

    row = connection.execute(
        """
        SELECT
            v.media_path,
            COALESCE(a.status, ''),
            a.output_path,
            COALESCE(a.attempts, 0)
        FROM videos v
        LEFT JOIN asr_runs a
            ON a.source_id = v.source_id
           AND a.video_id = v.video_id
        WHERE v.source_id = ?
          AND v.video_id = ?
          AND v.has_media = 1
        LIMIT 1
        """,
        (source.id, video_id),
    ).fetchone()
    if row is None:
        return (False, f"{source.id}/{video_id}: missing ledger row or has_media=0")

    media_path_value, status, output_path_value, attempts = row
    if media_path_value in (None, ""):
        return (False, f"{source.id}/{video_id}: media_path is empty")
    media_path = Path(str(media_path_value))
    if not media_path.exists():
        return (False, f"{source.id}/{video_id}: media file missing ({media_path})")

    ffprobe_value = (
        ffprobe_bin
        if ffprobe_bin is not None
        else find_executable_command("ffprobe")
    )
    has_valid_output = False
    if output_path_value not in (None, ""):
        output_path = Path(str(output_path_value))
        if output_path.exists():
            output_size = -1
            try:
                output_size = output_path.stat().st_size
            except OSError:
                output_size = -1
            if output_size > 0:
                has_valid_output = True
            elif ffprobe_value:
                has_audio_stream, _ = detect_audio_stream(
                    media_path=media_path,
                    ffprobe_bin=ffprobe_value,
                )
                has_valid_output = has_audio_stream is False

    should_run = force or not (str(status or "") == "success" and has_valid_output)
    if not should_run:
        print(f"[asr] {source.id}/{video_id}: up to date")
        return (True, None)

    source.asr_dir.mkdir(parents=True, exist_ok=True)
    final_dir = source.asr_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
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
        return (True, None)

    artifact_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    run_attempt = int(attempts) + 1
    started_at = now_utc_iso()

    upsert_asr_run(
        connection=connection,
        source_id=source.id,
        video_id=video_id,
        status="running",
        attempts=run_attempt,
        engine="command",
        output_path=str(output_path_value) if output_path_value else None,
        artifact_dir=str(artifact_dir),
        last_error=None,
        started_at=started_at,
        finished_at=None,
    )
    connection.commit()

    timeout = source.asr_timeout_sec if source.asr_timeout_sec > 0 else None
    if ffprobe_value:
        has_audio_stream, probe_error = detect_audio_stream(
            media_path=media_path,
            ffprobe_bin=ffprobe_value,
        )
        if has_audio_stream is False:
            final_output_path = final_dir / f"{video_id}.asr.srt"
            write_empty_srt(final_output_path)
            finished_at = now_utc_iso()
            retry_count, next_retry_at = mark_media_retry_state(
                connection=connection,
                source=source,
                video_id=video_id,
                reason="no audio stream detected during ASR",
                attempt_at=finished_at,
            )
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
                f"(wrote {final_output_path}; media_retry_count={retry_count} "
                f"next_retry_at={next_retry_at})"
            )
            return (True, None)
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
            output_path=str(output_path_value) if output_path_value else None,
            artifact_dir=str(artifact_dir),
            last_error=f"timeout after {source.asr_timeout_sec}s",
            started_at=started_at,
            finished_at=finished_at,
        )
        connection.commit()
        return (False, f"{source.id}/{video_id}: timeout")

    if completed.returncode != 0:
        finished_at = now_utc_iso()
        upsert_asr_run(
            connection=connection,
            source_id=source.id,
            video_id=video_id,
            status="error",
            attempts=run_attempt,
            engine="command",
            output_path=str(output_path_value) if output_path_value else None,
            artifact_dir=str(artifact_dir),
            last_error=f"command exit code {completed.returncode}",
            started_at=started_at,
            finished_at=finished_at,
        )
        connection.commit()
        return (False, f"{source.id}/{video_id}: command exit code {completed.returncode}")

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
            output_path=str(output_path_value) if output_path_value else None,
            artifact_dir=str(artifact_dir),
            last_error=(
                "ASR command succeeded but no subtitle file "
                f"found in {artifact_dir}"
            ),
            started_at=started_at,
            finished_at=finished_at,
        )
        connection.commit()
        return (False, f"{source.id}/{video_id}: no subtitle artifacts")

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
    return (True, None)


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

    connection = sqlite3.connect(str(db_path), timeout=30)
    create_schema(connection)
    ffprobe_bin = find_executable_command("ffprobe")

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
                output_path = Path(str(output_path_value))
                if output_path.exists():
                    output_size = -1
                    try:
                        output_size = output_path.stat().st_size
                    except OSError:
                        output_size = -1
                    if output_size > 0:
                        has_valid_output = True
                    elif ffprobe_bin:
                        has_audio_stream, _ = detect_audio_stream(
                            media_path=media_path,
                            ffprobe_bin=ffprobe_bin,
                        )
                        has_valid_output = has_audio_stream is False

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
                    retry_count, next_retry_at = mark_media_retry_state(
                        connection=connection,
                        source=source,
                        video_id=video_id,
                        reason="no audio stream detected during ASR",
                        attempt_at=finished_at,
                    )
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
                        f"(wrote {final_output_path}; media_retry_count={retry_count} "
                        f"next_retry_at={next_retry_at})"
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


def is_negative_infinite_loudnorm_value(raw_value: Any) -> bool:
    parsed = safe_float(raw_value)
    return parsed is not None and math.isinf(parsed) and parsed < 0


def is_silent_audio_loudness_error(error: str | None) -> bool:
    if not error:
        return False
    return str(error).strip().lower().startswith("silent audio detected")


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
        if is_negative_infinite_loudnorm_value(stats.get("input_i")):
            return None, "silent audio detected (input_i=-inf)"
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


def run_loudness_for_video(
    connection: sqlite3.Connection,
    source: SourceConfig,
    video_id: str,
    target_lufs: float = DEFAULT_LOUDNESS_TARGET_LUFS,
    max_boost_db: float = DEFAULT_LOUDNESS_MAX_BOOST_DB,
    max_cut_db: float = DEFAULT_LOUDNESS_MAX_CUT_DB,
    ffmpeg_bin: str = DEFAULT_LOUDNESS_FFMPEG_BIN,
) -> tuple[bool, str | None]:
    ffmpeg_candidate = Path(str(ffmpeg_bin)).expanduser()
    has_explicit_path = ffmpeg_candidate.is_absolute() or is_path_like_command(str(ffmpeg_bin))
    resolved_ffmpeg_bin = find_executable_command(str(ffmpeg_bin))
    if resolved_ffmpeg_bin is None:
        if has_explicit_path:
            return (False, f"ffmpeg binary not found: {ffmpeg_candidate}")
        return (
            False,
            f"ffmpeg binary '{ffmpeg_bin}' not found in PATH. Install ffmpeg or pass --ffmpeg-bin.",
        )
    ffmpeg_bin = resolved_ffmpeg_bin

    ffprobe_bin: str | None = None
    if has_explicit_path:
        sibling_ffprobe = Path(ffmpeg_bin).expanduser().with_name("ffprobe")
        if sibling_ffprobe.exists():
            ffprobe_bin = str(sibling_ffprobe)
    if ffprobe_bin is None:
        ffprobe_bin = find_executable_command("ffprobe")
    has_ffprobe = ffprobe_bin is not None

    row = connection.execute(
        """
        SELECT media_path
        FROM videos
        WHERE source_id = ?
          AND video_id = ?
          AND has_media = 1
          AND media_path IS NOT NULL
        LIMIT 1
        """,
        (source.id, video_id),
    ).fetchone()
    if row is None:
        return (False, f"{source.id}/{video_id}: missing ledger row or has_media=0")

    media_path = Path(str(row[0]))
    analyzed_at = now_utc_iso()
    safe_boost = max(0.0, float(max_boost_db))
    safe_cut = max(0.0, float(max_cut_db))

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
        retry_count, next_retry_at = mark_media_retry_state(
            connection=connection,
            source=source,
            video_id=video_id,
            reason="media file missing during loudness analysis",
            attempt_at=analyzed_at,
        )
        connection.commit()
        return (
            False,
            f"{source.id}/{video_id}: media file missing "
            f"(media_retry_count={retry_count} next_retry_at={next_retry_at})",
        )

    if has_ffprobe and ffprobe_bin is not None:
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
            retry_count, next_retry_at = mark_media_retry_state(
                connection=connection,
                source=source,
                video_id=video_id,
                reason="no audio stream detected during loudness analysis",
                attempt_at=analyzed_at,
            )
            connection.commit()
            print(
                f"[loudness] {source.id}/{video_id}: "
                "no audio stream (gain=+0.00dB; "
                f"media_retry_count={retry_count} "
                f"next_retry_at={next_retry_at})"
            )
            return (True, None)
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
        if is_silent_audio_loudness_error(error):
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
            retry_count, next_retry_at = mark_media_retry_state(
                connection=connection,
                source=source,
                video_id=video_id,
                reason="silent audio detected during loudness analysis",
                attempt_at=analyzed_at,
            )
            connection.commit()
            print(
                f"[loudness] {source.id}/{video_id}: "
                "silent audio (gain=+0.00dB; "
                f"media_retry_count={retry_count} "
                f"next_retry_at={next_retry_at})"
            )
            return (True, None)
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
        connection.commit()
        return (False, f"{source.id}/{video_id}: {error or 'loudness analysis failed'}")

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
    connection.commit()
    print(
        f"[loudness] {source.id}/{video_id}: "
        f"LUFS={input_lufs:.2f} gain={clipped_gain_db:+.2f}dB"
    )
    return (True, None)


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
    ffmpeg_candidate = Path(str(ffmpeg_bin)).expanduser()
    has_explicit_path = ffmpeg_candidate.is_absolute() or is_path_like_command(str(ffmpeg_bin))
    resolved_ffmpeg_bin = find_executable_command(str(ffmpeg_bin))
    if resolved_ffmpeg_bin is None:
        if has_explicit_path:
            raise RuntimeError(f"ffmpeg binary not found: {ffmpeg_candidate}")
        raise RuntimeError(
            f"ffmpeg binary '{ffmpeg_bin}' not found in PATH. "
            "Install ffmpeg or pass --ffmpeg-bin."
        )
    ffmpeg_bin = resolved_ffmpeg_bin
    ffprobe_bin: str | None = None
    if has_explicit_path:
        sibling_ffprobe = Path(ffmpeg_bin).expanduser().with_name("ffprobe")
        if sibling_ffprobe.exists():
            ffprobe_bin = str(sibling_ffprobe)
    if ffprobe_bin is None:
        ffprobe_bin = find_executable_command("ffprobe")
    has_ffprobe = ffprobe_bin is not None

    connection = sqlite3.connect(str(db_path), timeout=30)
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
                    retry_count, next_retry_at = mark_media_retry_state(
                        connection=connection,
                        source=source,
                        video_id=video_id,
                        reason="media file missing during loudness analysis",
                        attempt_at=analyzed_at,
                    )
                    source_missing += 1
                    print(
                        f"[loudness] {source.id}/{video_id}: media file missing "
                        f"(media_retry_count={retry_count} next_retry_at={next_retry_at})"
                    )
                    connection.commit()
                    continue

                if has_ffprobe:
                    has_audio_stream, probe_error = detect_audio_stream(
                        media_path=media_path,
                        ffprobe_bin=ffprobe_bin if ffprobe_bin is not None else "ffprobe",
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
                        retry_count, next_retry_at = mark_media_retry_state(
                            connection=connection,
                            source=source,
                            video_id=video_id,
                            reason="no audio stream detected during loudness analysis",
                            attempt_at=analyzed_at,
                        )
                        source_success += 1
                        print(
                            f"[loudness] {source.id}/{video_id}: "
                            "no audio stream (gain=+0.00dB; "
                            f"media_retry_count={retry_count} "
                            f"next_retry_at={next_retry_at})"
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
                    if is_silent_audio_loudness_error(error):
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
                        retry_count, next_retry_at = mark_media_retry_state(
                            connection=connection,
                            source=source,
                            video_id=video_id,
                            reason="silent audio detected during loudness analysis",
                            attempt_at=analyzed_at,
                        )
                        source_success += 1
                        print(
                            f"[loudness] {source.id}/{video_id}: "
                            "silent audio (gain=+0.00dB; "
                            f"media_retry_count={retry_count} "
                            f"next_retry_at={next_retry_at})"
                        )
                        connection.commit()
                        continue
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
    connection = sqlite3.connect(str(db_path), timeout=30)
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
    execution_mode: str = "legacy",
    metered_media_mode: str = DEFAULT_METERED_MEDIA_MODE,
    metered_min_archive_ids: int = DEFAULT_METERED_MIN_ARCHIVE_IDS,
    metered_playlist_end: int = DEFAULT_METERED_PLAYLIST_END,
) -> None:
    normalized_execution_mode = "queue" if str(execution_mode).strip().lower() == "queue" else "legacy"
    normalized_metered_mode = normalize_metered_media_mode(
        metered_media_mode,
        DEFAULT_METERED_MEDIA_MODE,
    )
    if normalized_metered_mode == "updates-only":
        print(
            "[backfill] metered updates-only mode: skip historical backfill windows"
        )
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path), timeout=30)
    create_schema(connection)
    any_work = False

    try:
        source_priority_stride = max(1, len(sources))
        for source_index, source in enumerate(sources):
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
                if normalized_execution_mode == "queue":
                    now_iso = now_utc_iso()
                    inserted, requeued, reprioritized, kept = enqueue_media_work_items(
                        connection=connection,
                        source_id=source.id,
                        video_ids=window_ids,
                        now_iso=now_iso,
                        source_slot=source_index,
                        source_stride=source_priority_stride,
                    )
                    connection.commit()
                    print(
                        f"[backfill-queue] {source.id}: queued media ids={len(window_ids)} "
                        f"(inserted={inserted}, requeued={requeued}, "
                        f"reprioritized={reprioritized}, kept={kept})"
                    )
                else:
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
                        respect_media_discovery_interval=False,
                        metered_media_mode=normalized_metered_mode,
                        metered_min_archive_ids=metered_min_archive_ids,
                        metered_playlist_end=metered_playlist_end,
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

    connection = sqlite3.connect(str(db_path), timeout=30)
    create_schema(connection)
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=since_hours)
    since_iso = since.replace(microsecond=0).isoformat()
    now_iso = now_utc_iso()

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

        queue_health_row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_count,
                COALESCE(SUM(CASE WHEN status = 'leased' THEN 1 ELSE 0 END), 0) AS leased_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND (next_retry_at IS NULL OR next_retry_at <= ?)
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_due_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND next_retry_at IS NOT NULL
                             AND next_retry_at > ?
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_wait_count,
                COALESCE(SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END), 0) AS dead_count
            FROM work_items
            WHERE source_id = ?
            """,
            (now_iso, now_iso, source.id),
        ).fetchone()
        if queue_health_row is not None:
            print(
                "queue summary (authoritative): "
                f"queued={int(queue_health_row[0] or 0)} "
                f"leased={int(queue_health_row[1] or 0)} "
                f"retry_due={int(queue_health_row[2] or 0)} "
                f"retry_wait={int(queue_health_row[3] or 0)} "
                f"dead={int(queue_health_row[4] or 0)}"
            )
        cooldown_active, cooldown_remaining_hours, cooldown_until, cooldown_reason = (
            get_source_network_cooldown_state(
                connection=connection,
                source_id=source.id,
            )
        )
        if cooldown_active:
            print(
                "source network cooldown: "
                f"until={cooldown_until or 'unknown'} "
                f"remaining={cooldown_remaining_hours:.2f}h"
            )
            if cooldown_reason:
                print(f"  reason: {cooldown_reason}")

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
            print("download_state errors: none")
        else:
            print("download_state errors (legacy stage state; queue truth is work_items):")
            for stage, video_id, retry_count, last_error, next_retry_at in failure_rows:
                print(
                    f"  stage={stage} video_id={video_id} retry_count={retry_count} "
                    f"next_retry_at={next_retry_at}"
                )
                if last_error:
                    print(f"    reason: {last_error}")

        queue_summary_rows = connection.execute(
            """
            SELECT stage, status, COUNT(*)
            FROM work_items
            WHERE source_id = ?
            GROUP BY stage, status
            ORDER BY stage ASC, status ASC
            """,
            (source.id,),
        ).fetchall()
        if queue_summary_rows:
            print("queue status counts:")
            for queue_stage, queue_status, queue_count in queue_summary_rows:
                print(
                    f"  stage={queue_stage} status={queue_status} "
                    f"count={int(queue_count or 0)}"
                )
        else:
            print("queue status counts: none")

        queue_pending_rows = connection.execute(
            """
            SELECT
                stage,
                video_id,
                status,
                attempt_count,
                COALESCE(next_retry_at, ''),
                COALESCE(last_error, '')
            FROM work_items
            WHERE source_id = ?
              AND status IN ('queued', 'leased', 'error')
            ORDER BY priority ASC, updated_at ASC, id ASC
            LIMIT ?
            """,
            (source.id, limit),
        ).fetchall()
        if queue_pending_rows:
            print("queue pending:")
            for (
                queue_stage,
                queue_video_id,
                queue_status,
                queue_attempt_count,
                queue_next_retry_at,
                queue_last_error,
            ) in queue_pending_rows:
                print(
                    f"  stage={queue_stage} video_id={queue_video_id} "
                    f"status={queue_status} attempts={int(queue_attempt_count or 0)} "
                    f"next_retry_at={queue_next_retry_at}"
                )
                if queue_last_error:
                    print(f"    reason: {queue_last_error}")
        else:
            print("queue pending: none")

    connection.close()


def show_queue_status_report(
    sources: list[SourceConfig],
    db_path: Path,
    limit: int = 20,
    only_unresolved: bool = False,
) -> None:
    if not db_path.exists():
        print(f"[queue-status] no ledger DB: {db_path}")
        return

    connection = sqlite3.connect(str(db_path), timeout=30)
    create_schema(connection)
    now_iso = now_utc_iso()

    for source in sources:
        summary_row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_count,
                COALESCE(SUM(CASE WHEN status = 'leased' THEN 1 ELSE 0 END), 0) AS leased_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND (next_retry_at IS NULL OR next_retry_at <= ?)
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_due_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND next_retry_at IS NOT NULL
                             AND next_retry_at > ?
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_wait_count,
                COALESCE(SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END), 0) AS dead_count
            FROM work_items
            WHERE source_id = ?
            """,
            (now_iso, now_iso, source.id),
        ).fetchone()
        unresolved_total = 0
        if summary_row is not None:
            unresolved_total = (
                int(summary_row[0] or 0)
                + int(summary_row[1] or 0)
                + int(summary_row[2] or 0)
                + int(summary_row[3] or 0)
                + int(summary_row[4] or 0)
            )
        if only_unresolved and unresolved_total <= 0:
            continue
        print(f"\n=== queue-status: {source.id} ===")
        cooldown_active, cooldown_remaining_hours, cooldown_until, cooldown_reason = (
            get_source_network_cooldown_state(
                connection=connection,
                source_id=source.id,
            )
        )
        if cooldown_active:
            print(
                "source network cooldown: "
                f"until={cooldown_until or 'unknown'} "
                f"remaining={cooldown_remaining_hours:.2f}h"
            )
            if cooldown_reason:
                print(f"  reason: {cooldown_reason}")
        if summary_row is not None:
            print(
                "queue unresolved total="
                f"{unresolved_total} "
                f"(queued={int(summary_row[0] or 0)} "
                f"leased={int(summary_row[1] or 0)} "
                f"retry_due={int(summary_row[2] or 0)} "
                f"retry_wait={int(summary_row[3] or 0)} "
                f"dead={int(summary_row[4] or 0)})"
            )

        by_stage_rows = connection.execute(
            """
            SELECT
                stage,
                COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_count,
                COALESCE(SUM(CASE WHEN status = 'leased' THEN 1 ELSE 0 END), 0) AS leased_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND (next_retry_at IS NULL OR next_retry_at <= ?)
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_due_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'error'
                             AND next_retry_at IS NOT NULL
                             AND next_retry_at > ?
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS retry_wait_count,
                COALESCE(SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END), 0) AS dead_count
            FROM work_items
            WHERE source_id = ?
            GROUP BY stage
            ORDER BY stage ASC
            """,
            (now_iso, now_iso, source.id),
        ).fetchall()
        if by_stage_rows:
            print("by stage:")
            for row in by_stage_rows:
                print(
                    f"  stage={row[0]} queued={int(row[1] or 0)} "
                    f"leased={int(row[2] or 0)} "
                    f"retry_due={int(row[3] or 0)} "
                    f"retry_wait={int(row[4] or 0)} "
                    f"dead={int(row[5] or 0)}"
                )
        else:
            print("by stage: none")

        recent_rows = connection.execute(
            """
            SELECT
                id,
                stage,
                video_id,
                status,
                attempt_count,
                COALESCE(next_retry_at, ''),
                COALESCE(last_error, ''),
                COALESCE(updated_at, '')
            FROM work_items
            WHERE source_id = ?
              AND status IN ('error', 'dead')
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (source.id, limit),
        ).fetchall()
        if recent_rows:
            print("recent failures:")
            for (
                item_id,
                stage,
                video_id,
                status,
                attempt_count,
                next_retry_at,
                last_error,
                updated_at,
            ) in recent_rows:
                print(
                    f"  id={int(item_id)} stage={stage} video_id={video_id} "
                    f"status={status} attempts={int(attempt_count or 0)} "
                    f"next_retry_at={next_retry_at} updated_at={updated_at}"
                )
                if last_error:
                    print(f"    reason: {last_error}")
        else:
            print("recent failures: none")

        recovered_summary_row = connection.execute(
            """
            SELECT
                COALESCE(COUNT(*), 0)
            FROM work_items
            WHERE source_id = ?
              AND status = 'success'
              AND attempt_count >= 2
            """,
            (source.id,),
        ).fetchone()
        recovered_total = (
            int(recovered_summary_row[0] or 0) if recovered_summary_row is not None else 0
        )
        print(f"recovered by retry total={recovered_total}")

        recovered_rows = connection.execute(
            """
            SELECT
                id,
                stage,
                video_id,
                attempt_count,
                COALESCE(updated_at, '')
            FROM work_items
            WHERE source_id = ?
              AND status = 'success'
              AND attempt_count >= 2
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (source.id, limit),
        ).fetchall()
        if recovered_rows:
            print("recent recovered:")
            for (
                item_id,
                stage,
                video_id,
                attempt_count,
                updated_at,
            ) in recovered_rows:
                retries = max(0, int(attempt_count or 0) - 1)
                print(
                    f"  id={int(item_id)} stage={stage} video_id={video_id} "
                    f"attempts={int(attempt_count or 0)} retries={retries} "
                    f"updated_at={updated_at}"
                )
        else:
            print("recent recovered: none")

    connection.close()


def requeue_work_items(
    sources: list[SourceConfig],
    db_path: Path,
    stages: list[str] | None = None,
    statuses: list[str] | None = None,
    error_contains: str | None = None,
    limit: int = 0,
    dry_run: bool = False,
    reset_attempts: bool = False,
) -> tuple[int, int]:
    if not db_path.exists():
        print(f"[queue-requeue] no ledger DB: {db_path}")
        return (0, 0)

    stage_filter = normalize_queue_stages(stages) if stages else []
    status_filter = [
        value
        for value in (statuses or ["error", "dead"])
        if value in {"queued", "leased", "error", "dead", "success"}
    ]
    if not status_filter:
        status_filter = ["error", "dead"]

    connection = sqlite3.connect(str(db_path), timeout=30)
    create_schema(connection)
    now_iso = now_utc_iso()
    total_selected = 0
    total_requeued = 0

    try:
        for source in sources:
            where_parts = ["source_id = ?"]
            params: list[Any] = [source.id]

            status_placeholders = ",".join("?" for _ in status_filter)
            where_parts.append(f"status IN ({status_placeholders})")
            params.extend(status_filter)

            if stage_filter:
                stage_placeholders = ",".join("?" for _ in stage_filter)
                where_parts.append(f"stage IN ({stage_placeholders})")
                params.extend(stage_filter)

            if error_contains:
                where_parts.append("COALESCE(last_error, '') LIKE ?")
                params.append(f"%{error_contains}%")

            select_sql = (
                """
                SELECT
                    id,
                    stage,
                    video_id,
                    status,
                    attempt_count,
                    COALESCE(last_error, '')
                FROM work_items
                WHERE
                """
                + " AND ".join(where_parts)
                + """
                ORDER BY updated_at ASC, id ASC
                """
            )
            select_params: list[Any] = list(params)
            if int(limit) > 0:
                select_sql += " LIMIT ?"
                select_params.append(max(1, int(limit)))

            rows = connection.execute(select_sql, select_params).fetchall()
            selected = len(rows)
            total_selected += selected

            print(
                f"[queue-requeue] source={source.id} selected={selected} "
                f"(dry_run={str(bool(dry_run)).lower()})"
            )
            preview_rows = rows[: min(5, selected)]
            for (
                item_id,
                stage,
                video_id,
                status,
                attempt_count,
                last_error,
            ) in preview_rows:
                print(
                    f"  id={int(item_id)} stage={stage} video_id={video_id} "
                    f"status={status} attempts={int(attempt_count or 0)}"
                )
                if last_error:
                    print(f"    reason: {last_error}")
            if selected > len(preview_rows):
                print(f"  ... and {selected - len(preview_rows)} more")

            if dry_run or selected <= 0:
                continue

            ids = [int(row[0]) for row in rows]
            id_placeholders = ",".join("?" for _ in ids)
            set_parts = [
                "status = 'queued'",
                "next_retry_at = NULL",
                "lease_owner = NULL",
                "lease_token = NULL",
                "lease_expires_at = NULL",
                "updated_at = ?",
            ]
            if reset_attempts:
                set_parts.append("attempt_count = 0")
            update_sql = (
                f"UPDATE work_items SET {', '.join(set_parts)} "
                f"WHERE id IN ({id_placeholders})"
            )
            update_params: list[Any] = [now_iso]
            update_params.extend(ids)
            updated = connection.execute(update_sql, update_params).rowcount
            connection.commit()
            total_requeued += int(updated or 0)
            print(
                f"[queue-requeue] source={source.id} requeued={int(updated or 0)} "
                f"(reset_attempts={str(bool(reset_attempts)).lower()})"
            )
    finally:
        connection.close()

    if dry_run:
        print(
            f"[queue-requeue] dry-run complete: matched={total_selected} "
            f"statuses={','.join(status_filter)}"
        )
    else:
        print(
            f"[queue-requeue] complete: selected={total_selected} requeued={total_requeued} "
            f"statuses={','.join(status_filter)} reset_attempts={str(bool(reset_attempts)).lower()}"
        )
    return (total_selected, total_requeued)


QUEUE_RECOVERY_PROFILES: dict[str, dict[str, Any]] = {
    "translate-row-factory": {
        "description": "Recover translate queue items that failed from tuple-index row access regression",
        "stages": ["translate"],
        "statuses": ["error", "dead"],
        "error_contains": "tuple indices must be integers or slices, not str",
    },
}


def run_queue_recover_known(
    sources: list[SourceConfig],
    db_path: Path,
    profiles: list[str] | None = None,
    limit: int = 0,
    dry_run: bool = False,
    reset_attempts: bool = False,
) -> None:
    requested_profiles = [str(value).strip() for value in (profiles or []) if str(value).strip()]
    if not requested_profiles or "all" in requested_profiles:
        selected_profiles = sorted(QUEUE_RECOVERY_PROFILES.keys())
    else:
        selected_profiles = [name for name in requested_profiles if name in QUEUE_RECOVERY_PROFILES]
    if not selected_profiles:
        print("[queue-recover-known] no valid profiles selected")
        return

    total_selected = 0
    total_requeued = 0
    for profile_name in selected_profiles:
        profile = QUEUE_RECOVERY_PROFILES[profile_name]
        print(
            f"[queue-recover-known] profile={profile_name} "
            f"description={profile.get('description', '')}"
        )
        selected, requeued = requeue_work_items(
            sources=sources,
            db_path=db_path,
            stages=cast(list[str], profile.get("stages") or []),
            statuses=cast(list[str], profile.get("statuses") or []),
            error_contains=str(profile.get("error_contains") or "").strip() or None,
            limit=max(0, int(limit)),
            dry_run=bool(dry_run),
            reset_attempts=bool(reset_attempts),
        )
        total_selected += int(selected or 0)
        total_requeued += int(requeued or 0)

    if dry_run:
        print(
            f"[queue-recover-known] dry-run complete: profiles={len(selected_profiles)} "
            f"matched={total_selected}"
        )
    else:
        print(
            f"[queue-recover-known] complete: profiles={len(selected_profiles)} "
            f"selected={total_selected} requeued={total_requeued}"
        )


def run_dict_bookmarks_export(
    db_path: Path,
    source_ids: list[str],
    output_path: Path,
    output_format: str,
    entry_status: str,
    limit: int,
    video_ids: list[str] | None = None,
) -> None:
    if output_format not in {"jsonl", "csv"}:
        raise ValueError("output_format must be jsonl or csv")
    if entry_status not in {"all", "missing", "known"}:
        raise ValueError("entry_status must be all/missing/known")
    safe_limit = max(0, int(limit))
    normalized_video_ids = [
        str(video_id).strip()
        for video_id in (video_ids or [])
        if str(video_id).strip()
    ]

    connection = sqlite3.connect(str(db_path), timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        where_clauses: list[str] = []
        params: list[Any] = []
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            where_clauses.append(f"source_id IN ({placeholders})")
            params.extend(source_ids)
        if normalized_video_ids:
            placeholders = ",".join("?" for _ in normalized_video_ids)
            where_clauses.append(f"video_id IN ({placeholders})")
            params.extend(normalized_video_ids)
        if entry_status == "missing":
            where_clauses.append("missing_entry = 1")
        elif entry_status == "known":
            where_clauses.append("missing_entry = 0")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        limit_sql = ""
        if safe_limit > 0:
            limit_sql = "LIMIT ?"
            params.append(safe_limit)

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
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()
    finally:
        connection.close()

    records = [serialize_dictionary_bookmark_row(row) for row in rows]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "jsonl":
        with output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")
    else:
        fieldnames = [
            "id",
            "source_id",
            "video_id",
            "track",
            "cue_start_ms",
            "cue_end_ms",
            "cue_text",
            "dict_entry_id",
            "dict_source_name",
            "lookup_term",
            "term",
            "term_norm",
            "definition",
            "missing_entry",
            "lookup_path_json",
            "lookup_path_label",
            "created_at",
            "updated_at",
        ]
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        "id": record["id"],
                        "source_id": record["source_id"],
                        "video_id": record["video_id"],
                        "track": record["track"],
                        "cue_start_ms": record["cue_start_ms"],
                        "cue_end_ms": record["cue_end_ms"],
                        "cue_text": record["cue_text"],
                        "dict_entry_id": record["dict_entry_id"],
                        "dict_source_name": record["dict_source_name"],
                        "lookup_term": record["lookup_term"],
                        "term": record["term"],
                        "term_norm": record["term_norm"],
                        "definition": record["definition"],
                        "missing_entry": 1 if record["missing_entry"] else 0,
                        "lookup_path_json": json.dumps(record["lookup_path"], ensure_ascii=False),
                        "lookup_path_label": record["lookup_path_label"],
                        "created_at": record["created_at"],
                        "updated_at": record["updated_at"],
                    }
                )

    missing_count = sum(1 for record in records if record["missing_entry"])
    known_count = len(records) - missing_count
    print(
        "[dict-bookmarks-export] "
        f"rows={len(records)} missing={missing_count} known={known_count} "
        f"format={output_format} output={output_path}"
    )


def parse_bool_like(value: Any, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_dict_bookmark_import_rows(input_path: Path, input_format: str) -> list[dict[str, Any]]:
    if input_format == "jsonl":
        rows: list[dict[str, Any]] = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line_index, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSONL at line {line_index}: {exc}"
                    ) from exc
                if not isinstance(parsed, dict):
                    raise ValueError(
                        f"Invalid JSONL at line {line_index}: expected object."
                    )
                rows.append(parsed)
        return rows

    if input_format == "csv":
        rows = []
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for parsed in reader:
                rows.append(dict(parsed))
        return rows

    raise ValueError("input_format must be jsonl or csv")


def write_records_as_jsonl_or_csv(
    records: list[dict[str, Any]],
    output_path: Path,
    output_format: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "jsonl":
        with output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")
        return

    if output_format != "csv":
        raise ValueError("output_format must be jsonl or csv")

    fieldnames: list[str] = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def normalize_dict_bookmark_import_row(
    raw_row: dict[str, Any],
    row_index: int,
    input_path: Path,
) -> dict[str, Any]:
    source_id = str(raw_row.get("source_id") or "").strip()
    video_id = str(raw_row.get("video_id") or "").strip()
    track = str(raw_row.get("track") or "").strip()
    term = str(raw_row.get("term") or "").strip()
    term_norm = normalize_dictionary_term(raw_row.get("term_norm") or term)
    lookup_term = str(raw_row.get("lookup_term") or "").strip()
    definition = str(raw_row.get("definition") or "").strip()
    dict_source_name = str(raw_row.get("dict_source_name") or "").strip()
    cue_text = str(raw_row.get("cue_text") or "")
    missing_entry = parse_bool_like(raw_row.get("missing_entry"), default=False)
    created_at = str(raw_row.get("created_at") or "").strip() or now_utc_iso()
    updated_at = str(raw_row.get("updated_at") or "").strip() or now_utc_iso()
    lookup_path_label = str(raw_row.get("lookup_path_label") or "").strip()

    if not source_id:
        raise ValueError(f"{input_path}:{row_index}: source_id is required.")
    if not video_id:
        raise ValueError(f"{input_path}:{row_index}: video_id is required.")
    if not term:
        raise ValueError(f"{input_path}:{row_index}: term is required.")
    if not term_norm:
        raise ValueError(f"{input_path}:{row_index}: term_norm is required.")

    try:
        cue_start_ms = int(raw_row.get("cue_start_ms"))
        cue_end_ms = int(raw_row.get("cue_end_ms"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{input_path}:{row_index}: cue_start_ms and cue_end_ms must be integers."
        ) from exc

    if cue_end_ms < cue_start_ms:
        cue_start_ms, cue_end_ms = cue_end_ms, cue_start_ms
    cue_start_ms = max(0, cue_start_ms)
    cue_end_ms = max(cue_start_ms, cue_end_ms)

    raw_lookup_path = raw_row.get("lookup_path")
    if raw_lookup_path in (None, ""):
        raw_lookup_path_json = raw_row.get("lookup_path_json")
        if raw_lookup_path_json not in (None, ""):
            if isinstance(raw_lookup_path_json, str):
                try:
                    raw_lookup_path = json.loads(raw_lookup_path_json)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"{input_path}:{row_index}: lookup_path_json is invalid JSON."
                    ) from exc
            else:
                raw_lookup_path = raw_lookup_path_json

    lookup_path = normalize_dictionary_lookup_path(raw_lookup_path)
    if not lookup_path:
        base_term = lookup_term or term
        base_norm = normalize_dictionary_term(base_term)
        if base_term or base_norm:
            lookup_path = [
                {
                    "level": 1,
                    "term": base_term or base_norm,
                    "term_norm": base_norm,
                    "source": "import",
                }
            ]
    if not lookup_path_label:
        lookup_path_label = build_dictionary_lookup_path_label(lookup_path)

    if missing_entry:
        dict_entry_id = make_missing_dict_entry_id(term_norm)
        if not definition:
            definition = "辞書エントリが見つかりません。"
        if not lookup_term:
            lookup_term = term
    else:
        try:
            dict_entry_id = int(raw_row.get("dict_entry_id"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{input_path}:{row_index}: dict_entry_id must be a positive integer for known entries."
            ) from exc
        if dict_entry_id <= 0:
            raise ValueError(
                f"{input_path}:{row_index}: dict_entry_id must be a positive integer for known entries."
            )
        if not definition:
            raise ValueError(f"{input_path}:{row_index}: definition is required.")

    lookup_path_json = ""
    if lookup_path:
        lookup_path_json = json.dumps(lookup_path, ensure_ascii=False, separators=(",", ":"))

    return {
        "source_id": source_id,
        "video_id": video_id,
        "track": track,
        "cue_start_ms": cue_start_ms,
        "cue_end_ms": cue_end_ms,
        "cue_text": cue_text,
        "dict_entry_id": dict_entry_id,
        "dict_source_name": dict_source_name,
        "lookup_term": lookup_term,
        "term": term,
        "term_norm": term_norm,
        "definition": definition,
        "missing_entry": 1 if missing_entry else 0,
        "lookup_path_json": lookup_path_json,
        "lookup_path_label": lookup_path_label,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def run_dict_bookmarks_import(
    db_path: Path,
    source_ids: list[str],
    input_path: Path,
    input_format: str,
    on_duplicate: str,
    dry_run: bool,
) -> None:
    if input_format not in {"jsonl", "csv"}:
        raise ValueError("input_format must be jsonl or csv")
    if on_duplicate not in {"skip", "upsert", "error"}:
        raise ValueError("on_duplicate must be skip/upsert/error")
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    started_at = now_utc_iso()
    source_scope = ",".join(sorted({str(item).strip() for item in source_ids if str(item).strip()}))
    raw_rows: list[dict[str, Any]] = []
    allowed_sources = set(source_ids)
    seen_composites: set[tuple[Any, ...]] = set()

    connection = sqlite3.connect(str(db_path), timeout=30)
    connection.row_factory = sqlite3.Row
    create_schema(connection)
    connection.commit()

    row_count = 0
    inserted = 0
    updated = 0
    skipped = 0
    errors = 0
    status = "completed"
    error_message = ""

    def write_import_run_log() -> None:
        connection.execute(
            """
            INSERT INTO dictionary_import_runs (
                source_scope,
                input_path,
                input_format,
                on_duplicate,
                dry_run,
                row_count,
                inserted_count,
                updated_count,
                skipped_count,
                error_count,
                status,
                error_message,
                started_at,
                finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_scope,
                str(input_path),
                input_format,
                on_duplicate,
                1 if dry_run else 0,
                row_count,
                inserted,
                updated,
                skipped,
                errors,
                status,
                error_message,
                started_at,
                now_utc_iso(),
            ),
        )

    try:
        raw_rows = load_dict_bookmark_import_rows(input_path, input_format)
        row_count = len(raw_rows)
        if not raw_rows:
            status = "noop"
            print(f"[dict-bookmarks-import] no rows in {input_path}")
            return

        for row_index, raw_row in enumerate(raw_rows, start=1):
            try:
                record = normalize_dict_bookmark_import_row(raw_row, row_index, input_path)
            except ValueError as exc:
                errors += 1
                print(f"[dict-bookmarks-import] row {row_index}: {exc}", file=sys.stderr)
                continue

            if allowed_sources and record["source_id"] not in allowed_sources:
                skipped += 1
                continue

            composite_key = (
                record["source_id"],
                record["video_id"],
                record["track"],
                record["cue_start_ms"],
                record["cue_end_ms"],
                record["dict_entry_id"],
            )
            if composite_key in seen_composites and on_duplicate == "skip":
                skipped += 1
                continue
            seen_composites.add(composite_key)

            existing = connection.execute(
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
                composite_key,
            ).fetchone()

            if existing is None:
                inserted += 1
                if dry_run:
                    continue
                connection.execute(
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
                        record["source_id"],
                        record["video_id"],
                        record["track"],
                        record["cue_start_ms"],
                        record["cue_end_ms"],
                        record["cue_text"],
                        record["dict_entry_id"],
                        record["dict_source_name"],
                        record["lookup_term"],
                        record["term"],
                        record["term_norm"],
                        record["definition"],
                        record["missing_entry"],
                        record["lookup_path_json"],
                        record["lookup_path_label"],
                        record["created_at"],
                        record["updated_at"],
                    ),
                )
                continue

            if on_duplicate == "error":
                errors += 1
                print(
                    "[dict-bookmarks-import] "
                    f"row {row_index}: duplicate composite key exists id={existing['id']}",
                    file=sys.stderr,
                )
                continue
            if on_duplicate == "skip":
                skipped += 1
                continue

            # upsert mode: avoid destructive overwrite by keeping non-empty existing values.
            merged_missing = int(existing["missing_entry"])
            if int(record["missing_entry"]) == 0:
                merged_missing = 0
            merged_definition = str(record["definition"] or "").strip() or str(existing["definition"] or "")
            if merged_missing == 1 and str(existing["definition"] or "").strip() and merged_definition == "辞書エントリが見つかりません。":
                merged_definition = str(existing["definition"])
            merged_cue_text = str(record["cue_text"] or "") or str(existing["cue_text"] or "")
            merged_dict_source_name = str(record["dict_source_name"] or "").strip() or str(existing["dict_source_name"] or "")
            merged_lookup_term = str(record["lookup_term"] or "").strip() or str(existing["lookup_term"] or "")
            merged_lookup_path_json = (
                str(record["lookup_path_json"] or "")
                if str(record["lookup_path_json"] or "")
                else str(existing["lookup_path_json"] or "")
            )
            merged_lookup_path_label = (
                str(record["lookup_path_label"] or "").strip()
                if str(record["lookup_path_label"] or "").strip()
                else str(existing["lookup_path_label"] or "")
            )

            updated += 1
            if dry_run:
                continue
            connection.execute(
                """
                UPDATE dictionary_bookmarks
                SET
                    cue_text = ?,
                    dict_source_name = ?,
                    lookup_term = ?,
                    term = ?,
                    term_norm = ?,
                    definition = ?,
                    missing_entry = ?,
                    lookup_path_json = ?,
                    lookup_path_label = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    merged_cue_text,
                    merged_dict_source_name,
                    merged_lookup_term,
                    record["term"],
                    record["term_norm"],
                    merged_definition,
                    merged_missing,
                    merged_lookup_path_json,
                    merged_lookup_path_label,
                    now_utc_iso(),
                    int(existing["id"]),
                ),
            )
        if errors > 0:
            status = "completed_with_errors"
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        raise
    finally:
        if dry_run:
            connection.rollback()
        else:
            connection.commit()
        try:
            write_import_run_log()
            connection.commit()
        except sqlite3.Error as exc:
            print(f"[dict-bookmarks-import] failed to write run log: {exc}", file=sys.stderr)
        connection.close()

    print(
        "[dict-bookmarks-import] "
        f"rows={row_count} inserted={inserted} updated={updated} skipped={skipped} "
        f"errors={errors} dry_run={str(dry_run).lower()} input={input_path}"
    )


def collect_dictionary_term_history_stats(
    connection: sqlite3.Connection,
    source_ids: list[str],
) -> dict[str, dict[str, Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        where_clauses.append(f"source_id IN ({placeholders})")
        params.extend(source_ids)
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    rows = connection.execute(
        f"""
        SELECT
            term_norm,
            MAX(CASE
                WHEN TRIM(COALESCE(term, '')) != '' THEN term
                ELSE term_norm
            END) AS term,
            COUNT(*) AS bookmark_count,
            COUNT(DISTINCT source_id || ':' || video_id) AS video_count,
            SUM(CASE WHEN missing_entry = 1 THEN 1 ELSE 0 END) AS missing_count,
            MIN(created_at) AS first_seen_at,
            MAX(updated_at) AS last_seen_at
        FROM dictionary_bookmarks
        {where_sql}
        GROUP BY term_norm
        """,
        tuple(params),
    ).fetchall()

    now_value = dt.datetime.now(dt.timezone.utc)
    stats_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        term_norm = str(row["term_norm"] or "")
        bookmark_count = int(row["bookmark_count"] or 0)
        video_count = int(row["video_count"] or 0)
        missing_count = int(row["missing_count"] or 0)
        first_seen_at = str(row["first_seen_at"] or "")
        last_seen_at = str(row["last_seen_at"] or "")
        reencounter_count = max(0, bookmark_count - 1)
        score = compute_review_priority_score(
            bookmark_count=bookmark_count,
            video_count=video_count,
            missing_count=missing_count,
            last_seen_at=last_seen_at,
            now_utc=now_value,
        )
        stats_map[term_norm] = {
            "term_norm": term_norm,
            "term": str(row["term"] or term_norm),
            "bookmark_count": bookmark_count,
            "video_count": video_count,
            "missing_count": missing_count,
            "reencounter_count": reencounter_count,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
            "review_priority": score,
        }
    return stats_map


def run_dict_bookmarks_curate(
    db_path: Path,
    source_ids: list[str],
    preset: str,
    output_path: Path,
    output_format: str,
    limit: int,
    min_bookmarks: int,
    min_videos: int,
) -> None:
    if output_format not in {"jsonl", "csv"}:
        raise ValueError("output_format must be jsonl or csv")
    if preset not in {"missing_review", "frequent_terms", "recent_saved", "review_cards"}:
        raise ValueError("preset must be missing_review/frequent_terms/recent_saved/review_cards")

    safe_limit = max(1, int(limit))
    safe_min_bookmarks = max(1, int(min_bookmarks))
    safe_min_videos = max(1, int(min_videos))

    connection = sqlite3.connect(str(db_path), timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        term_stats = collect_dictionary_term_history_stats(connection, source_ids=source_ids)
        where_clauses: list[str] = []
        params: list[Any] = []
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            where_clauses.append(f"db.source_id IN ({placeholders})")
            params.extend(source_ids)
        if preset == "missing_review":
            where_clauses.append("db.missing_entry = 1")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        records: list[dict[str, Any]] = []
        if preset in {"missing_review", "recent_saved", "review_cards"}:
            rows = connection.execute(
                f"""
                SELECT
                    db.id,
                    db.source_id,
                    db.video_id,
                    db.track,
                    db.cue_start_ms,
                    db.cue_end_ms,
                    db.cue_text,
                    db.dict_entry_id,
                    db.dict_source_name,
                    db.lookup_term,
                    db.term,
                    db.term_norm,
                    db.definition,
                    db.missing_entry,
                    db.lookup_path_json,
                    db.lookup_path_label,
                    db.created_at,
                    db.updated_at,
                    COALESCE(v.webpage_url, '') AS webpage_url
                FROM dictionary_bookmarks db
                LEFT JOIN videos v
                  ON v.source_id = db.source_id
                 AND v.video_id = db.video_id
                {where_sql}
                ORDER BY db.updated_at DESC, db.id DESC
                LIMIT ?
                """,
                (*params, safe_limit),
            ).fetchall()

            ja_cues_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

            def resolve_ja_cues(source_id: str, video_id: str) -> list[dict[str, Any]]:
                key = (source_id, video_id)
                if key in ja_cues_cache:
                    return ja_cues_cache[key]
                ja_path = resolve_ja_subtitle_path(connection, source_id, video_id)
                if ja_path is None:
                    ja_cues_cache[key] = []
                else:
                    ja_cues_cache[key] = parse_subtitle_cues(ja_path)
                return ja_cues_cache[key]

            for row in rows:
                serialized = serialize_dictionary_bookmark_row(row)
                stats = term_stats.get(serialized["term_norm"], {})
                if preset == "review_cards":
                    ja_text = find_best_subtitle_text_for_range(
                        resolve_ja_cues(serialized["source_id"], serialized["video_id"]),
                        serialized["cue_start_ms"],
                        serialized["cue_end_ms"],
                    )
                    local_jump_url = (
                        f"http://{DEFAULT_WEB_HOST}:{DEFAULT_WEB_PORT}/?"
                        + urlencode(
                            {
                                "source_id": serialized["source_id"],
                                "video_id": serialized["video_id"],
                                "t": str(max(0, int(round(serialized["cue_start_ms"] / 1000)))),
                            }
                        )
                    )
                    records.append(
                        {
                            "card_format_version": "v1",
                            "card_id": f"dictbm:{serialized['id']}",
                            "source_id": serialized["source_id"],
                            "video_id": serialized["video_id"],
                            "cue_start_ms": serialized["cue_start_ms"],
                            "cue_end_ms": serialized["cue_end_ms"],
                            "cue_start_label": format_ms_to_clock(serialized["cue_start_ms"]),
                            "cue_end_label": format_ms_to_clock(serialized["cue_end_ms"]),
                            "cue_en_text": serialized["cue_text"],
                            "cue_ja_text": ja_text,
                            "term": serialized["term"],
                            "term_norm": serialized["term_norm"],
                            "lookup_term": serialized["lookup_term"],
                            "definition": serialized["definition"],
                            "missing_entry": 1 if serialized["missing_entry"] else 0,
                            "lookup_path_label": serialized["lookup_path_label"],
                            "bookmark_count": int(stats.get("bookmark_count", 1)),
                            "video_count": int(stats.get("video_count", 1)),
                            "reencounter_count": int(stats.get("reencounter_count", 0)),
                            "review_priority": float(stats.get("review_priority", 0.0)),
                            "local_jump_url": local_jump_url,
                            "webpage_url": str(row["webpage_url"] or ""),
                            "created_at": serialized["created_at"],
                            "updated_at": serialized["updated_at"],
                        }
                    )
                else:
                    records.append(
                        {
                            "id": serialized["id"],
                            "source_id": serialized["source_id"],
                            "video_id": serialized["video_id"],
                            "track": serialized["track"],
                            "cue_start_ms": serialized["cue_start_ms"],
                            "cue_end_ms": serialized["cue_end_ms"],
                            "cue_start_label": format_ms_to_clock(serialized["cue_start_ms"]),
                            "cue_end_label": format_ms_to_clock(serialized["cue_end_ms"]),
                            "cue_text": serialized["cue_text"],
                            "lookup_term": serialized["lookup_term"],
                            "term": serialized["term"],
                            "term_norm": serialized["term_norm"],
                            "definition": serialized["definition"],
                            "missing_entry": 1 if serialized["missing_entry"] else 0,
                            "lookup_path_label": serialized["lookup_path_label"],
                            "bookmark_count": int(stats.get("bookmark_count", 1)),
                            "video_count": int(stats.get("video_count", 1)),
                            "reencounter_count": int(stats.get("reencounter_count", 0)),
                            "review_priority": float(stats.get("review_priority", 0.0)),
                            "updated_at": serialized["updated_at"],
                            "created_at": serialized["created_at"],
                        }
                    )
        else:
            rows = [
                stats
                for stats in term_stats.values()
                if int(stats.get("bookmark_count", 0)) >= safe_min_bookmarks
                and int(stats.get("video_count", 0)) >= safe_min_videos
            ]
            rows.sort(
                key=lambda item: (
                    float(item.get("review_priority", 0.0)),
                    int(item.get("bookmark_count", 0)),
                    int(item.get("video_count", 0)),
                    str(item.get("last_seen_at", "")),
                ),
                reverse=True,
            )
            for stats in rows[:safe_limit]:
                records.append(
                    {
                        "term_norm": str(stats.get("term_norm") or ""),
                        "term": str(stats.get("term") or stats.get("term_norm") or ""),
                        "bookmark_count": int(stats.get("bookmark_count", 0)),
                        "video_count": int(stats.get("video_count", 0)),
                        "missing_count": int(stats.get("missing_count", 0)),
                        "reencounter_count": int(stats.get("reencounter_count", 0)),
                        "first_seen_at": str(stats.get("first_seen_at", "")),
                        "last_seen_at": str(stats.get("last_seen_at", "")),
                        "review_priority": float(stats.get("review_priority", 0.0)),
                    }
                )

    finally:
        connection.close()

    write_records_as_jsonl_or_csv(records, output_path, output_format)
    print(
        "[dict-bookmarks-curate] "
        f"preset={preset} rows={len(records)} format={output_format} output={output_path}"
    )


def fetch_top_review_notification_item(
    connection: sqlite3.Connection,
    source_ids: list[str],
    web_url_base: str,
) -> dict[str, Any] | None:
    where_clauses: list[str] = []
    params: list[Any] = []
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        where_clauses.append(f"db.source_id IN ({placeholders})")
        params.extend(source_ids)
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    rows = connection.execute(
        f"""
        SELECT
            db.id,
            db.source_id,
            db.video_id,
            db.cue_start_ms,
            db.cue_end_ms,
            db.cue_text,
            db.lookup_term,
            db.term,
            db.term_norm,
            db.missing_entry,
            db.updated_at,
            db.created_at,
            db.definition,
            COALESCE(v.webpage_url, '') AS webpage_url
        FROM dictionary_bookmarks db
        LEFT JOIN videos v
          ON v.source_id = db.source_id
         AND v.video_id = db.video_id
        {where_sql}
        ORDER BY db.updated_at DESC, db.id DESC
        LIMIT 400
        """,
        tuple(params),
    ).fetchall()
    if not rows:
        return None

    term_stats = collect_dictionary_term_history_stats(connection, source_ids=source_ids)
    best_item: dict[str, Any] | None = None
    best_score: float | None = None
    for row in rows:
        term_norm = str(row["term_norm"] or "")
        stats = term_stats.get(term_norm, {})
        score = float(stats.get("review_priority", 0.0))
        missing_entry = bool(int(row["missing_entry"] or 0))
        if missing_entry:
            score += 1.0
        if best_score is None or score > best_score:
            cue_start_ms = int(row["cue_start_ms"] or 0)
            cue_end_ms = int(row["cue_end_ms"] or cue_start_ms)
            best_score = score
            best_item = {
                "id": int(row["id"]),
                "source_id": str(row["source_id"] or ""),
                "video_id": str(row["video_id"] or ""),
                "cue_start_ms": cue_start_ms,
                "cue_end_ms": cue_end_ms,
                "cue_start_label": format_ms_to_clock(cue_start_ms),
                "cue_end_label": format_ms_to_clock(cue_end_ms),
                "cue_text": str(row["cue_text"] or ""),
                "term": str(row["term"] or term_norm),
                "lookup_term": str(row["lookup_term"] or ""),
                "missing_entry": missing_entry,
                "review_priority": score,
                "webpage_url": str(row["webpage_url"] or ""),
            }

    if best_item is None:
        return None
    best_item["local_jump_url"] = build_local_jump_url(
        web_url_base=web_url_base,
        source_id=best_item["source_id"],
        video_id=best_item["video_id"],
        cue_start_ms=best_item["cue_start_ms"],
    )
    return best_item


def fetch_llm_unread_notification_item(
    connection: sqlite3.Connection,
    source_ids: list[str],
    since_iso: str,
    web_url_base: str,
) -> dict[str, Any] | None:
    where_clauses = [
        "db.updated_at > ?",
        "COALESCE(db.updated_at, '') != COALESCE(db.created_at, '')",
        "db.missing_entry = 0",
    ]
    params: list[Any] = [since_iso]
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        where_clauses.append(f"db.source_id IN ({placeholders})")
        params.extend(source_ids)

    where_sql = "WHERE " + " AND ".join(where_clauses)
    count_row = connection.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM dictionary_bookmarks db
        {where_sql}
        """,
        tuple(params),
    ).fetchone()
    unread_count = int(count_row["cnt"] if isinstance(count_row, sqlite3.Row) else count_row[0]) if count_row else 0
    if unread_count <= 0:
        return None

    row = connection.execute(
        f"""
        SELECT
            db.id,
            db.source_id,
            db.video_id,
            db.cue_start_ms,
            db.cue_end_ms,
            db.cue_text,
            db.lookup_term,
            db.term,
            db.updated_at,
            COALESCE(v.webpage_url, '') AS webpage_url
        FROM dictionary_bookmarks db
        LEFT JOIN videos v
          ON v.source_id = db.source_id
         AND v.video_id = db.video_id
        {where_sql}
        ORDER BY db.updated_at DESC, db.id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None

    source_id = str(row["source_id"])
    video_id = str(row["video_id"])
    cue_start_ms = int(row["cue_start_ms"] or 0)
    return {
        "unread_count": unread_count,
        "source_id": source_id,
        "video_id": video_id,
        "cue_start_ms": cue_start_ms,
        "cue_start_label": format_ms_to_clock(cue_start_ms),
        "cue_end_label": format_ms_to_clock(int(row["cue_end_ms"] or cue_start_ms)),
        "cue_text": str(row["cue_text"] or ""),
        "term": str(row["term"] or ""),
        "lookup_term": str(row["lookup_term"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "webpage_url": str(row["webpage_url"] or ""),
        "local_jump_url": build_local_jump_url(
            web_url_base=web_url_base,
            source_id=source_id,
            video_id=video_id,
            cue_start_ms=cue_start_ms,
        ),
    }


def run_notify(
    db_path: Path,
    source_ids: list[str],
    kind: str,
    web_url_base: str,
    llm_lookback_hours: int,
    cooldown_minutes: int,
    dry_run: bool,
) -> None:
    if kind not in {"review", "llm", "all"}:
        raise ValueError("kind must be review/llm/all")

    now_iso = now_utc_iso()
    now_dt = parse_iso_datetime_utc(now_iso) or dt.datetime.now(dt.timezone.utc)
    lookback_hours = max(1, int(llm_lookback_hours))
    llm_state_key = "notify.llm.last_checked_at"
    review_sent_key = "notify.review.last_sent_at"
    review_item_key_state = "notify.review.last_item_key"
    llm_sent_key = "notify.llm.last_sent_at"
    llm_item_key_state = "notify.llm.last_item_key"
    sent_events: list[str] = []

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path), timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        create_schema(connection)

        if kind in {"review", "all"}:
            review_item = fetch_top_review_notification_item(
                connection=connection,
                source_ids=source_ids,
                web_url_base=web_url_base,
            )
            if review_item:
                title = "復習しましょう！"
                term_display = review_item["lookup_term"] or review_item["term"] or "(term)"
                message = (
                    f"{term_display} | "
                    f"{review_item['cue_start_label']}-{review_item['cue_end_label']}"
                )
                review_item_key = build_notification_item_key(
                    "review",
                    (
                        review_item["source_id"],
                        review_item["video_id"],
                        str(int(review_item["cue_start_ms"])),
                        review_item["lookup_term"] or review_item["term"] or "",
                    ),
                )
                if is_notification_duplicate_within_cooldown(
                    connection=connection,
                    sent_at_state_key=review_sent_key,
                    item_key_state_key=review_item_key_state,
                    item_key=review_item_key,
                    cooldown_minutes=cooldown_minutes,
                    now_dt=now_dt,
                    dry_run=dry_run,
                ):
                    print(
                        "[notify] review skipped: duplicate within cooldown "
                        f"({max(0, int(cooldown_minutes))}m)"
                    )
                elif dry_run:
                    print(
                        "[notify] dry-run review "
                        f"url={review_item['local_jump_url']} message={message}"
                    )
                else:
                    ok, backend = run_macos_notification(
                        title=title,
                        message=message,
                        open_url=review_item["local_jump_url"],
                        group="substudy-review",
                    )
                    status = "sent" if ok else "failed"
                    print(
                        f"[notify] review {status} backend={backend} "
                        f"url={review_item['local_jump_url']}"
                    )
                    if ok:
                        set_app_state_value(connection, review_sent_key, now_iso)
                        set_app_state_value(
                            connection,
                            review_item_key_state,
                            review_item_key,
                        )
                sent_events.append("review")
            else:
                print("[notify] review skipped: no dictionary bookmarks.")

        if kind in {"llm", "all"}:
            saved_since = get_app_state_value(connection, llm_state_key, default="")
            if saved_since:
                since_iso = saved_since
            else:
                since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)
                since_iso = since_dt.replace(microsecond=0).isoformat()
            llm_item = fetch_llm_unread_notification_item(
                connection=connection,
                source_ids=source_ids,
                since_iso=since_iso,
                web_url_base=web_url_base,
            )
            if llm_item:
                title = "LLMが解説を追加しました"
                message = f"未読があります（{llm_item['unread_count']}件）"
                llm_item_key = build_notification_item_key(
                    "llm",
                    (
                        llm_item["source_id"],
                        llm_item["video_id"],
                        str(int(llm_item["cue_start_ms"])),
                        str(int(llm_item["unread_count"])),
                        llm_item["updated_at"],
                    ),
                )
                if is_notification_duplicate_within_cooldown(
                    connection=connection,
                    sent_at_state_key=llm_sent_key,
                    item_key_state_key=llm_item_key_state,
                    item_key=llm_item_key,
                    cooldown_minutes=cooldown_minutes,
                    now_dt=now_dt,
                    dry_run=dry_run,
                ):
                    print(
                        "[notify] llm skipped: duplicate within cooldown "
                        f"({max(0, int(cooldown_minutes))}m)"
                    )
                elif dry_run:
                    print(
                        "[notify] dry-run llm "
                        f"url={llm_item['local_jump_url']} count={llm_item['unread_count']}"
                    )
                else:
                    ok, backend = run_macos_notification(
                        title=title,
                        message=message,
                        open_url=llm_item["local_jump_url"],
                        group="substudy-llm",
                    )
                    status = "sent" if ok else "failed"
                    print(
                        f"[notify] llm {status} backend={backend} "
                        f"count={llm_item['unread_count']} url={llm_item['local_jump_url']}"
                    )
                    if ok:
                        set_app_state_value(connection, llm_sent_key, now_iso)
                        set_app_state_value(
                            connection,
                            llm_item_key_state,
                            llm_item_key,
                        )
                sent_events.append("llm")
            else:
                print("[notify] llm skipped: no unread LLM-updated bookmarks.")

            if not dry_run:
                set_app_state_value(connection, llm_state_key, now_iso)

        if dry_run:
            connection.rollback()
        else:
            connection.commit()
    finally:
        connection.close()

    if sent_events:
        print(f"[notify] completed events={','.join(sent_events)}")
    else:
        print("[notify] completed events=none")


def run_notify_install_macos(
    label: str,
    interval_minutes: int,
    python_bin: str,
    script_path: Path,
    config_path: Path,
    ledger_db_path: Path,
    source_ids: list[str],
    kind: str,
    web_url_base: str,
    llm_lookback_hours: int,
    cooldown_minutes: int,
    plist_path: Path | None,
    load_now: bool,
) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("notify-install-macos is supported only on macOS.")
    safe_label = str(label).strip() or DEFAULT_NOTIFY_MACOS_LABEL
    safe_interval = max(1, int(interval_minutes))
    safe_python = str(python_bin).strip() or sys.executable
    safe_script = script_path.resolve()
    safe_config = config_path.resolve()
    safe_plist_path = (
        plist_path.expanduser().resolve()
        if plist_path is not None
        else (Path.home() / "Library" / "LaunchAgents" / f"{safe_label}.plist")
    )

    program_args = [
        safe_python,
        str(safe_script),
        "notify",
        "--config",
        str(safe_config),
        "--kind",
        kind,
        "--web-url-base",
        web_url_base,
        "--llm-lookback-hours",
        str(max(1, int(llm_lookback_hours))),
        "--cooldown-minutes",
        str(max(0, int(cooldown_minutes))),
        "--ledger-db",
        str(ledger_db_path.resolve()),
    ]
    for source_id in source_ids:
        program_args.extend(["--source", source_id])

    logs_dir = Path.home() / "Library" / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    plist_payload = {
        "Label": safe_label,
        "ProgramArguments": program_args,
        "RunAtLoad": True,
        "StartInterval": safe_interval * 60,
        "StandardOutPath": str(logs_dir / "substudy-notify.out.log"),
        "StandardErrorPath": str(logs_dir / "substudy-notify.err.log"),
    }

    safe_plist_path.parent.mkdir(parents=True, exist_ok=True)
    with safe_plist_path.open("wb") as handle:
        plistlib.dump(plist_payload, handle, sort_keys=True)
    print(f"[notify-install-macos] wrote plist: {safe_plist_path}")

    if not load_now:
        return

    uid = os.getuid()
    bootstrap_target = f"gui/{uid}"
    subprocess.run(
        ["launchctl", "bootout", bootstrap_target, str(safe_plist_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    boot = subprocess.run(
        ["launchctl", "bootstrap", bootstrap_target, str(safe_plist_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if boot.returncode != 0:
        raise RuntimeError("launchctl bootstrap failed for notify agent.")
    subprocess.run(
        ["launchctl", "enable", f"{bootstrap_target}/{safe_label}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    print(f"[notify-install-macos] loaded agent: {safe_label}")


def run_notify_uninstall_macos(
    label: str,
    plist_path: Path | None,
) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("notify-uninstall-macos is supported only on macOS.")
    safe_label = str(label).strip() or DEFAULT_NOTIFY_MACOS_LABEL
    safe_plist_path = (
        plist_path.expanduser().resolve()
        if plist_path is not None
        else (Path.home() / "Library" / "LaunchAgents" / f"{safe_label}.plist")
    )
    uid = os.getuid()
    bootstrap_target = f"gui/{uid}"
    subprocess.run(
        ["launchctl", "bootout", bootstrap_target, str(safe_plist_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if safe_plist_path.exists():
        safe_plist_path.unlink()
    print(f"[notify-uninstall-macos] removed agent: {safe_label}")
    print(f"[notify-uninstall-macos] removed plist: {safe_plist_path}")


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


def get_app_state_value(
    connection: sqlite3.Connection,
    state_key: str,
    default: str = "",
) -> str:
    row = connection.execute(
        """
        SELECT state_value
        FROM app_state
        WHERE state_key = ?
        LIMIT 1
        """,
        (state_key,),
    ).fetchone()
    if row is None:
        return default
    if isinstance(row, sqlite3.Row):
        value = row["state_value"]
    else:
        value = row[0]
    return default if value in (None, "") else str(value)


def set_app_state_value(
    connection: sqlite3.Connection,
    state_key: str,
    state_value: str,
) -> None:
    now_iso = now_utc_iso()
    connection.execute(
        """
        INSERT INTO app_state (state_key, state_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at
        """,
        (state_key, str(state_value), now_iso),
    )


def build_local_jump_url(
    web_url_base: str,
    source_id: str,
    video_id: str,
    cue_start_ms: int,
) -> str:
    base = str(web_url_base or DEFAULT_NOTIFY_WEB_URL_BASE).strip()
    if not base:
        base = DEFAULT_NOTIFY_WEB_URL_BASE
    parsed = urlparse(base)
    if not parsed.scheme:
        base = f"http://{base.lstrip('/')}"
    base = base.rstrip("/")
    jump_second = max(0, int(round(int(cue_start_ms) / 1000)))
    query = urlencode(
        {
            "source_id": source_id,
            "video_id": video_id,
            "t": str(jump_second),
        }
    )
    return f"{base}/?{query}"


def run_macos_notification(
    title: str,
    message: str,
    open_url: str | None = None,
    group: str | None = None,
) -> tuple[bool, str]:
    clean_title = str(title or "").strip() or "Substudy"
    clean_message = str(message or "").strip() or "Notification"
    clean_url = str(open_url or "").strip()
    clean_group = str(group or "").strip()

    notifier = shutil.which("terminal-notifier")
    if not notifier:
        for candidate in (
            Path("/opt/homebrew/bin/terminal-notifier"),
            Path("/usr/local/bin/terminal-notifier"),
        ):
            if candidate.exists() and candidate.is_file():
                notifier = str(candidate)
                break
    if notifier:
        command = [
            notifier,
            "-title",
            clean_title,
            "-message",
            clean_message,
        ]
        if clean_group:
            command.extend(["-group", clean_group])
        if clean_url:
            command.extend(["-open", clean_url])
        completed = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return (completed.returncode == 0, "terminal-notifier")

    escaped_title = clean_title.replace("\\", "\\\\").replace('"', '\\"')
    escaped_message = clean_message.replace("\\", "\\\\").replace('"', '\\"')
    apple_script = f'display notification "{escaped_message}" with title "{escaped_title}"'
    completed = subprocess.run(
        ["osascript", "-e", apple_script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return (completed.returncode == 0, "osascript")


def build_notification_item_key(kind: str, parts: Iterable[str]) -> str:
    normalized_parts = [str(part).strip() for part in parts]
    return f"{kind}:{'|'.join(normalized_parts)}"


def is_notification_duplicate_within_cooldown(
    connection: sqlite3.Connection,
    sent_at_state_key: str,
    item_key_state_key: str,
    item_key: str,
    cooldown_minutes: int,
    now_dt: dt.datetime,
    dry_run: bool,
) -> bool:
    safe_minutes = max(0, int(cooldown_minutes))
    if safe_minutes <= 0 or not item_key:
        return False
    last_item_key = get_app_state_value(connection, item_key_state_key, default="")
    if last_item_key != item_key:
        return False
    last_sent_iso = get_app_state_value(connection, sent_at_state_key, default="")
    if not last_sent_iso:
        return False
    last_sent_dt = parse_iso_datetime_utc(last_sent_iso)
    if last_sent_dt is None:
        return False
    elapsed_seconds = (now_dt - last_sent_dt).total_seconds()
    if elapsed_seconds >= safe_minutes * 60:
        return False
    if dry_run:
        return False
    return True


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


def strip_subtitle_markup(raw_text: str) -> str:
    text = str(raw_text or "")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\{[^}]+\}", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_subtitle_document(subtitle_path: Path) -> ParsedSubtitleDocument:
    raw_text = subtitle_path.read_text(encoding="utf-8", errors="ignore")
    lines = raw_text.replace("\ufeff", "").splitlines()

    block_lines: list[list[str]] = []
    current_block: list[str] = []
    for raw_line in lines:
        if raw_line.strip() == "":
            if current_block:
                block_lines.append(current_block)
                current_block = []
            continue
        current_block.append(raw_line)
    if current_block:
        block_lines.append(current_block)

    blocks: list[dict[str, Any]] = []
    cues: list[SubtitleCueBlock] = []
    cue_id = 1
    for block_index, lines_in_block in enumerate(block_lines):
        timing_index = -1
        for idx, line in enumerate(lines_in_block):
            if "-->" in line:
                timing_index = idx
                break
        if timing_index < 0:
            blocks.append({"type": "raw", "lines": list(lines_in_block)})
            continue

        timing_line = lines_in_block[timing_index]
        start_raw, end_raw = [part.strip() for part in timing_line.split("-->", 1)]
        start_token = start_raw.split()[0] if start_raw else ""
        end_token = end_raw.split()[0] if end_raw else ""
        start_ms = parse_subtitle_timestamp_ms(start_token)
        end_ms = parse_subtitle_timestamp_ms(end_token)
        if start_ms is None or end_ms is None:
            blocks.append({"type": "raw", "lines": list(lines_in_block)})
            continue
        if end_ms < start_ms:
            start_ms, end_ms = end_ms, start_ms

        cue = SubtitleCueBlock(
            cue_id=cue_id,
            block_index=block_index,
            header_lines=list(lines_in_block[:timing_index]),
            timing_line=timing_line,
            text_lines=list(lines_in_block[timing_index + 1 :]),
            start_ms=start_ms,
            end_ms=end_ms,
        )
        cues.append(cue)
        blocks.append({"type": "cue", "cue_id": cue_id})
        cue_id += 1

    format_hint = subtitle_path.suffix.lower().lstrip(".")
    return ParsedSubtitleDocument(
        path=subtitle_path,
        format_hint=format_hint if format_hint in {"srt", "vtt"} else "",
        blocks=blocks,
        cues=cues,
    )


def normalize_subtitle_output_lines(text: str, fallback_lines: list[str]) -> list[str]:
    cleaned = str(text or "").replace("\r", "").strip()
    if cleaned:
        lines = [line.strip() for line in cleaned.split("\n") if line.strip()]
        if lines:
            return lines
    fallback = [line.strip() for line in fallback_lines if line.strip()]
    if fallback:
        return fallback
    return [""]


def render_subtitle_document(
    document: ParsedSubtitleDocument,
    translated_text_by_cue_id: dict[int, str],
) -> str:
    cue_lookup = {cue.cue_id: cue for cue in document.cues}
    rendered_blocks: list[str] = []
    for block in document.blocks:
        block_type = str(block.get("type") or "")
        if block_type == "raw":
            lines = [str(line) for line in block.get("lines", [])]
            rendered_blocks.append("\n".join(lines).rstrip("\n"))
            continue
        if block_type != "cue":
            continue
        cue_id = int(block.get("cue_id") or 0)
        cue = cue_lookup.get(cue_id)
        if cue is None:
            continue
        translated_value = translated_text_by_cue_id.get(cue_id, "")
        text_lines = normalize_subtitle_output_lines(translated_value, cue.text_lines)
        cue_lines = [*cue.header_lines, cue.timing_line, *text_lines]
        rendered_blocks.append("\n".join(cue_lines).rstrip("\n"))
    if not rendered_blocks:
        return ""
    return "\n\n".join(rendered_blocks).rstrip() + "\n"


def build_ja_subtitle_output_path(source_subtitle_path: Path, target_lang: str = "ja") -> Path:
    file_name = source_subtitle_path.name
    parts = file_name.split(".")
    safe_lang = str(target_lang or "ja").strip().lower() or "ja"

    if len(parts) >= 3:
        video_id = parts[0]
        extension = parts[-1]
        return source_subtitle_path.with_name(f"{video_id}.{safe_lang}.{extension}")
    if len(parts) == 2:
        video_id, extension = parts
        return source_subtitle_path.with_name(f"{video_id}.{safe_lang}.{extension}")
    extension = source_subtitle_path.suffix.lstrip(".") or "vtt"
    video_id = source_subtitle_path.stem
    return source_subtitle_path.with_name(f"{video_id}.{safe_lang}.{extension}")


def parse_json_loose(raw_text: str) -> Any | None:
    text = str(raw_text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = re.sub(r"^```[A-Za-z0-9_-]*\s*", "", text).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    candidates = [text]
    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start >= 0 and end > start:
            candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def extract_translation_text_fallback(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```[A-Za-z0-9_-]*\s*", "", text).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return text


def is_translation_placeholder(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return True
    known_placeholders = {
        "[empty completion]",
        "empty completion",
        "[no output]",
        "no output",
        "[unavailable]",
        "unavailable",
        "n/a",
    }
    if normalized in known_placeholders:
        return True
    if re.fullmatch(r"\[(empty|no|unavailable)[^]]*\]", normalized):
        return True
    return False


def is_json_fragment_text(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    if value.startswith("{") or value.startswith("["):
        return True
    if "{\"" in value or "\"ja\"" in value or "\"confidence\"" in value:
        return True
    if value.count("{") + value.count("}") >= 2:
        return True
    if value.endswith(":") or value.endswith(","):
        return True
    return False


def is_english_heavy_text(text: str) -> bool:
    value = str(text or "")
    ascii_letters = len(RE_TRANSLATION_ASCII.findall(value))
    ja_chars = len(RE_TRANSLATION_JA.findall(value))
    return ascii_letters >= max(10, ja_chars * 2)


def evaluate_translation_quality(
    document: ParsedSubtitleDocument,
    translations: dict[int, str],
    source_text_by_cue_id: dict[int, str],
) -> TranslationQualityReport:
    report = TranslationQualityReport(total_cues=len(document.cues))
    bad_ids: set[int] = set()

    for cue in document.cues:
        cue_id = int(cue.cue_id)
        translated_text = str(translations.get(cue_id) or "").strip()
        source_text = str(source_text_by_cue_id.get(cue_id) or "").strip()

        if not translated_text:
            report.empty_cues += 1
            bad_ids.add(cue_id)
            continue

        if is_json_fragment_text(translated_text):
            report.json_fragment_cues += 1
            bad_ids.add(cue_id)
        if is_english_heavy_text(translated_text):
            report.english_heavy_cues += 1
            bad_ids.add(cue_id)
        if source_text and translated_text == source_text:
            report.unchanged_cues += 1
            bad_ids.add(cue_id)

    report.bad_cue_ids = sorted(bad_ids)
    return report


def quality_report_passes_thresholds(
    report: TranslationQualityReport,
    json_fragment_threshold: float,
    english_heavy_threshold: float,
    unchanged_threshold: float,
) -> bool:
    return (
        report.json_fragment_rate() <= max(0.0, float(json_fragment_threshold))
        and report.english_heavy_rate() <= max(0.0, float(english_heavy_threshold))
        and report.unchanged_rate() <= max(0.0, float(unchanged_threshold))
    )


def format_quality_report_summary(report: TranslationQualityReport) -> str:
    return (
        f"total={report.total_cues} bad={len(report.bad_cue_ids)} "
        f"json={report.json_fragment_cues}({report.json_fragment_rate():.3f}) "
        f"english_heavy={report.english_heavy_cues}({report.english_heavy_rate():.3f}) "
        f"unchanged={report.unchanged_cues}({report.unchanged_rate():.3f}) "
        f"empty={report.empty_cues}"
    )


def build_quality_gate_stage_metrics(
    stage_name: str,
    report: TranslationQualityReport,
    passed: bool,
) -> TranslationStageMetrics:
    now_iso = now_utc_iso()
    return TranslationStageMetrics(
        stage_name=stage_name,
        model="rules:v1",
        input_cue_count=max(0, int(report.total_cues)),
        changed_cue_count=max(0, int(len(report.bad_cue_ids))),
        request_count=0,
        prompt_tokens=0,
        completion_tokens=0,
        total_tokens=0,
        elapsed_ms=0,
        status="completed" if passed else "failed",
        error_message="" if passed else format_quality_report_summary(report),
        started_at=now_iso,
        finished_at=now_iso,
    )


def parse_llm_usage_counts(payload: Any) -> tuple[int, int, int]:
    if not isinstance(payload, dict):
        return (0, 0, 0)
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return (0, 0, 0)
    try:
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
    except (TypeError, ValueError):
        prompt_tokens = 0
    try:
        completion_tokens = int(usage.get("completion_tokens") or 0)
    except (TypeError, ValueError):
        completion_tokens = 0
    try:
        total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
    except (TypeError, ValueError):
        total_tokens = prompt_tokens + completion_tokens
    return (max(0, prompt_tokens), max(0, completion_tokens), max(0, total_tokens))


def call_local_chat_completion(
    endpoint: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    top_p: float,
    max_tokens: int,
    timeout_sec: int,
    api_key: str | None = None,
) -> dict[str, Any]:
    payload = {
        "model": str(model),
        "messages": messages,
        "temperature": float(temperature),
        "top_p": float(top_p),
        "max_tokens": max(1, int(max_tokens)),
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"content-type": "application/json"}
    safe_api_key = str(api_key or "").strip()
    if safe_api_key:
        headers["authorization"] = f"Bearer {safe_api_key}"
    request = urllib_request.Request(
        str(endpoint),
        data=body,
        headers=headers,
        method="POST",
    )

    started = time.perf_counter()
    try:
        with urllib_request.urlopen(request, timeout=max(3, int(timeout_sec))) as response:
            response_body = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8", errors="replace")
        except OSError:
            details = ""
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        raise RuntimeError(
            f"LLM request failed: HTTP {exc.code} elapsed={elapsed_ms}ms body={details[:500]}"
        ) from exc
    except urllib_error.URLError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        raise RuntimeError(f"LLM request failed: {exc.reason} elapsed={elapsed_ms}ms") from exc

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    try:
        parsed = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"LLM response is not valid JSON: elapsed={elapsed_ms}ms body={response_body[:500]}"
        ) from exc

    content = ""
    if isinstance(parsed, dict):
        choices = parsed.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            message = first.get("message") if isinstance(first, dict) else {}
            if isinstance(message, dict):
                content = str(message.get("content") or "")

    prompt_tokens, completion_tokens, total_tokens = parse_llm_usage_counts(parsed)
    return {
        "content": content,
        "raw": parsed,
        "elapsed_ms": elapsed_ms,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    }


def extract_patch_map_from_llm_output(raw_text: str) -> dict[int, str]:
    parsed = parse_json_loose(raw_text)
    rows: list[Any] = []
    if isinstance(parsed, list):
        rows = parsed
    elif isinstance(parsed, dict):
        cues_value = parsed.get("cues")
        if isinstance(cues_value, list):
            rows = cues_value
        elif isinstance(parsed.get("items"), list):
            rows = parsed.get("items") or []
        else:
            single_cue_id = parsed.get("cue_id")
            single_text = parsed.get("ja")
            if single_cue_id is not None and single_text not in (None, ""):
                rows = [{"cue_id": single_cue_id, "ja": single_text}]
    patch_map: dict[int, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        cue_id_raw = row.get("cue_id")
        if cue_id_raw in (None, ""):
            continue
        try:
            cue_id = int(cue_id_raw)
        except (TypeError, ValueError):
            continue
        ja_text = str(row.get("ja") or "").strip()
        if not ja_text:
            continue
        patch_map[cue_id] = ja_text
    return patch_map


def apply_patch_map_to_translations(
    current_translations: dict[int, str],
    patch_map: dict[int, str],
    allowed_cue_ids: set[int],
) -> int:
    changed = 0
    for cue_id, raw_text in patch_map.items():
        if cue_id not in allowed_cue_ids:
            continue
        normalized = extract_translation_text_fallback(raw_text)
        if is_translation_placeholder(normalized):
            continue
        if not normalized:
            continue
        previous = str(current_translations.get(cue_id) or "")
        if normalized == previous:
            continue
        current_translations[cue_id] = normalized
        changed += 1
    return changed


def chunk_items(items: list[Any], chunk_size: int) -> list[list[Any]]:
    safe_size = max(1, int(chunk_size))
    return [items[index : index + safe_size] for index in range(0, len(items), safe_size)]


def build_local_translation_summary(
    source_id: str,
    video_id: str,
    source_track_kind: str,
    cue_count: int,
    translations: dict[int, str],
) -> str:
    safe_source_track_kind = normalize_translation_source_track(source_track_kind, "subtitle")
    preview: list[str] = []
    for cue_id in sorted(translations):
        text = str(translations.get(cue_id) or "").strip()
        if not text:
            continue
        preview.append(text)
        if len(preview) >= 2:
            break
    preview_text = " / ".join(preview)
    if len(preview_text) > 120:
        preview_text = preview_text[:117].rstrip() + "..."
    if preview_text:
        return (
            f"local-llm multi-stage translation "
            f"({source_id}/{video_id}, source_track={safe_source_track_kind}, "
            f"cues={max(0, int(cue_count))}): {preview_text}"
        )
    return (
        f"local-llm multi-stage translation "
        f"({source_id}/{video_id}, source_track={safe_source_track_kind}, "
        f"cues={max(0, int(cue_count))})"
    )


def append_source_track_to_method_version(method_version: str, source_track_kind: str) -> str:
    safe_method_version = str(method_version or "").strip()
    safe_source_track = normalize_translation_source_track(source_track_kind, "subtitle")
    marker = f"source-track={safe_source_track}"
    if marker in safe_method_version:
        return safe_method_version
    if not safe_method_version:
        return marker
    return f"{safe_method_version}|{marker}"


def find_best_subtitle_text_for_range(
    cues: list[dict[str, Any]],
    start_ms: int,
    end_ms: int,
) -> str:
    if not cues:
        return ""
    safe_start_ms = max(0, int(start_ms))
    safe_end_ms = max(safe_start_ms, int(end_ms))

    best_overlap = 0
    best_text = ""
    for cue in cues:
        cue_start = max(0, int(cue.get("start_ms") or 0))
        cue_end = max(cue_start, int(cue.get("end_ms") or cue_start))
        overlap = min(safe_end_ms, cue_end) - max(safe_start_ms, cue_start)
        if overlap > best_overlap:
            best_overlap = overlap
            best_text = str(cue.get("text") or "")
    if best_text:
        return best_text

    target_ms = safe_start_ms
    nearest_text = ""
    nearest_distance = None
    for cue in cues:
        cue_start = max(0, int(cue.get("start_ms") or 0))
        distance = abs(cue_start - target_ms)
        if nearest_distance is None or distance < nearest_distance:
            nearest_distance = distance
            nearest_text = str(cue.get("text") or "")
    if nearest_distance is not None and nearest_distance <= 1500:
        return nearest_text
    return ""


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
                    "language": "ASR",
                    "origin_kind": "generated",
                    "origin_detail": "asr",
                    "origin_label": "Generated/ASR",
                    "display_label": "[Generated/ASR] ASR",
                    "path": asr_key,
                }
            )

    subtitle_rows = connection.execute(
        """
        SELECT language, subtitle_path, origin_kind, origin_detail, ext
        FROM subtitles
        WHERE source_id = ?
          AND video_id = ?
        ORDER BY language COLLATE NOCASE ASC, subtitle_path ASC
        """,
        (source_id, video_id),
    ).fetchall()
    translation_output_origin_lookup = build_translation_output_origin_lookup(
        connection=connection,
        source_id=source_id,
        video_id=video_id,
    )
    for language, subtitle_path_value, origin_kind_value, origin_detail_value, ext in subtitle_rows:
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
        origin_kind, origin_detail = classify_subtitle_origin(
            language=language_label,
            subtitle_path=subtitle_path,
            translation_output_origin_lookup=translation_output_origin_lookup,
        )
        stored_origin_kind = normalize_subtitle_origin_kind(origin_kind_value, origin_kind)
        stored_origin_detail = str(origin_detail_value or "").strip() or origin_detail
        origin_label = format_subtitle_track_origin_label(stored_origin_kind, stored_origin_detail)
        display_label = f"[{origin_label}] {label}"
        tracks.append(
            {
                "track_id": f"subtitle:{encode_path_token(subtitle_path)}",
                "kind": "subtitle",
                "label": label,
                "language": language_label,
                "origin_kind": stored_origin_kind,
                "origin_detail": stored_origin_detail,
                "origin_label": origin_label,
                "display_label": display_label,
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


def resolve_ja_subtitle_path(
    connection: sqlite3.Connection,
    source_id: str,
    video_id: str,
) -> Path | None:
    row = connection.execute(
        """
        SELECT subtitle_path
        FROM subtitles
        WHERE source_id = ?
          AND video_id = ?
          AND (
                LOWER(COALESCE(language, '')) = 'ja'
             OR LOWER(COALESCE(language, '')) LIKE 'ja-%'
             OR LOWER(COALESCE(subtitle_path, '')) LIKE '%.ja.%'
          )
        ORDER BY
          CASE
            WHEN LOWER(COALESCE(language, '')) = 'ja' THEN 0
            WHEN LOWER(COALESCE(language, '')) LIKE 'ja-%' THEN 1
            ELSE 2
          END ASC,
          subtitle_path ASC
        LIMIT 1
        """,
        (source_id, video_id),
    ).fetchone()
    if row is None:
        return None
    path = Path(str(row[0]))
    if not path.exists() or not path.is_file():
        return None
    return path


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


def _infer_workspace_artifact_kind(path: Path) -> str:
    name = path.name.lower()
    if "missing_review" in name:
        return "missing_review"
    if "review_hints" in name:
        return "review_hints"
    if "review_cards" in name:
        return "review_cards"
    if "translation_qa" in name:
        return "translation_qa"
    if "frequent_terms" in name:
        return "frequent_terms"
    if "recent_saved" in name:
        return "recent_saved"
    if "translation" in name:
        return "translation"
    if "import" in name:
        return "import"
    return "artifact"


def collect_workspace_artifacts(
    root_dir: Path,
    limit: int,
) -> list[dict[str, Any]]:
    safe_limit = max(1, int(limit))
    exports_dir = root_dir / "exports"
    if not exports_dir.exists() or not exports_dir.is_dir():
        return []

    candidates: list[tuple[float, Path, int]] = []
    for glob_pattern in ("**/*.jsonl", "**/*.csv", "**/*.txt", "**/*.log", "**/*.json"):
        for path in exports_dir.glob(glob_pattern):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            candidates.append((float(stat.st_mtime), path, int(stat.st_size)))

    candidates.sort(key=lambda item: item[0], reverse=True)
    rows: list[dict[str, Any]] = []
    for modified_epoch, path, size_bytes in candidates[:safe_limit]:
        try:
            relative_path = str(path.relative_to(root_dir))
        except ValueError:
            relative_path = str(path.name)
        try:
            token = encode_path_token(path.resolve())
        except OSError:
            token = ""
        updated_at = dt.datetime.fromtimestamp(
            modified_epoch,
            tz=dt.timezone.utc,
        ).replace(microsecond=0).isoformat()
        rows.append(
            {
                "name": path.name,
                "relative_path": relative_path,
                "kind": _infer_workspace_artifact_kind(path),
                "size_bytes": size_bytes,
                "updated_at": updated_at,
                "open_url": f"/artifact/{token}" if token else "",
                "download_url": f"/artifact/{token}?download=1" if token else "",
            }
        )
    return rows


def load_workspace_review_hints(root_dir: Path) -> dict[str, dict[str, str]]:
    hints_path = root_dir / "exports" / "llm" / "review_hints.jsonl"
    if not hints_path.exists() or not hints_path.is_file():
        return {}

    hints_by_card_id: dict[str, dict[str, str]] = {}
    try:
        with hints_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(parsed, dict):
                    continue
                card_id = str(parsed.get("card_id") or "").strip()
                if not card_id:
                    continue
                hint_payload: dict[str, str] = {}
                for key in (
                    "one_line_hint_ja",
                    "one_line_hint_en",
                    "common_mistake",
                    "memory_hook",
                ):
                    value = str(parsed.get(key) or "").strip()
                    if value:
                        hint_payload[key] = value
                if hint_payload:
                    hints_by_card_id[card_id] = hint_payload
    except OSError:
        return {}
    return hints_by_card_id


def apply_workspace_review_hints(
    review_cards: list[dict[str, Any]],
    hints_by_card_id: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    if not review_cards or not hints_by_card_id:
        return review_cards
    for card in review_cards:
        card_id = str(card.get("card_id") or "").strip()
        if not card_id:
            continue
        hint_payload = hints_by_card_id.get(card_id)
        if not hint_payload:
            continue
        for key, value in hint_payload.items():
            if value:
                card[key] = value
    return review_cards


def load_workspace_translation_qa(root_dir: Path) -> dict[str, dict[str, str]]:
    qa_path = root_dir / "exports" / "llm" / "translation_qa.jsonl"
    if not qa_path.exists() or not qa_path.is_file():
        return {}

    qa_by_card_id: dict[str, dict[str, str]] = {}
    try:
        with qa_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(parsed, dict):
                    continue
                card_id = str(parsed.get("card_id") or "").strip()
                if not card_id:
                    continue
                qa_result = str(parsed.get("qa_result") or "").strip().lower()
                if qa_result != "check":
                    continue
                payload: dict[str, str] = {
                    "qa_result": "check",
                }
                reason = str(parsed.get("reason") or "").strip()
                suggested_ja = str(parsed.get("suggested_ja") or "").strip()
                if reason:
                    payload["qa_reason"] = reason
                if suggested_ja:
                    payload["qa_suggested_ja"] = suggested_ja
                qa_by_card_id[card_id] = payload
    except OSError:
        return {}
    return qa_by_card_id


def apply_workspace_translation_qa(
    cards: list[dict[str, Any]],
    qa_by_card_id: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    if not cards or not qa_by_card_id:
        return cards
    for card in cards:
        card_id = str(card.get("card_id") or "").strip()
        if not card_id:
            continue
        qa_payload = qa_by_card_id.get(card_id)
        if not qa_payload:
            continue
        for key, value in qa_payload.items():
            if value:
                card[key] = value
    return cards


def apply_workspace_missing_entry_states(
    missing_cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not missing_cards:
        return missing_cards
    default_missing_definition = "辞書エントリが見つかりません。"
    for card in missing_cards:
        qa_result = str(card.get("qa_result") or "").strip().lower()
        definition = str(card.get("definition") or "").strip()
        status = "pending"
        label = "LLM補完待ち"
        if definition and definition != default_missing_definition:
            status = "enriched"
            label = "補完済み"
        if qa_result == "check":
            status = "needs_review"
            label = "要再確認"
        card["missing_status"] = status
        card["missing_status_label"] = label
    return missing_cards


def collect_workspace_review_and_missing_rows(
    connection: sqlite3.Connection,
    source_ids: list[str],
    review_limit: int,
    missing_limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    safe_review_limit = max(1, int(review_limit))
    safe_missing_limit = max(1, int(missing_limit))
    scan_limit = max(safe_review_limit * 10, safe_missing_limit * 10, 320)

    where_clauses: list[str] = []
    params: list[Any] = []
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        where_clauses.append(f"db.source_id IN ({placeholders})")
        params.extend(source_ids)
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    rows = connection.execute(
        f"""
        SELECT
            db.id,
            db.source_id,
            db.video_id,
            db.track,
            db.cue_start_ms,
            db.cue_end_ms,
            db.cue_text,
            db.dict_entry_id,
            db.dict_source_name,
            db.lookup_term,
            db.term,
            db.term_norm,
            db.definition,
            db.missing_entry,
            db.lookup_path_json,
            db.lookup_path_label,
            db.created_at,
            db.updated_at,
            COALESCE(v.webpage_url, '') AS webpage_url
        FROM dictionary_bookmarks db
        LEFT JOIN videos v
          ON v.source_id = db.source_id
         AND v.video_id = db.video_id
        {where_sql}
        ORDER BY db.updated_at DESC, db.id DESC
        LIMIT ?
        """,
        (*params, scan_limit),
    ).fetchall()
    if not rows:
        return ([], [])

    term_stats = collect_dictionary_term_history_stats(connection, source_ids=source_ids)

    selected_review: list[dict[str, Any]] = []
    selected_missing: list[dict[str, Any]] = []
    for row in rows:
        serialized = serialize_dictionary_bookmark_row(row)
        serialized["webpage_url"] = str(row["webpage_url"] or "")
        if len(selected_review) < safe_review_limit:
            selected_review.append(serialized)
        if serialized["missing_entry"] and len(selected_missing) < safe_missing_limit:
            selected_missing.append(serialized)
        if len(selected_review) >= safe_review_limit and len(selected_missing) >= safe_missing_limit:
            break

    ja_cues_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    def resolve_ja_cues_for_workspace(source_id: str, video_id: str) -> list[dict[str, Any]]:
        key = (source_id, video_id)
        if key in ja_cues_cache:
            return ja_cues_cache[key]
        ja_path = resolve_ja_subtitle_path(connection, source_id, video_id)
        if ja_path is None:
            ja_cues_cache[key] = []
        else:
            ja_cues_cache[key] = parse_subtitle_cues(ja_path)
        return ja_cues_cache[key]

    def to_workspace_card(serialized: dict[str, Any], include_card_id: bool) -> dict[str, Any]:
        term_norm = str(serialized.get("term_norm") or "")
        stats = term_stats.get(term_norm, {})
        cue_start_ms = int(serialized["cue_start_ms"])
        cue_end_ms = int(serialized["cue_end_ms"])
        card_id = f"dictbm:{int(serialized['id'])}"
        card = {
            "card_id": card_id,
            "source_id": str(serialized["source_id"]),
            "video_id": str(serialized["video_id"]),
            "track": str(serialized.get("track") or ""),
            "cue_start_ms": cue_start_ms,
            "cue_end_ms": cue_end_ms,
            "cue_start_label": format_ms_to_clock(cue_start_ms),
            "cue_end_label": format_ms_to_clock(cue_end_ms),
            "cue_en_text": str(serialized.get("cue_text") or ""),
            "cue_ja_text": find_best_subtitle_text_for_range(
                resolve_ja_cues_for_workspace(
                    str(serialized["source_id"]),
                    str(serialized["video_id"]),
                ),
                cue_start_ms,
                cue_end_ms,
            ),
            "term": str(serialized.get("term") or ""),
            "term_norm": term_norm,
            "lookup_term": str(serialized.get("lookup_term") or ""),
            "definition": str(serialized.get("definition") or ""),
            "missing_entry": 1 if bool(serialized.get("missing_entry")) else 0,
            "lookup_path_label": str(serialized.get("lookup_path_label") or ""),
            "bookmark_count": int(stats.get("bookmark_count", 1)),
            "video_count": int(stats.get("video_count", 1)),
            "reencounter_count": int(stats.get("reencounter_count", 0)),
            "review_priority": float(stats.get("review_priority", 0.0)),
            "local_jump_url": (
                "/?"
                + urlencode(
                    {
                        "source_id": str(serialized["source_id"]),
                        "video_id": str(serialized["video_id"]),
                        "t": str(max(0, int(round(cue_start_ms / 1000)))),
                    }
                )
            ),
            "webpage_url": str(serialized.get("webpage_url") or ""),
            "created_at": str(serialized.get("created_at") or ""),
            "updated_at": str(serialized.get("updated_at") or ""),
        }
        if not include_card_id:
            card["id"] = int(serialized["id"])
        return card

    review_cards = [to_workspace_card(item, include_card_id=True) for item in selected_review]
    missing_cards = [to_workspace_card(item, include_card_id=False) for item in selected_missing]
    return (review_cards, missing_cards)


def is_workspace_ja_subtitle(language: str | None, subtitle_path: str | None) -> bool:
    return bool(classify_ja_subtitle_variant(language, subtitle_path))


def is_workspace_english_subtitle(language: str | None, subtitle_path: str | None) -> bool:
    if is_workspace_ja_subtitle(language, subtitle_path):
        return False
    normalized = str(language or "").strip().lower()
    if normalized and language_rank_for_translation_source(normalized, "ja") <= 2:
        return True
    path_tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", str(subtitle_path or "").strip().lower())
        if token
    }
    return bool(path_tokens & {"en", "eng", "english"})


def build_workspace_source_processing_summary(source_id: str = "") -> dict[str, Any]:
    return {
        "source_id": source_id,
        "total_videos": 0,
        "complete_count": 0,
        "pending_total": 0,
        "english_subtitles_ready": 0,
        "english_subtitles_missing": 0,
        "source_text_ready": 0,
        "source_text_missing": 0,
        "ja_subtitles_ready": 0,
        "ja_subtitles_missing": 0,
        "ja_subtitles_ready_playable": 0,
        "ja_subtitles_missing_playable": 0,
        "meta_ready": 0,
        "meta_missing": 0,
        "media_ready": 0,
        "media_missing": 0,
        "asr_ready": 0,
        "asr_pending": 0,
        "loudness_ready": 0,
        "loudness_pending": 0,
        "pipeline_buckets": {
            "complete": 0,
            "ja_pending": 0,
            "loudness_pending": 0,
            "source_text_pending": 0,
            "meta_media_pending": 0,
        },
    }


def build_source_auto_tag_metrics(summary: dict[str, Any]) -> dict[str, float]:
    total_videos = max(0, int(summary.get("total_videos", 0) or 0))
    media_ready = max(0, int(summary.get("media_ready", 0) or 0))

    def total_ratio(value: Any) -> float:
        if total_videos <= 0:
            return 0.0
        return float(int(value or 0)) / float(total_videos)

    def media_ratio(value: Any) -> float:
        if media_ready <= 0:
            return 0.0
        return float(int(value or 0)) / float(media_ready)

    metrics: dict[str, float] = {
        "total_videos": float(total_videos),
        "complete_count": float(int(summary.get("complete_count", 0) or 0)),
        "pending_total": float(int(summary.get("pending_total", 0) or 0)),
        "english_subtitles_ready": float(int(summary.get("english_subtitles_ready", 0) or 0)),
        "english_subtitles_missing": float(int(summary.get("english_subtitles_missing", 0) or 0)),
        "source_text_ready": float(int(summary.get("source_text_ready", 0) or 0)),
        "source_text_missing": float(int(summary.get("source_text_missing", 0) or 0)),
        "ja_subtitles_ready": float(int(summary.get("ja_subtitles_ready", 0) or 0)),
        "ja_subtitles_missing": float(int(summary.get("ja_subtitles_missing", 0) or 0)),
        "ja_subtitles_ready_playable": float(int(summary.get("ja_subtitles_ready_playable", 0) or 0)),
        "ja_subtitles_missing_playable": float(int(summary.get("ja_subtitles_missing_playable", 0) or 0)),
        "meta_ready": float(int(summary.get("meta_ready", 0) or 0)),
        "meta_missing": float(int(summary.get("meta_missing", 0) or 0)),
        "media_ready": float(media_ready),
        "media_missing": float(int(summary.get("media_missing", 0) or 0)),
        "asr_ready": float(int(summary.get("asr_ready", 0) or 0)),
        "asr_pending": float(int(summary.get("asr_pending", 0) or 0)),
        "loudness_ready": float(int(summary.get("loudness_ready", 0) or 0)),
        "loudness_pending": float(int(summary.get("loudness_pending", 0) or 0)),
    }
    metrics["claude_subtitles_ready"] = metrics["ja_subtitles_ready"]
    metrics["claude_subtitles_missing"] = metrics["ja_subtitles_missing"]
    metrics["claude_subtitles_ready_playable"] = metrics["ja_subtitles_ready_playable"]
    metrics["claude_subtitles_missing_playable"] = metrics["ja_subtitles_missing_playable"]

    metrics["complete_ratio"] = total_ratio(summary.get("complete_count", 0))
    metrics["pending_ratio"] = total_ratio(summary.get("pending_total", 0))
    metrics["english_subtitles_ready_ratio"] = total_ratio(summary.get("english_subtitles_ready", 0))
    metrics["english_subtitles_missing_ratio"] = total_ratio(summary.get("english_subtitles_missing", 0))
    metrics["source_text_ready_ratio"] = total_ratio(summary.get("source_text_ready", 0))
    metrics["source_text_missing_ratio"] = total_ratio(summary.get("source_text_missing", 0))
    metrics["ja_subtitles_ready_ratio"] = total_ratio(summary.get("ja_subtitles_ready", 0))
    metrics["ja_subtitles_missing_ratio"] = total_ratio(summary.get("ja_subtitles_missing", 0))
    metrics["ja_subtitles_ready_playable_ratio"] = media_ratio(summary.get("ja_subtitles_ready_playable", 0))
    metrics["ja_subtitles_missing_playable_ratio"] = media_ratio(summary.get("ja_subtitles_missing_playable", 0))
    metrics["claude_subtitles_ready_ratio"] = metrics["ja_subtitles_ready_ratio"]
    metrics["claude_subtitles_missing_ratio"] = metrics["ja_subtitles_missing_ratio"]
    metrics["claude_subtitles_ready_playable_ratio"] = metrics["ja_subtitles_ready_playable_ratio"]
    metrics["claude_subtitles_missing_playable_ratio"] = metrics["ja_subtitles_missing_playable_ratio"]
    metrics["meta_ready_ratio"] = total_ratio(summary.get("meta_ready", 0))
    metrics["meta_missing_ratio"] = total_ratio(summary.get("meta_missing", 0))
    metrics["media_ready_ratio"] = total_ratio(summary.get("media_ready", 0))
    metrics["media_missing_ratio"] = total_ratio(summary.get("media_missing", 0))
    metrics["asr_ready_ratio"] = media_ratio(summary.get("asr_ready", 0))
    metrics["asr_pending_ratio"] = media_ratio(summary.get("asr_pending", 0))
    metrics["loudness_ready_ratio"] = media_ratio(summary.get("loudness_ready", 0))
    metrics["loudness_pending_ratio"] = media_ratio(summary.get("loudness_pending", 0))
    return metrics


def compute_source_auto_tags(
    summary: dict[str, Any] | None,
    rules: list[AutoTagRule],
) -> list[str]:
    if summary is None or not rules:
        return []
    total_videos = max(0, int(summary.get("total_videos", 0) or 0))
    metrics = build_source_auto_tag_metrics(summary)
    matched_tags: list[str] = []
    for rule in rules:
        if total_videos < int(rule.min_total_videos):
            continue
        value = float(metrics.get(rule.metric, 0.0))
        threshold = float(rule.threshold)
        matched = False
        if rule.comparator == "gte":
            matched = value >= threshold
        elif rule.comparator == "gt":
            matched = value > threshold
        elif rule.comparator == "lte":
            matched = value <= threshold
        elif rule.comparator == "lt":
            matched = value < threshold
        elif rule.comparator == "eq":
            matched = math.isclose(value, threshold, rel_tol=1e-9, abs_tol=1e-9)
        if matched:
            matched_tags.append(rule.tag)
    return normalize_source_tags(matched_tags)


def merge_source_tag_sets(manual_tags: Any, auto_tags: Any) -> list[str]:
    return normalize_source_tags([*normalize_source_tags(manual_tags), *normalize_source_tags(auto_tags)])


def apply_source_tags_payload(
    payload: dict[str, Any],
    manual_tags: Any,
    auto_tags: Any,
) -> dict[str, Any]:
    normalized_manual_tags = normalize_source_tags(manual_tags)
    normalized_auto_tags = normalize_source_tags(auto_tags)
    payload["manual_tags"] = normalized_manual_tags
    payload["auto_tags"] = normalized_auto_tags
    payload["tags"] = merge_source_tag_sets(normalized_manual_tags, normalized_auto_tags)
    return payload


def accumulate_workspace_source_processing_summary(
    target: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    count_keys = (
        "total_videos",
        "complete_count",
        "pending_total",
        "english_subtitles_ready",
        "english_subtitles_missing",
        "source_text_ready",
        "source_text_missing",
        "ja_subtitles_ready",
        "ja_subtitles_missing",
        "ja_subtitles_ready_playable",
        "ja_subtitles_missing_playable",
        "meta_ready",
        "meta_missing",
        "media_ready",
        "media_missing",
        "asr_ready",
        "asr_pending",
        "loudness_ready",
        "loudness_pending",
    )
    for key in count_keys:
        target[key] = int(target.get(key, 0)) + int(summary.get(key, 0))
    target_buckets = cast(dict[str, Any], target.get("pipeline_buckets") or {})
    source_buckets = cast(dict[str, Any], summary.get("pipeline_buckets") or {})
    for bucket_key in (
        "complete",
        "ja_pending",
        "loudness_pending",
        "source_text_pending",
        "meta_media_pending",
    ):
        target_buckets[bucket_key] = int(target_buckets.get(bucket_key, 0)) + int(
            source_buckets.get(bucket_key, 0)
        )
    target["pipeline_buckets"] = target_buckets


def collect_workspace_source_processing_summary(
    connection: sqlite3.Connection,
    source_ids: list[str],
) -> dict[str, Any]:
    requested_source_ids = sorted(
        {
            str(source_id).strip()
            for source_id in source_ids
            if str(source_id).strip()
        }
    )
    source_filter_sql = ""
    source_params: tuple[Any, ...] = ()
    if requested_source_ids:
        placeholders = ",".join("?" for _ in requested_source_ids)
        source_filter_sql = f"WHERE source_id IN ({placeholders})"
        source_params = tuple(requested_source_ids)

    video_rows = connection.execute(
        f"""
        SELECT
            source_id,
            video_id,
            COALESCE(meta_path, '') AS meta_path,
            COALESCE(has_media, 0) AS has_media,
            COALESCE(audio_loudness_analyzed_at, '') AS audio_loudness_analyzed_at
        FROM videos
        {source_filter_sql}
        ORDER BY source_id ASC, video_id ASC
        """,
        source_params,
    ).fetchall()

    subtitle_rows = connection.execute(
        f"""
        SELECT
            source_id,
            video_id,
            COALESCE(language, '') AS language,
            COALESCE(subtitle_path, '') AS subtitle_path
        FROM subtitles
        {source_filter_sql}
        ORDER BY source_id ASC, video_id ASC, subtitle_path ASC
        """,
        source_params,
    ).fetchall()

    asr_rows = connection.execute(
        f"""
        SELECT
            source_id,
            video_id
        FROM asr_runs
        WHERE LOWER(COALESCE(status, '')) = 'success'
          {"AND source_id IN (" + ",".join("?" for _ in requested_source_ids) + ")" if requested_source_ids else ""}
        ORDER BY source_id ASC, video_id ASC
        """,
        source_params,
    ).fetchall()

    summaries_by_source: dict[str, dict[str, Any]] = {
        source_id: build_workspace_source_processing_summary(source_id)
        for source_id in requested_source_ids
    }
    video_flags_by_key: dict[tuple[str, str], dict[str, bool | str]] = {}

    for row in video_rows:
        source_id = str(row["source_id"] or "").strip()
        video_id = str(row["video_id"] or "").strip()
        if not source_id or not video_id:
            continue
        summaries_by_source.setdefault(
            source_id,
            build_workspace_source_processing_summary(source_id),
        )
        video_flags_by_key[(source_id, video_id)] = {
            "meta_ready": bool(str(row["meta_path"] or "").strip()),
            "media_ready": bool(int(row["has_media"] or 0)),
            "loudness_ready": bool(str(row["audio_loudness_analyzed_at"] or "").strip()),
            "has_english_subtitle": False,
            "has_ja_subtitle": False,
            "has_asr": False,
        }

    for row in subtitle_rows:
        source_id = str(row["source_id"] or "").strip()
        video_id = str(row["video_id"] or "").strip()
        flags = video_flags_by_key.get((source_id, video_id))
        if flags is None:
            continue
        language = str(row["language"] or "")
        subtitle_path = str(row["subtitle_path"] or "")
        if is_workspace_ja_subtitle(language, subtitle_path):
            flags["has_ja_subtitle"] = True
            continue
        if is_workspace_english_subtitle(language, subtitle_path):
            flags["has_english_subtitle"] = True

    for row in asr_rows:
        source_id = str(row["source_id"] or "").strip()
        video_id = str(row["video_id"] or "").strip()
        flags = video_flags_by_key.get((source_id, video_id))
        if flags is None:
            continue
        flags["has_asr"] = True

    for (source_id, _video_id), flags in video_flags_by_key.items():
        summary = summaries_by_source.setdefault(
            source_id,
            build_workspace_source_processing_summary(source_id),
        )
        meta_ready = bool(flags["meta_ready"])
        media_ready = bool(flags["media_ready"])
        english_ready = bool(flags["has_english_subtitle"])
        ja_ready = bool(flags["has_ja_subtitle"])
        asr_ready = bool(flags["has_asr"])
        source_text_ready = bool(english_ready or asr_ready)
        loudness_ready = bool(flags["loudness_ready"]) if media_ready else False

        summary["total_videos"] = int(summary["total_videos"]) + 1
        summary["meta_ready"] = int(summary["meta_ready"]) + int(meta_ready)
        summary["meta_missing"] = int(summary["meta_missing"]) + int(not meta_ready)
        summary["media_ready"] = int(summary["media_ready"]) + int(media_ready)
        summary["media_missing"] = int(summary["media_missing"]) + int(not media_ready)
        summary["english_subtitles_ready"] = int(summary["english_subtitles_ready"]) + int(
            english_ready
        )
        summary["english_subtitles_missing"] = int(
            summary["english_subtitles_missing"]
        ) + int(not english_ready)
        summary["source_text_ready"] = int(summary["source_text_ready"]) + int(source_text_ready)
        summary["source_text_missing"] = int(summary["source_text_missing"]) + int(
            not source_text_ready
        )
        summary["ja_subtitles_ready"] = int(summary["ja_subtitles_ready"]) + int(ja_ready)
        summary["ja_subtitles_missing"] = int(summary["ja_subtitles_missing"]) + int(not ja_ready)
        if media_ready:
            summary["ja_subtitles_ready_playable"] = int(summary["ja_subtitles_ready_playable"]) + int(
                ja_ready
            )
            summary["ja_subtitles_missing_playable"] = int(
                summary["ja_subtitles_missing_playable"]
            ) + int(not ja_ready)

        if media_ready:
            summary["asr_ready"] = int(summary["asr_ready"]) + int(asr_ready)
            summary["asr_pending"] = int(summary["asr_pending"]) + int(not asr_ready)
            summary["loudness_ready"] = int(summary["loudness_ready"]) + int(loudness_ready)
            summary["loudness_pending"] = int(summary["loudness_pending"]) + int(
                not loudness_ready
            )

        bucket_key = "complete"
        if not meta_ready or not media_ready:
            bucket_key = "meta_media_pending"
        elif not source_text_ready:
            bucket_key = "source_text_pending"
        elif not ja_ready:
            bucket_key = "ja_pending"
        elif not loudness_ready:
            bucket_key = "loudness_pending"

        bucket_counts = cast(dict[str, Any], summary["pipeline_buckets"])
        bucket_counts[bucket_key] = int(bucket_counts.get(bucket_key, 0)) + 1
        if bucket_key == "complete":
            summary["complete_count"] = int(summary["complete_count"]) + 1

    summaries: list[dict[str, Any]] = list(summaries_by_source.values())
    for summary in summaries:
        summary["pending_total"] = max(
            0,
            int(summary["total_videos"]) - int(summary["complete_count"]),
        )

    summaries.sort(
        key=lambda item: (
            -int(item["pending_total"]),
            -int(item["total_videos"]),
            str(item["source_id"]),
        )
    )

    totals = build_workspace_source_processing_summary()
    for summary in summaries:
        accumulate_workspace_source_processing_summary(totals, summary)
    totals["source_count"] = len(summaries)

    return {
        "sources": summaries,
        "totals": totals,
    }


def collect_workspace_download_monitor(
    connection: sqlite3.Connection,
    source_ids: list[str],
    since_hours: int,
    run_limit: int,
    pending_limit: int,
) -> dict[str, Any]:
    safe_hours = max(1, int(since_hours))
    safe_run_limit = max(1, int(run_limit))
    safe_pending_limit = max(1, int(pending_limit))
    since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=safe_hours)
    since_iso = since_dt.replace(microsecond=0).isoformat()

    source_filter_sql_runs = ""
    source_filter_sql_pending = ""
    source_params: list[Any] = []
    if source_ids:
        placeholders = ",".join("?" for _ in source_ids)
        source_filter_sql_runs = f"AND source_id IN ({placeholders})"
        source_filter_sql_pending = f"AND source_id IN ({placeholders})"
        source_params = list(source_ids)

    recent_runs = connection.execute(
        f"""
        SELECT
            source_id,
            started_at,
            stage,
            status,
            COALESCE(target_count, 0) AS target_count,
            COALESCE(success_count, 0) AS success_count,
            COALESCE(failure_count, 0) AS failure_count,
            COALESCE(exit_code, 0) AS exit_code,
            COALESCE(error_message, '') AS error_message
        FROM download_runs
        WHERE started_at >= ?
          {source_filter_sql_runs}
        ORDER BY started_at DESC, run_id DESC
        LIMIT ?
        """,
        (since_iso, *source_params, safe_run_limit),
    ).fetchall()

    pending_rows = connection.execute(
        f"""
        SELECT
            source_id,
            stage,
            video_id,
            retry_count,
            COALESCE(next_retry_at, '') AS next_retry_at,
            COALESCE(last_error, '') AS last_error,
            updated_at
        FROM download_state
        WHERE status = 'error'
          {source_filter_sql_pending}
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (*source_params, safe_pending_limit),
    ).fetchall()

    pending_count_row = connection.execute(
        f"""
        SELECT COUNT(*) AS pending_count
        FROM download_state
        WHERE status = 'error'
          {source_filter_sql_pending}
        """,
        tuple(source_params),
    ).fetchone()
    pending_count = int(pending_count_row["pending_count"]) if pending_count_row else 0

    return {
        "since_hours": safe_hours,
        "recent_runs": [
            {
                "source_id": str(row["source_id"] or ""),
                "started_at": str(row["started_at"] or ""),
                "stage": str(row["stage"] or ""),
                "status": str(row["status"] or ""),
                "target_count": int(row["target_count"] or 0),
                "success_count": int(row["success_count"] or 0),
                "failure_count": int(row["failure_count"] or 0),
                "exit_code": int(row["exit_code"] or 0),
                "error_message": str(row["error_message"] or ""),
            }
            for row in recent_runs
        ],
        "pending_failures": [
            {
                "source_id": str(row["source_id"] or ""),
                "stage": str(row["stage"] or ""),
                "video_id": str(row["video_id"] or ""),
                "retry_count": int(row["retry_count"] or 0),
                "next_retry_at": str(row["next_retry_at"] or ""),
                "last_error": str(row["last_error"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
            for row in pending_rows
        ],
        "pending_count": pending_count,
    }


def collect_workspace_import_monitor(
    connection: sqlite3.Connection,
    source_ids: list[str],
    run_limit: int,
) -> dict[str, Any]:
    safe_run_limit = max(1, int(run_limit))
    scan_limit = max(safe_run_limit * 6, 24)
    rows = connection.execute(
        """
        SELECT
            run_id,
            source_scope,
            input_path,
            input_format,
            on_duplicate,
            dry_run,
            row_count,
            inserted_count,
            updated_count,
            skipped_count,
            error_count,
            status,
            COALESCE(error_message, '') AS error_message,
            started_at,
            finished_at
        FROM dictionary_import_runs
        ORDER BY finished_at DESC, run_id DESC
        LIMIT ?
        """,
        (scan_limit,),
    ).fetchall()

    scope_filter = {str(item).strip() for item in source_ids if str(item).strip()}

    def include_row(row: sqlite3.Row) -> bool:
        if not scope_filter:
            return True
        source_scope_raw = str(row["source_scope"] or "").strip()
        if not source_scope_raw:
            return True
        source_scope_items = {
            item.strip()
            for item in source_scope_raw.split(",")
            if item.strip()
        }
        return not source_scope_items.isdisjoint(scope_filter)

    filtered_rows = [row for row in rows if include_row(row)][:safe_run_limit]

    recent_runs: list[dict[str, Any]] = [
        {
            "run_id": int(row["run_id"] or 0),
            "source_scope": str(row["source_scope"] or ""),
            "input_path": str(row["input_path"] or ""),
            "input_format": str(row["input_format"] or ""),
            "on_duplicate": str(row["on_duplicate"] or ""),
            "dry_run": bool(int(row["dry_run"] or 0)),
            "row_count": int(row["row_count"] or 0),
            "inserted_count": int(row["inserted_count"] or 0),
            "updated_count": int(row["updated_count"] or 0),
            "skipped_count": int(row["skipped_count"] or 0),
            "error_count": int(row["error_count"] or 0),
            "status": str(row["status"] or ""),
            "error_message": str(row["error_message"] or ""),
            "started_at": str(row["started_at"] or ""),
            "finished_at": str(row["finished_at"] or ""),
        }
        for row in filtered_rows
    ]

    latest_summary: dict[str, Any] | None = None
    if recent_runs:
        latest = recent_runs[0]
        latest_summary = {
            "status": str(latest["status"] or ""),
            "dry_run": bool(latest["dry_run"]),
            "row_count": int(latest["row_count"] or 0),
            "inserted_count": int(latest["inserted_count"] or 0),
            "updated_count": int(latest["updated_count"] or 0),
            "skipped_count": int(latest["skipped_count"] or 0),
            "error_count": int(latest["error_count"] or 0),
            "finished_at": str(latest["finished_at"] or ""),
            "error_message": str(latest["error_message"] or ""),
        }

    return {
        "recent_runs": recent_runs,
        "latest": latest_summary,
    }


def read_managed_targets_payload(config_path: Path) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
    managed_path = resolve_managed_targets_path(config_path)
    default_payload: dict[str, Any] = {
        "version": MANAGED_TARGETS_FORMAT_VERSION,
        "targets": [],
    }
    if not managed_path.exists():
        return managed_path, default_payload, []
    try:
        parsed = json.loads(managed_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return managed_path, default_payload, []
    if not isinstance(parsed, dict):
        return managed_path, default_payload, []
    raw_targets = parsed.get("targets")
    if not isinstance(raw_targets, list):
        raw_targets = []
    normalized_targets: list[dict[str, Any]] = []
    for raw_item in raw_targets:
        if not isinstance(raw_item, dict):
            continue
        source_id = str(raw_item.get("id", "")).strip()
        if not source_id:
            continue
        normalized_targets.append(dict(raw_item))
    payload = dict(parsed)
    try:
        payload_version = int(payload.get("version") or MANAGED_TARGETS_FORMAT_VERSION)
    except (TypeError, ValueError):
        payload_version = MANAGED_TARGETS_FORMAT_VERSION
    payload["version"] = payload_version
    payload["targets"] = normalized_targets
    return managed_path, payload, normalized_targets


def write_managed_targets_payload(managed_path: Path, payload: dict[str, Any]) -> None:
    managed_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    temp_path = managed_path.with_name(f"{managed_path.name}.tmp")
    temp_path.write_text(serialized, encoding="utf-8")
    temp_path.replace(managed_path)


def normalize_source_target_id(raw_value: Any) -> str:
    source_id = str(raw_value or "").strip()
    if not source_id:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", source_id):
        return ""
    return source_id


def serialize_source_target(source: SourceConfig, video_count: int = 0) -> dict[str, Any]:
    return apply_source_tags_payload(
        {
        "id": source.id,
        "platform": source.platform,
        "enabled": bool(source.enabled),
        "watch_kind": source.watch_kind,
        "target_handle": source.target_handle or "",
        "handle": source.handle or "",
        "url": source.url,
        "data_dir": str(source.data_dir),
        "origin": source.origin,
        "video_count": max(0, int(video_count)),
        },
        manual_tags=source.tags,
        auto_tags=[],
    )


def build_web_handler(
    db_path: Path,
    static_dir: Path,
    allowed_source_ids: set[str],
    config_path: Path = DEFAULT_CONFIG,
    restrict_to_source_ids: bool = False,
) -> type[BaseHTTPRequestHandler]:
    static_root = static_dir.resolve()
    workspace_root = db_path.resolve().parent.parent
    web_config_path = config_path

    class SubstudyWebHandler(BaseHTTPRequestHandler):
        server_version = "SubstudyWeb/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write(
                f"[web] {self.address_string()} - {fmt % args}\n"
            )

        def _open_connection(self) -> sqlite3.Connection:
            connection = sqlite3.connect(str(db_path), timeout=30)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
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

        def _resolve_allowed_media_path(self, requested_path: Path) -> Path | None:
            requested_path_value = str(requested_path)
            if not requested_path_value:
                return None
            with self._open_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT source_id, media_path
                    FROM videos
                    WHERE has_media = 1
                      AND media_path = ?
                    LIMIT 8
                    """,
                    (requested_path_value,),
                ).fetchall()
            for row in rows:
                source_id = str(row["source_id"] or "").strip()
                if not source_id or not self._is_source_allowed(source_id):
                    continue
                media_path_value = row["media_path"]
                if media_path_value in (None, ""):
                    continue
                media_path = Path(str(media_path_value))
                if not media_path.exists() or not media_path.is_file():
                    continue
                return media_path
            return None

        def _serve_media_file(self, token: str) -> None:
            decoded_path = decode_path_token(token)
            media_path = None if decoded_path is None else self._resolve_allowed_media_path(decoded_path)
            if media_path is None:
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

        def _serve_workspace_artifact_file(self, token: str, force_download: bool = False) -> None:
            artifact_path = decode_path_token(token)
            exports_root = (workspace_root / "exports").resolve()
            if artifact_path is None:
                self._send_error_json(404, "Artifact file not found.")
                return
            try:
                resolved_path = artifact_path.resolve()
            except OSError:
                self._send_error_json(404, "Artifact file not found.")
                return
            if resolved_path != exports_root and exports_root not in resolved_path.parents:
                self._send_error_json(403, "Access denied.")
                return
            if not resolved_path.exists() or not resolved_path.is_file():
                self._send_error_json(404, "Artifact file not found.")
                return
            try:
                payload = resolved_path.read_bytes()
            except OSError:
                self._send_error_json(500, "Failed to read artifact file.")
                return

            suffix = resolved_path.suffix.lower()
            if suffix in {".jsonl", ".log", ".txt"}:
                content_type = "text/plain"
            elif suffix == ".json":
                content_type = "application/json"
            elif suffix == ".csv":
                content_type = "text/csv"
            else:
                content_type = mimetypes.guess_type(str(resolved_path))[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header(
                "Content-Type",
                f"{content_type}; charset=utf-8" if content_type.startswith("text/") else content_type,
            )
            self.send_header("Content-Length", str(len(payload)))
            filename = resolved_path.name.replace('"', "")
            if force_download:
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            else:
                self.send_header("Content-Disposition", f'inline; filename="{filename}"')
            self.end_headers()
            self.wfile.write(payload)

        def _load_config_bundle(self) -> tuple[GlobalConfig, list[SourceConfig]]:
            return load_config(web_config_path)

        def _load_all_config_sources(self) -> list[SourceConfig]:
            _, sources = self._load_config_bundle()
            return sources

        def _resolve_effective_source_scope(self) -> set[str] | None:
            try:
                configured_sources = self._load_all_config_sources()
            except (FileNotFoundError, ValueError, KeyError):
                if restrict_to_source_ids:
                    return set(allowed_source_ids)
                if allowed_source_ids:
                    return set(allowed_source_ids)
                return None

            enabled_source_ids = {source.id for source in configured_sources if source.enabled}
            if restrict_to_source_ids:
                return enabled_source_ids & set(allowed_source_ids)
            return enabled_source_ids

        def _load_config_source_map(self) -> dict[str, SourceConfig]:
            try:
                configured_sources = self._load_all_config_sources()
            except (FileNotFoundError, ValueError, KeyError):
                return {}
            return {
                source.id: source
                for source in configured_sources
            }

        def _is_source_allowed(self, source_id: str) -> bool:
            scope = self._resolve_effective_source_scope()
            if scope is None:
                return True
            return source_id in scope

        def _normalize_source(self, source_id: Any) -> str | None:
            if source_id in (None, ""):
                return None
            normalized = str(source_id).strip()
            if not normalized:
                return None
            return normalized

        def _handle_api_source_targets_get(self) -> None:
            try:
                global_config, configured_sources = self._load_config_bundle()
            except (FileNotFoundError, ValueError, KeyError) as exc:
                self._send_error_json(500, f"Failed to read config: {exc}")
                return

            video_count_by_source: dict[str, int] = {}
            processing_summary_by_source: dict[str, dict[str, Any]] = {}
            with self._open_connection() as connection:
                rows = connection.execute(
                    """
                    SELECT source_id, COUNT(*) AS video_count
                    FROM videos
                    GROUP BY source_id
                    """
                ).fetchall()
                for row in rows:
                    source_id = str(row["source_id"] or "")
                    video_count_by_source[source_id] = int(row["video_count"] or 0)
                processing_summary = collect_workspace_source_processing_summary(
                    connection=connection,
                    source_ids=[source.id for source in configured_sources],
                )
                processing_summary_by_source = {
                    str(item.get("source_id") or "").strip(): item
                    for item in cast(list[dict[str, Any]], processing_summary.get("sources") or [])
                    if str(item.get("source_id") or "").strip()
                }

            effective_scope = self._resolve_effective_source_scope()
            targets: list[dict[str, Any]] = []
            for source in configured_sources:
                item = serialize_source_target(
                    source,
                    video_count=video_count_by_source.get(source.id, 0),
                )
                auto_tags = compute_source_auto_tags(
                    processing_summary_by_source.get(source.id),
                    global_config.auto_tag_rules,
                )
                apply_source_tags_payload(item, manual_tags=source.tags, auto_tags=auto_tags)
                if effective_scope is None:
                    item["active_in_web"] = True
                else:
                    item["active_in_web"] = source.id in effective_scope
                targets.append(item)
            self._send_json(
                {
                    "targets": targets,
                    "managed_path": str(resolve_managed_targets_path(web_config_path)),
                }
            )

        def _handle_api_source_targets_upsert(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return

            source_id = normalize_source_target_id(payload.get("id"))
            if not source_id:
                self._send_error_json(
                    400,
                    "id is required and must match [A-Za-z0-9][A-Za-z0-9._-]{0,79}.",
                )
                return
            platform = str(payload.get("platform", "tiktok") or "tiktok").strip().lower()
            if platform != "tiktok":
                self._send_error_json(400, "Only `tiktok` platform is supported for managed targets.")
                return
            watch_kind = normalize_source_watch_kind(payload.get("watch_kind"))
            target_handle = normalize_tiktok_handle(
                payload.get("target_handle")
                if payload.get("target_handle") not in (None, "")
                else payload.get("handle")
            )
            url = str(payload.get("url", "")).strip()
            if not url and target_handle:
                url = build_tiktok_source_url(target_handle, watch_kind)
            if not url:
                self._send_error_json(400, "target_handle or url is required.")
                return

            managed_entry: dict[str, Any] = {
                "id": source_id,
                "platform": platform,
                "watch_kind": watch_kind,
                "enabled": parse_bool_like(payload.get("enabled"), default=True),
                "url": url,
            }
            tags = normalize_source_tags(payload.get("tags"))
            if tags:
                managed_entry["tags"] = tags
            if target_handle:
                managed_entry["target_handle"] = target_handle
                if watch_kind == SOURCE_WATCH_KIND_POSTS:
                    managed_entry["handle"] = target_handle
            data_dir = str(payload.get("data_dir", "")).strip()
            if data_dir:
                managed_entry["data_dir"] = data_dir
            video_url_template = str(payload.get("video_url_template", "")).strip()
            if video_url_template:
                managed_entry["video_url_template"] = video_url_template

            managed_path, managed_payload, managed_targets = read_managed_targets_payload(web_config_path)
            updated = False
            for index, row in enumerate(managed_targets):
                if str(row.get("id", "")).strip() != source_id:
                    continue
                managed_targets[index] = managed_entry
                updated = True
                break
            if not updated:
                managed_targets.append(managed_entry)
            managed_payload["version"] = MANAGED_TARGETS_FORMAT_VERSION
            managed_payload["targets"] = managed_targets
            try:
                write_managed_targets_payload(managed_path, managed_payload)
            except OSError as exc:
                self._send_error_json(500, f"Failed to write managed targets file: {exc}")
                return

            self._send_json(
                {
                    "status": "updated" if updated else "created",
                    "id": source_id,
                    "managed_path": str(managed_path),
                }
            )

        def _handle_api_source_targets_remove(self) -> None:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._send_error_json(400, str(exc))
                return
            source_id = normalize_source_target_id(payload.get("id"))
            if not source_id:
                self._send_error_json(400, "id is required.")
                return

            managed_path, managed_payload, managed_targets = read_managed_targets_payload(web_config_path)
            filtered_targets = [
                row for row in managed_targets
                if str(row.get("id", "")).strip() != source_id
            ]
            if len(filtered_targets) == len(managed_targets):
                self._send_error_json(404, f"No managed override found for source id: {source_id}")
                return
            managed_payload["version"] = MANAGED_TARGETS_FORMAT_VERSION
            managed_payload["targets"] = filtered_targets
            try:
                write_managed_targets_payload(managed_path, managed_payload)
            except OSError as exc:
                self._send_error_json(500, f"Failed to write managed targets file: {exc}")
                return

            self._send_json(
                {
                    "status": "removed",
                    "id": source_id,
                    "managed_path": str(managed_path),
                }
            )

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
            if translation_filter not in {
                "all",
                "ja_only",
                "ja",
                "ja-local",
                "ja-asr-local",
                "ja_missing",
            }:
                translation_filter = "all"

            limit = clamp_int(query.get("limit", [None])[0], default=180, minimum=1, maximum=1000)
            offset = clamp_int(query.get("offset", [None])[0], default=0, minimum=0, maximum=20000)
            effective_scope = self._resolve_effective_source_scope()
            if effective_scope is not None and not effective_scope:
                self._send_json(
                    {
                        "videos": [],
                        "count": 0,
                        "sources": [],
                        "translation_filter": translation_filter,
                    }
                )
                return

            def any_japanese_subtitle_sql(subtitle_alias: str) -> str:
                language_expr = f"LOWER(COALESCE({subtitle_alias}.language, ''))"
                path_expr = f"LOWER(COALESCE({subtitle_alias}.subtitle_path, ''))"
                return f"""
                (
                    {language_expr} = 'ja'
                    OR {language_expr} LIKE 'ja-%'
                    OR {language_expr} = 'jp'
                    OR {language_expr} LIKE 'jp-%'
                    OR {language_expr} = 'jpn'
                    OR {language_expr} LIKE 'jpn-%'
                    OR {language_expr} LIKE '%.ja'
                    OR {language_expr} LIKE '%.ja-%'
                    OR {language_expr} LIKE '%.jp'
                    OR {language_expr} LIKE '%.jp-%'
                    OR {language_expr} LIKE '%.jpn'
                    OR {language_expr} LIKE '%.jpn-%'
                    OR {language_expr} LIKE '%japanese%'
                    OR {path_expr} LIKE '%.ja.%'
                    OR {path_expr} LIKE '%.ja-%'
                    OR {path_expr} LIKE '%.jp.%'
                    OR {path_expr} LIKE '%.jp-%'
                    OR {path_expr} LIKE '%.jpn.%'
                    OR {path_expr} LIKE '%.jpn-%'
                    OR {path_expr} LIKE '%japanese%'
                )
                """

            def generated_japanese_subtitle_exclusion_sql(subtitle_alias: str) -> str:
                language_expr = f"LOWER(COALESCE({subtitle_alias}.language, ''))"
                path_expr = f"LOWER(COALESCE({subtitle_alias}.subtitle_path, ''))"
                return f"""
                (
                    {language_expr} NOT LIKE 'ja-local%'
                    AND {language_expr} NOT LIKE 'ja-asr-local%'
                    AND {path_expr} NOT LIKE '%.ja-local.%'
                    AND {path_expr} NOT LIKE '%.ja-local-%'
                    AND {path_expr} NOT LIKE '%.ja-asr-local.%'
                    AND {path_expr} NOT LIKE '%.ja-asr-local-%'
                )
                """

            def ja_subtitle_exists_clause(video_alias: str) -> str:
                return f"""
                EXISTS (
                    SELECT 1
                    FROM subtitles sja
                    WHERE sja.source_id = {video_alias}.source_id
                      AND sja.video_id = {video_alias}.video_id
                      AND {any_japanese_subtitle_sql('sja')}
                )
                """

            def ja_variant_exists_clause(video_alias: str, variant: str) -> str:
                normalized_variant = str(variant or "").strip().lower()
                if normalized_variant == "ja_only":
                    return ja_subtitle_exists_clause(video_alias)
                if normalized_variant == "ja_missing":
                    return f"NOT ({ja_subtitle_exists_clause(video_alias)})"
                if normalized_variant == "ja-asr-local":
                    return f"""
                    EXISTS (
                        SELECT 1
                        FROM subtitles sja
                        WHERE sja.source_id = {video_alias}.source_id
                          AND sja.video_id = {video_alias}.video_id
                          AND (
                            LOWER(COALESCE(sja.language, '')) = 'ja-asr-local'
                            OR LOWER(COALESCE(sja.language, '')) LIKE 'ja-asr-local-%'
                            OR LOWER(COALESCE(sja.subtitle_path, '')) LIKE '%.ja-asr-local.%'
                          )
                    )
                    """
                if normalized_variant == "ja-local":
                    return f"""
                    EXISTS (
                        SELECT 1
                        FROM subtitles sja
                        WHERE sja.source_id = {video_alias}.source_id
                          AND sja.video_id = {video_alias}.video_id
                          AND (
                            LOWER(COALESCE(sja.language, '')) = 'ja-local'
                            OR LOWER(COALESCE(sja.language, '')) LIKE 'ja-local-%'
                            OR LOWER(COALESCE(sja.subtitle_path, '')) LIKE '%.ja-local.%'
                          )
                    )
                    """
                if normalized_variant == "ja":
                    return f"""
                    EXISTS (
                        SELECT 1
                        FROM subtitles sja
                        WHERE sja.source_id = {video_alias}.source_id
                          AND sja.video_id = {video_alias}.video_id
                          AND {any_japanese_subtitle_sql('sja')}
                          AND {generated_japanese_subtitle_exclusion_sql('sja')}
                    )
                    """
                return ""

            def public_tracks_match_translation_filter(
                public_tracks: list[dict[str, Any]],
                filter_value: str,
            ) -> bool:
                normalized_filter = str(filter_value or "all").strip().lower()
                if normalized_filter == "all":
                    return True
                variants: set[str] = set()
                for track in public_tracks:
                    if str(track.get("kind") or "").strip().lower() != "subtitle":
                        continue
                    label = str(track.get("label") or "").strip().lower()
                    if not label:
                        continue
                    variant = classify_ja_subtitle_variant(label, label)
                    if variant:
                        variants.add(variant)
                if normalized_filter == "ja_only":
                    return bool(variants)
                if normalized_filter == "ja_missing":
                    return not variants
                return normalized_filter in variants

            where_clauses = [
                "v.has_media = 1",
                "v.media_path IS NOT NULL",
            ]
            params: list[Any] = []
            if effective_scope is not None:
                placeholders = ",".join("?" for _ in sorted(effective_scope))
                where_clauses.append(f"v.source_id IN ({placeholders})")
                params.extend(sorted(effective_scope))
            if source_filter:
                where_clauses.append("v.source_id = ?")
                params.append(source_filter)
            translation_filter_clause = ja_variant_exists_clause("v", translation_filter)
            if translation_filter_clause:
                where_clauses.append(translation_filter_clause)

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
                        COALESCE(vd.created_at, '') AS disliked_created_at,
                        COALESCE(vni.created_at, '') AS not_interested_created_at,
                        COALESCE(vps.impression_count, 0) AS playback_impression_count,
                        COALESCE(vps.play_count, 0) AS playback_play_count,
                        COALESCE(vps.total_watch_seconds, 0) AS playback_total_watch_seconds,
                        COALESCE(vps.completed_count, 0) AS playback_completed_count,
                        COALESCE(vps.fast_skip_count, 0) AS playback_fast_skip_count,
                        COALESCE(vps.shallow_skip_count, 0) AS playback_shallow_skip_count,
                        COALESCE(vps.last_served_at, '') AS playback_last_served_at,
                        COALESCE(vps.last_played_at, '') AS playback_last_played_at,
                        COALESCE(vps.last_completed_at, '') AS playback_last_completed_at,
                        vps.last_position_seconds AS playback_last_position_seconds,
                        CASE
                            WHEN vf.video_id IS NULL THEN 0
                            ELSE 1
                        END AS is_favorite,
                        CASE
                            WHEN vd.video_id IS NULL THEN 0
                            ELSE 1
                        END AS is_disliked,
                        CASE
                            WHEN vni.video_id IS NULL THEN 0
                            ELSE 1
                        END AS is_not_interested,
                        (
                            SELECT COUNT(DISTINCT (
                                COALESCE(sb.track, '')
                                || ':'
                                || CAST(sb.start_ms AS TEXT)
                                || ':'
                                || CAST(sb.end_ms AS TEXT)
                            ))
                            FROM subtitle_bookmarks sb
                            WHERE sb.source_id = v.source_id
                              AND sb.video_id = v.video_id
                        ) AS cue_bookmark_count,
                        (
                            SELECT COUNT(*)
                            FROM dictionary_bookmarks db
                            WHERE db.source_id = v.source_id
                              AND db.video_id = v.video_id
                        ) AS dictionary_bookmark_count,
                        (
                            SELECT COUNT(DISTINCT COALESCE(db.term_norm, ''))
                            FROM dictionary_bookmarks db
                            WHERE db.source_id = v.source_id
                              AND db.video_id = v.video_id
                              AND TRIM(COALESCE(db.term_norm, '')) != ''
                        ) AS dictionary_bookmark_unique_term_count,
                        COALESCE(vn.note, '') AS video_note,
                        v.audio_lufs,
                        v.audio_gain_db
                    FROM videos v
                    LEFT JOIN video_favorites vf
                      ON vf.source_id = v.source_id
                     AND vf.video_id = v.video_id
                    LEFT JOIN video_dislikes vd
                      ON vd.source_id = v.source_id
                     AND vd.video_id = v.video_id
                    LEFT JOIN video_not_interested vni
                      ON vni.source_id = v.source_id
                     AND vni.video_id = v.video_id
                    LEFT JOIN video_playback_stats vps
                      ON vps.source_id = v.source_id
                     AND vps.video_id = v.video_id
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

                source_scope_ids: list[str] | None = None
                if source_filter:
                    source_scope_ids = [source_filter]
                elif effective_scope is not None:
                    source_scope_ids = sorted(effective_scope)

                source_where_clauses = [
                    "has_media = 1",
                    "media_path IS NOT NULL",
                    "media_path <> ''",
                ]
                source_params: list[Any] = []
                if source_scope_ids is not None:
                    placeholders = ",".join("?" for _ in source_scope_ids)
                    source_where_clauses.append(f"source_id IN ({placeholders})")
                    source_params.extend(source_scope_ids)
                source_translation_filter_clause = ja_variant_exists_clause("videos", translation_filter)
                if source_translation_filter_clause:
                    source_where_clauses.append(source_translation_filter_clause)
                source_rows = connection.execute(
                    f"""
                    SELECT source_id, media_path
                    FROM videos
                    WHERE {" AND ".join(source_where_clauses)}
                    ORDER BY source_id ASC
                    """,
                    tuple(source_params),
                ).fetchall()
                source_ids: list[str] = []
                source_seen: set[str] = set()
                for row in source_rows:
                    source_id_value = str(row["source_id"] or "").strip()
                    if not source_id_value:
                        continue
                    if source_id_value in source_seen:
                        continue
                    media_path_value = row["media_path"]
                    if media_path_value in (None, ""):
                        continue
                    media_path = Path(str(media_path_value))
                    if not media_path.exists() or not media_path.is_file():
                        continue
                    source_seen.add(source_id_value)
                    source_ids.append(source_id_value)
                if source_scope_ids is not None:
                    for source_id_value in source_scope_ids:
                        if source_id_value in source_seen:
                            continue
                        source_seen.add(source_id_value)
                        source_ids.append(source_id_value)

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
                            "language": track.get("language", track["label"]),
                            "origin_kind": track.get("origin_kind", "upstream"),
                            "origin_detail": track.get("origin_detail", ""),
                            "origin_label": track.get("origin_label", ""),
                            "display_label": track.get("display_label", track["label"]),
                        }
                        for track in tracks
                    ]
                    if not public_tracks_match_translation_filter(public_tracks, translation_filter):
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
                            "is_disliked": bool(row["is_disliked"]),
                            "disliked_created_at": (
                                ""
                                if row["disliked_created_at"] in (None, "")
                                else str(row["disliked_created_at"])
                            ),
                            "is_not_interested": bool(row["is_not_interested"]),
                            "not_interested_created_at": (
                                ""
                                if row["not_interested_created_at"] in (None, "")
                                else str(row["not_interested_created_at"])
                            ),
                            "cue_bookmark_count": max(0, int(row["cue_bookmark_count"] or 0)),
                            "dictionary_bookmark_count": max(
                                0, int(row["dictionary_bookmark_count"] or 0)
                            ),
                            "dictionary_bookmark_unique_term_count": max(
                                0, int(row["dictionary_bookmark_unique_term_count"] or 0)
                            ),
                            "playback_stats": {
                                "impression_count": max(0, int(row["playback_impression_count"] or 0)),
                                "play_count": max(0, int(row["playback_play_count"] or 0)),
                                "total_watch_seconds": (
                                    0.0
                                    if row["playback_total_watch_seconds"] in (None, "")
                                    else max(0.0, float(row["playback_total_watch_seconds"]))
                                ),
                                "completed_count": max(0, int(row["playback_completed_count"] or 0)),
                                "fast_skip_count": max(0, int(row["playback_fast_skip_count"] or 0)),
                                "shallow_skip_count": max(0, int(row["playback_shallow_skip_count"] or 0)),
                                "last_served_at": (
                                    ""
                                    if row["playback_last_served_at"] in (None, "")
                                    else str(row["playback_last_served_at"])
                                ),
                                "last_played_at": (
                                    ""
                                    if row["playback_last_played_at"] in (None, "")
                                    else str(row["playback_last_played_at"])
                                ),
                                "last_completed_at": (
                                    ""
                                    if row["playback_last_completed_at"] in (None, "")
                                    else str(row["playback_last_completed_at"])
                                ),
                                "last_position_seconds": safe_float(row["playback_last_position_seconds"]),
                            },
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

        def _handle_api_workspace(self, query: dict[str, list[str]]) -> None:
            source_filter = self._normalize_source(query.get("source_id", [None])[0])
            if source_filter and not self._is_source_allowed(source_filter):
                self._send_error_json(403, "Source is not allowed.")
                return

            review_limit = clamp_int(
                query.get("review_limit", [None])[0],
                default=24,
                minimum=1,
                maximum=160,
            )
            missing_limit = clamp_int(
                query.get("missing_limit", [None])[0],
                default=20,
                minimum=1,
                maximum=160,
            )
            run_limit = clamp_int(
                query.get("run_limit", [None])[0],
                default=10,
                minimum=1,
                maximum=80,
            )
            import_run_limit = clamp_int(
                query.get("import_run_limit", [None])[0],
                default=6,
                minimum=1,
                maximum=80,
            )
            pending_limit = clamp_int(
                query.get("pending_limit", [None])[0],
                default=20,
                minimum=1,
                maximum=200,
            )
            since_hours = clamp_int(
                query.get("since_hours", [None])[0],
                default=72,
                minimum=1,
                maximum=24 * 30,
            )
            artifact_limit = clamp_int(
                query.get("artifact_limit", [None])[0],
                default=24,
                minimum=1,
                maximum=240,
            )

            effective_scope = self._resolve_effective_source_scope()
            source_scope: list[str] = []
            if source_filter:
                source_scope = [source_filter]
            elif effective_scope is not None:
                source_scope = sorted(effective_scope)

            with self._open_connection() as connection:
                review_cards, missing_entries = collect_workspace_review_and_missing_rows(
                    connection=connection,
                    source_ids=source_scope,
                    review_limit=review_limit,
                    missing_limit=missing_limit,
                )
                download_monitor = collect_workspace_download_monitor(
                    connection=connection,
                    source_ids=source_scope,
                    since_hours=since_hours,
                    run_limit=run_limit,
                    pending_limit=pending_limit,
                )
                source_processing = collect_workspace_source_processing_summary(
                    connection=connection,
                    source_ids=source_scope,
                )
                import_monitor = collect_workspace_import_monitor(
                    connection=connection,
                    source_ids=source_scope,
                    run_limit=import_run_limit,
                )
            try:
                global_config, configured_sources = self._load_config_bundle()
            except (FileNotFoundError, ValueError, KeyError):
                global_config = GlobalConfig(
                    ledger_db=DEFAULT_LEDGER_DB,
                    ledger_csv=DEFAULT_LEDGER_CSV,
                    source_order="random",
                    auto_tag_rules=[],
                )
                configured_sources = []
            source_config_by_id = {
                source.id: source
                for source in configured_sources
            }
            for item in cast(list[dict[str, Any]], source_processing.get("sources") or []):
                source_config = source_config_by_id.get(str(item.get("source_id") or "").strip())
                manual_tags = list(source_config.tags) if source_config is not None else []
                auto_tags = compute_source_auto_tags(item, global_config.auto_tag_rules)
                apply_source_tags_payload(item, manual_tags=manual_tags, auto_tags=auto_tags)
            review_hints_by_card_id = load_workspace_review_hints(workspace_root)
            review_cards = apply_workspace_review_hints(review_cards, review_hints_by_card_id)
            translation_qa_by_card_id = load_workspace_translation_qa(workspace_root)
            review_cards = apply_workspace_translation_qa(review_cards, translation_qa_by_card_id)
            missing_entries = apply_workspace_translation_qa(missing_entries, translation_qa_by_card_id)
            missing_entries = apply_workspace_missing_entry_states(missing_entries)

            artifacts = collect_workspace_artifacts(
                root_dir=workspace_root,
                limit=artifact_limit,
            )
            self._send_json(
                {
                    "source_id": source_filter or "",
                    "review_cards": review_cards,
                    "missing_entries": missing_entries,
                    "download_monitor": download_monitor,
                    "source_processing": source_processing,
                    "import_monitor": import_monitor,
                    "artifacts": artifacts,
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

        def _fetch_video_preference_state(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
        ) -> dict[str, Any]:
            favorite_row = connection.execute(
                """
                SELECT created_at
                FROM video_favorites
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            dislike_row = connection.execute(
                """
                SELECT created_at
                FROM video_dislikes
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            not_interested_row = connection.execute(
                """
                SELECT created_at
                FROM video_not_interested
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            return {
                "is_favorite": favorite_row is not None,
                "favorite_created_at": (
                    ""
                    if favorite_row is None or favorite_row["created_at"] in (None, "")
                    else str(favorite_row["created_at"])
                ),
                "is_disliked": dislike_row is not None,
                "disliked_created_at": (
                    ""
                    if dislike_row is None or dislike_row["created_at"] in (None, "")
                    else str(dislike_row["created_at"])
                ),
                "is_not_interested": not_interested_row is not None,
                "not_interested_created_at": (
                    ""
                    if (
                        not_interested_row is None
                        or not_interested_row["created_at"] in (None, "")
                    )
                    else str(not_interested_row["created_at"])
                ),
            }

        def _fetch_video_playback_stats(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
        ) -> dict[str, Any]:
            row = connection.execute(
                """
                SELECT
                    impression_count,
                    play_count,
                    total_watch_seconds,
                    completed_count,
                    fast_skip_count,
                    shallow_skip_count,
                    last_served_at,
                    last_played_at,
                    last_completed_at,
                    last_position_seconds
                FROM video_playback_stats
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            if row is None:
                return {
                    "impression_count": 0,
                    "play_count": 0,
                    "total_watch_seconds": 0.0,
                    "completed_count": 0,
                    "fast_skip_count": 0,
                    "shallow_skip_count": 0,
                    "last_served_at": "",
                    "last_played_at": "",
                    "last_completed_at": "",
                    "last_position_seconds": None,
                }
            return {
                "impression_count": max(0, int(row["impression_count"] or 0)),
                "play_count": max(0, int(row["play_count"] or 0)),
                "total_watch_seconds": (
                    0.0
                    if row["total_watch_seconds"] in (None, "")
                    else max(0.0, float(row["total_watch_seconds"]))
                ),
                "completed_count": max(0, int(row["completed_count"] or 0)),
                "fast_skip_count": max(0, int(row["fast_skip_count"] or 0)),
                "shallow_skip_count": max(0, int(row["shallow_skip_count"] or 0)),
                "last_served_at": (
                    ""
                    if row["last_served_at"] in (None, "")
                    else str(row["last_served_at"])
                ),
                "last_played_at": (
                    ""
                    if row["last_played_at"] in (None, "")
                    else str(row["last_played_at"])
                ),
                "last_completed_at": (
                    ""
                    if row["last_completed_at"] in (None, "")
                    else str(row["last_completed_at"])
                ),
                "last_position_seconds": safe_float(row["last_position_seconds"]),
            }

        def _record_video_playback_stats(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
            impression_increment: int,
            play_increment: int,
            watch_seconds: float,
            completed_increment: int,
            fast_skip_increment: int,
            shallow_skip_increment: int,
            last_position_seconds: float | None,
        ) -> dict[str, Any]:
            row = connection.execute(
                """
                SELECT
                    impression_count,
                    play_count,
                    total_watch_seconds,
                    completed_count,
                    fast_skip_count,
                    shallow_skip_count,
                    last_served_at,
                    last_played_at,
                    last_completed_at,
                    last_position_seconds,
                    created_at
                FROM video_playback_stats
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            now_iso = now_utc_iso()
            safe_impression_increment = max(0, int(impression_increment))
            safe_play_increment = max(0, int(play_increment))
            safe_watch_seconds = max(0.0, float(watch_seconds))
            safe_completed_increment = max(0, int(completed_increment))
            safe_fast_skip_increment = max(0, int(fast_skip_increment))
            safe_shallow_skip_increment = max(0, int(shallow_skip_increment))
            safe_last_position_seconds = (
                None
                if last_position_seconds is None
                else max(0.0, float(last_position_seconds))
            )

            if row is None:
                created_at = now_iso
                impression_count = safe_impression_increment
                play_count = safe_play_increment
                total_watch_seconds = safe_watch_seconds
                completed_count = safe_completed_increment
                fast_skip_count = safe_fast_skip_increment
                shallow_skip_count = safe_shallow_skip_increment
                last_served_at = now_iso if safe_impression_increment > 0 else ""
                last_played_at = (
                    now_iso
                    if (safe_play_increment > 0 or safe_watch_seconds > 0 or safe_completed_increment > 0)
                    else ""
                )
                last_completed_at = now_iso if safe_completed_increment > 0 else ""
                connection.execute(
                    """
                    INSERT INTO video_playback_stats (
                        source_id,
                        video_id,
                        impression_count,
                        play_count,
                        total_watch_seconds,
                        completed_count,
                        fast_skip_count,
                        shallow_skip_count,
                        last_served_at,
                        last_played_at,
                        last_completed_at,
                        last_position_seconds,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        video_id,
                        impression_count,
                        play_count,
                        total_watch_seconds,
                        completed_count,
                        fast_skip_count,
                        shallow_skip_count,
                        last_served_at or None,
                        last_played_at or None,
                        last_completed_at or None,
                        safe_last_position_seconds,
                        created_at,
                        now_iso,
                    ),
                )
            else:
                created_at = (
                    now_iso
                    if row["created_at"] in (None, "")
                    else str(row["created_at"])
                )
                impression_count = max(0, int(row["impression_count"] or 0)) + safe_impression_increment
                play_count = max(0, int(row["play_count"] or 0)) + safe_play_increment
                total_watch_seconds = (
                    0.0
                    if row["total_watch_seconds"] in (None, "")
                    else max(0.0, float(row["total_watch_seconds"]))
                ) + safe_watch_seconds
                completed_count = max(0, int(row["completed_count"] or 0)) + safe_completed_increment
                fast_skip_count = max(0, int(row["fast_skip_count"] or 0)) + safe_fast_skip_increment
                shallow_skip_count = (
                    max(0, int(row["shallow_skip_count"] or 0)) + safe_shallow_skip_increment
                )
                last_served_at = (
                    now_iso
                    if safe_impression_increment > 0
                    else (
                        ""
                        if row["last_served_at"] in (None, "")
                        else str(row["last_served_at"])
                    )
                )
                last_played_at = (
                    now_iso
                    if (safe_play_increment > 0 or safe_watch_seconds > 0 or safe_completed_increment > 0)
                    else (
                        ""
                        if row["last_played_at"] in (None, "")
                        else str(row["last_played_at"])
                    )
                )
                last_completed_at = (
                    now_iso
                    if safe_completed_increment > 0
                    else (
                        ""
                        if row["last_completed_at"] in (None, "")
                        else str(row["last_completed_at"])
                    )
                )
                next_last_position_seconds = (
                    safe_last_position_seconds
                    if safe_last_position_seconds is not None
                    else safe_float(row["last_position_seconds"])
                )
                connection.execute(
                    """
                    UPDATE video_playback_stats
                    SET impression_count = ?,
                        play_count = ?,
                        total_watch_seconds = ?,
                        completed_count = ?,
                        fast_skip_count = ?,
                        shallow_skip_count = ?,
                        last_served_at = ?,
                        last_played_at = ?,
                        last_completed_at = ?,
                        last_position_seconds = ?,
                        updated_at = ?
                    WHERE source_id = ?
                      AND video_id = ?
                    """,
                    (
                        impression_count,
                        play_count,
                        total_watch_seconds,
                        completed_count,
                        fast_skip_count,
                        shallow_skip_count,
                        last_served_at or None,
                        last_played_at or None,
                        last_completed_at or None,
                        next_last_position_seconds,
                        now_iso,
                        source_id,
                        video_id,
                    ),
                )

            return self._fetch_video_playback_stats(connection, source_id, video_id)

        def _toggle_video_preference_mark(
            self,
            connection: sqlite3.Connection,
            source_id: str,
            video_id: str,
            selected_table: str,
            cleared_tables: Iterable[str] = (),
        ) -> tuple[bool, set[str]]:
            existing = connection.execute(
                f"""
                SELECT 1
                FROM {selected_table}
                WHERE source_id = ?
                  AND video_id = ?
                LIMIT 1
                """,
                (source_id, video_id),
            ).fetchone()
            if existing is None:
                connection.execute(
                    f"""
                    INSERT INTO {selected_table}(source_id, video_id, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (source_id, video_id, now_utc_iso()),
                )
                cleared_other_tables: set[str] = set()
                for cleared_table in cleared_tables:
                    other_existing = connection.execute(
                        f"""
                        SELECT 1
                        FROM {cleared_table}
                        WHERE source_id = ?
                          AND video_id = ?
                        LIMIT 1
                        """,
                        (source_id, video_id),
                    ).fetchone()
                    if other_existing is None:
                        continue
                    cleared_other_tables.add(str(cleared_table))
                    if other_existing is not None:
                        connection.execute(
                            f"""
                            DELETE FROM {cleared_table}
                            WHERE source_id = ?
                              AND video_id = ?
                            """,
                            (source_id, video_id),
                        )
                return True, cleared_other_tables

            connection.execute(
                f"""
                DELETE FROM {selected_table}
                WHERE source_id = ?
                  AND video_id = ?
                """,
                (source_id, video_id),
            )
            return False, set()

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

                favorited, cleared_tables = self._toggle_video_preference_mark(
                    connection=connection,
                    source_id=source_id,
                    video_id=video_id,
                    selected_table="video_favorites",
                    cleared_tables=("video_dislikes", "video_not_interested"),
                )
                state_payload = self._fetch_video_preference_state(connection, source_id, video_id)
                connection.commit()

            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "is_favorite": favorited,
                    "favorite_created_at": str(state_payload["favorite_created_at"] or ""),
                    "is_disliked": bool(state_payload["is_disliked"]),
                    "disliked_created_at": str(state_payload["disliked_created_at"] or ""),
                    "is_not_interested": bool(state_payload["is_not_interested"]),
                    "not_interested_created_at": str(state_payload["not_interested_created_at"] or ""),
                    "cleared_dislike": "video_dislikes" in cleared_tables,
                    "cleared_not_interested": "video_not_interested" in cleared_tables,
                }
            )

        def _handle_api_toggle_dislike(self) -> None:
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

                disliked, cleared_tables = self._toggle_video_preference_mark(
                    connection=connection,
                    source_id=source_id,
                    video_id=video_id,
                    selected_table="video_dislikes",
                    cleared_tables=("video_favorites", "video_not_interested"),
                )
                state_payload = self._fetch_video_preference_state(connection, source_id, video_id)
                connection.commit()

            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "is_favorite": bool(state_payload["is_favorite"]),
                    "favorite_created_at": str(state_payload["favorite_created_at"] or ""),
                    "is_disliked": disliked,
                    "disliked_created_at": str(state_payload["disliked_created_at"] or ""),
                    "is_not_interested": bool(state_payload["is_not_interested"]),
                    "not_interested_created_at": str(state_payload["not_interested_created_at"] or ""),
                    "cleared_favorite": "video_favorites" in cleared_tables,
                    "cleared_not_interested": "video_not_interested" in cleared_tables,
                }
            )

        def _handle_api_toggle_not_interested(self) -> None:
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

                not_interested, cleared_tables = self._toggle_video_preference_mark(
                    connection=connection,
                    source_id=source_id,
                    video_id=video_id,
                    selected_table="video_not_interested",
                    cleared_tables=("video_favorites", "video_dislikes"),
                )
                state_payload = self._fetch_video_preference_state(connection, source_id, video_id)
                connection.commit()

            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "is_favorite": bool(state_payload["is_favorite"]),
                    "favorite_created_at": str(state_payload["favorite_created_at"] or ""),
                    "is_disliked": bool(state_payload["is_disliked"]),
                    "disliked_created_at": str(state_payload["disliked_created_at"] or ""),
                    "is_not_interested": not_interested,
                    "not_interested_created_at": str(state_payload["not_interested_created_at"] or ""),
                    "cleared_favorite": "video_favorites" in cleared_tables,
                    "cleared_dislike": "video_dislikes" in cleared_tables,
                }
            )

        def _handle_api_record_playback_stats(self) -> None:
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

            try:
                impression_increment = int(payload.get("impression_increment", 0) or 0)
                play_increment = int(payload.get("play_increment", 0) or 0)
                completed_increment = int(payload.get("completed_increment", 0) or 0)
                fast_skip_increment = int(payload.get("fast_skip_increment", 0) or 0)
                shallow_skip_increment = int(payload.get("shallow_skip_increment", 0) or 0)
            except (TypeError, ValueError):
                self._send_error_json(
                    400,
                    "impression_increment, play_increment, completed_increment, fast_skip_increment, and shallow_skip_increment must be integers.",
                )
                return
            try:
                watch_seconds = float(payload.get("watch_seconds", 0) or 0)
            except (TypeError, ValueError):
                self._send_error_json(400, "watch_seconds must be numeric.")
                return
            last_position_seconds_raw = payload.get("last_position_seconds")
            if last_position_seconds_raw in (None, ""):
                last_position_seconds = None
            else:
                try:
                    last_position_seconds = float(last_position_seconds_raw)
                except (TypeError, ValueError):
                    self._send_error_json(400, "last_position_seconds must be numeric.")
                    return

            with self._open_connection() as connection:
                if not self._validate_video_exists(connection, source_id, video_id):
                    self._send_error_json(404, "Video not found.")
                    return
                stats_payload = self._record_video_playback_stats(
                    connection=connection,
                    source_id=source_id,
                    video_id=video_id,
                    impression_increment=impression_increment,
                    play_increment=play_increment,
                    watch_seconds=watch_seconds,
                    completed_increment=completed_increment,
                    fast_skip_increment=fast_skip_increment,
                    shallow_skip_increment=shallow_skip_increment,
                    last_position_seconds=last_position_seconds,
                )
                connection.commit()

            self._send_json(
                {
                    "source_id": source_id,
                    "video_id": video_id,
                    "playback_stats": stats_payload,
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
                if path.startswith("/artifact/"):
                    token = path[len("/artifact/") :]
                    force_download = parse_bool_flag(query.get("download", [None])[0], default=False)
                    self._serve_workspace_artifact_file(token, force_download=force_download)
                    return
                if path == "/api/source-targets":
                    self._handle_api_source_targets_get()
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
                if path == "/api/workspace":
                    self._handle_api_workspace(query)
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
                if path == "/api/dislikes/toggle":
                    self._handle_api_toggle_dislike()
                    return
                if path == "/api/not-interested/toggle":
                    self._handle_api_toggle_not_interested()
                    return
                if path == "/api/playback-stats/record":
                    self._handle_api_record_playback_stats()
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
                if path == "/api/source-targets/upsert":
                    self._handle_api_source_targets_upsert()
                    return
                if path == "/api/source-targets/remove":
                    self._handle_api_source_targets_remove()
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
    config_path: Path,
    host: str,
    port: int,
    restrict_to_source_ids: bool = False,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path), timeout=30) as bootstrap_connection:
        bootstrap_connection.execute("PRAGMA journal_mode=WAL")
        create_schema(bootstrap_connection)
        bootstrap_connection.commit()

    if not WEB_STATIC_DIR.exists():
        raise FileNotFoundError(f"Web static directory not found: {WEB_STATIC_DIR}")

    handler_cls = build_web_handler(
        db_path=db_path,
        static_dir=WEB_STATIC_DIR,
        allowed_source_ids=set(source_ids),
        config_path=config_path,
        restrict_to_source_ids=restrict_to_source_ids,
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


def language_rank_for_translation_source(language: str, target_lang: str) -> int:
    normalized = str(language or "").strip().lower()
    safe_target = str(target_lang or "ja").strip().lower()
    if not normalized:
        return 30
    if normalized == safe_target or normalized.startswith(f"{safe_target}-"):
        return 1000
    if normalized == "en":
        return 0
    if normalized.startswith("en-"):
        return 1
    if normalized in {"und", "na"}:
        return 2
    normalized_tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", normalized)
        if token
    }
    if normalized_tokens & {"en", "eng", "english"}:
        return 1
    return 10


def normalize_translation_source_track(value: str | None, default: str = "subtitle") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"subtitle", "asr", "auto"}:
        return normalized
    safe_default = str(default or "subtitle").strip().lower()
    return safe_default if safe_default in {"subtitle", "asr", "auto"} else "subtitle"


def infer_translation_source_lang(language: str, fallback: str = "en") -> str:
    normalized = str(language or "").strip().lower()
    safe_fallback = str(fallback or "en").strip().lower() or "en"
    if not normalized:
        return safe_fallback
    if normalized in {"und", "na"}:
        return safe_fallback
    tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", normalized)
        if token
    }
    if tokens & {"en", "eng", "english"}:
        return "en"
    return normalized


def collect_local_translation_targets(
    connection: sqlite3.Connection,
    source_ids: list[str],
    target_lang: str,
    source_track: str,
    video_ids: list[str] | None,
    limit: int,
    include_translated: bool,
    overwrite: bool,
) -> list[dict[str, Any]]:
    safe_target = str(target_lang or "ja").strip().lower() or "ja"
    safe_source_track = normalize_translation_source_track(source_track, "subtitle")
    video_filter_set = {str(video_id).strip() for video_id in (video_ids or []) if str(video_id).strip()}

    subtitle_candidates_by_video: dict[tuple[str, str], dict[str, Any]] = {}
    if safe_source_track in {"subtitle", "auto"}:
        subtitle_rows = connection.execute(
            """
            SELECT
                s.source_id,
                s.video_id,
                COALESCE(s.language, '') AS language,
                s.subtitle_path,
                LOWER(COALESCE(s.ext, '')) AS ext
            FROM subtitles s
            JOIN videos v
              ON v.source_id = s.source_id
             AND v.video_id = s.video_id
            WHERE v.has_media = 1
              AND s.subtitle_path IS NOT NULL
              AND s.subtitle_path <> ''
              AND LOWER(COALESCE(s.ext, '')) IN ('srt', 'vtt')
            ORDER BY s.source_id ASC, s.video_id ASC, s.subtitle_path ASC
            """
        ).fetchall()
        for row in subtitle_rows:
            source_id = str(row["source_id"])
            if source_id not in source_ids:
                continue
            video_id = str(row["video_id"])
            if video_filter_set and video_id not in video_filter_set:
                continue
            language = str(row["language"] or "").strip().lower()
            rank = language_rank_for_translation_source(language, safe_target)
            if rank >= 1000:
                continue
            subtitle_path = Path(str(row["subtitle_path"]))
            if not subtitle_path.exists() or not subtitle_path.is_file():
                continue
            target_key = (source_id, video_id)
            existing = subtitle_candidates_by_video.get(target_key)
            candidate = {
                "source_id": source_id,
                "video_id": video_id,
                "source_lang": infer_translation_source_lang(language, "en"),
                "subtitle_path": subtitle_path,
                "ext": str(row["ext"] or "").strip().lower(),
                "rank": rank,
                "source_track_kind": "subtitle",
            }
            if existing is None:
                subtitle_candidates_by_video[target_key] = candidate
                continue
            if int(candidate["rank"]) < int(existing["rank"]):
                subtitle_candidates_by_video[target_key] = candidate
                continue
            if (
                int(candidate["rank"]) == int(existing["rank"])
                and str(candidate["subtitle_path"]) < str(existing["subtitle_path"])
            ):
                subtitle_candidates_by_video[target_key] = candidate

    asr_candidates_by_video: dict[tuple[str, str], dict[str, Any]] = {}
    if safe_source_track in {"asr", "auto"}:
        asr_rows = connection.execute(
            """
            SELECT
                a.source_id,
                a.video_id,
                a.output_path
            FROM asr_runs a
            JOIN videos v
              ON v.source_id = a.source_id
             AND v.video_id = a.video_id
            WHERE v.has_media = 1
              AND a.status = 'success'
              AND a.output_path IS NOT NULL
              AND a.output_path <> ''
            ORDER BY a.source_id ASC, a.video_id ASC, a.updated_at DESC
            """
        ).fetchall()
        for row in asr_rows:
            source_id = str(row["source_id"])
            if source_id not in source_ids:
                continue
            video_id = str(row["video_id"])
            if video_filter_set and video_id not in video_filter_set:
                continue
            target_key = (source_id, video_id)
            if target_key in asr_candidates_by_video:
                continue
            subtitle_path = Path(str(row["output_path"]))
            if not subtitle_path.exists() or not subtitle_path.is_file():
                continue
            ext = subtitle_path.suffix.lstrip(".").lower()
            if ext not in {"srt", "vtt"}:
                continue
            asr_candidates_by_video[target_key] = {
                "source_id": source_id,
                "video_id": video_id,
                "source_lang": "en",
                "subtitle_path": subtitle_path,
                "ext": ext,
                "rank": 0,
                "source_track_kind": "asr",
            }

    best_by_video: dict[tuple[str, str], dict[str, Any]] = {}
    if safe_source_track == "subtitle":
        best_by_video = subtitle_candidates_by_video
    elif safe_source_track == "asr":
        best_by_video = asr_candidates_by_video
    else:
        all_keys = sorted(set(subtitle_candidates_by_video) | set(asr_candidates_by_video))
        for key in all_keys:
            subtitle_candidate = subtitle_candidates_by_video.get(key)
            asr_candidate = asr_candidates_by_video.get(key)
            if subtitle_candidate is None and asr_candidate is None:
                continue
            if subtitle_candidate is None:
                best_by_video[key] = asr_candidate
                continue
            if asr_candidate is None:
                best_by_video[key] = subtitle_candidate
                continue
            subtitle_sort_key = (0, int(subtitle_candidate["rank"]), str(subtitle_candidate["subtitle_path"]))
            asr_sort_key = (1, int(asr_candidate["rank"]), str(asr_candidate["subtitle_path"]))
            best_by_video[key] = subtitle_candidate if subtitle_sort_key <= asr_sort_key else asr_candidate

    selected: list[dict[str, Any]] = []
    for key in sorted(best_by_video):
        candidate = best_by_video[key]
        source_id = str(candidate["source_id"])
        video_id = str(candidate["video_id"])
        subtitle_path = Path(str(candidate["subtitle_path"]))
        output_path = build_ja_subtitle_output_path(subtitle_path, safe_target)

        if output_path.exists() and output_path.is_file() and not overwrite:
            continue
        if not include_translated:
            active_row = connection.execute(
                """
                SELECT 1
                FROM translation_runs
                WHERE source_id = ?
                  AND video_id = ?
                  AND target_lang = ?
                  AND status = 'active'
                LIMIT 1
                """,
                (source_id, video_id, safe_target),
            ).fetchone()
            if active_row is not None:
                continue

        candidate["output_path"] = output_path
        selected.append(candidate)
        if limit > 0 and len(selected) >= limit:
            break
    return selected


def run_translation_stage_draft(
    document: ParsedSubtitleDocument,
    endpoint: str,
    model: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    timeout_sec: int,
    api_key: str | None,
) -> tuple[dict[int, str], TranslationStageMetrics]:
    cue_count = len(document.cues)
    metrics = TranslationStageMetrics(
        stage_name="draft",
        model=str(model),
        input_cue_count=cue_count,
        started_at=now_utc_iso(),
    )
    translations: dict[int, str] = {}
    plain_texts = [strip_subtitle_markup(" ".join(cue.text_lines)) for cue in document.cues]

    for index, cue in enumerate(document.cues):
        current_text = plain_texts[index]
        if not current_text:
            translations[cue.cue_id] = ""
            continue
        prev_text = plain_texts[index - 1] if index > 0 else ""
        next_text = plain_texts[index + 1] if index + 1 < cue_count else ""
        payload = {
            "task": "translate subtitle cue en->ja",
            "cue_id": cue.cue_id,
            "prev_en": prev_text,
            "en": current_text,
            "next_en": next_text,
            "constraints": [
                "keep original meaning",
                "conversational Japanese for subtitles",
                "no explanations",
                "avoid overlong wording",
            ],
            "output_schema": {"ja": "string", "confidence": "high|medium|low"},
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a subtitle translator. "
                    "Return only JSON without markdown."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            },
        ]
        result = call_local_chat_completion(
            endpoint=endpoint,
            model=model,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            timeout_sec=timeout_sec,
            api_key=api_key,
        )
        metrics.request_count += 1
        metrics.elapsed_ms += int(result["elapsed_ms"])
        metrics.prompt_tokens += int(result["prompt_tokens"])
        metrics.completion_tokens += int(result["completion_tokens"])
        metrics.total_tokens += int(result["total_tokens"])

        parsed = parse_json_loose(str(result["content"]))
        translated_text = ""
        if isinstance(parsed, dict):
            translated_text = str(parsed.get("ja") or "").strip()
        elif isinstance(parsed, str):
            translated_text = parsed.strip()
        if not translated_text:
            translated_text = extract_translation_text_fallback(str(result["content"]))
        if is_translation_placeholder(translated_text):
            translated_text = ""
        if not translated_text:
            translated_text = current_text
        translations[cue.cue_id] = translated_text
        if translated_text != current_text:
            metrics.changed_cue_count += 1

    metrics.finished_at = now_utc_iso()
    return translations, metrics


def run_translation_stage_refine_chunks(
    document: ParsedSubtitleDocument,
    current_translations: dict[int, str],
    endpoint: str,
    model: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    chunk_size: int,
    timeout_sec: int,
    api_key: str | None,
) -> TranslationStageMetrics:
    metrics = TranslationStageMetrics(
        stage_name="refine",
        model=str(model),
        input_cue_count=len(document.cues),
        started_at=now_utc_iso(),
    )
    if not document.cues:
        metrics.finished_at = now_utc_iso()
        return metrics

    for chunk in chunk_items(document.cues, chunk_size=max(1, int(chunk_size))):
        chunk_payload: list[dict[str, Any]] = []
        for cue in chunk:
            en_text = strip_subtitle_markup(" ".join(cue.text_lines))
            chunk_payload.append(
                {
                    "cue_id": cue.cue_id,
                    "en": en_text,
                    "ja": str(current_translations.get(cue.cue_id) or ""),
                }
            )
        messages = [
            {
                "role": "system",
                "content": (
                    "Refine Japanese subtitle drafts with local context. "
                    "Return only JSON as {\"cues\":[{\"cue_id\":1,\"ja\":\"...\"}]}."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "refine subtitle wording",
                        "constraints": [
                            "preserve cue boundaries",
                            "keep meaning aligned with EN",
                            "avoid notes and annotations",
                        ],
                        "cues": chunk_payload,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
        ]
        try:
            result = call_local_chat_completion(
                endpoint=endpoint,
                model=model,
                messages=messages,
                temperature=temperature,
                top_p=top_p,
                max_tokens=max_tokens,
                timeout_sec=timeout_sec,
                api_key=api_key,
            )
        except RuntimeError as exc:
            metrics.status = "failed"
            metrics.error_message = str(exc)
            break

        metrics.request_count += 1
        metrics.elapsed_ms += int(result["elapsed_ms"])
        metrics.prompt_tokens += int(result["prompt_tokens"])
        metrics.completion_tokens += int(result["completion_tokens"])
        metrics.total_tokens += int(result["total_tokens"])
        patch_map = extract_patch_map_from_llm_output(str(result["content"]))
        allowed_ids = {cue.cue_id for cue in chunk}
        metrics.changed_cue_count += apply_patch_map_to_translations(
            current_translations=current_translations,
            patch_map=patch_map,
            allowed_cue_ids=allowed_ids,
        )

    metrics.finished_at = now_utc_iso()
    return metrics


def run_translation_stage_global(
    document: ParsedSubtitleDocument,
    current_translations: dict[int, str],
    endpoint: str,
    model: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    timeout_sec: int,
    global_max_cues: int,
    api_key: str | None,
) -> TranslationStageMetrics:
    metrics = TranslationStageMetrics(
        stage_name="global",
        model=str(model),
        input_cue_count=len(document.cues),
        started_at=now_utc_iso(),
    )
    cue_count = len(document.cues)
    if cue_count == 0:
        metrics.finished_at = now_utc_iso()
        return metrics
    if cue_count > max(1, int(global_max_cues)):
        metrics.status = "skipped"
        metrics.error_message = (
            f"cue_count={cue_count} exceeds global_max_cues={max(1, int(global_max_cues))}"
        )
        metrics.finished_at = now_utc_iso()
        return metrics

    payload_cues: list[dict[str, Any]] = []
    for cue in document.cues:
        payload_cues.append(
            {
                "cue_id": cue.cue_id,
                "en": strip_subtitle_markup(" ".join(cue.text_lines)),
                "ja": str(current_translations.get(cue.cue_id) or ""),
            }
        )

    messages = [
        {
            "role": "system",
            "content": (
                "Perform final consistency pass for Japanese subtitle cues. "
                "Return JSON only as {\"cues\":[{\"cue_id\":1,\"ja\":\"...\"}]}."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "task": "global consistency pass",
                    "constraints": [
                        "keep each cue independent",
                        "harmonize wording and tone across the file",
                        "keep proper nouns consistent",
                    ],
                    "cues": payload_cues,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        },
    ]
    try:
        result = call_local_chat_completion(
            endpoint=endpoint,
            model=model,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            timeout_sec=timeout_sec,
            api_key=api_key,
        )
    except RuntimeError as exc:
        metrics.status = "failed"
        metrics.error_message = str(exc)
        metrics.finished_at = now_utc_iso()
        return metrics

    metrics.request_count = 1
    metrics.elapsed_ms = int(result["elapsed_ms"])
    metrics.prompt_tokens = int(result["prompt_tokens"])
    metrics.completion_tokens = int(result["completion_tokens"])
    metrics.total_tokens = int(result["total_tokens"])
    patch_map = extract_patch_map_from_llm_output(str(result["content"]))
    allowed_ids = {cue.cue_id for cue in document.cues}
    metrics.changed_cue_count = apply_patch_map_to_translations(
        current_translations=current_translations,
        patch_map=patch_map,
        allowed_cue_ids=allowed_ids,
    )
    metrics.finished_at = now_utc_iso()
    return metrics


def build_source_text_by_cue_id(document: ParsedSubtitleDocument) -> dict[int, str]:
    source_text_by_cue_id: dict[int, str] = {}
    for cue in document.cues:
        source_text_by_cue_id[int(cue.cue_id)] = strip_subtitle_markup(" ".join(cue.text_lines)).strip()
    return source_text_by_cue_id


def extract_audit_issue_map_from_llm_output(raw_text: str) -> dict[int, str]:
    parsed = parse_json_loose(raw_text)
    rows: list[Any] = []
    if isinstance(parsed, list):
        rows = parsed
    elif isinstance(parsed, dict):
        cues_value = parsed.get("cues")
        if isinstance(cues_value, list):
            rows = cues_value
        elif isinstance(parsed.get("items"), list):
            rows = parsed.get("items") or []
        else:
            cue_id = parsed.get("cue_id")
            issue = parsed.get("issue")
            if cue_id not in (None, "") and issue not in (None, ""):
                rows = [{"cue_id": cue_id, "issue": issue}]

    issue_map: dict[int, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        cue_id_raw = row.get("cue_id")
        if cue_id_raw in (None, ""):
            continue
        try:
            cue_id = int(cue_id_raw)
        except (TypeError, ValueError):
            continue
        issue = str(row.get("issue") or "").strip().lower()
        needs_fix_raw = row.get("needs_fix")
        needs_fix = False
        if isinstance(needs_fix_raw, bool):
            needs_fix = needs_fix_raw
        elif isinstance(needs_fix_raw, (int, float)):
            needs_fix = bool(int(needs_fix_raw))
        elif isinstance(needs_fix_raw, str):
            needs_fix = needs_fix_raw.strip().lower() in {"1", "true", "yes", "y"}

        if needs_fix or issue:
            issue_map[cue_id] = issue or "check"
    return issue_map


def run_translation_stage_quality_audit(
    document: ParsedSubtitleDocument,
    current_translations: dict[int, str],
    source_text_by_cue_id: dict[int, str],
    cue_ids: list[int],
    endpoint: str,
    model: str,
    max_tokens: int,
    chunk_size: int,
    timeout_sec: int,
    api_key: str | None,
) -> tuple[dict[int, str], TranslationStageMetrics]:
    metrics = TranslationStageMetrics(
        stage_name="quality-audit",
        model=str(model),
        input_cue_count=max(0, int(len(cue_ids))),
        started_at=now_utc_iso(),
    )
    if not cue_ids:
        metrics.finished_at = now_utc_iso()
        return {}, metrics

    cue_lookup = {int(cue.cue_id): cue for cue in document.cues}
    flagged_ids = sorted({int(cue_id) for cue_id in cue_ids if int(cue_id) in cue_lookup})
    if not flagged_ids:
        metrics.finished_at = now_utc_iso()
        return {}, metrics

    issue_map: dict[int, str] = {}
    for chunk in chunk_items(flagged_ids, chunk_size=max(1, int(chunk_size))):
        payload_cues: list[dict[str, Any]] = []
        for cue_id in chunk:
            cue = cue_lookup.get(cue_id)
            if cue is None:
                continue
            current_text = str(current_translations.get(cue_id) or "").strip()
            source_text = str(source_text_by_cue_id.get(cue_id) or "").strip()
            suspected_issues: list[str] = []
            if is_json_fragment_text(current_text):
                suspected_issues.append("json_fragment")
            if is_english_heavy_text(current_text):
                suspected_issues.append("english_heavy")
            if source_text and current_text == source_text:
                suspected_issues.append("unchanged")
            payload_cues.append(
                {
                    "cue_id": cue_id,
                    "en": source_text,
                    "ja": current_text,
                    "suspected_issues": suspected_issues,
                }
            )
        if not payload_cues:
            continue

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a subtitle translation quality auditor. "
                    "Return JSON only as {\"cues\":[{\"cue_id\":1,\"issue\":\"json_fragment|english_heavy|unchanged|other\","
                    "\"needs_fix\":true}]}. "
                    "Do not include any markdown."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "audit cue quality and decide repair necessity",
                        "target_lang": "ja",
                        "cues": payload_cues,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
        ]
        try:
            result = call_local_chat_completion(
                endpoint=endpoint,
                model=model,
                messages=messages,
                temperature=0.0,
                top_p=1.0,
                max_tokens=max(1, int(max_tokens)),
                timeout_sec=timeout_sec,
                api_key=api_key,
            )
        except RuntimeError as exc:
            metrics.status = "failed"
            metrics.error_message = str(exc)
            break

        metrics.request_count += 1
        metrics.elapsed_ms += int(result["elapsed_ms"])
        metrics.prompt_tokens += int(result["prompt_tokens"])
        metrics.completion_tokens += int(result["completion_tokens"])
        metrics.total_tokens += int(result["total_tokens"])

        chunk_issues = extract_audit_issue_map_from_llm_output(str(result["content"]))
        for cue_id, issue in chunk_issues.items():
            if cue_id in flagged_ids:
                issue_map[cue_id] = issue or "check"

    metrics.changed_cue_count = max(0, int(len(issue_map)))
    metrics.finished_at = now_utc_iso()
    return issue_map, metrics


def run_translation_stage_quality_repair(
    document: ParsedSubtitleDocument,
    current_translations: dict[int, str],
    source_text_by_cue_id: dict[int, str],
    cue_ids: list[int],
    endpoint: str,
    model: str,
    max_tokens: int,
    chunk_size: int,
    timeout_sec: int,
    api_key: str | None,
) -> TranslationStageMetrics:
    metrics = TranslationStageMetrics(
        stage_name="quality-repair",
        model=str(model),
        input_cue_count=max(0, int(len(cue_ids))),
        started_at=now_utc_iso(),
    )
    if not cue_ids:
        metrics.finished_at = now_utc_iso()
        return metrics

    cue_lookup = {int(cue.cue_id): cue for cue in document.cues}
    repair_ids = sorted({int(cue_id) for cue_id in cue_ids if int(cue_id) in cue_lookup})
    if not repair_ids:
        metrics.finished_at = now_utc_iso()
        return metrics

    for chunk in chunk_items(repair_ids, chunk_size=max(1, int(chunk_size))):
        payload_cues: list[dict[str, Any]] = []
        for cue_id in chunk:
            cue = cue_lookup.get(cue_id)
            if cue is None:
                continue
            payload_cues.append(
                {
                    "cue_id": cue_id,
                    "en": str(source_text_by_cue_id.get(cue_id) or ""),
                    "ja": str(current_translations.get(cue_id) or "").strip(),
                }
            )
        if not payload_cues:
            continue

        messages = [
            {
                "role": "system",
                "content": (
                    "You repair Japanese subtitle cues. "
                    "Return JSON only as {\"cues\":[{\"cue_id\":1,\"ja\":\"...\"}]}. "
                    "Keep cue boundaries and avoid explanations."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": "repair problematic subtitle cues",
                        "constraints": [
                            "preserve meaning",
                            "natural conversational Japanese",
                            "no JSON fragments in ja text",
                            "no English leftovers unless proper noun",
                        ],
                        "cues": payload_cues,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
        ]
        try:
            result = call_local_chat_completion(
                endpoint=endpoint,
                model=model,
                messages=messages,
                temperature=0.0,
                top_p=1.0,
                max_tokens=max(1, int(max_tokens)),
                timeout_sec=timeout_sec,
                api_key=api_key,
            )
        except RuntimeError as exc:
            metrics.status = "failed"
            metrics.error_message = str(exc)
            break

        metrics.request_count += 1
        metrics.elapsed_ms += int(result["elapsed_ms"])
        metrics.prompt_tokens += int(result["prompt_tokens"])
        metrics.completion_tokens += int(result["completion_tokens"])
        metrics.total_tokens += int(result["total_tokens"])

        patch_map = extract_patch_map_from_llm_output(str(result["content"]))
        metrics.changed_cue_count += apply_patch_map_to_translations(
            current_translations=current_translations,
            patch_map=patch_map,
            allowed_cue_ids=set(chunk),
        )

    metrics.finished_at = now_utc_iso()
    return metrics


def validate_subtitle_timing_match(source_path: Path, output_path: Path) -> tuple[int, bool]:
    source_cues = parse_subtitle_cues(source_path)
    output_cues = parse_subtitle_cues(output_path)
    if len(source_cues) != len(output_cues):
        return (len(source_cues), False)
    for source_cue, output_cue in zip(source_cues, output_cues):
        if int(source_cue.get("start_ms") or 0) != int(output_cue.get("start_ms") or 0):
            return (len(source_cues), False)
        if int(source_cue.get("end_ms") or 0) != int(output_cue.get("end_ms") or 0):
            return (len(source_cues), False)
    return (len(source_cues), True)


def run_translate_local(
    db_path: Path,
    source_ids: list[str],
    endpoint: str,
    api_key: str | None,
    source_lang: str,
    target_lang: str,
    draft_model: str,
    refine_model: str,
    global_model: str,
    draft_max_tokens: int,
    refine_max_tokens: int,
    global_max_tokens: int,
    temperature: float,
    top_p: float,
    chunk_size: int,
    global_max_cues: int,
    timeout_sec: int,
    limit: int,
    source_track: str,
    include_translated: bool,
    overwrite: bool,
    dry_run: bool,
    agent: str,
    method: str,
    method_version: str,
    quality_enforce: bool,
    quality_loop_max_rounds: int,
    quality_json_fragment_threshold: float,
    quality_english_heavy_threshold: float,
    quality_unchanged_threshold: float,
    quality_audit_model: str,
    quality_repair_model: str,
    quality_audit_max_tokens: int,
    quality_repair_max_tokens: int,
    video_ids: list[str] | None = None,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path), timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        create_schema(connection)
        connection.commit()

        targets = collect_local_translation_targets(
            connection=connection,
            source_ids=source_ids,
            target_lang=target_lang,
            source_track=source_track,
            video_ids=video_ids,
            limit=max(0, int(limit)),
            include_translated=bool(include_translated),
            overwrite=bool(overwrite),
        )
        if not targets:
            print("[translate-local] no targets found")
            return

        print(
            "[translate-local] targets="
            f"{len(targets)} source_lang={source_lang} target_lang={target_lang} "
            f"source_track={normalize_translation_source_track(source_track, 'subtitle')}"
        )

        success_count = 0
        failed_count = 0
        skipped_count = 0

        for target in targets:
            source_id = str(target["source_id"])
            video_id = str(target["video_id"])
            source_path = Path(str(target["subtitle_path"]))
            output_path = Path(str(target["output_path"]))
            source_track_kind = normalize_translation_source_track(
                str(target.get("source_track_kind") or source_track),
                "subtitle",
            )
            run_source_lang = str(target.get("source_lang") or source_lang or "en").strip().lower() or "en"
            run_method_version = append_source_track_to_method_version(method_version, source_track_kind)
            started_at = now_utc_iso()
            stage_metrics: list[TranslationStageMetrics] = []

            print(
                f"[translate-local] start {source_id}/{video_id} "
                f"input={source_path.name} source_track={source_track_kind}"
            )
            try:
                document = parse_subtitle_document(source_path)
                cue_count = len(document.cues)
                if cue_count == 0:
                    print(f"[translate-local] skip {source_id}/{video_id}: no cue blocks")
                    skipped_count += 1
                    continue

                translations, draft_metrics = run_translation_stage_draft(
                    document=document,
                    endpoint=endpoint,
                    model=draft_model,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max(1, int(draft_max_tokens)),
                    timeout_sec=timeout_sec,
                    api_key=api_key,
                )
                stage_metrics.append(draft_metrics)

                refine_metrics = run_translation_stage_refine_chunks(
                    document=document,
                    current_translations=translations,
                    endpoint=endpoint,
                    model=refine_model,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max(1, int(refine_max_tokens)),
                    chunk_size=max(1, int(chunk_size)),
                    timeout_sec=timeout_sec,
                    api_key=api_key,
                )
                stage_metrics.append(refine_metrics)

                global_metrics = run_translation_stage_global(
                    document=document,
                    current_translations=translations,
                    endpoint=endpoint,
                    model=global_model,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max(1, int(global_max_tokens)),
                    timeout_sec=timeout_sec,
                    global_max_cues=max(1, int(global_max_cues)),
                    api_key=api_key,
                )
                stage_metrics.append(global_metrics)

                source_text_by_cue_id = build_source_text_by_cue_id(document)
                quality_report = evaluate_translation_quality(
                    document=document,
                    translations=translations,
                    source_text_by_cue_id=source_text_by_cue_id,
                )
                quality_passed = quality_report_passes_thresholds(
                    report=quality_report,
                    json_fragment_threshold=quality_json_fragment_threshold,
                    english_heavy_threshold=quality_english_heavy_threshold,
                    unchanged_threshold=quality_unchanged_threshold,
                )
                stage_metrics.append(
                    build_quality_gate_stage_metrics(
                        stage_name="quality-gate-initial",
                        report=quality_report,
                        passed=quality_passed,
                    )
                )
                print(
                    f"[translate-local] quality {source_id}/{video_id} "
                    f"{format_quality_report_summary(quality_report)}"
                )

                max_quality_rounds = max(0, int(quality_loop_max_rounds))
                if not quality_passed and max_quality_rounds > 0:
                    for round_index in range(max_quality_rounds):
                        bad_cue_ids = list(quality_report.bad_cue_ids)
                        if not bad_cue_ids:
                            break

                        audit_issue_map, audit_metrics = run_translation_stage_quality_audit(
                            document=document,
                            current_translations=translations,
                            source_text_by_cue_id=source_text_by_cue_id,
                            cue_ids=bad_cue_ids,
                            endpoint=endpoint,
                            model=quality_audit_model,
                            max_tokens=max(1, int(quality_audit_max_tokens)),
                            chunk_size=max(1, int(chunk_size)),
                            timeout_sec=timeout_sec,
                            api_key=api_key,
                        )
                        stage_metrics.append(audit_metrics)

                        repair_target_ids = sorted(
                            set(audit_issue_map.keys()) if audit_issue_map else set(bad_cue_ids)
                        )
                        repair_metrics = run_translation_stage_quality_repair(
                            document=document,
                            current_translations=translations,
                            source_text_by_cue_id=source_text_by_cue_id,
                            cue_ids=repair_target_ids,
                            endpoint=endpoint,
                            model=quality_repair_model,
                            max_tokens=max(1, int(quality_repair_max_tokens)),
                            chunk_size=max(1, int(chunk_size)),
                            timeout_sec=timeout_sec,
                            api_key=api_key,
                        )
                        stage_metrics.append(repair_metrics)

                        quality_report = evaluate_translation_quality(
                            document=document,
                            translations=translations,
                            source_text_by_cue_id=source_text_by_cue_id,
                        )
                        quality_passed = quality_report_passes_thresholds(
                            report=quality_report,
                            json_fragment_threshold=quality_json_fragment_threshold,
                            english_heavy_threshold=quality_english_heavy_threshold,
                            unchanged_threshold=quality_unchanged_threshold,
                        )
                        stage_metrics.append(
                            build_quality_gate_stage_metrics(
                                stage_name=f"quality-gate-r{round_index + 1}",
                                report=quality_report,
                                passed=quality_passed,
                            )
                        )
                        print(
                            f"[translate-local] quality-round {round_index + 1} {source_id}/{video_id} "
                            f"{format_quality_report_summary(quality_report)}"
                        )
                        if quality_passed:
                            break

                if bool(quality_enforce) and not quality_passed:
                    raise RuntimeError(
                        "quality gate failed: "
                        f"{format_quality_report_summary(quality_report)} "
                        f"(thresholds: json<={quality_json_fragment_threshold:.3f}, "
                        f"english_heavy<={quality_english_heavy_threshold:.3f}, "
                        f"unchanged<={quality_unchanged_threshold:.3f})"
                    )

                rendered_text = render_subtitle_document(document, translations)
                if not rendered_text.strip():
                    raise RuntimeError("rendered subtitle is empty")

                cue_match = True
                if dry_run:
                    cue_count = len(document.cues)
                else:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(rendered_text, encoding="utf-8", newline="\n")
                    cue_count, cue_match = validate_subtitle_timing_match(
                        source_path=source_path,
                        output_path=output_path,
                    )

                finished_at = now_utc_iso()
                summary = build_local_translation_summary(
                    source_id=source_id,
                    video_id=video_id,
                    source_track_kind=source_track_kind,
                    cue_count=cue_count,
                    translations=translations,
                )
                summary = f"{summary} | quality={format_quality_report_summary(quality_report)}"

                if dry_run:
                    success_count += 1
                    print(
                        f"[translate-local] dry-run ok {source_id}/{video_id} "
                        f"cues={cue_count} output={output_path.name}"
                    )
                    continue

                status = "active" if cue_match else "failed"
                run_id = record_translation_run(
                    connection=connection,
                    source_id=source_id,
                    video_id=video_id,
                    source_path=source_path,
                    output_path=output_path,
                    cue_count=cue_count,
                    cue_match=cue_match,
                    agent=agent,
                    method=method,
                    method_version=run_method_version,
                    summary=summary,
                    source_lang=run_source_lang,
                    target_lang=str(target_lang or "ja"),
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                )
                for metric in stage_metrics:
                    record_translation_stage_metrics(
                        connection=connection,
                        translation_run_id=run_id,
                        source_id=source_id,
                        video_id=video_id,
                        stage_metrics=metric,
                    )
                connection.commit()

                if cue_match:
                    success_count += 1
                    print(
                        f"[translate-local] ok {source_id}/{video_id} "
                        f"cues={cue_count} output={output_path}"
                    )
                else:
                    failed_count += 1
                    print(
                        f"[translate-local] NG timing mismatch {source_id}/{video_id} "
                        f"cues={cue_count} output={output_path}"
                    )
            except Exception as exc:
                failed_count += 1
                finished_at = now_utc_iso()
                error_summary = (
                    "local-llm multi-stage translation failed "
                    f"({source_id}/{video_id}, source_track={source_track_kind}): {exc}"
                )
                if not dry_run:
                    run_id = record_translation_run(
                        connection=connection,
                        source_id=source_id,
                        video_id=video_id,
                        source_path=source_path,
                        output_path=output_path,
                        cue_count=0,
                        cue_match=False,
                        agent=agent,
                        method=method,
                        method_version=run_method_version,
                        summary=error_summary,
                        source_lang=run_source_lang,
                        target_lang=str(target_lang or "ja"),
                        status="failed",
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                    for metric in stage_metrics:
                        record_translation_stage_metrics(
                            connection=connection,
                            translation_run_id=run_id,
                            source_id=source_id,
                            video_id=video_id,
                            stage_metrics=metric,
                        )
                    connection.commit()
                print(f"[translate-local] failed {source_id}/{video_id}: {exc}", file=sys.stderr)

        print(
            "[translate-local] done "
            f"success={success_count} skipped={skipped_count} failed={failed_count}"
        )


def run_translate_local_for_video(
    connection: sqlite3.Connection,
    db_path: Path,
    source: SourceConfig,
    video_id: str,
    target_lang: str = "ja-local",
    source_track: str = "auto",
    timeout_sec: int = 60,
    dry_run: bool = False,
) -> tuple[bool, str | None]:
    safe_target_lang = str(target_lang or "ja-local").strip() or "ja-local"
    safe_source_track = normalize_translation_source_track(source_track, "auto")
    safe_timeout_sec = max(10, int(timeout_sec))

    targets = collect_local_translation_targets(
        connection=connection,
        source_ids=[source.id],
        target_lang=safe_target_lang,
        source_track=safe_source_track,
        video_ids=[video_id],
        limit=1,
        include_translated=False,
        overwrite=False,
    )
    if not targets:
        return (
            False,
            f"{source.id}/{video_id}: translate target not ready "
            f"(source_track={safe_source_track}, target_lang={safe_target_lang})",
        )

    endpoint = os.environ.get("SUBSTUDY_LOCAL_LLM_ENDPOINT", DEFAULT_LOCAL_LLM_ENDPOINT)
    api_key = os.environ.get("SUBSTUDY_LOCAL_LLM_API_KEY", "")
    run_translate_local(
        db_path=db_path,
        source_ids=[source.id],
        endpoint=endpoint,
        api_key=api_key,
        source_lang="en",
        target_lang=safe_target_lang,
        draft_model=DEFAULT_LOCAL_TRANSLATE_DRAFT_MODEL,
        refine_model=DEFAULT_LOCAL_TRANSLATE_REFINE_MODEL,
        global_model=DEFAULT_LOCAL_TRANSLATE_GLOBAL_MODEL,
        draft_max_tokens=160,
        refine_max_tokens=480,
        global_max_tokens=1200,
        temperature=0.1,
        top_p=0.9,
        chunk_size=12,
        global_max_cues=240,
        timeout_sec=safe_timeout_sec,
        limit=1,
        source_track=safe_source_track,
        include_translated=False,
        overwrite=False,
        dry_run=bool(dry_run),
        agent="local-llm",
        method="multi-stage",
        method_version="20b-draft+120b-refine+120b-global-v1",
        quality_enforce=False,
        quality_loop_max_rounds=DEFAULT_LOCAL_TRANSLATE_QUALITY_LOOP_MAX_ROUNDS,
        quality_json_fragment_threshold=DEFAULT_LOCAL_TRANSLATE_QUALITY_JSON_FRAGMENT_THRESHOLD,
        quality_english_heavy_threshold=DEFAULT_LOCAL_TRANSLATE_QUALITY_ENGLISH_HEAVY_THRESHOLD,
        quality_unchanged_threshold=DEFAULT_LOCAL_TRANSLATE_QUALITY_UNCHANGED_THRESHOLD,
        quality_audit_model=DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MODEL,
        quality_repair_model=DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MODEL,
        quality_audit_max_tokens=DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MAX_TOKENS,
        quality_repair_max_tokens=DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MAX_TOKENS,
        video_ids=[video_id],
    )

    row = connection.execute(
        """
        SELECT status, COALESCE(summary, '')
        FROM translation_runs
        WHERE source_id = ?
          AND video_id = ?
          AND target_lang = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (source.id, video_id, safe_target_lang),
    ).fetchone()
    if row is None:
        return (False, f"{source.id}/{video_id}: no translation_runs row after translate-local")

    status = str(row[0] or "").strip().lower()
    summary = str(row[1] or "").strip()
    if status == "active":
        return (True, None)
    return (
        False,
        summary or f"{source.id}/{video_id}: translate-local finished with status={status or 'unknown'}",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Substudy sync and ledger tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Download updates and refresh ledger (incremental by default)",
    )
    sync_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    sync_parser.add_argument("--source", action="append", dest="sources")
    sync_parser.add_argument(
        "--source-order",
        choices=["config", "random"],
        default=None,
        help="Source processing order (default: config global.source_order or random).",
    )
    sync_parser.add_argument(
        "--execution-mode",
        choices=["legacy", "queue"],
        default="legacy",
        help="Execution mode: legacy direct processing or queue producer mode.",
    )
    sync_parser.add_argument(
        "--no-producer-lock",
        action="store_true",
        help="Disable producer lock in queue mode (unsafe; advanced use only).",
    )
    sync_parser.add_argument("--dry-run", action="store_true")
    sync_parser.add_argument("--skip-media", action="store_true")
    sync_parser.add_argument("--skip-subs", action="store_true")
    sync_parser.add_argument("--skip-meta", action="store_true")
    sync_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cap subtitle/meta targets processed in this sync run (0 = unbounded).",
    )
    sync_parser.add_argument(
        "--upstream-sub-langs-override",
        help=(
            "Override subtitle download to fetch only matching upstream subtitle tracks "
            "for this run. Disables subtitle archive gating for the run."
        ),
    )
    sync_parser.add_argument(
        "--network-profile",
        choices=["normal", "weak", "auto"],
        default="normal",
        help=(
            "Media download behavior by network condition: "
            "normal=download media, weak=skip media, auto=probe and decide."
        ),
    )
    sync_parser.add_argument(
        "--network-probe-url",
        default=DEFAULT_NETWORK_PROBE_URL,
        help=f"Probe URL for --network-profile auto (default: {DEFAULT_NETWORK_PROBE_URL})",
    )
    sync_parser.add_argument(
        "--network-probe-timeout-sec",
        type=int,
        default=DEFAULT_NETWORK_PROBE_TIMEOUT_SEC,
        help=f"Probe timeout seconds for auto network profile (default: {DEFAULT_NETWORK_PROBE_TIMEOUT_SEC})",
    )
    sync_parser.add_argument(
        "--network-probe-bytes",
        type=int,
        default=DEFAULT_NETWORK_PROBE_BYTES,
        help=f"Probe byte size for throughput estimation (default: {DEFAULT_NETWORK_PROBE_BYTES})",
    )
    sync_parser.add_argument(
        "--weak-net-min-kbps",
        type=float,
        default=DEFAULT_WEAK_NET_MIN_KBPS,
        help=f"Auto profile threshold: below this kbps is weak (default: {DEFAULT_WEAK_NET_MIN_KBPS})",
    )
    sync_parser.add_argument(
        "--weak-net-max-rtt-ms",
        type=float,
        default=DEFAULT_WEAK_NET_MAX_RTT_MS,
        help=f"Auto profile threshold: above this RTT is weak (default: {DEFAULT_WEAK_NET_MAX_RTT_MS})",
    )
    sync_parser.add_argument(
        "--metered-media-mode",
        choices=["off", "updates-only"],
        default=DEFAULT_METERED_MEDIA_MODE,
        help=(
            "Media policy for metered links: "
            "off=normal behavior, updates-only=download only recent updates for already-seeded sources."
        ),
    )
    sync_parser.add_argument(
        "--metered-min-archive-ids",
        type=int,
        default=DEFAULT_METERED_MIN_ARCHIVE_IDS,
        help=(
            "Minimum media archive IDs required to treat a source as seeded in updates-only mode "
            f"(default: {DEFAULT_METERED_MIN_ARCHIVE_IDS})."
        ),
    )
    sync_parser.add_argument(
        "--metered-playlist-end",
        type=int,
        default=DEFAULT_METERED_PLAYLIST_END,
        help=(
            "Max playlist_end used in updates-only mode to cap discovery scan range "
            f"(default: {DEFAULT_METERED_PLAYLIST_END})."
        ),
    )
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
    backfill_parser.add_argument(
        "--source-order",
        choices=["config", "random"],
        default=None,
        help="Source processing order (default: config global.source_order or random).",
    )
    backfill_parser.add_argument(
        "--execution-mode",
        choices=["legacy", "queue"],
        default="legacy",
        help="Execution mode: legacy direct processing or queue producer mode.",
    )
    backfill_parser.add_argument(
        "--no-producer-lock",
        action="store_true",
        help="Disable producer lock in queue mode (unsafe; advanced use only).",
    )
    backfill_parser.add_argument("--dry-run", action="store_true")
    backfill_parser.add_argument("--skip-media", action="store_true")
    backfill_parser.add_argument("--skip-subs", action="store_true")
    backfill_parser.add_argument("--skip-meta", action="store_true")
    backfill_parser.add_argument(
        "--upstream-sub-langs-override",
        help=(
            "Override subtitle download to fetch only matching upstream subtitle tracks "
            "for this run. Disables subtitle archive gating for the run."
        ),
    )
    backfill_parser.add_argument(
        "--network-profile",
        choices=["normal", "weak", "auto"],
        default="normal",
        help=(
            "Media download behavior by network condition: "
            "normal=download media, weak=skip media, auto=probe and decide."
        ),
    )
    backfill_parser.add_argument(
        "--network-probe-url",
        default=DEFAULT_NETWORK_PROBE_URL,
        help=f"Probe URL for --network-profile auto (default: {DEFAULT_NETWORK_PROBE_URL})",
    )
    backfill_parser.add_argument(
        "--network-probe-timeout-sec",
        type=int,
        default=DEFAULT_NETWORK_PROBE_TIMEOUT_SEC,
        help=f"Probe timeout seconds for auto network profile (default: {DEFAULT_NETWORK_PROBE_TIMEOUT_SEC})",
    )
    backfill_parser.add_argument(
        "--network-probe-bytes",
        type=int,
        default=DEFAULT_NETWORK_PROBE_BYTES,
        help=f"Probe byte size for throughput estimation (default: {DEFAULT_NETWORK_PROBE_BYTES})",
    )
    backfill_parser.add_argument(
        "--weak-net-min-kbps",
        type=float,
        default=DEFAULT_WEAK_NET_MIN_KBPS,
        help=f"Auto profile threshold: below this kbps is weak (default: {DEFAULT_WEAK_NET_MIN_KBPS})",
    )
    backfill_parser.add_argument(
        "--weak-net-max-rtt-ms",
        type=float,
        default=DEFAULT_WEAK_NET_MAX_RTT_MS,
        help=f"Auto profile threshold: above this RTT is weak (default: {DEFAULT_WEAK_NET_MAX_RTT_MS})",
    )
    backfill_parser.add_argument(
        "--metered-media-mode",
        choices=["off", "updates-only"],
        default=DEFAULT_METERED_MEDIA_MODE,
        help=(
            "Media policy for metered links. "
            "updates-only disables historical backfill windows."
        ),
    )
    backfill_parser.add_argument(
        "--metered-min-archive-ids",
        type=int,
        default=DEFAULT_METERED_MIN_ARCHIVE_IDS,
        help=(
            "Reserved for sync compatibility in updates-only mode "
            f"(default: {DEFAULT_METERED_MIN_ARCHIVE_IDS})."
        ),
    )
    backfill_parser.add_argument(
        "--metered-playlist-end",
        type=int,
        default=DEFAULT_METERED_PLAYLIST_END,
        help=(
            "Reserved for sync compatibility in updates-only mode "
            f"(default: {DEFAULT_METERED_PLAYLIST_END})."
        ),
    )
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

    queue_worker_parser = subparsers.add_parser(
        "queue-worker",
        help="Lease and process work_items with per-video stage workers",
    )
    queue_worker_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    queue_worker_parser.add_argument("--source", action="append", dest="sources")
    queue_worker_parser.add_argument(
        "--stage",
        action="append",
        dest="stages",
        choices=["media", "subs", "meta", "asr", "loudness", "translate"],
        help="Stage filter (repeatable). Defaults to media+subs+meta+asr+loudness+translate.",
    )
    queue_worker_parser.add_argument(
        "--worker-id",
        help="Optional stable worker identity (default: host-pid-random).",
    )
    queue_worker_parser.add_argument(
        "--lease-sec",
        type=int,
        default=DEFAULT_QUEUE_LEASE_SEC,
        help=f"Lease duration seconds (default: {DEFAULT_QUEUE_LEASE_SEC}).",
    )
    queue_worker_parser.add_argument(
        "--poll-sec",
        type=float,
        default=DEFAULT_QUEUE_POLL_SEC,
        help=f"Idle polling interval seconds (default: {DEFAULT_QUEUE_POLL_SEC}).",
    )
    queue_worker_parser.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="Stop after this many leased items (0 = unlimited).",
    )
    queue_worker_parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_QUEUE_MAX_ATTEMPTS,
        help=f"Mark item dead after this many failed attempts (default: {DEFAULT_QUEUE_MAX_ATTEMPTS}).",
    )
    queue_worker_parser.add_argument(
        "--no-enqueue-downstream",
        action="store_true",
        help="Disable downstream work item enqueue on success (media->subs/meta/asr/loudness, subs/asr->translate).",
    )
    queue_worker_parser.add_argument(
        "--translate-target-lang",
        default="ja-local",
        help="Target language label for translate stage runs (default: ja-local).",
    )
    queue_worker_parser.add_argument(
        "--translate-source-track",
        choices=["subtitle", "asr", "auto"],
        default="auto",
        help="Source track preference for translate stage (default: auto).",
    )
    queue_worker_parser.add_argument(
        "--translate-timeout-sec",
        type=int,
        default=60,
        help="HTTP timeout per translate request (default: 60).",
    )
    queue_worker_parser.add_argument(
        "--once",
        action="store_true",
        help="Try to process at most one currently due item, then exit.",
    )
    queue_worker_parser.add_argument("--dry-run", action="store_true")
    queue_worker_parser.add_argument("--ledger-db", type=Path)

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

    queue_status_parser = subparsers.add_parser(
        "queue-status",
        help="Show unresolved queue work items and recent queue failures",
    )
    queue_status_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    queue_status_parser.add_argument("--source", action="append", dest="sources")
    queue_status_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max recent failed items per source.",
    )
    queue_status_parser.add_argument(
        "--only-unresolved",
        action="store_true",
        help="Show only sources that currently have unresolved queue items.",
    )
    queue_status_parser.add_argument("--ledger-db", type=Path)

    queue_requeue_parser = subparsers.add_parser(
        "queue-requeue",
        help="Requeue selected queue work_items (default: status error/dead)",
    )
    queue_requeue_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    queue_requeue_parser.add_argument("--source", action="append", dest="sources")
    queue_requeue_parser.add_argument(
        "--stage",
        action="append",
        dest="stages",
        choices=["media", "subs", "meta", "asr", "loudness", "translate"],
        help="Stage filter (repeatable). Defaults to all stages.",
    )
    queue_requeue_parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        choices=["queued", "leased", "error", "dead", "success"],
        help="Current status filter (repeatable). Defaults to error+dead.",
    )
    queue_requeue_parser.add_argument(
        "--error-contains",
        help="Optional substring filter against last_error.",
    )
    queue_requeue_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max matched items per source (0 = no limit).",
    )
    queue_requeue_parser.add_argument(
        "--reset-attempts",
        action="store_true",
        help="Reset attempt_count to 0 on requeue.",
    )
    queue_requeue_parser.add_argument("--dry-run", action="store_true")
    queue_requeue_parser.add_argument("--ledger-db", type=Path)

    queue_recover_known_parser = subparsers.add_parser(
        "queue-recover-known",
        help="Requeue known recoverable queue failures with predefined filters",
    )
    queue_recover_known_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    queue_recover_known_parser.add_argument("--source", action="append", dest="sources")
    queue_recover_known_parser.add_argument(
        "--profile",
        action="append",
        dest="profiles",
        choices=["all", *sorted(QUEUE_RECOVERY_PROFILES.keys())],
        help="Recovery profile (repeatable). Defaults to all known profiles.",
    )
    queue_recover_known_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max matched items per source for each profile (0 = no limit).",
    )
    queue_recover_known_parser.add_argument(
        "--reset-attempts",
        action="store_true",
        help="Reset attempt_count to 0 on requeue.",
    )
    queue_recover_known_parser.add_argument("--dry-run", action="store_true")
    queue_recover_known_parser.add_argument("--ledger-db", type=Path)

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

    dict_bookmarks_export_parser = subparsers.add_parser(
        "dict-bookmarks-export",
        help="Export dictionary bookmarks for LLM/review workflows",
    )
    dict_bookmarks_export_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    dict_bookmarks_export_parser.add_argument("--source", action="append", dest="sources")
    dict_bookmarks_export_parser.add_argument("--ledger-db", type=Path)
    dict_bookmarks_export_parser.add_argument(
        "--format",
        choices=["jsonl", "csv"],
        default="jsonl",
        help="Export format (default: jsonl)",
    )
    dict_bookmarks_export_parser.add_argument(
        "--entry-status",
        choices=["all", "missing", "known"],
        default="all",
        help="Filter by dictionary entry status (default: all)",
    )
    dict_bookmarks_export_parser.add_argument(
        "--video-id",
        action="append",
        dest="video_ids",
        help="Optional video_id filter (repeatable).",
    )
    dict_bookmarks_export_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row cap (0 = no limit).",
    )
    dict_bookmarks_export_parser.add_argument(
        "--output",
        type=Path,
        help="Output file path. Defaults to exports/dictionary_bookmarks_<status>_<utc>.<format>",
    )

    dict_bookmarks_import_parser = subparsers.add_parser(
        "dict-bookmarks-import",
        help="Import dictionary bookmarks from JSONL/CSV with duplicate policy control",
    )
    dict_bookmarks_import_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    dict_bookmarks_import_parser.add_argument("--source", action="append", dest="sources")
    dict_bookmarks_import_parser.add_argument("--ledger-db", type=Path)
    dict_bookmarks_import_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input file path (JSONL/CSV).",
    )
    dict_bookmarks_import_parser.add_argument(
        "--format",
        choices=["jsonl", "csv"],
        default="jsonl",
        help="Input format (default: jsonl)",
    )
    dict_bookmarks_import_parser.add_argument(
        "--on-duplicate",
        choices=["skip", "upsert", "error"],
        default="upsert",
        help="Duplicate composite-key behavior (default: upsert)",
    )
    dict_bookmarks_import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report without writing DB changes.",
    )

    dict_bookmarks_curate_parser = subparsers.add_parser(
        "dict-bookmarks-curate",
        help="Materialize curated dictionary bookmark views for review/study",
    )
    dict_bookmarks_curate_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    dict_bookmarks_curate_parser.add_argument("--source", action="append", dest="sources")
    dict_bookmarks_curate_parser.add_argument("--ledger-db", type=Path)
    dict_bookmarks_curate_parser.add_argument(
        "--preset",
        choices=["missing_review", "frequent_terms", "recent_saved", "review_cards"],
        required=True,
        help="Curated view preset to materialize.",
    )
    dict_bookmarks_curate_parser.add_argument(
        "--format",
        choices=["jsonl", "csv"],
        default="jsonl",
        help="Output format (default: jsonl)",
    )
    dict_bookmarks_curate_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Row cap for output (default: 200)",
    )
    dict_bookmarks_curate_parser.add_argument(
        "--min-bookmarks",
        type=int,
        default=2,
        help="Minimum bookmark count for frequent_terms (default: 2)",
    )
    dict_bookmarks_curate_parser.add_argument(
        "--min-videos",
        type=int,
        default=1,
        help="Minimum distinct videos for frequent_terms (default: 1)",
    )
    dict_bookmarks_curate_parser.add_argument(
        "--output",
        type=Path,
        help="Output file path. Defaults to exports/dictionary_bookmarks_<preset>_<utc>.<format>",
    )

    notify_parser = subparsers.add_parser(
        "notify",
        help="Send local study notifications (review / LLM-updated unread)",
    )
    notify_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    notify_parser.add_argument("--source", action="append", dest="sources")
    notify_parser.add_argument("--ledger-db", type=Path)
    notify_parser.add_argument(
        "--kind",
        choices=["review", "llm", "all"],
        default="all",
        help="Notification kind (default: all)",
    )
    notify_parser.add_argument(
        "--web-url-base",
        default=DEFAULT_NOTIFY_WEB_URL_BASE,
        help="Base URL opened when notification is clicked (default: http://127.0.0.1:8876)",
    )
    notify_parser.add_argument(
        "--llm-lookback-hours",
        type=int,
        default=DEFAULT_NOTIFY_LLM_LOOKBACK_HOURS,
        help="Initial lookback for LLM update detection when no state exists (default: 24)",
    )
    notify_parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=DEFAULT_NOTIFY_COOLDOWN_MINUTES,
        help=f"Suppress duplicate notifications within this window (default: {DEFAULT_NOTIFY_COOLDOWN_MINUTES})",
    )
    notify_parser.add_argument("--dry-run", action="store_true")

    notify_install_parser = subparsers.add_parser(
        "notify-install-macos",
        help="Install periodic notification scheduler via macOS launchd",
    )
    notify_install_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    notify_install_parser.add_argument("--source", action="append", dest="sources")
    notify_install_parser.add_argument("--ledger-db", type=Path)
    notify_install_parser.add_argument(
        "--label",
        default=DEFAULT_NOTIFY_MACOS_LABEL,
        help=f"LaunchAgent label (default: {DEFAULT_NOTIFY_MACOS_LABEL})",
    )
    notify_install_parser.add_argument(
        "--interval-minutes",
        type=int,
        default=DEFAULT_NOTIFY_INTERVAL_MINUTES,
        help=f"Notification interval minutes (default: {DEFAULT_NOTIFY_INTERVAL_MINUTES})",
    )
    notify_install_parser.add_argument(
        "--kind",
        choices=["review", "llm", "all"],
        default="all",
        help="Notification kind for scheduled runs (default: all)",
    )
    notify_install_parser.add_argument(
        "--web-url-base",
        default=DEFAULT_NOTIFY_WEB_URL_BASE,
        help="Base URL opened when notification is clicked",
    )
    notify_install_parser.add_argument(
        "--llm-lookback-hours",
        type=int,
        default=DEFAULT_NOTIFY_LLM_LOOKBACK_HOURS,
        help="Initial lookback for LLM update detection when state is empty",
    )
    notify_install_parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=DEFAULT_NOTIFY_COOLDOWN_MINUTES,
        help=f"Suppress duplicate notifications within this window (default: {DEFAULT_NOTIFY_COOLDOWN_MINUTES})",
    )
    notify_install_parser.add_argument(
        "--python-bin",
        default=sys.executable,
        help="Python executable for LaunchAgent ProgramArguments",
    )
    notify_install_parser.add_argument(
        "--script-path",
        type=Path,
        default=Path(__file__).resolve(),
        help="Path to substudy.py used by LaunchAgent",
    )
    notify_install_parser.add_argument(
        "--plist-path",
        type=Path,
        help="Optional custom LaunchAgent plist path",
    )
    notify_install_parser.add_argument(
        "--no-load",
        action="store_true",
        help="Write plist but do not load/bootstrap immediately.",
    )

    notify_uninstall_parser = subparsers.add_parser(
        "notify-uninstall-macos",
        help="Uninstall periodic notification LaunchAgent on macOS",
    )
    notify_uninstall_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    notify_uninstall_parser.add_argument("--source", action="append", dest="sources")
    notify_uninstall_parser.add_argument(
        "--label",
        default=DEFAULT_NOTIFY_MACOS_LABEL,
        help=f"LaunchAgent label (default: {DEFAULT_NOTIFY_MACOS_LABEL})",
    )
    notify_uninstall_parser.add_argument(
        "--plist-path",
        type=Path,
        help="Optional custom LaunchAgent plist path",
    )

    translate_local_parser = subparsers.add_parser(
        "translate-local",
        help="Translate subtitles with local multi-stage LLM pipeline (20b draft + 120b refinements)",
    )
    translate_local_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    translate_local_parser.add_argument("--source", action="append", dest="sources")
    translate_local_parser.add_argument("--ledger-db", type=Path)
    translate_local_parser.add_argument(
        "--video-id",
        action="append",
        dest="video_ids",
        help="Optional video_id filter (repeatable).",
    )
    translate_local_parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Max subtitle files to process in this run (default: 1).",
    )
    translate_local_parser.add_argument(
        "--include-translated",
        action="store_true",
        help="Include files that already have active translation_runs rows.",
    )
    translate_local_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing target subtitle files.",
    )
    translate_local_parser.add_argument(
        "--source-lang",
        default="en",
        help="Source language label stored in translation_runs (default: en).",
    )
    translate_local_parser.add_argument(
        "--target-lang",
        default="ja",
        help="Target language label stored in translation_runs (default: ja).",
    )
    translate_local_parser.add_argument(
        "--source-track",
        choices=["subtitle", "asr", "auto"],
        default="subtitle",
        help=(
            "Translation input track source: "
            "subtitle=subtitle table only, asr=asr_runs only, auto=prefer subtitle then ASR."
        ),
    )
    translate_local_parser.add_argument(
        "--endpoint",
        default=os.environ.get("SUBSTUDY_LOCAL_LLM_ENDPOINT", DEFAULT_LOCAL_LLM_ENDPOINT),
        help=f"OpenAI-compatible endpoint (default: {DEFAULT_LOCAL_LLM_ENDPOINT})",
    )
    translate_local_parser.add_argument(
        "--api-key",
        default=os.environ.get("SUBSTUDY_LOCAL_LLM_API_KEY", ""),
        help="Optional API key for endpoint auth.",
    )
    translate_local_parser.add_argument(
        "--draft-model",
        default=DEFAULT_LOCAL_TRANSLATE_DRAFT_MODEL,
        help=f"Stage1 cue-level model (default: {DEFAULT_LOCAL_TRANSLATE_DRAFT_MODEL})",
    )
    translate_local_parser.add_argument(
        "--refine-model",
        default=DEFAULT_LOCAL_TRANSLATE_REFINE_MODEL,
        help=f"Stage2 chunk-level model (default: {DEFAULT_LOCAL_TRANSLATE_REFINE_MODEL})",
    )
    translate_local_parser.add_argument(
        "--global-model",
        default=DEFAULT_LOCAL_TRANSLATE_GLOBAL_MODEL,
        help=f"Stage3 global-pass model (default: {DEFAULT_LOCAL_TRANSLATE_GLOBAL_MODEL})",
    )
    translate_local_parser.add_argument(
        "--draft-max-tokens",
        type=int,
        default=160,
        help="Stage1 max_tokens (default: 160).",
    )
    translate_local_parser.add_argument(
        "--refine-max-tokens",
        type=int,
        default=480,
        help="Stage2 max_tokens (default: 480).",
    )
    translate_local_parser.add_argument(
        "--global-max-tokens",
        type=int,
        default=1200,
        help="Stage3 max_tokens (default: 1200).",
    )
    translate_local_parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Sampling temperature (default: 0.1).",
    )
    translate_local_parser.add_argument(
        "--top-p",
        type=float,
        default=0.9,
        help="Nucleus sampling top_p (default: 0.9).",
    )
    translate_local_parser.add_argument(
        "--chunk-size",
        type=int,
        default=12,
        help="Cues per refine request (default: 12).",
    )
    translate_local_parser.add_argument(
        "--global-max-cues",
        type=int,
        default=240,
        help="Skip stage3 when cue count exceeds this (default: 240).",
    )
    translate_local_parser.add_argument(
        "--timeout-sec",
        type=int,
        default=60,
        help="HTTP timeout per request (default: 60).",
    )
    translate_local_parser.add_argument(
        "--agent",
        default="local-llm",
        help="translation_runs.agent value (default: local-llm).",
    )
    translate_local_parser.add_argument(
        "--method",
        default="multi-stage",
        help="translation_runs.method value (default: multi-stage).",
    )
    translate_local_parser.add_argument(
        "--method-version",
        default="20b-draft+120b-refine+120b-global-v1",
        help="translation_runs.method_version value.",
    )
    translate_local_parser.add_argument(
        "--quality-enforce",
        action="store_true",
        help="Fail this run when quality thresholds are not met after quality loop.",
    )
    translate_local_parser.add_argument(
        "--quality-loop-max-rounds",
        type=int,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_LOOP_MAX_ROUNDS,
        help=(
            "Max rounds for audit->repair->re-audit loop "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_LOOP_MAX_ROUNDS}, 0 disables)."
        ),
    )
    translate_local_parser.add_argument(
        "--quality-json-fragment-threshold",
        type=float,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_JSON_FRAGMENT_THRESHOLD,
        help=(
            "Allowed json_fragment_rate after quality loop "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_JSON_FRAGMENT_THRESHOLD})."
        ),
    )
    translate_local_parser.add_argument(
        "--quality-english-heavy-threshold",
        type=float,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_ENGLISH_HEAVY_THRESHOLD,
        help=(
            "Allowed english_heavy_rate after quality loop "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_ENGLISH_HEAVY_THRESHOLD})."
        ),
    )
    translate_local_parser.add_argument(
        "--quality-unchanged-threshold",
        type=float,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_UNCHANGED_THRESHOLD,
        help=(
            "Allowed unchanged_rate after quality loop "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_UNCHANGED_THRESHOLD})."
        ),
    )
    translate_local_parser.add_argument(
        "--quality-audit-model",
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MODEL,
        help=f"Quality audit model (default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MODEL})",
    )
    translate_local_parser.add_argument(
        "--quality-repair-model",
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MODEL,
        help=f"Quality repair model (default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MODEL})",
    )
    translate_local_parser.add_argument(
        "--quality-audit-max-tokens",
        type=int,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MAX_TOKENS,
        help=(
            "Quality audit max_tokens "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_AUDIT_MAX_TOKENS})."
        ),
    )
    translate_local_parser.add_argument(
        "--quality-repair-max-tokens",
        type=int,
        default=DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MAX_TOKENS,
        help=(
            "Quality repair max_tokens "
            f"(default: {DEFAULT_LOCAL_TRANSLATE_QUALITY_REPAIR_MAX_TOKENS})."
        ),
    )
    translate_local_parser.add_argument("--dry-run", action="store_true")

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

    if args.command == "notify-uninstall-macos":
        try:
            run_notify_uninstall_macos(
                label=str(args.label),
                plist_path=args.plist_path,
            )
            return 0
        except (RuntimeError, OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    try:
        global_config, all_sources = load_config(args.config)
        sources = select_sources(all_sources, args.sources)
        sources = apply_upstream_sub_langs_override(
            sources,
            getattr(args, "upstream_sub_langs_override", None),
        )
    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    ledger_db_path = resolve_output_path(getattr(args, "ledger_db", None), global_config.ledger_db)
    ledger_csv_path = resolve_output_path(getattr(args, "ledger_csv", None), global_config.ledger_csv)

    if args.command == "sync":
        source_order_mode = normalize_source_order_mode(
            getattr(args, "source_order", None) or global_config.source_order,
            fallback="random",
        )
        run_sources = order_sources_for_run(
            sources=sources,
            mode=source_order_mode,
            command_name="sync",
        )
        effective_skip_media, _network_decision = resolve_skip_media_with_network_profile(
            command_name="sync",
            explicit_skip_media=bool(args.skip_media),
            network_profile=str(args.network_profile or "normal"),
            network_probe_url=str(args.network_probe_url or DEFAULT_NETWORK_PROBE_URL),
            network_probe_timeout_sec=max(2, int(args.network_probe_timeout_sec)),
            network_probe_bytes=max(1024, int(args.network_probe_bytes)),
            weak_net_min_kbps=max(50.0, float(args.weak_net_min_kbps)),
            weak_net_max_rtt_ms=max(50.0, float(args.weak_net_max_rtt_ms)),
        )
        metered_media_mode = normalize_metered_media_mode(
            getattr(args, "metered_media_mode", DEFAULT_METERED_MEDIA_MODE),
            DEFAULT_METERED_MEDIA_MODE,
        )
        metered_min_archive_ids = max(0, int(getattr(args, "metered_min_archive_ids", DEFAULT_METERED_MIN_ARCHIVE_IDS)))
        metered_playlist_end = max(1, int(getattr(args, "metered_playlist_end", DEFAULT_METERED_PLAYLIST_END)))
        execution_mode = str(getattr(args, "execution_mode", "legacy") or "legacy").strip().lower()
        if execution_mode == "queue":
            try:
                lock_enabled = not bool(getattr(args, "no_producer_lock", False)) and not bool(
                    args.dry_run
                )
                if bool(getattr(args, "no_producer_lock", False)) and not bool(args.dry_run):
                    print("[producer-lock] disabled by --no-producer-lock")
                with queue_producer_lock(
                    get_queue_producer_lock_path(ledger_db_path),
                    enabled=lock_enabled,
                ):
                    if args.dry_run:
                        queue_db_path = ":memory:"
                    else:
                        ledger_db_path.parent.mkdir(parents=True, exist_ok=True)
                        queue_db_path = str(ledger_db_path)

                    queue_connection = sqlite3.connect(queue_db_path, timeout=30)
                    if queue_db_path != ":memory:":
                        queue_connection.execute("PRAGMA journal_mode=WAL")
                    create_schema(queue_connection)
                    try:
                        if bool(args.skip_media):
                            print("[sync-queue] media discovery skipped by explicit --skip-media")
                        else:
                            source_priority_stride = max(1, len(run_sources))
                            for source_index, source in enumerate(run_sources):
                                source_playlist_end = source.playlist_end
                                if metered_media_mode == "updates-only":
                                    media_archive_count = len(read_archive_ids(source.media_archive))
                                    (
                                        skip_by_metered,
                                        _force_break_on_existing,
                                        source_playlist_end,
                                        metered_reason,
                                    ) = resolve_metered_media_policy(
                                        source_id=source.id,
                                        mode=metered_media_mode,
                                        media_archive_count=media_archive_count,
                                        configured_playlist_end=source.playlist_end,
                                        min_archive_ids=metered_min_archive_ids,
                                        metered_playlist_end=metered_playlist_end,
                                    )
                                    print(f"[sync-queue] {metered_reason}")
                                    if skip_by_metered:
                                        continue
                                try:
                                    enqueue_source_media_discovery(
                                        connection=queue_connection,
                                        source=source,
                                        dry_run=bool(args.dry_run),
                                        run_label="sync-queue",
                                        playlist_start=1,
                                        playlist_end=source_playlist_end,
                                        enforce_poll_interval=True,
                                        source_slot=source_index,
                                        source_stride=source_priority_stride,
                                    )
                                except Exception as exc:
                                    print(
                                        f"[sync-queue] {source.id}: discovery failed ({exc})",
                                        file=sys.stderr,
                                    )
                    finally:
                        queue_connection.close()

                if not args.skip_ledger and not args.dry_run:
                    print("[sync-queue] skip ledger rebuild (no media/subs/meta files were downloaded)")
                elif args.dry_run and not args.skip_ledger:
                    print("dry-run: skip ledger rebuild")
                return 0
            except ProducerLockAcquisitionError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2

        sync_connection: sqlite3.Connection | None = None
        if not args.dry_run:
            ledger_db_path.parent.mkdir(parents=True, exist_ok=True)
            sync_connection = sqlite3.connect(str(ledger_db_path), timeout=30)
            sync_connection.execute("PRAGMA journal_mode=WAL")
            create_schema(sync_connection)
        try:
            sync_stage_limit = normalize_optional_stage_limit(getattr(args, "limit", None))
            run_legacy_sync_sources(
                sources=run_sources,
                dry_run=bool(args.dry_run),
                skip_media=bool(effective_skip_media),
                skip_subs=bool(args.skip_subs),
                skip_meta=bool(args.skip_meta),
                connection=sync_connection,
                metered_media_mode=metered_media_mode,
                metered_min_archive_ids=metered_min_archive_ids,
                metered_playlist_end=metered_playlist_end,
                limit=sync_stage_limit,
            )
        finally:
            if sync_connection is not None:
                sync_connection.close()

        if not args.skip_ledger and not args.dry_run:
            build_ledger(
                run_sources,
                ledger_db_path,
                ledger_csv_path,
                incremental=not args.full_ledger,
            )
        elif args.dry_run and not args.skip_ledger:
            print("dry-run: skip ledger rebuild")
        return 0

    if args.command == "backfill":
        source_order_mode = normalize_source_order_mode(
            getattr(args, "source_order", None) or global_config.source_order,
            fallback="random",
        )
        run_sources = order_sources_for_run(
            sources=sources,
            mode=source_order_mode,
            command_name="backfill",
        )
        effective_skip_media, _network_decision = resolve_skip_media_with_network_profile(
            command_name="backfill",
            explicit_skip_media=bool(args.skip_media),
            network_profile=str(args.network_profile or "normal"),
            network_probe_url=str(args.network_probe_url or DEFAULT_NETWORK_PROBE_URL),
            network_probe_timeout_sec=max(2, int(args.network_probe_timeout_sec)),
            network_probe_bytes=max(1024, int(args.network_probe_bytes)),
            weak_net_min_kbps=max(50.0, float(args.weak_net_min_kbps)),
            weak_net_max_rtt_ms=max(50.0, float(args.weak_net_max_rtt_ms)),
        )
        metered_media_mode = normalize_metered_media_mode(
            getattr(args, "metered_media_mode", DEFAULT_METERED_MEDIA_MODE),
            DEFAULT_METERED_MEDIA_MODE,
        )
        metered_min_archive_ids = max(0, int(getattr(args, "metered_min_archive_ids", DEFAULT_METERED_MIN_ARCHIVE_IDS)))
        metered_playlist_end = max(1, int(getattr(args, "metered_playlist_end", DEFAULT_METERED_PLAYLIST_END)))
        execution_mode = str(getattr(args, "execution_mode", "legacy") or "legacy").strip().lower()
        try:
            lock_enabled = (
                execution_mode == "queue"
                and not bool(getattr(args, "no_producer_lock", False))
                and not bool(args.dry_run)
            )
            if execution_mode == "queue" and bool(getattr(args, "no_producer_lock", False)) and not bool(args.dry_run):
                print("[producer-lock] disabled by --no-producer-lock")
            with queue_producer_lock(
                get_queue_producer_lock_path(ledger_db_path),
                enabled=lock_enabled,
            ):
                run_backfill(
                    sources=run_sources,
                    db_path=ledger_db_path,
                    csv_path=ledger_csv_path,
                    dry_run=args.dry_run,
                    skip_media=effective_skip_media,
                    skip_subs=args.skip_subs,
                    skip_meta=args.skip_meta,
                    skip_ledger=args.skip_ledger,
                    full_ledger=args.full_ledger,
                    windows_override=args.windows,
                    reset=args.reset,
                    execution_mode=execution_mode,
                    metered_media_mode=metered_media_mode,
                    metered_min_archive_ids=metered_min_archive_ids,
                    metered_playlist_end=metered_playlist_end,
                )
            return 0
        except ProducerLockAcquisitionError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    if args.command == "queue-worker":
        run_queue_worker(
            sources=sources,
            db_path=ledger_db_path,
            stages=getattr(args, "stages", None),
            worker_id=getattr(args, "worker_id", None),
            lease_seconds=max(30, int(args.lease_sec)),
            poll_interval_sec=max(0.2, float(args.poll_sec)),
            max_items=max(0, int(args.max_items)),
            once=bool(args.once),
            dry_run=bool(args.dry_run),
            max_attempts=max(1, int(args.max_attempts)),
            enqueue_downstream=not bool(getattr(args, "no_enqueue_downstream", False)),
            translate_target_lang=str(getattr(args, "translate_target_lang", "ja-local") or "ja-local"),
            translate_source_track=str(getattr(args, "translate_source_track", "auto") or "auto"),
            translate_timeout_sec=max(10, int(getattr(args, "translate_timeout_sec", 60))),
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

    if args.command == "queue-status":
        show_queue_status_report(
            sources=sources,
            db_path=ledger_db_path,
            limit=max(1, args.limit),
            only_unresolved=bool(getattr(args, "only_unresolved", False)),
        )
        return 0

    if args.command == "queue-requeue":
        requeue_work_items(
            sources=sources,
            db_path=ledger_db_path,
            stages=getattr(args, "stages", None),
            statuses=getattr(args, "statuses", None),
            error_contains=str(getattr(args, "error_contains", "") or "").strip() or None,
            limit=max(0, int(args.limit)),
            dry_run=bool(args.dry_run),
            reset_attempts=bool(getattr(args, "reset_attempts", False)),
        )
        return 0

    if args.command == "queue-recover-known":
        run_queue_recover_known(
            sources=sources,
            db_path=ledger_db_path,
            profiles=getattr(args, "profiles", None),
            limit=max(0, int(args.limit)),
            dry_run=bool(args.dry_run),
            reset_attempts=bool(getattr(args, "reset_attempts", False)),
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

    if args.command == "dict-bookmarks-export":
        export_format = str(args.format).strip().lower()
        entry_status = str(args.entry_status).strip().lower()
        timestamp_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        default_output = DEFAULT_DICT_BOOKMARK_EXPORT_DIR / (
            f"dictionary_bookmarks_{entry_status}_{timestamp_utc}.{export_format}"
        )
        output_path = resolve_output_path(args.output, default_output)
        run_dict_bookmarks_export(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            output_path=output_path,
            output_format=export_format,
            entry_status=entry_status,
            limit=max(0, int(args.limit)),
            video_ids=args.video_ids,
        )
        return 0

    if args.command == "dict-bookmarks-import":
        input_path = resolve_output_path(args.input, args.input)
        run_dict_bookmarks_import(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            input_path=input_path,
            input_format=str(args.format).strip().lower(),
            on_duplicate=str(args.on_duplicate).strip().lower(),
            dry_run=bool(args.dry_run),
        )
        return 0

    if args.command == "dict-bookmarks-curate":
        preset = str(args.preset).strip().lower()
        output_format = str(args.format).strip().lower()
        timestamp_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        default_output = DEFAULT_DICT_BOOKMARK_EXPORT_DIR / (
            f"dictionary_bookmarks_{preset}_{timestamp_utc}.{output_format}"
        )
        output_path = resolve_output_path(args.output, default_output)
        run_dict_bookmarks_curate(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            preset=preset,
            output_path=output_path,
            output_format=output_format,
            limit=max(1, int(args.limit)),
            min_bookmarks=max(1, int(args.min_bookmarks)),
            min_videos=max(1, int(args.min_videos)),
        )
        return 0

    if args.command == "notify":
        run_notify(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            kind=str(args.kind).strip().lower(),
            web_url_base=str(args.web_url_base),
            llm_lookback_hours=max(1, int(args.llm_lookback_hours)),
            dry_run=bool(args.dry_run),
        )
        return 0

    if args.command == "notify-install-macos":
        try:
            run_notify_install_macos(
                label=str(args.label),
                interval_minutes=max(1, int(args.interval_minutes)),
                python_bin=str(args.python_bin),
                script_path=resolve_output_path(args.script_path, Path(__file__).resolve()),
                config_path=resolve_output_path(args.config, DEFAULT_CONFIG),
                ledger_db_path=resolve_output_path(getattr(args, "ledger_db", None), ledger_db_path),
                source_ids=[source.id for source in sources],
                kind=str(args.kind).strip().lower(),
                web_url_base=str(args.web_url_base),
                llm_lookback_hours=max(1, int(args.llm_lookback_hours)),
                plist_path=args.plist_path,
                load_now=not bool(args.no_load),
            )
            return 0
        except (RuntimeError, OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    if args.command == "translate-local":
        run_translate_local(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            endpoint=str(args.endpoint),
            api_key=str(args.api_key or "").strip() or None,
            source_lang=str(args.source_lang or "en").strip().lower(),
            target_lang=str(args.target_lang or "ja").strip().lower(),
            draft_model=str(args.draft_model),
            refine_model=str(args.refine_model),
            global_model=str(args.global_model),
            draft_max_tokens=max(1, int(args.draft_max_tokens)),
            refine_max_tokens=max(1, int(args.refine_max_tokens)),
            global_max_tokens=max(1, int(args.global_max_tokens)),
            temperature=float(args.temperature),
            top_p=float(args.top_p),
            chunk_size=max(1, int(args.chunk_size)),
            global_max_cues=max(1, int(args.global_max_cues)),
            timeout_sec=max(3, int(args.timeout_sec)),
            limit=max(0, int(args.limit)),
            source_track=normalize_translation_source_track(str(args.source_track), "subtitle"),
            include_translated=bool(args.include_translated),
            overwrite=bool(args.overwrite),
            dry_run=bool(args.dry_run),
            agent=str(args.agent),
            method=str(args.method),
            method_version=str(args.method_version),
            quality_enforce=bool(args.quality_enforce),
            quality_loop_max_rounds=max(0, int(args.quality_loop_max_rounds)),
            quality_json_fragment_threshold=max(0.0, float(args.quality_json_fragment_threshold)),
            quality_english_heavy_threshold=max(0.0, float(args.quality_english_heavy_threshold)),
            quality_unchanged_threshold=max(0.0, float(args.quality_unchanged_threshold)),
            quality_audit_model=str(args.quality_audit_model),
            quality_repair_model=str(args.quality_repair_model),
            quality_audit_max_tokens=max(1, int(args.quality_audit_max_tokens)),
            quality_repair_max_tokens=max(1, int(args.quality_repair_max_tokens)),
            video_ids=args.video_ids,
        )
        return 0

    if args.command == "web":
        run_web_ui(
            db_path=ledger_db_path,
            source_ids=[source.id for source in sources],
            config_path=resolve_output_path(args.config, DEFAULT_CONFIG),
            host=str(args.host),
            port=max(1, min(65535, int(args.port))),
            restrict_to_source_ids=bool(args.sources),
        )
        return 0

    print("error: unsupported command", file=sys.stderr)
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("[substudy] interrupted by user.", file=sys.stderr)
        raise SystemExit(130)
