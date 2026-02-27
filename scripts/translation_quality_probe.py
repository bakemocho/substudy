#!/usr/bin/env python3
"""Lightweight quality probe for local subtitle translations."""

from __future__ import annotations

import argparse
import importlib.util
import re
import sqlite3
import sys
from pathlib import Path


def load_substudy_module(repo_root: Path):
    module_path = repo_root / "scripts" / "substudy.py"
    spec = importlib.util.spec_from_file_location("substudy_quality_probe_module", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RE_ASCII = re.compile(r"[A-Za-z]")
RE_JA = re.compile(r"[ぁ-んァ-ヶ一-龯々ー]")


def is_json_fragment(text: str) -> bool:
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


def english_heavy(text: str) -> bool:
    value = str(text or "")
    letters = len(RE_ASCII.findall(value))
    ja_chars = len(RE_JA.findall(value))
    return letters >= max(10, ja_chars * 2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe translation quality metrics from subtitle tracks.")
    parser.add_argument("--ledger-db", type=Path, required=True)
    parser.add_argument("--source", required=True, help="source_id")
    parser.add_argument("--target-lang", default="ja-local", help="target subtitle language label")
    parser.add_argument(
        "--source-lang",
        default="NA.eng-US",
        help="source subtitle language label used for unchanged comparison",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Top videos by unchanged-cue ratio to print (default: 10)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    substudy = load_substudy_module(repo_root)

    if not args.ledger_db.exists():
        raise FileNotFoundError(f"ledger db not found: {args.ledger_db}")

    connection = sqlite3.connect(str(args.ledger_db))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                s.video_id,
                MAX(CASE
                    WHEN LOWER(COALESCE(s.language, '')) = LOWER(?)
                    THEN s.subtitle_path
                END) AS target_path,
                MAX(CASE
                    WHEN LOWER(COALESCE(s.language, '')) = LOWER(?)
                    THEN s.subtitle_path
                END) AS source_path
            FROM subtitles s
            WHERE s.source_id = ?
            GROUP BY s.video_id
            HAVING target_path IS NOT NULL
            ORDER BY s.video_id ASC
            """,
            (str(args.target_lang), str(args.source_lang), str(args.source)),
        ).fetchall()
    finally:
        connection.close()

    video_count = 0
    total_cues = 0
    timing_mismatch_cues = 0
    empty_cues = 0
    json_fragment_cues = 0
    english_heavy_cues = 0
    unchanged_cues = 0
    per_video: list[tuple[str, int, int, int]] = []

    for row in rows:
        video_id = str(row["video_id"])
        target_path = Path(str(row["target_path"]))
        source_path = Path(str(row["source_path"])) if row["source_path"] else None
        if not target_path.exists():
            continue
        target_cues = substudy.parse_subtitle_cues(target_path)
        source_cues = []
        if source_path is not None and source_path.exists():
            source_cues = substudy.parse_subtitle_cues(source_path)
        source_by_time = {
            (int(cue.get("start_ms") or 0), int(cue.get("end_ms") or 0)): str(cue.get("text") or "").strip()
            for cue in source_cues
        }

        video_count += 1
        video_total = 0
        video_unchanged = 0
        video_json = 0
        video_english_heavy = 0

        for cue in target_cues:
            video_total += 1
            total_cues += 1
            start_ms = int(cue.get("start_ms") or 0)
            end_ms = int(cue.get("end_ms") or 0)
            text = str(cue.get("text") or "").strip()
            if not text:
                empty_cues += 1
                continue
            if is_json_fragment(text):
                json_fragment_cues += 1
                video_json += 1
            if english_heavy(text):
                english_heavy_cues += 1
                video_english_heavy += 1
            source_text = source_by_time.get((start_ms, end_ms), "")
            if source_text:
                if source_text == text:
                    unchanged_cues += 1
                    video_unchanged += 1
            else:
                timing_mismatch_cues += 1

        per_video.append((video_id, video_total, video_unchanged, video_json + video_english_heavy))

    def rate(value: int) -> float:
        if total_cues <= 0:
            return 0.0
        return float(value) / float(total_cues)

    print(f"source={args.source}")
    print(f"target_lang={args.target_lang}")
    print(f"source_lang={args.source_lang}")
    print(f"videos={video_count}")
    print(f"total_cues={total_cues}")
    print(f"timing_mismatch_cues={timing_mismatch_cues}")
    print(f"empty_cues={empty_cues}")
    print(f"json_fragment_cues={json_fragment_cues}")
    print(f"english_heavy_cues={english_heavy_cues}")
    print(f"unchanged_cues={unchanged_cues}")
    print(f"json_fragment_rate={rate(json_fragment_cues):.4f}")
    print(f"english_heavy_rate={rate(english_heavy_cues):.4f}")
    print(f"unchanged_rate={rate(unchanged_cues):.4f}")
    print(f"empty_rate={rate(empty_cues):.4f}")

    if per_video:
        print("")
        print("top_videos_by_unchanged_ratio:")
        sorted_videos = sorted(
            per_video,
            key=lambda item: (item[2] / item[1]) if item[1] else 0.0,
            reverse=True,
        )
        for video_id, video_total, video_unchanged, video_issue in sorted_videos[: max(0, int(args.top))]:
            unchanged_ratio = (video_unchanged / video_total) if video_total else 0.0
            print(
                f"  {video_id} cues={video_total} unchanged={video_unchanged} "
                f"unchanged_ratio={unchanged_ratio:.3f} issue_score={video_issue}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
