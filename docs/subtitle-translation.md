# 字幕和訳 手順書

このドキュメントは、英語字幕ファイルの和訳を依頼するときの実運用手順と要件を定義する。

## 1. 目的

- 英語字幕を日本語字幕へ変換し、`substudy` の字幕トラックとして利用可能にする。
- タイムスタンプを維持したまま翻訳し、動画再生と字幕同期を崩さない。
- バッチ運用できる形で、再実行・差分実行が可能な手順にする。

## 2. 前提

- 入力字幕は `.srt` または `.vtt`（UTF-8想定）。
- `substudy` は字幕ファイル名から `video_id` と `language` を推定する。
- 追加した字幕をUIで使うには、翻訳後に `ledger` 更新が必要。

## 3. 出力要件

### MUST

- **タイムスタンプを変更しないこと**。
- **キュー数（字幕ブロック数）を変更しないこと**。
- 各キューで、翻訳対象は字幕テキスト行のみとし、番号行・時刻行は保持すること。
- 出力エンコーディングは UTF-8、改行は LF を使用すること。
- ファイル名は `video_id.ja.<ext>` 形式にすること。
  - 例: `7603444228575890709.ja.srt`

### SHOULD

- 日本語は自然だが意訳しすぎないこと（学習向けに原文対応が追える訳）。
- 固有名詞、数値、通貨、単位は原文情報を保持すること。
- 口語フィラー（um, uh, you know など）は必要に応じて「えっと」「その」などで軽く反映すること。
- 不明箇所は空欄にせず、最も妥当な訳を入れること。

### MUST NOT

- 訳注の追記（`[注]` など）を勝手に挿入しない。
- 字幕の順序を入れ替えない。
- 1つの字幕ファイルに別動画の内容を混在させない。

## 4. 推奨保存先と命名

- 推奨: 元字幕と同じ `subs_dir` に保存。
- 例: `/Users/.../storiesofcz_subs/7603444228575890709.ja.srt`
- 既存が `video_id.en.srt` の場合でも、和訳は `video_id.ja.srt` で統一。

## 5. 実行手順

1. 翻訳対象ファイルを列挙する。

```bash
cd /Users/bakemocho/gitwork_bk/substudy
mkdir -p logs
find /Users/bakemocho/Audio/tiktok/english -type f \( -name "*.srt" -o -name "*.vtt" \) | sort > logs/translation_targets.txt
```

2. 対象を小さなバッチに分けて依頼する。
   - 目安: 1回 20〜100 ファイル。
   - 長時間ジョブは途中中断に備えてバッチを小さく保つ。

3. 「入力ファイル」「出力規則」「要件」を明示して翻訳実行させる。
   - 下の「依頼テンプレート」をそのまま使ってよい。

4. 翻訳後に検証する（キュー数・時刻一致）。

```bash
python3 - <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, "scripts")
from substudy import parse_subtitle_cues

pairs = [
    # (原文, 和訳)
    ("/path/to/7603444228575890709.en.srt", "/path/to/7603444228575890709.ja.srt"),
]

ok = True
for src_raw, ja_raw in pairs:
    src = Path(src_raw)
    ja = Path(ja_raw)
    src_cues = parse_subtitle_cues(src)
    ja_cues = parse_subtitle_cues(ja)
    if len(src_cues) != len(ja_cues):
        ok = False
        print(f"[NG] cue count mismatch: {src} ({len(src_cues)}) != {ja} ({len(ja_cues)})")
        continue
    for i, (a, b) in enumerate(zip(src_cues, ja_cues), start=1):
        if a["start_ms"] != b["start_ms"] or a["end_ms"] != b["end_ms"]:
            ok = False
            print(f"[NG] timing mismatch: {ja} cue#{i}")
            break
    if ok:
        print(f"[OK] {ja}")

raise SystemExit(0 if ok else 1)
PY
```

5. ledger を更新して翻訳字幕をトラックに反映する。

```bash
python3 scripts/substudy.py ledger --config config/sources.toml --incremental
```

6. Web UI でトラック選択に `ja` が出るか確認する。

```bash
python3 scripts/substudy.py web --config config/sources.toml
```

## 6. 依頼テンプレート

以下をそのまま渡して、`INPUT_FILES` と `OUTPUT_DIR` だけ埋める。

```text
字幕ファイルを英語から日本語へ翻訳してください。

目的:
- 学習用途の日本語字幕を作成する。

入力:
- INPUT_FILES に列挙した .srt / .vtt

出力:
- OUTPUT_DIR に UTF-8 で保存
- ファイル名は video_id.ja.<ext> 形式

必須要件:
1) タイムスタンプ行は完全一致で維持
2) キュー数は元ファイルと一致
3) 変換対象は字幕テキストのみ
4) 番号行・時刻行・ブロック順序を変更しない
5) 訳注を勝手に挿入しない

品質方針:
- 意訳しすぎず、原文対応が追える自然な日本語
- 固有名詞・数値・通貨・単位は保持
- 口語フィラーは必要に応じて軽く反映

実行手順:
- 既存の .ja.* があれば上書きせずスキップ
- 処理した件数、スキップ件数、失敗件数を最後に報告
- 失敗ファイルは理由とともに一覧化
```

## 7. 受け入れ基準

- 和訳ファイルが `video_id.ja.<ext>` で生成されている。
- 原文と和訳でキュー数・時刻が一致する。
- `ledger --incremental` 後、UIトラックに和訳が表示される。
- 誤訳レビュー以前の段階として、形式崩れゼロである。

## 8. 運用メモ

- まずは1ソースで小規模検証し、問題なければ全体展開する。
- 品質改善は「訳語集（glossary）」を別ファイルで管理すると安定する。
- 翻訳はコストがかかるため、再実行は差分のみを対象にする。
