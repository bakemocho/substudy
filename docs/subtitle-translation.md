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
- **動画ファイルが存在する字幕のみ**を翻訳対象とする（字幕だけあって動画がないものは対象外）。

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
- 例: `/path/to/subs/7603444228575890709.ja.srt`
- 既存が `video_id.en.srt` の場合でも、和訳は `video_id.ja.srt` で統一。

## 5. 実行手順

1. 翻訳対象ファイルを列挙する（動画あり字幕のみ）。

```bash
cd /path/to/substudy
mkdir -p logs
python3 scripts/substudy.py ledger --config config/sources.toml --incremental
sqlite3 data/master_ledger.sqlite "
SELECT DISTINCT s.subtitle_path
FROM subtitles s
JOIN videos v
  ON v.source_id = s.source_id
 AND v.video_id = s.video_id
WHERE v.has_media = 1
  AND s.subtitle_path IS NOT NULL
  AND s.subtitle_path <> ''
  AND lower(COALESCE(s.ext, '')) IN ('srt', 'vtt')
ORDER BY s.subtitle_path;
" > logs/translation_targets.txt
```

2. 対象を小さなバッチに分けて依頼する。
   - **件数よりキュー数でバッチを切る方が安定する。**
   - 段階的に拡大: 1件 → 5件 → 15〜20件 → 30〜50件。
   - 昇格条件: キュー数一致・時刻一致が全件OK、失敗率1%未満、手動サンプルで致命的誤訳なし。

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

5. 翻訳ログに記録する（`master_ledger.sqlite` の `translation_runs` テーブル）。

   各レコードに以下を含める:
   - `source_id`, `video_id`, `source_path`, `output_path`, `cue_count`, `cue_match`
   - `agent`, `method`, `method_version`
   - `created_at`, `finished_at`（ISO 8601 UTC）
   - `summary`（内容の日本語要約。学習時の内容把握に有用）

6. ledger を更新して翻訳字幕をトラックに反映する。

```bash
python3 scripts/substudy.py ledger --config config/sources.toml --incremental
```

7. Web UI でトラック選択に `ja` が出るか確認する。

```bash
python3 scripts/substudy.py web --config config/sources.toml
```

## 6. 依頼テンプレート

翻訳完了後、各ファイルの内容要約を `summary` として翻訳ログに記録すること。

### テンプレート

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

記録:
- 翻訳後、各ファイルの内容を日本語で短く要約し summary として報告
- 翻訳ログ (master_ledger.sqlite の translation_runs テーブル) にレコードを追記
```

## 7. 翻訳ログ

翻訳記録は `master_ledger.sqlite` の `translation_runs` テーブルで管理する。

### テーブル概要

| カラム | 説明 |
|--------|------|
| `run_id` | 自動採番の主キー |
| `source_id` | ソース識別子（`sources` テーブル FK） |
| `video_id` | 動画ID |
| `source_lang` / `target_lang` | 言語ペア（デフォルト `en` → `ja`） |
| `source_path` / `output_path` | 入力・出力ファイルパス |
| `cue_count` | キュー数 |
| `cue_match` | キュー数一致なら `1`、不一致なら `0` |
| `agent` | 翻訳エージェント（例: `claude-opus-4-6`） |
| `method` / `method_version` | 翻訳手法とバージョン（例: `direct` / `v1`） |
| `summary` | 内容の日本語要約 |
| `status` | `active` / `superseded` / `failed` |
| `started_at` / `finished_at` | 処理開始・完了時刻 |
| `created_at` | レコード作成時刻 |

### ステータス運用

- `active`: 現在有効な翻訳。
- `superseded`: 新しい翻訳に置き換えられた（履歴として保持）。
- `failed`: 翻訳失敗。

同じ `(source_id, video_id, target_lang)` へ新規 `active` を追加する前に、既存 `active` を `superseded` へ更新する。
運用時は必ずトランザクションで実施する:

```sql
BEGIN;
UPDATE translation_runs
SET status = 'superseded'
WHERE source_id = :source_id
  AND video_id = :video_id
  AND target_lang = :target_lang
  AND status = 'active';

INSERT INTO translation_runs (
  source_id, video_id, source_lang, target_lang,
  source_path, output_path, cue_count, cue_match,
  agent, method, method_version, summary,
  status, started_at, finished_at, created_at
) VALUES (
  :source_id, :video_id, 'en', 'ja',
  :source_path, :output_path, :cue_count, :cue_match,
  :agent, :method, :method_version, :summary,
  'active', :started_at, :finished_at, :created_at
);
COMMIT;
```

同じ `(source_id, video_id, target_lang)` に複数行が存在しうる（履歴）。最新の有効翻訳を取得するには:

```sql
SELECT * FROM translation_runs
WHERE source_id = ? AND video_id = ? AND target_lang = 'ja' AND status = 'active'
ORDER BY created_at DESC LIMIT 1;
```

### INSERT時の必須項目

`NOT NULL` 制約があるため、最低限以下を必ず設定する:

- `source_id`
- `video_id`
- `source_path`
- `output_path`
- `created_at`（ISO 8601 UTC 文字列）

`status` は省略時 `active` になるが、明示指定を推奨する。

### クエリ例

```bash
# 全 active 翻訳の一覧
sqlite3 data/master_ledger.sqlite \
  "SELECT video_id, agent, summary FROM translation_runs WHERE status='active'"

# JSON 形式で export（旧 logs/translation_log.json 相当）
sqlite3 -json data/master_ledger.sqlite \
  "SELECT video_id, source_path, output_path, cue_count, cue_match, summary, finished_at AS translated_at FROM translation_runs WHERE status='active'"
```

### 旧 JSON ログとの関係

`logs/translation_log.json` の既存レコードは `translation_runs` テーブルにマイグレーション済み。JSON ログファイルはアーカイブとして保持するが、新規記録は DB に直接書き込む。

環境再構築時に再マイグレーションが必要な場合は、次の例を使う:

```bash
python3 - <<'PY'
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

db_path = Path("data/master_ledger.sqlite")
json_path = Path("logs/translation_log.json")
if not json_path.exists():
    raise SystemExit(f"missing: {json_path}")

rows = json.loads(json_path.read_text(encoding="utf-8"))
now_iso = datetime.now(timezone.utc).isoformat()

conn = sqlite3.connect(db_path)
try:
    conn.execute("PRAGMA foreign_keys = ON")
    for row in rows:
        source_id = str(row.get("source_id") or "source_1")
        video_id = str(row.get("video_id") or "")
        if not video_id:
            continue
        conn.execute(
            """
            INSERT INTO translation_runs (
              source_id, video_id, source_lang, target_lang,
              source_path, output_path, cue_count, cue_match,
              agent, method, method_version, summary,
              status, finished_at, created_at
            ) VALUES (?, ?, 'en', 'ja', ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                source_id,
                video_id,
                str(row.get("source_path") or ""),
                str(row.get("output_path") or ""),
                row.get("cue_count"),
                row.get("cue_match"),
                str(row.get("agent") or "claude-opus-4-6"),
                str(row.get("method") or "direct"),
                str(row.get("method_version") or "v1"),
                str(row.get("summary") or ""),
                str(row.get("translated_at") or now_iso),
                now_iso,
            ),
        )
    conn.commit()
finally:
    conn.close()
PY
```

## 8. 受け入れ基準

- 和訳ファイルが `video_id.ja.<ext>` で生成されている。
- 原文と和訳でキュー数・時刻が一致する。
- `ledger --incremental` 後、UIトラックに和訳が表示される。
- 誤訳レビュー以前の段階として、形式崩れゼロである。
- `translation_runs` テーブルに `summary` 付きでレコードが記録されている。

## 9. 運用メモ

- まずは1ソースで小規模検証し、問題なければ全体展開する。
- 品質改善は「訳語集（glossary）」を別ファイルで管理すると安定する。
- 翻訳はコストがかかるため、再実行は差分のみを対象にする。
