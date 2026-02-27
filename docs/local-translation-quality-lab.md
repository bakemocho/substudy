# Local Translation Quality Lab

## 1. Goal

- `translate-local` の品質改善を、仮説 -> 検証 -> 記録の短いループで進める。
- 優先課題は次の2点:
  - 英語のまま残るキューの削減
  - JSON断片が字幕に混入する不具合の根絶

## 2. Scope

- Pipeline: `scripts/substudy.py translate-local`
- Target variants: `ja-local`, `ja-asr-local`
- Initial validation source: `careervidz`

## 3. Common Metrics

- `json_fragment_rate`:
  - 字幕テキストが `{` 開始、`"ja"` 含有、`"confidence"` 含有などの JSON 断片パターンに一致したキュー割合
- `unchanged_rate`:
  - 同一タイムスタンプの英語原文キューと完全一致したキュー割合
- `english_heavy_rate`:
  - 英字が過半で日本語文字が少ないキュー割合（簡易判定）
- `timing_mismatch_cues`:
  - 同一タイムスタンプの英語キューが見つからないキュー数

## 4. Probe Command

```bash
python3 scripts/translation_quality_probe.py \
  --ledger-db data/master_ledger.sqlite \
  --source careervidz \
  --target-lang ja-local \
  --source-lang NA.eng-US \
  --top 5
```

## 5. Hypothesis Backlog

### H1: JSON parse failure時のフォールバックが英語温存を増やしている

- Rationale:
  - Stage1でJSON抽出失敗時、最終的に原文採用になる経路がある。
- Validation:
  - Stage1の parse fail 件数をカウントし、`unchanged_rate` と相関を見る。

### H2: max_tokens不足で途中打ち切りJSONが発生している

- Rationale:
  - `{\"ja\": ...` 途中切れが頻発している。
- Validation:
  - `refine-max-tokens`, `global-max-tokens`, `chunk-size` を段階変更して `json_fragment_rate` を比較。

### H3: Stage2/3 の patch apply 失敗が無検知で通過している

- Rationale:
  - patch不適用時に既存テキスト(英語含む)が残る設計。
- Validation:
  - patch parse/apply失敗数の計測を追加し、`unchanged_rate` の変化を見る。

### H4: 成功判定が timing のみで品質NGを取りこぼしている

- Rationale:
  - 現在の最終検証は cue数/時刻整合性中心。
- Validation:
  - 品質ゲート追加後に `json_fragment_rate` が実質0になるか確認。

## 6. Iteration Log

### Iteration 00 (Baseline)

- Date: 2026-02-27
- Config:
  - endpoint: `http://127.0.0.1:11435/v1/chat/completions`
  - models: draft=`gpt-oss:20b`, refine=`gpt-oss:120b`, global=`gpt-oss:120b`
  - defaults: `draft=160`, `refine=480`, `global=1200`, `temperature=0.1`, `top_p=0.9`, `chunk=12`
- Probe result (`careervidz`, `ja-local`):
  - `videos=186`
  - `total_cues=3505`
  - `json_fragment_rate=0.1994`
  - `english_heavy_rate=0.5073`
  - `unchanged_rate=0.5110`
  - `timing_mismatch_cues=0`
- Interpretation:
  - フォーマット整合は通るが、品質は低い。
  - 優先は `json_fragment_rate` と `unchanged_rate` の同時改善。

## 7. Next Step (Iteration 01)

- 変更案:
  - まずパラメータのみ変更して切り分け:
    - `--temperature 0`
    - `--top-p 1`
    - `--refine-max-tokens 1000`
    - `--global-max-tokens 2200`
    - `--chunk-size 8`
- 検証:
  - 同じ source (`careervidz`) で `--limit 20` 実行
  - Probe 指標が以下を満たすか確認:
    - `json_fragment_rate <= 0.05`
    - `unchanged_rate <= 0.30`
  - 未達なら、次ループでコード変更に進む。
