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

## 7. Iteration 01 (Parameter-only A/B, single short video)

- Date: 2026-02-27
- Target video: `careervidz/7309803358792060192` (`8 cues`)
- Baseline config:
  - `draft=160`, `refine=480`, `global=1200`, `temperature=0.1`, `top_p=0.9`, `chunk=12`
  - output: `ja-local-exp-base`
- Iteration config:
  - `draft=320`, `refine=1000`, `global=2200`, `temperature=0`, `top_p=1`, `chunk=8`
  - output: `ja-local-exp-iter01`
- Result (same video, cue-level compare):
  - Baseline:
    - `unchanged_rate=0.875` (7/8)
    - `json_fragment_rate=0.375` (3/8)
    - `english_heavy_rate=0.875` (7/8)
  - Iteration:
    - `unchanged_rate=0.000` (0/8)
    - `json_fragment_rate=0.000` (0/8)
    - `english_heavy_rate=0.000` (0/8)
- Interpretation:
  - パラメータ変更だけで、短尺動画では品質崩れが大きく改善。
  - H2（出力打ち切り/トークン不足）の寄与が高い可能性。
- Stage runtime snapshot (`translation_stage_runs`, same video):
  - Baseline (`ja-local-exp-base`):
    - draft (`gpt-oss:20b`): `request_count=8`, `elapsed_ms=40253`
    - refine (`gpt-oss:120b`): `request_count=1`, `elapsed_ms=22513`
    - global (`gpt-oss:120b`): `request_count=1`, `elapsed_ms=35138`
  - Iteration (`ja-local-exp-iter01`):
    - draft (`gpt-oss:20b`): `request_count=8`, `elapsed_ms=40399`
    - refine (`gpt-oss:120b`): `request_count=1`, `elapsed_ms=36746`
    - global (`gpt-oss:120b`): `request_count=1`, `elapsed_ms=31533`

## 8. Operational Findings (blocking factor)

- 120b endpoint latency is unstable for batch runs.
  - Quick health probe sample:
    - `gpt-oss:20b`: simple request completed in ~`3.74s`
    - `gpt-oss:120b`: same shape timed out at `30s` in one trial, completed in `25.04s` in another trial
- Long-running `translate-local` batches frequently stayed in response-wait state and required manual stop.
- Implication:
  - Quality-only comparison must be run in small units (`limit=1`, short-cue videos) until endpoint stability is improved.

## 9. Iteration 02 (Parameter-only A/B, hard 5 videos)

- Date: 2026-02-28
- Target set (baselineで `unchanged` が高かった5本):
  - `7428319288404053281`
  - `7359222341664034081`
  - `7379892278581628193`
  - `7395572078088375585`
  - `7311695207592922401`
- Baseline track:
  - `ja-local`
- Iteration track:
  - `ja-local-exp-iter01-hard5`
  - params: `draft=320`, `refine=1000`, `global=2200`, `temperature=0`, `top_p=1`, `chunk=8`
- Result (63 cues total, same 5 videos):
  - Baseline (`ja-local`):
    - `unchanged_rate=0.921` (58/63)
    - `json_fragment_rate=0.222` (14/63)
    - `english_heavy_rate=0.921` (58/63)
  - Iteration (`ja-local-exp-iter01-hard5`):
    - `unchanged_rate=0.048` (3/63)
    - `json_fragment_rate=0.000` (0/63)
    - `english_heavy_rate=0.048` (3/63)
- Interpretation:
  - `json_fragment` はゼロ化し、英語残存も大幅に減少。
  - パラメータ変更のみで改善再現性あり（H2を強く支持）。
- Runtime note (`translation_stage_runs`, iter hard5):
  - 1動画あたり:
    - draft (20b): `~72s〜123s`
    - refine (120b): `~59s〜115s`
    - global (120b): `~48s〜80s`
  - 品質は改善したが、処理時間は長い。

## 10. Next Step (Iteration 03)

- 方針A（品質維持 + 安定運用）:
  - `iter01` パラメータを暫定デフォルト候補にして、`limit` を小さく運用
  - 120b待ち対策として watchdog/retry を実装（CLI側）
- 方針B（原因追跡）:
  - Stage1/2/3 の parse fail / patch apply fail / fallback採用回数をメトリクス化
  - `translation_stage_runs` か別テーブルに記録して H1/H3 を検証
- 成功基準:
  - `json_fragment_rate <= 0.01`
  - `unchanged_rate <= 0.10`
  - 実行中断なしで `limit=10` を完走

## 11. Current Status Snapshot (2026-02-28)

- 品質傾向:
  - 既定パラメータ帯（`draft=160/refine=480/global=1200`, `temperature=0.1`, `top_p=0.9`, `chunk=12`）では、`careervidz` の `ja-local` に英語残存・JSON断片混入が目立つ。
  - `iter01` パラメータ（`draft=320/refine=1000/global=2200`, `temperature=0`, `top_p=1`, `chunk=8`）は hard set でも再現性のある改善を確認済み。
- 運用上のボトルネック:
  - `gpt-oss:120b` の応答遅延が大きく、動画本数を増やすと待機時間が不安定。
- 実装上の未解決点:
  - 成功判定が現状ほぼ timing 一致中心で、品質NG（英語残存/JSON断片）を失敗として扱えていない。
  - Stageごとの parse失敗や patch不適用の可視化が不足。

## 12. Iteration 03 Plan (LLM Audit + Targeted Repair)

目的:

- `translate-local` 実行内で品質監査と局所修正を自動化し、英語残存とJSON断片を実運用で抑え込む。

実装方針:

1. ルールベース品質ゲートを追加する。
   - cue単位で `json_fragment`, `english_heavy`, `unchanged` を判定。
   - 破損文字列（JSON断片っぽい字幕）を採用しない。
2. 監査ステージ（local LLM）を追加する。
   - 品質ゲートでNGになったcueだけを監査対象にする。
   - 監査出力は `cue_id`, `issue`, `suggested_ja` のJSON固定。
3. 修正ステージ（local LLM）を追加する。
   - 監査でNG判定されたcueのみ局所再翻訳する。
   - 全体再翻訳ではなく targeted patch に限定してコストを制御する。
4. 有界ループ化する。
   - `audit -> repair -> re-audit` を最大2回で打ち切る。
   - 規定回数内で改善しない場合は `failed` 扱いか、理由付きでフォールバックする。
5. 成功条件を timing + quality に拡張する。
   - timing一致に加えて、品質率しきい値を満たすことを成功条件にする。

初期しきい値（案）:

- `json_fragment_rate == 0`
- `english_heavy_rate <= 0.10`
- `unchanged_rate <= 0.15`（source_track=`subtitle`）

検証計画:

- Step 1: `careervidz` hard 5本で `ja-local-exp-iter03` を作り、Iteration 02 と比較。
- Step 2: `limit=10` で連続実行し、処理時間と失敗率を観測。
- Step 3: stableなら `ja-local` 既定パラメータへ段階反映。

## 13. Iteration 03-A (Quality loop prototype implemented)

- Date: 2026-02-28
- Code changes:
  - `translate-local` に rule-based quality gate を追加
  - `quality-audit` / `quality-repair` stage を追加（LLM, cue単位 patch）
  - `audit -> repair -> re-audit` ループを `--quality-loop-max-rounds` で制御
  - `--quality-enforce` 指定時のみ、品質しきい値未達を run failure として扱う
- New CLI options (主要):
  - `--quality-enforce`
  - `--quality-loop-max-rounds`
  - `--quality-json-fragment-threshold`
  - `--quality-english-heavy-threshold`
  - `--quality-unchanged-threshold`
  - `--quality-audit-model`, `--quality-repair-model`
  - `--quality-audit-max-tokens`, `--quality-repair-max-tokens`

Smoke results:

- Case A (short video, iter01 params):
  - target: `careervidz/7309803358792060192`
  - initial quality: `bad=0`
  - quality loop: not needed
- Case B (hard video, default-like params + quality loop 1):
  - target: `careervidz/7428319288404053281`
  - initial quality: `bad=9`, `json=1`, `english_heavy=9`, `unchanged=9`
  - after round1: `bad=0`, `json=0`, `english_heavy=0`, `unchanged=0`

Interpretation:

- 品質崩れケースで loop が実際に発火し、1 roundで回復できるケースを確認。
- 120b待ちの遅延は引き続きボトルネック。`limit` 小さめ運用は継続前提。
