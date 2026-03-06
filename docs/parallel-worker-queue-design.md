# 並列ワーカー化設計書（同一source同時実行対応）

更新日: 2026-03-07  
状態: In Progress（Phase 0-4 と launchd/manual 分離ステップを実装済み、運用検証中）

## 0. 実装進捗（2026-03-07）

### 実装済み

- DB スキーマに `work_items` / `source_poll_state` / `worker_heartbeats` を追加。
- `sync` / `backfill` に `--execution-mode legacy|queue` を追加。
- `sync --execution-mode queue` で source ごとの discovery を実行し、`work_items(stage=media)` へ投入。
- `source_poll_state.next_poll_at` を使った source ごとの discovery 間隔制御（既定 24h）を実装。
- `backfill --execution-mode queue` で window 取得済み ID を `work_items` へ投入。
- queue 導入に伴う回帰テストを追加（スキーマ作成、enqueue 再投入、poll 間隔抑制）。
- `queue-worker` コマンドを追加し、`work_items` の lease 取得/期限切れ回収/heartbeat/retry/dead を実装。
- worker から `sync_source` を stage 別（`media/subs/meta`）に再利用できるようにし、候補限定実行（single video）を追加。
- `queue-worker --stage asr` を追加し、単一動画ASR実行を queue 経由で処理できるようにした。
- `queue-worker --stage loudness` を追加し、単一動画ラウドネス解析を queue 経由で処理できるようにした。
- `queue-worker --stage translate` を追加し、単一動画 translate-local 実行を queue 経由で処理できるようにした。
- worker 実行時は source 共通 `urls.txt` を使わず、一時 `urls.*.txt` を使うようにした。
- legacy 経路の `sync_source` でも source 共通 `urls.txt` ではなく、一時 `urls.*.txt` を使うようにした。
- `downloads` に `work_items` の status 集計と pending 行表示を追加。
- `queue-worker` 実行中の lease keepalive（期限延長）を追加。
- `media` 成功時に `subs/meta/asr/loudness` を downstream として自動 enqueue する連鎖を追加。
- `subs/asr` 成功時に `translate` を downstream として自動 enqueue する連鎖を追加。
- legacy 側の `running` 回収ロジックを撤去し、queue lease 回収へ統一した。
- `sync/backfill --execution-mode queue` に producer 共有ロックを追加し、launchd/手動の同時 producer 起動を抑止。
- `run_daily_sync.sh` / `run_weekly_full_sync.sh` を queue 構成へ移行し、`sync/backfill` producer と `queue-worker` 複数起動を分離。
- `install_launchd.sh` を producer（daily/weekly）と worker（media/pipeline）の複数ジョブ構成へ拡張。
- `technical-guide.md` に producer/worker 分離運用（手動コマンド・install時オプション）を追記。
- daily/weekly ランナーから `asr/loudness/translate-local` 直実行レーンを撤去し、queue-worker 駆動へ統一。

### 未着手/継続中

- なし（Phase 0-4 の当初タスクは完了）。

## 1. 背景

現在の `sync/backfill` は source 単位で直列に近い実行モデルで、同一 source を別 terminal から同時実行すると競合しやすい。

主な競合要因（導入前）:

- 起動時に legacy 経路で `running` 回収を行っていたため、他プロセス実行中レコードへの誤作用余地があった（現在は撤去済み）。
- source 共通の `urls.txt` を毎回上書きしていた（現在は run-local 一時ファイルへ移行済み）。
- `media_archive`/`subs_archive` 共有前提で、同時更新時の整合性保証が弱い

一方で、source の更新チェックは高コストであり、頻度を抑えつつ、既存 retry/ASR/loudness/translate を並列に回したい。

## 2. 目標（案3: 完全並列）

1. 同一 source を複数プロセス/複数 terminal から安全に同時実行できる。  
2. 処理単位を `source` から `video_id x stage` に分解し、ワーカーがキューを奪取して処理する。  
3. source の更新チェック（ID発見）は既定 24h ごと（source別）に制御する。  
4. 失敗再試行・進捗可視化・中断復旧を DB 中心で一貫管理する。  
5. 既存運用（daily/weekly）を段階移行できる。

## 3. 非目標

- 初期段階での外部メッセージブローカー導入（Kafka/Redis 等）はしない。  
- いきなり全 stage を同時に切り替えない（段階導入）。

## 4. 要件

### 機能要件

- ワーカーは `queued` タスクを lease 取得して処理する。
- lease 失効時は他ワーカーが再取得できる。
- 同一 `(source_id, stage, video_id)` は重複実行されない（同時 lease は1つ）。
- retry は task レベルで `attempt_count / next_retry_at` 管理。
- source 更新チェックは source ごとの最終実行時刻で抑制。

### 非機能要件

- SQLite WAL 前提で動作する。
- 中断/クラッシュ後に自動回復できる（手動リセット不要）。
- 既存 DB (`download_state`, `download_runs`, `asr_runs`) との互換性を維持しつつ移行する。

## 5. 提案アーキテクチャ

### 5.1 コンポーネント

- `producer`:
  - source 更新チェック（ID discovery）
  - 発見した `video_id` を task キューへ投入
- `worker`:
  - stage 別に task を lease 取得して実行
  - 結果を task 状態へ反映（success/error/retry）
- `coordinator`:
  - ポーリング間隔管理（source別 24h）
  - stale lease 回収

### 5.2 DB テーブル（新規案）

1. `work_items`
- 主キー: `id`
- 一意: `(source_id, stage, video_id)`
- 主列:
  - `source_id`, `stage`, `video_id`
  - `status` (`queued|leased|success|error|dead`)
  - `priority`（小さいほど先）
  - `attempt_count`
  - `next_retry_at`
  - `lease_owner`
  - `lease_token`
  - `lease_expires_at`
  - `last_error`
  - `created_at`, `updated_at`, `started_at`, `finished_at`

2. `source_poll_state`
- 主キー: `source_id`
- 主列:
  - `last_poll_at`
  - `next_poll_at`
  - `poll_interval_hours`（既定 24）
  - `last_poll_status`
  - `last_error`

3. `worker_heartbeats`
- 主キー: `worker_id`
- 主列: `host`, `pid`, `started_at`, `last_heartbeat_at`

4. `run_events`（任意、監査用）
- 各 state transition を append で保存

## 6. タスク状態遷移

`queued -> leased -> success`

失敗時:

`leased -> queued(next_retry_at=...)`（retry 可能）  
`leased -> dead`（最大試行超過または非retry）

lease 失効時:

`leased(期限切れ) -> queued`

## 7. lease方式（同一source同時実行の核）

- 取得時に `lease_token` を払い出し、更新時は `WHERE lease_token = ?` で条件更新。
- ワーカーは heartbeat とは別に lease 延長（処理長い stage 用）。
- 失効 lease は定期的に回収。

ポイント:

- `running` を一括 `error` 化する現行ロジックは廃止（または queue モード時は無効化）。
- 他プロセス実行中のタスクを誤回収しない。

## 8. ファイル競合対策

### 8.1 `urls.txt` 共有をやめる

- 現行の source 共通 `archives/urls.txt` は同時実行に不向き。
- `run_id`/`worker_id` 単位の一時ファイルへ変更:
  - 例: `archives/tmp/urls.<run_id>.<worker_id>.txt`
- 実行後に削除。

### 8.2 archive ファイルの扱い

- 将来的には DB キューを真実源にして archive 依存を下げる。
- 移行期は archive を「補助キャッシュ」と位置付ける。

## 9. source更新チェック（1日1回）

- `source_poll_state.next_poll_at` を参照し、未到達なら discovery をスキップ。
- 既定 `poll_interval_hours=24`。
- backfill は別経路として扱い、必要に応じてこの制約をバイパス可能にする。

## 10. 既存コマンドとの対応

- `sync`:
  - 役割を producer 中心へ縮小（ID発見 + queue投入）
- `backfill`:
  - 発見 ID の queue 投入に特化
- `downloads`:
  - `download_state` + `work_items` の両方を表示して移行期の観測性を確保
- `asr/loudness/translate-local`:
  - 既存ロジックは維持しつつ、入力を `work_items` 駆動へ置換

## 11. 移行フェーズ

### Phase 0: 観測強化

- `work_items` なしで現行指標を拡充（処理時間、retry理由分類）。

### Phase 1: queue導入（read-only併走）

- `sync/backfill` 後に task を投入するだけ（worker未適用）。
- 既存処理結果と queue の整合を検証。

### Phase 2: `meta`/`subs` から worker 化

- 比較的副作用の小さい stage から切替。
- `urls.txt` 一時ファイル化を先行。

### Phase 3: `media` worker 化 + audio fallback

- donor 取得・merge・learned format を task 駆動に移植。
- lease 延長を実装（長時間処理対策）。

### Phase 4: 回収

- source 単位 `running` 回収ロジックを撤去。
- 必要なら `download_runs` を要約テーブル化。

## 12. 失敗時のロールバック方針

- フラグで旧実装へ戻せるようにする:
  - `--execution-mode legacy|queue`
- queue モード障害時は legacy に即時切替可能にする。

## 13. 受け入れ基準

1. 同一 source で 2 プロセス同時実行しても、同一 `(source, stage, video)` の二重実行が発生しない。  
2. プロセスクラッシュ後、lease timeout で別ワーカーが再開できる。  
3. source discovery が source別に 24h 抑制される。  
4. 既存 daily/weekly の結果（成功件数、失敗率）が劣化しない。  
5. `downloads` で retry待ち/実行中/失敗理由を一貫表示できる。

## 14. 実装順（推奨）

1. `--execution-mode` と新テーブル追加  
2. `source_poll_state` 導入（既存 `app_state` 依存を整理）  
3. `urls.txt` の run-local 化  
4. `meta` worker  
5. `subs` worker  
6. `media` worker（fallback含む）  
7. legacy 回収コードの撤去

## 15. オープン事項

- archive ファイルを将来的に完全廃止するか。  
- worker 優先度（新規取得 vs retry）のデフォルト方針。  
- stage 間依存（`media -> subs/meta -> asr`）を queue 上でどう表現するか（単純投入 or DAG）。

## 16. launchd + 手動並行運用計画（安全優先）

### 16.1 方針

- 並行実行を 2 系統に分離する。
  - `producer`: `sync/backfill --execution-mode queue`（ID 発見と queue 投入）
  - `worker`: `queue-worker`（stage 実処理）
- 安全性の鍵は以下:
  - producer は常に単一起動（共有ロック必須）
  - worker は複数起動可（DB lease で重複処理防止）

### 16.2 同時実行ポリシー

1. `sync --execution-mode queue`: 同時実行 `1`（launchd/手動合算）
2. `backfill --execution-mode queue`: 同時実行 `1`（launchd/手動合算）
3. `queue-worker`: 同時実行 `N`（増減可能）
4. `legacy sync/backfill`: 運用停止（互換のため CLI は残す）
5. `asr/loudness/translate-local` 直実行: 移行完了後は原則停止（queue-worker に統一）

### 16.3 ロック設計（producer 専用）

- ロック対象:
  - `sync --execution-mode queue`
  - `backfill --execution-mode queue`
- ロックファイル:
  - `data/locks/producer.lock`（固定パス）
- 要件:
  - launchd と手動の双方が同じロックを使う
  - `queue-worker` はロック対象外
  - ロック取得失敗時は即時終了し、運用ログに理由を残す

### 16.4 運用構成（最終形）

1. launchd producer ジョブ:
   - `sync --execution-mode queue --skip-ledger`
   - `backfill --execution-mode queue --skip-ledger`
2. launchd worker ジョブ（複数定義可）:
   - `queue-worker --stage media --max-items ...`
   - `queue-worker --stage subs --stage meta --stage asr --stage loudness --stage translate`
3. launchd catch-up ジョブ:
   - `ledger --incremental`
   - `downloads`
4. 手動運用:
   - producer は「ロック付きコマンド」でのみ起動
   - worker は必要に応じて追加起動可

### 16.5 実装ステップ

1. 完了: producer ラッパー追加（ロック取得/解放、共通ログ）
2. 完了: `run_daily_sync.sh` / `run_weekly_full_sync.sh` を queue 構成へ移行
3. 完了: launchd 定義を producer/worker 分離
4. 完了: 手動運用向けコマンド例を `technical-guide.md` に追記
5. 完了: 既存 legacy 直実行レーン（asr/loudness/translate-local 直呼び）を段階停止

### 16.6 検証項目（受け入れ）

1. launchd producer 実行中に手動 producer 起動しても、2 本目は安全に reject される
2. launchd worker + 手動 worker を同時起動しても、同一 `(source, stage, video)` 重複処理が発生しない
3. 24h poll 制御が維持され、source への過剰アクセスが増えない
4. `downloads` で retry/dead/leased が観測可能
5. daily/weekly 成果（成功件数、失敗率）が悪化しない
