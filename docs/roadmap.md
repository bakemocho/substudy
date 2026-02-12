# Roadmap (updated)

Current priority is subtitle-first study UX: fast cue navigation + EN/JA parallel reading.

## Immediate: playback-card UX polish + dictionary continuity

Goal: reduce visual/interaction friction during continuous study.

- Add progress timer in video card (bottom-right overlay):
  - circular pie-style timer on top of video card
  - base color: semi-transparent milky gray/white
  - fill grows clockwise from 12 o'clock based on playback progress ratio
  - near video end, filled semi-transparent area should cover the full circle
  - keep timer in sync with pause/seek/replay
- Add viewport snap behavior for video card:
  - card snaps to top/bottom edges while scrolling (sticky "click" feeling)
  - apply slight resistance/hysteresis when leaving snapped state
  - avoid scroll-chain conflict with subtitle reel / bookmark panel internal scroll
- Expand hover dictionary targets:
  - enable same hover dictionary in English words inside subtitle reel rows
  - enable same hover dictionary in English words inside saved bookmark text
  - keep existing subtitle-overlay hover behavior unchanged
- Keep reel focus stable while using hover dictionary:
  - when pointer is on subtitle-reel word/popup/bridge, pause reel auto-follow scroll
  - resume auto-follow only after hover session ends
- Place dictionary popup near hover source card:
  - overlay words -> popup in video card (current behavior)
  - reel words -> popup near reel card word
  - bookmark words -> popup near bookmark card word
  - avoid routing every popup to video card when source is outside overlay
- Add hover bridge between word and popup:
  - fill the gap with transparent hover-hit region so pointer can travel without losing context
  - prevent accidental re-target to words behind the gap while moving to popup
  - keep popup hide timing stable (no flicker while crossing)
- Define recursive dictionary tree interaction rules (graph-mode draft):
  - treat first popup as `lv.1` node (source word), then nested popup lookups as `lv.2+` child nodes
  - model is tree-only (parent -> child lookup path), no collocation/semantic edge expansion here
  - click on a lookup term pins the node (popup stays visible even when pointer leaves)
  - outside click or `Esc` clears the full tree and dictionary loop state
  - when hovering a node, freeze that node position; other nodes re-layout toward stable positions

Acceptance:

- Timer draw direction/fill matches actual video progress for play/pause/seek.
- Card snap feels deterministic: easy to snap, slightly resistant to unsnap.
- Hover dictionary opens from overlay/reel/bookmark consistently.
- While using hover dictionary in subtitle reel, active cue auto-scroll does not yank the view.
- Popup appears adjacent to the hover source region (overlay/reel/bookmark), not always in video card.
- Moving pointer from word to popup does not collapse/switch lookup unexpectedly.
- Click-pin / outside-click / `Esc` rules coexist with existing hover-hide behavior.
- In graph-mode, hovered node stays fixed while non-hovered nodes can settle around it.

Status (2026-02-12):

- Implemented:
  - video-card progress timer
  - viewport snap + release resistance
  - transparent hover bridge between word and popup
  - hover dictionary hooks in subtitle reel and saved bookmarks
  - reel hover session now pauses auto-follow scrolling; auto-follow restores on pointer leave or short idle
  - popup placement now anchors to hover source context (overlay/reel/bookmark), not just video-card fixed position
  - click on subtitle words now pins dictionary popup; outside click or `Esc` clears the popup/loop state
- Done as immediate scope:
  - hover dictionary continuity items are treated as complete for current UX baseline
  - remaining refinements are tracked in later phases (`4`, `7`) as iterative improvements, not blockers

## 0) Done / baseline

- EIJIRO dictionary indexing (`dict-index`) + hover lookup + collocation ranking.
- Hover dictionary UX improvements (copy, loop, scroll isolation, prefetch).
- Translation runbook + translation log table (`translation_runs`) in SQLite.
- EN/JA subtitle tracks can be added by file naming (`video_id.<lang>.<ext>`) and reflected by `ledger`.

## 1) Done: translated-availability filter (prerequisite for parallel cards)

Goal: enable stable study-mode feed slicing before EN/JA card expansion.

- Add feed filter mode:
  - `all`
  - `ja_only` (和訳済みのみ)
  - `ja_missing` (未和訳のみ)
- Source of truth:
  - subtitle track existence (`video_id.ja.*`)
  - optional `translation_runs` status as supplemental signal
- Keep behavior deterministic in UI:
  - persist selected filter in URL/query + local storage
  - apply filter without breaking shuffle/history behavior

Acceptance:

- Filtered feed count is consistent across reloads.
- `ja_only` mode never shows videos without JA track.
- Missing JA data degrades gracefully (no crash).

## 2) Done: subtitle reel in player side panel

Goal: repurpose the current "字幕ブックマーク" area as a subtitle reel panel.

- Replace panel default content with reel list centered on current cue.
- Reel row should show:
  - line 1: English cue
  - line 2: Japanese cue (if available)
- Keep cue click behavior: jump video to cue `start_ms`.
- Keep bookmark feature, but move it behind each reel row (row action) instead of occupying panel as primary view.

Acceptance:

- During playback, active cue stays centered and updates smoothly.
- Reel click seeks accurately.
- Existing bookmark APIs remain usable (no data migration required).

## 3) Done: EN/JA parallel subtitle mapping

Goal: stable 1:1 view for bilingual cues.

- Add client-side cue aligner:
  - primary key: `start_ms/end_ms`
  - fallback: nearest timestamp within threshold
- Rendering rules:
  - JA missing -> show EN only
  - EN missing (rare) -> show JA with marker
- Add panel toggle:
  - `EN only`
  - `EN + JA` (default for study mode)

Acceptance:

- For typical tracks, most cues align without visible mismatch.
- No crash/blank panel when one language track is missing.

## 4) In progress: hover dictionary recursive tree graph

Goal: make hover dictionary a reusable learning surface.

Status:

- [x] Hover辞書にブックマーク機能を追加する（保存/解除トグル）
- [x] ブックマーク保存時に term/definition/context を `dictionary_bookmarks` へ永続化する
- [x] 保存済み状態をポップアップ内で即時反映し、リロード後も維持する
- [x] 辞書エントリ未登録語（`辞書エントリが見つかりません。`）でもブックマーク保存/解除できる
- [ ] 再帰ホバー辞書ノードの重なり回避・安定再配置を継続改善する

- Add dictionary bookmark action in hover popup.
  - bookmark unit: term/definition + video context (source/video/track/time/cue text)
  - persist in dedicated table (`dictionary_bookmarks`)
- Add recursive hover inside dictionary popup.
  - hovering English words in definitions opens next-level popup lookup
  - build parent->child tree nodes by actual lookup path (`lv.1`, `lv.2`, ...)
  - keep depth guard + visited-term guard to prevent infinite loop
- Graph-mode rendering policy:
  - tree visualization only (no collocation edges)
  - hovered node is temporarily fixed in place
  - non-hovered nodes can continue force-layout relaxation
  - click-pinned node remains fixed/visible until explicit clear
- Prerequisite alignment with Immediate block:
  - popup placement/focus policy across overlay/reel/bookmark contexts (done)
  - reel auto-follow freeze/resume policy during dictionary hover sessions (done)
  - click-pin / outside-click / `Esc` coexistence rules (baseline single-popup behavior done)
  - treat these as mandatory before enabling recursive graph-mode by default
- Keep MVP storage model simple:
  - subtitle JSON migration is not required for this step
  - can be implemented with existing subtitle files + SQLite metadata

Acceptance:

- Popup bookmark toggle is responsive and persists after reload.
- Popup context policy is deterministic before recursive hover is enabled globally.
- Recursive tree graph can grow to `lv.2+` via lookup-in-lookup without UI lock/conflict.
- Hovered node freeze + surrounding-node re-layout behavior is visually stable.
- click-pin / outside-click / `Esc` clear behavior is deterministic.
- Existing subtitle hover behavior remains intact.

## 4.5) Next: dictionary bookmark knowledge pipeline (LLM参照・知識集積・教材化)

Goal: save-to-learn で終わらせず、辞書ブックマークを継続的に学習資産化する。

- Add export/query workflow for bookmark reuse:
  - `dictionary_bookmarks` から用途別に抽出（未登録語 / 頻出語 / 文脈つき）
  - LLM投入用の安定フォーマット（JSONL or CSV）を用意
- Add LLM-assisted enrichment pipeline:
  - 未登録語に対する定義・訳語・用例候補の追記フロー
  - 既存エントリとの重複判定とマージポリシー
- Add knowledge accumulation model:
  - term単位の履歴（初回保存時刻、出現動画数、再遭遇回数）
  - cue文脈の再利用（例文カード化、復習優先度付け）
- Add materialization outputs:
  - 復習カード（EN/JA + cue + timestamp link）
  - 「未登録語レビュー」バッチ
  - 学習セッション用の curated list（難語/頻出/最近保存）

Acceptance:

- ブックマークDBからLLM入力用データを再現可能に抽出できる。
- 未登録語の補完結果を再投入しても重複/破壊が起きない。
- 学習カード出力がワンクリック相当で再生成できる。

## 5) Next: similar-scene suggestion via cue embeddings

Goal: let users discover semantically similar scenes from current subtitle context.

- Build embeddings for subtitle cue windows (current cue + surrounding cues).
- Index vectors locally (FAISS or equivalent local ANN index).
- Add `Similar Scenes` action from current cue / subtitle reel row.
- Retrieval rules:
  - exclude near-duplicate neighbors in same video/time neighborhood
  - return diverse results across videos/sources when possible
- Result card should show:
  - EN cue text
  - JA cue text (if available)
  - source/video and timestamp
  - click-to-jump behavior

Acceptance:

- Query from current cue returns relevant top-k candidates quickly.
- Results are not dominated by same-video adjacent cues.
- Jump from suggestion lands on correct timestamp and updates subtitle state.

## 6) Next: reel linguistics overlay (NLP + dependencies)

Goal: treat subtitle reel as grammar visualization surface.

- Add English token/POS layer for each reel row.
- Add dependency edges (head -> dependent) as overlay arrows on reel lines.
- Color-code tokens by POS category.
- Add EN/JA alignment color hints:
  - same color group for likely aligned EN token span and JA phrase span
  - fallback to neutral color when alignment confidence is low
- Keep interactions readable:
  - overlay can be toggled on/off
  - hover dictionary and dependency overlay do not conflict

Acceptance:

- Dependency arrows render without blocking line selection/click.
- POS colors are consistent across videos and tracks.
- EN/JA alignment coloring remains stable when seeking/scrolling.

## 7) Quality/performance pass for reel + dictionary coexistence

- Ensure wheel/keyboard focus rules are unambiguous between:
  - reel scrolling
  - hover dictionary popup
  - video seek / next-prev
- Keep dictionary lookup latency low with prefetch + cache hit monitoring.
- Add lightweight regression checks for:
  - cue sync drift
  - popup interaction conflicts
  - keybind conflicts

## 8) Translation pipeline hardening

- Add helper command for translation target extraction (video-present subtitle set).
- Add DB write helper for `translation_runs` (active/superseded transaction pattern).
- Keep translation automation opt-in until quality/cost metrics stabilize.

## 9) Next: extension-based ingest migration (`yt-dlp` dependency reduction)

Goal: support easier local distribution and reduce operational dependence on `yt-dlp`.

- Introduce ingest-provider abstraction:
  - current `yt-dlp` path as one provider
  - browser-extension ingest as another provider
- Add local bridge/queue for extension-captured payloads.
- Keep existing `ledger`/`web` compatibility by converting captures to canonical ingest schema.
- Operate in mixed mode first:
  - `extension_preferred`
  - `extension_only`
  - `ytdlp_only` (legacy fallback)

Reference:

- See `docs/extension-ingest-mvp.md` for phased implementation details and acceptance criteria.

## Backlog

- Unified subtitle JSON model (optional, only when metadata pressure grows).
- Word importance ranking by frequency/learning priority.
- Extended grammar visualization beyond reel view.
- `bep-eng.txt` integration remains out of scope for current roadmap.
