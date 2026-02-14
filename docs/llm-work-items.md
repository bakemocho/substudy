# LLM Work Items for Automations

This document defines recurring automation-ready LLM workflows for dictionary bookmark study.

Primary objective:

- Run enrichment and QA continuously with minimal manual steps.
- Keep outputs import-safe for `dict-bookmarks-import`.

## 1. Automation Set (Recommended)

Use one automation per responsibility.

| ID  | Name               | Frequency       | Purpose                                      | Output                               |
| --- | ------------------ | --------------- | -------------------------------------------- | ------------------------------------ |
| A1  | Missing Export     | Daily           | Export unresolved dictionary bookmarks       | `exports/llm/missing_review.jsonl`   |
| A2  | Missing Enrichment | Daily           | LLM enrichment for missing entries           | `exports/llm/enriched_missing.jsonl` |
| A3  | Missing Import     | Daily           | Validate + import enriched rows              | DB updates + import log              |
| A4  | Review Card Hints  | Daily or Weekly | Generate short study hints from review cards | `exports/llm/review_hints.jsonl`     |
| A5  | Translation QA     | Weekly          | Detect suspicious EN/JA cue mismatch         | `exports/llm/translation_qa.jsonl`   |

Suggested order in one daily chain:

1. A1 (`Missing Export`)
2. A2 (`Missing Enrichment`)
3. A3 (`Missing Import`)
4. A4 (`Review Card Hints`)

A5 is independent and can run weekly.

## 2. Command Contracts

### A1. Missing Export

```bash
python3 scripts/substudy.py dict-bookmarks-curate \
  --preset missing_review \
  --format jsonl \
  --limit 200 \
  --output exports/llm/missing_review.jsonl
```

Skip rule:

- If output has 0 rows, downstream A2/A3 may no-op.

### A2. Missing Enrichment (LLM)

Input file:

- `exports/llm/missing_review.jsonl`

Preferred context source:

- If richer context is needed, additionally export `review_cards` and join by cue identifiers.

Output file:

- `exports/llm/enriched_missing.jsonl`

Output must remain importable (see section 3).

### A3. Missing Import

Validation first:

```bash
python3 scripts/substudy.py dict-bookmarks-import \
  --input exports/llm/enriched_missing.jsonl \
  --on-duplicate upsert \
  --dry-run
```

If dry-run passes (`errors=0`), run actual import:

```bash
python3 scripts/substudy.py dict-bookmarks-import \
  --input exports/llm/enriched_missing.jsonl \
  --on-duplicate upsert
```

### A4. Review Card Hints

Export source cards:

```bash
python3 scripts/substudy.py dict-bookmarks-curate \
  --preset review_cards \
  --format jsonl \
  --limit 200 \
  --output exports/llm/review_cards.jsonl
```

LLM writes:

- `exports/llm/review_hints.jsonl`

### A5. Translation QA

Input (recommended):

- `exports/llm/review_cards.jsonl`

Output:

- `exports/llm/translation_qa.jsonl`

## 3. Import-Safe Schema (Critical)

`dict-bookmarks-import` requires stricter fields than a generic LLM payload.

Minimum fields per row for re-import:

- `source_id`
- `video_id`
- `track`
- `cue_start_ms` (integer)
- `cue_end_ms` (integer)
- `term`
- `term_norm` (recommended; if omitted, it may be normalized from `term`)
- `lookup_term` (recommended)
- `definition`
- `missing_entry` (`1` for missing-entry enrichment)

`dict_entry_id` rules:

- If `missing_entry=1`: `dict_entry_id` can be omitted (auto-generated).
- If `missing_entry=0`: positive integer `dict_entry_id` is required.

Identifier immutability rule:

- Never modify `source_id`, `video_id`, `track`, `cue_start_ms`, `cue_end_ms`.

## 4. LLM Output Spec by Task

### Missing Enrichment Output

Required:

- `source_id`, `video_id`, `track`, `cue_start_ms`, `cue_end_ms`
- `term`, `lookup_term`, `definition`, `missing_entry`

Optional:

- `definition_en`
- `examples` (0-2 short)
- `confidence` (`high|medium|low`)
- `notes`

Acceptance:

- `definition` is non-empty.
- No contradiction with cue context (`cue_en_text` or `cue_text`).
- Output is valid JSONL (one object per line).

### Review Hint Output

Required:

- `card_id`
- `term`
- `one_line_hint_ja`
- `one_line_hint_en`

Optional:

- `common_mistake`
- `memory_hook`

Acceptance:

- Hint lines are concise (roughly <= 80 chars).

### Translation QA Output

Required:

- `card_id` (or stable cue identifier)
- `qa_result` (`ok|check`)
- `reason`

Optional:

- `suggested_ja` (only when `qa_result=check`)

Acceptance:

- Mark `check` only for high-suspicion mismatch.

## 5. Automation Guardrails

### Global guardrails

- Prefer cue-context accuracy over dictionary completeness.
- If uncertain, keep `missing_entry=1`, lower confidence, and explain ambiguity.
- Keep outputs machine-parseable JSONL.
- Avoid long prose.

### Failure behavior

- If export command fails: stop chain.
- If LLM output parse fails: stop import and keep artifact for manual inspection.
- If import dry-run has `errors>0`: do not run actual import.

### Duplicate policy

- Default: `--on-duplicate upsert`
- Conservative mode: `skip`
- Strict QA: `error`

## 6. Suggested Automation Prompts (Codex App)

Use these as automation prompts (task only; schedule/workspace are configured separately in UI).

### Prompt for A2 Missing Enrichment

Read `exports/llm/missing_review.jsonl` and generate `exports/llm/enriched_missing.jsonl` as JSONL.
For each row, fill a concise context-aware `definition` and keep identifiers unchanged.
Output rows must stay import-safe for `dict-bookmarks-import` (`source_id`, `video_id`, `track`, `cue_start_ms`, `cue_end_ms`, `term`, `definition`, `missing_entry=1`).
If uncertain, add low confidence and brief notes rather than hallucinating.

### Prompt for A4 Review Card Hints

Read `exports/llm/review_cards.jsonl` and generate `exports/llm/review_hints.jsonl`.
Create short one-line EN/JA hints per card, plus optional common mistake and memory hook.
Keep each hint concise and avoid copying long dictionary definitions.

### Prompt for A5 Translation QA

Read `exports/llm/review_cards.jsonl` and generate `exports/llm/translation_qa.jsonl`.
Set `qa_result=check` only for high-confidence mismatch between EN and JA cue meaning.
Include a short reason and suggested JA only when check is needed.

## 7. Operational Checklist

Before enabling daily automation:

1. Verify command paths from repo root.
2. Run A1 -> A2 -> A3 once manually.
3. Confirm dry-run import returns `errors=0`.
4. Spot-check at least 10 rows in web UI context.
5. Enable schedule.

After deployment:

1. Monitor import summary counts (`inserted/updated/skipped/errors`).
2. Review weekly QA output volume (`check` ratio should stay low).
3. Adjust batch limits (`--limit`) to keep run time stable.
