# LLM Work Items

This document defines what to delegate to LLMs, in what order, and with what I/O contract.

Target scope: dictionary bookmark knowledge reuse (`4.5` baseline) and follow-up study material generation.

## 1. Priority Order

1. Missing-entry enrichment (highest ROI)
2. Context-aware definition cleanup/rerank
3. Study-card explanation generation (EN/JA)
4. Translation QA (EN/JA cue consistency)
5. Optional: personalized review session curation

## 2. Input Sources

Use existing CLI outputs as stable LLM input:

- missing review:
  - `python3 scripts/substudy.py dict-bookmarks-curate --preset missing_review --format jsonl --limit 200 --output exports/llm/missing_review.jsonl`
- review cards:
  - `python3 scripts/substudy.py dict-bookmarks-curate --preset review_cards --format jsonl --limit 200 --output exports/llm/review_cards.jsonl`
- frequent terms:
  - `python3 scripts/substudy.py dict-bookmarks-curate --preset frequent_terms --format jsonl --limit 200 --output exports/llm/frequent_terms.jsonl`

Recommended ramp-up batch size:

- dry run: 1 item
- pilot: 5 items
- small batch: 10 items
- steady run: 20-50 items

## 3. Task Specs

### 3.1 Missing-entry enrichment

Goal:

- Fill dictionary bookmarks where `missing_entry=1`.

Required input fields:

- Preferred (`review_cards` export): `term`, `lookup_term`, `cue_en_text`, `cue_ja_text`, `source_id`, `video_id`, `cue_start_ms`, `cue_end_ms`, `track`
- Fallback (`missing_review` export): `term`, `lookup_term`, `cue_text`, `source_id`, `video_id`, `cue_start_ms`, `cue_end_ms`, `track`

Expected output fields (JSONL):

- `source_id`
- `video_id`
- `cue_start_ms`
- `term`
- `lookup_term`
- `definition` (short, study-focused, Japanese allowed)
- `definition_en` (optional concise EN gloss)
- `examples` (0-2 short examples)
- `confidence` (`high|medium|low`)
- `notes` (optional; ambiguity or caution)
- `track` (required for re-import)
- `cue_end_ms` (required for re-import)
- `missing_entry` (`1` for enrichment rows; default)
- `dict_entry_id` (optional when `missing_entry=1`; required if `missing_entry=0`)

Acceptance:

- no empty `definition`
- no contradictory claim vs cue context (`cue_en_text` or `cue_text`)
- confidence is present

### 3.2 Context-aware cleanup / rerank

Goal:

- Improve usefulness when entry is too broad or noisy.

Input:

- bookmark export with existing definition + cue context

Output:

- `definition_refined` (single best sense for this cue)
- `sense_reason` (why this sense matches cue)
- `alternatives` (optional short list)

Acceptance:

- refined definition is shorter and more cue-relevant than original

### 3.3 Study-card explanation generation

Goal:

- Produce reusable micro-lessons for spaced review.

Input:

- `review_cards` JSONL

Output:

- `card_id`
- `term`
- `one_line_hint_ja`
- `one_line_hint_en`
- `common_mistake`
- `memory_hook` (optional)

Acceptance:

- each hint fits one line (roughly <= 80 chars)
- no copy-paste of long source definition

### 3.4 EN/JA translation QA

Goal:

- Flag possible mismatch between `cue_en_text` and `cue_ja_text`.

Output:

- `card_id` or cue identifier
- `qa_result` (`ok|check`)
- `reason`
- `suggested_ja` (only if `check`)

Acceptance:

- only high-suspicion cases should be marked `check`

### 3.5 Session curation (optional)

Goal:

- Build a focused study queue (for example: weak + recent + frequent).

Output:

- ranked list with short reason per row

## 4. Merge / Reuse Policy

When re-importing enriched outputs:

- default: `--on-duplicate upsert`
- use `skip` for conservative runs
- use `error` only in strict QA pipelines

Recommended import command:

```bash
python3 scripts/substudy.py dict-bookmarks-import \
  --input exports/llm/enriched_missing.jsonl \
  --on-duplicate upsert
```

Import contract notes:

- Re-import schema is stricter than LLM-facing schema.
- At minimum keep these identifiers unchanged: `source_id`, `video_id`, `track`, `cue_start_ms`, `cue_end_ms`.
- For known dictionary entries: include positive integer `dict_entry_id`.
- For missing-entry enrichment: set `missing_entry=1`; `dict_entry_id` can be omitted (auto-generated).

## 5. Prompt Rules (for any LLM agent)

- Prefer cue-context accuracy over dictionary completeness.
- Do not hallucinate: if uncertain, output low confidence and explain ambiguity.
- Keep output machine-parseable JSONL (one JSON object per line).
- Preserve original identifiers (`source_id`, `video_id`, `cue_start_ms`) unchanged.
- Avoid overly long prose; optimize for quick review in UI.

## 6. Quality Checklist

Before import:

1. JSONL parse passes for all lines.
2. Required IDs are present for every row.
3. `definition`/`definition_refined` is non-empty.
4. Spot-check 10 samples against actual cue context.
5. Duplicate keys merge as expected with selected policy.
6. Run import validation in dry-run mode and ensure `errors=0`:

```bash
python3 scripts/substudy.py dict-bookmarks-import \
  --input exports/llm/enriched_missing.jsonl \
  --on-duplicate upsert \
  --dry-run
```

## 7. Suggested Next Milestone

After this document is operational:

1. Automate `missing_review -> LLM -> import` as one script wrapper.
2. Add "unread LLM enrichment" counter to existing notification flow.
3. Expose enriched fields directly in hover dictionary popup/reel card.
