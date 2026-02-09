#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from substudy import lookup_dictionary_entries, normalize_dictionary_term


DICT_CONTEXT_MAX_CANDIDATES = 9
DICT_CONTEXT_PER_TERM_LIMIT = 4
DICT_CONTEXT_TOTAL_LIMIT = 12
DICT_CONTEXT_CORE_MIN_RESULTS = 2
DICT_COLLAPSE_PARTICLE_WORDS = {
    "back",
    "up",
    "out",
    "off",
    "on",
    "in",
    "away",
    "down",
    "over",
    "around",
    "through",
    "apart",
    "along",
    "across",
    "by",
    "about",
    "into",
}
DICT_IRREGULAR_BASE_FORMS = {
    "made": "make",
    "gone": "go",
    "went": "go",
    "gave": "give",
    "given": "give",
    "took": "take",
    "taken": "take",
    "came": "come",
    "did": "do",
    "done": "do",
    "was": "be",
    "were": "be",
    "been": "be",
    "saw": "see",
    "seen": "see",
}
WORD_PATTERN = re.compile(r"[A-Za-z]+(?:['’][A-Za-z]+)*")


def split_normalized_words(value: str) -> list[str]:
    normalized = normalize_dictionary_term(value)
    if not normalized:
        return []
    return [part for part in normalized.split(" ") if part]


def derive_core_terms(base_term: str) -> list[str]:
    normalized_base = normalize_dictionary_term(base_term)
    if not normalized_base:
        return []
    terms: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        normalized = normalize_dictionary_term(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        terms.append(normalized)

    add(normalized_base)
    add(DICT_IRREGULAR_BASE_FORMS.get(normalized_base, ""))

    if normalized_base.endswith("'s"):
        add(normalized_base[:-2])
    if normalized_base.endswith("ies") and len(normalized_base) > 4:
        add(normalized_base[:-3] + "y")
    if normalized_base.endswith("ing") and len(normalized_base) > 5:
        stem = normalized_base[:-3]
        add(stem)
        add(stem + "e")
    if normalized_base.endswith("ed") and len(normalized_base) > 4:
        stem = normalized_base[:-2]
        add(stem)
        add(stem + "e")
    if normalized_base.endswith("es") and len(normalized_base) > 4:
        add(normalized_base[:-2])
    if normalized_base.endswith("s") and len(normalized_base) > 3:
        add(normalized_base[:-1])
    return terms[:3]


def build_lookup_terms(words: list[str], hover_index: int, base_term: str) -> tuple[list[str], list[str], set[str]]:
    terms: list[str] = []
    seen: set[str] = set()
    core_terms = derive_core_terms(base_term)

    def add_term(value: str) -> None:
        normalized = normalize_dictionary_term(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        terms.append(normalized)

    def add_range(start_index: int, end_index: int) -> None:
        if start_index < 0 or end_index < start_index or end_index >= len(words):
            return
        phrase = " ".join(part.strip() for part in words[start_index : end_index + 1] if part.strip())
        add_term(phrase)

    def word_at(word_index: int) -> str:
        if word_index < 0 or word_index >= len(words):
            return ""
        return normalize_dictionary_term(words[word_index])

    add_range(hover_index, hover_index + 1)
    add_range(hover_index, hover_index + 2)
    add_range(hover_index, hover_index + 3)

    head = word_at(hover_index)
    if head:
        max_tail = min(len(words) - 1, hover_index + 4)
        for tail_index in range(hover_index + 2, max_tail + 1):
            tail = word_at(tail_index)
            if tail and tail in DICT_COLLAPSE_PARTICLE_WORDS:
                add_term(f"{head} {tail}")

    tail = word_at(hover_index)
    if tail in DICT_COLLAPSE_PARTICLE_WORDS:
        min_head = max(0, hover_index - 4)
        for head_index in range(hover_index - 1, min_head - 1, -1):
            candidate_head = word_at(head_index)
            if candidate_head and candidate_head not in DICT_COLLAPSE_PARTICLE_WORDS:
                add_term(f"{candidate_head} {tail}")
                break

    for core_term in core_terms:
        add_term(core_term)

    lookup_terms = terms
    if len(lookup_terms) > DICT_CONTEXT_MAX_CANDIDATES:
        lookup_terms = lookup_terms[:DICT_CONTEXT_MAX_CANDIDATES]
        replace_index = len(lookup_terms) - 1
        for core_term in core_terms:
            if core_term in lookup_terms:
                continue
            if replace_index < 0:
                break
            lookup_terms[replace_index] = core_term
            replace_index -= 1

    context_word_set: set[str] = set()
    start_index = max(0, hover_index - 3)
    end_index = min(len(words) - 1, hover_index + 5)
    for word_index in range(start_index, end_index + 1):
        for token in split_normalized_words(words[word_index]):
            context_word_set.add(token)
    for core_term in core_terms:
        for token in split_normalized_words(core_term):
            context_word_set.add(token)
    return lookup_terms, core_terms, context_word_set


def result_key(row: dict[str, Any]) -> str:
    row_id = int(row.get("id", 0) or 0)
    if row_id > 0:
        return f"id:{row_id}"
    return "\u0000".join(
        [
            str(row.get("source_name", "")),
            str(row.get("term_norm", "")),
            str(row.get("definition", "")),
        ]
    )


@dataclass
class Candidate:
    score: int
    is_core_entry: bool
    lookup_term: str
    row: dict[str, Any]
    key: str


def score_candidate(row: dict[str, Any], lookup_term: str, context_word_set: set[str], core_term_set: set[str]) -> dict[str, Any]:
    lookup_normalized = normalize_dictionary_term(lookup_term)
    row_term_normalized = normalize_dictionary_term(str(row.get("term_norm") or row.get("term") or ""))
    lookup_words = split_normalized_words(lookup_normalized)
    row_words = split_normalized_words(row_term_normalized)
    overlap_count = sum(1 for word in row_words if word in context_word_set)

    is_core_lookup = lookup_normalized in core_term_set
    is_core_entry = is_core_lookup and row_term_normalized == lookup_normalized
    is_exact = bool(lookup_normalized and row_term_normalized == lookup_normalized)
    is_prefix = bool((not is_exact) and lookup_normalized and row_term_normalized.startswith(lookup_normalized + " "))

    score = 0
    score += overlap_count * 100
    score += len(lookup_words) * 18
    score += len(row_words) * 3
    if is_exact:
        score += 36
    if is_prefix:
        score += 14
    if is_core_lookup:
        score += 10
    if (not is_core_lookup) and len(lookup_words) <= 2 and overlap_count <= 2:
        score -= 40
    return {
        "score": score,
        "is_core_entry": is_core_entry,
        "is_core_lookup": is_core_lookup,
        "lookup_normalized": lookup_normalized,
        "row_term_normalized": row_term_normalized,
        "overlap_count": overlap_count,
    }


def emulate(connection: sqlite3.Connection, sentence: str, hover_word: str, hover_index: int | None) -> dict[str, Any]:
    words = WORD_PATTERN.findall(sentence)
    if not words:
        raise ValueError("No words found in sentence.")
    if hover_index is None:
        normalized_hover = normalize_dictionary_term(hover_word)
        hover_index = -1
        for index, word in enumerate(words):
            if normalize_dictionary_term(word) == normalized_hover:
                hover_index = index
                break
        if hover_index < 0:
            raise ValueError(f"Word not found in sentence: {hover_word}")
    if hover_index < 0 or hover_index >= len(words):
        raise ValueError(f"hover_index is out of range: {hover_index}")

    base_term = words[hover_index]
    lookup_terms, core_terms, context_word_set = build_lookup_terms(words, hover_index, base_term)
    core_term_set = set(core_terms)
    base_normalized = core_terms[0] if core_terms else normalize_dictionary_term(base_term)

    payload_by_term: dict[str, dict[str, Any]] = {}
    for term in lookup_terms:
        exact_only = len(split_normalized_words(term)) < 3 and term != base_normalized
        payload_by_term[term] = lookup_dictionary_entries(
            connection,
            term,
            limit=DICT_CONTEXT_PER_TERM_LIMIT,
            exact_only=exact_only,
        )

    candidates: list[Candidate] = []
    seen_keys: set[str] = set()
    for lookup_term in lookup_terms:
        payload = payload_by_term.get(lookup_term, {})
        rows = payload.get("results", [])
        inserted = 0
        for row in rows:
            key = result_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            scored = score_candidate(row, lookup_term, context_word_set, core_term_set)
            if (
                scored["is_core_lookup"]
                and (not scored["is_core_entry"])
                and len(split_normalized_words(scored["row_term_normalized"])) <= 1
                and scored["row_term_normalized"] not in core_term_set
                and scored["overlap_count"] <= 0
            ):
                continue
            candidates.append(
                Candidate(
                    score=scored["score"],
                    is_core_entry=scored["is_core_entry"],
                    lookup_term=lookup_term,
                    row=row,
                    key=key,
                )
            )
            inserted += 1
            if inserted >= DICT_CONTEXT_PER_TERM_LIMIT:
                break

    candidates.sort(key=lambda candidate: candidate.score, reverse=True)
    selected = candidates[:DICT_CONTEXT_TOTAL_LIMIT]
    selected_core_count = sum(1 for candidate in selected if candidate.is_core_entry)
    if selected_core_count < DICT_CONTEXT_CORE_MIN_RESULTS:
        selected_keys = {candidate.key for candidate in selected}
        for candidate in candidates:
            if not candidate.is_core_entry or candidate.key in selected_keys:
                continue
            replace_index = len(selected) - 1
            while replace_index >= 0 and selected[replace_index].is_core_entry:
                replace_index -= 1
            if replace_index < 0:
                break
            selected_keys.remove(selected[replace_index].key)
            selected[replace_index] = candidate
            selected_keys.add(candidate.key)
            selected_core_count += 1
            if selected_core_count >= DICT_CONTEXT_CORE_MIN_RESULTS:
                break

    selected.sort(key=lambda candidate: candidate.score, reverse=True)

    grouped: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for candidate in selected:
        row = candidate.row
        key = normalize_dictionary_term(str(row.get("term_norm") or row.get("term") or "")) or candidate.key
        if key not in grouped:
            grouped[key] = {
                "term": str(row.get("term") or row.get("term_norm") or ""),
                "items": [],
                "_seen": set(),
            }
            ordered_keys.append(key)
        definition = str(row.get("definition") or "").strip()
        from_term = str(candidate.lookup_term)
        entry_key = normalize_dictionary_term(from_term) + "\u0000" + definition
        if entry_key in grouped[key]["_seen"]:
            continue
        grouped[key]["_seen"].add(entry_key)
        grouped[key]["items"].append({"from": from_term, "definition": definition, "score": candidate.score})

    groups = []
    for key in ordered_keys:
        group = grouped[key]
        group.pop("_seen", None)
        groups.append(group)

    return {
        "words": words,
        "hover_index": hover_index,
        "hover_word": base_term,
        "core_terms": core_terms,
        "lookup_terms": lookup_terms,
        "groups": groups,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emulate subtitle hover dictionary ranking.")
    parser.add_argument("--db", default="data/master_ledger.sqlite", help="Path to SQLite DB.")
    parser.add_argument("--sentence", required=True, help="Subtitle sentence.")
    parser.add_argument("--word", required=True, help="Hovered word.")
    parser.add_argument("--index", type=int, default=None, help="Hovered word index within sentence words.")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        result = emulate(connection, args.sentence, args.word, args.index)
    finally:
        connection.close()

    print(f"hover_word={result['hover_word']} hover_index={result['hover_index']}")
    print(f"core_terms={result['core_terms']}")
    print("lookup_terms:")
    for term in result["lookup_terms"]:
        print(f"  - {term}")

    print("\nranked_groups:")
    for group in result["groups"]:
        print(f"* {group['term']}")
        for item in group["items"]:
            print(f"    from: {item['from']}")
            print(f"    def : {item['definition']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
