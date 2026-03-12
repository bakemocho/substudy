PYTHON ?= python3
CONFIG ?= config/sources.toml
LEDGER_DB ?=
TRANSLATE_TARGET_LANG ?= ja-local
TRANSLATE_SOURCE_TRACK ?= auto
TRANSLATE_LIMIT ?= 20
TRANSLATE_TIMEOUT ?= 300
QUEUE_REQUEUE_ARGS ?=
QUEUE_RECOVER_KNOWN_ARGS ?=
QUEUE_STATUS_ARGS ?=
UPSTREAM_JA_SUB_LANGS ?= ja.*,ja,jp.*,jpn.*
YTDLP_UPDATE_MODE ?= auto
YTDLP_UPDATE_TRIGGER ?= manual
YTDLP_UPDATE_CURL_CFFI ?= 1
LEDGER_DB_ARG := $(if $(strip $(LEDGER_DB)),--ledger-db $(LEDGER_DB),)
SYNC_LIMIT_ARG := $(if $(strip $(LIMIT)),--limit $(LIMIT),)
SYNC_BASE_CMD = $(PYTHON) scripts/substudy.py sync --config $(CONFIG) $(LEDGER_DB_ARG) $(SYNC_LIMIT_ARG)
SYNC_META_ARGS = --skip-media --skip-subs
SYNC_SUBS_ARGS = --skip-media --skip-meta
SYNC_SUBS_JA_ARGS = $(SYNC_SUBS_ARGS) --upstream-sub-langs-override "$(UPSTREAM_JA_SUB_LANGS)"
YTDLP_UPDATE_CURL_ARG := $(if $(filter 0 false off,$(strip $(YTDLP_UPDATE_CURL_CFFI))),--no-uv-with-curl-cffi,--uv-with-curl-cffi)

define require-source
	@if [ -z "$(SOURCE)" ]; then \
		echo "error: SOURCE is required (usage: make $(1) SOURCE=<source_id> [LIMIT=<n>])" >&2; \
		exit 1; \
	fi
endef

.PHONY: init-local sync sync-dry sync-meta-only sync-meta-missing sync-meta-source sync-subs-missing sync-subs-ja-missing sync-subs-source sync-subs-ja-source backfill backfill-dry ledger ledger-full ledger-inc asr asr-dry downloads queue-status queue-status-unresolved queue-requeue queue-recover-known queue-recover-known-dry queue-heal loudness dict-index translate-local translate-local-all ytdlp-update daily daily-source privacy-check test

init-local:
	./scripts/init_local.sh

sync:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG)

sync-dry:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG) --dry-run

sync-meta-only:
	$(SYNC_BASE_CMD) $(SYNC_META_ARGS)

sync-meta-missing: sync-meta-only

sync-meta-source:
	$(call require-source,sync-meta-source)
	$(SYNC_BASE_CMD) --source "$(SOURCE)" $(SYNC_META_ARGS)

sync-subs-missing:
	$(SYNC_BASE_CMD) $(SYNC_SUBS_ARGS)

sync-subs-ja-missing:
	$(SYNC_BASE_CMD) $(SYNC_SUBS_JA_ARGS)

sync-subs-source:
	$(call require-source,sync-subs-source)
	$(SYNC_BASE_CMD) --source "$(SOURCE)" $(SYNC_SUBS_ARGS)

sync-subs-ja-source:
	$(call require-source,sync-subs-ja-source)
	$(SYNC_BASE_CMD) --source "$(SOURCE)" $(SYNC_SUBS_JA_ARGS)

ledger:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG) $(LEDGER_DB_ARG)

ledger-full:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG) $(LEDGER_DB_ARG)

ledger-inc:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG) $(LEDGER_DB_ARG) --incremental

backfill:
	$(PYTHON) scripts/substudy.py backfill --config $(CONFIG)

backfill-dry:
	$(PYTHON) scripts/substudy.py backfill --config $(CONFIG) --dry-run

asr:
	$(PYTHON) scripts/substudy.py asr --config $(CONFIG)

asr-dry:
	$(PYTHON) scripts/substudy.py asr --config $(CONFIG) --dry-run

downloads:
	$(PYTHON) scripts/substudy.py downloads --config $(CONFIG)

queue-status:
	$(PYTHON) scripts/substudy.py queue-status --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_STATUS_ARGS)

queue-status-unresolved:
	$(PYTHON) scripts/substudy.py queue-status --config $(CONFIG) $(LEDGER_DB_ARG) --only-unresolved $(QUEUE_STATUS_ARGS)

queue-requeue:
	$(PYTHON) scripts/substudy.py queue-requeue --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_REQUEUE_ARGS)

queue-recover-known:
	$(PYTHON) scripts/substudy.py queue-recover-known --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_RECOVER_KNOWN_ARGS)

queue-recover-known-dry:
	$(PYTHON) scripts/substudy.py queue-recover-known --config $(CONFIG) $(LEDGER_DB_ARG) --dry-run $(QUEUE_RECOVER_KNOWN_ARGS)

queue-heal:
	@echo "1) recover known queue failures"
	$(PYTHON) scripts/substudy.py queue-recover-known --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_RECOVER_KNOWN_ARGS)
	@echo "2) show unresolved queue failures"
	$(PYTHON) scripts/substudy.py queue-status --config $(CONFIG) $(LEDGER_DB_ARG) --only-unresolved --limit 20 $(QUEUE_STATUS_ARGS)

loudness:
	$(PYTHON) scripts/substudy.py loudness --config $(CONFIG)

dict-index:
	$(PYTHON) scripts/substudy.py dict-index --config $(CONFIG)

translate-local:
	$(PYTHON) scripts/substudy.py translate-local --config $(CONFIG) $(LEDGER_DB_ARG) --target-lang $(TRANSLATE_TARGET_LANG) --source-track $(TRANSLATE_SOURCE_TRACK) --limit $(TRANSLATE_LIMIT) --timeout-sec $(TRANSLATE_TIMEOUT)

translate-local-all:
	$(PYTHON) scripts/substudy.py translate-local --config $(CONFIG) $(LEDGER_DB_ARG) --target-lang $(TRANSLATE_TARGET_LANG) --source-track $(TRANSLATE_SOURCE_TRACK) --limit 0 --timeout-sec $(TRANSLATE_TIMEOUT)

ytdlp-update:
	$(PYTHON) scripts/substudy.py ytdlp-update --config $(CONFIG) $(LEDGER_DB_ARG) --mode $(YTDLP_UPDATE_MODE) --trigger $(YTDLP_UPDATE_TRIGGER) $(YTDLP_UPDATE_CURL_ARG)

daily:
	./scripts/run_daily_sync.sh

daily-source:
	@if [ -z "$(SOURCE)" ]; then \
		echo "error: SOURCE is required (usage: make daily-source SOURCE=<source_id>)" >&2; \
		exit 1; \
	fi
	./scripts/run_daily_sync.sh --source "$(SOURCE)"

privacy-check:
	./scripts/privacy_check.sh

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py"
