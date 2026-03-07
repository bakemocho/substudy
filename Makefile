PYTHON ?= python3
CONFIG ?= config/sources.toml
LEDGER_DB ?=
TRANSLATE_TARGET_LANG ?= ja-local
TRANSLATE_SOURCE_TRACK ?= auto
TRANSLATE_LIMIT ?= 20
TRANSLATE_TIMEOUT ?= 300
QUEUE_REQUEUE_ARGS ?=
QUEUE_RECOVER_KNOWN_ARGS ?=
LEDGER_DB_ARG := $(if $(strip $(LEDGER_DB)),--ledger-db $(LEDGER_DB),)

.PHONY: sync sync-dry backfill backfill-dry ledger ledger-full ledger-inc asr asr-dry downloads queue-status queue-requeue queue-recover-known loudness dict-index translate-local translate-local-all daily daily-source privacy-check test

sync:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG)

sync-dry:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG) --dry-run

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
	$(PYTHON) scripts/substudy.py queue-status --config $(CONFIG) $(LEDGER_DB_ARG)

queue-requeue:
	$(PYTHON) scripts/substudy.py queue-requeue --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_REQUEUE_ARGS)

queue-recover-known:
	$(PYTHON) scripts/substudy.py queue-recover-known --config $(CONFIG) $(LEDGER_DB_ARG) $(QUEUE_RECOVER_KNOWN_ARGS)

loudness:
	$(PYTHON) scripts/substudy.py loudness --config $(CONFIG)

dict-index:
	$(PYTHON) scripts/substudy.py dict-index --config $(CONFIG)

translate-local:
	$(PYTHON) scripts/substudy.py translate-local --config $(CONFIG) $(LEDGER_DB_ARG) --target-lang $(TRANSLATE_TARGET_LANG) --source-track $(TRANSLATE_SOURCE_TRACK) --limit $(TRANSLATE_LIMIT) --timeout-sec $(TRANSLATE_TIMEOUT)

translate-local-all:
	$(PYTHON) scripts/substudy.py translate-local --config $(CONFIG) $(LEDGER_DB_ARG) --target-lang $(TRANSLATE_TARGET_LANG) --source-track $(TRANSLATE_SOURCE_TRACK) --limit 0 --timeout-sec $(TRANSLATE_TIMEOUT)

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
