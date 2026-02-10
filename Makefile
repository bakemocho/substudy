PYTHON ?= python3
CONFIG ?= config/sources.toml

.PHONY: sync sync-dry backfill backfill-dry ledger ledger-inc asr asr-dry downloads loudness dict-index privacy-check

sync:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG)

sync-dry:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG) --dry-run

ledger:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG)

ledger-inc:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG) --incremental

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

loudness:
	$(PYTHON) scripts/substudy.py loudness --config $(CONFIG)

dict-index:
	$(PYTHON) scripts/substudy.py dict-index --config $(CONFIG)

privacy-check:
	./scripts/privacy_check.sh
