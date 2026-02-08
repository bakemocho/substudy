PYTHON ?= python3
CONFIG ?= config/sources.toml

.PHONY: sync sync-dry ledger

sync:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG)

sync-dry:
	$(PYTHON) scripts/substudy.py sync --config $(CONFIG) --dry-run

ledger:
	$(PYTHON) scripts/substudy.py ledger --config $(CONFIG)

