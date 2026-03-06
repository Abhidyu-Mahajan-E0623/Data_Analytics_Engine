VENV := .venv

ifeq ($(OS),Windows_NT)
PYTHON := $(VENV)/Scripts/python.exe
PIP := $(VENV)/Scripts/pip.exe
else
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
endif

LATEST_RUN_CMD := $(PYTHON) -c "from pathlib import Path; p=Path('Output/hypotheses'); runs=sorted([d.name for d in p.iterdir() if d.is_dir()]) if p.exists() else []; print(runs[-1] if runs else '')"

.PHONY: venv install run evaluate lint test

venv:
	python -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip

install:
	$(PIP) install -r requirements.txt

run:
	@if [ -z "$(DOMAIN)" ]; then echo "Usage: make run DOMAIN=sales"; exit 1; fi
	$(PYTHON) -m src.cli generate --domain $(DOMAIN)
	$(PYTHON) -m src.cli create-monitoring-tables
	@RUN_ID=`$(LATEST_RUN_CMD)`; \
	if [ -z "$$RUN_ID" ]; then echo "No run_id found in Output/hypotheses"; exit 1; fi; \
	echo "Publishing run_id=$$RUN_ID"; \
	$(PYTHON) -m src.cli publish --run-id $$RUN_ID

evaluate:
	@if [ -z "$(RUN_ID)" ]; then echo "Usage: make evaluate RUN_ID=<id>"; exit 1; fi
	$(PYTHON) -m src.cli evaluate --run-id $(RUN_ID)

lint:
	$(PYTHON) -m compileall src tests

test:
	$(PYTHON) -m pytest -q
