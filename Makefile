PYTHON ?= python

.PHONY: lint typecheck test notebooks docs package schemas smoke-canonical perf-check clean

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

test:
	pytest -q

notebooks:
	pytest tests/test_notebook.py -q

docs:
	mkdocs build --strict

package:
	$(PYTHON) -m build

schemas:
	$(PYTHON) scripts/generate_schemas.py

smoke-canonical:
	$(PYTHON) scripts/canonical_pipeline.py --config examples/configs/canonical/belgium_full_stack.yaml

perf-check:
	$(PYTHON) scripts/check_perf_baseline.py --baseline tests/perf_baselines/canonical_pipeline.json --config examples/configs/canonical/belgium_full_stack.yaml

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov build dist site
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	find . -name ".DS_Store" -type f -delete
	rm -rf src/*.egg-info tmp/*
