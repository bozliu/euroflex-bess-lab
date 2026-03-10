PYTHON ?= python

.PHONY: lint typecheck test notebooks docs package schemas smoke-canonical perf-check demo-gif clean sanitize-runtime

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

test: sanitize-runtime
	pytest -q --ignore=tests/test_repo_hygiene.py
	$(MAKE) sanitize-runtime
	pytest -q tests/test_repo_hygiene.py

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

demo-gif:
	$(PYTHON) scripts/render_demo_gif.py

sanitize-runtime:
	find . -name ".DS_Store" -type f -delete
	rm -rf tmp
	find artifacts -mindepth 1 ! -name ".gitkeep" -exec rm -rf {} +

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov build dist site
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	find . -name ".DS_Store" -type f -delete
	rm -rf src/*.egg-info tmp
	find artifacts -mindepth 1 ! -name ".gitkeep" -exec rm -rf {} +
