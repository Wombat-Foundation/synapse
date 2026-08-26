SHELL=/bin/bash
.DEFAULT_GOAL=_help

# [ENUM] Styling / Colors
STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || echo -e "\033[36m")
STYLE_RESET := $(shell tput sgr0 2>/dev/null || echo -e "\033[0m")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Linting, formatting
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: format
format: ##H Format with ruff
	ruff format .
	ruff check --fix .

.PHONY: lint
lint: ##H Lint the code with mypy
	uv run mypy


p ?=

.PHONY: test
test: ##H Run tests, e.g., on tests/storage/
	test "${p}"
	uv run trial $(p)

.PHONY: build
build: ##H Build the package (requires hatch)
	$(VENV)/bin/pip install hatch
	$(VENV)/bin/hatch build

.PHONY: publish
publish: build ##H Upload the package to PyPI using twine
	$(VENV)/bin/pip install twine
	$(VENV)/bin/twine upload dist/*

.PHONY: clean
clean: ##H Clean the virtual environment and caches
	#rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf .mypy_cache

.PHONY: _help
_help: ##H Show this help, list available targets
	@grep -hE '^[a-zA-Z0-9_\/-]+:[[:space:]]*##H .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":[[:space:]]*##H "}; {printf "$(STYLE_CYAN)%-15s$(STYLE_RESET) %s\n", $$1, $$2}'
