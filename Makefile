SHELL=/bin/bash
.DEFAULT_GOAL=_help

# [ENUM] Styling / Colors
STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || echo -e "\033[36m")
STYLE_RESET := $(shell tput sgr0 2>/dev/null || echo -e "\033[0m")

# Default virtual environment
VENV=.venv
PYTHON=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

.PHONY: init
init: ##H Initialize .venv virtual dev env
	python3 -m venv $(VENV)
	-direnv allow

.PHONY: deps
deps: ##H Install standard and dev dependencies
	$(VENV)/bin/pip install -r requirements.txt -r requirements-dev.txt

.PHONY: install
install: ##H Install locally and setup systemd user service
	sudo mkdir -p /opt/matrix-premid
	sudo python3 -m venv /opt/matrix-premid/venv
	sudo /opt/matrix-premid/venv/bin/pip install .
	sudo ln -sf /opt/matrix-premid/venv/bin/matrix-premid /usr/local/bin/matrix-premid
	@echo "Setting up systemd service..."
	if [ -n "$$SUDO_USER" ]; then sudo -u "$$SUDO_USER" /usr/local/bin/matrix-premid install-service; else /usr/local/bin/matrix-premid install-service; fi

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Unit tests and local running
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: test
test:   ##H Run unit tests with coverage
	PYTHONPATH=src $(VENV)/bin/python -m pytest --cov=matrix_premid --cov-report=term-missing tests/

.PHONY: run
run:    ##H Run the application locally
	PYTHONPATH=src $(PYTHON) -m matrix_premid --debug

.PHONY: start
start: ##H Start the background systemd service
	systemctl --user start matrix-premid.service

.PHONY: restart
restart: ##H Restart the background systemd service
	systemctl --user restart matrix-premid.service

.PHONY: log
log:	##H Watch journalctl logs of installed/running service
	journalctl --user -fu matrix-premid

.PHONY: stop
stop: ##H Stop the background systemd service
	systemctl --user stop matrix-premid.service


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Linting, formatting
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


LINT_LOCS_PY = $$(git ls-files 'src/**/*.py' 'tests/*.py')

.PHONY: format
format: ##H Format the code using Black
	$(VENV)/bin/black src/ tests/
	$(VENV)/bin/isort src/ tests/
	-prettier -w .
	-pre-commit run --all-files


.PHONY: lint
lint: ##H Lint the code using Flake8
	flake8 src/
	flake8 --max-line-length=100 tests/
	pylint src/ tests/
	ruff check src/ tests/

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
