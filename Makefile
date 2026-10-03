.DEFAULT_GOAL:=help

VENV_NAME:=.venv
REQS_PROD:=requirements.txt
SETUP_FILE:=pyproject.toml
SOURCES = src

DOCKER_DEV_SERVICE:=dev
DOCKER_DEV_NO_GCP_SERVICE:=dev_no_gcp
DOCKER_PROD_SERVICE:=prod
DOCKER_TEST_SERVICE:=test

GCP_PROJECT:=world-fishing-827
GCP_DOCKER_VOLUME:=gcp

PYTHON_VERSION:=3.12
UV_VERSION := 0.10.9

VENV:=uv venv
PIP:=uv pip
PIP_COMPILE:=uv pip compile

# ---------------------
# DOCKER
# ---------------------

.PHONY: docker-build  ## Builds docker image.
docker-build:
	docker compose build

.PHONY: docker-volume  ## Creates the docker volume for GCP.
docker-volume:
	docker volume create --name ${GCP_DOCKER_VOLUME}

.PHONY: docker-gcp ## gcp: Authenticates to google cloud and configure the project.
docker-gcp: docker-volume
	docker compose run gcloud auth application-default login
	docker compose run gcloud config set project ${GCP_PROJECT}
	docker compose run gcloud auth application-default set-quota-project ${GCP_PROJECT}

.PHONY: docker-test ## Runs tests using prod image, exporting coverage.xml report.
docker-test:
	docker compose run --rm ${DOCKER_TEST_SERVICE}

.PHONY: docker-shell ## Enters to docker container shell.
docker-shell: docker-volume
	docker compose run --rm -it ${DOCKER_DEV_SERVICE}

.PHONY: reqs  ## Compiles requirements.txt with pip-tools.
reqs:
	docker compose run --rm ${DOCKER_DEV_NO_GCP_SERVICE} -c \
		'${PIP_COMPILE} -o ${REQS_PROD} ${SETUP_FILE} -v'

.PHONY: reqs-upgrade  ## Upgrades requirements.txt with pip-tools.
reqs-upgrade:
	docker compose run --rm ${DOCKER_DEV_NO_GCP_SERVICE} -c \
		'${PIP_COMPILE} -o ${REQS_PROD} ${SETUP_FILE} -U -v'

# ---------------------
# VIRTUAL ENVIRONMENT
# ---------------------

.PHONY: uv  ## Installs UV
uv:
	curl -LsSf https://astral.sh/uv/install.sh | UV_VERSION=$(UV_VERSION) sh
	uv python pin ${PYTHON_VERSION}

.PHONY: venv  ## Creates virtual environment.
venv:
	${VENV} ${VENV_NAME}

.PHONY: upgrade-pip  ## Upgrades pip.
upgrade-pip:
	${PIP} install pip==25.2

.PHONY: install-test  ## Install and only test dependencies.
install-test: upgrade-pip
	${PIP} install -r requirements-test.txt

.PHONY: install  ## Install the package in editable mode & all dependencies for local development.
install: upgrade-pip
	${PIP} install -e .[lint,dev,build]
	make install-test

.PHONY: test  ## Run all unit tests exporting coverage.xml report.
test:
	python -m pytest -m "not integration" --cov-report term --cov-report=xml --cov=$(SOURCES)

# ---------------------
# QUALITY CHECKS
# ---------------------

.PHONY: hooks  ## Install and pre-commit hooks.
hooks:
	python -m pre_commit install --install-hooks
	python -m pre_commit install --hook-type commit-msg

.PHONY: format  ## Auto-format python source files according with PEP8.
format:
	python -m black $(SOURCES)
	python -m ruff check --fix $(SOURCES)
	python -m ruff format $(SOURCES)

.PHONY: lint  ## Lint python source files.
lint:
	python -m ruff check $(SOURCES)
	python -m ruff format --check $(SOURCES)
	python -m black $(SOURCES) --check --diff

.PHONY: codespell  ## Use Codespell to do spell checking.
codespell:
	python -m codespell_lib

.PHONY: typecheck  ## Perform type-checking.
typecheck:
	python -m mypy

.PHONY: audit  ## Use pip-audit to scan for known vulnerabilities.
audit:
	python -m pip_audit .

.PHONY: pre-commit  ## Run all pre-commit hooks.
pre-commit:
	python -m pre_commit run --all-files

.PHONY: all  ## Run the standard set of checks performed in CI.
all: lint codespell typecheck audit test

# ---------------------
# PACKAGE BUILD
# ---------------------


.PHONY: build  ## Build a source distribution and a wheel distribution.
build: all clean
	python -m build

.PHONY: publish  ## Publish the distribution to PyPI.
publish: build
	python -m twine upload dist/* --verbose

.PHONY: clean  ## Clear local caches and build artifacts.
clean:
	# remove Python file artifacts
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]'`
	rm -f `find . -type f -name '*~'`
	rm -f `find . -type f -name '.*~'`
	rm -rf .cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	# remove build artifacts
	rm -rf build
	rm -rf dist
	rm -rf `find . -name '*.egg-info'`
	rm -rf `find . -name '*.egg'`
	# remove test and coverage artifacts
	rm -rf .tox/
	rm -f .coverage
	rm -f .coverage.*
	rm -rf coverage.*
	rm -rf htmlcov/
	rm -rf .pytest_cache
	rm -rf htmlcov


# ---------------------
# HELP
# ---------------------

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
