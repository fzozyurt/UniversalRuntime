.PHONY: install quality test compatibility contracts

install:
	python -m pip install -e '.[dev,langgraph,fastapi,a2a]'

quality:
	ruff check .
	ruff format --check .
	mypy src services
	python scripts/check_architecture.py
	python scripts/validate_contracts.py

test:
	pytest -q --cov=src --cov=services --cov-report=term-missing --cov-fail-under=80

compatibility:
	pytest -q -m compatibility tests/compatibility

contracts:
	python scripts/validate_contracts.py
