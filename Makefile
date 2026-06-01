fmt:
	isort openai_rq tests
	black openai_rq tests
	ruff check openai_rq tests --fix

install:
	pip install -e .[dev]
