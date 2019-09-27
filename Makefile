develop:
	pip install -e ".[test,doc]"
.PHONY: develop

tag:
	@git tag v`python setup.py --version`
.PHONY: tag

changelog:
	@git log --no-merges v`python setup.py --version`.. --pretty="format:  * %s"
.PHONY: changelog

clean:
	rm -rf build dist *.egg-info
	find . -type d -name "__pycache__" | xargs rm -rf
	find . -name "*.pyc" -delete
.PHONY: clean

lint:
	mkdir -p .build
	pylint --rcfile tools/pylint/pylintrc huskar_sdk_v2 | tee .build/pylint.out
.PHONY: lint

test: clean lint
	tox
.PHONY: test

hooks:
	ln -sf ../../tools/git-hooks/pre-commit .git/hooks/pre-commit
.PHONY: hooks
