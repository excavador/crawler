venv/bin/activate: requirements.txt
	@$(CURDIR)/scripts/venv_create

venv: venv/bin/activate

pep8:
	./scripts/venv pep8 *.py
