.PHONY: build-package

COLOUR_GREEN=\033[0;32m
COLOUR_RED=\033[0;31m
COLOUR_BLUE=\033[0;34m
END_COLOUR=\033[0m

build-package:
	@echo "$(COLOUR_GREEN)Building package ... $(END_COLOUR)"
	@uv build
	@echo "$(COLOUR_GREEN)Finished building package... $(END_COLOUR)"

# Note that this target uses the Trusted Publishers feature of PyPi and Gitub Actions. See https://docs.pypi.org/trusted-publishers/adding-a-publisher/
publish-package:
	@echo "$(COLOUR_GREEN)Publishing package... $(END_COLOUR)"
	@uv publish
	@echo "$(COLOUR_GREEN)Finished publishing package... $(END_COLOUR)"

start-devcontainer:
	devcontainer up --workspace-folder .

devcontainer-shell:
	containerid=`docker ps | grep brickops | awk '{print $$1; exit}'` && \
	echo "bash into container $$containerid" && \
	docker exec -w /workspaces/brickops/ -it "$$containerid" bash

precommit-checks:
	uv run pre-commit run --all-files

ruff:
	uv run ruff check --output-format=github .

mypy:
	uv run mypy --strict .
