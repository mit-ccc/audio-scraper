SHELL := /bin/bash

.PHONY: up down deploy clean

up: containers down
	docker compose up --remove-orphans --scale transcribe=1

containers:
	set -Eeuo pipefail && \
	for target in $$(find images/ -depth 1 -type d -exec basename {} \;); do \
		docker build -t "audio-scraper-$$target" "images/$$target"; \
	done

down: conf.env docker-compose.yml
	docker compose down
	docker compose rm -fsv

clean:
	find . -name '*.pyc'       -not -path '*/\.git/*' -exec rm -f {} \+
	find . -name '*.pyo'       -not -path '*/\.git/*' -exec rm -f {} \+
	find . -name '*~'          -not -path '*/\.git/*' -exec rm -f {} \+
	find . -name tags          -not -path '*/\.git/*' -exec rm -f {} \+
	find . -name tags.lock     -not -path '*/\.git/*' -exec rm -f {} \+
	find . -name '*.egg-info'  -not -path '*/\.git/*' -exec rm -rf {} \+
	find . -name env -type d   -not -path '*/\.git/*' -exec rm -rf {} \+
	find . -name '__pycache__' -not -path '*/\.git/*' -exec rm -rf {} \+
	find . -name '.mypy_cache' -not -path '*/\.git/*' -exec rm -rf {} \+

deploy: conf.env deploy/infra.yml
	set -a && source conf.env && ./deploy/containers.sh \
		&& ./deploy/ensure-secret.sh \
		&& ./deploy/infra.sh
