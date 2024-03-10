SHELL := /bin/bash

.PHONY: up down deploy clean

up: down
	docker compose up --build --remove-orphans --scale transcribe=1

down: conf.env docker-compose.yml
	docker compose down
	docker compose rm -fsv

deploy: conf.env deploy/infra.yml
	set -a && source conf.env && ./deploy/containers.sh \
		&& ./deploy/ensure-secret.sh \
		&& ./deploy/infra.sh

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
