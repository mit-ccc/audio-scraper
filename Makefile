SHELL := /bin/bash

.PHONY: up down deploy

up: down
	set -a && source conf.env && docker compose up --build \
		--scale transcribe="$$N_TRANSCRIBE"

down: conf.env docker-compose.yml
	docker compose down
	docker compose rm -fsv

deploy: conf.env deploy/infra.yml
	set -a && source conf.env && ./deploy/containers.sh \
		&& ./deploy/ensure-secret.sh \
		&& ./deploy/infra.sh
