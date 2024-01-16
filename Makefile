SHELL := /bin/bash

.PHONY: local stop-local start-local deploy containers secret infra

local: stop-local start-local

stop-local: conf.env docker-compose.yml
	docker-compose down
	docker-compose rm -fsv

start-local: conf.env docker-compose.yml
	set -a && source conf.env && docker-compose up -d --build \
		--scale transcribe="$$N_TRANSCRIBE"

deploy: containers secret infra

containers: conf.env
	set -a && source conf.env && ./bin/containers.sh

secret: conf.env
	set -a && source conf.env && ./bin/ensure-secret.sh

infra: conf.env aws-deploy/templates/infrastructure.yml
	set -a && source conf.env && ./bin/infra.sh
