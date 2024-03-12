SHELL := /bin/bash

.PHONY: up down containers secrets start stop delete dashboard status clean

up: containers secrets
	@minikube kubectl apply -f manifest.yaml

down: stop

containers: start
	@set -Eeuo pipefail && \
	eval $$(minikube docker-env) && \
	for target in $$(find images/ -depth 1 -type d -exec basename {} \;); do \
		docker build -t "audio-scraper-$$target" "images/$$target"; \
	done

secrets: start
	@minikube kubectl delete secret env-secrets || true
	@minikube kubectl create secret generic env-secrets --from-env-file=secrets.env

start:
	@minikube start --driver=docker --memory 4096 --cpus 2

stop:
	@minikube stop

delete: start
	@minikube delete

dashboard: start
	@minikube dashboard

status: start
	@minikube status

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

# deploy: conf.env deploy/infra.yml
# 	set -a && source conf.env && ./deploy/containers.sh \
# 		&& ./deploy/ensure-secret.sh \
# 		&& ./deploy/infra.sh
