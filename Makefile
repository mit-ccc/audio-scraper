SHELL := /bin/bash

.PHONY: up containers secrets start stop delete dashboard status clean

up: containers secrets
	@minikube kubectl -- apply -f deploy.yaml

containers: start
	@set -Eeuo pipefail && \
	eval $$(minikube docker-env) && \
	while IFS= read -r -d '' target \
	do \
		docker build -t "audio-scraper-$$target" "images/$$target"; \
	done <  <(find images/ -depth 1 -type d -exec basename {} \;)

secrets:
	@minikube kubectl -- delete secret env-secrets 2>&1 1>/dev/null || true
	@minikube kubectl -- create secret generic env-secrets --from-env-file=secrets.env

start:
	@minikube start --driver=docker --addons=nvidia-gpu-device-plugin \
		--memory 4096 --cpus 2 --disk-size=50g
	@minikube addons enable metrics-server

stop:
	@minikube stop

delete:
	@minikube delete

dashboard:
	@minikube dashboard

status:
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
