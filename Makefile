SHELL := /bin/bash

.PHONY: up containers secrets start stop delete dashboard status clean

up: start containers secrets
	@minikube kubectl -- apply -f deploy.yaml

containers:
	set -Eeuo pipefail && \
	eval $$(minikube docker-env) && \
	for target in images/*; do \
		docker build -t "audio-scraper-$$(basename "$$target")" "$$target"; \
	done

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
