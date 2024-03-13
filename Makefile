SHELL := /bin/bash

.PHONY: up down containers secrets start stop delete dashboard status clean

up: start containers secrets
	@minikube kubectl -- apply -f deploy.yaml

down:
	@minikube kubectl -- delete -f deploy.yaml > /dev/null 2>&1 || true

containers:
	set -Eeuo pipefail && \
	eval $$(minikube docker-env) && \
	for target in images/*; do \
		docker build -t "audio-scraper-$$(basename "$$target")" "$$target"; \
	done

secrets:
	@minikube kubectl -- delete secret env-secrets > /dev/null 2>&1 || true
	@minikube kubectl -- create secret generic env-secrets --from-env-file=secrets.env

start:
	@minikube start --driver=docker --container-runtime=docker --gpus all \
		--memory 4096 --cpus 2 --disk-size=50g \
		--mount --mount-string "$$(pwd)/data:/hostdata"
	@minikube addons enable nvidia-gpu-device-plugin
	@minikube addons enable metrics-server

stop:
	@minikube stop

delete:
	@minikube delete

dashboard:
	@minikube dashboard --url=true

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
