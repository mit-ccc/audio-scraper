SHELL := /bin/bash

.PHONY: up containers secrets down start stop delete dashboard status clean

#
# Running our app
#

up: containers secrets
	@minikube kubectl -- apply -f deploy.yaml

containers:
	set -Eeuo pipefail && \
	eval $$(minikube docker-env) && \
	for target in images/*; do \
		docker build -t "audio-scraper-$$(basename "$$target")" "$$target"; \
	done

secrets:
	@minikube kubectl -- delete secret env-secrets || true
	@minikube kubectl -- create secret generic env-secrets --from-env-file=secrets.env

down:
	@minikube kubectl -- delete -f deploy.yaml || true
	@minikube kubectl -- delete pod --all || true
	@minikube kubectl -- delete pvc --all || true
	@minikube kubectl -- delete pv --all || true

#
# Cluster management
#

# adjust these memory and cpu values to suit how many replicas you're running
start:
	@minikube start --driver=docker --container-runtime=docker \
		--gpus=all --memory=35328 --cpus=18 --disk-size=50g \
		--mount --mount-string="$$(pwd)/data:/hostdata"
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

#
# Misc
#

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
