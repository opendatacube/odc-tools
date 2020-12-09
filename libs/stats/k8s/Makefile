k8=kubectl -n processing
id=stats-scratch
labport=8899
define HELP
make apply|start|stop|info|logs|exec|clean

Typical workflow is:

  >  make start      # scale cluster to 1 replicas
  >  make exec       # exec into and run bash
  >  #.. do stuff .. #
  >  make stop       # scale cluster down to 0 replicas

If making changes to deployment:

  >  make apply
  >  make start      # default replicas is 0, so need to start
endef
export HELP

help:
	@echo "$$HELP"

apply:
	$(k8) apply -f $(id).yaml

start:
	$(k8) scale --replicas=1 deployments/$(id)

stop:
	$(k8) scale --replicas=0 deployments/$(id)

info:
	$(k8) get "deployments/$(id)"
	$(k8) get pods -l "app=$(id)"

describe:
	$(k8) describe pods -l "app=$(id)"

echo-pod:
	@echo "$$($(k8) get pods -l app=$(id) | awk 'FNR==2 {print $$1}')"

exec-lab:
	$(k8) exec -ti "$$($(k8) get pods -l app=$(id) | awk 'FNR==2 {print $$1}')" -- jupyter-lab --port=$(labport) --ip=0.0.0.0 --no-browser
fwd-lab:
	$(k8) port-forward "pod/$$($(k8) get pods -l app=$(id) | awk 'FNR==2 {print $$1}')" $(labport):$(labport)

exec:
	$(k8) exec -ti "$$($(k8) get pods -l app=$(id) | awk 'FNR==2 {print $$1}')" -- bash

logs:
	$(k8) logs -f "$$($(k8) get pods -l app=$(id) | awk 'FNR==2 {print $$1}')"

clean:
	$(k8) delete -f $(id).yaml


PHONY: help start stop info apply clean exec echo-pod
