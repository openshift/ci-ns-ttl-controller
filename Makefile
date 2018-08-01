build:
	go build ./cmd/...
.PHONY: build

test:
	go test ./...
.PHONY: test

deploy: set-namespace deploy-ci-ns-ttl-controller
.PHONY: deploy

set-namespace:
	if [[ "$(shell oc project -q )" != "ci" ]]; then oc new-project ci; fi
.PHONY: set-namespace

deploy-ci-ns-ttl-controller: deploy-ci-ns-ttl-controller-build deploy-ci-ns-ttl-controller-infra
.PHONY: deploy-ci-ns-ttl-controller

deploy-ci-ns-ttl-controller-build:
	oc apply -f deploy/controller-build.yaml
.PHONY: deploy-ci-ns-ttl-controller-build

deploy-ci-ns-ttl-controller-infra:
	oc apply -f deploy/controller.yaml
	oc apply -f deploy/controller-rbac.yaml
.PHONY: deploy-ci-ns-ttl-controller-infra