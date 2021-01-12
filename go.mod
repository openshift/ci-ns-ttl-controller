module github.com/openshift/ci-ns-ttl-controller

go 1.15

require (
	github.com/sirupsen/logrus v1.7.0
	k8s.io/api v0.19.3
	k8s.io/apimachinery v0.19.3
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	k8s.io/test-infra v0.0.0-20201204222726-c33718b66ba5
)

replace k8s.io/client-go => k8s.io/client-go v0.19.3
