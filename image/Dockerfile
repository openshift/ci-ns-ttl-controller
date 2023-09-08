FROM registry.ci.openshift.org/openshift/centos:stream9

RUN dnf install -y ca-certificates
ADD ci-ns-ttl-controller /ci-ns-ttl-controller
ENTRYPOINT ["/ci-ns-ttl-controller"]
