FROM alpine:3.10

RUN apk add --no-cache ca-certificates
ADD ci-ns-ttl-controller /ci-ns-ttl-controller
ENTRYPOINT ["/ci-ns-ttl-controller"]
