# Trendyol/gaos, an HTTP chaos server. Upstream ships a Go 1.14/amd64-only
# Dockerfile; this builds the same pinned commit for the host arch.
FROM golang:1.26 AS build
ARG GAOS_COMMIT=771d620
RUN git clone https://github.com/Trendyol/gaos /src && cd /src && git checkout "$GAOS_COMMIT" \
 && CGO_ENABLED=0 go build -o /gaos .

FROM gcr.io/distroless/static-debian12
COPY --from=build /gaos /gaos
ENTRYPOINT ["/gaos"]
