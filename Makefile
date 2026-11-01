# Everything runs in Docker except the Go and Gradle builds of the tests.
COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down demo test it e2e latency catchup chaos couchbase reproduce tables

up:            ## PostgreSQL, Redpanda, Elasticsearch, cdc, indexer, api, Prometheus, Grafana
	$(COMPOSE) up -d --build --wait postgres redpanda elasticsearch cdc indexer api prometheus grafana

down:
	$(COMPOSE) --profile chaos --profile couchbase --profile tools down -v
	docker compose -f bench/catchup/docker-compose.yml down -v

demo: up       ## seed 10k products and search them
	$(COMPOSE) run --rm loadgen -seed 10000
	$(COMPOSE) run --rm reconcile -wait 2m
	curl -s 'localhost:8080/search?q=slim+jeans&limit=3'; echo

test:
	go test -race ./...

it:            ## pipeline against real containers (Testcontainers)
	go test -race -tags integration -count=1 -timeout 15m ./internal/pipeline/

e2e:           ## black-box tests of cmd/all with Stove (needs JDK 21)
	cd e2e && ./gradlew --no-daemon test

latency: up    ## write→searchable at 1k/5k/10k writes/s, 3 × 60 s each
	bench/latency.sh 3 60s

chaos: up      ## failure matrix, zero lost documents required
	bench/chaos.sh

catchup:       ## 10M rows, go-pq-cdc-kafka vs Debezium, same limits (stops the main stack first)
	$(COMPOSE) stop
	for i in 1 2 3; do for c in gopqcdc debezium; do CPUS=1 MEM=1g bench/catchup.sh $$c 10000000; done; done
	for c in gopqcdc debezium; do CPUS=2 MEM=1g bench/catchup.sh $$c 10000000; done

couchbase:     ## optional second path: Couchbase → go-dcp-elasticsearch
	$(COMPOSE) stop cdc indexer api
	bench/couchbase.sh 3 60s

tables:        ## rebuild the README result tables from bench/results
	python3 bench/tables.py

reproduce: test it latency chaos catchup tables   ## every number in the README (about 2 hours)
