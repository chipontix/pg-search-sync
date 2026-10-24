# Findings in Trendyol libraries

What came up while building and breaking this project, with a reproduction
for each. Status is updated when an issue or PR is opened upstream.

## kafka-cronsumer v1.8.11: a crash loses retry records that take longer than the commit interval

`internal/consumer.go` `ReadMessage` calls `FetchMessage` and then
`CommitMessages` for the record **before** it is handed to `consumeFn`. With
the default `CommitInterval` (1 s) the commit is flushed asynchronously, so
for fast handlers it trails processing and a crash only causes duplicates.
When one record takes longer than the interval, its offset (and the offset
of the next record, already fetched and blocked on the unbuffered message
channel) is committed while the handler is still running. A crash at that
moment loses both: up to `Concurrency + 1` records per crash.

[`upstream/cronsumer-crash`](upstream/cronsumer-crash): 1 partition,
`Concurrency: 1`, `@every 1s`, `NonStopWork`; the handler sleeps `WORK_MS`
and then appends the key to a file with fsync. `run.sh` kills the process
with SIGKILL five times while it is processing, then lets a fresh instance
drain the topic.

| handler time | records | processed at least once | duplicates | lost |
|---|---|---|---|---|
| 20 ms | 200 | 200 | 78 | 0 |
| 1500 ms | 20 | 10 | 0 | **10** (2 per kill) |

The group's committed offset is at the end of the topic in both runs, so the
lost records are never redelivered. The README's "When to avoid" section
mentions auto-commit; this pins down when it turns into loss. A possible
change: commit after `consumeFn` (and after re-producing to the retry or
dead-letter topic) behind an option.

Related, from reading the code only (not reproduced): in `cronsumer.go`
`produce`, a failed `ProduceWithRetryOption` to the retry or dead-letter
topic is logged and the record is dropped, after its offset was committed.

In this project the retry path is only used for documents that still fail
after 30 s of in-process retries, the indexer's own offsets are committed
after the batch, and `reconcile -repair` re-emits anything that went
missing. None of the failure-matrix runs lost a document.

## gaos (771d620): three bugs

[`upstream/gaos/repro.sh`](upstream/gaos/repro.sh) builds the pinned commit
with `-race` and shows all three.

1. **Data races.** `Rate.Execute` / `Limit.Execute` (`runner/scenario.go`)
   increment `n` without synchronisation, and `Method.Handler`
   (`runner/runner.go`) increments `cnt` and reassigns `m.Scenario` from
   every request goroutine. 400 requests from 32 clients: 7–8
   `WARNING: DATA RACE` reports. Under concurrency `rate` decisions and the
   `direct` switch are unreliable.
2. **`redirect` drops the query string.** `req.SetRequestURI(r.Host +
   string(ctx.Path()))`: `/proxy?refresh=wait_for&x=1` reaches the upstream
   as `/proxy`. `ctx.RequestURI()` keeps it.
3. **`span` ignores `end` when `start` is empty.** `NewSpan` checks
   `len(s.Start) == 0 || eerr != nil` before clearing `end`; it should check
   `len(s.End)`. A scenario with only `"end": "2020-01-01…"` still runs
   `accept`.

Neither affected this project's chaos runs (only `POST /_bulk` and
`PUT /products` go through gaos, without query strings, and the 429 window
uses both `start` and `end`).

## go-pq-cdc-kafka v1.4.5: change LSN not on `cdc.Message`

go-pq-cdc delivers `CommitLSN` on every `ListenerContext` (since
2026-09-10), but go-pq-cdc-kafka's `Message` does not carry it, so a sink
that wants an ordering key has to bring its own. This project uses a
trigger-maintained row version instead. Already tracked upstream as
[Trendyol/go-pq-cdc-kafka#50](https://github.com/Trendyol/go-pq-cdc-kafka/issues/50);
nothing new to file.

## Not filed

- go-dcp-elasticsearch: write→searchable is ~490 ms idle and seconds under
  1000 writes/s, with `cbgo_dcp_latency_ms` in seconds while bulks take
  ~17 ms. The delay is before the listener; I have not isolated whether it
  is Couchbase, gocbcore or go-dcp, so there is nothing actionable to report.
