# Upstream PR: kafka-cronsumer, commit a retry record only after it is handled

Prepared for Rabiya to open. Nothing has been filed upstream yet.

| | |
|---|---|
| Library | [Trendyol/kafka-cronsumer](https://github.com/Trendyol/kafka-cronsumer), `main` at `78b8f18` (= v1.8.11) |
| Fork, branch | [`chipontix/kafka-cronsumer` `fix/commit-after-handled`](https://github.com/chipontix/kafka-cronsumer/tree/fix/commit-after-handled) |
| Commits | `eacc75e` test (fails on main), `5897aa6` fix |
| Project branch using it | `cronsumer-commit-after-handled` in this repository (`replace` on the fork) |

## Contribution rules

kafka-cronsumer has no `CONTRIBUTING.md` and no PR template. The rules are the
"Contribute" section of its README:

- Use issues for everything. Small change: just send a PR. Bigger change:
  open an issue for discussion first.
- A PR should have a test case and documentation.
- Follow the [code of conduct](https://github.com/Trendyol/kafka-cronsumer/blob/main/CODE-OF-CONDUCT.md).

From the repository itself: commit titles are Conventional Commits (`fix:`,
`feat:`, `test:`); CI runs `go test` (`make test`), golangci-lint v1.64.8 with
`.golangci.yml`, and integration tests against a broker. The README says
nothing about AI-assisted contributions; the PR text says it anyway.

This change moves when offsets are committed, which is behaviour the README
documents, so it counts as a bigger change: **open the issue first, link the
branch, then open the PR** once a maintainer agrees on the approach (or asks
for an opt-in flag instead; see the PR text).

## Not already taken

Checked on 2026-11-01, all issues and PRs, open and closed:

- [#102](https://github.com/Trendyol/kafka-cronsumer/issues/102) / [#105](https://github.com/Trendyol/kafka-cronsumer/pull/105):
  a record lost when the cron window cancels **between fetch and commit**.
  Fixed by committing with `context.WithoutCancel`. The issue says explicitly
  that it does "not change the existing commit-before-handler ordering". That
  ordering is what this PR changes.
- [#101](https://github.com/Trendyol/kafka-cronsumer/issues/101) / [#106](https://github.com/Trendyol/kafka-cronsumer/pull/106):
  `Stop` races; graceful shutdown. A graceful stop does not lose records; a
  crash does.
- [#96](https://github.com/Trendyol/kafka-cronsumer/issues/96) (open): the snapshot guard; a different path.
- Searches for `commit`, `lost`, `at-least-once` find nothing else.

## Reproduction

1. **Unit test, no broker.** `Test_Listen_Should_Not_Commit_Record_While_Handler_Is_Running`
   (`internal/commit_order_test.go`) runs `Listen` and one worker against a
   fake reader, blocks `consumeFn` on offset 0 and checks which offsets were
   committed meanwhile.

   ```bash
   cd kafka-cronsumer
   git checkout eacc75e && go test -race -run Test_Listen_Should_Not_Commit ./internal/
   # FAIL: offsets [0 1] committed while the handler of offset 0 is still running
   git checkout 5897aa6 && go test -race ./...
   # ok, all packages
   ```

2. **Real broker, SIGKILL.** [`docs/upstream/cronsumer-crash`](docs/upstream/cronsumer-crash):
   1 partition, `Concurrency: 1`, the handler sleeps `WORK_MS` and then
   appends the key to a file with fsync; `run.sh` kills the process five times
   while it is processing, then a fresh instance drains the topic. On v1.8.11
   (see [docs/upstream.md](docs/upstream.md)):

   | handler time | records | lost |
   |---|---|---|
   | 20 ms | 200 | 0 (78 duplicates) |
   | 1500 ms | 20 | **10**, 2 per kill |

   Re-run on 2026-11-01 for both versions with
   [`compare.sh`](docs/upstream/cronsumer-crash/compare.sh). Each version
   gets its own fresh single-node `apache/kafka:3.9.1`, then 20 records, a
   1500 ms handler and five SIGKILLs. Raw output:
   [`bench/results/cronsumer-crash/`](bench/results/cronsumer-crash/).

   | cronsumer | processed (unique / total) | lost | group offset after drain |
   |---|---|---|---|
   | v1.8.11 | 10 / 10 | **10**: k004 k005 k008 k009 k010 k013 k014 k015 k018 k019 | not read (the describe call hung, see below) |
   | fork `5897aa6` | 20 / 24 | **0** (4 duplicates) | 20 of 20 |

   The VM was shared with other workloads (host load average around 100). In
   the fork run the broker stalled for about 4 minutes after the third kill:
   the consumer logged `i/o timeout` and `Group Coordinator Not Available`,
   then carried on. That costs time and decides nothing about loss: a record
   counts as processed only when its key is fsynced to the log. In the
   v1.8.11 run `kafka-consumer-groups --describe` hung on the same stalls
   and was stopped, so that row has no offset. The earlier run on Redpanda
   (table above) recorded the committed offset at the end of the topic.

   ```bash
   git checkout main && docs/upstream/cronsumer-crash/compare.sh v1.8.11 1500 20 2
   git checkout cronsumer-commit-after-handled && docs/upstream/cronsumer-crash/compare.sh fork 1500 20 2
   ```

## Issue text

> **Title:** bug: a crash loses retry records whose offsets are committed before consumeFn runs
>
> `kafkaConsumer.ReadMessage` (`internal/consumer.go`) calls `FetchMessage`
> and then `CommitMessages` for the record before `Listen` hands it to a
> worker. With the default async `CommitInterval` (1 s), the offset of a record
> that is still in `consumeFn`, and of the next record (already fetched and
> waiting on the unbuffered message channel), reaches the broker while they
> are being handled. If the process dies at that moment (OOM kill, SIGKILL
> after a pod's grace period, node loss), the restarted group resumes after
> them and they are never processed or sent to the dead-letter topic: up to
> `Concurrency + 1` records per crash.
>
> The chance per crash grows with handler time: roughly
> `(Concurrency + 1) × handler time / CommitInterval`, capped at 1. A standalone
> reproduction against a real broker (1 partition, `Concurrency: 1`, `@every 1s`,
> five SIGKILLs while processing) lost 0 of 200 records with a 20 ms handler
> and 10 of 20 with a 1500 ms handler. In both runs the group's committed
> offset was at the end of the topic, so the lost records are never redelivered.
> With the proposed fix, the same 1500 ms run lost 0 of 20 (4 duplicates).
>
> A graceful `Stop` is not affected (#106 drains the workers).
>
> The README lists "Messages should be committed (we use auto-commit interval
> for increasing performance)" under *When to avoid*. Committing after the
> handler keeps the async commit interval and its performance, and turns a
> crash from lost records into redelivered ones, which matches what
> kafka-konsumer does on its main topic (kafka-konsumer uses cronsumer for its
> retry topic). Would a PR that commits each record after it is handled be
> welcome, or would you prefer it behind a config flag?
>
> Reproduction and a proposed fix with a regression test:
> https://github.com/chipontix/kafka-cronsumer/tree/fix/commit-after-handled
>
> *The analysis, reproduction, fix and tests were prepared with the help of an
> AI coding agent (Claude Code, Anthropic); I reviewed them and can answer
> questions.*

## PR text

> **Title:** fix: commit a retry record's offset only after it is handled
>
> Closes #<issue>.
>
> **Problem.** `ReadMessage` committed every record right after
> `FetchMessage`, before it reached `consumeFn`. With the default async
> `CommitInterval`, the offset of a record still being handled, and of the next
> record already fetched and waiting for a worker, could reach the broker, and a
> crash at that moment lost both: the restarted group resumed after them.
>
> **Change.**
> - `ReadMessage` only fetches, and records the offset as pending for its
>   partition.
> - New `Consumer.Ack(kafka.Message)`, called by every path that finishes
>   with a record: the worker after `consumeFn` (and the retry/DLQ produce on
>   failure), the header-filter skip, the next-iteration and backoff
>   re-produce in `Listen`, and the shutdown re-publish.
> - A partition is committed up to the last record before the first one still
>   in flight. Workers with `Concurrency > 1` finish out of order, and a
>   commit never passes a running handler. Pending offsets are kept sorted, so a
>   rebalance that rewinds the reader is handled; the async commit loop in
>   kafka-go keeps the highest offset per partition, so a late lower commit
>   does not move the group back.
> - The commit keeps using a context detached from the listening window
>   (#105), now `context.Background()` in `Ack`.
> - README, *When to avoid*: the commit line now describes the new behaviour
>   (committed after handling, asynchronously; a crash can redeliver).
>
> **Behaviour change.** A crash now redelivers records that were in flight,
> instead of skipping them. Retry handlers already have to be idempotent (each
> failed record comes back on the next run), so duplicates are not a new kind
> of event for them. Throughput is unchanged: commits are still queued to
> kafka-go and flushed every `CommitInterval`. Unchanged and out of scope: a
> failed produce to the retry or dead-letter topic is still logged and the
> record is acked, as before.
> If you would rather keep the old ordering as the default, I can put this
> behind a `consumer.commitAfterHandle` option.
>
> **Tests.**
> - `Test_Listen_Should_Not_Commit_Record_While_Handler_Is_Running`: `Listen`
>   plus one worker on a fake reader, `consumeFn` blocked on offset 0. It fails on
>   `main` (`offsets [0 1] committed while the handler of offset 0 is still
>   running`) and passes with the fix; it is the first commit of the branch, so
>   it can be checked out alone.
> - `Test_Ack_Should_Commit_Only_Up_To_First_Unacked_Record`: out-of-order
>   acks never commit past an unacked offset.
> - `Test_ReadMessage_Should_Return_Fetched_Message_When_Context_Cancelled_After_Fetch`:
>   the #105 test, adjusted: no commit in `ReadMessage`, one commit with a live
>   context on `Ack`. `Test_ReadMessage_Should_Return_Error_When_Commit_Fails` is
>   removed, because `ReadMessage` no longer commits; a commit error in `Ack` is logged.
> - `go test -race ./...` and `golangci-lint` v1.64.8 with `.golangci.yml` pass.
>   The integration tests were not run locally.
> - Real broker (Kafka 3.9.1, 1 partition, `Concurrency: 1`, 1500 ms handler,
>   five SIGKILLs while processing): `main` lost 10 of 20 records, this branch
>   lost 0 of 20 and redelivered 4.
>
> *This fix and its tests were prepared with the help of an AI coding agent
> (Claude Code, Anthropic). I reviewed the change and ran the tests.*

## What this changes for pg-search-sync

The indexer uses cronsumer through kafka-konsumer for `products.retry`. In the
failure matrix nothing was lost, because `reconcile -repair` is the backstop and
retries only carry documents that already failed for 30 s. With the fix, a kill
while cronsumer drains should cause redelivery, not loss. The SIGKILL
reproduction shows it (0 lost instead of 10 of 20). The full failure matrix
(`make chaos`) has not been re-run on that branch: it needs the whole stack,
and the shared VM is overloaded. `go test -race ./...` passes on that branch.
