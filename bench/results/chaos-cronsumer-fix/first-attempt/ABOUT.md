# First attempt, 2026-11-02, kept as recorded

Not the result quoted in the README: that is `../chaos.jsonl`, one pass on one commit.

15 scenario starts on commits e218e7b and 645f173. 12 reached reconcile, all with
0 missing / 0 stale / 0 extra. 3 aborted before reconcile: 2 with an empty load
report and 1 in the settling step, whose output was discarded at the time, so its
cause is not known. It did not recur after the settling step began to retry.
The empty report came from a loadgen bug (+Inf in the JSON report), 5 of the
14 runs that reached the end of the load. The commit message of the fix says "5
of 12"; 5 of 14 is right.

Why the pass was repeated: the kill-in-drain snapshot of the last run here
(`chaos-logs/*.204529.group-at-kill.txt`) shows products.retry at lag 0 when the
fixed-delay kill landed, so that scenario was not hitting a drain. The 3 earlier
snapshots failed because of an rpk address bug in the overlay.
