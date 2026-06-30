# Slow records and progress logging

## A single record is taking a very long time — is that a bug?

Usually not. Senzing verbs are synchronous, **uncancellable JNI calls**. A record that resolves into a
large entity (extensive graph traversal / many merges) can legitimately take far longer than the
typical ~200ms. It runs to completion.

Handle it with observability, not abortion:

- Log a `LONG_RECORD` warning + increment a counter when a record exceeds a threshold (e.g. 300s).
- **Never** abort, skip, or relocate a still-running record — you cannot cancel the in-flight verb,
  and the `WITH_INFO` / `AFFECTED_ENTITIES` result still matters.
- Spark task cancellation cannot interrupt an in-flight verb — account for this in any join/timeout
  logic (expect a bounded wait that may still outlast the timeout).

## What should per-task progress logging show?

- In-memory counters: succeeded / skipped / errored / retried / long.
- A periodic progress line every N records with the breakdown plus **both** the interval rate (since
  the last report) and the cumulative average rate (records/sec) — the interval rate is what reveals a
  slowdown.
- A labeled prefix (data source / partition id) on every line.
- A final summary at task end: totals + elapsed + average rate.

## Other common issues

- Engine init hangs/fails after minutes → likely two environments created in one process, or schema
  not applied. See architecture/initialization-separation.
- "Unknown data source" on add → config drift; run the active-vs-default config check and retry once.
  See architecture/error-handling.
- Native library load failure on an executor → FAT-jar extraction / `patchelf $ORIGIN` / preload
  ordering. See build/fat-jar-and-native-extraction.
