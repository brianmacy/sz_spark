# Dead-letter (DLQ) capture and reprocess for the streaming feeder

**Status:** implemented (code + tests). Wired in `glue.ParquetStreamFeeder` +
`glue.DeadLetterReprocess`.

## The gap this closes
`core.AddCore.run` returns a `SplitResult(good, errors)` per engine pass: the affected-entity
`good` frame and the classified-failure `errors` frame (`work.ErrorTaxonomy` →
`model.ErrorRow`, carrying `dataSource / recordId / payload / category / errorCode / message /
attempts`). The batch jobs (`AddUpdateJob`, `RedoJob`, `DeleteJob`, `SearchJob`) already persist
both frames. The **streaming feeder did not** — it called `AddCore.run` and discarded the result,
so every failed record vanished into the transient `staging` dir (overwritten each micro-batch).
A record could get a `DSRC_RECORD` row, fail to resolve, and leave **no `RES_ENT_OKEY` and no error
artifact** — a silent orphan.

## The sink (on-prem, Parquet)
`ParquetStreamFeeder` now captures the `SplitResult` in `foreachBatch` and writes, per micro-batch,
in `SaveMode.Append`:

| Frame | Sink arg | Meaning |
|---|---|---|
| `errors` | `deadLetter=` | durable **dead-letter dir** — the DLQ equivalent. One shard per batch. |
| `good` | `output=` | append-only affected-entity **change-notification feed** (optional but cheap). |

Both are **opt-in**: if the arg is empty the write is skipped, preserving the prior behavior
(back-compat — nothing breaks for callers that don't set them). Logic lives in the top-level,
unit-tested `ParquetStreamFeeder.writeSinks`.

### Directory layout
The two sinks are independent directories the operator names via the feeder's `deadLetter=` and
`output=` args; each accumulates one Parquet shard per micro-batch (`SaveMode.Append`). A typical
layout under a shared base dir:

```
<base>/inbox/       # parquet shards produced by the drainer (the feeder's readStream source)
<base>/archive/     # cleanSource=archive lands consumed inbox files here
<base>/checkpoint/  # exactly-once file-feed tracking
<base>/staging/     # transient per-batch SplitResult scratch (overwritten each batch)
<base>/deadLetter/  # errors frame  → ErrorRow shards (the DLQ)          [deadLetter=]
<base>/output/      # good frame    → AffectedEntityRow change-feed shards [output=]
```

`deadLetter/` and `output/` are the only durable, append-only sinks; `staging/` is transient and
must never be read as a system of record (it is overwritten every micro-batch — the gap this
feature closes).

### Exactly-once / durability
The writes are inside `foreachBatch`, which **re-runs a failed batch**, so a shard can be written
more than once. `SaveMode.Append` + idempotent downstream is the accepted at-least-once model — the
same one `addRecord` itself relies on. Dedup keys when consuming the sinks:
- dead-letter errors: `(dataSource, recordId, category)` (`ErrorRow.dedupKey`).
- affected output: `(dataSource, recordId, entityId)`.

A shard is **never dropped**; at worst it is duplicated and de-duplicated on read. This is the same
persist-then-proceed discipline as Stage 1's persist-then-ack (see `RABBITMQ_INGEST.md`).

## Reprocess path
`glue.DeadLetterReprocess` is a **thin, one-pass helper** (not a scheduler). Steps:

1. Read the dead-letter dir (`deadLetter=`).
2. Keep only **reprocessable** categories — `RETRY_EXHAUSTED`, `CONFIG_RELEVANT`,
   `REPLACE_CONFLICT` (transient / config-curable). **Terminal** categories — `BAD_INPUT` and
   `NOT_FOUND` — stay **quarantined**: re-feeding a malformed or unresolvable payload just loops.
   `Retryable` never lands here (it is retried in-process, and exhaustion is re-classified to
   `RETRY_EXHAUSTED`); `Systemic` failures fail the Spark task loudly and never reach the error
   frame. Net: `selectReprocessable` re-emits exactly the transient/config-curable rows and leaves
   everything else in place for human review.
3. Project each surviving `ErrorRow` back to the `InputRecord` columns
   (`dataSource, recordId, payload`) and write them as Parquet shards into a **re-feed dir** in
   `SaveMode.Append`. Point the feeder's `inbox=` at that dir (or have a supervisor move the shards
   into the inbox) and the running streaming query picks them up.

```
spark-submit --class com.senzing.spark.glue.DeadLetterReprocess sz-spark-assembly.jar \
  deadLetter=$IO_BASE/deadletter  reFeed=$IO_BASE/inbox
```

**Cadence is operational**, deliberately left to a cron/supervisor: sweep the dead-letter dir on an
interval, decide move-vs-copy, and archive the shards you've re-fed so they aren't re-emitted every
sweep. The pure filter (`DeadLetterReprocess.selectReprocessable`) is unit-tested; only the timing
policy is left out.

## Databricks variant (document, don't build)
Same shape, swap the writer:

| Concern | On-prem (this repo) | Databricks |
|---|---|---|
| Dead-letter sink | Parquet dir, `SaveMode.Append` in `foreachBatch` | **Delta "quarantine" table**, `append` in `foreachBatch` |
| Reprocess | read dir → filter → re-feed Parquet shards | `MERGE` the quarantine table's reprocessable rows back into the inbox, or a `readStream` off it with **CDF** (Change Data Feed) |
| Querying failures | `spark.read.parquet(deadLetter)` | plain SQL over the quarantine table (`WHERE category = …`) |
| Ingest source | file-stream `format("parquet")` | Auto Loader `format("cloudFiles")` |

The Delta quarantine table is strictly a **better** DLQ (queryable, `MERGE`-reprocessable, CDF),
but Delta/Auto Loader are DBR-proprietary → they live only in the Databricks glue variant, never in
the portable core. The `errors` frame handed to the writer is identical; only the sink writer
differs.

## Parallels and the risk it removes
- **Reference RabbitMQ consumer:** an add failure past its retry budget is `nack`'d to a RabbitMQ
  **dead-letter queue** for later review/redrive. The Parquet dead-letter dir here is that DLQ's
  on-prem equivalent — same purpose, file-based sink.
- **Silent-orphan risk (the reason this exists):** without a DLQ, a record with a `DSRC_RECORD` but
  **no `RES_ENT_OKEY` and no error artifact** is lost with no trace. The dead-letter sink guarantees
  every failed record leaves a reviewable, reprocessable row.
