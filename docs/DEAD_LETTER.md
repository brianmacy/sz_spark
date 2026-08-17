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
4. **Archive the swept shards** — the pass snapshots the shard files present at the start, then
   renames exactly those out of the dead-letter dir into `archive=` (default `<deadLetter>-archived`)
   after the re-emit. This makes the sweep **idempotent**: a second pass over an unchanged dir
   re-emits nothing (previously it re-emitted every shard on every run). Shards the feeder writes
   *during* a sweep are not in the snapshot and are picked up next pass. Archived shards remain
   `spark.read.parquet(archive)`-queryable, so the quarantined terminal rows are still available for
   human triage.

```
spark-submit --class com.senzing.spark.glue.DeadLetterReprocess sz-spark-assembly.jar \
  deadLetter=$IO_BASE/deadletter  reFeed=$IO_BASE/inbox  archive=$IO_BASE/deadletter-archived
```

Re-feeding is safe to repeat: `add_record` is idempotent on `(DATA_SOURCE, RECORD_ID)`, and the
terminal-category filter keeps `BAD_INPUT`/`NOT_FOUND` out of the loop. **Cadence** (how often to
sweep) is still an operational cron/supervisor decision — but the archive step means a sweep is now
self-cleaning, not a re-emit-everything hazard. Covered by `DeadLetterReprocessSpec` (re-emit once,
archive, second pass = 0).

### Single-process reprocessor (`jobs.DeadLetterJob`) — the convergent, contention-safe driver
`DeadLetterReprocess` above is the one-pass primitive. `jobs.DeadLetterJob` is the **runnable job**
that wraps it in a **bounded, convergent loop** and enforces the single-process re-drive.

Why a loop, not one pass: reprocessing the DLQ tends to **create more DLQ** (a re-driven record can
fail again), and `selectReprocessable` drops `attempts`, so a re-failed record starts a fresh
`ErrorRow` with no cross-generation memory — a naive loop would re-drive a genuinely-stuck record
forever. Each round sweeps generation *r* → re-feed inbox → **serial re-drive** → this round's
failures land in generation *r+1*.

Why single-process is the point (not just "less contention"): the re-drive runs `master=local[1]`
and the inbox is `coalesce(1)`'d, so the Senzing verb runs on **one thread, engine initialized
once**. That REMOVES the concurrency that produces `REPLACE_CONFLICT` (the dominant reprocessable
category is a lock/replace race) and gives `RETRY_EXHAUSTED` a fresh budget — a serial pass drains
the bulk of the queue by construction.

Termination (bounded rounds + shrink check — no schema change):
- **drained** — the sweep finds nothing reprocessable, or a round's residue is empty;
- **not shrinking** — a round's reprocessable residue did not fall below what it re-drove (zero
  progress) ⇒ the residue is genuinely stuck;
- **cap** — `maxRounds` (default 3) reached.

On any non-drained stop the final generation is moved to `quarantine=` for human review and never
re-driven again. Re-driven **successes** append to the `output=` `$AFFECTED` feed so the entity-mart
sees them. Covered by `DeadLetterJobSpec` (drain, shrink-stop→quarantine, maxRounds cap).

```
spark-submit --class com.senzing.spark.jobs.DeadLetterJob sz-spark-assembly.jar \
  deadLetter=$IO_BASE/deadletter  work=$IO_BASE/deadletter-reprocess \
  quarantine=$IO_BASE/deadletter-quarantine  output=$IO_BASE/affected \
  runId=dlq-reprocess  maxRounds=3
```
(The job pins `master=local[1]` itself — do NOT override it with a multi-core master, or you
reintroduce the very concurrency the single process exists to remove.)

### Kafka-path re-drive
The dead-letter sink is engine-level (`OverlappingBatchEngine`), so it is identical for all sources —
`InboxSource`, **`KafkaSource`**, and `DeltaSource`. There is no Kafka-specific re-drive: run
`DeadLetterReprocess` to project the failures into a re-feed **inbox** dir, then feed that dir with
the parquet source (no need to re-produce to Kafka):

```
spark-submit --class com.senzing.spark.glue.ParquetParallelFeeder sz-spark-assembly.jar \
  source=inbox trigger=availableNow inbox=$IO_BASE/deadletter-refeed processing=$IO_BASE/redrive-proc ...
```

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
