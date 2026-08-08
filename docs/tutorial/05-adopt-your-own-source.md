# 05 · Adopt your own source

The engine doesn't care where records come from. Teach it a new source by implementing one interface —
no changes to the engine, the feeder, or the output side.

## The interface

```scala
final case class Chunk(bounds: String, df: Dataset[InputRecord], nextCursor: String)

trait RecordSource {
  def initialCursor: String                    // where to resume (a committed cursor, or "")
  def nextChunk(cursor: String): Option[Chunk]  // claim the next batch, or None if nothing's ready
  def commit(bounds: String): Unit             // mark that batch done
  def reclaim(): Unit                           // recover a prior run's in-flight work
}
```

`nextChunk` returns a **lazy** `Dataset[InputRecord]` — the record data rides Spark partitions to the
executors; the driver only does metadata (claim / commit). An `InputRecord` is
`(dataSource, recordId, payload)` where `payload` is the raw record JSON.

## Pick a flavor

| Your storage | Flavor | `commit` | `reclaim` |
|---|---|---|---|
| POSIX filesystem, or a queue you drain to files | **Dispose** | delete/archive the claimed files | move in-flight files back to the inbox |
| Anything with a monotonic cursor (offset, version, id) | **Watermark** | advance the durable cursor | no-op — restart re-reads from the cursor |

On object storage, always choose **watermark** (atomic rename isn't available).

## Worked example — a watermark source

Say your records land in a table with a monotonically increasing `id`. The cursor is the id; a batch
is an id range.

```scala
final class MyTableSource(spark: SparkSession, cfg: MyCfg, batch: Int) extends RecordSource {
  private val wm = new OffsetWatermark(fs, checkpointFile, start = cfg.startId)   // reuse the shipped watermark

  def initialCursor: String = wm.committedOffset.toString

  def nextChunk(cursor: String): Option[Chunk] = {
    val start  = cursor.toLong
    val latest = maxIdAvailable()                 // your metadata read
    if (start >= latest) None
    else {
      val end = math.min(start + batch, latest)   // COUNT-bounded — keeps the feeder tail-free
      val df  = readRange(start, end)              // lazy: SELECT ... WHERE id >= start AND id < end
        .selectExpr("data_source AS dataSource", "record_id AS recordId", "body AS payload")
        .as[InputRecord]
      Some(Chunk(bounds = s"$start-$end", df = df, nextCursor = end.toString))
    }
  }

  def commit(bounds: String): Unit = { val (s, e) = parse(bounds); wm.complete(s, e) }
  def reclaim(): Unit = ()                          // watermark flavor: nothing to reclaim
}
```

That's the whole contract. `KafkaSource` and `DeltaSource` are ~60 lines each following exactly this
shape — read them for a real reference.

## Wire it in

The feeder selects a source by name
([`ParquetParallelFeeder`](../../src/main/scala/com/senzing/spark/glue/ParquetParallelFeeder.scala)).
Add a case:

```scala
case "mytable" => new MyTableSource(spark, MyCfg.from(args), recordsPerBatch)
```

Then run it exactly like any other source: `source=mytable …`. The overlapping-batch engine, the
affected-entity output, dead-letter, and at-least-once all come for free.

## Three rules

1. **Bound by count, not by "read everything available."** A batch should be ~`recordsPerBatch`
   records so one slow record can't stall a giant task. This is the single most important rule for
   throughput.
2. **`nextChunk` is called serially — advance the cursor safely.** Return `nextCursor` = the position
   *after* this chunk; the engine threads it back to you.
3. **Be replay-safe.** A batch that fails is re-read on restart. Re-adding a resolved record is a no-op,
   so at-least-once is fine — just don't rely on exactly-once.

---

Next: **[06 · Adapt your own replication](06-adapt-your-own-replication.md)** — do something with the
resolved entities.
