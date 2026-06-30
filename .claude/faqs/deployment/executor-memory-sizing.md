# Executor memory sizing (Senzing is NATIVE — off-heap)

## Why normal Spark memory settings are not enough

The Senzing engine is **native C code** reached via JNI. Its working memory is **native/off-heap** —
it does **not** live in the Spark JVM heap (`spark.executor.memory`). If you size the executor only by
JVM heap, the cluster manager (YARN/K8s/Databricks) will **OOM-kill the container** when the native
heap grows, because that native usage counts against the container's **overhead**, not its heap.

## The sizing rule of thumb

Per **executor JVM** (which hosts one shared engine — see the engine-lifecycle FAQ):

```
native RAM ≈ 4 GB  (base engine)  +  1 GB × (concurrent Sz processing threads)
```

A "Sz processing thread" is one Spark task driving the engine single-threaded, so the concurrent count
equals **`spark.executor.cores`** (tasks run one engine call at a time each). Examples:

| executor cores | native RAM (≈ 4 GB + cores×1 GB) |
|---|---|
| 2 | 6 GB |
| 4 | 8 GB |
| 8 | 12 GB |

## How to tell Spark (this is mandatory)

That native RAM must be reserved as **`spark.executor.memoryOverhead`** (the non-heap allowance), NOT
as `spark.executor.memory`:

```
spark.executor.cores          = N
spark.executor.memory         = 2g            # JVM heap — modest; records stream, little is on-heap
spark.executor.memoryOverhead = (4 + N) g     # 4 GB engine + 1 GB per concurrent Sz thread (+ headroom)
```

So a container is roughly `memory + memoryOverhead` = JVM heap + (4 GB + cores GB). On Databricks set
these in the cluster Spark config; on spark-submit pass `--conf`.

## How this ties to the scaling model

Concurrency is the executor/worker count (no threads-per-worker knob), and **memory scales with that
same knob**: more cores per executor = more concurrent Sz threads = proportionally more native RAM
(`+1 GB` each) on top of the 4 GB engine base. Size executors so `memoryOverhead` covers it, or the
container gets killed under load. Prefer more executors with moderate cores over a few fat executors if
native RAM per node is tight.
