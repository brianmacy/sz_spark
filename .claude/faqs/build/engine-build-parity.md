# Engine-build parity: the FAT jar bundles whatever `SENZING_DIR` points at

## The trap

`sbt stageNatives` copies the native engine (`libSz.so` + `data/` + `resources/`) **and** the Java
SDK jar out of `$SENZING_DIR` (default `/opt/senzing`) and bakes them into the FAT jar. Whatever
build happens to be installed there is the engine your Spark executors will run — silently. There is
**no** signal at build or run time that it differs from the fleet you are comparing against.

This bit us on 2026-08-07: the Spark feeder measured ~20% slower than the native Rust fleet on the
same DB. After ruling out host, executor topology, connection counts, network, and lock contention,
the cause was a **stale engine in the jar** — `/opt/senzing` was `4.4.0.26151` (built 2026-05-31),
while the Rust fleet ran `4.4.0.DEVELOPMENT` carrying newer DB-round-trip reductions (arena / redo
de-batch / candidate-read). Same feeder, same DB, same concurrency — the older engine just did more
DB round-trips per record. Rebuilding the jar against the fleet's engine restored parity
(`.142`-Spark host CPU 44% → 55.5% ≈ `.141`-Rust 57%; total system add 1562 → 1732 rec/s).

## The rule (do this BEFORE any cross-loader or cross-build A/B)

The feeder's runtime engine `apiVersion` (from `get_stats`) MUST equal the fleet's:

```
docker logs sz-spark-feeder | grep -oE '"apiVersion":"[^"]+"' | tail -1   # feeder
docker logs compose-app-1   | grep -oE '"apiVersion":"[^"]+"' | tail -1   # fleet (on .141)
```

If they differ, the jar was built against the wrong engine — rebuild against the fleet's, then
redeploy and re-check. The full procedure (building a hybrid `SENZING_DIR` from a G2 DEV dist +
the matching `sz-sdk.jar`, and the `UnsatisfiedLinkError` you hit if the jar and natives come from
different builds) is in **`docs/BUILD_AGAINST_FLEET_ENGINE.md`**.

## Why the native-drift gate does NOT catch this

The per-build "native drift gate" compares a new sz_spark jar's native-CRC manifest against the
**prior sz_spark jar**. It only proves consistency *with the last sz_spark build* — and every
sz_spark build pulled from the same stale `/opt/senzing`, so the CRC matched every time while the
whole series ran the wrong engine. Use the **`apiVersion` check against the fleet**, not the drift
gate, for parity. The drift gate answers "did the natives change since my last build?"; the parity
check answers "am I running the same engine as the thing I'm comparing to?" — different questions.

## Corollary: a loader A/B is only valid at matched engine build

Rust-consumer vs Spark-feeder throughput/CPU is only comparable when **both link the same engine
build** (and the same libpq major — see `docs/PERFORMANCE.md` finding #4). At matched build the two
are performance-equivalent (same duty cycle, same per-record engine cost); a mismatched build is a
confound in the same class as a mismatched thread count or a host hardware asymmetry, and it can be
the *dominant* term, not a rounding error.
