# Record-transform plugin (load-time record rewrite)

sz_spark can run the same **record-transform plugin** as `sz_queue_combined_consumer --record-transform-plugin` (for example
`libsz_semkey_transform.so`, which adds the semantic-key embeddings): every add/update record is passed through the plugin just
before `addRecord`. The plugin contract is the C ABI version 1 in `sz_queue_combined_consumer` `crates/transform-abi/src/lib.rs`
(`sz_rt_abi_version`, `sz_rt_create`, `sz_rt_transform`, `sz_rt_free`, `sz_rt_destroy`).

## Configure

Set on the **executors** (and the driver for local runs), with the same names as the Rust consumer:

| Variable | Default | Meaning |
|---|---|---|
| `SENZING_RECORD_TRANSFORM_PLUGIN` | unset (no hook, no overhead) | absolute path of the plugin `.so`, present on **every** node |
| `SENZING_RECORD_TRANSFORM_CONFIG` | empty | opaque string passed to `sz_rt_create` (the plugin's JSON, e.g. its model directories) |

```bash
spark-submit ... \
  --conf spark.executorEnv.SENZING_RECORD_TRANSFORM_PLUGIN=/opt/semkey/libsz_semkey_transform.so \
  --conf spark.executorEnv.SENZING_RECORD_TRANSFORM_CONFIG='{"person_model_dir":"/opt/semkey/models/person","biz_model_dir":"/opt/semkey/models/biz","max_sessions":1}' \
  ... com.senzing.spark.jobs.AddUpdateJob input=...
```

The plugin, its runtime libraries (for example `libonnxruntime.so.1` next to it, `$ORIGIN` rpath) and any model files are **not** part
of the FAT jar: install them on each node (image, init script or shared mount) at the path given above. A repository that was loaded
with the plugin's features configured (for example the semantic keys) must keep loading with it: a node without the plugin fails
the task rather than load untransformed records.

## Behavior

- Add/update only. Delete, search and redo records are never transformed.
- `DATA_SOURCE` and `RECORD_ID` are taken from the original record; the plugin only rewrites the payload.
- One plugin handle per executor JVM, created on the first record, destroyed at JVM shutdown. `sz_rt_transform` is called
  concurrently from all task threads on that handle (the ABI requires it to be thread-safe).
- The plugin returns **unchanged** (record used as is), **replaced** (new UTF-8 record) or **error**. An error dead-letters that
  record as `BAD_INPUT` with the plugin's message (the original payload is kept in the error row) and the task continues.
- A configured plugin that cannot be loaded (missing file, missing symbol, ABI version other than 1, `sz_rt_create` failure) fails the
  task loudly (systemic); it never falls back to loading untransformed records.

## How it is built

`native/szrt/szrt_jni.c` is a ~150-line JNI shim (`dlopen` + the five symbols) behind `com.senzing.spark.transform.SzRtNative`. No
third-party FFI library (the JVM on Databricks DBR 17.3 is Java 17, which has no stable foreign-function API). `stageNatives`
compiles it with gcc against the build JDK's headers (`-Wall -Wextra -Werror`) into `native/linux-<arch>/lib/libszrt_jni.so`, so the
FAT jar carries it and the existing self-extraction puts it next to `libSz.so`. Plain-JVM runs and tests point at it with
`-Dsz.rt.jni.lib=<path>` (`sbt buildSzRtJni` builds `target/szrt/libszrt_jni.so`; `Test` sets the property).

## Tests

`RecordTransformIT` (tag `IntegrationTest`, real plugins, no mocks): unchanged / replaced / error, UTF-8, 16 threads x 2000 records on
one handle, a path that is not a plugin, the worker's `BAD_INPUT` dead-letter, and an end-to-end `AddCore` run on the local SQLite
engine that proves the loaded entity carries the plugin's field and a rejected record is absent. It FAILS (does not skip) when the
plugin path is missing.

```bash
(cd ~/dev/sz_queue_combined_consumer && cargo build -p sz-record-transform-example)
EX=~/dev/sz_queue_combined_consumer/target/debug/libsz_record_transform_example.so
# plugin-level tests
SZRT_EXAMPLE_PLUGIN=$EX sbt 'set Test/testOptions := Seq(Tests.Argument("-n","com.senzing.spark.IntegrationTest"))' 'testOnly *RecordTransformIT'
# + engine end to end: first the scripts/it-local.sh environment (SQLite engine, InitJob dataSources=TEST, SZ_IT=1), and
SENZING_RECORD_TRANSFORM_PLUGIN=$EX SENZING_RECORD_TRANSFORM_CONFIG='{"PHONE_NUMBER":"702-555-0100"}'
# optional: SZRT_SEMKEY_PLUGIN / SZRT_SEMKEY_CONFIG run the production semkey plugin through the shim
```
