# Record-transform plugin hook (load-time record rewrite)

## What and where

`SENZING_RECORD_TRANSFORM_PLUGIN` (+ `SENZING_RECORD_TRANSFORM_CONFIG`) on the executors make `Verbs.add` pass every add/update payload
through a C-ABI v1 plugin (the same libraries `sz_queue_combined_consumer` loads) before `addRecord`. Code: `transform/RecordTransform.scala`
(`PluginTransform`, `RecordTransformProvider`), Java `SzRtNative`/`SzRtException`, C shim `native/szrt/szrt_jni.c`. Full reference:
`docs/RECORD_TRANSFORM.md`.

## Design rules

- **Add/update only.** Delete, search, redo are never transformed (matches the Rust consumer; redo records are engine-generated).
- **Key from the original record.** DATA_SOURCE/RECORD_ID are not read from the transformed payload.
- **One handle per executor JVM**, called concurrently by all tasks (ABI requires thread-safety). Created lazily in
  `RecordTransformProvider`, destroyed in a shutdown hook. The shim never `dlclose`s the plugin (onnxruntime keeps state).
- **Error split:** plugin `SZ_RT_ERROR` -> `RecordTransformException` -> `ErrorTaxonomy` BadInput (dead-letter, task continues).
  Anything that prevents loading the plugin (missing file/symbol, ABI != 1, create failure) is systemic and fails the task: never
  load untransformed records into a repository whose config expects the plugin's features.

## Rejected approaches

- **JNA / jnr-ffi:** dual-licensed LGPL (owner rule: Apache/MIT/BSD only) and extra dependencies. **FFM (java.lang.foreign):** stable
  only from Java 22; Databricks DBR 17.3 runs Java 17. Chosen: ~150 lines of C, JNI, no dependency.
- **Bundling the plugin/models in the FAT jar:** models are large and the plugin has its own runtime libraries; install per node.
- **Mock plugin in tests:** the project rule is no mocks; the tests use the real example plugin of sz_queue_combined_consumer and the
  production semkey plugin.

## Gotchas

- Native methods live in a Java class: a Scala `object` would put them on `X$` and change the JNI symbol names.
- The shim is loaded from `-Dsz.rt.jni.lib` (dev/test) or the FAT-jar extraction dir (`SzEngineProvider.nativeLibDir`, set after the
  engine is built; the worker factory acquires the engine before building the verb, so the order holds).
- No JNI critical section is held during `sz_rt_transform` (input is copied to a native buffer first) so the GC is never blocked.
