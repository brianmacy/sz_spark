# FAT jar build and runtime native self-extraction

## Goal

A self-contained FAT jar that carries the Senzing Java SDK jar **and** all native libs/data/resources/
config, so a Spark executor with no Senzing install self-extracts and uses them at startup.

## Build sequence (`stageNatives` task, then `sbt assembly`)

1. **Source Senzing from a local licensed copy** — either `.deb`s under `$SENZING_DEB_DIR/<arch>/`
   (`dpkg-deb -x` to unpack without running the EULA-prompting preinst), or an installed `$SENZING_DIR`.
2. **Stage FOUR trees** into `src/main/resources/native/linux-<arch>/`: `lib/`, `data/`, `resources/`,
   and `config/` — the CONFIGPATH file set (`cfgVariant.json`, `customGn|On|Sn.txt`,
   `defaultGNRCP.config`), which lives under `er/resources/templates/`, **not** `er/resources/` root.
3. **Overlay** `config/overrides/{data,resources,config}/*` onto the staged stock trees (user config
   wins; ships empty — see `build/config-overrides`).
4. **`patchelf --set-rpath '$ORIGIN'` (WITHOUT `--force-rpath`) only on bundled siblings that lack a
   RUNPATH** (e.g. `libgcc_s.so.1`, any bundled `libssl`/`libcrypto`). **Do NOT patchelf `libSz.so`** —
   it is ~450 MB and already RUNPATH=`$ORIGIN`; `--force-rpath` would downgrade it to legacy `DT_RPATH`
   and change symbol-resolution precedence. patchelf alone does **not** fix dlopen-by-soname (step 6).
5. **`sbt -J-Xmx8g assembly`** (big heap for the native payload), then a verify task asserts the jar
   contains exactly the `readelf`-derived staged set.

## The dlopen-by-soname trap (most load-bearing detail)

A single `System.load("libSz.so")` is **not** sufficient, and `$ORIGIN`/RUNPATH does **not** make it
sufficient. Verified via `readelf -d`:

- `libSz.so` directly NEEDs `libssl.so.3`, `libcrypto.so.3`, `libm`, `libc` (RUNPATH=`$ORIGIN` covers
  these).
- `libszvec.so` and `libszzstd.so` are **dlopen'd by soname** (not in libSz's NEEDED) and need
  `libgcc_s.so.1`. The DB plugin (`libpostgresqlplugin.so`→`libpq.so.5`; MSSQL→ODBC; MySQL→
  `libmysqlclient`) is also dlopen'd by soname.
- **The dynamic linker applies a lib's RUNPATH only to its direct NEEDED — never to dlopen-by-soname.**

What the engine does (FAT jar):

1. `System.load()` the helpers in **dependency order** (`NativeLibLoader.loadHelpers`):
   `libgcc_s.so.1 → libszzstd.so → libszvec.so`, so each later dlopen returns an already-loaded handle.
2. Do **not** `System.load` `libSz.so`. The SDK's `Native*Jni` classes run `System.loadLibrary("Sz")`
   themselves, and JNI binds a library to the class loader of the class that loads it. The engine
   defines `com.senzing.sdk.core` (plus `SzCoreEnvironmentFactory`, the only sz_spark class naming it)
   child-first through a `NativeAwareClassLoader` whose `findLibrary(name)` returns
   `<extractDir>/lib/System.mapLibraryName(name)`; the JDK asks that before `java.library.path`, so
   libSz.so is found with **no `LD_LIBRARY_PATH`**. The SDK interfaces (`com.senzing.sdk.*`) stay on
   the normal class path, so callers use `SzEnvironment`/`SzEngine`/`SzException` unchanged. One such
   loader per JVM (`SzEnvironmentFactory.nativeAware`, called once by `SzEngineProvider`).

Without the loader, `System.load`-ing libSz.so by absolute path does NOT satisfy the SDK's own
`System.loadLibrary("Sz")` (it searches `java.library.path`) — that is why `LD_LIBRARY_PATH` used to be
mandatory. Setting `spark.executorEnv.LD_LIBRARY_PATH=<extractDir>/lib` at launch still works as an
alternative, and remains the way to resolve a DB client closure staged into `lib/`.

Verified (arm64 Ubuntu 24.04, SQLite): `NativeLoaderIT` and the FAT jar's `SelfCheck` both resolve
records with `LD_LIBRARY_PATH` unset; the SQLite plugin was found without being preloaded. Not yet
verified without `LD_LIBRARY_PATH`: PostgreSQL/MSSQL/MySQL plugins (their client libraries come from the
system loader path as before).

## What to bundle

- Bundle into `lib/` only NEEDED non-system libs a slim image may lack: `libgcc_s.so.1`, and
  `libssl.so.3` + `libcrypto.so.3` if the image lacks OpenSSL 3.
- **`libstdc++` is NOT needed** for this dist (verified: no GLIBCXX/CXXABI undefined symbols).
- GLIBC floor is **2.34** (verified) — enforce at startup and publish as a deploy precondition.
- The executor image must still provide the chosen **DB client's** full closure on the system loader
  path (or stage it into `lib/` for an air-gapped variant, relying on the launch `LD_LIBRARY_PATH`).

## Runtime self-extraction

- Detect FAT-jar mode via marker resource `native/linux-<arch>/lib/libSz.so`; pick `<arch>` from
  `os.arch`.
- Extract **once per node** into `$SENZING_EXTRACT_DIR` (default `/var/tmp`) under a SHA-256-of-jar dir,
  guarded by a file lock + `.ready` sentinel (atomic: unpack to `.tmp`, fsync, rename).
- Ordered helper `System.load` (above), then `SzEnvironmentFactory.nativeAware(libDir).build(...)`.
- Rewrite the **three distinct** PIPELINE paths in the engine config: `SUPPORTPATH`→`<extract>/data`,
  `RESOURCEPATH`→`<extract>/resources`, `CONFIGPATH`→`<extract>/config`. They are **not**
  interchangeable; pointing CONFIGPATH at `resources/` root silently degrades resolution.
- Clean stale extractions; remove on shutdown unless `SENZING_KEEP_EXTRACTED=true`.

## Key paths in an installed dist

- SDK jar: `/opt/senzing/er/sdk/java/sz-sdk.jar` (filename is `sz-sdk.jar`; not on Maven Central — add
  as an `unmanagedJars` local dependency).
- Native libs `/opt/senzing/er/lib`; data `/opt/senzing/data`; resources `/opt/senzing/er/resources`
  (schema DDL under `resources/schema/`, config templates + the CONFIGPATH file set under
  `resources/templates/`).
