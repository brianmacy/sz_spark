# Building the Senzing-on-Spark FAT jar

How to package the **Senzing V4 SDK** and its native libraries into a single self-extracting
**FAT jar** (uber jar) for deployment to **Spark / Databricks** executors. This is the standalone
"how do I package Senzing into a Spark FAT jar?" guide; see [`DESIGN.md`](DESIGN.md) §4 for the
rationale and [`RUNBOOK.md`](RUNBOOK.md) for deploy preconditions.

## Why a FAT jar

The Senzing Java SDK jar (`sz-sdk.jar`) and the native libraries (`libSz.so` and friends) are **not on
Maven Central** and cannot be redistributed. A Spark executor has no `/opt/senzing` install, so the job
must carry the engine with it: the FAT jar bundles the SDK jar + native libs + data + resources +
config, and **self-extracts them on each executor node at startup**.

## Prerequisites

- **JDK 17 or 21** (Temurin recommended). Match the JDK of your target Databricks runtime.
- **sbt** (1.x; the project pins the exact version in `project/build.properties`).
- **A local licensed Senzing dist** (`senzingsdk-runtime`) — installed at `$SENZING_DIR` (default
  `/opt/senzing`), or unpacked `.deb`s at `$SENZING_DEB_DIR/<arch>/*.deb`. This is the source of the
  SDK jar and natives; it is **never committed**.
- Target stack: **Spark 4.0 / Scala 2.13** (Spark 4 dropped Scala 2.12). Pin `scalaVersion` to the
  exact Databricks runtime — a 2.12↔2.13 mismatch under `provided` scope is a runtime
  `NoSuchMethodError`.

## Dependency wiring

- **Spark deps are `% "provided"`** — Spark is already on the cluster; bundling it bloats the jar and
  risks version clashes.
- **The Senzing SDK jar is an unmanaged local dependency**, not a Maven coordinate:
  `unmanagedJars += <SENZING_DIR>/er/sdk/java/sz-sdk.jar` (the filename is `sz-sdk.jar`, **not**
  `sz-sdk-java.jar`).
- Jackson and the DB JDBC driver (for `InitJob`'s schema step) are bundled.

## Build commands

```bash
export SENZING_DIR=/opt/senzing     # local licensed install (source of SDK jar + natives)

sbt compile                         # compile against the local SDK jar
sbt test                            # full non-integration suite (unit + spark-local, fake engine)
sbt scalafmtAll                     # format
sbt stageNatives                    # stage native libs/data/resources/config into the build (gitignored)
sbt -J-Xmx8g assembly               # build the FAT jar (big heap for the ~464 MB native payload)
```

The assembled jar lands at `target/scala-2.13/sz-spark-assembly.jar` and is **~265 MB** (the ~464 MB
staged payload compresses into the jar) — not multi-GB.

### Why the big heap (`-Xmx8g`)

`sbt assembly` holds the native payload in memory while building the uber jar. The payload is dominated
by `libSz.so` (~430 MB, already stripped). A modest heap will OOM the assembly task. **`-J-Xmx4g` is
the documented minimum; `-J-Xmx8g` is recommended for headroom** — this guide and the Databricks build
use `8g`.

## `stageNatives` — what gets bundled

`stageNatives` runs before `assembly` and its output is **gitignored** (`src/main/resources/native/`).
It stages four **distinct** trees into `src/main/resources/native/linux-<arch>/`:

| Tree | Bundled from | Runtime PIPELINE key |
|---|---|---|
| `lib/` | `er/lib/*.so` + the NEEDED non-system libs a slim image may lack | (loaded via `LD_LIBRARY_PATH` + ordered `System.load`) |
| `data/` | `data/` | `SUPPORTPATH` |
| `resources/` | `er/resources/` | `RESOURCEPATH` |
| `config/` | `er/resources/templates/` (`cfgVariant.json`, `customGn|On|Sn.txt`, `defaultGNRCP.config`) | `CONFIGPATH` |

The three PIPELINE keys point at three **different** subdirectories — do not collapse them.

It then:
1. **Overlays** `config/overrides/{data,resources,config}/*` onto the stock trees (your config wins).
   These override files are your own config and are **exempt** from the no-redistribution rule — fine
   to commit and ship.
2. Bundles only the **NEEDED non-system** libs a slim image may lack (`libgcc_s.so.1`; `libssl.so.3` +
   `libcrypto.so.3` if the image lacks OpenSSL 3). `libstdc++` is **not** needed by this dist.
3. Runs `patchelf --set-rpath '$ORIGIN'` (**without** `--force-rpath`) **only on bundled siblings that
   lack a RUNPATH**. **Never patchelf `libSz.so`** — it already has `RUNPATH=$ORIGIN`, and
   `--force-rpath` would downgrade it to legacy `DT_RPATH` and change loader precedence.

## What is bundled vs. never committed

| Bundled into the FAT jar (build time, gitignored) | Never committed / never published |
|---|---|
| `sz-sdk.jar`, `libSz.so` + siblings, `data/`, `resources/`, config templates | The SDK jar, native libs, `.deb`s, **and the FAT jar itself** |

> **Redistribution rule:** this repo has no rights to redistribute the Senzing SDK. The native staging
> dir (`src/main/resources/native/`), any `.debs/` dir, and `*-assembly-*.jar` are gitignored. Build
> the jar **locally** and upload it to **your own** workspace/volume; never attach it to a release or
> have CI publish it.

## Verifying the bundle

```bash
SZ_IT=1 sbt "testOnly *FatJarIT -- -n com.senzing.spark.IntegrationTest"
```

`FatJarIT` runs the jar inside a slim container (`temurin:21-jre`, **no** `/opt/senzing`) and confirms
it self-extracts and resolves entities — the real proof that the native bundling and the
dlopen-by-soname fix work on an executor-like image. See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for
the native-loading failure modes this guards against.

## Multi-architecture

The default build ships **x86_64 only** (the licensed dist available). Build `aarch64` only if
`stageNatives` sources a real aarch64 dist — do not advertise an arch you have not staged.
