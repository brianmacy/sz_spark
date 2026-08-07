# Building sz_spark against the fleet's engine build (MANDATORY parity check)

**Why this doc exists:** on 2026-08-07 the Spark feeder measured ~20% slower than the native Rust
fleet. After ruling out host/executors/connections/network/contention, the cause was a **stale engine
build bundled in the FAT jar**: `sbt stageNatives` pulls natives from `SENZING_DIR` (default
`/opt/senzing`), which was `4.4.0.26151` (a 2-month-old release), while the fleet ran
`4.4.0.DEVELOPMENT` with newer DB-round-trip reductions. Same feeder, same DB, same concurrency — the
older engine just did more DB round-trips per record. Rebuilding against the fleet's engine restored
parity (`.142`-Spark CPU 44% → 55.5% ≈ `.141`-Rust 57%). CLAUDE.md already says it: **run the same
engine build on every node.** This is the procedure + the assertion that keeps it from regressing.

## THE RULE
Before any A/B or deploy, the feeder's engine `apiVersion` (from `get_stats`) MUST equal the fleet's.
Check both:
```
# feeder (once a get_stats sample logs):
docker logs sz-spark-feeder | grep -oE '"apiVersion":"[^"]+"' | tail -1
# fleet consumer:
docker logs compose-app-1  | grep -oE '"apiVersion":"[^"]+"' | tail -1   # (on .141)
```
If they differ, the jar was built against the wrong engine — rebuild (below).

## Build against the fleet engine (the DEVELOPMENT build)
`stageNatives` needs a `SENZING_DIR` in the `/opt/senzing` layout (`er/lib`, `er/resources`,
`er/sdk/java/sz-sdk.jar`, `data/`) whose **natives AND the sz-sdk.jar come from the SAME build** — the
JNI symbol set changes between builds (e.g. DEV removed `NativeEngineJni.getLastExceptionCode`), so an
old jar + new natives → `UnsatisfiedLinkError`.

The DEV engine is a G2 build output, but it lacks the Java SDK jar (Rust doesn't need it), so build the
jar too:
```
# 1. DEV native dist (a G2 build output; verify it's the fleet build)
DIST=/home/bmacy/dev/G2/dev/build/dist          # libSz.so should report 4.4.0.DEVELOPMENT_VERSION
strings $DIST/lib/libSz.so | grep -oE '4\.4\.0\.[A-Z0-9_]+' | sort -u

# 2. Build the matching DEV sz-sdk.jar from the SAME G2 source tree
cd /home/bmacy/dev/G2/dev/apps/g2/java/sz-sdk-java
mvn -q -DskipTests -Drevision=4.4.0.DEVELOPMENT package   # javadoc errors are cosmetic; target/sz-sdk.jar is produced

# 3. Assemble a hybrid SENZING_DIR: DEV natives + DEV jar (NOT /opt/senzing's stale jar)
rm -rf /tmp/dev-senzing; mkdir -p /tmp/dev-senzing/er/sdk/java
ln -sf $DIST/lib       /tmp/dev-senzing/er/lib
ln -sf $DIST/resources /tmp/dev-senzing/er/resources
ln -sf $DIST/data      /tmp/dev-senzing/data
ln -sf /home/bmacy/dev/G2/dev/apps/g2/java/sz-sdk-java/target/sz-sdk.jar /tmp/dev-senzing/er/sdk/java/sz-sdk.jar

# 4. Build the FAT jar against it, then VERIFY the bundled engine version
cd /home/bmacy/dev/sz_spark
SENZING_DIR=/tmp/dev-senzing sbt stageNatives assembly verifyAssembly
unzip -p target/scala-2.13/sz-spark-assembly.jar native/linux-x86_64/lib/libSz.so \
  | strings | grep -oE '4\.4\.0\.[A-Z0-9_]+' | sort -u   # MUST be DEVELOPMENT_VERSION, not 26151
```
Then build the image, deploy, and re-check the runtime `apiVersion` matches the fleet (THE RULE above).

## ⚠ Gotchas learned the hard way
- The per-build "native drift gate" (comparing a new sz_spark jar's native CRC manifest to the PRIOR
  sz_spark jar) only proves consistency *with the last sz_spark build* — it will NOT catch a mismatch
  vs the fleet, because every sz_spark build used the same stale `/opt/senzing`. Use the apiVersion
  check against the FLEET, not the drift gate, for parity.
- Old jar + new natives → `UnsatisfiedLinkError` (JNI surface differs). Jar and natives must match.
- `run-redo.sh` sources `env.sh` ONCE at loop start, so an `IMAGE` change needs a full loop restart;
  stale loops keep launching the old image and fight the new one over the `sz-spark-redo` name.
