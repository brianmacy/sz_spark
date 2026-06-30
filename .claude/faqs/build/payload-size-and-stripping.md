# FAT-jar payload size and stripping

## How big is the FAT jar?

About **265 MB** (compressed in the jar) from a **~464 MB** staged native payload — NOT multi-GB.
Measured on a current installed dist:

| Tree | Size | Notes |
|---|---|---|
| `er/lib` | 443 MB | almost entirely **`libSz.so` ≈ 430 MB** (the engine); every other lib is <7 MB |
| `data` | 14 MB | the 13 MB address ONNX model (older dists shipped a ~2.2 GB CRF tree — gone) |
| `er/resources` | 7.3 MB | mostly `cfgVariant.json` (6.5 MB) |

`libSz.so` is the irreducible floor — it is the entity-resolution engine.

## Are the binaries stripped?

**Yes, already.** `file libSz.so` reports `stripped`, and `strip --strip-unneeded` removes **0 bytes**.
The "unstripped, inflated 10–25×" warning in older notes referred to a *stale Docker image*, not the
current dist.

The build's `stageNatives` task still runs `strip --strip-unneeded` on every bundled `.so` — a **no-op
on this dist**, but a cheap safety net so an unstripped/future dist can't bloat the jar. It strips the
staged COPIES only, never `$SENZING_DIR`.

## What gets bundled

Four trees into `native/linux-<arch>/{lib,data,resources,config}` (`config` = the CONFIGPATH file set
from `resources/templates/`). All DB plugins ship (sqlite/postgresql/aurora/mssql ≈ a few MB total —
negligible). `libSz.so` is left un-`patchelf`'d (already RUNPATH=$ORIGIN); only bundled siblings get
`--set-rpath '$ORIGIN'` (no `--force-rpath`).

## Assembly heap

`-J-Xmx8g` was over-cautious for a ~465 MB payload; **`-J-Xmx4g` is plenty**. Build with
`sbt stageNatives -J-Xmx4g assembly`; verify with `verifyAssembly` (or `FatJarIT`).
