# Next Steps

## Immediate (pre-merge)

1. **User reviews and approves the first commit.**
   - Run `git diff --stat` / `git status` for a full tree view.
   - When approved, commit with a message covering the full M0–M16 build.
   - Nothing in the working tree is staged; no destructive operations will run without explicit approval.

2. **Hash-pin CI actions** — ✅ DONE. `.github/workflows/ci.yml` SHA-pins the latest majors
   (`actions/checkout@…9c091bb # v7.0.0`, `actions/setup-java@…1bcf9fb # v5.4.0`,
   `actions/cache@…55cc834 # v6.1.0`). `.github/dependabot.yml` covers `github-actions` only —
   Dependabot has no sbt support, so Scala/sbt deps are bumped manually (all at latest stable as of
   2026-06-30; sbt kept on the 1.x line since 2.0 is a breaking major).

3. **Wire a licensed Senzing dist into CI** (or document clearly why CI will always fail the SDK check).
   - The `ci.yml` `Verify Senzing SDK` step will fail on `ubuntu-latest` unless a self-hosted runner with `senzingsdk-runtime` installed is used, or a private fetch step is added.
   - Options: self-hosted runner, private artifact fetch, or leave CI in "compile-only-with-stub" mode and document it prominently.

## Short-term (next session)

4. **Validate DRAFT deployment tutorials on real clusters** — `docs/tutorials/spark-onprem.md`, `aws-emr.md`, and `databricks.md` are marked DRAFT and have not been tested on live infrastructure. Walk through each tutorial on its target platform; fix any discrepancies; remove the DRAFT warning header when validated. Update the `tutorials.md` FAQ entry to reflect status.

5. **Multi-node cluster validation** — run `AddUpdateJob` against a real Spark standalone cluster (or Databricks) with ≥2 executors on a shared PostgreSQL instance. Confirm per-JVM singleton behavior, no cross-task env conflicts.

6. **MSSQL / MySQL DDL** — add `SchemaApplier` DDL files for both dialects and exercise via `InitSpec` or a dedicated IT.

7. **`reinitialize()` concurrency certification** — confirm with Senzing support or the MCP that `reinitialize(id)` is safe when other threads hold the engine read-lock and are mid-verb. Document the answer in the threading FAQ.

8. **Dependabot review cadence** — after the first commit, dependabot will open its first PRs within a week. Establish a review/merge cadence; the 21-day cooldown gives breathing room.

## Medium-term

9. **Databricks cluster policy docs** — `docs/DATABRICKS.md` covers init scripts and cluster config. Validate against a real DBR 14+ cluster and update as needed.

10. **Redo dedicated worker** — the current `RedoJob` drains redo as a standalone Spark job. Evaluate whether a single-threaded dedicated redo worker (non-Spark) is preferable for high-throughput scenarios where per-task drain causes contention. See FAQ `redo/redo-strategy`.

11. **Performance benchmarks** — run `AddUpdateJob` on a representative dataset (≥1M records, 4+ executors) and capture records/sec, redo queue depth, and GC pressure. Add to `docs/DESIGN.md`.
