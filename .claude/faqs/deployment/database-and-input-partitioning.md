# Database choice and input partitioning

## What database backs Senzing here?

A **co-located SQL database** — PostgreSQL, MSSQL, or MySQL. This is the deployment pattern Senzing's
largest commercial customers use with Spark/Databricks.

**Never use SQLite for anything real.** SQLite has no concurrent writes and cannot serve many Spark
tasks at once; it is dev-only and single-process.

## Why does input partitioning matter for entity resolution?

**Do not sort or group input by a resolution key** (name, address, zip, or any field that drives
matching). When records that resolve to the same entity arrive back-to-back, the engine experiences
heavy lock contention on that entity, and throughput collapses.

- Partition input **randomly** (shuffle).
- **Never repartition by a PII / resolution grouping key** to "optimize" — it does the opposite.

## Deployment locality

The deployment must be **completely local**: no runtime dependency on external Senzing services beyond
the bundled SDK and the co-located database. (The Senzing-MCP is a development-time aid, not a runtime
dependency.)
