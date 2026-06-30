# Senzing SDK facts — use the Senzing-MCP

## Where do I get authoritative Senzing API facts?

From the connected **Senzing-MCP** server — it is the **exclusive source of truth** for Senzing facts.
LLM training data about Senzing is frequently wrong (attribute names, method signatures, V3-vs-V4 APIs,
package paths). This FAQ tells you *which patterns this project uses*; the Senzing-MCP tells you the
*exact current API*.

Before writing or reviewing Senzing code:

- Exact V4 signatures → `get_sdk_reference` / `generate_scaffold`.
- JSON mappings → `mapping_workflow` (not hand-coded).
- Init / threading / redo / deploy approaches → `search_docs` with `category: anti_patterns`.
- Never simulate or fabricate entity-resolution results.

## Which SDK version?

**V4** (`SzEnvironment` / `SzEngine` / `SzCoreEnvironment`), not V3 (`G2Engine`).

## Confirmed V4 Java SDK shape

```
com.senzing.sdk.{SzEngine, SzEnvironment}
com.senzing.sdk.core.SzCoreEnvironment

SzCoreEnvironment.newBuilder()
    .instanceName(name)
    .settings(engineConfigJson)
    .verboseLogging(verbose)
    .build();

env.getEngine();
env.getActiveConfigId();
env.getConfigManager().getDefaultConfigId();
env.reinitialize(configId);
env.destroy();
```

Verify any other signature (add/delete/search/redo verbs, the `SZ_WITH_INFO` flag) against the
Senzing-MCP, not from memory.

## Where is the SDK jar?

Not on Maven Central (those artifacts are stale/community). It ships with `senzingsdk-runtime` at
`/opt/senzing/er/sdk/java/sz-sdk.jar`.
