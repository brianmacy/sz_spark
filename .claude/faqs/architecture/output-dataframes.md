# Output DataFrames per operation

## What does each operation emit?

Each task produces an output DataFrame:

- **add / update / delete** → the deduped list of affected entity IDs for the records the task
  processed.
- **search** → the search request paired with its results.

## Where do the "affected entity IDs" come from?

They are **not** a separate query. They come from the `AFFECTED_ENTITIES` field of the **WITH_INFO**
response. Call the mutating verb with the `SZ_WITH_INFO` flag, parse `AFFECTED_ENTITIES` out of the
returned JSON, and dedupe per task into the output DataFrame.

Get the exact V4 method names and the flag enum from the Senzing-MCP (`get_sdk_reference`) — do not
guess them.

## Good output vs. error output

Split each task's output: successfully processed records' affected entities go to the output
DataFrame; records that fail classification (bad input, retry-exhausted) go to a separate **error
DataFrame**. See the architecture/error-handling FAQ for the exception taxonomy that drives the split.
