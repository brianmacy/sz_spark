# Per-record error taxonomy

## How should a task classify Senzing exceptions?

So each task emits a clean "good output" vs. "error" DataFrame, classify per record:

- **`SzBadInputException`** → bad row. Route to the error DataFrame and continue. A malformed record
  must never fail the whole task.
- **`SzRetryableException`** → transient. Retry in-task with jittered exponential backoff up to a
  budget; if the budget is exhausted, route to the error DataFrame.
- **`SzConfigurationException` / `SzBadInputException`** → config-relevant. First run the config-drift
  check (active vs. default config id); if the config changed, retry the record **once** on the
  refreshed config before classifying it as bad input.
- **Other `SzException` / `Error`** → systemic. Fail the task **loudly** — do not swallow it. A
  silent failure that drops records is worse than a crash.

## Why retry-once on a config change?

A record can be rejected (e.g. unknown data source) only because this executor's engine is running an
older config than another process just activated. Reinitializing to the current default config and
retrying once distinguishes "genuinely bad input" from "stale config" without failing the task.
