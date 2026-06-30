# Redistribution rules — do NOT publish the SDK

## What must never be committed or published?

This repo has **no rights to redistribute the Senzing SDK**. Never commit or publish:

- the built FAT jar (it bundles the SDK + native libs),
- the Senzing Java SDK jar (`sz-sdk.jar`),
- the native libraries,
- Senzing `.deb` packages.

## How is that enforced?

- `.gitignore` excludes the native staging dir (`src/main/resources/native/`), any `.debs/` staging
  dir, `build/libs/`, and `*-assembly-*.jar`.
- These artifacts are sourced from a **local licensed install** at build time.
- The jar is built **locally**; CI must **not** publish a bundled artifact, and it must **not** be
  attached to releases.

## What IS allowed?

A user's own config override files under `config/overrides/...` — those are their config, not SDK
redistribution, and are fine to commit and ship. See build/config-overrides.
