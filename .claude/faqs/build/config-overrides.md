# Baking customized Senzing config into the FAT jar

## Can a user ship their own customized Senzing config in the jar?

Yes — this is a supported DevOps hook. The build stages the **stock** Senzing `data/`, `resources/`, and
`config/` (the CONFIGPATH file set), then **overlays** any files from a committed override directory
(`config/overrides/{data,resources,config}/`) on top, so a customized config template or resource file
wins over the stock one.

## What about users who don't customize anything?

Most don't. The overlay step ships wired up but **empty** — an empty override directory is a no-op and
the stock files ship unchanged. A user drops customized files into `config/overrides/...` and rebuilds.

## Are these override files OK to commit?

Yes. They are the **user's own** config, not Senzing SDK binaries, so they are exempt from the
no-redistribution rule — fine to commit to the repo and ship in the jar. (Contrast: the SDK jar,
native libs, `.deb`s, and the built FAT jar must never be committed/published — see
deployment/redistribution.)

## Do customizations take effect automatically at runtime?

For **runtime resources** (e.g. generic thresholds, resource files the engine reads): yes. Because the
runtime rewrites `RESOURCEPATH`/`SUPPORTPATH`/`CONFIGPATH` to the extracted dir, the engine reads the
overlaid versions with no extra flag.

For the **registered config** (data sources, features, ER rules): no. Bundling a changed config
template does not re-register the default config in the database — that must also be applied by the
one-time init step (see architecture/initialization-separation).

Use the Senzing-MCP to learn what each customizable file actually controls.
