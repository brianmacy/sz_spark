#!/usr/bin/env bash
# Run the SZ_IT integration tests against a REAL local Senzing engine using SQLite
# (single-process dev only — never for production/concurrent use). Requires a local licensed
# Senzing install at $SENZING_DIR (default /opt/senzing).
#
#   ./scripts/it-local.sh
#
# This is the suite that actually exercises entity resolution; the default `sbt test` is the fast
# plumbing suite (no engine).
set -euo pipefail

SENZING_DIR="${SENZING_DIR:-/opt/senzing}"
DB="${SZ_SQLITE_DB:-/tmp/szsqlite/G2C.db}"

[ -f "$SENZING_DIR/er/sdk/java/sz-sdk.jar" ] || { echo "ERROR: no licensed Senzing dist at $SENZING_DIR" >&2; exit 1; }

mkdir -p "$(dirname "$DB")"
cp -f "$SENZING_DIR/er/resources/templates/G2C.db.template" "$DB"  # fresh SQLite DB (schema only)

export SENZING_DIR
export LD_LIBRARY_PATH="$SENZING_DIR/er/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
CONFIGPATH="${SENZING_CONFIGPATH:-/etc/opt/senzing}"  # standard install config dir
export SENZING_ENGINE_CONFIGURATION_JSON="{\"PIPELINE\":{\"CONFIGPATH\":\"$CONFIGPATH\",\"RESOURCEPATH\":\"$SENZING_DIR/er/resources\",\"SUPPORTPATH\":\"$SENZING_DIR/data\"},\"SQL\":{\"CONNECTION\":\"sqlite3://na:na@$DB\"}}"
export SZ_IT=1
export JAVA_TOOL_OPTIONS="-Dspark.master=local[2]"   # job mains use cluster master in prod; local here

# 1) One-time init: register the default config + TEST data source into the SQLite DB.
sbt -batch "Test/runMain com.senzing.spark.jobs.InitJob dialect=sqlite dataSources=TEST"

# 2) Integration tests against the real engine (override the default IntegrationTest exclusion).
sbt -batch 'set Test/testOptions := Seq(Tests.Argument("-n","com.senzing.spark.IntegrationTest"))' \
  "testOnly com.senzing.spark.it.EngineIT"
