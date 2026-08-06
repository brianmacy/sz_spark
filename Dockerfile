# sz_spark loader image — Spark 4.0.1 on Ubuntu 24.04 (Noble) + Java 21 + libpq5
#
# Purpose: run the sz_spark FAT jar (com.senzing.spark.*) as a Spark loader against
# your co-located SQL database (this image is wired for PostgreSQL). It packages the
# Spark 4.0.1 runtime the jar was compiled against plus the PostgreSQL client closure
# the bundled Senzing native plugin needs, so a single `docker run` can drive any of
# the jobs (add/update, delete, search, redo) or the streaming ingest path.
#
# ── Build context ───────────────────────────────────────────────────────────────
# This image needs the FAT jar `sz-spark-assembly.jar` present in the build context.
# Build the jar first from a host that has a licensed Senzing dist (see docs/BUILD.md):
#
#   export SENZING_DIR=/opt/senzing        # your local licensed install
#   sbt stageNatives
#   sbt -J-Xmx8g assembly                  # -> target/scala-2.13/sz-spark-assembly.jar
#   cp target/scala-2.13/sz-spark-assembly.jar ./sz-spark-assembly.jar
#
# then build the image (context = the repo root, where this Dockerfile and the copied
# jar live):
#
#   docker build -t sz_spark:latest .
#
# The FAT jar is NOT redistributable and is gitignored — it is only ever added to the
# build context locally, never committed or pushed to a registry you do not control.
#
# WHY Ubuntu 24.04 base (not the stock apache/spark 22.04 image):
#   the Senzing natives bundled in the FAT jar are compiled on Ubuntu 24.04 and
#   require glibc >= 2.38 (GlibcCheck floor 2.34). apache/spark:*-ubuntu is 22.04
#   (glibc 2.35) -> too old. So we rebase the Spark dist onto ubuntu:24.04.
#
# WHY Spark 4.0.1:
#   sz_spark build.sbt pins sparkVersion=4.0.1 (Provided), anchored to DBR 17.3 LTS.
#   The jar's Spark symbols are 4.0.1 -> running on a different Spark minor risks
#   NoSuchMethodError. Run on the SAME Spark the jar was compiled against.
#
# WHY libpq5 (THE SENZ0087 FIX):
#   native/linux-x86_64/lib/libpostgresqlplugin.so (bundled in the FAT jar) is
#   dlopen'd by soname and dynamically NEEDs libpq.so.5 + its closure (libssl3,
#   libgssapi-krb5, libldap, libsasl2, libgnutls, ...). stageNatives does NOT bundle
#   libpq (system lib). Without it the PG plugin fails at init:
#     SENZ0087 FAILED TO LOAD LIBRARY[libpostgresqlplugin.so]: libpq.so.5:
#             cannot open shared object file
#   apt installing libpq5 pulls the whole dependency chain automatically.
#
# WHY libpq 5.17 (THE CHUNKED-ROWS FIX):
#   libpostgresqlplugin.so calls PQsetChunkedRowsMode — the PG17 chunked-rows client
#   API. noble's stock libpq5 is 16.14 (libpq.so.5.16) which does NOT export that
#   symbol, so the plugin silently falls back to a slower non-chunked path and burns
#   extra client CPU. We therefore graft the libpq.so.5.17 file (17.10) out of
#   senzing/senzingsdk-runtime:4.3.3 (Debian 13/trixie) and repoint libpq.so.5 at it.
#   VERIFIED ABI-CLEAN against noble: libpq.so.5.17 requires max GLIBC_2.38 (noble
#   provides 2.39) and OPENSSL_3.0.0 (noble provides), and every NEEDED soname
#   (libssl.so.3, libgssapi_krb5.so.2, libldap.so.2, libkrb5.so.3, libsasl2.so.2,
#   libzstd.so.1, ...) is already supplied by noble's apt-installed libpq5 dependency
#   closure. Only the libpq object itself is the trixie build; all its deps stay
#   native-noble. dpkg still reports 16.14 (the grafted .17 file is not dpkg-tracked)
#   — resolution is by the libpq.so.5 soname.
#
# The FAT jar self-extracts native/linux-x86_64/{lib,data,resources} at runtime to
# $SENZING_EXTRACT_DIR/<sha>/ (default /var/tmp); LD_LIBRARY_PATH must point at the
# extracted <sha>/lib at JVM launch (dlopen-by-soname). The container therefore needs
# NO /opt/senzing install — everything (natives, support data, schema DDL, templates)
# ships inside the jar.

# ── Stage 1: pull the Spark 4.0.1 distribution from the official image ──────────
FROM apache/spark:4.0.1-scala2.13-java21-ubuntu AS spark-source

# ── Stage 1b: source of the libpq.so.5.17 object (Debian 13/trixie build) ───────
# Only the single libpq shared object is taken from here; its dependency closure is
# satisfied by noble's own libpq5 apt install below (see WHY libpq 5.17 above).
FROM senzing/senzingsdk-runtime:4.3.3 AS libpq-source

# ── Stage 2: Ubuntu 24.04 runtime (glibc 2.39) + JRE 21 + libpq5 ────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        libpq5 \
        procps \
        tini \
        bash \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Graft libpq 5.17 over noble's stock 5.16 and repoint the soname symlink. The 5.16
# file is left in place (harmless, dpkg-owned); only the libpq.so.5 soname — which the
# PG plugin dlopens — is moved to 5.17 so PQsetChunkedRowsMode resolves.
COPY --from=libpq-source /usr/lib/x86_64-linux-gnu/libpq.so.5.17 /usr/lib/x86_64-linux-gnu/libpq.so.5.17
RUN ln -sf libpq.so.5.17 /usr/lib/x86_64-linux-gnu/libpq.so.5 \
    && ldconfig

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy the entire Spark 4.0.1 distribution from the official image.
COPY --from=spark-source /opt/spark /opt/spark

# NOTE: S3A jars (hadoop-aws, aws bundle, spark-hadoop-cloud) are DELIBERATELY OMITTED
# — this image targets an on-prem PostgreSQL load, no object storage. All I/O is local
# FS / JDBC. Add them back if you deploy against cloud object storage.

# The sz_spark FAT jar (self-contained: Senzing natives + support data + schema DDL).
# Must exist in the build context — see "Build context" above.
COPY sz-spark-assembly.jar /opt/sz/sz-spark-assembly.jar

# Match the official image's spark user (uid/gid 185).
RUN groupadd -g 185 spark && useradd -u 185 -g 185 -m spark

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# The engine self-extracts its native payload here at first use (once per container).
ENV SENZING_EXTRACT_DIR=/var/tmp

WORKDIR /opt/spark

USER spark
