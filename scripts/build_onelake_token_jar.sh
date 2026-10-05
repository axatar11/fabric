#!/usr/bin/env bash
# Rebuild common/jars/onelake-cli-token-provider.jar (Java 11 bytecode for Spark/Java 17).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/common/jars-src/org/fabric/onelake/EnvAccessTokenProvider.java"
OUT="$ROOT/common/jars/onelake-cli-token-provider.jar"
TMP="${TMPDIR:-/tmp}/hadoop-azure-deps"
mkdir -p "$TMP"
HA="$TMP/hadoop-azure-3.3.4.jar"
HC="$TMP/hadoop-common-3.3.4.jar"
for url in \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar"
do
  f="$TMP/$(basename "$url")"
  if [[ ! -f "$f" ]]; then curl -sL "$url" -o "$f"; fi
done
HA="$TMP/hadoop-azure-3.3.4.jar"
HC="$TMP/hadoop-common-3.3.4.jar"
WORKDIR="$ROOT/common/jars-src"
javac --release 8 -cp "$HA:$HC" -d "$WORKDIR" "$SRC"
jar cf "$OUT" -C "$WORKDIR" org/fabric/onelake/EnvAccessTokenProvider.class
echo "Wrote $OUT (Java 8 bytecode)"
