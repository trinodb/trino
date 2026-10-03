#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

mvn -f pom.xml -DskipTests package
mvn -f pom.xml -DskipTests dependency:copy-dependencies -DincludeScope=runtime

mkdir -p examples/trino/plugin
cp target/ydb-trino-0.1.0.jar examples/trino/plugin
cp target/dependency/*.jar examples/trino/plugin

cd examples
docker-compose down
docker-compose up -d
