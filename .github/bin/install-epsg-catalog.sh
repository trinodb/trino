#!/usr/bin/env bash
set -euo pipefail

# Temporary integration wiring until the first epsg-catalog release is published.
project_directory=$(git rev-parse --show-toplevel)
catalog_ref=$(cat "${project_directory}/.github/epsg-catalog-ref")
if [[ ! "${catalog_ref}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Invalid epsg-catalog commit: ${catalog_ref}" >&2
    exit 1
fi
catalog_directory="${RUNNER_TEMP:?}/epsg-catalog"
if [ ! -d "${catalog_directory}/.git" ]; then
    git init "${catalog_directory}"
    git -C "${catalog_directory}" remote add origin https://github.com/trinodb/epsg-catalog.git
fi
git -C "${catalog_directory}" fetch --depth=1 origin "${catalog_ref}"
git -C "${catalog_directory}" checkout --detach "${catalog_ref}"

# Use the library's Maven configuration, rather than Trino's MAVEN_ARGS.
(
    cd "${catalog_directory}"
    env -u MAVEN_ARGS ./mvnw -B -ntp -DskipTests -Dair.check.skip-all=true \
        -Dmaven.source.skip=true install "$@"
)
