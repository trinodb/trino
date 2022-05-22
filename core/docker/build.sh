#!/usr/bin/env bash

set -euo pipefail

ARCHITECTURES=(amd64 arm64)
TRINO_VERSION=
TAG_PREFIX=
INCLUDE_PLUGINS=
EXCLUDE_PLUGINS=

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-a <ARCHITECTURES>] [-r <REVISION>]
Builds the Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to: ${ARCHITECTURES[*]}
-r       Build the specified Trino revision (version), downloads all required artifacts
-t       Tag prefix for the result image, defaults to trino:\$TRINO_VERSION
-i       Comma-separated list of plugins to incluude, defaults to all
-e       Comma-separated list of plugins to exclude, defaults to none
EOF
}

while getopts ":a:h:r:t:i:e:" o; do
    case "${o}" in
        a)
            IFS=, read -ra ARCHITECTURES <<< "$OPTARG"
            ;;
        r)
            TRINO_VERSION=${OPTARG}
            ;;
        t)
            TAG_PREFIX=${OPTARG}
            ;;
        i)
            INCLUDE_PLUGINS=${OPTARG}
            ;;
        e)
            EXCLUDE_PLUGINS=${OPTARG}
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))

if [ -n "$INCLUDE_PLUGINS" ] && [ -n "$EXCLUDE_PLUGINS" ]; then
    echo >&2 "Can't specify both -i and -e"
    exit 1
fi

if [ -n "$INCLUDE_PLUGINS" ] || [ -n "$EXCLUDE_PLUGINS" ]; then
    if [ -z "$TAG_PREFIX" ]; then
        echo >&2 "Tag prefix must be specified with the '-t' option when building a custom image with only a subset of plugins"
        exit 1
    fi
fi

SOURCE_DIR="../.."
WORK_DIR="$(mktemp -d)"

# Retrieve the script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

pushd ${SOURCE_DIR}
if [ -n "$TRINO_VERSION" ]; then
    for artifactId in io.trino:trino-server:"${TRINO_VERSION}":provisio io.trino:trino-cli:"${TRINO_VERSION}":jar:executable; do
        ./mvnw dependency:get -Dtransitive=false -Dartifact="$artifactId"
    done
    LOCAL_REPO=$(./mvnw -B help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
    cp "$LOCAL_REPO/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz" "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
    cp "$LOCAL_REPO/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar" "${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar"
    chmod +x "${WORK_DIR}/trino-cli-${TRINO_VERSION}-executable.jar"
else
    # Move to the root directory to run maven for current version.
    TRINO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)

    cp "core/trino-server/target/trino-server-${TRINO_VERSION}.tar.gz" "${WORK_DIR}"
    cp "client/trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar" "${WORK_DIR}"
fi
popd >/dev/null

tar -C "${WORK_DIR}" -xzf "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
rm "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
cp -R bin "${WORK_DIR}/trino-server-${TRINO_VERSION}"
cp -R default "${WORK_DIR}/"

base="${WORK_DIR}/trino-server-${TRINO_VERSION}"
etc="${WORK_DIR}/default/etc"
declare -A catalogs
for file in "$etc"/catalog/*.properties; do
    [ -f "$file" ] || continue
    if line=$(grep '^connector.name=' "$file") && plugin=$(cut -d= -f2 <<<"$line"); then
        catalogs[$plugin]=$file
    fi
done
if [ -n "${INCLUDE_PLUGINS:-}" ]; then
    mkdir -p "$base/plugin.enabled" "$etc/catalog.enabled"
    IFS=, read -ra names <<< "$INCLUDE_PLUGINS"
    for name in "${names[@]}"; do
        mv "$base/plugin/$name" "$base/plugin.enabled/$name"
        [ -z "${catalogs[$name]:-}" ] || mv "${catalogs[$name]}" "$etc/catalog.enabled/$name.properties"
    done
    rm -rf "$etc/catalog" "$base/plugin"
    mv "$etc/catalog.enabled" "$etc/catalog"
    mv "$base/plugin.enabled" "$base/plugin"
elif [ -n "${EXCLUDE_PLUGINS:-}" ]; then
    mkdir -p "$base/plugin.disabled" "$etc/catalog.disabled"
    IFS=, read -ra names <<< "$EXCLUDE_PLUGINS"
    for name in "${names[@]}"; do
        rm -rf "$base/plugin/$name"
        [ -z "${catalogs[$name]:-}" ] || rm -rf "${catalogs[$name]}"
    done
fi

TAG_PREFIX="${TAG_PREFIX:-trino:$TRINO_VERSION}"

for arch in "${ARCHITECTURES[@]}"; do
    docker build \
        "${WORK_DIR}" \
        --pull \
        --platform "linux/$arch" \
        -f Dockerfile \
        -t "${TAG_PREFIX}-$arch" \
        --build-arg "TRINO_VERSION=${TRINO_VERSION}"
done

rm -r "${WORK_DIR}"

source container-test.sh

for arch in "${ARCHITECTURES[@]}"; do
    test_container "${TAG_PREFIX}-$arch" "linux/$arch"
    docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' "${TAG_PREFIX}-$arch"
done
