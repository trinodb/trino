#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-a <ARCHITECTURES>] [-r <REVISION>]
Builds the Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to amd64,arm64
-r       Build the specified Trino revision (version), downloads all required artifacts
EOF
}

ARCHITECTURES=(amd64 arm64)
TRINO_VERSION=

while getopts ":a:h:r:" o; do
    case "${o}" in
        a)
            IFS=, read -ra ARCHITECTURES <<< "$OPTARG"
            ;;
        r)
            TRINO_VERSION=${OPTARG}
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

TAG_PREFIX="trino:${TRINO_VERSION}"

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
