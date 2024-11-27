#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-a <ARCHITECTURES>] [-r <VERSION>]
Builds the Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to amd64,arm64,ppc64le
-r       Build the specified Trino release version, downloads all required artifacts
-j       Build the Trino release with specified JDK distribution
-x       Skip image tests
EOF
}

# Retrieve the script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

SOURCE_DIR="${SCRIPT_DIR}/../.."

ARCHITECTURES=(amd64 arm64 ppc64le)
TRINO_VERSION=

JDK_RELEASE=$(cat "${SOURCE_DIR}/core/jdk/current")
JDKS_PATH="${SOURCE_DIR}/core/jdk"

SKIP_TESTS=false

while getopts ":a:h:r:j:x" o; do
    case "${o}" in
        a)
            IFS=, read -ra ARCH_ARG <<< "$OPTARG"
            for arch in "${ARCH_ARG[@]}"; do
                if echo "${ARCHITECTURES[@]}" | grep -v -w "$arch" &>/dev/null; then
                    usage
                    exit 0
                fi
            done
            ARCHITECTURES=("${ARCH_ARG[@]}")
            ;;
        r)
            TRINO_VERSION=${OPTARG}
            ;;
        h)
            usage
            exit 0
            ;;
        j)
            JDK_RELEASE="${OPTARG}"
            ;;
        x)
           SKIP_TESTS=true
           ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))

function prop {
    grep "^${1}=" "${2}" | cut -d'=' -f2-
}

function check_environment() {
    if ! command -v jq &> /dev/null; then
        echo >&2 "Please install jq"
        exit 1
    fi
}

function jdk_download_link() {
  local RELEASE_PATH="${1}"
  local ARCH="${2}"

  if [ -f "${RELEASE_PATH}/${ARCH}" ]; then
    prop "distributionUrl" "${RELEASE_PATH}/${ARCH}"
  else
     echo "${ARCH} is not supported for JDK release ${RELEASE_PATH}"
     exit 1
  fi
}

check_environment

if [ -n "$TRINO_VERSION" ]; then
    echo "🎣 Downloading server and client artifacts for release version ${TRINO_VERSION}"
    for artifactId in io.trino:trino-server:"${TRINO_VERSION}":tar.gz io.trino:trino-cli:"${TRINO_VERSION}":jar:executable; do
        "${SOURCE_DIR}/mvnw" -C dependency:get -Dtransitive=false -Dartifact="$artifactId"
    done
    local_repo=$("${SOURCE_DIR}/mvnw" -B help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
    trino_server="$local_repo/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
    trino_client="$local_repo/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"
    chmod +x "$trino_client"
else
    TRINO_VERSION=$("${SOURCE_DIR}/mvnw" -f "${SOURCE_DIR}/pom.xml" --quiet help:evaluate -Dexpression=project.version -DforceStdout)
    echo "🎯 Using currently built artifacts from the core/trino-server and client/trino-cli modules and version ${TRINO_VERSION}"
    trino_server="${SOURCE_DIR}/core/trino-server/target/trino-server-${TRINO_VERSION}.tar.gz"
    trino_client="${SOURCE_DIR}/client/trino-cli/target/trino-cli-${TRINO_VERSION}-executable.jar"
fi

echo "🧱 Preparing the image build context directory"
WORK_DIR="$(mktemp -d)"
cp "$trino_server" "${WORK_DIR}/"
cp "$trino_client" "${WORK_DIR}/"
tar -C "${WORK_DIR}" -xzf "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
rm "${WORK_DIR}/trino-server-${TRINO_VERSION}.tar.gz"
cp -R bin "${WORK_DIR}/trino-server-${TRINO_VERSION}"
cp -R default "${WORK_DIR}/"

TAG_PREFIX="trino:${TRINO_VERSION}"

for arch in "${ARCHITECTURES[@]}"; do
    echo "🫙  Building the image for $arch with JDK ${JDK_RELEASE}"
    docker build \
        "${WORK_DIR}" \
        --progress=plain \
        --pull \
        --build-arg ARCH="${arch}" \
        --build-arg JDK_VERSION="${JDK_RELEASE}" \
        --build-arg JDK_DOWNLOAD_LINK="$(jdk_download_link "${JDKS_PATH}/${JDK_RELEASE}" "${arch}")" \
        --platform "linux/$arch" \
        -f Dockerfile \
        -t "${TAG_PREFIX}-$arch" \
        --build-arg "TRINO_VERSION=${TRINO_VERSION}"
done

echo "🧹 Cleaning up the build context directory"
rm -r "${WORK_DIR}"

echo -n "🏃 Testing built images"
if [[ "${SKIP_TESTS}" == "true" ]];then
  echo " (skipped)"
else
  echo
  source container-test.sh
  for arch in "${ARCHITECTURES[@]}"; do
      test_container "${TAG_PREFIX}-$arch" "linux/$arch"
      docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' "${TAG_PREFIX}-$arch"
  done
fi

