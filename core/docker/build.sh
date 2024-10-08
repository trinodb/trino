#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-a <ARCHITECTURES>] [-r <VERSION>]
Builds the Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to amd64,arm64,ppc64le
-b       Build the Trino release with the base image tag
-r       Build the specified Trino release version, downloads all required artifacts
-t       Build the Trino release with specified Temurin JDK release
EOF
}

# Retrieve the script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

SOURCE_DIR="${SCRIPT_DIR}/../.."

ARCHITECTURES=(amd64 arm64 ppc64le)
BASE_IMAGE_TAG=
TRINO_VERSION=

JDK_RELEASE=$(cat "${SOURCE_DIR}/core/jdk/current")
JDKS_PATH="${SOURCE_DIR}/core/jdk"

while getopts ":a:b:h:r:t" o; do
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
        b)
            BASE_IMAGE_TAG="${OPTARG}"
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

TRINO_VERSION=$("${SOURCE_DIR}/mvnw" -f "${SOURCE_DIR}/pom.xml" --quiet help:evaluate -Dexpression=project.version -DforceStdout)

echo "🧱 Preparing the image build context directory"
WORK_DIR="$(mktemp -d)"

TAG_PREFIX="uchimera.azurecr.io/cccs/ubi-minimal-jdk:${BASE_IMAGE_TAG}"

for arch in "${ARCHITECTURES[@]}"; do
    echo "🫙  Building the image for $arch with JDK ${JDK_RELEASE}"
    docker build \
        "${WORK_DIR}" \
        --progress=plain \
        --pull \
        --build-arg JDK_VERSION="${JDK_RELEASE}" \
        --build-arg JDK_DOWNLOAD_LINK="$(jdk_download_link "${JDKS_PATH}/${JDK_RELEASE}" "${arch}")" \
        --platform "linux/$arch" \
        -f Dockerfile \
        -t "${TAG_PREFIX}-$arch" \

done

echo "🧹 Cleaning up the build context directory"
rm -r "${WORK_DIR}"
