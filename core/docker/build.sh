#!/usr/bin/env bash

set -xeuo pipefail

usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-a <ARCHITECTURES>] [-r <VERSION>]
Builds the Trino Docker image

-h       Display help
-a       Build the specified comma-separated architectures, defaults to amd64,arm64,ppc64le
-r       Build the specified Trino release version, downloads all required artifacts
-j       Build the Trino release with specified Temurin JDK release
EOF
}

# Retrieve the script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

SOURCE_DIR="${SCRIPT_DIR}/../.."

ARCHITECTURES=(amd64 arm64 ppc64le)
BASE_IMAGE_TAG=
TRINO_VERSION=

<<<<<<< HEAD
while getopts ":a:h:r:j:t:" o; do
=======
# Must match https://api.adoptium.net/q/swagger-ui/#/Release%20Info/getReleaseNames
TEMURIN_RELEASE=$(cat "${SOURCE_DIR}/.temurin-release")

while getopts ":a:h:r:t:" o; do
>>>>>>> 444
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
        t)
            TEMURIN_RELEASE="${OPTARG}"
            ;;
        t)
            BASE_IMAGE_TAG="${OPTARG}"
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))

function check_environment() {
    if ! command -v jq &> /dev/null; then
        echo >&2 "Please install jq"
        exit 1
    fi
}

function temurin_download_link() {
  local RELEASE_NAME="${1}"
  local ARCH="${2}"

  case "${ARCH}" in
    arm64)
      echo "https://api.adoptium.net/v3/binary/version/${RELEASE_NAME}/linux/aarch64/jdk/hotspot/normal/eclipse?project=jdk"
    ;;
    amd64)
      echo "https://api.adoptium.net/v3/binary/version/${RELEASE_NAME}/linux/x64/jdk/hotspot/normal/eclipse?project=jdk"
    ;;
    ppc64le)
      echo "https://api.adoptium.net/v3/binary/version/${RELEASE_NAME}/linux/ppc64le/jdk/hotspot/normal/eclipse?project=jdk"
    ;;
  *)
    echo "${ARCH} is not supported for Docker image"
    exit 1
    ;;
  esac
}

check_environment

TRINO_VERSION=$("${SOURCE_DIR}/mvnw" -f "${SOURCE_DIR}/pom.xml" --quiet help:evaluate -Dexpression=project.version -DforceStdout)

echo "🧱 Preparing the image build context directory"
WORK_DIR="$(mktemp -d)"

TAG_PREFIX="uchimera.azurecr.io/cccs/ubi-minimal-jdk:${BASE_IMAGE_TAG}"

for arch in "${ARCHITECTURES[@]}"; do
    echo "🫙  Building the image for $arch with Temurin Release ${TEMURIN_RELEASE}"
    docker build \
        "${WORK_DIR}" \
        --progress=plain \
        --pull \
        --build-arg JDK_VERSION="${TEMURIN_RELEASE}" \
        --build-arg JDK_DOWNLOAD_LINK="$(temurin_download_link "${TEMURIN_RELEASE}" "${arch}")" \
        --platform "linux/$arch" \
        -f Dockerfile \
        -t "${TAG_PREFIX}-$arch" \

done

echo "🧹 Cleaning up the build context directory"
rm -r "${WORK_DIR}"
