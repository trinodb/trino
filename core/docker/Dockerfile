# syntax=docker/dockerfile:1-labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ghcr.io/airlift/jvmkill:latest AS jvmkill

# Download and extract JDK using a build platform to avoid tar extract issue while running in qemu
FROM --platform=$BUILDPLATFORM redhat/ubi10-minimal:latest AS jdk-download
ARG JDK_DOWNLOAD_LINK
ARG JDK_VERSION
ENV JAVA_HOME="/usr/lib/jvm/${JDK_VERSION}"

RUN \
    set -xeuo pipefail && \
    microdnf install -y tar gzip && \
    # Install JDK from the provided archive link \
    echo "Downloading JDK from ${JDK_DOWNLOAD_LINK}" && \
    mkdir -p "${JAVA_HOME}" && \
    curl -#LfS --retry 8 "${JDK_DOWNLOAD_LINK}" -o /jdktarfile && \
    tar -zxf /jdktarfile --strip 1 -C "${JAVA_HOME}" && \
    echo "Done"

# curl is required by health-check and is not part of the hardened core runtime. Donate it from
# the hardened curl image, whose shared libraries are byte-identical to the core runtime's.
FROM registry.access.redhat.com/hi/curl:latest AS curl

# tar (required to support `kubectl cp`) and less (used by the CLI as a pager) have no hardened
# image, so take just the binaries from UBI. Their dependencies -- libacl, libselinux, libattr,
# libpcre2, libtinfo and libc -- are all already present in the core runtime.
FROM redhat/ubi10:latest AS utils

RUN \
    set -xeuo pipefail && \
    dnf install --setopt install_weak_deps=false --nodocs -y less

# Red Hat Hardened Image (Project Hummingbird). Distroless, but unlike other distroless bases it
# retains bash and coreutils (required by the launcher wrapper script and by health-check),
# libstdc++ (required by snappy and duckdb) and zlib. Note this image publishes amd64 and arm64
# only -- there is no ppc64le.
FROM registry.access.redhat.com/hi/core-runtime:latest
ARG JDK_VERSION
ARG ARCH
ENV JAVA_HOME="/usr/lib/jvm/${JDK_VERSION}"
ENV PATH=$PATH:$JAVA_HOME/bin
ENV LANG=C.UTF-8
ENV CATALOG_MANAGEMENT=static
# the hardened image defaults to an unprivileged user; root is needed to lay down the image
USER root
COPY --from=jdk-download $JAVA_HOME $JAVA_HOME
COPY --from=curl /usr/bin/curl /usr/bin/curl
COPY --from=curl /usr/lib64/ /usr/lib64/
COPY --from=utils /usr/bin/tar /usr/bin/tar
COPY --from=utils /usr/bin/less /usr/bin/less

# the hardened image ships no shadow-utils, so declare the trino user directly
RUN \
    set -xeu && \
    echo 'trino:x:1000:1000::/home/trino:/bin/bash' >> /etc/passwd && \
    echo 'trino:x:1000:' >> /etc/group && \
    mkdir -p /home/trino /usr/lib/trino /data/trino && \
    chown -R "trino:trino" /home/trino /usr/lib/trino /data/trino

COPY --chown=trino:trino trino-cli.jar /usr/bin/trino
COPY --chown=trino:trino --exclude=bin/darwin-* --exclude=bin/linux-* trino-server /usr/lib/trino
COPY --chown=trino:trino trino-server/bin/linux-${ARCH} /usr/lib/trino/bin/linux-${ARCH}
COPY --chown=trino:trino default/etc /etc/trino
COPY --chown=trino:trino --from=jvmkill /libjvmkill.so /usr/lib/trino/bin

EXPOSE 8080
USER trino:trino
CMD ["/usr/lib/trino/bin/launcher", "run", "--etc-dir", "/etc/trino"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /usr/lib/trino/bin/health-check
