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
FROM centos:centos7
LABEL maintainer="Presto community <https://prestosql.io/community.html>"

ENV JAVA_HOME /usr/lib/jvm/java-11
RUN \
    set -xeu && \
    yum -y -q update && \
    yum -y -q install java-11-openjdk-devel less && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/* /var/tmp/*

RUN \
    set -xeu && \
    mkdir -p /data/presto

ARG PRESTO_VERSION
ARG PRESTO_LOCATION=presto-server/target/presto-server-${PRESTO_VERSION}.tar.gz
ADD ${PRESTO_LOCATION} /tmp

ARG CLIENT_VERSION=${PRESTO_VERSION}
ARG CLIENT_LOCATION=presto-cli/target/presto-cli-${CLIENT_VERSION}-executable.jar
ADD ${CLIENT_LOCATION} /usr/bin/presto

RUN \
    set -xeu && \
    if [[ ! -d /tmp/presto-server-${PRESTO_VERSION} ]]; then \
        tar -C /tmp -xzf /tmp/presto-server-${PRESTO_VERSION}.tar.gz && \
        rm /tmp/presto-server-${PRESTO_VERSION}.tar.gz; \
    fi && \
    cp -r /tmp/presto-server-${PRESTO_VERSION} /usr/lib/presto && \
    rm -r /tmp/presto-server-${PRESTO_VERSION} && \
    chmod 755 /usr/bin/presto

COPY presto-docker-image/bin /usr/lib/presto/bin
COPY presto-docker-image/etc /usr/lib/presto/default/etc

EXPOSE 8080

RUN \
    set -xeu && \
    groupadd presto --gid 1000 && \
    useradd presto --uid 1000 --gid 1000 && \
    chown -R "presto:presto" /usr/lib/presto /data/presto

USER presto:presto
CMD ["/usr/lib/presto/bin/run-presto"]
