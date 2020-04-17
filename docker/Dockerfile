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
    rm -rf /tmp/* /var/tmp/* && \
    groupadd presto --gid 1000 && \
    useradd presto --uid 1000 --gid 1000 && \
    mkdir -p /usr/lib/presto /data/presto && \
    chown -R "presto:presto" /usr/lib/presto /data/presto

ARG PRESTO_VERSION
COPY presto-cli-${PRESTO_VERSION}-executable.jar /usr/bin/presto
COPY --chown=presto:presto presto-server-${PRESTO_VERSION} /usr/lib/presto

EXPOSE 8080
USER presto:presto
ENV LANG en_US.UTF-8
CMD ["/usr/lib/presto/bin/run-presto"]
