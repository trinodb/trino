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
FROM azul/zulu-openjdk-centos:11

ENV JAVA_HOME /usr/lib/jvm/zulu11
RUN \
    set -xeu && \
    yum -y -q install less && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    groupadd trino --gid 1000 && \
    useradd trino --uid 1000 --gid 1000 && \
    mkdir -p /usr/lib/trino /data/trino && \
    chown -R "trino:trino" /usr/lib/trino /data/trino

ARG TRINO_VERSION
COPY trino-cli-${TRINO_VERSION}-executable.jar /usr/bin/trino
COPY --chown=trino:trino trino-server-${TRINO_VERSION} /usr/lib/trino
COPY --chown=trino:trino default/etc /etc/trino

EXPOSE 8080
USER trino:trino
ENV LANG en_US.UTF-8
CMD ["/usr/lib/trino/bin/run-trino"]
