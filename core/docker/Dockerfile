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
ARG AZUL_DOCKER_TAG=17.0.3
FROM azul/zulu-openjdk:$AZUL_DOCKER_TAG

ENV JAVA_HOME /usr/lib/jvm/zulu17

RUN \
    set -xeu && \
    apt-get update -q && \
    apt-get install -y -q less python curl && \
    rm -rf /var/lib/apt/lists/* && \
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
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /usr/lib/trino/bin/health-check
