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
FROM centos:centos8
LABEL maintainer="Presto community <https://prestosql.io/community.html>"

ENV JAVA_HOME /usr/lib/jvm/zulu11
ENV PATH $PATH:$JAVA_HOME/bin

RUN \
    set -xeu && \
    dnf -y -q update && \
    dnf -y -q install less python3 && \
    dnf -q clean all && \
    alternatives --set python /usr/bin/python3 && \
    dnf -q clean all && \
    rm -rf /var/cache/dnf && \
    rm -rf /tmp/* /var/tmp/* && \
    groupadd presto --gid 1000 && \
    useradd presto --uid 1000 --gid 1000 && \
    mkdir -p /usr/lib/presto /data/presto && \
    chown -R "presto:presto" /usr/lib/presto /data/presto

ARG TARGETPLATFORM
RUN if [[ "$TARGETPLATFORM" == "linux/arm64" ]] ; then \
      mkdir -p $JAVA_HOME && \
      curl -sS 'https://cdn.azul.com/zulu-embedded/bin/zulu11.41.75-ca-jdk11.0.8-linux_aarch64.tar.gz' | \
              tar --strip-components=1 -xz -C $JAVA_HOME ; \
    elif [[ "$TARGETPLATFORM" == "linux/amd64" ]] ; then \
        dnf -y -q install https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
          dnf -y -q update && \
          dnf -y -q install zulu11 ; \
    else \
       echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ; \
    fi

ARG PRESTO_VERSION
COPY presto-cli-${PRESTO_VERSION}-executable.jar /usr/bin/presto
COPY --chown=presto:presto presto-server-${PRESTO_VERSION} /usr/lib/presto

EXPOSE 8080
USER presto:presto
ENV LANG en_US.UTF-8
CMD ["/usr/lib/presto/bin/run-presto"]
