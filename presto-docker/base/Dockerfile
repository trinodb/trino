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
FROM openjdk:11-slim
LABEL MAINTAINER "Presto Software Foundation <prestosql.io>"

ARG VERSION
ENV PRESTO_VERSION=${VERSION}
ENV PRESTO_HOME=/usr/local/presto
ENV BASE_URL=https://repo1.maven.org/maven2

# install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo rsync python wget python-pip python-dev build-essential
RUN pip install jinja2

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

WORKDIR /usr/local

RUN wget -q ${BASE_URL}/io/prestosql/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
RUN tar xvzf presto-server-${PRESTO_VERSION}.tar.gz -C /usr/local/
RUN ln -s /usr/local/presto-server-${PRESTO_VERSION} $PRESTO_HOME

ADD scripts ${PRESTO_HOME}/scripts

# Create data dir
RUN mkdir -p $PRESTO_HOME/data
VOLUME ["$PRESTO_HOME/data"]