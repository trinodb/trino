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

FROM eclipse-temurin:25-jdk-noble

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl git gosu procps unzip \
    && rm -rf /var/lib/apt/lists/*

COPY trino-dev-entrypoint.sh /usr/local/bin/trino-dev-entrypoint

RUN chmod 0755 /usr/local/bin/trino-dev-entrypoint

WORKDIR /workspace

ENTRYPOINT ["/usr/local/bin/trino-dev-entrypoint"]
CMD ["sleep", "infinity"]
