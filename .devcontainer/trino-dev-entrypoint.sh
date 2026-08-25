#!/usr/bin/env bash
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

set -euo pipefail

if [[ "$(id -u)" == "0" ]]; then
    mkdir -p /home/ubuntu/.m2
    chown ubuntu:ubuntu /home/ubuntu/.m2

    if [[ -S /var/run/docker.sock ]]; then
        docker_gid="$(stat --format='%g' /var/run/docker.sock)"
        docker_group="$(getent group "${docker_gid}" | cut --delimiter=: --fields=1 || true)"
        if [[ -z "${docker_group}" ]]; then
            docker_group="docker-host"
            groupadd --gid "${docker_gid}" "${docker_group}"
        fi
        usermod --append --groups "${docker_group}" ubuntu
    fi
    exec gosu ubuntu "$@"
fi

exec "$@"
