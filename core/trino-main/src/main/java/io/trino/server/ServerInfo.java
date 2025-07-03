/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.node.NodeState;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ServerInfo(
        String nodeId,
        NodeState state,
        NodeVersion nodeVersion,
        String environment,
        boolean coordinator,
        Optional<String> coordinatorId,
        boolean starting,
        Duration uptime)
{
    public ServerInfo
    {
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(state, "state is null");
        requireNonNull(nodeVersion, "nodeVersion is null");
        requireNonNull(environment, "environment is null");
        requireNonNull(coordinatorId, "coordinatorId is null");
        requireNonNull(uptime, "uptime is null");
    }
}
