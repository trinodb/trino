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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class ServerInfo
{
    private final NodeVersion nodeVersion;
    private final String environment;
    private final boolean coordinator;
    private final boolean starting;

    // optional to maintain compatibility with older servers
    private final Optional<Duration> uptime;
    private final Optional<String> coordinatorId;

    @JsonCreator
    public ServerInfo(
            @JsonProperty("nodeVersion") NodeVersion nodeVersion,
            @JsonProperty("environment") String environment,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("starting") boolean starting,
            @JsonProperty("uptime") Optional<Duration> uptime,
            @JsonProperty("coordinatorId") Optional<String> coordinatorId)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.coordinator = coordinator;
        this.starting = starting;
        this.uptime = requireNonNull(uptime, "uptime is null");
        this.coordinatorId = requireNonNull(coordinatorId, "coordinatorId is null");
    }

    @JsonProperty
    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @JsonProperty
    public boolean isStarting()
    {
        return starting;
    }

    @JsonProperty
    public Optional<Duration> getUptime()
    {
        return uptime;
    }

    @JsonProperty
    public Optional<String> getCoordinatorId()
    {
        return coordinatorId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerInfo that = (ServerInfo) o;
        return Objects.equals(nodeVersion, that.nodeVersion) &&
                Objects.equals(environment, that.environment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeVersion, environment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeVersion", nodeVersion)
                .add("environment", environment)
                .add("coordinator", coordinator)
                .add("uptime", uptime.orElse(null))
                .add("coordinatorId", coordinatorId.orElse(null))
                .omitNullValues()
                .toString();
    }
}
