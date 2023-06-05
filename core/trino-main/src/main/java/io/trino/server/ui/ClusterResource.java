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
package io.trino.server.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import javax.annotation.concurrent.Immutable;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/ui/api/cluster")
public class ClusterResource
{
    private final NodeVersion version;
    private final String environment;
    private final long startTime = System.nanoTime();

    @Inject
    public ClusterResource(NodeVersion nodeVersion, NodeInfo nodeInfo)
    {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = nodeInfo.getEnvironment();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Produces(APPLICATION_JSON)
    public ClusterInfo getInfo()
    {
        return new ClusterInfo(version, environment, nanosSince(startTime));
    }

    @Immutable
    public static class ClusterInfo
    {
        private final NodeVersion nodeVersion;
        private final String environment;
        private final Duration uptime;

        public ClusterInfo(NodeVersion nodeVersion, String environment, Duration uptime)
        {
            this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
            this.environment = requireNonNull(environment, "environment is null");
            this.uptime = requireNonNull(uptime, "uptime is null");
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
        public Duration getUptime()
        {
            return uptime;
        }
    }
}
