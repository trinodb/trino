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

import com.google.inject.Inject;
import com.sun.management.OperatingSystemMXBean;
import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.memory.LocalMemoryManager;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/status")
public class StatusResource
{
    private final NodeInfo nodeInfo;
    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final long startTime = System.nanoTime();
    private final int logicalCores;
    private final LocalMemoryManager memoryManager;
    private final MemoryMXBean memoryMXBean;

    private OperatingSystemMXBean operatingSystemMXBean;

    @Inject
    public StatusResource(NodeVersion nodeVersion, NodeInfo nodeInfo, ServerConfig serverConfig, LocalMemoryManager memoryManager)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = nodeInfo.getEnvironment();
        this.coordinator = serverConfig.isCoordinator();
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.logicalCores = Runtime.getRuntime().availableProcessors();

        if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
            // we want the com.sun.management sub-interface of java.lang.management.OperatingSystemMXBean
            this.operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }
    }

    @ResourceSecurity(PUBLIC)
    @HEAD
    @Produces(APPLICATION_JSON) // to match the GET route
    public Response statusPing()
    {
        return Response.ok().build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(APPLICATION_JSON)
    public NodeStatus getStatus()
    {
        return new NodeStatus(
                nodeInfo.getNodeId(),
                version,
                environment,
                coordinator,
                nanosSince(startTime),
                nodeInfo.getExternalAddress(),
                nodeInfo.getInternalAddress(),
                memoryManager.getInfo(),
                logicalCores,
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getProcessCpuLoad(),
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getSystemCpuLoad(),
                memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getUsed());
    }
}
