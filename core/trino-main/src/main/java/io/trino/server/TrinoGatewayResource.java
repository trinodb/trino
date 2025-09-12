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
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.memory.MemoryPoolInfo;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;

@Path("/v1/integrations/trinoGateway")
public class TrinoGatewayResource
{
    private final ClusterMemoryManager clusterMemoryManager;

    @Inject
    public TrinoGatewayResource(ClusterMemoryManager clusterMemoryManager)
    {
        this.clusterMemoryManager = clusterMemoryManager;
    }

    @GET
    @Path("metrics")
    @ResourceSecurity(MANAGEMENT_READ)
    public ClusterMetrics clusterMetrics()
    {
        long totalFreeBytes = clusterMemoryManager.getAllNodesMemoryInfo()
                .values()
                .stream()
                .flatMap(Optional::stream)
                .map(MemoryInfo::getPool)
                .mapToLong(MemoryPoolInfo::getFreeBytes)
                .sum();
        double totalSystemLoad = clusterMemoryManager.getAllNodesMemoryInfo()
                .values()
                .stream()
                .flatMap(Optional::stream)
                .mapToDouble(MemoryInfo::getSystemCpuLoad)
                .sum();
        return new ClusterMetrics(totalFreeBytes, totalSystemLoad);
    }

    public record ClusterMetrics(long totalFreeBytes, double totalSystemLoad)
    {}
}
