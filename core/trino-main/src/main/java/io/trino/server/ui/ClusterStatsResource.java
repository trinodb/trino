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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryState;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.ClusterMemoryManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.server.BasicQueryInfo;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static com.google.common.math.DoubleMath.roundToLong;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/ui/api/stats")
public class ClusterStatsResource
{
    private final InternalNodeManager nodeManager;
    private final DispatchManager dispatchManager;
    private final boolean isIncludeCoordinator;
    private final ClusterMemoryManager clusterMemoryManager;

    @Inject
    public ClusterStatsResource(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager, DispatchManager dispatchManager, ClusterMemoryManager clusterMemoryManager)
    {
        this.isIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterStats getClusterStats()
    {
        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;
        long activeNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(node -> isIncludeCoordinator || !node.isCoordinator())
                .count();

        long activeCoordinators = nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(InternalNode::isCoordinator)
                .count();
        long totalAvailableProcessors = clusterMemoryManager.getTotalAvailableProcessors();

        long runningDrivers = 0;
        double memoryReservation = 0;

        long totalInputRows = dispatchManager.getStats().getConsumedInputRows().getTotalCount();
        long totalInputBytes = dispatchManager.getStats().getConsumedInputBytes().getTotalCount();
        long totalCpuTimeSecs = dispatchManager.getStats().getConsumedCpuTimeSecs().getTotalCount();

        for (BasicQueryInfo query : dispatchManager.getQueries()) {
            if (query.getState() == QueryState.QUEUED) {
                queuedQueries++;
            }
            else if (query.getState() == QueryState.RUNNING) {
                if (query.getQueryStats().isFullyBlocked()) {
                    blockedQueries++;
                }
                else {
                    runningQueries++;
                }
            }

            if (!query.getState().isDone()) {
                totalInputBytes += query.getQueryStats().getRawInputDataSize().toBytes();
                totalInputRows += query.getQueryStats().getRawInputPositions();
                totalCpuTimeSecs += roundToLong(query.getQueryStats().getTotalCpuTime().getValue(SECONDS), HALF_UP);

                memoryReservation += query.getQueryStats().getUserMemoryReservation().toBytes();
                runningDrivers += query.getQueryStats().getRunningDrivers();
            }
        }

        return new ClusterStats(
                runningQueries,
                blockedQueries,
                queuedQueries,
                activeCoordinators,
                activeNodes,
                runningDrivers,
                totalAvailableProcessors,
                memoryReservation,
                totalInputRows,
                totalInputBytes,
                totalCpuTimeSecs);
    }

    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeCoordinators;
        private final long activeWorkers;
        private final long runningDrivers;

        private final long totalAvailableProcessors;

        private final double reservedMemory;

        private final long totalInputRows;
        private final long totalInputBytes;
        private final long totalCpuTimeSecs;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("activeCoordinators") long activeCoordinators,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers,
                @JsonProperty("totalAvailableProcessors") long totalAvailableProcessors,
                @JsonProperty("reservedMemory") double reservedMemory,
                @JsonProperty("totalInputRows") long totalInputRows,
                @JsonProperty("totalInputBytes") long totalInputBytes,
                @JsonProperty("totalCpuTimeSecs") long totalCpuTimeSecs)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeCoordinators = activeCoordinators;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.totalAvailableProcessors = totalAvailableProcessors;
            this.reservedMemory = reservedMemory;
            this.totalInputRows = totalInputRows;
            this.totalInputBytes = totalInputBytes;
            this.totalCpuTimeSecs = totalCpuTimeSecs;
        }

        @JsonProperty
        public long getRunningQueries()
        {
            return runningQueries;
        }

        @JsonProperty
        public long getBlockedQueries()
        {
            return blockedQueries;
        }

        @JsonProperty
        public long getQueuedQueries()
        {
            return queuedQueries;
        }

        @JsonProperty
        public long getActiveCoordinators()
        {
            return activeCoordinators;
        }

        @JsonProperty
        public long getActiveWorkers()
        {
            return activeWorkers;
        }

        @JsonProperty
        public long getRunningDrivers()
        {
            return runningDrivers;
        }

        @JsonProperty
        public long getTotalAvailableProcessors()
        {
            return totalAvailableProcessors;
        }

        @JsonProperty
        public double getReservedMemory()
        {
            return reservedMemory;
        }

        @JsonProperty
        public long getTotalInputRows()
        {
            return totalInputRows;
        }

        @JsonProperty
        public long getTotalInputBytes()
        {
            return totalInputBytes;
        }

        @JsonProperty
        public long getTotalCpuTimeSecs()
        {
            return totalCpuTimeSecs;
        }
    }
}
