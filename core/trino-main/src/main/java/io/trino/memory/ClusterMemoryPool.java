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
package io.trino.memory;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryAllocation;
import io.trino.spi.memory.MemoryPoolInfo;
import org.weakref.jmx.Managed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ClusterMemoryPool
{
    @GuardedBy("this")
    private long totalDistributedBytes;

    @GuardedBy("this")
    private long reservedDistributedBytes;

    @GuardedBy("this")
    private long reservedRevocableDistributedBytes;

    @GuardedBy("this")
    private int nodes;

    @GuardedBy("this")
    private int blockedNodes;

    @GuardedBy("this")
    private int assignedQueries;

    // Does not include queries with zero memory usage
    @GuardedBy("this")
    private final Map<QueryId, Long> queryMemoryReservations = new HashMap<>();

    @GuardedBy("this")
    private final Map<QueryId, List<MemoryAllocation>> queryMemoryAllocations = new HashMap<>();

    @GuardedBy("this")
    private final Map<QueryId, Long> queryMemoryRevocableReservations = new HashMap<>();

    public synchronized MemoryPoolInfo getInfo()
    {
        return new MemoryPoolInfo(
                totalDistributedBytes,
                reservedDistributedBytes,
                reservedRevocableDistributedBytes,
                ImmutableMap.copyOf(queryMemoryReservations),
                ImmutableMap.copyOf(queryMemoryAllocations),
                ImmutableMap.copyOf(queryMemoryRevocableReservations),
                // not providing per-task memory info for cluster-wide pool
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Managed
    public synchronized long getTotalDistributedBytes()
    {
        return totalDistributedBytes;
    }

    @Managed
    public synchronized long getFreeDistributedBytes()
    {
        return totalDistributedBytes - reservedDistributedBytes - reservedRevocableDistributedBytes;
    }

    @Managed
    public synchronized long getReservedDistributedBytes()
    {
        return reservedDistributedBytes;
    }

    @Managed
    public synchronized long getReservedRevocableDistributedBytes()
    {
        return reservedRevocableDistributedBytes;
    }

    @Managed
    public synchronized int getNodes()
    {
        return nodes;
    }

    @Managed
    public synchronized int getBlockedNodes()
    {
        return blockedNodes;
    }

    @Managed
    public synchronized int getAssignedQueries()
    {
        return assignedQueries;
    }

    public synchronized Map<QueryId, Long> getQueryMemoryReservations()
    {
        return ImmutableMap.copyOf(queryMemoryReservations);
    }

    public synchronized Map<QueryId, Long> getQueryMemoryRevocableReservations()
    {
        return ImmutableMap.copyOf(queryMemoryRevocableReservations);
    }

    public synchronized void update(List<MemoryInfo> memoryInfos, int assignedQueries)
    {
        nodes = 0;
        blockedNodes = 0;
        totalDistributedBytes = 0;
        reservedDistributedBytes = 0;
        reservedRevocableDistributedBytes = 0;
        this.assignedQueries = assignedQueries;
        this.queryMemoryReservations.clear();
        this.queryMemoryAllocations.clear();
        this.queryMemoryRevocableReservations.clear();

        for (MemoryInfo info : memoryInfos) {
            MemoryPoolInfo poolInfo = info.getPool();
            nodes++;
            if (poolInfo.getFreeBytes() + poolInfo.getReservedRevocableBytes() <= 0) {
                blockedNodes++;
            }
            totalDistributedBytes += poolInfo.getMaxBytes();
            reservedDistributedBytes += poolInfo.getReservedBytes();
            reservedRevocableDistributedBytes += poolInfo.getReservedRevocableBytes();
            for (Map.Entry<QueryId, Long> entry : poolInfo.getQueryMemoryReservations().entrySet()) {
                queryMemoryReservations.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
            for (Map.Entry<QueryId, List<MemoryAllocation>> entry : poolInfo.getQueryMemoryAllocations().entrySet()) {
                queryMemoryAllocations.merge(entry.getKey(), entry.getValue(), this::mergeQueryAllocations);
            }
            for (Map.Entry<QueryId, Long> entry : poolInfo.getQueryMemoryRevocableReservations().entrySet()) {
                queryMemoryRevocableReservations.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }
    }

    private List<MemoryAllocation> mergeQueryAllocations(List<MemoryAllocation> left, List<MemoryAllocation> right)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        Map<String, MemoryAllocation> mergedAllocations = new HashMap<>();

        for (MemoryAllocation allocation : left) {
            mergedAllocations.put(allocation.getTag(), allocation);
        }

        for (MemoryAllocation allocation : right) {
            mergedAllocations.merge(
                    allocation.getTag(),
                    allocation,
                    (a, b) -> new MemoryAllocation(a.getTag(), a.getAllocation() + b.getAllocation()));
        }

        return new ArrayList<>(mergedAllocations.values());
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("totalDistributedBytes", totalDistributedBytes)
                .add("freeDistributedBytes", getFreeDistributedBytes())
                .add("reservedDistributedBytes", reservedDistributedBytes)
                .add("reservedRevocableDistributedBytes", reservedRevocableDistributedBytes)
                .add("nodes", nodes)
                .add("blockedNodes", blockedNodes)
                .add("assignedQueries", assignedQueries)
                .add("queryMemoryReservations", queryMemoryReservations)
                .add("queryMemoryAllocations", queryMemoryAllocations)
                .add("queryMemoryRevocableReservations", queryMemoryRevocableReservations)
                .toString();
    }
}
