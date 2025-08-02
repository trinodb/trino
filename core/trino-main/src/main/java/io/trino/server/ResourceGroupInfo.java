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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.ResourceGroupState;
import io.trino.spi.resourcegroups.SchedulingPolicy;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/*
 * This class is exposed to external systems via ResourceGroupStateInfoResource and QueryStateInfoResource.
 * Be careful while changing it.
 */
public record ResourceGroupInfo(
        ResourceGroupId id,
        ResourceGroupState state,
        SchedulingPolicy schedulingPolicy,
        int schedulingWeight,
        DataSize softMemoryLimit,
        int softConcurrencyLimit,
        int hardConcurrencyLimit,
        DataSize hardPhysicalDataScanLimit,
        int maxQueuedQueries,
        DataSize memoryUsage,
        Duration cpuUsage,
        DataSize physicalInputDataUsage,
        int numQueuedQueries,
        int numRunningQueries,
        int numEligibleSubGroups,
        Optional<List<ResourceGroupInfo>> subGroups,
        Optional<List<QueryStateInfo>> runningQueries)
{
    public ResourceGroupInfo
    {
        requireNonNull(id, "id is null");
        requireNonNull(state, "state is null");
        requireNonNull(schedulingPolicy, "schedulingPolicy is null");
        requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        requireNonNull(memoryUsage, "memoryUsage is null");
        requireNonNull(cpuUsage, "cpuUsage is null");
        requireNonNull(hardPhysicalDataScanLimit, "hardPhysicalDataScanLimit is null");
        requireNonNull(physicalInputDataUsage, "physicalInputDataUsage is null");
        subGroups = subGroups.map(ImmutableList::copyOf);
        runningQueries = runningQueries.map(ImmutableList::copyOf);
    }
}
