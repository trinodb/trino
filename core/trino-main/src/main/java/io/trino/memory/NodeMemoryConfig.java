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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import java.util.Optional;

// This is separate from MemoryManagerConfig because it's difficult to test the default value of maxQueryMemoryPerNode
@DefunctConfig({
        "deprecated.legacy-system-pool-enabled",
        "query.max-total-memory-per-node",
})
public class NodeMemoryConfig
{
    public static final long AVAILABLE_HEAP_MEMORY = Runtime.getRuntime().maxMemory();
    public static final String QUERY_MAX_MEMORY_PER_NODE_CONFIG = "query.max-memory-per-node";
    public static final String QUERY_MAX_MEMORY_PER_TASK_CONFIG = "query.max-memory-per-task";

    private boolean isReservedPoolDisabled = true;

    private DataSize maxQueryMemoryPerNode = DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3));

    private Optional<DataSize> maxQueryMemoryPerTask = Optional.empty();

    private DataSize heapHeadroom = DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3));

    @NotNull
    public DataSize getMaxQueryMemoryPerNode()
    {
        return maxQueryMemoryPerNode;
    }

    @Config(QUERY_MAX_MEMORY_PER_NODE_CONFIG)
    public NodeMemoryConfig setMaxQueryMemoryPerNode(DataSize maxQueryMemoryPerNode)
    {
        this.maxQueryMemoryPerNode = maxQueryMemoryPerNode;
        return this;
    }

    @NotNull
    public Optional<DataSize> getMaxQueryMemoryPerTask()
    {
        return maxQueryMemoryPerTask;
    }

    @Config(QUERY_MAX_MEMORY_PER_TASK_CONFIG)
    @LegacyConfig("query.max-total-memory-per-task")
    @ConfigDescription("Sets memory limit enforced for a single task; there is no memory limit by default")
    public NodeMemoryConfig setMaxQueryMemoryPerTask(DataSize maxQueryMemoryPerTask)
    {
        this.maxQueryMemoryPerTask = Optional.ofNullable(maxQueryMemoryPerTask);
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "experimental.reserved-pool-enabled", replacedBy = "experimental.reserved-pool-disabled")
    public void setReservedPoolEnabled(boolean reservedPoolEnabled)
    {
        isReservedPoolDisabled = !reservedPoolEnabled;
    }

    @Deprecated
    public boolean isReservedPoolDisabled()
    {
        return isReservedPoolDisabled;
    }

    @Deprecated
    @Config("experimental.reserved-pool-disabled")
    public NodeMemoryConfig setReservedPoolDisabled(boolean reservedPoolDisabled)
    {
        this.isReservedPoolDisabled = reservedPoolDisabled;
        return this;
    }

    @NotNull
    public DataSize getHeapHeadroom()
    {
        return heapHeadroom;
    }

    @Config("memory.heap-headroom-per-node")
    @ConfigDescription("The amount of heap memory to set aside as headroom/buffer (e.g., for untracked allocations)")
    public NodeMemoryConfig setHeapHeadroom(DataSize heapHeadroom)
    {
        this.heapHeadroom = heapHeadroom;
        return this;
    }
}
