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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.spi.memory.MemoryPoolId;
import io.trino.spi.memory.MemoryPoolInfo;

import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class LocalMemoryManager
{
    public static final MemoryPoolId GENERAL_POOL = new MemoryPoolId("general");
    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private DataSize maxMemory;
    private Map<MemoryPoolId, MemoryPool> pools;

    @Inject
    public LocalMemoryManager(NodeMemoryConfig config)
    {
        this(config, Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    LocalMemoryManager(NodeMemoryConfig config, long availableMemory)
    {
        requireNonNull(config, "config is null");
        configureMemoryPools(config, availableMemory);
    }

    private void configureMemoryPools(NodeMemoryConfig config, long availableMemory)
    {
        validateHeapHeadroom(config, availableMemory);
        maxMemory = DataSize.ofBytes(availableMemory - config.getHeapHeadroom().toBytes());
        ImmutableMap.Builder<MemoryPoolId, MemoryPool> builder = ImmutableMap.builder();
        long generalPoolSize = maxMemory.toBytes();
        verify(generalPoolSize > 0, "general memory pool size is 0");
        builder.put(GENERAL_POOL, new MemoryPool(GENERAL_POOL, DataSize.ofBytes(generalPoolSize)));
        this.pools = builder.buildOrThrow();
    }

    private void validateHeapHeadroom(NodeMemoryConfig config, long availableMemory)
    {
        long maxQueryTotalMemoryPerNode = config.getMaxQueryMemoryPerNode().toBytes();
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        // (availableMemory - maxQueryTotalMemoryPerNode) bytes will be available for the general pool and the
        // headroom/untracked allocations, so the heapHeadroom cannot be larger than that space.
        if (heapHeadroom < 0 || heapHeadroom + maxQueryTotalMemoryPerNode > availableMemory) {
            throw new IllegalArgumentException(
                    format("Invalid memory configuration. The sum of max query memory per node (%s) and heap headroom (%s) cannot be larger than the available heap memory (%s)",
                            maxQueryTotalMemoryPerNode,
                            heapHeadroom,
                            availableMemory));
        }
    }

    public MemoryInfo getInfo()
    {
        ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> builder = ImmutableMap.builder();
        for (Map.Entry<MemoryPoolId, MemoryPool> entry : pools.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getInfo());
        }
        return new MemoryInfo(OPERATING_SYSTEM_MX_BEAN.getAvailableProcessors(), maxMemory, builder.buildOrThrow());
    }

    public List<MemoryPool> getPools()
    {
        return ImmutableList.copyOf(pools.values());
    }

    public MemoryPool getGeneralPool()
    {
        return pools.get(GENERAL_POOL);
    }
}
