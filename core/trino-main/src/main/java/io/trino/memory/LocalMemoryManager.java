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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.sun.management.UnixOperatingSystemMXBean;
import io.airlift.units.DataSize;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.clamp;
import static java.lang.String.format;

public final class LocalMemoryManager
{
    private final MemoryPool memoryPool;

    private static final Supplier<Integer> AVAILABLE_PROCESSORS = Suppliers
            .memoizeWithExpiration(Runtime.getRuntime()::availableProcessors, 30, TimeUnit.SECONDS);

    // Clamp value because according to the documentation: if the recent CPU usage is not available, the method returns a negative value.
    private static final Supplier<Double> SYSTEM_CPU_LOAD = Suppliers
            .memoizeWithExpiration(() -> clamp(((UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getCpuLoad(), 0.0, 1.0), 5, TimeUnit.SECONDS);

    @Inject
    public LocalMemoryManager(NodeMemoryConfig config)
    {
        this(config, Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    public LocalMemoryManager(NodeMemoryConfig config, long availableMemory)
    {
        validateHeapHeadroom(config, availableMemory);
        DataSize memoryPoolSize = DataSize.ofBytes(availableMemory - config.getHeapHeadroom().toBytes());
        verify(memoryPoolSize.toBytes() > 0, "memory pool size is 0");
        memoryPool = new MemoryPool(memoryPoolSize);
    }

    private void validateHeapHeadroom(NodeMemoryConfig config, long availableMemory)
    {
        long maxQueryTotalMemoryPerNode = config.getMaxQueryMemoryPerNode().toBytes();
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        // (availableMemory - maxQueryTotalMemoryPerNode) bytes will be available for the memory pool and the
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
        return new MemoryInfo(AVAILABLE_PROCESSORS.get(), SYSTEM_CPU_LOAD.get(), memoryPool.getInfo());
    }

    public MemoryPool getMemoryPool()
    {
        return memoryPool;
    }
}
