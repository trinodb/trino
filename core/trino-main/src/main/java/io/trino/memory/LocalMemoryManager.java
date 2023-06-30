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
import com.google.inject.Inject;
import io.airlift.units.DataSize;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public final class LocalMemoryManager
{
    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private final MemoryPool memoryPool;

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
        return new MemoryInfo(OPERATING_SYSTEM_MX_BEAN.getAvailableProcessors(), memoryPool.getInfo());
    }

    public MemoryPool getMemoryPool()
    {
        return memoryPool;
    }
}
