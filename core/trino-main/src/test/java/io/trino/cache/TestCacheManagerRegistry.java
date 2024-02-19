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
package io.trino.cache;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCacheManagerRegistry
{
    private static final TaskId TASK_ID = new TaskId(new StageId("id", 0), 1, 2);
    private static final String TEST_CACHE_MANAGER = "test-manager";

    private LocalMemoryManager memoryManager;
    private TestCacheManager cacheManager;
    private CacheManagerRegistry registry;

    @BeforeEach
    public void setup()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(10, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(100, MEGABYTE));

        memoryManager = new LocalMemoryManager(config, DataSize.of(110, MEGABYTE).toBytes());
        registry = new CacheManagerRegistry(new CacheConfig(), memoryManager, newDirectExecutorService(), new TestingBlockEncodingSerde(), new CacheStats());
        registry.addCacheManagerFactory(new TestCacheManagerFactory());
        registry.loadCacheManager(TEST_CACHE_MANAGER, ImmutableMap.of());
    }

    @Test
    public void testRevokeMemoryOnListener()
    {
        assertThat(cacheManager.tryAllocateMemory(DataSize.of(90, MEGABYTE).toBytes())).isTrue();

        // revoke should not be triggered
        assertThat(cacheManager.getBytesToRevoke()).isEmpty();

        // allocating query memory should trigger cache revoke
        ListenableFuture<Void> memoryFuture = memoryManager.getMemoryPool().reserve(TASK_ID, "allocation", DataSize.of(80, MEGABYTE).toBytes());
        assertThat(memoryFuture).isNotDone();
        assertThat(cacheManager.getBytesToRevoke()).hasValue(DataSize.of(100, MEGABYTE).toBytes());

        // freeing memory should unblock memory future
        assertThat(cacheManager.tryAllocateMemory(20)).isTrue();
        assertThat(memoryFuture).isDone();
    }

    @Test
    public void testRevokeMemoryOnBigAllocation()
    {
        assertThat(cacheManager.tryAllocateMemory(DataSize.of(90, MEGABYTE).toBytes())).isTrue();
        assertThat(cacheManager.getBytesToRevoke()).isEmpty();
        assertThat(registry.getNonEmptyRevokeCount()).isEqualTo(0);
        assertThat(registry.getDistributionSizeRevokedMemory().getCount()).isEqualTo(0);

        assertThat(cacheManager.tryAllocateMemory(DataSize.of(95, MEGABYTE).toBytes())).isFalse();
        assertThat(cacheManager.getBytesToRevoke()).hasValue(DataSize.of(20, MEGABYTE).toBytes());
        assertThat(registry.getNonEmptyRevokeCount()).isEqualTo(1);
        assertThat(registry.getDistributionSizeRevokedMemory().getCount()).isEqualTo(1);
        assertThat(registry.getDistributionSizeRevokedMemory().getAvg()).isEqualTo(DataSize.of(20, MEGABYTE).toBytes());
    }

    private class TestCacheManagerFactory
            implements CacheManagerFactory
    {
        @Override
        public String getName()
        {
            return TEST_CACHE_MANAGER;
        }

        @Override
        public CacheManager create(Map<String, String> config, CacheManagerContext context)
        {
            requireNonNull(context, "context is null");
            requireNonNull(context.revocableMemoryAllocator(), "revocableMemoryAllocator is null");
            requireNonNull(context.blockEncodingSerde(), "revocableMemoryAllocator is null");
            cacheManager = new TestCacheManager(context.revocableMemoryAllocator());
            return cacheManager;
        }
    }

    private static class TestCacheManager
            implements CacheManager
    {
        private final MemoryAllocator allocator;
        private OptionalLong bytesToRevoke = OptionalLong.empty();

        private TestCacheManager(MemoryAllocator allocator)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
        }

        @Override
        public SplitCache getSplitCache(PlanSignature signature)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long revokeMemory(long bytesToRevoke)
        {
            this.bytesToRevoke = OptionalLong.of(bytesToRevoke);
            return bytesToRevoke;
        }

        private boolean tryAllocateMemory(long bytes)
        {
            return allocator.trySetBytes(bytes);
        }

        private OptionalLong getBytesToRevoke()
        {
            return bytesToRevoke;
        }
    }
}
