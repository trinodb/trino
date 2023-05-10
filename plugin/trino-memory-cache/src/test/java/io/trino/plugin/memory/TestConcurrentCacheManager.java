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
package io.trino.plugin.memory;

import io.trino.spi.Page;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.memory.TestMemoryCacheManager.createOneMegaBytePage;
import static io.trino.plugin.memory.TestMemoryCacheManager.createPlanSignature;
import static io.trino.plugin.memory.TestMemoryCacheManager.getChannelRetainedSizeInBytes;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestConcurrentCacheManager
{
    private static final PlanSignature SIGNATURE_1 = createPlanSignature("sig1");
    private static final PlanSignature SIGNATURE_2 = createPlanSignature("sig2");
    private static final CacheSplitId SPLIT1 = new CacheSplitId("1");
    private static final CacheSplitId SPLIT2 = new CacheSplitId("122");
    private static final CacheSplitId SPLIT3 = new CacheSplitId("2");
    private static final CacheSplitId SPLIT4 = new CacheSplitId("123");

    private Page oneMegabytePage;
    private ConcurrentCacheManager cacheManager;
    private long allocatedRevocableMemory;

    @BeforeMethod
    public void setup()
    {
        oneMegabytePage = createOneMegaBytePage();
        allocatedRevocableMemory = 0;
        CacheManagerContext context = new CacheManagerContext()
        {
            @Override
            public MemoryAllocator revocableMemoryAllocator()
            {
                return bytes -> {
                    checkArgument(bytes >= 0);
                    allocatedRevocableMemory = bytes;
                    return true;
                };
            }

            @Override
            public BlockEncodingSerde blockEncodingSerde()
            {
                return new TestingBlockEncodingSerde();
            }
        };
        cacheManager = new ConcurrentCacheManager(context);
    }

    @Test
    public void testConcurrentManagerRevoke()
            throws IOException
    {
        // make sure splits are cached in two MemoryCacheMaangers
        assertThat(cacheManager.getCacheManager(SIGNATURE_1, SPLIT1)).isEqualTo(cacheManager.getCacheManager(SIGNATURE_1, SPLIT2));
        assertThat(cacheManager.getCacheManager(SIGNATURE_2, SPLIT3)).isEqualTo(cacheManager.getCacheManager(SIGNATURE_2, SPLIT4));
        assertThat(cacheManager.getCacheManager(SIGNATURE_1, SPLIT1)).isNotEqualTo(cacheManager.getCacheManager(SIGNATURE_2, SPLIT3));
        assertThat(allocatedRevocableMemory).isEqualTo(0);

        // cache some splits
        storePage(SIGNATURE_1, SPLIT1);
        storePage(SIGNATURE_1, SPLIT2);
        storePage(SIGNATURE_2, SPLIT1);
        storePage(SIGNATURE_2, SPLIT2);
        assertThat(cacheManager.getCachedSplitSizeDistribution().get(0.5)).isEqualTo(getChannelRetainedSizeInBytes(oneMegabytePage.getBlock(0)));
        assertThatMemoryMatches();

        // revoke ~1.5MBs, the oldest splits should be purged from both sub-managers
        long initialAllocatedMemory = allocatedRevocableMemory;
        long revokedMemory = cacheManager.revokeMemory(1_500_000, 1);

        assertThat(revokedMemory).isPositive();
        assertThat(initialAllocatedMemory - revokedMemory).isEqualTo(allocatedRevocableMemory);

        assertSplitIsCached(SIGNATURE_1, SPLIT2);
        assertSplitIsCached(SIGNATURE_2, SPLIT2);

        assertSplitIsNotCached(SIGNATURE_1, SPLIT1);
        assertSplitIsNotCached(SIGNATURE_2, SPLIT1);

        // revoke everything
        assertThat(cacheManager.getRevokeMemoryTime().getAllTime().getCount()).isZero();
        assertThat(cacheManager.revokeMemory(10_000_000)).isPositive();
        assertThat(allocatedRevocableMemory).isZero();
        assertThat(cacheManager.getRevokeMemoryTime().getAllTime().getCount()).isNotZero();

        // nothing to revoke
        assertThat(cacheManager.revokeMemory(10_000_000)).isZero();
    }

    private void assertThatMemoryMatches()
    {
        long totalManagersMemory = Arrays.stream(cacheManager.getCacheManagers())
                .mapToLong(MemoryCacheManager::getRevocableBytes)
                .sum();
        assertThat(allocatedRevocableMemory).isEqualTo(totalManagersMemory);
    }

    private void assertSplitIsNotCached(PlanSignature signature, CacheSplitId splitId)
            throws IOException
    {
        try (SplitCache cache = cacheManager.getSplitCache(signature)) {
            assertThat(cache.loadPages(splitId)).isEmpty();
        }
    }

    private void assertSplitIsCached(PlanSignature signature, CacheSplitId splitId)
            throws IOException
    {
        try (SplitCache cache = cacheManager.getSplitCache(signature)) {
            assertThat(cache.loadPages(splitId)).isPresent();
        }
    }

    private void storePage(PlanSignature signature, CacheSplitId splitId)
            throws IOException
    {
        try (SplitCache cache = cacheManager.getSplitCache(signature)) {
            Optional<ConnectorPageSink> sinkOptional = cache.storePages(splitId);
            assertThat(sinkOptional).isPresent();
            sinkOptional.get().appendPage(oneMegabytePage);
            sinkOptional.get().finish();
        }
    }
}
