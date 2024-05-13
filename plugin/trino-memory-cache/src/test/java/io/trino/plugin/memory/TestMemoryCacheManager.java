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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryCacheManager.Channel;
import io.trino.plugin.memory.MemoryCacheManager.SplitKey;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.memory.MemoryCacheManager.MAP_ENTRY_SIZE;
import static io.trino.plugin.memory.MemoryCacheManager.MAX_CACHED_CHANNELS_PER_COLUMN;
import static io.trino.plugin.memory.TestUtils.assertBlockEquals;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestMemoryCacheManager
{
    private static final CacheColumnId COLUMN1 = new CacheColumnId("col1");
    private static final CacheColumnId COLUMN2 = new CacheColumnId("col2");
    private static final CacheColumnId COLUMN3 = new CacheColumnId("col3");
    private static final CacheSplitId SPLIT1 = new CacheSplitId("split1");
    private static final CacheSplitId SPLIT2 = new CacheSplitId("split2");

    private Page oneMegabytePage;
    private MemoryCacheManager cacheManager;
    private long allocatedRevocableMemory;
    private long memoryLimit;

    @BeforeEach
    public void setup()
    {
        oneMegabytePage = createOneMegaBytePage();
        allocatedRevocableMemory = 0;
        memoryLimit = Long.MAX_VALUE;
        cacheManager = new MemoryCacheManager(
                bytes -> {
                    checkArgument(bytes >= 0);
                    if (bytes > memoryLimit) {
                        return false;
                    }
                    allocatedRevocableMemory = bytes;
                    return true;
                },
                false);
    }

    @Test
    public void testCachePages()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig");

        // split data should not be cached yet
        SplitCache cache = cacheManager.getSplitCache(signature);
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        long idSize = ObjectToIdMap.getEntrySize(canonicalizePlanSignature(signature), PlanSignature::getRetainedSizeInBytes)
                + ObjectToIdMap.getEntrySize(COLUMN1, CacheColumnId::getRetainedSizeInBytes);
        long tupleDomainIdSize = ObjectToIdMap.getEntrySize(TupleDomain.<CacheColumnId>all(), tupleDomain -> tupleDomain.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes));
        // SplitCache doesn't allocate any revocable memory
        assertThat(allocatedRevocableMemory).isEqualTo(0L);

        Optional<ConnectorPageSink> sinkOptional = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        // active sink doesn't allocate any revocable memory
        assertThat(allocatedRevocableMemory).isEqualTo(0L);

        // second sink should not be present as split data is already being cached
        assertThat(cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        Block block = oneMegabytePage.getBlock(0);
        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(oneMegabytePage);

        // make sure memory usage is accounted for page sink
        assertThat(sink.getMemoryUsage()).isEqualTo(block.getRetainedSizeInBytes());
        assertThat(allocatedRevocableMemory).isEqualTo(0L);

        // make sure memory is transferred to cacheManager after sink is finished
        sink.finish();
        long channelSize = getChannelRetainedSizeInBytes(block);
        long cacheEntrySize = MAP_ENTRY_SIZE + SplitKey.INSTANCE_SIZE + SPLIT1.getRetainedSizeInBytes() + channelSize;
        assertThat(allocatedRevocableMemory).isEqualTo(cacheEntrySize + idSize + tupleDomainIdSize);

        // split data should be available now
        Optional<ConnectorPageSource> sourceOptional = cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sourceOptional).isPresent();

        // ensure cached pages are correct
        ConnectorPageSource source = sourceOptional.get();
        assertThat(source.getMemoryUsage()).isEqualTo(block.getRetainedSizeInBytes());
        assertBlockEquals(source.getNextPage().getBlock(0), block);
        assertThat(source.isFinished()).isTrue();

        // make sure no data is available for other signatures
        PlanSignature anotherSignature = createPlanSignature("sig2");
        SplitCache anotherCache = cacheManager.getSplitCache(anotherSignature);
        assertThat(anotherCache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        assertThat(allocatedRevocableMemory).isEqualTo(cacheEntrySize + idSize + tupleDomainIdSize);
        anotherCache.close();

        // store data for another split
        sink = cache.storePages(SPLIT2, TupleDomain.all(), TupleDomain.all()).orElseThrow();
        sink.appendPage(oneMegabytePage);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(2 * cacheEntrySize + idSize + tupleDomainIdSize);

        // data for both splits should be cached
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        assertThat(cache.loadPages(SPLIT2, TupleDomain.all(), TupleDomain.all())).isPresent();

        // revoke memory and make sure only the least recently used split is left
        cacheManager.revokeMemory(500_000);
        assertThat(allocatedRevocableMemory).isEqualTo(cacheEntrySize + idSize + tupleDomainIdSize);
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        assertThat(cache.loadPages(SPLIT2, TupleDomain.all(), TupleDomain.all())).isPresent();

        // make sure no new split data is cached when memory limit is lowered
        memoryLimit = 1_500_000;
        sink = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all()).orElseThrow();
        sink.appendPage(oneMegabytePage);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(cacheEntrySize + idSize + tupleDomainIdSize);
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        cache.close();
    }

    @Test
    public void testPredicate()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig", COLUMN1, COLUMN2);
        SplitCache cache = cacheManager.getSplitCache(signature);

        // append a split with predicate
        Domain domain = Domain.singleValue(INTEGER, 42L);
        TupleDomain<CacheColumnId> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN1, domain));
        ConnectorPageSink sink = cache.storePages(
                SPLIT1,
                predicate,
                TupleDomain.all()).orElseThrow();
        Block col = new IntArrayBlock(1, Optional.empty(), new int[] {42});
        sink.appendPage(new Page(col, col));
        sink.finish();

        long idSize = ObjectToIdMap.getEntrySize(canonicalizePlanSignature(signature), PlanSignature::getRetainedSizeInBytes)
                + ObjectToIdMap.getEntrySize(COLUMN1, CacheColumnId::getRetainedSizeInBytes)
                + ObjectToIdMap.getEntrySize(COLUMN2, CacheColumnId::getRetainedSizeInBytes)
                + ObjectToIdMap.getEntrySize(predicate, tupleDomain -> tupleDomain.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes))
                + ObjectToIdMap.getEntrySize(TupleDomain.<CacheColumnId>all(), tupleDomain -> tupleDomain.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes));
        long cacheEntrySize = MAP_ENTRY_SIZE + SplitKey.INSTANCE_SIZE + SPLIT1.getRetainedSizeInBytes() + getChannelRetainedSizeInBytes(col);
        assertThat(allocatedRevocableMemory).isEqualTo(idSize + cacheEntrySize * 2);

        // entire tuple domain must much
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        assertThat(cache.loadPages(SPLIT1, TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN1, domain,
                        COLUMN2, Domain.singleValue(INTEGER, 43L))),
                TupleDomain.all())).isEmpty();
        assertThat(cache.loadPages(SPLIT1, predicate, predicate)).isEmpty();
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), predicate)).isEmpty();

        assertThat(cache.loadPages(SPLIT1, predicate, TupleDomain.all())).isPresent();

        // revoking should remove tuple domain ids
        cache.close();
        cacheManager.revokeMemory(1_000_000);
        assertThat(allocatedRevocableMemory).isEqualTo(0L);
    }

    @Test
    public void testColumnCaching()
            throws IOException
    {
        // split data should not be cached yet
        SplitCache cacheCol12 = cacheManager.getSplitCache(createPlanSignature("sig", COLUMN1, COLUMN2));
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(1);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(2);
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(0);
        assertThat(cacheCol12.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        Optional<ConnectorPageSink> sinkOptional = cacheCol12.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        ConnectorPageSink sink = sinkOptional.get();

        // create another cache with reverse column order
        SplitCache cacheCol21 = cacheManager.getSplitCache(createPlanSignature("sig", COLUMN2, COLUMN1));
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(1);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(2);

        // split data with reverse column order should be available after sink is finished
        Block col1BlockStore1 = new IntArrayBlock(2, Optional.empty(), new int[] {0, 1});
        Block col2BlockStore1 = new IntArrayBlock(2, Optional.empty(), new int[] {10, 11});
        sink.appendPage(new Page(col1BlockStore1, col2BlockStore1));
        sink.finish();
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(2);
        assertPageSourceEquals(cacheCol21.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), col2BlockStore1, col1BlockStore1);

        // subset of columns should also be cached
        SplitCache cacheCol2 = cacheManager.getSplitCache(createPlanSignature("sig", COLUMN2));
        assertPageSourceEquals(cacheCol2.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), col2BlockStore1);

        // data for column1 and column3 should be cached together with separate store id
        SplitCache cacheCol13 = cacheManager.getSplitCache(createPlanSignature("sig", COLUMN1, COLUMN3));
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(1);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(3);
        assertThat(cacheCol13.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        Block col1BlockStore2 = new IntArrayBlock(2, Optional.empty(), new int[] {20, 21});
        Block col3BlockStore2 = new IntArrayBlock(2, Optional.empty(), new int[] {30, 31});
        sinkOptional = cacheCol13.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        sink = sinkOptional.get();
        sink.appendPage(new Page(col1BlockStore2, col3BlockStore2));
        sink.finish();
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(4);

        // (col1, col2) page source should still use "store no 1" blocks
        assertPageSourceEquals(cacheCol12.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), col1BlockStore1, col2BlockStore1);

        // (col1, col3) page source should use "store no 2" blocks
        assertPageSourceEquals(cacheCol13.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), col1BlockStore2, col3BlockStore2);

        // cache should return the newest entries
        SplitCache cacheCol123 = cacheManager.getSplitCache(createPlanSignature("sig", COLUMN1, COLUMN2, COLUMN3));
        Block col1BlockStore3 = new IntArrayBlock(2, Optional.empty(), new int[] {50, 51});
        Block col2BlockStore3 = new IntArrayBlock(2, Optional.empty(), new int[] {60, 61});
        Block col3BlockStore3 = new IntArrayBlock(2, Optional.empty(), new int[] {70, 71});
        sinkOptional = cacheCol123.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        sink = sinkOptional.get();
        sink.appendPage(new Page(col1BlockStore3, col2BlockStore3, col3BlockStore3));
        sink.finish();
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(7);
        assertPageSourceEquals(cacheCol13.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), col1BlockStore3, col3BlockStore3);

        // make sure group by columns do not use non-aggregated cached column data
        SplitCache groupByCacheCol1 = cacheManager.getSplitCache(new PlanSignature(
                new SignatureKey("sig"),
                Optional.of(ImmutableList.of(COLUMN1)),
                ImmutableList.of(COLUMN1),
                ImmutableList.of(INTEGER)));
        assertThat(groupByCacheCol1.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        // make sure all ids are removed after revoke
        cacheCol12.close();
        cacheCol123.close();
        cacheCol2.close();
        cacheCol13.close();
        cacheCol21.close();
        groupByCacheCol1.close();
        cacheManager.revokeMemory(1_000_000);
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(0);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(0);
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(0);
        assertThat(cacheManager.getRevocableBytes()).isEqualTo(0);
    }

    @Test
    public void testLruCache()
    {
        SplitCache cacheA = cacheManager.getSplitCache(createPlanSignature("sigA", COLUMN1));
        SplitCache cacheB = cacheManager.getSplitCache(createPlanSignature("sigB", COLUMN1));

        // cache two pages to different sinks
        Optional<ConnectorPageSink> sinkOptional = cacheA.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        sinkOptional.get().appendPage(oneMegabytePage);
        sinkOptional.get().finish();

        sinkOptional = cacheB.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        sinkOptional.get().appendPage(oneMegabytePage);
        sinkOptional.get().finish();

        // both pages should be cached
        assertThat(cacheB.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        assertThat(cacheA.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();

        // only latest used page should be cached after revoke
        cacheManager.revokeMemory(500_000);
        assertThat(cacheA.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        assertThat(cacheB.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
    }

    @Test
    public void testMaxChannelsPerColumn()
            throws IOException
    {
        // store MAX_CACHED_CHANNELS_PER_COLUMN column ids for col1
        for (int i = 1; i <= MAX_CACHED_CHANNELS_PER_COLUMN; i++) {
            List<CacheColumnId> columns = IntStream.range(0, i + 1)
                    .mapToObj(col -> new CacheColumnId("col" + col))
                    .collect(toImmutableList());
            List<Type> columnsTypes = columns
                    .stream().map(col -> INTEGER)
                    .collect(toImmutableList());
            PlanSignature signature = new PlanSignature(
                    new SignatureKey("sig"),
                    Optional.empty(),
                    columns,
                    columnsTypes);
            try (SplitCache cache = cacheManager.getSplitCache(signature)) {
                Optional<ConnectorPageSink> sinkOptional = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
                assertThat(sinkOptional).isPresent();
                ConnectorPageSink sink = sinkOptional.get();
                sink.appendPage(new Page(nCopies(
                        columns.size(),
                        new IntArrayBlock(1, Optional.empty(), new int[] {i}))
                        .toArray(new Block[0])));
                sink.finish();
            }
        }

        int splitCount = MAX_CACHED_CHANNELS_PER_COLUMN * (MAX_CACHED_CHANNELS_PER_COLUMN + 1) / 2 + MAX_CACHED_CHANNELS_PER_COLUMN;
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(1);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(MAX_CACHED_CHANNELS_PER_COLUMN + 1);
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(splitCount);

        // add another channel for col1
        List<CacheColumnId> columns = ImmutableList.of(new CacheColumnId("col1"), new CacheColumnId("col100"));
        List<Type> columnsTypes = columns
                .stream().map(col -> INTEGER)
                .collect(toImmutableList());
        PlanSignature signature = new PlanSignature(
                new SignatureKey("sig"),
                Optional.empty(),
                columns,
                columnsTypes);
        SplitCache cache = cacheManager.getSplitCache(signature);
        Optional<ConnectorPageSink> sinkOptional = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        ConnectorPageSink sink = sinkOptional.get();
        Block block = new IntArrayBlock(1, Optional.empty(), new int[] {0});
        sink.appendPage(new Page(nCopies(columns.size(), block).toArray(new Block[0])));
        sink.finish();

        // oldest column from col1 should be purged
        assertThat(cacheManager.getCachedPlanSignaturesCount()).isEqualTo(1);
        assertThat(cacheManager.getCachedColumnIdsCount()).isEqualTo(MAX_CACHED_CHANNELS_PER_COLUMN + 2);
        assertThat(cacheManager.getCachedSplitsCount()).isEqualTo(splitCount + 1);
        assertPageSourceEquals(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all()), block, block);
    }

    private void assertPageSourceEquals(Optional<ConnectorPageSource> sourceOptional, Block... expectedBlocks)
    {
        assertThat(sourceOptional).isPresent();
        ConnectorPageSource source = sourceOptional.get();
        Page actualPage = source.getNextPage();
        assertThat(source.isFinished()).isTrue();
        assertThat(actualPage.getChannelCount()).isEqualTo(expectedBlocks.length);
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(actualPage.getBlock(i), expectedBlocks[i]);
        }
    }

    @Test
    public void testSinkAbort()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig");

        // create new SplitCache
        SplitCache cache = cacheManager.getSplitCache(signature);

        // start caching of new split
        Optional<ConnectorPageSink> sinkOptional = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all());
        assertThat(sinkOptional).isPresent();
        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(oneMegabytePage);
        assertThat(sink.getMemoryUsage()).isEqualTo(oneMegabytePage.getBlock(0).getRetainedSizeInBytes());
        assertThat(cache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();

        // active sink shouldn't allocate any revocable memory
        cache.close();
        assertThat(allocatedRevocableMemory).isEqualTo(0L);

        // no data should be cached after abort
        sink.abort();
        assertThat(allocatedRevocableMemory).isEqualTo(0L);
        assertThat(cacheManager.getSplitCache(signature).loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
    }

    @Test
    public void testPlanSignatureRevoke()
            throws IOException
    {
        Page smallPage = new Page(new IntArrayBlock(1, Optional.empty(), new int[] {0}));
        PlanSignature bigSignature = createPlanSignature(IntStream.range(0, 500_000).mapToObj(Integer::toString).collect(joining()));
        PlanSignature secondBigSignature = createPlanSignature(IntStream.range(0, 500_001).mapToObj(Integer::toString).collect(joining()));

        // cache some data for first signature
        assertThat(allocatedRevocableMemory).isEqualTo(0);
        SplitCache cache = cacheManager.getSplitCache(bigSignature);
        ConnectorPageSink sink = cache.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all()).orElseThrow();
        sink.appendPage(smallPage);
        sink.finish();
        cache.close();

        // make sure page is present with new SplitCache instance
        SplitCache anotherCache = cacheManager.getSplitCache(bigSignature);
        assertThat(anotherCache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();

        // cache data for another signature
        SplitCache cacheForSecondSignature = cacheManager.getSplitCache(secondBigSignature);
        sink = cacheForSecondSignature.storePages(SPLIT1, TupleDomain.all(), TupleDomain.all()).orElseThrow();
        sink.appendPage(smallPage);
        sink.finish();

        // both splits should be still cached
        assertThat(anotherCache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        assertThat(cacheForSecondSignature.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        anotherCache.close();
        cacheForSecondSignature.close();

        // revoke some small amount of memory
        assertThat(cacheManager.revokeMemory(100)).isPositive();

        // only one split (for secondBigSignature signature) should be cached, because initial bigSignature was purged
        anotherCache = cacheManager.getSplitCache(bigSignature);
        cacheForSecondSignature = cacheManager.getSplitCache(secondBigSignature);
        assertThat(anotherCache.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isEmpty();
        assertThat(cacheForSecondSignature.loadPages(SPLIT1, TupleDomain.all(), TupleDomain.all())).isPresent();
        anotherCache.close();
    }

    static long getChannelRetainedSizeInBytes(Block block)
    {
        Channel channel = new Channel(new SplitKey(0, 0, new CacheSplitId("id"), 0, 0), 0);
        channel.setBlocks(new Block[] {block});
        channel.setLoaded();
        return channel.getRetainedSizeInBytes();
    }

    static PlanSignature createPlanSignature(String signature)
    {
        return createPlanSignature(signature, COLUMN1);
    }

    private static PlanSignature createPlanSignature(String signature, CacheColumnId... ids)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.copyOf(ids),
                Stream.of(ids).map(ignore -> (Type) INTEGER).collect(toImmutableList()));
    }

    static Page createOneMegaBytePage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(0);
        while (blockBuilder.getRetainedSizeInBytes() < 1024 * 1024) {
            BIGINT.writeLong(blockBuilder, 42L);
        }
        Page page = new Page(blockBuilder.getPositionCount(), blockBuilder.build());
        page.compact();
        return page;
    }
}
