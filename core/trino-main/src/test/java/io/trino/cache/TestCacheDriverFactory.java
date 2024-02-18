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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.execution.ScheduledSplit;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.DevNullOperator;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSplit;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.cache.CacheDriverFactory.MAX_UNENFORCED_PREDICATE_VALUE_COUNT;
import static io.trino.cache.CacheDriverFactory.appendRemainingPredicates;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.PlanTester.getTupleDomainJsonCodec;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCacheDriverFactory
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final SignatureKey SIGNATURE_KEY = new SignatureKey("key");
    private static final CacheSplitId SPLIT_ID = new CacheSplitId("split");
    private static final ScheduledSplit SPLIT = new ScheduledSplit(0, new PlanNodeId("id"), new Split(TEST_CATALOG_HANDLE, new TestingSplit(false, ImmutableList.of())));
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    private TestSplitCache splitCache;
    private CacheManagerRegistry registry;
    private JsonCodec<TupleDomain> tupleDomainCodec;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    public void setUp()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(16, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(32, MEGABYTE));
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setEnabled(true);
        registry = new CacheManagerRegistry(cacheConfig, new LocalMemoryManager(config, DataSize.of(1024, MEGABYTE).toBytes()), new TestingBlockEncodingSerde(), new CacheStats());
        TestCacheManagerFactory cacheManagerFactory = new TestCacheManagerFactory();
        registry.loadCacheManager(cacheManagerFactory, ImmutableMap.of());
        splitCache = cacheManagerFactory.getCacheManager().getSplitCache();
        TypeManager typeManager = new TestingTypeManager();
        tupleDomainCodec = getTupleDomainJsonCodec(new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager), typeManager);
        scheduledExecutor = Executors.newScheduledThreadPool(1);
    }

    @AfterEach
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testCreateDriverForOriginalPlan()
    {
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(), ImmutableList.of()),
                TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();

        // expect driver for original plan because cacheSplit is empty
        CacheDriverFactory cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(), signature, operatorIdAllocator);
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.empty());
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter filters data completely
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(input -> TupleDomain.none(), identity()), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because enforced predicate is pruned to empty tuple domain
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(identity(), input -> TupleDomain.none()), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter is too big
        Domain bigDomain = Domain.multipleValues(BIGINT, LongStream.range(0, MAX_UNENFORCED_PREDICATE_VALUE_COUNT + 1)
                .boxed()
                .collect(toImmutableList()));
        cacheDriverFactory = createCacheDriverFactory(
                new TestPageSourceProvider(input -> TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("column"), bigDomain)), identity()),
                signature,
                operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();
    }

    @Test
    public void testCreateDriverWhenDynamicFilterWasChanged()
    {
        CacheColumnId columnId = new CacheColumnId("cacheColumnId");
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(columnId), ImmutableList.of(BIGINT)),
                TupleDomain.all());
        ColumnHandle columnHandle = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> originalDynamicPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.singleValue(BIGINT, 0L)));

        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        AtomicReference<TupleDomain<ColumnHandle>> commonDynamicPredicate = new AtomicReference<>(TupleDomain.all());
        TestDynamicFilter commonDynamicFilter = new TestDynamicFilter(commonDynamicPredicate::get, true);
        Map<ColumnHandle, CacheColumnId> columnHandles = ImmutableMap.of(columnHandle, columnId);
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(columnId, columnHandle),
                () -> createStaticDynamicFilter(ImmutableList.of(commonDynamicFilter)),
                () -> createStaticDynamicFilter(ImmutableList.of(new TestDynamicFilter(() -> originalDynamicPredicate, true))),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());

        // baseSignature should use original dynamic filter because it contains more domains
        splitCache.setExpectedPredicates(TupleDomain.all(), originalDynamicPredicate.transformKeys(columnHandles::get));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();

        // baseSignature should use common dynamic filter as it uses same domains
        commonDynamicPredicate.set(TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L, 1L)))));
        splitCache.setExpectedPredicates(TupleDomain.all(), commonDynamicPredicate.get().transformKeys(columnHandles::get));
        cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
    }

    @Test
    public void testPrunesAndProjectsPredicates()
    {
        CacheColumnId projectedScanColumnId = new CacheColumnId("projectedScanColumnId");
        CacheColumnId projectedColumnId = new CacheColumnId("projectedColumnId");
        CacheColumnId nonProjectedScanColumnId = new CacheColumnId("nonProjectedScanColumnId");

        ColumnHandle projectedScanColumnHandle = new TestingColumnHandle("projectedScanColumnId");
        ColumnHandle nonProjectedScanColumnHandle = new TestingColumnHandle("nonProjectedScanColumnHandle");
        Map<CacheColumnId, ColumnHandle> columnHandles = ImmutableMap.of(projectedScanColumnId, projectedScanColumnHandle, nonProjectedScanColumnId, nonProjectedScanColumnHandle);

        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(projectedScanColumnId, projectedColumnId), ImmutableList.of(BIGINT, BIGINT)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnId, Domain.singleValue(BIGINT, 100L),
                        projectedColumnId, Domain.singleValue(BIGINT, 110L),
                        nonProjectedScanColumnId, Domain.singleValue(BIGINT, 120L))));

        StaticDynamicFilter dynamicFilter = createStaticDynamicFilter(ImmutableList.of(new TestDynamicFilter(
                () -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, Domain.singleValue(BIGINT, 200L),
                        nonProjectedScanColumnHandle, Domain.singleValue(BIGINT, 220L))),
                true)));

        PageSourceProvider pageSourceProvider = new TestPageSourceProvider(
                // unenforcedPredicateSupplier
                input -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, Domain.singleValue(BIGINT, 300L),
                        nonProjectedScanColumnHandle, Domain.singleValue(BIGINT, 310L))),
                // prunePredicateSupplier
                input -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, Domain.singleValue(BIGINT, 400L),
                        nonProjectedScanColumnHandle, Domain.singleValue(BIGINT, 410L))));
        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                pageSourceProvider,
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                columnHandles,
                () -> dynamicFilter,
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());

        splitCache.setExpectedPredicates(
                // predicate
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedColumnId, Domain.singleValue(BIGINT, 110L),
                        projectedScanColumnId, Domain.singleValue(BIGINT, 400L))),
                // unenforcedPredicate
                TupleDomain.withColumnDomains(ImmutableMap.of(projectedScanColumnId, Domain.singleValue(BIGINT, 300L))));
        splitCache.setExpectedCacheSplitId(appendRemainingPredicates(
                SPLIT_ID,
                Optional.of(tupleDomainCodec.toJson(normalizeTupleDomain(TupleDomain.withColumnDomains(ImmutableMap.of(nonProjectedScanColumnId, Domain.singleValue(BIGINT, 410L)))))),
                Optional.of(tupleDomainCodec.toJson(normalizeTupleDomain(TupleDomain.withColumnDomains(ImmutableMap.of(nonProjectedScanColumnId, Domain.singleValue(BIGINT, 310L))))))));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), SPLIT, Optional.of(SPLIT_ID));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
    }

    private static class TestPageSourceProvider
            implements PageSourceProvider
    {
        private final Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> unenforcedPredicateSupplier;
        private final Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> prunePredicateSupplier;

        public TestPageSourceProvider(
                Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> unenforcedPredicateSupplier,
                Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> prunePredicateSupplier)
        {
            this.unenforcedPredicateSupplier = unenforcedPredicateSupplier;
            this.prunePredicateSupplier = prunePredicateSupplier;
        }

        public TestPageSourceProvider()
        {
            // mimic connector returning compact effective predicate on extra column
            this(identity(), identity());
        }

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            return new FixedPageSource(rowPagesBuilder(BIGINT).build());
        }

        @Override
        public TupleDomain<ColumnHandle> getUnenforcedPredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> predicate)
        {
            return unenforcedPredicateSupplier.apply(predicate);
        }

        @Override
        public TupleDomain<ColumnHandle> prunePredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> predicate)
        {
            return prunePredicateSupplier.apply(predicate);
        }
    }

    private CacheDriverFactory createCacheDriverFactory(TestPageSourceProvider pageSourceProvider, PlanSignatureWithPredicate signature, AtomicInteger operatorIdAllocator)
    {
        DriverFactory driverFactory = createDriverFactory(operatorIdAllocator);
        return new CacheDriverFactory(
                TEST_SESSION,
                pageSourceProvider,
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(new CacheColumnId("column"), new TestingColumnHandle("column")),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());
    }

    private DriverFactory createDriverFactory(AtomicInteger operatorIdAllocator)
    {
        return new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());
    }

    private DriverContext createDriverContext()
    {
        return TestingTaskContext.createTaskContext(directExecutor(), scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static class TestDynamicFilter
            implements DynamicFilter
    {
        private final Supplier<TupleDomain<ColumnHandle>> dynamicPredicateSupplier;
        private boolean complete;

        public TestDynamicFilter(Supplier<TupleDomain<ColumnHandle>> dynamicPredicateSupplier, boolean complete)
        {
            this.dynamicPredicateSupplier = dynamicPredicateSupplier;
            this.complete = complete;
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete()
        {
            return complete;
        }

        @Override
        public boolean isAwaitable()
        {
            return false;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return dynamicPredicateSupplier.get();
        }

        @Override
        public OptionalLong getPreferredDynamicFilterTimeout()
        {
            return OptionalLong.of(0L);
        }
    }

    private static class TestCacheManagerFactory
            implements CacheManagerFactory
    {
        private final TestCacheManager cacheManager = new TestCacheManager();

        public TestCacheManager getCacheManager()
        {
            return cacheManager;
        }

        @Override
        public String getName()
        {
            return "TestCacheManager";
        }

        @Override
        public TestCacheManager create(Map<String, String> config, CacheManagerContext context)
        {
            return cacheManager;
        }
    }

    private static class TestCacheManager
            implements CacheManager
    {
        private final TestSplitCache splitCache = new TestSplitCache();

        public TestSplitCache getSplitCache()
        {
            return splitCache;
        }

        @Override
        public TestSplitCache getSplitCache(PlanSignature signature)
        {
            return splitCache;
        }

        @Override
        public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long revokeMemory(long bytesToRevoke)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestSplitCache
            implements SplitCache
    {
        private Optional<TupleDomain<CacheColumnId>> expectedPredicate = Optional.empty();
        private Optional<TupleDomain<CacheColumnId>> expectedUnenforcedPredicate = Optional.empty();
        private Optional<CacheSplitId> expectedCacheSplitId = Optional.empty();

        public void setExpectedPredicates(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.expectedPredicate = Optional.of(predicate);
            this.expectedUnenforcedPredicate = Optional.of(unenforcedPredicate);
        }

        public void setExpectedCacheSplitId(CacheSplitId expectedCacheSplitId)
        {
            this.expectedCacheSplitId = Optional.of(expectedCacheSplitId);
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.expectedPredicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            this.expectedUnenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            this.expectedCacheSplitId.ifPresent(expected -> assertThat(splitId).isEqualTo(expected));
            return Optional.of(new EmptyPageSource());
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.expectedPredicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            this.expectedUnenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            this.expectedCacheSplitId.ifPresent(expected -> assertThat(splitId).isEqualTo(expected));
            return Optional.empty();
        }

        @Override
        public void close() {}
    }
}
