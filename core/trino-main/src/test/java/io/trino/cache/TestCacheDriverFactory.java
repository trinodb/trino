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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.execution.ScheduledSplit;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
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
import static io.trino.cache.CacheDriverFactory.MAX_DYNAMIC_FILTER_VALUE_COUNT;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCacheDriverFactory
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    private TestSplitCache splitCache;
    private CacheManagerRegistry registry;
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
                new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), ImmutableList.of()),
                TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);

        // expect driver for original plan because cacheSplit is empty
        CacheDriverFactory cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(), signature, operatorIdAllocator);
        ScheduledSplit scheduledSplit = new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split);
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), scheduledSplit, Optional.empty());
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter filters data completely
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(input -> TupleDomain.none()), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), scheduledSplit, Optional.of(new CacheSplitId("split")));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter is too big
        Domain bigDomain = Domain.multipleValues(BIGINT, LongStream.range(0, MAX_DYNAMIC_FILTER_VALUE_COUNT + 1)
                .boxed()
                .collect(toImmutableList()));
        cacheDriverFactory = createCacheDriverFactory(
                new TestPageSourceProvider(input -> TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("column"), bigDomain))),
                signature,
                operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), scheduledSplit, Optional.of(new CacheSplitId("split")));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();
    }

    private CacheDriverFactory createCacheDriverFactory(TestPageSourceProvider pageSourceProvider, PlanSignatureWithPredicate signature, AtomicInteger operatorIdAllocator)
    {
        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(2, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(3, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());
        return new CacheDriverFactory(
                TEST_SESSION,
                pageSourceProvider,
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(new CacheColumnId("column"), new TestingColumnHandle("column")),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());
    }

    @Test
    public void testCreateDriverWhenDynamicFilterWasChanged()
    {
        CacheColumnId columnId = new CacheColumnId("cacheColumnId");
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(columnId), ImmutableList.of(BIGINT)),
                TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);
        ColumnHandle columnHandle = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> originalDynamicPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.create(ValueSet.of(BIGINT, 0L), false)));

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        AtomicReference<TupleDomain<ColumnHandle>> commonDynamicPredicate = new AtomicReference<>(TupleDomain.all());
        TestDynamicFilter commonDynamicFilter = new TestDynamicFilter(commonDynamicPredicate::get, true);
        Map<ColumnHandle, CacheColumnId> dynamicFilterColumnMapping = ImmutableMap.of(columnHandle, new CacheColumnId("cacheColumnId"));
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(columnId, columnHandle),
                () -> createStaticDynamicFilter(ImmutableList.of(commonDynamicFilter)),
                () -> createStaticDynamicFilter(ImmutableList.of(new TestDynamicFilter(() -> originalDynamicPredicate, true))),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());

        // baseSignature should use original dynamic filter because it contains more domains
        splitCache.setExpectedPredicates(TupleDomain.all(), originalDynamicPredicate.transformKeys(dynamicFilterColumnMapping::get));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("id")));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();

        // baseSignature should use common dynamic filter as it uses same domains
        commonDynamicPredicate.set(TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.create(ValueSet.of(BIGINT, 0L, 1L), false))));
        splitCache.setExpectedPredicates(TupleDomain.all(), commonDynamicPredicate.get().transformKeys(dynamicFilterColumnMapping::get));
        cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("id")));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
    }

    private static class TestPageSourceProvider
            implements PageSourceProvider
    {
        private final Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> simplifyPredicateSupplier;

        public TestPageSourceProvider(Function<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> simplifyPredicateSupplier)
        {
            this.simplifyPredicateSupplier = simplifyPredicateSupplier;
        }

        public TestPageSourceProvider()
        {
            // mimic connector returning compact effective predicate on extra column
            this(inputPredicate -> inputPredicate.intersect(TupleDomain.withColumnDomains(
                    ImmutableMap.of(new TestingColumnHandle("extra_column"), Domain.singleValue(DOUBLE, 42.0)))));
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
            return simplifyPredicateSupplier.apply(predicate);
        }
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
        private Optional<TupleDomain<CacheColumnId>> predicate = Optional.empty();
        private Optional<TupleDomain<CacheColumnId>> unenforcedPredicate = Optional.empty();

        public void setExpectedPredicates(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.predicate = Optional.of(predicate);
            this.unenforcedPredicate = Optional.of(unenforcedPredicate);
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.predicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            this.unenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            return Optional.of(new EmptyPageSource());
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            this.predicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            this.unenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            return Optional.empty();
        }

        @Override
        public void close() {}
    }
}
