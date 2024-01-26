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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.CacheConfig;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.CacheDriverContext;
import io.trino.cache.CacheDriverFactory;
import io.trino.cache.CacheManagerRegistry;
import io.trino.cache.CacheMetrics;
import io.trino.cache.CacheStats;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.execution.ScheduledSplit;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.cache.CacheDriverFactory.MIN_PROCESSED_SPLITS;
import static io.trino.cache.CacheDriverFactory.THRASHING_CACHE_THRESHOLD;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.operator.PageTestUtils.createPage;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCacheDataOperator
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    private CacheManagerRegistry registry;

    @BeforeEach
    public void setUp()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(10, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(100, MEGABYTE));
        LocalMemoryManager memoryManager = new LocalMemoryManager(config, DataSize.of(110, MEGABYTE).toBytes());
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setEnabled(true);
        registry = new CacheManagerRegistry(cacheConfig, memoryManager, new TestingBlockEncodingSerde(), new CacheStats());
        registry.loadCacheManager();
    }

    @Test
    public void testLimitCache()
    {
        PlanSignature signature = createPlanSignature("sig");
        CacheManager.SplitCache splitCache = registry.getCacheManager().getSplitCache(signature);
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();

        CacheDataOperator.CacheDataOperatorFactory operatorFactory = new CacheDataOperator.CacheDataOperatorFactory(
                0,
                planNodeIdAllocator.getNextId(),
                DataSize.of(1024, DataSize.Unit.BYTE).toBytes());
        DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        CacheMetrics cacheMetrics = new CacheMetrics();
        CacheStats cacheStats = new CacheStats();

        driverContext.setCacheDriverContext(new CacheDriverContext(Optional.empty(), splitCache.storePages(new CacheSplitId("split1"), TupleDomain.all(), TupleDomain.all()), EMPTY, cacheMetrics, cacheStats, Metrics.EMPTY));
        CacheDataOperator cacheDataOperator = (CacheDataOperator) operatorFactory.createOperator(driverContext);

        // sink was not aborted - there is a space in a cache. The page was passed through and split is going to be cached
        Page smallPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 10)));
        cacheDataOperator.addInput(smallPage);
        assertThat(cacheDataOperator.getOutput()).isEqualTo(smallPage);
        cacheDataOperator.finish();
        assertThat(cacheMetrics.getSplitNotCachedCount()).isEqualTo(0);
        assertThat(cacheMetrics.getSplitCachedCount()).isEqualTo(1);

        // sink was aborted - there is no sufficient space in a cache. The page was passed through but split is not going to be cached
        Page bigPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 2_000)));
        driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        driverContext.setCacheDriverContext(new CacheDriverContext(Optional.empty(), splitCache.storePages(new CacheSplitId("split2"), TupleDomain.all(), TupleDomain.all()), EMPTY, cacheMetrics, cacheStats, Metrics.EMPTY));
        cacheDataOperator = (CacheDataOperator) operatorFactory.createOperator(driverContext);

        cacheDataOperator.addInput(bigPage);
        cacheDataOperator.finish();

        assertThat(cacheDataOperator.getOutput()).isEqualTo(bigPage);
        assertThat(cacheMetrics.getSplitNotCachedCount()).isEqualTo(1);
        assertThat(cacheMetrics.getSplitCachedCount()).isEqualTo(1);
    }

    @Test
    public void testCachingThreshold()
    {
        PlanSignature signature = createPlanSignature("sig");
        Page bigPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 128)));
        Page smallPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 16)));
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        CacheDataOperator.CacheDataOperatorFactory cacheDataOperatorFactory = new CacheDataOperator.CacheDataOperatorFactory(
                operatorIdAllocator.incrementAndGet(),
                planNodeIdAllocator.getNextId(),
                DataSize.of(1024, DataSize.Unit.BYTE).toBytes());

        PassThroughOperator.PassThroughOperatorFactory passThroughOperatorFactory =
                new PassThroughOperator.PassThroughOperatorFactory(operatorIdAllocator.incrementAndGet(), planNodeIdAllocator.getNextId(), () -> smallPage);

        List<DriverFactory> driverFactories = ImmutableList.of(
                prepareDriverFactory(operatorIdAllocator, 2, preparePassThroughOperator(() -> smallPage)),
                new DriverFactory(
                        operatorIdAllocator.incrementAndGet(),
                        true,
                        false,
                        ImmutableList.of(passThroughOperatorFactory, cacheDataOperatorFactory),
                        OptionalInt.empty()),
                prepareDriverFactory(operatorIdAllocator, 2, preparePassThroughOperator(() -> smallPage)));

        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                new PlanSignatureWithPredicate(signature, TupleDomain.all()),
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                driverFactories,
                new CacheStats());

        // process splits where split's page is small. All splits will be successfully cached
        createAndRunDriver(0, MIN_PROCESSED_SPLITS, cacheDriverFactory);
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitCachedCount()).isEqualTo(MIN_PROCESSED_SPLITS);
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitNotCachedCount()).isEqualTo(0);

        int splitToBeRejectedCount = (int) Math.ceil((MIN_PROCESSED_SPLITS * (1.0f - THRASHING_CACHE_THRESHOLD)) / THRASHING_CACHE_THRESHOLD);

        // try to process splits that cannot be cached because its page sizes exceeds threshold size
        // caching is not going to be "disabled" because threshold was not exceeded.
        passThroughOperatorFactory.setPageSupplier(() -> bigPage);
        createAndRunDriver(MIN_PROCESSED_SPLITS, MIN_PROCESSED_SPLITS + splitToBeRejectedCount, cacheDriverFactory);
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitCachedCount()).isEqualTo(MIN_PROCESSED_SPLITS);
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitNotCachedCount()).isEqualTo(splitToBeRejectedCount);

        // exceed threshold
        CacheSplitId splitId = new CacheSplitId(String.format("split_%d", MIN_PROCESSED_SPLITS + splitToBeRejectedCount));
        DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        try (Driver driver = cacheDriverFactory.createDriver(driverContext, new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(splitId))) {
            assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();
        }
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitCachedCount()).isEqualTo(MIN_PROCESSED_SPLITS);
        assertThat(cacheDriverFactory.getCacheMetrics().getSplitNotCachedCount()).isEqualTo(splitToBeRejectedCount);
    }

    private static PlanSignature createPlanSignature(String signature)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.of(new CacheColumnId("id")),
                ImmutableList.of(INTEGER));
    }

    private void createAndRunDriver(int start, int end, CacheDriverFactory cacheDriverFactory)
    {
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);
        for (int i = start; i < end; i++) {
            CacheSplitId splitId = new CacheSplitId(String.format("split_%d", i));
            DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
            try (Driver driver = cacheDriverFactory.createDriver(driverContext, new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(splitId))) {
                driver.process(new Duration(10.0, TimeUnit.SECONDS), 100);
                assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
            }
        }
    }

    private DriverFactory prepareDriverFactory(AtomicInteger operatorIdAllocator, int operatorsCount, Function<Integer, OperatorFactory> operatorFactoryProvider)
    {
        return new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                IntStream.range(0, operatorsCount).mapToObj(i -> operatorFactoryProvider.apply(operatorIdAllocator.incrementAndGet())).toList(),
                OptionalInt.empty());
    }

    private Function<Integer, OperatorFactory> preparePassThroughOperator(Supplier<Page> pageSupplier)
    {
        return (operatorId) -> new PassThroughOperator.PassThroughOperatorFactory(operatorId, planNodeIdAllocator.getNextId(), pageSupplier);
    }

    private static class TestPageSourceProvider
            implements PageSourceProvider
    {
        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            return new FixedPageSource(ImmutableList.of());
        }

        @Override
        public TupleDomain<ColumnHandle> getUnenforcedPredicate(
                Session session,
                Split split,
                TableHandle table,
                TupleDomain<ColumnHandle> predicate)
        {
            return TupleDomain.all();
        }
    }

    private static class PassThroughOperator
            implements Operator
    {
        private static class PassThroughOperatorFactory
                implements OperatorFactory
        {
            private final int operatorId;
            private final PlanNodeId planNodeId;
            private Supplier<Page> pageSupplier;

            public PassThroughOperatorFactory(int operatorId, PlanNodeId planNodeId, Supplier<Page> pageSupplier)
            {
                this.operatorId = operatorId;
                this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
                this.pageSupplier = requireNonNull(pageSupplier, "pageSupplier is null");
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                return new PassThroughOperator(driverContext.addOperatorContext(operatorId, planNodeId, PassThroughOperator.class.getSimpleName()), pageSupplier.get());
            }

            @Override
            public void noMoreOperators()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                return new PassThroughOperatorFactory(operatorId, planNodeId, pageSupplier);
            }

            void setPageSupplier(Supplier<Page> pageSupplier)
            {
                this.pageSupplier = pageSupplier;
            }
        }

        private final OperatorContext context;
        private final Page page;
        private boolean finished;

        public PassThroughOperator(OperatorContext context, Page page)
        {
            this.context = requireNonNull(context, "context is null");
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return context;
        }

        @Override
        public boolean needsInput()
        {
            return !finished;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getOutput()
        {
            finished = true;
            return page;
        }

        @Override
        public void finish()
        {
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }
    }
}
