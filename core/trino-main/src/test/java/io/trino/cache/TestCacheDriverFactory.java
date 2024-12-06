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
import io.trino.operator.OperatorDriverFactory;
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
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_ROW_FILTERING;
import static io.trino.cache.CacheDriverFactory.MAX_UNENFORCED_PREDICATE_VALUE_COUNT;
import static io.trino.cache.CacheDriverFactory.appendRemainingPredicates;
import static io.trino.cache.CacheDriverFactory.getDynamicRowFilteringUnenforcedPredicate;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilterSupplier;
import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.PlanTester.getTupleDomainJsonCodec;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createRemoteSplit;
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
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("test");

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
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.empty()));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because split got scheduled on non-preferred node
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(
                createDriverContext(),
                createSplit(Optional.of(new CacheSplitId("split")), false));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter filters data completely
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(input -> TupleDomain.none(), identity()), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because enforced predicate is pruned to empty tuple domain
        cacheDriverFactory = createCacheDriverFactory(new TestPageSourceProvider(identity(), input -> TupleDomain.none()), signature, operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();

        // expect driver for original plan because dynamic filter is too big
        Domain bigDomain = multipleValues(BIGINT, LongStream.range(0, MAX_UNENFORCED_PREDICATE_VALUE_COUNT + 1)
                .boxed()
                .collect(toImmutableList()));
        cacheDriverFactory = createCacheDriverFactory(
                new TestPageSourceProvider(_ -> TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("column"), bigDomain)), identity()),
                signature,
                operatorIdAllocator);
        driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isEmpty();
    }

    @Test
    public void testCreateDriverWithSmallerDynamicFilter()
    {
        CacheColumnId cacheColumnId = new CacheColumnId("cacheColumn");
        ColumnHandle columnHandle = new TestingColumnHandle("column");

        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(cacheColumnId), ImmutableList.of(BIGINT)),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(cacheColumnId, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList()))));
        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        Map<ColumnHandle, CacheColumnId> columnHandles = ImmutableMap.of(columnHandle, cacheColumnId);

        // use original dynamic filter
        CacheDriverFactory cacheDriverFactory = createCacheDriverFactory(
                signature,
                columnHandles,
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnHandle, multipleValues(BIGINT, LongStream.range(0L, 5000L).boxed().toList()))), true))),
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnHandle, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList()))), true))),
                driverFactory);

        Optional<ConnectorPageSource> pageSource = Optional.of(new EmptyPageSource());
        splitCache.addExpectedCacheLookup(
                Optional.of(SPLIT_ID),
                Optional.of(TupleDomain.withColumnDomains(
                        ImmutableMap.of(cacheColumnId, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList())))),
                Optional.of(TupleDomain.withColumnDomains(
                        ImmutableMap.of(cacheColumnId, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList())))),
                pageSource);
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
        assertThat(driver.getDriverContext().getCacheDriverContext().get().pageSource()).isEqualTo(pageSource);

        // use common dynamic filter
        cacheDriverFactory = createCacheDriverFactory(
                signature,
                columnHandles,
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnHandle, multipleValues(BIGINT, LongStream.range(0L, 150L).boxed().toList()))), true))),
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnHandle, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList()))), true))),
                driverFactory);

        splitCache.addExpectedCacheLookup(
                Optional.of(SPLIT_ID),
                Optional.of(TupleDomain.withColumnDomains(
                        ImmutableMap.of(cacheColumnId, multipleValues(BIGINT, LongStream.range(0L, 100L).boxed().toList())))),
                Optional.of(TupleDomain.withColumnDomains(
                        ImmutableMap.of(cacheColumnId, multipleValues(BIGINT, LongStream.range(0L, 150L).boxed().toList())))),
                pageSource);
        driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
        assertThat(driver.getDriverContext().getCacheDriverContext().get().pageSource()).isEqualTo(pageSource);
    }

    @Test
    public void testCreateDriverWhenDynamicFilterWasChanged()
    {
        CacheColumnId columnId = new CacheColumnId("cacheColumnId");
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(columnId), ImmutableList.of(BIGINT)),
                TupleDomain.all());
        ColumnHandle columnHandle = new TestingColumnHandle("column");
        TupleDomain<ColumnHandle> originalDynamicPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, singleValue(BIGINT, 0L)));

        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        TestDynamicFilter commonDynamicFilter = new TestDynamicFilter(TupleDomain.all(), false);
        Map<ColumnHandle, CacheColumnId> columnHandles = ImmutableMap.of(columnHandle, columnId);
        CacheDriverFactory cacheDriverFactory = createCacheDriverFactory(
                signature,
                columnHandles,
                createStaticDynamicFilterSupplier(ImmutableList.of(commonDynamicFilter)),
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(originalDynamicPredicate, true))),
                driverFactory);

        // baseSignature should use original dynamic filter because it contains more domains
        splitCache.addExpectedCacheLookup(TupleDomain.all(), originalDynamicPredicate.transformKeys(columnHandles::get));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();

        // baseSignature should use common dynamic filter as it uses same domains
        commonDynamicFilter.setDynamicPredicate(
                TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, multipleValues(BIGINT, ImmutableList.of(0L, 1L)))),
                true);
        splitCache.addExpectedCacheLookup(TupleDomain.all(), commonDynamicFilter.getCurrentPredicate().transformKeys(columnHandles::get));
        cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
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
                        projectedScanColumnId, singleValue(BIGINT, 100L),
                        projectedColumnId, singleValue(BIGINT, 110L),
                        nonProjectedScanColumnId, singleValue(BIGINT, 120L))));

        StaticDynamicFilter dynamicFilter = createStaticDynamicFilter(ImmutableList.of(new TestDynamicFilter(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, singleValue(BIGINT, 200L),
                        nonProjectedScanColumnHandle, singleValue(BIGINT, 220L))),
                true)));

        PageSourceProvider pageSourceProvider = new TestPageSourceProvider(
                // unenforcedPredicateSupplier
                _ -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, singleValue(BIGINT, 300L),
                        nonProjectedScanColumnHandle, singleValue(BIGINT, 310L))),
                // prunePredicateSupplier
                _ -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedScanColumnHandle, singleValue(BIGINT, 400L),
                        nonProjectedScanColumnHandle, singleValue(BIGINT, 410L))));
        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                0,
                true,
                true,
                OptionalInt.empty(),
                PLAN_NODE_ID,
                Session.builder(TEST_SESSION)
                        // dynamic row filtering prevents propagation of domain values
                        .setSystemProperty(ENABLE_DYNAMIC_ROW_FILTERING, "false")
                        .build(),
                pageSourceProvider,
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                columnHandles,
                createStaticDynamicFilterSupplier(ImmutableList.of(dynamicFilter)),
                createStaticDynamicFilterSupplier(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());

        splitCache.addExpectedCacheLookup(
                // cacheId
                appendRemainingPredicates(
                        SPLIT_ID,
                        Optional.of(tupleDomainCodec.toJson(normalizeTupleDomain(TupleDomain.withColumnDomains(ImmutableMap.of(nonProjectedScanColumnId, singleValue(BIGINT, 410L)))))),
                        Optional.of(tupleDomainCodec.toJson(normalizeTupleDomain(TupleDomain.withColumnDomains(ImmutableMap.of(nonProjectedScanColumnId, singleValue(BIGINT, 310L))))))),
                // predicate
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        projectedColumnId, singleValue(BIGINT, 110L),
                        projectedScanColumnId, singleValue(BIGINT, 400L))),
                // unenforcedPredicate
                TupleDomain.withColumnDomains(ImmutableMap.of(projectedScanColumnId, singleValue(BIGINT, 300L))));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();
    }

    @Test
    public void testDynamicRowFilteringUnenforcedPredicate()
    {
        Split split = new Split(TEST_CATALOG_HANDLE, createRemoteSplit(), Optional.of(SPLIT_ID), true);
        CacheColumnId nonDfColumnId = new CacheColumnId("nonDfColumnId");
        CacheColumnId dfColumnId1 = new CacheColumnId("dfColumnId");
        CacheColumnId dfColumnId2 = new CacheColumnId("dfColumnId2");

        ColumnHandle nonDfColumnHandle = new TestingColumnHandle("nonDfColumnHandle");
        ColumnHandle dfColumnHandle1 = new TestingColumnHandle("dfColumnHandle1");
        ColumnHandle dfColumnHandle2 = new TestingColumnHandle("dfColumnHandle2");

        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(SIGNATURE_KEY, Optional.empty(), ImmutableList.of(nonDfColumnId, dfColumnId1, dfColumnId2), ImmutableList.of(BIGINT, BIGINT, BIGINT)),
                TupleDomain.all());
        TestPageSourceProvider pageSourceProvider = new TestPageSourceProvider(
                (_) -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        nonDfColumnHandle, singleValue(BIGINT, 10L),
                        dfColumnHandle1, multipleValues(BIGINT, ImmutableList.of(20L, 22L, 23L)))),
                (_) -> TupleDomain.withColumnDomains(ImmutableMap.of(
                        dfColumnHandle1, multipleValues(BIGINT, ImmutableList.of(20L, 21L)))));
        DriverFactory driverFactory = createDriverFactory(new AtomicInteger());
        CacheDriverFactory cacheDriverFactory = createCacheDriverFactory(
                pageSourceProvider,
                signature,
                ImmutableMap.of(nonDfColumnHandle, nonDfColumnId, dfColumnHandle1, dfColumnId1, dfColumnHandle2, dfColumnId2),
                createStaticDynamicFilterSupplier(ImmutableList.of(new TestDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                        dfColumnHandle1, multipleValues(BIGINT, ImmutableList.of(20L, 21L, 23L)),
                        dfColumnHandle2, singleValue(BIGINT, 30L))), true))),
                createStaticDynamicFilterSupplier(ImmutableList.of(EMPTY)),
                driverFactory);
        // signature unenforced predicate consists of pruned DF columns which are intersected with unenforced predicate of delegate page source provider
        splitCache.addExpectedCacheLookup(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dfColumnId1, multipleValues(BIGINT, ImmutableList.of(20L, 21L)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        nonDfColumnId, singleValue(BIGINT, 10L),
                        dfColumnId1, singleValue(BIGINT, 20L))));
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), createSplit(Optional.of(SPLIT_ID)));
        assertThat(driver.getDriverContext().getCacheDriverContext()).isPresent();

        // delegate provider returns TupleDomain.none()
        assertThat(getDynamicRowFilteringUnenforcedPredicate(
                new TestPageSourceProvider((_) -> TupleDomain.none(), (_) -> TupleDomain.none()),
                TEST_SESSION,
                split,
                TEST_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(dfColumnHandle1, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.none());

        // delegate provider returns TupleDomain.all()
        assertThat(getDynamicRowFilteringUnenforcedPredicate(
                new TestPageSourceProvider((_) -> TupleDomain.all(), (_) -> TupleDomain.all()),
                TEST_SESSION,
                split,
                TEST_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(dfColumnHandle1, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.all());
    }

    private Optional<ScheduledSplit> createSplit(Optional<CacheSplitId> cacheSplitId)
    {
        return createSplit(cacheSplitId, true);
    }

    private Optional<ScheduledSplit> createSplit(Optional<CacheSplitId> cacheSplitId, boolean splitAddressEnforced)
    {
        return Optional.of(new ScheduledSplit(
                0,
                PLAN_NODE_ID,
                new Split(TEST_CATALOG_HANDLE, createRemoteSplit(), cacheSplitId, splitAddressEnforced)));
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

    private CacheDriverFactory createCacheDriverFactory(
            PlanSignatureWithPredicate signature,
            Map<ColumnHandle, CacheColumnId> columnHandles,
            Supplier<StaticDynamicFilter> commonDynamicFilterSupplier,
            Supplier<StaticDynamicFilter> originalDynamicFilterSupplier,
            DriverFactory driverFactory)
    {
        return createCacheDriverFactory(
                new TestPageSourceProvider(),
                signature,
                columnHandles,
                commonDynamicFilterSupplier,
                originalDynamicFilterSupplier,
                driverFactory);
    }

    private CacheDriverFactory createCacheDriverFactory(
            TestPageSourceProvider pageSourceProvider,
            PlanSignatureWithPredicate signature,
            Map<ColumnHandle, CacheColumnId> columnHandles,
            Supplier<StaticDynamicFilter> commonDynamicFilterSupplier,
            Supplier<StaticDynamicFilter> originalDynamicFilterSupplier,
            DriverFactory driverFactory)
    {
        return new CacheDriverFactory(
                0,
                true,
                true,
                OptionalInt.empty(),
                PLAN_NODE_ID,
                TEST_SESSION,
                pageSourceProvider,
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                columnHandles.entrySet().stream().collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey)),
                commonDynamicFilterSupplier,
                originalDynamicFilterSupplier,
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());
    }

    private CacheDriverFactory createCacheDriverFactory(
            TestPageSourceProvider pageSourceProvider,
            PlanSignatureWithPredicate signature,
            AtomicInteger operatorIdAllocator)
    {
        DriverFactory driverFactory = createDriverFactory(operatorIdAllocator);
        return new CacheDriverFactory(0,
                true,
                true,
                OptionalInt.empty(),
                PLAN_NODE_ID,
                TEST_SESSION,
                pageSourceProvider,
                registry,
                tupleDomainCodec,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(new CacheColumnId("column"), new TestingColumnHandle("column")),
                createStaticDynamicFilterSupplier(ImmutableList.of(EMPTY)),
                createStaticDynamicFilterSupplier(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory, driverFactory),
                new CacheStats());
    }

    private DriverFactory createDriverFactory(AtomicInteger operatorIdAllocator)
    {
        return new OperatorDriverFactory(
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
        private TupleDomain<ColumnHandle> dynamicPredicate;
        private boolean complete;
        private CompletableFuture<?> blocked = new CompletableFuture<>();

        public TestDynamicFilter(TupleDomain<ColumnHandle> dynamicPredicate, boolean complete)
        {
            this.dynamicPredicate = dynamicPredicate;
            this.complete = complete;
        }

        public void setDynamicPredicate(TupleDomain<ColumnHandle> dynamicPredicate, boolean complete)
        {
            checkState(!this.complete);
            this.dynamicPredicate = dynamicPredicate;
            this.complete = complete;
            CompletableFuture<?> blocked = this.blocked;
            if (!complete) {
                this.blocked = new CompletableFuture<>();
            }
            blocked.complete(null);
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return blocked;
        }

        @Override
        public boolean isComplete()
        {
            return complete;
        }

        @Override
        public boolean isAwaitable()
        {
            return !complete;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return dynamicPredicate;
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
        public long revokeMemory(long bytesToRevoke)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestSplitCache
            implements SplitCache
    {
        private final LinkedList<CacheLookup> expectedCacheLookups = new LinkedList<>();

        public void addExpectedCacheLookup(TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            addExpectedCacheLookup(Optional.empty(), Optional.of(predicate), Optional.of(unenforcedPredicate), Optional.of(new EmptyPageSource()));
        }

        public void addExpectedCacheLookup(CacheSplitId cacheSplitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            addExpectedCacheLookup(Optional.of(cacheSplitId), Optional.of(predicate), Optional.of(unenforcedPredicate), Optional.of(new EmptyPageSource()));
        }

        public void addExpectedCacheLookup(
                Optional<CacheSplitId> expectedCacheSplitId,
                Optional<TupleDomain<CacheColumnId>> expectedPredicate,
                Optional<TupleDomain<CacheColumnId>> expectedUnenforcedPredicate,
                Optional<ConnectorPageSource> pageSource)
        {
            expectedCacheLookups.add(new CacheLookup(expectedCacheSplitId, expectedPredicate, expectedUnenforcedPredicate, pageSource));
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            assertThat(expectedCacheLookups.size()).isGreaterThan(0);
            CacheLookup cacheLookup = expectedCacheLookups.pollFirst();
            cacheLookup.expectedPredicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            cacheLookup.expectedUnenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            cacheLookup.expectedCacheSplitId.ifPresent(expected -> assertThat(splitId).isEqualTo(expected));
            return cacheLookup.pageSource;
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate)
        {
            assertThat(expectedCacheLookups.size()).isGreaterThan(0);
            CacheLookup cacheLookup = expectedCacheLookups.pollFirst();
            cacheLookup.expectedPredicate.ifPresent(expected -> assertThat(predicate).isEqualTo(expected));
            cacheLookup.expectedUnenforcedPredicate.ifPresent(expected -> assertThat(unenforcedPredicate).isEqualTo(expected));
            cacheLookup.expectedCacheSplitId.ifPresent(expected -> assertThat(splitId).isEqualTo(expected));
            return Optional.empty();
        }

        @Override
        public void close() {}

        record CacheLookup(
                Optional<CacheSplitId> expectedCacheSplitId,
                Optional<TupleDomain<CacheColumnId>> expectedPredicate,
                Optional<TupleDomain<CacheColumnId>> expectedUnenforcedPredicate,
                Optional<ConnectorPageSource> pageSource) {}
    }
}
