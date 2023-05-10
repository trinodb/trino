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
import io.trino.execution.ScheduledSplit;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.DevNullOperator;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.testing.TestingSplit;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCacheDriverFactory
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    private CacheManagerRegistry registry;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(16, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(32, MEGABYTE));
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setEnabled(true);
        registry = new CacheManagerRegistry(cacheConfig, new LocalMemoryManager(config, DataSize.of(1024, MEGABYTE).toBytes()), new TestingBlockEncodingSerde());
        registry.loadCacheManager();
        scheduledExecutor = Executors.newScheduledThreadPool(1);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testUpdateCache()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory));

        PlanSignature allPlanSignature = new PlanSignature(new SignatureKey("signature"), Optional.empty(), ImmutableList.of(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        PlanSignature nonePlanSignature = allPlanSignature.withDynamicPredicate(TupleDomain.none());

        // it is the first time when splitCache is updated in driver factory - expect that it was set in factory
        cacheDriverFactory.updateSplitCache(allPlanSignature, false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(allPlanSignature);

        // dynamic filter was not changed - do not update it in factory
        cacheDriverFactory.updateSplitCache(allPlanSignature.withDynamicPredicate(TupleDomain.all()), false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(allPlanSignature);

        // dynamic filter was changed - do update it in factory
        cacheDriverFactory.updateSplitCache(nonePlanSignature, true);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(nonePlanSignature);

        // previous non-final "all" signature was passed - do update it in factory
        cacheDriverFactory.updateSplitCache(allPlanSignature, false);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(allPlanSignature);

        // again final "none" signature was passed - do update it in factory
        cacheDriverFactory.updateSplitCache(nonePlanSignature, true);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(nonePlanSignature);

        // "all" signature was passed with complete flag - do not update it in factory
        cacheDriverFactory.updateSplitCache(allPlanSignature, true);
        assertThat(cacheDriverFactory.getCachePlanSignature()).isEqualTo(nonePlanSignature);
    }

    @Test
    public void testCreateDriverForOriginalPlan()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
        AtomicInteger operatorIdAllocator = new AtomicInteger();
        ConnectorSplit connectorSplit = new TestingSplit(false, ImmutableList.of());
        Split split = new Split(TEST_CATALOG_HANDLE, connectorSplit);

        DriverFactory driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(0, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(1, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory));

        // expect driver for original plan because cacheSplit is empty
        Driver driver = cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.empty());
        assertTrue(driver.getDriverContext().getCacheDriverContext().isEmpty());

        driverFactory = new DriverFactory(
                operatorIdAllocator.incrementAndGet(),
                true,
                false,
                ImmutableList.of(
                        new DevNullOperator.DevNullOperatorFactory(2, planNodeIdAllocator.getNextId()),
                        new DevNullOperator.DevNullOperatorFactory(3, planNodeIdAllocator.getNextId())),
                OptionalInt.empty());

        cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(input -> TupleDomain.none()),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                () -> createStaticDynamicFilter(ImmutableList.of(EMPTY)),
                ImmutableList.of(driverFactory, driverFactory));

        // expect driver for original plan because dynamic filter filters data completely
        driver = cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("split")));
        assertTrue(driver.getDriverContext().getCacheDriverContext().isEmpty());
    }

    @Test
    public void testCreateDriverWhenDynamicFilterWasChanged()
    {
        PlanSignature signature = new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), ImmutableList.of(), TupleDomain.all(), TupleDomain.all());
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
        TestDynamicFilter commonDynamicFilter = new TestDynamicFilter(commonDynamicPredicate::get, false);
        Map<ColumnHandle, CacheColumnId> dynamicFilterColumnMapping = ImmutableMap.of(columnHandle, new CacheColumnId("cacheColumnId"));
        CacheDriverFactory cacheDriverFactory = new CacheDriverFactory(
                TEST_SESSION,
                new TestPageSourceProvider(),
                registry,
                TEST_TABLE_HANDLE,
                signature,
                ImmutableMap.of(new CacheColumnId("cacheColumnId"), columnHandle),
                () -> createStaticDynamicFilter(ImmutableList.of(commonDynamicFilter)),
                () -> createStaticDynamicFilter(ImmutableList.of(new TestDynamicFilter(() -> originalDynamicPredicate, true))),
                ImmutableList.of(driverFactory, driverFactory));

        // baseSignature's dynamic filters should be changed because new dynamic filter was supplied
        cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("id")));
        assertThat(cacheDriverFactory.getCachePlanSignature().getDynamicPredicate()).isEqualTo(originalDynamicPredicate.transformKeys(dynamicFilterColumnMapping::get));

        // baseSignature should use common dynamic filter as it uses same domains
        commonDynamicPredicate.set(TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.create(ValueSet.of(BIGINT, 0L, 1L), false))));
        cacheDriverFactory.createDriver(createDriverContext(), new ScheduledSplit(0, planNodeIdAllocator.getNextId(), split), Optional.of(new CacheSplitId("id")));
        assertThat(cacheDriverFactory.getCachePlanSignature().getDynamicPredicate()).isEqualTo(commonDynamicPredicate.get().transformKeys(dynamicFilterColumnMapping::get));
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
        public TupleDomain<ColumnHandle> simplifyPredicate(
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
        public long getPreferredDynamicFilterTimeout()
        {
            return 0;
        }
    }
}
