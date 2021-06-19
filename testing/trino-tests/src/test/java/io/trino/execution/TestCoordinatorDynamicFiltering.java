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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.split.EmptySplit;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingHandleResolver;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingPageSinkProvider;
import io.trino.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCoordinatorDynamicFiltering
        extends AbstractTestQueryFramework
{
    private static final TestingMetadata.TestingColumnHandle SUPP_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("suppkey", 2, BIGINT);
    private static final TestingMetadata.TestingColumnHandle ADDRESS_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("address", 2, createVarcharType(40));
    private static final TestingMetadata.TestingColumnHandle SS_SOLD_SK_HANDLE = new TestingMetadata.TestingColumnHandle("ss_sold_date_sk", 0, BIGINT);

    private volatile TupleDomain<ColumnHandle> expectedDynamicFilter;

    @BeforeClass
    public void setup()
    {
        // create lineitem table in test connector
        getQueryRunner().installPlugin(new TestPlugin());
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().installPlugin(new TpcdsPlugin());
        getQueryRunner().installPlugin(new MemoryPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
        getQueryRunner().createCatalog("tpcds", "tpcds", ImmutableMap.of());
        getQueryRunner().createCatalog("memory", "memory", ImmutableMap.of());
        computeActual("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
        computeActual("CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer");
        computeActual("CREATE TABLE store_sales AS SELECT * FROM tpcds.tiny.store_sales");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
        return DistributedQueryRunner.builder(session)
                .setExtraProperties(ImmutableMap.of(
                        // keep limits lower to test edge cases
                        "dynamic-filtering.small-partitioned.max-distinct-values-per-driver", "10"))
                .build();
    }

    @Test(timeOut = 30_000)
    public void testJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testBroadcastJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithLargeBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.orders ON lineitem.orderkey = orders.orderkey",
                TupleDomain.all());
    }

    @Test(timeOut = 30_000)
    public void testBroadcastJoinWithLargeBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.orders ON lineitem.orderkey = orders.orderkey",
                TupleDomain.all());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testInequalityJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey <= supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey < supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey >= supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey > supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test(timeOut = 30_000)
    public void testIsNotDistinctFromJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk = store.s_closed_date_sk",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.create(ValueSet.of(BIGINT, 2451189L), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk IS NOT DISTINCT FROM store.s_closed_date_sk",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.create(ValueSet.of(BIGINT, 2451189L), true))));
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk IS NOT DISTINCT FROM store.s_closed_date_sk AND store.s_closed_date_sk < 0",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.onlyNull(BIGINT))));
    }

    @Test(timeOut = 30_000)
    public void testBroadcastJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithImplicitCoercion()
    {
        // setup fact table with integer suppkey
        computeActual("CREATE TABLE memory.default.supplier_decimal AS SELECT name, CAST(suppkey as decimal(19, 0)) suppkey_decimal FROM tpch.tiny.supplier");

        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN memory.default.supplier_decimal s ON lineitem.suppkey = s.suppkey_decimal AND s.name >= 'Supplier#000000080'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        multipleValues(BIGINT, LongStream.rangeClosed(80L, 100L).boxed().collect(toImmutableList())))));

        computeActual("CREATE TABLE memory.default.supplier_varchar AS SELECT name, CAST(address as varchar(42)) address FROM tpch.tiny.supplier");

        List<String> values = computeActual("SELECT address FROM memory.default.supplier_varchar WHERE name >= 'Supplier#000000080'")
                .getOnlyColumn()
                .map(Object::toString)
                .collect(toImmutableList());

        assertQueryDynamicFilters(
                "SELECT * FROM customer JOIN memory.default.supplier_varchar s ON customer.address = s.address AND s.name >= 'Supplier#000000080'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        ADDRESS_KEY_HANDLE,
                        multipleValues(createVarcharType(40), values))));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        assertQueryDynamicFilters(
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN tpch.tiny.partsupp ON t.suppkey = partsupp.suppkey AND partsupp.suppkey IN (2, 3)",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 2L))));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey WHERE supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey WHERE supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'abc')",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testBroadcastSemiJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'abc')",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithLargeBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM tpch.tiny.orders)",
                TupleDomain.all());
    }

    @Test(timeOut = 30_000)
    public void testBroadcastSemiJoinWithLargeBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM tpch.tiny.orders)",
                TupleDomain.all());
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'Supplier#000000001')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testBroadcastSemiJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'Supplier#000000001')",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier)",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithMultipleDynamicFiltersOnProbe()
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        assertQueryDynamicFilters(
                "SELECT * FROM (" +
                        "SELECT lineitem.suppkey FROM lineitem WHERE lineitem.suppkey IN " +
                        "(SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name IN ('Supplier#000000001', 'Supplier#000000002'))) t " +
                        "WHERE t.suppkey IN (SELECT partsupp.suppkey FROM tpch.tiny.partsupp WHERE partsupp.suppkey IN (2, 3))",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 2L))));
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private void assertQueryDynamicFilters(@Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        assertQueryDynamicFilters(getSession(), query, expectedTupleDomain);
    }

    private void assertQueryDynamicFilters(Session session, @Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        expectedDynamicFilter = expectedTupleDomain;
        computeActual(session, query);
    }

    private class TestPlugin
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                private final ConnectorMetadata metadata = new TestingMetadata();

                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public ConnectorHandleResolver getHandleResolver()
                {
                    return new TestingHandleResolver();
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(metadata);
                }
            });
        }
    }

    private class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new ConnectorSplitManager()
            {
                @Override
                public ConnectorSplitSource getSplits(
                        ConnectorTransactionHandle transaction,
                        ConnectorSession session,
                        ConnectorTableHandle table,
                        SplitSchedulingStrategy splitSchedulingStrategy,
                        DynamicFilter dynamicFilter)
                {
                    AtomicBoolean splitProduced = new AtomicBoolean();

                    assertFalse(dynamicFilter.isBlocked().isDone(), "Dynamic filter should be initially blocked");

                    return new ConnectorSplitSource()
                    {
                        @Override
                        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
                        {
                            CompletableFuture<?> blocked = dynamicFilter.isBlocked();

                            if (blocked.isDone()) {
                                splitProduced.set(true);
                                return completedFuture(new ConnectorSplitBatch(ImmutableList.of(new EmptySplit(new CatalogName("test"))), isFinished()));
                            }

                            return blocked.thenApply(ignored -> {
                                // yield until dynamic filter is fully loaded
                                return new ConnectorSplitBatch(ImmutableList.of(), false);
                            });
                        }

                        @Override
                        public void close()
                        {
                        }

                        @Override
                        public boolean isFinished()
                        {
                            if (!dynamicFilter.isComplete() || !splitProduced.get()) {
                                return false;
                            }

                            assertEquals(dynamicFilter.getCurrentPredicate(), expectedDynamicFilter);
                            assertTrue(dynamicFilter.isBlocked().isDone());

                            return true;
                        }
                    };
                }
            };
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return new ConnectorPageSourceProvider()
            {
                @Override
                public ConnectorPageSource createPageSource(
                        ConnectorTransactionHandle transaction,
                        ConnectorSession session,
                        ConnectorSplit split,
                        ConnectorTableHandle table,
                        List<ColumnHandle> columns,
                        DynamicFilter dynamicFilter)
                {
                    return new EmptyPageSource();
                }
            };
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }
}
