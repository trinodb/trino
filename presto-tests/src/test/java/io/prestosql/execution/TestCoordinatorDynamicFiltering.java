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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.EmptyPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.split.EmptySplit;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingHandleResolver;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingPageSinkProvider;
import io.prestosql.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
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

    private volatile TupleDomain<ColumnHandle> expectedDynamicFilter;

    @BeforeClass
    public void setup()
    {
        // create lineitem table in test connector
        getQueryRunner().installPlugin(new TestPlugin());
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
        computeActual("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
        return DistributedQueryRunner.builder(session)
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
    public void testJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, true)), false))));
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
                        Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, true)), false))));
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
        return Session.builder(this.getQueryRunner().getDefaultSession())
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
                        TupleDomain<ColumnHandle> dynamicFilter)
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
