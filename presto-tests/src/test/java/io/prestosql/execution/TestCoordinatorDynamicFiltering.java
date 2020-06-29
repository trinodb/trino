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
import io.airlift.units.Duration;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@Test(singleThreaded = true)
public class TestCoordinatorDynamicFiltering
        extends AbstractTestQueryFramework
{
    private static final TestingColumnHandle SUPP_KEY_HANDLE = new TestingColumnHandle("suppkey", 2, BIGINT);

    private final AtomicReference<TupleDomain<ColumnHandle>> expectedDynamicFilter = new AtomicReference<>(TupleDomain.all());

    private ExecutorService executorService;

    @BeforeClass
    public void setup()
    {
        executorService = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executorService.shutdownNow();
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
                .setExtraProperties(ImmutableMap.of("query.min-schedule-split-batch-size", "1"))
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        getQueryRunner().installPlugin(new TestPlugin(expectedDynamicFilter));
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
        computeActual("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod()
    {
        expectedDynamicFilter.set(TupleDomain.all());
    }

    @Test
    public void testJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test
    public void testJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test
    public void testBroadcastJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test
    public void testJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test
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

    private void assertQueryDynamicFilters(@Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        expectedDynamicFilter.set(expectedTupleDomain);
        computeActual(query);
    }

    private void assertQueryDynamicFilters(Session session, @Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        expectedDynamicFilter.set(expectedTupleDomain);
        computeActual(session, query);
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private class TestPlugin
            implements Plugin
    {
        private final AtomicReference<TupleDomain<ColumnHandle>> expectedDynamicFilter;

        private TestPlugin(AtomicReference<TupleDomain<ColumnHandle>> expectedDynamicFilter)
        {
            this.expectedDynamicFilter = requireNonNull(expectedDynamicFilter, "expectedDynamicFilter is null");
        }

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
                    return new TestConnector(metadata, Duration.valueOf("10s"), expectedDynamicFilter);
                }
            });
        }
    }

    private class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;
        private final Duration scanDuration;
        private final AtomicReference<TupleDomain<ColumnHandle>> expectedDynamicFilter;

        private TestConnector(ConnectorMetadata metadata, Duration scanDuration, AtomicReference<TupleDomain<ColumnHandle>> expectedDynamicFilter)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.scanDuration = requireNonNull(scanDuration, "scanDuration is null");
            this.expectedDynamicFilter = requireNonNull(expectedDynamicFilter, "expectedDynamicFilter is null");
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
                        Supplier<TupleDomain<ColumnHandle>> dynamicFilter)
                {
                    long start = System.nanoTime();
                    AtomicBoolean splitProduced = new AtomicBoolean();

                    return new ConnectorSplitSource()
                    {
                        @Override
                        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
                        {
                            // producing single empty split allows to assert that dynamic filters will be collected for broadcast
                            // joins once the first probe side task starts running
                            if (!splitProduced.get()) {
                                splitProduced.set(true);
                                return completedFuture(new ConnectorSplitBatch(ImmutableList.of(new EmptySplit(new CatalogName("test"))), false));
                            }

                            return CompletableFuture.supplyAsync(
                                    () -> {
                                        try {
                                            sleep(50);
                                            return new ConnectorSplitBatch(ImmutableList.of(), isFinished());
                                        }
                                        catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    },
                                    executorService);
                        }

                        @Override
                        public void close()
                        {
                        }

                        @Override
                        public boolean isFinished()
                        {
                            if (dynamicFilter.get().equals(expectedDynamicFilter.get())) {
                                // if we received expected dynamic filter then we are done
                                return true;
                            }
                            else if (Duration.nanosSince(start).compareTo(scanDuration) > 0) {
                                throw new AssertionError(format(
                                        "Received %s instead of expected dynamic filter %s after waiting for %s",
                                        dynamicFilter.get().toString(session),
                                        expectedDynamicFilter.get().toString(session),
                                        scanDuration));
                            }
                            // expected dynamic filter is not set yet
                            return false;
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
