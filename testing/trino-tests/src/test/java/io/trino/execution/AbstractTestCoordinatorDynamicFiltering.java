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
import io.trino.operator.RetryPolicy;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingPageSinkProvider;
import io.trino.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestCoordinatorDynamicFiltering
        extends AbstractTestQueryFramework
{
    private static final TestingMetadata.TestingColumnHandle ORDERKEY_HANDLE = new TestingMetadata.TestingColumnHandle("orderkey", 0, BIGINT);
    private static final TestingMetadata.TestingColumnHandle SUPP_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("suppkey", 2, BIGINT);
    private static final TestingMetadata.TestingColumnHandle ADDRESS_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("address", 2, createVarcharType(40));
    private static final TestingMetadata.TestingColumnHandle SS_SOLD_SK_HANDLE = new TestingMetadata.TestingColumnHandle("ss_sold_date_sk", 0, BIGINT);

    private volatile Set<ColumnHandle> expectedDynamicFilterColumnsCovered;
    private volatile Consumer<TupleDomain<ColumnHandle>> expectedCoordinatorDynamicFilterAssertion;
    private volatile Consumer<TupleDomain<ColumnHandle>> expectedTableScanDynamicFilterAssertion;

    @BeforeAll
    public void setup()
    {
        // create lineitem table in test connector
        getQueryRunner().installPlugin(new TestingPlugin(getRetryPolicy() == RetryPolicy.TASK));
        getQueryRunner().installPlugin(new TpchPlugin());
        getQueryRunner().installPlugin(new TpcdsPlugin());
        getQueryRunner().installPlugin(new MemoryPlugin());
        getQueryRunner().createCatalog("test", "test", ImmutableMap.of());
        getQueryRunner().createCatalog(
                "tpch",
                "tpch",
                ImmutableMap.of("tpch.partitioning-enabled", "false", "tpch.splits-per-node", "16"));
        getQueryRunner().createCatalog("tpcds", "tpcds", ImmutableMap.of());
        getQueryRunner().createCatalog("memory", "memory", ImmutableMap.of("memory.splits-per-node", "16"));
        computeActual("CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem");
        computeActual("CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer");
        computeActual("CREATE TABLE store_sales AS SELECT * FROM tpcds.tiny.store_sales");
    }

    protected abstract RetryPolicy getRetryPolicy();

    @Test
    @Timeout(30)
    public void testJoinWithEmptyBuildSide()
    {
        testJoinWithEmptyBuildSide(BROADCAST);
        testJoinWithEmptyBuildSide(PARTITIONED);
    }

    private void testJoinWithEmptyBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.none());
    }

    @Test
    @Timeout(30)
    public void testJoinWithLargeBuildSide()
    {
        testJoinWithLargeBuildSide(BROADCAST);
        testJoinWithLargeBuildSide(PARTITIONED);
    }

    private void testJoinWithLargeBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem JOIN tpch.tiny.orders ON lineitem.orderkey = orders.orderkey",
                Set.of(ORDERKEY_HANDLE),
                TupleDomain.all());
    }

    @Test
    @Timeout(30)
    public void testMultiColumnJoinWithDifferentCardinalitiesInBuildSide()
    {
        testMultiColumnJoinWithDifferentCardinalitiesInBuildSide(BROADCAST);
        testMultiColumnJoinWithDifferentCardinalitiesInBuildSide(PARTITIONED);
    }

    private void testMultiColumnJoinWithDifferentCardinalitiesInBuildSide(JoinDistributionType joinDistributionType)
    {
        // orderkey has high cardinality, suppkey has low cardinality due to filter
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem l1 " +
                        "JOIN tpch.tiny.lineitem l2 ON l1.orderkey = l2.orderkey AND l1.suppkey = l2.suppkey " +
                        "WHERE l2.suppkey BETWEEN 1 AND 10",
                Set.of(ORDERKEY_HANDLE, SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        multipleValues(BIGINT, LongStream.rangeClosed(1L, 10L).boxed().collect(toImmutableList())))));
    }

    @Test
    @Timeout(30)
    public void testJoinWithSelectiveBuildSide()
    {
        testJoinWithSelectiveBuildSide(BROADCAST);
        testJoinWithSelectiveBuildSide(PARTITIONED);
    }

    private void testJoinWithSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test
    @Timeout(30)
    public void testInequalityJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey <= supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey < supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey >= supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey > supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test
    @Timeout(30)
    public void testIsNotDistinctFromJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk = store.s_closed_date_sk",
                Set.of(SS_SOLD_SK_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.create(ValueSet.of(BIGINT, 2451189L), false))));
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk IS NOT DISTINCT FROM store.s_closed_date_sk",
                Set.of(SS_SOLD_SK_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.create(ValueSet.of(BIGINT, 2451189L), true))));
        assertQueryDynamicFilters(
                "SELECT * FROM store_sales JOIN tpcds.tiny.store ON store_sales.ss_sold_date_sk IS NOT DISTINCT FROM store.s_closed_date_sk AND store.s_closed_date_sk < 0",
                Set.of(SS_SOLD_SK_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SS_SOLD_SK_HANDLE,
                        Domain.onlyNull(BIGINT))));
    }

    @Test
    @Timeout(30)
    public void testJoinWithImplicitCoercion()
    {
        // setup fact table with integer suppkey
        computeActual("CREATE TABLE memory.default.supplier_decimal AS SELECT name, CAST(suppkey as decimal(19, 0)) suppkey_decimal FROM tpch.tiny.supplier");

        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN memory.default.supplier_decimal s ON lineitem.suppkey = s.suppkey_decimal AND s.name >= 'Supplier#000000080'",
                Set.of(SUPP_KEY_HANDLE),
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
                Set.of(ADDRESS_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        ADDRESS_KEY_HANDLE,
                        multipleValues(createVarcharType(40), values))));
    }

    @Test
    @Timeout(30)
    public void testJoinWithNonSelectiveBuildSide()
    {
        testJoinWithNonSelectiveBuildSide(BROADCAST);
        testJoinWithNonSelectiveBuildSide(PARTITIONED);
    }

    protected void testJoinWithNonSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test
    @Timeout(30)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        testJoinWithMultipleDynamicFiltersOnProbe(BROADCAST);
        testJoinWithMultipleDynamicFiltersOnProbe(PARTITIONED);
    }

    private void testJoinWithMultipleDynamicFiltersOnProbe(JoinDistributionType joinDistributionType)
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN tpch.tiny.partsupp ON t.suppkey = partsupp.suppkey AND partsupp.suppkey IN (2, 3)",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 2L))));
    }

    @Test
    @Timeout(30)
    public void testRightJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey WHERE supplier.name = 'abc'",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.none());
    }

    @Test
    @Timeout(30)
    public void testRightJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test
    @Timeout(30)
    public void testRightJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem RIGHT JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey WHERE supplier.name = 'Supplier#000000001'",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test
    @Timeout(30)
    public void testSemiJoinWithEmptyBuildSide()
    {
        testSemiJoinWithEmptyBuildSide(BROADCAST);
        testSemiJoinWithEmptyBuildSide(PARTITIONED);
    }

    private void testSemiJoinWithEmptyBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'abc')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.none());
    }

    @Test
    @Timeout(30)
    public void testSemiJoinWithLargeBuildSide()
    {
        testSemiJoinWithLargeBuildSide(BROADCAST);
        testSemiJoinWithLargeBuildSide(PARTITIONED);
    }

    private void testSemiJoinWithLargeBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM tpch.tiny.orders)",
                Set.of(ORDERKEY_HANDLE),
                TupleDomain.all());
    }

    @Test
    @Timeout(30)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        testSemiJoinWithSelectiveBuildSide(BROADCAST);
        testSemiJoinWithSelectiveBuildSide(PARTITIONED);
    }

    private void testSemiJoinWithSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name = 'Supplier#000000001')",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test
    @Timeout(30)
    public void testSemiJoinWithNonSelectiveBuildSide()
    {
        testSemiJoinWithNonSelectiveBuildSide(BROADCAST);
        testSemiJoinWithNonSelectiveBuildSide(PARTITIONED);
    }

    protected void testSemiJoinWithNonSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem WHERE lineitem.suppkey IN (SELECT supplier.suppkey FROM tpch.tiny.supplier)",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test
    @Timeout(30)
    public void testSemiJoinWithMultipleDynamicFiltersOnProbe()
    {
        testSemiJoinWithMultipleDynamicFiltersOnProbe(BROADCAST);
        testSemiJoinWithMultipleDynamicFiltersOnProbe(PARTITIONED);
    }

    private void testSemiJoinWithMultipleDynamicFiltersOnProbe(JoinDistributionType joinDistributionType)
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM (" +
                        "SELECT lineitem.suppkey FROM lineitem WHERE lineitem.suppkey IN " +
                        "(SELECT supplier.suppkey FROM tpch.tiny.supplier WHERE supplier.name IN ('Supplier#000000001', 'Supplier#000000002'))) t " +
                        "WHERE t.suppkey IN (SELECT partsupp.suppkey FROM tpch.tiny.partsupp WHERE partsupp.suppkey IN (2, 3))",
                Set.of(SUPP_KEY_HANDLE),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 2L))));
    }

    protected Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                // disable semi join to inner join rewrite to test semi join operators explicitly
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }

    private void assertQueryDynamicFilters(@Language("SQL") String query, Set<ColumnHandle> expectedColumnsCovered, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        assertQueryDynamicFilters(getSession(), query, expectedColumnsCovered, expectedTupleDomain);
    }

    private void assertQueryDynamicFilters(Session session, @Language("SQL") String query, Set<ColumnHandle> expectedColumnsCovered, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        assertQueryDynamicFilters(session, query, expectedColumnsCovered, collectedDomain -> assertThat(collectedDomain).isEqualTo(expectedTupleDomain));
    }

    protected void assertQueryDynamicFilters(
            Session session,
            @Language("SQL") String query,
            Set<ColumnHandle> expectedColumnsCovered,
            Consumer<TupleDomain<ColumnHandle>> expectedTupleDomainAssertion)
    {
        expectedDynamicFilterColumnsCovered = expectedColumnsCovered;
        expectedCoordinatorDynamicFilterAssertion = expectedTupleDomainAssertion;
        expectedTableScanDynamicFilterAssertion = expectedTupleDomainAssertion;

        computeActual(session, query);
    }

    private class TestingPlugin
            implements Plugin
    {
        private final boolean isTaskRetryMode;

        public TestingPlugin(boolean isTaskRetryMode)
        {
            this.isTaskRetryMode = isTaskRetryMode;
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
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(metadata, isTaskRetryMode);
                }
            });
        }
    }

    private class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;
        private final boolean isTaskRetryMode;

        private TestConnector(ConnectorMetadata metadata, boolean isTaskRetryMode)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.isTaskRetryMode = isTaskRetryMode;
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
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
                        DynamicFilter dynamicFilter,
                        Constraint constraint)
                {
                    if (!isTaskRetryMode) {
                        // In task retry mode, dynamic filter collection is done outside the join stage,
                        // so it's not necessary that dynamicFilter will be blocked initially.
                        assertThat(dynamicFilter.isBlocked().isDone())
                                .describedAs("Dynamic filter should be initially blocked")
                                .isFalse();
                    }
                    assertThat(dynamicFilter.getColumnsCovered())
                            .describedAs("columns covered")
                            .isEqualTo(expectedDynamicFilterColumnsCovered);

                    AtomicBoolean splitProduced = new AtomicBoolean();
                    return new ConnectorSplitSource()
                    {
                        @Override
                        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
                        {
                            CompletableFuture<?> blocked = dynamicFilter.isBlocked();

                            if (blocked.isDone()) {
                                splitProduced.set(true);
                                return completedFuture(new ConnectorSplitBatch(ImmutableList.of(createRemoteSplit()), isFinished()));
                            }

                            return blocked.thenApply(_ -> {
                                // yield until dynamic filter is fully loaded
                                return new ConnectorSplitBatch(ImmutableList.of(), false);
                            });
                        }

                        @Override
                        public void close() {}

                        @Override
                        public boolean isFinished()
                        {
                            assertThat(dynamicFilter.getColumnsCovered())
                                    .describedAs("columns covered")
                                    .isEqualTo(expectedDynamicFilterColumnsCovered);

                            if (!dynamicFilter.isComplete() || !splitProduced.get()) {
                                return false;
                            }

                            assertThat(dynamicFilter.isBlocked().isDone()).isTrue();
                            expectedCoordinatorDynamicFilterAssertion.accept(dynamicFilter.getCurrentPredicate());

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
                    assertThat(dynamicFilter.getColumnsCovered())
                            .describedAs("columns covered")
                            .isEqualTo(expectedDynamicFilterColumnsCovered);

                    return new EmptyPageSource()
                    {
                        @Override
                        public CompletableFuture<?> isBlocked()
                        {
                            return dynamicFilter.isBlocked();
                        }

                        @Override
                        public boolean isFinished()
                        {
                            assertThat(dynamicFilter.getColumnsCovered())
                                    .describedAs("columns covered")
                                    .isEqualTo(expectedDynamicFilterColumnsCovered);

                            if (!dynamicFilter.isComplete()) {
                                return false;
                            }

                            // ConnectorPageSource is blocked until the dynamicFilter is complete
                            assertThat(dynamicFilter.isBlocked().isDone()).isTrue();
                            expectedTableScanDynamicFilterAssertion.accept(dynamicFilter.getCurrentPredicate());

                            return true;
                        }
                    };
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
