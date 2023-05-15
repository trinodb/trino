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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryStats;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.SplitSource;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakeDynamicFiltering
        extends AbstractTestQueryFramework
{
    private final String bucketName = "delta-lake-test-dynamic-filtering-" + randomNameSuffix();
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();

        QueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                "default",
                ImmutableMap.of("delta.register-table-procedure.enabled", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());

        ImmutableList.of(LINE_ITEM, ORDERS).forEach(table -> {
            String tableName = table.getTableName();
            hiveMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/databricks/" + tableName, tableName);
            queryRunner.execute(format("CALL %1$s.system.register_table('%2$s', '%3$s', 's3://%4$s/%3$s')",
                    DELTA_CATALOG,
                    "default",
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(JoinDistributionType.values())
                .collect(toDataProvider());
    }

    @Test(timeOut = 60_000, dataProvider = "joinDistributionTypes")
    public void testDynamicFiltering(JoinDistributionType joinDistributionType)
    {
        String query = "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 59995 AND orders.totalprice < 60000";
        MaterializedResultWithQueryId filteredResult = getDistributedQueryRunner().executeWithQueryId(sessionWithDynamicFiltering(true, joinDistributionType), query);
        MaterializedResultWithQueryId unfilteredResult = getDistributedQueryRunner().executeWithQueryId(sessionWithDynamicFiltering(false, joinDistributionType), query);
        assertEqualsIgnoreOrder(filteredResult.getResult().getMaterializedRows(), unfilteredResult.getResult().getMaterializedRows());

        QueryInputStats filteredStats = getQueryInputStats(filteredResult.getQueryId());
        QueryInputStats unfilteredStats = getQueryInputStats(unfilteredResult.getQueryId());
        assertGreaterThan(unfilteredStats.numberOfSplits, filteredStats.numberOfSplits);
        assertGreaterThan(unfilteredStats.inputPositions, filteredStats.inputPositions);
    }

    @Test(timeOut = 30_000)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        QueryRunner runner = getQueryRunner();
        TransactionManager transactionManager = runner.getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(true);
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, "dynamic_filtering_wait_timeout", "1s")
                .build()
                .beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        QualifiedObjectName tableName = new QualifiedObjectName(DELTA_CATALOG, "default", "orders");
        Optional<TableHandle> tableHandle = runner.getMetadata().getTableHandle(session, tableName);
        assertTrue(tableHandle.isPresent());
        SplitSource splitSource = runner.getSplitManager()
                .getSplits(session, Span.getInvalid(), tableHandle.get(), new IncompleteDynamicFilter(), alwaysTrue());
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
        }
        splitSource.close();
        assertFalse(splits.isEmpty());
    }

    private Session sessionWithDynamicFiltering(boolean enabled, JoinDistributionType joinDistributionType)
    {
        return Session.builder(noJoinReordering(joinDistributionType))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, String.valueOf(enabled))
                .setCatalogSessionProperty(DELTA_CATALOG, "dynamic_filtering_wait_timeout", "1h")
                .build();
    }

    private QueryInputStats getQueryInputStats(QueryId queryId)
    {
        QueryStats stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        long numberOfSplits = stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .mapToLong(OperatorStats::getTotalDrivers)
                .sum();
        long inputPositions = stats.getPhysicalInputPositions();
        return new QueryInputStats(numberOfSplits, inputPositions);
    }

    private static class IncompleteDynamicFilter
            implements DynamicFilter
    {
        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.HOURS.sleep(1);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }));
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return true;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();
        }
    }

    private static class QueryInputStats
    {
        final long numberOfSplits;
        final long inputPositions;

        QueryInputStats(long numberOfSplits, long inputPositions)
        {
            this.numberOfSplits = numberOfSplits;
            this.inputPositions = inputPositions;
        }
    }
}
