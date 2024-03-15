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
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.SplitSource;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
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
            hiveMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/databricks73/" + tableName, tableName);
            queryRunner.execute(format("CALL %1$s.system.register_table('%2$s', '%3$s', 's3://%4$s/%3$s')",
                    DELTA_CATALOG,
                    "default",
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @Test
    @Timeout(60)
    public void testDynamicFiltering()
    {
        for (JoinDistributionType joinDistributionType : JoinDistributionType.values()) {
            String query = "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 59995 AND orders.totalprice < 60000";
            MaterializedResultWithPlan filteredResult = getDistributedQueryRunner().executeWithPlan(sessionWithDynamicFiltering(true, joinDistributionType), query);
            MaterializedResultWithPlan unfilteredResult = getDistributedQueryRunner().executeWithPlan(sessionWithDynamicFiltering(false, joinDistributionType), query);
            assertEqualsIgnoreOrder(filteredResult.result().getMaterializedRows(), unfilteredResult.result().getMaterializedRows());

            QueryStats filteredStats = getQueryStats(filteredResult.queryId());
            QueryStats unfilteredStats = getQueryStats(unfilteredResult.queryId());
            assertGreaterThan(unfilteredStats.getPhysicalInputPositions(), filteredStats.getPhysicalInputPositions());
        }
    }

    @Test
    @Timeout(30)
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
        Optional<TableHandle> tableHandle = runner.getPlannerContext().getMetadata().getTableHandle(session, tableName);
        assertThat(tableHandle.isPresent()).isTrue();
        CompletableFuture<Void> dynamicFilterBlocked = new CompletableFuture<>();
        try {
            SplitSource splitSource = runner.getSplitManager()
                    .getSplits(session, Span.getInvalid(), tableHandle.get(), new BlockedDynamicFilter(dynamicFilterBlocked), alwaysTrue());
            List<Split> splits = new ArrayList<>();
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
            }
            splitSource.close();
            assertThat(splits.isEmpty()).isFalse();
        }
        finally {
            dynamicFilterBlocked.complete(null);
        }
    }

    private Session sessionWithDynamicFiltering(boolean enabled, JoinDistributionType joinDistributionType)
    {
        return Session.builder(noJoinReordering(joinDistributionType))
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, String.valueOf(enabled))
                .setCatalogSessionProperty(DELTA_CATALOG, "dynamic_filtering_wait_timeout", "1h")
                .build();
    }

    private QueryStats getQueryStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
    }

    private static class BlockedDynamicFilter
            implements DynamicFilter
    {
        private final CompletableFuture<?> isBlocked;

        public BlockedDynamicFilter(CompletableFuture<?> isBlocked)
        {
            this.isBlocked = requireNonNull(isBlocked, "isBlocked is null");
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return ImmutableSet.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return isBlocked;
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
}
